package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// writeHoldRecord stores a Query Group's read hold record in the fixture's
// Redis, as its owner would have.
func writeHoldRecord(t *testing.T, f *cutoverStallFixture, qg execution.QueryGroupIdentity, record readhold.Record) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.redisClient.Set(context.Background(), holdRecordKey(f, qg), raw, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
}

// thresholdCut cuts the fixture group's Segment by a threshold edit, which
// keeps the group, runs one FULL Slot of the new Segment, and returns the
// group and its current and retained previous schedules.
func thresholdCut(t *testing.T) (*cutoverStallFixture, execution.QueryGroupIdentity, execution.FrozenQueryGroupSchedule, execution.FrozenQueryGroupSchedule) {
	t.Helper()
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	qg := f.queryGroup
	installEditedStrategies(t, ctx, f.redisClient, 1725000600, func(first map[string]any) {
		item := first["items"].([]any)[0].(map[string]any)
		for _, algorithm := range item["algorithms"].([]any) {
			for _, group := range algorithm.(map[string]any)["config"].([]any) {
				for _, condition := range group.([]any) {
					condition.(map[string]any)["threshold"] = 81
				}
			}
		}
	})
	f.clock.Store((f.base + 59) * 1000)
	for n := 0; n < 2; n++ {
		if err := f.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	_ = runOneSlotFull(t, f)
	current, err := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, qg, f.progress(ctx).NextSlot)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, qg, current.Segment.Start-1)
	if err != nil || previous.Segment.End == nil {
		t.Fatalf("the previous Segment is not retained; the fixture does not test it: %+v %v", previous, err)
	}
	return f, qg, current, previous
}

// A group whose record is already past its previous Segment without that
// Segment's closing facts, while the Segment is still retained, goes on after
// a restart: the facts cannot be rebuilt, and waiting for them waited until a
// content edit pruned the Segment. The skip is counted.
func TestARecordPastTheRetainedPreviousSegmentWithoutItsClosureDoesNotStopTheGroup(t *testing.T) {
	f, qg, current, _ := thresholdCut(t)
	writeHoldRecord(t, f, qg, readhold.Record{SinceSlot: 1, HoldMillis: 60_000, SegmentStart: current.Segment.Start})
	restartUntilFull(t, f, qg)
	if skipped := f.bundle.dependencies.ReadHolds.closeSkipped.Load(); skipped == 0 {
		t.Fatal("the skipped close was not counted")
	}
}

// A restart reads nothing of the retained previous Segment's content: the
// route and delay are the group's and come from the current Segment, so a
// corrupt old content object cannot pause the group.
func TestACorruptPreviousSegmentContentDoesNotStopTheGroup(t *testing.T) {
	ctx := context.Background()
	f, qg, _, previous := thresholdCut(t)
	keys, err := f.redisClient.Keys(ctx, "*:qgobj:"+string(previous.Segment.ObjectDigest)).Result()
	if err != nil || len(keys) == 0 {
		t.Fatalf("no content object of the previous Segment: %v %v", keys, err)
	}
	for _, key := range keys {
		if err := f.redisClient.Set(ctx, key, "{not a content object", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.repository.ConfigureObjectCache(1, 1); err != nil {
		t.Fatal(err)
	}
	restartUntilFull(t, f, qg)
}

// A held group that lost a Plan to another group more than a week ago: its
// record dropped the departed Plan's closure, while the Segment the Plan left
// is still the group's retained previous one. The next restart -- a rollout
// -- must not stop the group until the next content edit.
//
// Strategy 1001's time_delay edit moves its Plan to a new group, while 1002
// takes 1001's old query and so the group 1001 left: that group's previous
// Segment holds the departed Plan, its current one 1002's.
func TestAHeldGroupThatLostAPlanAWeekAgoKeepsRunningAfterARestart(t *testing.T) {
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	left := f.queryGroup
	installEditedStrategies(t, ctx, f.redisClient, 1725000600, func(first map[string]any) {
		first["items"].([]any)[0].(map[string]any)["time_delay"] = 120
	})
	// 1002 as 1001 was: the same query, so the group 1001 left.
	raw, err := f.redisClient.Get(ctx, "alarm-config.strategy_1001").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var second map[string]any
	if err := json.Unmarshal(raw, &second); err != nil {
		t.Fatal(err)
	}
	second["id"] = 1002
	item := second["items"].([]any)[0].(map[string]any)
	item["id"], item["time_delay"] = 12, 0
	encoded, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.redisClient.Set(ctx, "alarm-config.strategy_1002", encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	f.clock.Store((f.base + 59) * 1000)
	for n := 0; n < 2; n++ {
		if err := f.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	timeline, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, left)
	if err != nil {
		t.Fatal(err)
	}
	var current, previous execution.FrozenQueryGroupSchedule
	for at := timeline.Segment.Start; ; {
		schedule, err := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, left, at)
		if err != nil {
			t.Fatalf("the group 1001 left has no Segment after the cut; the fixture does not test it: %v", err)
		}
		if schedule.Segment.End == nil {
			current = schedule
			break
		}
		previous, at = schedule, *schedule.Segment.End
	}
	if previous.Segment.End == nil || len(previous.Plans) == 0 || len(current.Plans) == 0 || previous.Plans[0].Key() == current.Plans[0].Key() {
		t.Fatalf("the group's previous Segment does not hold the departed Plan beside a current one: %+v / %+v", previous.Plans, current.Plans)
	}
	f.queryGroup, f.runner = left, settledRunner(f.bundle, left)
	_ = runOneSlotFull(t, f)
	// The departed Plan's closure was dropped after a week.
	writeHoldRecord(t, f, left, readhold.Record{SinceSlot: 1, HoldMillis: 60_000, SegmentStart: current.Segment.Start})
	restartUntilFull(t, f, left)
	if skipped := f.bundle.dependencies.ReadHolds.closeSkipped.Load(); skipped == 0 {
		t.Fatal("the skipped close was not counted")
	}
}
