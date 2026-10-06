package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// movedPlanFixture edits strategy 1001 with edit, cuts over, and runs the
// Query Group its Plan moved to through one FULL Slot.
func movedPlanFixture(t *testing.T, edit func(first map[string]any)) (*cutoverStallFixture, execution.QueryGroupIdentity, execution.QueryGroupIdentity) {
	t.Helper()
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	old := f.queryGroup
	oldSchedule := f.initialSchedule
	installEditedStrategies(t, ctx, f.redisClient, 1725000600, edit)
	f.clock.Store((f.base + 59) * 1000)
	for n := 0; n < 2; n++ {
		if err := f.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	var next execution.QueryGroupIdentity
	for _, candidate := range f.bundle.queryGroups {
		schedule, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, candidate)
		if err == nil && len(schedule.Plans) == 1 && schedule.Plans[0].Key() == oldSchedule.Plans[0].Key() && candidate != old {
			next = candidate
		}
	}
	if next == "" {
		t.Fatal("edit did not move the Plan to a new Query Group")
	}
	f.queryGroup = next
	f.runner = settledRunner(f.bundle, next)
	_ = runOneSlotFull(t, f)
	return f, old, next
}

// expireOldContent drops the old Query Group's snapshot and object keys, as
// the zero-bridge expiry test does to stand for content retention having
// passed.
func expireOldContent(t *testing.T, f *cutoverStallFixture, oldSchedule execution.FrozenQueryGroupSchedule) {
	t.Helper()
	ctx := context.Background()
	keys, err := f.redisClient.Keys(ctx, "*"+string(oldSchedule.Segment.Publication.SnapshotRevision)+"*").Result()
	if err != nil {
		t.Fatal(err)
	}
	objectKeys, err := f.redisClient.Keys(ctx, "*:qgobj:"+string(oldSchedule.Segment.ObjectDigest)).Result()
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, objectKeys...)
	if len(keys) == 0 {
		t.Fatal("no content keys found to expire")
	}
	if err := f.redisClient.Del(ctx, keys...).Err(); err != nil {
		t.Fatal(err)
	}
}

// A query edit (not a time_delay change) moves the Plan to a new Query Group
// whose route differs: nothing is inherited. After the old group's content
// has passed its retention, the new group's owner restarts. It must keep
// evaluating; the link to the old group carries nothing it needs.
func TestAQueryEditedGroupKeepsRunningAfterTheOldContentIsGone(t *testing.T) {
	restartQueryEditedGroup(t, true)
}

// The same restart while the old content is still kept: the control.
func TestAQueryEditedGroupWithItsOldContentKeepsRunning(t *testing.T) {
	restartQueryEditedGroup(t, false)
}

// restartQueryEditedGroup edits strategy 1001's query, runs the group its
// Plan moved to, optionally lets the old content expire, and restarts the
// new group's owner; the group must complete a FULL Slot again. The edit
// keeps the state generation, so the Plan is linked to keep ordering; the
// route changed, so no lateness carries over.
func restartQueryEditedGroup(t *testing.T, deleteContent bool) {
	ctx := context.Background()
	f, old, next := movedPlanFixture(t, func(first map[string]any) {
		first["items"].([]any)[0].(map[string]any)["query_configs"].([]any)[0].(map[string]any)["agg_method"] = "max"
	})
	oldSchedule, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	links, _, err := f.repository.ReadHoldPredecessors(ctx, schedule)
	if err != nil || len(links) != 1 || links[0].QueryGroup != old || len(links[0].Plans) != 1 || links[0].Plans[0].SameRoute {
		t.Fatalf("a query edit that keeps the state is linked, as another route: %+v %v", links, err)
	}
	if deleteContent {
		expireOldContent(t, f, oldSchedule)
		if err := f.repository.ConfigureObjectCache(1, 1); err != nil {
			t.Fatal(err)
		}
	}
	restartUntilFull(t, f, next)
	if record, _ := f.bundle.dependencies.ReadHolds.controller.Reading(next); record.ArrivalAgeMillis != 0 {
		t.Fatalf("another route's lateness carried over: %+v", record)
	}
}

// restartUntilFull restarts next's owner -- its read hold memory for the
// group is gone -- and runs the group until it completes a FULL Slot, which it
// must, whatever its predecessor left. It returns the Progress after it.
func restartUntilFull(t *testing.T, f *cutoverStallFixture, next execution.QueryGroupIdentity) execution.ScheduleProgress {
	t.Helper()
	ctx := context.Background()
	holds := f.bundle.dependencies.ReadHolds
	holds.mu.Lock()
	holds.groups[next].prepared = execution.ScheduleSegmentFact{}
	holds.mu.Unlock()
	holds.controller.Forget(next)
	before := f.progress(ctx)
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if nextAt := f.runner.NextReadyAt(); nextAt.After(f.now()) {
			f.clock.Store(nextAt.UnixMilli() + 1)
		}
		at := f.now()
		result, attempted, err := f.runner.RunOne(ctx)
		lastErr = err
		if err == nil && attempted && result.Completed && result.CompletionKind == execution.CompletionFull {
			return f.progress(ctx)
		}
		f.clock.Store(at.Add(30 * time.Second).UnixMilli())
	}
	after := f.progress(ctx)
	lease, _ := holds.groups[next].session.Current()
	current, _ := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, next, after.NextSlot)
	t.Logf("PrepareSchedule now: %v", holds.PrepareSchedule(ctx, current, lease.Fence))
	t.Fatalf("the group stopped evaluating after a restart: last error %v; NextSlot %d -> %d over %s",
		lastErr, before.NextSlot, after.NextSlot, f.now().Sub(time.Unix(int64(before.NextSlot), 0)))
	return after
}

// holdRecordKey is a Query Group's read hold record in the fixture's Redis.
func holdRecordKey(f *cutoverStallFixture, qg execution.QueryGroupIdentity) string {
	return productionPhaseTwoPrefix(f.cfg.Redis.StatePrefix, "ownership") + ":{" + ownership.ControlHashTag(qg) + "}:" +
		productionPhaseTwoPrefix(f.cfg.Redis.StatePrefix, "schedule") + ":" + readhold.Namespace
}

// delayEdited moves strategy 1001's Plan to a new group by a time_delay
// edit: same state, same route.
func delayEdited(t *testing.T) (*cutoverStallFixture, execution.QueryGroupIdentity, execution.QueryGroupIdentity, controlplane.ReadHoldLinkPlan) {
	t.Helper()
	f, old, next := movedPlanFixture(t, func(first map[string]any) { first["items"].([]any)[0].(map[string]any)["time_delay"] = 120 })
	schedule, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	links, _, err := f.repository.ReadHoldPredecessors(context.Background(), schedule)
	if err != nil || len(links) != 1 || links[0].QueryGroup != old || len(links[0].Plans) != 1 || !links[0].Plans[0].SameRoute ||
		links[0].Plans[0].PreviousSlot <= 0 {
		t.Fatalf("a time_delay edit is linked by the same route, with the old last Slot: %+v %v", links, err)
	}
	return f, old, next, links[0].Plans[0]
}

// withoutPredecessorProgress makes the old group's Progress unreadable, so
// its last Slot's contract cannot give the exact hold and the record decides.
func withoutPredecessorProgress(f *cutoverStallFixture) {
	f.bundle.dependencies.ReadHolds.progress = &fakeProductionProgressReader{byGroup: map[execution.QueryGroupIdentity]execution.ProgressLoadResult{}}
}

func predecessorCount(f *cutoverStallFixture, reason string) uint64 {
	return f.bundle.dependencies.ReadHolds.controller.Stats().Predecessors[reason]
}

// (a') The old group's closed Segment is pruned and its content gone; a
// time_delay-edited group restarts. The link itself says the old last Slot
// and offset, so nothing of the old timeline or content is read.
func TestADelayEditedGroupKeepsRunningAfterTheOldSegmentAndContentAreGone(t *testing.T) {
	ctx := context.Background()
	f, old, next, _ := delayEdited(t)
	oldSchedule, err := f.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	expireOldContent(t, f, oldSchedule)
	if err := f.repository.ConfigureObjectCache(1, 1); err != nil {
		t.Fatal(err)
	}
	timelines, err := f.redisClient.Keys(ctx, "*:schedule_timeline:"+string(old)).Result()
	if err != nil || len(timelines) != 1 {
		t.Fatalf("old timeline keys %v %v", timelines, err)
	}
	if err := f.redisClient.Del(ctx, timelines...).Err(); err != nil {
		t.Fatal(err)
	}
	restartUntilFull(t, f, next)
}

// (c) The old group, at zero and so without a record, has frozen its last
// Slot of the Plan and gone on: the zero is proven and the group reads at
// once.
func TestAZeroPredecessorThatMovedOnIsProvenZero(t *testing.T) {
	f, old, next, link := delayEdited(t)
	if n, _ := f.redisClient.Exists(context.Background(), holdRecordKey(f, old)).Result(); n != 0 {
		t.Fatal("a zero group kept a record; the fixture does not test a missing one")
	}
	moved := execution.ScheduleProgress{Identity: execution.ProgressIdentity{QueryGroup: old}, NextSlot: link.PreviousSlot + 120}
	holds := f.bundle.dependencies.ReadHolds
	holds.progress = &fakeProductionProgressReader{byGroup: map[execution.QueryGroupIdentity]execution.ProgressLoadResult{old: {Status: execution.ProgressFound, Progress: &moved}}}
	after := restartUntilFull(t, f, next)
	if predecessorCount(f, readhold.PredecessorZeroProven) == 0 || after.LastCompletion == nil || after.LastCompletion.Contract.ReadHoldMillis != 0 {
		t.Fatalf("a moved-on zero predecessor was not proven zero: %v %+v", holds.controller.Stats().Predecessors, after.LastCompletion)
	}
}

// (e) The old group's record holds a nonzero hold and was never closed --
// its owner did not come back. The group reads its first Slots as late as the
// bound asks, never refused, and takes the old arrival age (same route).
func TestAnOpenPredecessorRecordIsReadAsTheBound(t *testing.T) {
	ctx := context.Background()
	f, old, next, _ := delayEdited(t)
	spec, err := f.bundle.dependencies.ReadHolds.spec(ctx, f.initialSchedule)
	if err != nil {
		t.Fatal(err)
	}
	open := readhold.Record{SinceSlot: 1, HoldMillis: 150_000, ArrivalAgeMillis: 400_000, SegmentStart: f.initialSchedule.Segment.Start,
		Plans: []readhold.PlanRecord{{PlanRef: spec.Plans[0], ArrivalAgeMillis: 400_000}}}
	raw, err := json.Marshal(open)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.redisClient.Set(ctx, holdRecordKey(f, old), raw, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	withoutPredecessorProgress(f)
	after := restartUntilFull(t, f, next)
	record, _ := f.bundle.dependencies.ReadHolds.controller.Reading(next)
	if predecessorCount(f, readhold.PredecessorRecordOpen) == 0 || record.ArrivalAgeMillis != 400_000 || after.LastCompletion.Contract.ReadHoldMillis <= 0 {
		t.Fatalf("an open predecessor was not read as the bound with its arrival age: %v %+v %+v",
			f.bundle.dependencies.ReadHolds.controller.Stats().Predecessors, record, after.LastCompletion)
	}
}

// (f) A predecessor record that does not decode is read as the bound; the
// group's own record that does not decode is replaced, and the group runs.
func TestCorruptRecordsNeverStopTheGroup(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "predecessor", true: "own"}[own], func(t *testing.T) {
			ctx := context.Background()
			f, old, next, _ := delayEdited(t)
			key := holdRecordKey(f, old)
			if own {
				key = holdRecordKey(f, next)
			}
			if err := f.redisClient.Set(ctx, key, "{not a record", time.Hour).Err(); err != nil {
				t.Fatal(err)
			}
			withoutPredecessorProgress(f)
			restartUntilFull(t, f, next)
			stats := f.bundle.dependencies.ReadHolds.controller.Stats()
			if !own && stats.Predecessors[readhold.PredecessorRecordCorrupt] == 0 {
				t.Fatalf("a corrupt predecessor was not counted: %v", stats.Predecessors)
			}
			if own {
				raw, err := f.redisClient.Get(ctx, key).Bytes()
				if _, decodeErr := readhold.Decode(raw); stats.OwnCorrupt == 0 || err != nil || decodeErr != nil {
					t.Fatalf("the group's corrupt record was not replaced: corrupt %d, %v %v", stats.OwnCorrupt, err, decodeErr)
				}
			}
		})
	}
}

// A time_delay change seven days back: the new group's zero bridge has
// expired, and so has every fact about the old group. A restart that finds
// the group's last Slot completed with a partial gap - or mid-Slot - cannot
// prove the zero, and the group can never complete another Slot to prove it.
func TestADelayEditedGroupIsNotStoppedByAPartialLastCompletionPastTheRecordLifetime(t *testing.T) {
	h, _, schedule, fence, _, p := runtimeExpiredZeroBridge(t)
	p.LastDataSlot = p.LastCompletion.Slot
	p.LastFullSlot = p.LastCompletion.Slot - 60
	p.LastCompletionKind = execution.CompletionPartialGap
	p.LastCompletion.Kind = execution.CompletionPartialGap
	h.progress = &fakeProductionProgressReader{byGroup: map[execution.QueryGroupIdentity]execution.ProgressLoadResult{p.Identity.QueryGroup: {Status: execution.ProgressFound, Progress: &p}}}
	if err := h.PrepareSchedule(context.Background(), schedule, fence); err != nil {
		t.Fatalf("delay-edited group blocked for good after a partial last completion: %v", err)
	}
}
