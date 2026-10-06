package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
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
// new group's owner; the group must complete a FULL Slot again.
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
	links, err := f.repository.ReadHoldPredecessors(ctx, schedule)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("links of the query-edited group: %+v", links)
	if deleteContent {
		expireOldContent(t, f, oldSchedule)
		if err := f.repository.ConfigureObjectCache(1, 1); err != nil {
			t.Fatal(err)
		}
	}
	// The owner restarts: its read-hold memory for the group is gone.
	holds := f.bundle.dependencies.ReadHolds
	holds.mu.Lock()
	holds.groups[next].prepared = execution.ScheduleSegmentFact{}
	holds.mu.Unlock()
	holds.controller.Forget(next)
	before := f.progress(ctx)
	full := 0
	var lastErr error
	for attempt := 0; attempt < 20 && full == 0; attempt++ {
		if nextAt := f.runner.NextReadyAt(); nextAt.After(f.now()) {
			f.clock.Store(nextAt.UnixMilli() + 1)
		}
		at := f.now()
		result, attempted, err := f.runner.RunOne(ctx)
		lastErr = err
		if err == nil && attempted && result.Completed && result.CompletionKind == execution.CompletionFull {
			full++
			break
		}
		f.clock.Store(at.Add(30 * time.Second).UnixMilli())
	}
	after := f.progress(ctx)
	if full == 0 {
		lease, _ := holds.groups[next].session.Current()
		current, _ := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, next, after.NextSlot)
		t.Logf("PrepareSchedule now: %v", holds.PrepareSchedule(ctx, current, lease.Fence))
		t.Fatalf("query-edited group stopped evaluating after a restart: last error %v; NextSlot %d -> %d over %s",
			lastErr, before.NextSlot, after.NextSlot, f.now().Sub(time.Unix(int64(before.NextSlot), 0)))
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

// A predecessor Query Group that keeps other Plans moves on to the Slots of
// its next Segment. Its zero hold at the closed Segment's last Slot is still
// what it was; once it has completed a later Slot it must still be confirmed.
func TestAZeroPredecessorThatMovedOnIsStillConfirmed(t *testing.T) {
	h, f, schedule, _, old, _ := runtimeExpiredZeroBridge(t)
	links, err := f.repository.ReadHoldPredecessors(context.Background(), schedule)
	if err != nil || len(links) != 1 {
		t.Fatalf("links=%+v %v", links, err)
	}
	oldSchedule, err := h.catalog.ReadFrozenSchedule(context.Background(), old, links[0].ClosedAt-1)
	if err != nil {
		t.Fatal(err)
	}
	final := oldSchedule.Plans[0].Spec
	last := int64(*oldSchedule.Segment.End) - 1
	last -= (last - int64(final.Alignment)) % final.EvaluationIntervalSeconds
	loaded, err := f.production.dependencies.Progress.LoadProgress(context.Background(), execution.ProgressIdentity{QueryGroup: old})
	if err != nil || loaded.Progress == nil || loaded.Progress.LastCompletion == nil {
		t.Fatalf("old progress %+v %v", loaded, err)
	}
	p := *loaded.Progress
	if p.LastCompletion.Slot != execution.EvaluationTime(last) {
		t.Fatalf("old group's last completion %d, want the closed Segment's last Slot %d", p.LastCompletion.Slot, last)
	}
	// The old group goes on with its other Plans: one more Slot completed in
	// the Segment after the boundary, under no hold.
	moved := *p.LastCompletion
	moved.Slot += 60
	moved.Contract.Slot.EvaluationTime = moved.Slot
	moved.Contract.ScheduleSegmentStart = *oldSchedule.Segment.End
	moved.CompletedAt = time.Unix(int64(moved.Slot)+20, 0).UTC().Format(time.RFC3339Nano)
	p.LastCompletion = &moved
	p.LastFullSlot, p.LastDataSlot, p.NextSlot = moved.Slot, moved.Slot, moved.Slot+60
	h.progress = &fakeProductionProgressReader{byGroup: map[execution.QueryGroupIdentity]execution.ProgressLoadResult{old: {Status: execution.ProgressFound, Progress: &p}}}
	h.now = func() time.Time { return time.Unix(int64(moved.Slot)+30, 0) }
	confirmed, err := h.zeroPredecessor(context.Background(), oldSchedule)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmed {
		t.Fatal("a zero predecessor that completed one later Slot is no longer confirmed: its successor is blocked from then on")
	}
}
