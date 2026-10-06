package main

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Query Group whose open Segment had its output context revised long ago:
// the contexts the Segment opened with and the publication it opened under
// are past their retention, while the Query Group object and the revised
// contexts are kept. Its Slots read the revised contexts by content and run;
// so must the read hold's own read of the group's route and delay, after
// the owner restarts. A long-lived Segment of a strategy edited only in what
// it renders with (any edit of the strategy document, even its update time)
// is this group.
func TestARevisedSegmentWhoseFirstContextsAreGoneKeepsRunning(t *testing.T) {
	restartRevisedSegment(t, true)
}

// The same restart while the first contexts and the publication are still
// kept: the control.
func TestARevisedSegmentWithItsFirstContextsKeepsRunning(t *testing.T) {
	restartRevisedSegment(t, false)
}

func restartRevisedSegment(t *testing.T, expire bool) {
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	installEditedStrategies(t, ctx, f.redisClient, 1725000600, func(first map[string]any) {
		first["name"] = "renamed"
	})
	f.clock.Store((f.base + 59) * 1000)
	for n := 0; n < 2; n++ {
		if err := f.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	revised, err := f.production.dependencies.Catalog.ReadFrozenSchedule(ctx, f.queryGroup, execution.EvaluationTime(f.base+60))
	if err != nil {
		t.Fatal(err)
	}
	segment := revised.Segment
	if segment.Start != f.initialSchedule.Segment.Start || segment.ObjectDigest != f.initialSchedule.Segment.ObjectDigest ||
		len(segment.OutputContextRevisions) != 1 || execution.SameOutputContextRefs(segment.OutputContextRefs, segment.OutputContextRevisions[0].Refs) {
		t.Fatalf("a rename must keep the Segment and revise its output context: before=%+v after=%+v", f.initialSchedule.Segment, segment)
	}
	since := segment.OutputContextRevisions[0].Since
	if current, err := f.repository.LoadActivation(ctx); err != nil || current.Current.SnapshotRevision == segment.Publication.SnapshotRevision {
		t.Fatalf("the rename must activate a publication other than the Segment's: %+v %v", current.Current, err)
	}
	// Every Slot that renders with the first contexts completes while they
	// are kept.
	for f.progress(ctx).NextSlot <= since {
		_ = runOneSlotFull(t, f)
	}
	holds := f.bundle.dependencies.ReadHolds
	if expire {
		catalogPrefix := productionPhaseTwoPrefix(f.cfg.Redis.StatePrefix, "catalog")
		keys := []string{catalogPrefix + ":manifest:" + string(segment.Publication.SnapshotRevision)}
		for _, ref := range segment.OutputContextRefs {
			keys = append(keys, catalogPrefix+":outctx:"+string(ref.Digest))
		}
		if removed, err := f.redisClient.Del(ctx, keys...).Result(); err != nil || removed != int64(len(keys)) {
			t.Fatalf("expired %d of %v: %v", removed, keys, err)
		}
	}
	if err := holds.repository.ConfigureObjectCache(1, 1); err != nil {
		t.Fatal(err)
	}
	restartUntilFull(t, f, f.queryGroup)
	// The hold itself was prepared, not stood in for: its route and delay
	// are read from the Query Group object alone.
	if counts := degradedCounts(f); len(counts) != 0 {
		t.Fatalf("the hold was degraded instead of prepared: %v", counts)
	}
}
