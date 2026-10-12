package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func retainedSlot(group string, evaluation, following int64, retained uint64) observability.Observation {
	return observability.Observation{Stage: observability.StageSlotCompleted, Result: observability.ResultSuccess,
		Duration: time.Millisecond, DurationKnown: true, FollowingSlot: following,
		Trace:           observability.TraceFields{QueryGroupKey: group, EvaluationTime: evaluation},
		SlotBudgetUsage: &observability.SlotBudgetUsageFacts{RetainedBytes: retained}}
}

// The observation memory line reserves for the retained pool what the owned
// Query Groups were measured to retain and a quarter more, within the pool's
// ceiling and never under what it holds; the ceiling until every owned group
// has a reading, and again once the reading is stale.
func TestTheRetainedReservationIsTheOwnedGroupsMeasuredPeak(t *testing.T) {
	const ceiling = 1000
	now := time.Unix(3600, 0)
	clock := func() time.Time { return now }
	census := observability.NewRetainedPeakCensus(5*time.Minute, clock)
	census.SetReplayAge(10 * time.Minute)
	source := newWorkerCostSource(census, nil, clock)
	owned := []execution.QueryGroupIdentity{"qg-hourly", "qg-minute"}
	source.boundByOwned(func() []execution.QueryGroupIdentity { return owned })
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("before any reading: %d, want the ceiling", got)
	}
	census.Observe(context.Background(), retainedSlot("qg-hourly", 3600, 7200, 100))
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("one owned group unmeasured: %d, want the ceiling", got)
	}
	census.Observe(context.Background(), retainedSlot("qg-minute", 3600, 3660, 60))
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != 200 {
		t.Fatalf("peaks 100 and 60: %d, want 160 and a quarter, 200", got)
	}
	if got := source.RetainedReservation(ceiling, 250); got != 250 {
		t.Fatalf("holding 250: %d, want what it holds", got)
	}
	// Twenty minutes after the hourly Slot its peak still counts: the census
	// holds it until its next Slot. The minute group's 60 went with its own
	// two periods and the replay age; its latest Slot retained 40.
	now = now.Add(20 * time.Minute)
	census.Observe(context.Background(), retainedSlot("qg-minute", 4800, 4860, 40))
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != 175 {
		t.Fatalf("twenty minutes after the hourly Slot: %d, want (100 + 40) and a quarter, 175", got)
	}
	// The same reading, three heartbeats and a second later, is stale.
	now = now.Add(rosterPeakStale + time.Second)
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("a reading %v old: %d, want the ceiling", rosterPeakStale+time.Second, got)
	}
	now = now.Add(-rosterPeakStale - time.Second)
	// Near the ceiling the reservation is the ceiling.
	census.Observe(context.Background(), retainedSlot("qg-minute", 4860, 4920, 900))
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("peaks 100 and 900: %d, want the ceiling", got)
	}
	// A group taken over and not yet run here is unmeasured.
	owned = append(owned, "qg-new")
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("a new owned group: %d, want the ceiling", got)
	}
	// Nothing owned is no measurement either.
	owned = nil
	source.Costs()
	if got := source.RetainedReservation(ceiling, 0); got != ceiling {
		t.Fatalf("nothing owned: %d, want the ceiling", got)
	}
}
