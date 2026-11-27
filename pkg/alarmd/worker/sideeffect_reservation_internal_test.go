package worker

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/racedetector"
)

// This exercises the production aggregation boundary, not synthetic reservation
// counters. Both streams remain alive until the second merge has returned.
func TestProcessSideEffectBudgetCapsTwoLiveSlotAggregates(t *testing.T) {
	for _, kind := range []string{"state", "event", "gap"} {
		t.Run(kind, func(t *testing.T) {
			coordinator := &SlotExecutionCoordinator{budget: sideEffectTestBudget(kind)}
			first := &streamedExecution{slotStream: &slotStream{coordinator: coordinator}}
			second := &streamedExecution{slotStream: &slotStream{coordinator: coordinator}}
			defer first.releaseProvisional()
			defer second.releaseProvisional()
			firstResult := sideEffectTestResult(kind, "qg-first")
			secondResult := sideEffectTestResult(kind, "qg-second")
			if err := first.mergeProvisional(context.Background(), firstResult, 0); err != nil {
				t.Fatalf("first Slot fitting budget rejected: %v", err)
			}
			err := second.mergeProvisional(context.Background(), secondResult, 0)
			if err == nil {
				t.Fatalf("two live Slots each retain one %s under process limit 1: second merge accepted", kind)
			}
			if !reflect.DeepEqual(first.withWindow(), firstResult) {
				t.Fatal("rejection changed another Slot's accepted aggregate")
			}
			if len(second.evaluated.Plans) != 0 {
				t.Fatal("rejected Slot retained effects before reservation succeeded")
			}
		})
	}
}

func TestProvisionalMeasurementDoesNotCopyLargeEventPayload(t *testing.T) {
	result := sideEffectTestResult("event", "qg")
	result.Plans[0].StateResults[0].Events[0].EventID = strings.Repeat("a", 4<<20)
	// Size accounting must not build another event-sized JSON buffer. Small
	// reflection/iteration allocations are unrelated to payload length.
	const runs = 100
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for index := 0; index < runs; index++ {
		_, _ = evaluationRetainedSize(execution.StatePreflightResult{}, result)
	}
	runtime.ReadMemStats(&after)
	if bytesPerOp := (after.TotalAlloc - before.TotalAlloc) / runs; !racedetector.Enabled && bytesPerOp > 64<<10 {
		t.Fatalf("size accounting copied event payload: %d bytes/op", bytesPerOp)
	}
}

func TestSideEffectBudgetRejectionDoesNotMutateAcceptedAggregate(t *testing.T) {
	for _, kind := range []string{"state", "event", "gap"} {
		t.Run(kind, func(t *testing.T) {
			budget := sideEffectTestBudget(kind)
			var target execution.EvaluationResult
			first := sideEffectTestResult(kind, "qg")
			if err := mergeProvisional(&target, first, budget); err != nil {
				t.Fatal(err)
			}
			next := sideEffectTestResult(kind, "qg")
			// A distinct Plan prevents Gap deduplication from hiding growth.
			next.Plans[0].Plan.StrategyID = "second-plan"
			if err := mergeProvisional(&target, next, budget); err == nil {
				t.Fatal("second effect unexpectedly fit the limit")
			}
			if !reflect.DeepEqual(target, first) {
				t.Fatalf("rejected %s merge changed accepted aggregate: plans=%d", kind, len(target.Plans))
			}
		})
	}
}

func TestSharedEffectRejectionIsAtomicAcrossCountsAndBytes(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{budget: sideEffectTestBudget("event")}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator}}
	defer stream.releaseProvisional()
	result := sideEffectTestResult("event", "qg")
	if err := stream.mergeProvisional(context.Background(), result, 0); err != nil {
		t.Fatal(err)
	}
	beforeBytes := coordinator.reservations.retainedBytes
	if err := stream.mergeProvisional(context.Background(), result, 100); err == nil {
		t.Fatal("event overcommit accepted")
	}
	if coordinator.reservations.events != 1 || coordinator.reservations.states != 1 || coordinator.reservations.retainedBytes != beforeBytes {
		t.Fatal("failed multidimensional admission left a partial reservation")
	}
	stream.releaseProvisional()
	if stream.evaluated.Plans != nil || coordinator.reservations.events != 0 || coordinator.reservations.states != 0 || coordinator.reservations.retainedBytes != 0 {
		t.Fatal("released effects still retained")
	}
}

func TestDuplicateGapDoesNotAccumulateReservations(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{budget: sideEffectTestBudget("gap")}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator}}
	defer stream.releaseProvisional()
	result := sideEffectTestResult("gap", "qg")
	if err := stream.mergeProvisional(context.Background(), result, 0); err != nil {
		t.Fatal(err)
	}
	before := stream.retainedTotal()
	for range 100 {
		if err := stream.mergeProvisional(context.Background(), result, 0); err != nil {
			t.Fatal(err)
		}
	}
	if stream.retainedTotal() != before || coordinator.reservations.gaps != 1 {
		t.Fatal("shared duplicate gap was charged repeatedly")
	}
}

func sideEffectTestBudget(kind string) ProvisionalBudget {
	budget := ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20,
		MaxStateMutations: 100, MaxEvents: 100, MaxGapMutations: 100}
	switch kind {
	case "state":
		budget.MaxStateMutations = 1
	case "event":
		budget.MaxEvents = 1
	case "gap":
		budget.MaxGapMutations = 1
	}
	return budget
}

func sideEffectTestResult(kind, qg string) execution.EvaluationResult {
	plan := execution.PlanEvaluationResult{Plan: execution.PlanIdentity{StrategyID: "first-plan"}}
	switch kind {
	case "state":
		plan.StateResults = []execution.StateEvaluation{{}}
	case "event":
		plan.StateResults = []execution.StateEvaluation{{Events: []contract.TriggerEventV1{{}}}}
	case "gap":
		plan.GuardBeforeEvents = []execution.PlanGapMutation{{}}
	}
	return execution.EvaluationResult{
		Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: execution.QueryGroupIdentity(qg)}},
		Plans:    []execution.PlanEvaluationResult{plan},
	}
}

// A batch whose side effects the pool took and whose results the append then
// refuses - two batches clearing one Plan's gap marker differently - is given
// back with the Slot: once the stream releases, the pool holds none of the
// Slot's State writes, events, gap mutations or bytes. The counts were once
// the Slot's only after the append took the batch, and a refused one left
// them in the pool, the process's caps the smaller for it until it restarted.
func TestARefusedAppendGivesItsSideEffectsBackWithTheSlot(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{budget: sideEffectTestBudget("")}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator}}
	ctx := context.Background()
	first := sideEffectTestResult("state", "qg")
	first.Plans[0].GuardAfterState = []execution.PlanGapMutation{gapMergeStatement(t, execution.GapClear, "", 0, execution.GapScope{})}
	if err := stream.mergeProvisional(ctx, first, 64); err != nil {
		t.Fatal(err)
	}
	next := sideEffectTestResult("event", "qg")
	next.Plans[0].GuardAfterState = []execution.PlanGapMutation{gapMergeStatement(t, execution.GapClear, "", 0,
		execution.GapScope{LevelID: 1, HasLevel: true})}
	if err := stream.mergeProvisional(ctx, next, 64); err == nil {
		t.Fatal("fixture: two batches clearing one marker differently were merged")
	}
	if coordinator.reservations.events == 0 {
		t.Fatal("fixture: the refused batch's event was never taken of the pool")
	}
	stream.releaseProvisional()
	left := &coordinator.reservations
	if left.states != 0 || left.events != 0 || left.gaps != 0 || left.retainedBytes != 0 {
		t.Fatalf("after the Slot let go the pool holds %d State writes, %d events, %d gap mutations and %d bytes of it, want none",
			left.states, left.events, left.gaps, left.retainedBytes)
	}
}
