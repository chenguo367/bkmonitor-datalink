// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// twoConsumers is a Slot whose slices two streams consume side by side, and
// the stream that dispatches them, each holding its own bytes of the Slot,
// wired as the dispatch wires them (sideBySideStreams).
func twoConsumers(co *SlotExecutionCoordinator) (dispatch, first, second *streamedExecution) {
	dispatch = &streamedExecution{slotStream: &slotStream{coordinator: co, began: true, slices: &sliceReading{}}}
	streams := sideBySideStreams(dispatch, 2, nil, nil)
	return dispatch, streams[0], streams[1]
}

// Each of k consumers fills its windows within its own S/(2k): the outside
// its bound counts is its own bytes and 1/k of what is no consumer's - here
// the dispatch's - not the other consumer's, so the two together with the
// dispatch's never pass half the share; the Slot's total, which the share
// judges, is every stream's. With one stream the bound is S/2 less all of it
// (015 C1 4.1).
func TestEachOfKConsumersFillsItsWindowsWithinItsOwnShareOfTheSlot(t *testing.T) {
	co := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxSeries: 10, MaxRetainedBytes: 4000, MaxStateMutations: 10,
		MaxEvents: 10, MaxGapMutations: 10}}
	share := co.qgShareBytes()
	if share != 3000 {
		t.Fatalf("fixture: the share of a 4000-byte pool is %d, want 3000", share)
	}
	dispatch, first, second := twoConsumers(co)
	dispatch.chargeRetained(retainPhaseGap, 40)
	first.chargeRetained(retainPhaseInput, 100)
	second.chargeRetained(retainPhaseInput, 300)
	for name, stream := range map[string]*streamedExecution{"dispatch": dispatch, "first": first, "second": second} {
		if total := stream.retainedTotal(); total != 440 {
			t.Fatalf("the %s stream reads the Slot's total as %d, want every stream's 440", name, total)
		}
	}
	for _, want := range []struct {
		name                          string
		stream                        *streamedExecution
		outside, bound, sliceLimitWas uint64
	}{
		{"first", first, 100 + 40/2, 750 - 120, (750 - 120) / 2},
		{"second", second, 300 + 40/2, 750 - 320, (750 - 320) / 2},
	} {
		if outside, bound, limit := want.stream.windowOutside(), want.stream.windowBound(), want.stream.sliceLimit(); outside != want.outside ||
			bound != want.bound || limit != want.sliceLimitWas {
			t.Fatalf("the %s consumer counts %d outside, bounds its window at %d and its slices at %d; want %d, %d and %d",
				want.name, outside, bound, limit, want.outside, want.bound, want.sliceLimitWas)
		}
	}
	if filled := first.windowOutside() + first.windowBound() + second.windowOutside() + second.windowBound(); filled != share/2 {
		t.Fatalf("both consumers' windows filled to their bounds hold %d with what is outside them, want the Slot side's %d", filled, share/2)
	}

	alone := &streamedExecution{slotStream: &slotStream{coordinator: co, began: true, slices: &sliceReading{}}}
	alone.chargeRetained(retainPhaseGap, 40)
	alone.chargeRetained(retainPhaseInput, 400)
	if outside, bound := alone.windowOutside(), alone.windowBound(); outside != 440 || bound != 1500-440 {
		t.Fatalf("one stream holding 440 counts %d outside and bounds its window at %d, want 440 and %d", outside, bound, 1500-440)
	}
}

// What a merge reserves is measured against the Slot's results as they stand
// when it merges: a Plan another consumer merged first since this one
// measured is that consumer's to keep, and this merge reserves only its
// series of it. Two streams merging two series of one Plan reserve what one
// stream merging both does; reserved on the measure taken before, the
// Plan's first series would be charged twice.
func TestTwoConsumersMergingOnePlanChargeItsFirstSeriesOnce(t *testing.T) {
	budget := ProvisionalBudget{MaxSeries: 10, MaxRetainedBytes: 1 << 30, MaxStateMutations: 10, MaxEvents: 10, MaxGapMutations: 10}
	contract := execution.FrozenExecutionContractRef{QueryRevision: "query-v1"}
	plan := execution.PlanIdentity{StrategyID: "1"}
	series := func(digest string) execution.EvaluationResult { return onePlanSeries(contract, plan, digest) }
	ctx := context.Background()

	alone := &streamedExecution{slotStream: &slotStream{coordinator: &SlotExecutionCoordinator{budget: budget}, began: true}}
	defer alone.releaseProvisional()
	for _, digest := range []string{"series-a", "series-b"} {
		if err := alone.mergeProvisional(ctx, series(digest), 0); err != nil {
			t.Fatal(err)
		}
	}
	want := alone.retainedByPhase[retainPhaseOutput]

	// The pool is this test's own: the Slot's release of its streams is the
	// dispatch's, wired with the consumers (015 C1 step 3.2).
	_, first, second := twoConsumers(&SlotExecutionCoordinator{budget: budget})
	measured := first.measureMerge(series("series-a"), 0, 0)
	if err := second.mergeProvisional(ctx, series("series-b"), 0); err != nil {
		t.Fatal(err)
	}
	if err := first.mergeMeasured(ctx, series("series-a"), measured); err != nil {
		t.Fatal(err)
	}
	if got := first.retainedByPhase[retainPhaseOutput] + second.retainedByPhase[retainPhaseOutput]; got != want {
		t.Fatalf("the two consumers reserved %d for the Plan's output, want the %d one stream merging both series does", got, want)
	}
	// Measured again as it merged: less than the measure taken before the
	// other consumer merged the Plan, which counted the Plan's first series as
	// this merge's.
	if reserved, before := first.retainedByPhase[retainPhaseOutput], measured.effects+measured.summarized; reserved >= before {
		t.Fatalf("the consumer that merged the Plan second reserved %d for its output, want less than the %d it measured "+
			"before the other merged the Plan", reserved, before)
	}
	if _, kept := second.firstMerged[plan]; !kept {
		t.Fatal("the consumer that merged the Plan first does not keep it")
	}
	if _, kept := first.firstMerged[plan]; kept {
		t.Fatal("the consumer that merged the Plan second keeps it too")
	}
}

// With one stream nothing merges between the caller's measure and the merge,
// so the merge reserves what its caller measured - the number the window
// check before it read - and does not size the result again: sized again it
// is the same number, at the cost of a second walk of every result the Slot
// merges. Handed a measure 64 bytes above what sizing the result gives, the
// merge reserves the measure handed; sized again it would reserve 64 less.
func TestOneStreamMergeReservesWhatItsCallerMeasured(t *testing.T) {
	budget := ProvisionalBudget{MaxSeries: 10, MaxRetainedBytes: 1 << 30, MaxStateMutations: 10, MaxEvents: 10, MaxGapMutations: 10}
	contract := execution.FrozenExecutionContractRef{QueryRevision: "query-v1"}
	plan := execution.PlanIdentity{StrategyID: "1"}
	ctx := context.Background()

	alone := &streamedExecution{slotStream: &slotStream{coordinator: &SlotExecutionCoordinator{budget: budget}, began: true}}
	defer alone.releaseProvisional()
	if err := alone.mergeProvisional(ctx, onePlanSeries(contract, plan, "series-a"), 0); err != nil {
		t.Fatal(err)
	}
	next := onePlanSeries(contract, plan, "series-b")
	handed := alone.measureMerge(next, 0, 0)
	handed.effects += 64
	before := alone.retainedByPhase[retainPhaseOutput]
	if err := alone.mergeMeasured(ctx, next, handed); err != nil {
		t.Fatal(err)
	}
	if got, want := alone.retainedByPhase[retainPhaseOutput]-before, handed.effects+handed.summarized; got != want {
		t.Fatalf("one stream's merge reserved %d for its output, want the %d its caller measured (%d is the result sized again)",
			got, want, want-64)
	}
}

// onePlanSeries is one series' result of plan under contract: its level
// outcome and its State mutation.
func onePlanSeries(contract execution.FrozenExecutionContractRef, plan execution.PlanIdentity, digest string) execution.EvaluationResult {
	key := execution.StateKeyIdentity{Plan: plan, SeriesIdentityDigest: execution.SeriesIdentityDigest(digest)}
	return execution.EvaluationResult{Contract: contract, Result: observability.ResultSuccess,
		Plans: []execution.PlanEvaluationResult{{Plan: plan, Disposition: execution.PlanDecided,
			LevelOutcomes: []execution.LevelOutcome{{Plan: plan, SeriesIdentityDigest: key.SeriesIdentityDigest}},
			StateResults:  []execution.StateEvaluation{{Mutation: execution.StateMutation{Identity: key}}}}}}
}
