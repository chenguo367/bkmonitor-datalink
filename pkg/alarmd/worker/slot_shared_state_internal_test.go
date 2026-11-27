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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// What a Slot merges as it finishes - no-data, the Plans without series,
// the tail - is after every slice it read, whichever slice the stream that
// finishes consumed last: a reason of equal rank a later slice said stays
// the Slot's. Ordered by the finishing stream's own slice, the finishing
// batch would stand before that slice's and take its place (015 C1 step 3.1).
func TestWhatASlotMergesAsItFinishesIsAfterEverySlice(t *testing.T) {
	contract := execution.FrozenExecutionContractRef{QueryRevision: "query-v1"}
	plan := execution.PlanIdentity{StrategyID: "1"}
	batch := func(reason string) execution.EvaluationResult {
		return execution.EvaluationResult{Contract: contract, Result: observability.ResultDegraded, ReasonCode: execution.ReasonCode(reason),
			Plans: []execution.PlanEvaluationResult{{Plan: plan, Disposition: execution.PlanUnavailable, ReasonCode: execution.ReasonCode(reason)}}}
	}
	co := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxSeries: 10, MaxRetainedBytes: 1 << 30, MaxStateMutations: 10,
		MaxEvents: 10, MaxGapMutations: 10}}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: co, began: true}}
	defer stream.releaseProvisional()
	// A stream consumed slice 3 and said QUERY_TIMEOUT of the Plan.
	stream.sliceNumber = 3
	if err := stream.mergeProvisional(context.Background(), batch("QUERY_TIMEOUT"), 0); err != nil {
		t.Fatal(err)
	}
	// The stream that finishes consumed slice 0 last.
	stream.sliceNumber, stream.finishing = 0, true
	if err := stream.mergeProvisional(context.Background(), batch("QUERY_UNAVAILABLE"), 0); err != nil {
		t.Fatal(err)
	}
	merged := stream.withWindow()
	if merged.ReasonCode != "QUERY_TIMEOUT" || len(merged.Plans) != 1 || merged.Plans[0].ReasonCode != "QUERY_TIMEOUT" {
		t.Fatalf("the Slot says %s and the Plan %+v, want slice 3's QUERY_TIMEOUT kept over the finishing batch's", merged.ReasonCode, merged.Plans)
	}
}

// Streams of one Slot merge into its results side by side: every Plan each
// merged is the Slot's once, the State results it counted are the Slot's
// effects in hand, each merge took its own turn, and what a stream's window
// holds is that stream's own. A merge counted against the results as another
// stream appended to them would lose a Plan or a count (run with -race).
func TestStreamsOfOneSlotMergeIntoItsResultsSideBySide(t *testing.T) {
	contract := execution.FrozenExecutionContractRef{QueryRevision: "query-v1"}
	co := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxSeries: 1000, MaxRetainedBytes: 1 << 30, MaxStateMutations: 1000,
		MaxEvents: 1000, MaxGapMutations: 1000}}
	slot := &slotStream{coordinator: co, began: true}
	streams := []*streamedExecution{{slotStream: slot}, {slotStream: slot}}
	defer streams[0].releaseProvisional()
	const each = 50
	var wait sync.WaitGroup
	for number, stream := range streams {
		stream.sliceNumber = number
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range each {
				plan := execution.PlanIdentity{StrategyID: fmt.Sprintf("%d-%d", number, index)}
				batch := execution.EvaluationResult{Contract: contract, Result: observability.ResultSuccess,
					Plans: []execution.PlanEvaluationResult{{Plan: plan, Disposition: execution.PlanDecided,
						StateResults: []execution.StateEvaluation{{Mutation: execution.StateMutation{
							Identity: execution.StateKeyIdentity{Plan: plan, SeriesIdentityDigest: "series"}}}}}}}
				if err := stream.mergeProvisional(context.Background(), batch, 0); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wait.Wait()
	if len(slot.evaluated.Plans) != 2*each || slot.summary.planCount() != 2*each || slot.summary.merged != 2*each ||
		slot.effects.states != 2*each {
		t.Fatalf("the Slot holds %d Plans, its summary %d in %d merges, its effects %+v; want %d of each",
			len(slot.evaluated.Plans), slot.summary.planCount(), slot.summary.merged, slot.effects, 2*each)
	}
	for _, plan := range slot.evaluated.Plans {
		if len(plan.StateResults)+len(plan.LevelOutcomes)+len(plan.NoDataEnds) != 0 {
			t.Fatalf("the Slot's results hold %s's window lists, which are the stream's and go with its window", plan.Plan.StrategyID)
		}
	}
	for number, stream := range streams {
		if len(stream.fill.results) != each {
			t.Fatalf("stream %d's window holds %d Plans' results, want its own %d", number, len(stream.fill.results), each)
		}
		for _, held := range stream.fill.results {
			if !strings.HasPrefix(held.Plan.StrategyID, fmt.Sprintf("%d-", number)) {
				t.Fatalf("stream %d's window holds %s, another stream's", number, held.Plan.StrategyID)
			}
		}
	}
}

// What the Slot noted it read written before it (windows.seen) is noted by
// windows cut on any stream and read by a slice beginning, side by side:
// every Plan a window found is in it once they are done, and no read sees
// the set while it is being written (run with -race).
func TestTheSlotsSeenSetTakesNotesFromWindowsSideBySide(t *testing.T) {
	first, second := firstSeenDue(), firstSeenDue()
	second.Identity.StrategyID = "1002"
	stream := &streamedExecution{slotStream: &slotStream{header: execution.InternalExecutionHeader{Contract: frozenRenewalContract(),
		DuePlans: []execution.DuePlan{first, second}}}}
	own := ownVersion(t, first)
	earlier := execution.ApplyVersion{StateApplyEpoch: own.StateApplyEpoch, EvaluationTime: own.EvaluationTime - 60, SlotDigest: "earlier"}
	written := func(due execution.DuePlan) []execution.RuntimeStateView {
		return []execution.RuntimeStateView{{Identity: execution.StateKeyIdentity{Plan: due.Identity, StateGeneration: due.StateGeneration,
			SeriesIdentityDigest: "series"}, Status: execution.StateFoundReady, PersistedApplyVersion: earlier}}
	}
	var wait sync.WaitGroup
	for _, due := range []execution.DuePlan{first, second} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 200 {
				stream.noteSeenViews(written(due))
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for range 200 {
			_ = stream.seenSnapshot()
			_ = stream.unseenPlans()
		}
	}()
	wait.Wait()
	if unseen := stream.unseenPlans(); len(unseen) != 0 {
		t.Fatalf("Plans %v are unseen after windows found both", unseen)
	}
}
