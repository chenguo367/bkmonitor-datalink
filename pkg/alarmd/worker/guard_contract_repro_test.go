// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker_test

import (
	"context"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// A round that brings a new incomplete input to an already guarded Level
// degrades legally, and the reason it degrades for is this round's.
//
// It used to fail the whole Plan, every Slot, for as long as the input stayed
// incomplete: the outcome took its reason off the stored marker and the guard
// this round proposed took the fold of this round's inputs, so the result
// contract's two comparisons could not both be satisfied -- one wanted the
// stored reason, the other wanted the final marker's, and those had become
// different values. Two objects on the deployment did this on every Slot.
//
// The fixture is the production shape: a Level scope already GAPPED for
// GAP_SKIPPED, and this round's algorithm dependency comes back PARTIAL.
func TestGuardContractRealWorkerRepro(t *testing.T) {
	header, batches := workerG4StreamFixture(t, strategy.DetectorKindSimpleRingRatio)
	ports, eval, co := workerG4Coordinator(t)
	ports.gapMissing = false
	ports.gapScopes = []execution.GapScopeState{{
		Scope: execution.GapScope{HasLevel: true, LevelID: 5}, Status: execution.GapStatusGapped,
		ReasonCode: execution.ReasonCode(contract.ReasonGapSkipped), RequiredFullSlots: 3,
	}}
	completion := execution.QueryExecutionCompletion{AllRequiredCompleted: true}
	for _, b := range batches {
		completion.PhysicalQueries = append(completion.PhysicalQueries, execution.PhysicalQueryCompletion{
			Ref: b.CompletionRef, PhysicalQuery: b.PhysicalQuery, QueryRevision: b.QueryRevision,
			Completeness: execution.CompletenessFull, DataState: execution.DataStateData, Delivery: b.Delivery,
		})
	}
	for i := range completion.PhysicalQueries {
		if batches[i].Inputs[0].Role == execution.InputRoleAlgorithmDependency {
			completion.PhysicalQueries[i].Completeness = execution.CompletenessPartial
			completion.PhysicalQueries[i].PartialEvidence = &execution.PartialEvidence{
				Kind: execution.PartialEvidenceOmissionStable, Version: 1,
				EvidenceDigest: strings.Repeat("d", 64), OmissionOnly: true, ReturnedRecordsStable: true,
			}
		}
	}
	completion.CompletionBindings = accessShapedCompletionBindings(t, header, completion.PhysicalQueries)
	ports.executeOverride = streamExecution(header, batches, completion)

	result, err := co.Execute(context.Background(), workerSlotRequest(header.Contract))
	if err != nil {
		t.Fatalf("result=%+v error=%v evaluations=%+v", result, err, eval.results)
	}

	// Completing is not enough: a build that dropped either comparison would
	// also complete. What this pins is that the two agree, and on which value.
	if len(eval.results) != 1 || len(eval.results[0].Plans) != 1 {
		t.Fatalf("evaluations = %+v, want one Plan result", eval.results)
	}
	plan := eval.results[0].Plans[0]
	if len(plan.LevelOutcomes) != 1 {
		t.Fatalf("outcomes = %+v, want one", plan.LevelOutcomes)
	}
	outcome := plan.LevelOutcomes[0]
	if outcome.Outcome != execution.LevelOutcomeUnknown {
		t.Fatalf("outcome = %+v, want UNKNOWN under the guard", outcome)
	}
	// This round's reason, not the stored marker's. The marker says why the
	// guard has been up; the outcome says why this round could not decide, and
	// this round could not decide because an input came back PARTIAL.
	want := execution.ReasonCode(contract.ReasonQueryPartial)
	if outcome.ReasonCode != want {
		t.Fatalf("outcome reason = %q, want %q: the outcome is this round's statement, and taking the "+
			"stored marker's reason is what made it disagree with the guard", outcome.ReasonCode, want)
	}
	if outcome.ReasonCode == execution.ReasonCode(contract.ReasonGapSkipped) {
		t.Fatal("the outcome carried the stored marker's reason, which is the defect this pins")
	}

	if len(plan.GuardBeforeEvents) != 1 || len(plan.GuardBeforeEvents[0].Scopes) != 1 {
		t.Fatalf("guard = %+v, want one scope proposed", plan.GuardBeforeEvents)
	}
	scope := plan.GuardBeforeEvents[0].Scopes[0]
	if scope.Scope != (execution.GapScope{HasLevel: true, LevelID: 5}) {
		t.Fatalf("guard scope = %+v, want the Level the input belongs to", scope.Scope)
	}
	// The whole point: one value, not two derivations that happen to meet.
	if scope.ReasonCode != outcome.ReasonCode {
		t.Fatalf("guard reason %q and outcome reason %q disagree; they are the same fold of the same "+
			"inputs and the result contract compares them", scope.ReasonCode, outcome.ReasonCode)
	}
	// And the State side writes nothing for it. A Level with an incomplete
	// input of its own this round is frozen (InputAllowsStateAdvance), so
	// buildMutation never writes a new guard reason for it; its stored
	// reason stays as loaded and the marker above is the guard that covers
	// the outcome. That is why the State guard's reason cannot be the one
	// that disagrees with the fold: a Level under a fold does not advance.
	if len(plan.StateResults) != 0 {
		t.Fatalf("state results = %+v, want none: a Level under this round's fold is frozen and writes no guard reason of its own", plan.StateResults)
	}
}
