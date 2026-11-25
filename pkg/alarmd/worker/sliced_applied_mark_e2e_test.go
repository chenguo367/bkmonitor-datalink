// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot read in slices of a Query Group of two Plans, that fails at its
// third slice after the windows of the first two landed, applied neither
// Plan whole: every series of both was in the slice it never read. Through
// Execute on production's wiring - access, the real evaluator, the State
// store over redis-server as the State port, and the slot-applied mark
// store as the execution evidence port (cmd/alarmd
// runtime_phase_two_bundle.go) - the attempt leaves no mark that says both
// were applied, and the query-free finalization past the replay window
// records the Slot as the gap it is.
func TestASlicedSlotFailingAfterSomeSlicesLandedLeavesItsPlansUnapplied(t *testing.T) {
	// 400 hosts of 8 KiB, read in slices of about ten series, and windows of 4 State writes, fewer
	// than a slice holds of one Plan: a read evaluates its Plans one after another (prepared Plan by
	// Plan), so a window is cut while the second slice evaluates the first Plan again, after the
	// evaluation passed the second.
	rig := appliedMarkRig(t, 400, 8<<10, 4)
	plans := secondShadowPlan(t, rig)
	marks := wireShadowSlotAppliedMarks(t, rig)
	at := frozenContract().Slot.EvaluationTime
	var slices uint64
	for round := 0; round < 3; round++ {
		at += 60
		result, err := executeShadowPlans(rig, at, nil)
		if err != nil || !result.Completed {
			t.Fatalf("warming Slot at %d: result=%+v err=%v", at, result, err)
		}
		slices = result.Usage.InputSlices
	}
	if slices < 3 {
		t.Fatalf("fixture: the Query Group is read in %d slices, want at least three", slices)
	}

	// The measured Slot: its third slice the provider refuses.
	var mu sync.Mutex
	landed, stateGiven := 0, uint64(0)
	worker.SetWindowLandedForTest(t, func(state, _, _ uint64) {
		mu.Lock()
		defer mu.Unlock()
		landed++
		stateGiven += state
	})
	rig.provider.mu.Lock()
	rig.provider.failSlicesAfter, rig.provider.seen, rig.provider.failing = 2, nil, nil
	rig.provider.mu.Unlock()
	at += 60
	var slot execution.SlotIdentity
	result, err := executeShadowPlans(rig, at, func(request *execution.SlotExecutionRequest) {
		liveShadowRequest(request)
		slot = request.Contract.Slot
	})
	if err == nil && result.Completed {
		t.Fatal("fixture: the Slot completed though its third slice's reads all failed")
	}
	if !errors.Is(err, errShadowProviderDown) {
		t.Logf("the Slot failed with %v", err)
	}
	mu.Lock()
	windows, written := landed, stateGiven
	mu.Unlock()
	if windows < 2 || written == 0 {
		t.Fatalf("fixture: the failing attempt landed %d windows giving back %d bytes of State, want the windows "+
			"of its first two slices written", windows, written)
	}

	evidence, err := marks.Read(context.Background(), slot, plans)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d slices a round; the failing attempt landed %d windows, gave back %d bytes of State and left a mark of %d of %d Plans",
		slices, windows, written, evidence.PlansApplied, evidence.PlansTotal)
	if evidence.Kind != execution.EvidenceNoneFound || evidence.PlansApplied != 0 {
		t.Fatalf("the attempt left a mark of %d of %d Plans applied (%s), want none: neither was applied whole, "+
			"both had series in the slices it never read", evidence.PlansApplied, evidence.PlansTotal, evidence.Kind)
	}

	// The retry, past the replay window, finishing without a query.
	rig.provider.mu.Lock()
	rig.provider.failSlicesAfter = 0
	rig.provider.mu.Unlock()
	rig.ports.finalizationMode = execution.FinalizationGapSkipped
	result, err = executeShadowPlans(rig, at, func(request *execution.SlotExecutionRequest) {
		liveShadowRequest(request)
		request.ReplayExpired, request.AttemptNo = true, 2
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletionKind != execution.CompletionGapSkipped {
		t.Fatalf("completion = %q, want GAP_SKIPPED: the Slot did miss its window", result.CompletionKind)
	}
	completion := rig.ports.lastProgress.Completion
	if completion.Evidence != nil && completion.Evidence.FullyApplied() {
		t.Fatalf("the query-free completion carried evidence %+v, every Plan applied: the Progress fold opens "+
			"no gap for it, and the series of the slice never read go unrecorded", *completion.Evidence)
	}
}

// appliedMarkRig is the shadow rig with the State cache on, as 0.7 runs it
// by default, which gives the store as many keys a call as the rig has hosts,
// the State port the store itself, the series bounded only by the pool, and
// windows of states State writes.
func appliedMarkRig(t *testing.T, hosts, padding int, states uint64) *shadowRig {
	t.Helper()
	rig := newShadowRigWith(t, hosts, padding, shadowRecoveryOff, 0, nil, 64<<20)
	rig.wiring.State = rig.store.ExecutionStore
	rig.budget.MaxSeries, rig.budget.MaxStateMutations = 1<<20, states
	coordinator, err := worker.NewSlotExecutionCoordinator(rig.wiring, rig.budget)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = coordinator
	return rig
}

// secondShadowPlan gives the rig's Query Group a second Plan beside its
// first: the same ring ratio compiled for a strategy of its own, reading the
// same query, so every series is evaluated once for each. It returns both.
func secondShadowPlan(t *testing.T, rig *shadowRig) []execution.PlanIdentity {
	t.Helper()
	facts := ringRatioQueryFacts(t)
	compiled, _ := ringRatioCompiledPlanFor(t, "8", string(execution.LogicalQueryRef(facts.QueryRevision)), shadowRecoveryOff, 0, nil)
	rig.frozen.mu.Lock()
	defer rig.frozen.mu.Unlock()
	second := rig.frozen.plan.DuePlans[0]
	second.Identity.StrategyID, second.CompiledPlan = "8", compiled
	rig.frozen.plan.DuePlans = append(rig.frozen.plan.DuePlans, second)
	return []execution.PlanIdentity{rig.frozen.plan.DuePlans[0].Identity, second.Identity}
}

// wireShadowSlotAppliedMarks wires the slot-applied mark store as
// production does, over the rig's Redis, as the execution evidence port, and
// remakes the coordinator with it.
func wireShadowSlotAppliedMarks(t *testing.T, rig *shadowRig) *state.SlotAppliedMarkStore {
	t.Helper()
	_, backend := openG3AStateStore(t, rig.address, "alarmd-shadow-applied")
	t.Cleanup(func() { _ = backend.Close() })
	router, err := state.NewFixedRouter("shadow-applied", backend)
	if err != nil {
		t.Fatal(err)
	}
	marks, err := state.NewSlotAppliedMarkStore("alarmd-shadow", router)
	if err != nil {
		t.Fatal(err)
	}
	rig.wiring.ExecutionEvidence = marks
	coordinator, err := worker.NewSlotExecutionCoordinator(rig.wiring, rig.budget)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = coordinator
	return marks
}

// liveShadowRequest places the Slot's recovery and keep windows around now,
// as liveSlotRequest does: the mark lives to keep-until, and one in the past
// is never written.
func liveShadowRequest(request *execution.SlotExecutionRequest) {
	now := time.Now()
	request.RecoveryUntilUnixMilli = now.Add(10 * time.Minute).UnixMilli()
	request.KeepUntilUnixMilli = now.Add(20 * time.Minute).UnixMilli()
}

// executeShadowPlans is the rig's execute for every due Plan of its Query
// Group, each one a consumer of every requirement: the Slot at evaluation,
// edit applied to its request last.
func executeShadowPlans(rig *shadowRig, evaluation execution.EvaluationTime,
	edit func(*execution.SlotExecutionRequest)) (execution.SlotExecutionResult, error) {
	rig.t.Helper()
	rig.provider.round(int64(evaluation), nil, nil, false)
	rig.readMu.Lock()
	rig.readAt = time.Unix(int64(evaluation)+120, 0)
	rig.readMu.Unlock()
	rig.frozen.mu.Lock()
	due := append([]execution.DuePlan(nil), rig.frozen.plan.DuePlans...)
	requirements := append([]execution.DataRequirement(nil), rig.frozen.plan.Requirements...)
	rig.frozen.mu.Unlock()
	keys := make([]execution.PlanKey, 0, len(due))
	consumers := make([]execution.DataRequirementConsumer, 0, len(due))
	for index := range due {
		due[index].CompletionDeadlineUnixMilli = int64(evaluation)*1000 + 600_000
		consumers = append(consumers, execution.DataRequirementConsumer{
			Consumer:                  execution.ConsumerRef{Plan: due[index].Identity, LevelID: 5, HasLevel: true},
			ConsumerDeadlineUnixMilli: due[index].CompletionDeadlineUnixMilli, DownstreamExecutionReserveMilliSec: 5_000})
		keys = append(keys, due[index].Key())
	}
	for index := range requirements {
		requirements[index].Consumers = append([]execution.DataRequirementConsumer(nil), consumers...)
	}
	digest, err := execution.DeriveDuePlanSetDigest(due, requirements)
	if err != nil {
		rig.t.Fatal(err)
	}
	rig.frozen.mu.Lock()
	rig.frozen.plan.DuePlans, rig.frozen.plan.Requirements = due, requirements
	rig.frozen.mu.Unlock()
	request := rig.base
	request.Contract.Slot.EvaluationTime, request.Contract.DuePlanSetDigest = evaluation, digest
	request.ExpectedNextSlot = evaluation
	request.DuePlanTargets = execution.FrozenDuePlanTargets{DuePlanSetDigest: digest, Plans: keys}
	if edit != nil {
		edit(&request)
	}
	return rig.coordinator.Execute(context.Background(), request)
}
