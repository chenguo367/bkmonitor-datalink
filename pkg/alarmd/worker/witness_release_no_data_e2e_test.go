// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// orderedReads has a Slot read the same series in the same order every read,
// whatever the scheduler does: its Plan's one physical query a slice (a
// threshold, read on the primary window alone), one slice at a time - two
// permits, a read width of one, no slice read ahead - and every window
// landing before the next slice is read (SetApplySeriallyForTest). A slice is
// let go at the batch that takes it past its limit and split by the sample of
// what it read until then; read over two physical queries at once, as a
// ring ratio's current and previous windows are, their batches interleaved
// as their goroutines ran, a slice was let go a series or two apart from one
// read to the next, split elsewhere, and the slices of the rounds after it
// went their own way.
func orderedReads(t *testing.T, budget *worker.ProvisionalBudget) {
	t.Helper()
	worker.SetApplySeriallyForTest(t)
	budget.QueryPermits = 2
}

// The no-data family's members (014 next9 ⑥) left unwritten under both
// words let go of their witnesses as their window lands, as the real
// family's do (014 next9, ④ §7 item 4): through Execute on the store as
// production wires it, over redis-server.

// noDataWitnessRig is witnessRig over a Plan detecting no-data, its State
// cache on (the no-data family's members kept compact, as in production),
// with both words or neither. ordered is the rig over a threshold Plan
// detecting no-data instead, read one slice at a time (orderedReads).
func noDataWitnessRig(t *testing.T, hosts, padding int, pool, states uint64, skip, ordered bool) *shadowRig {
	t.Helper()
	noData := &contract.NoDataConfigV1{Continuous: 1, Level: 2}
	var rig *shadowRig
	if ordered {
		facts := ringRatioQueryFacts(t)
		compiled, requirements := thresholdCompiledPlanOver(t, "7", string(execution.LogicalQueryRef(facts.QueryRevision)), 1, "100",
			noData, shadowRecoveryOff)
		rig = newShadowRigPlanned(t, hosts, padding, 16<<20, facts, compiled, requirements)
	} else {
		rig = newShadowRigWith(t, hosts, padding, shadowRecoveryOff, 1, noData, 16<<20)
	}
	wiring, budget := rig.wiring, rig.budget
	wiring.State = rig.store.ExecutionStore
	budget.MaxSeries = 1 << 20
	if pool > 0 {
		budget.MaxRetainedBytes = pool
	}
	if states > 0 {
		budget.MaxStateMutations = states
	}
	if ordered {
		orderedReads(t, &budget)
	}
	coordinator, err := worker.NewSlotExecutionCoordinator(wiring, budget)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = coordinator
	if skip {
		rig.skipWord.Store(7)
		rig.skipNoDataWord.Store(7)
	}
	return rig
}

// A Slot read in slices whose members of both families its round leaves
// unwritten - the no-data family's under its own word - holds no witness
// of its windows' keys once they have landed: at the last window's landing,
// the no-data series' among them, the store holds none.
func TestTheWitnessesOfTheNoDataMembersAWindowLeavesUnwrittenGoAsItLands(t *testing.T) {
	rig := noDataWitnessRig(t, 24, 40<<10, 0, 0, true, false)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 10; round++ {
		at += 60
		witnessStep(t, "warming", rig, at)
	}
	if open, _ := rig.storedMembersOf(at, segment.FamilyNoData); open != 1 {
		t.Fatalf("fixture: %d open members on the no-data family's page, want the whole-item group", open)
	}
	before := rig.skippedSoFar()
	noDataSkips := rig.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip)
	var mu sync.Mutex
	var held []int
	worker.SetWindowLandedForTest(t, func(uint64, uint64, uint64) {
		mu.Lock()
		defer mu.Unlock()
		held = append(held, rig.store.ExecutionStore.RuntimeWitnessStats().Items)
	})
	at += 60
	result := witnessStep(t, "measured", rig, at)
	mu.Lock()
	landings := append([]int(nil), held...)
	mu.Unlock()
	if result.Usage.InputSlices < 2 || len(landings) < 2 {
		t.Fatalf("fixture: the Slot was read in %d slices and landed %d windows, want it windowed", result.Usage.InputSlices, len(landings))
	}
	// Members of both families left: the no-data family's round skipped.
	if rig.skippedSoFar() == before ||
		rig.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip) == noDataSkips {
		t.Fatal("fixture: the measured Slot left no member of the no-data family unwritten")
	}
	if last := landings[len(landings)-1]; last != 0 {
		t.Fatalf("at its last window's landing the store held %d witnesses of the Slot (by landing %v), want none: "+
			"the members its windows left unwritten, the no-data family's among them, kept theirs", last, landings)
	}
}

// With a share small enough that what the Slot holds beside its slices
// decides them, a Slot of a Plan detecting no-data whose members of both
// families are left unwritten is read in the slices the same Slot is read
// in when every member is written, round after round: a member left
// unwritten holds nothing beside the slices once its window has landed.
// Both read the same series in the same order (orderedReads), so where a
// slice is let go is what the two hold beside it, not where their queries'
// goroutines stood: read over a ring ratio's two queries as they ran, on one
// core the two Slots were a slice apart every time.
func TestANoDataPlansSlotLeavingItsMembersUnwrittenIsReadInTheSlicesOfOneWritingThem(t *testing.T) {
	const hosts, rounds = 2000, 12
	const pool = 2 << 20
	skipping, writing := noDataWitnessRig(t, hosts, 0, pool, 16, true, true), noDataWitnessRig(t, hosts, 0, pool, 16, false, true)
	at := frozenContract().Slot.EvaluationTime
	left, compared, sliced := int64(0), 0, uint64(0)
	noDataSkips := skipping.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip)
	for round := 0; round < rounds; round++ {
		at += 60
		before := skipping.skippedSoFar()
		written := witnessStep(t, "writing", writing, at)
		sliced = max(sliced, written.Usage.InputSlices)
		// Held beside its slices, the members it left unwritten took the
		// Slot past the leaves a tree may have, or past its share, while the
		// same Slot writing them was read: no fixture failing, the claim.
		skipped, err := skipping.execute(at, nil, nil, false)
		if err != nil || !skipped.Completed {
			t.Fatalf("round %d: the Slot leaving its members unwritten did not complete (%v) where the one writing them was read in %d slices, %d let go",
				round+1, err, written.Usage.InputSlices, written.Usage.InputSlicesDiscarded)
		}
		t.Logf("round %d: skipping %d slices (%d let go), writing %d (%d let go), %d members left unwritten",
			round+1, skipped.Usage.InputSlices, skipped.Usage.InputSlicesDiscarded, written.Usage.InputSlices,
			written.Usage.InputSlicesDiscarded, skipping.skippedSoFar()-before)
		if skipping.skippedSoFar() == before {
			continue
		}
		left += skipping.skippedSoFar() - before
		compared++
		if skipped.Usage.InputSlices != written.Usage.InputSlices || skipped.Usage.InputSlicesDiscarded != written.Usage.InputSlicesDiscarded {
			t.Fatalf("round %d: read in %d slices, %d let go, leaving members unwritten; %d and %d writing every one",
				round+1, skipped.Usage.InputSlices, skipped.Usage.InputSlicesDiscarded,
				written.Usage.InputSlices, written.Usage.InputSlicesDiscarded)
		}
	}
	if compared == 0 || left < hosts/2 {
		t.Fatalf("fixture: %d rounds left %d members unwritten, want rounds leaving most of the %d", compared, left, hosts)
	}
	if skipping.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip) == noDataSkips {
		t.Fatal("fixture: no round left a member of the no-data family unwritten")
	}
	if sliced < 2 {
		t.Fatalf("fixture: the Slot was read in at most %d slices, want it sliced", sliced)
	}
}
