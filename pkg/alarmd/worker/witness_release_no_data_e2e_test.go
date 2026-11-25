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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The no-data family's members (014 next9 ⑥) left unwritten under both
// words let go of their witnesses as their window lands, as the real
// family's do (014 next9, ④ §7 item 4): through Execute on the store as
// production wires it, over redis-server.

// noDataWitnessRig is witnessRig over a Plan detecting no-data, its State
// cache on (the no-data family's members kept compact, as in production),
// with both words or neither.
func noDataWitnessRig(t *testing.T, hosts, padding int, pool, states uint64, skip bool) *shadowRig {
	t.Helper()
	rig := newShadowRigWith(t, hosts, padding, shadowRecoveryOff, 1, &contract.NoDataConfigV1{Continuous: 1, Level: 2}, 16<<20)
	wiring, budget := rig.wiring, rig.budget
	wiring.State = rig.store.ExecutionStore
	budget.MaxSeries = 1 << 20
	if pool > 0 {
		budget.MaxRetainedBytes = pool
	}
	if states > 0 {
		budget.MaxStateMutations = states
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
	rig := noDataWitnessRig(t, 24, 40<<10, 0, 0, true)
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
// in when every member is written, round after round.
func TestANoDataPlansSlotLeavingItsMembersUnwrittenIsReadInTheSlicesOfOneWritingThem(t *testing.T) {
	const hosts, rounds = 2000, 12
	const pool = 2 << 20
	skipping, writing := noDataWitnessRig(t, hosts, 0, pool, 16, true), noDataWitnessRig(t, hosts, 0, pool, 16, false)
	at := frozenContract().Slot.EvaluationTime
	left, compared, sliced := int64(0), 0, uint64(0)
	for round := 0; round < rounds; round++ {
		at += 60
		before := skipping.skippedSoFar()
		skipped := witnessStep(t, "skipping", skipping, at)
		written := witnessStep(t, "writing", writing, at)
		sliced = max(sliced, written.Usage.InputSlices)
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
	if sliced < 2 {
		t.Fatalf("fixture: the Slot was read in at most %d slices, want it sliced", sliced)
	}
}
