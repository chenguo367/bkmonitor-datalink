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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The witnesses of the keys a window reads and leaves unwritten (014 next9,
// ④ §7 item 4): let go of in the store as the window lands, not kept to the
// attempt's end beside the share the Slot's slices are cut by. Through
// Execute on the State store as production wires it - the store itself, no
// wrapper, as the State port (cmd/alarmd runtime_phase_two_bundle.go) - over
// redis-server, with the segment skip leaving the steady members unwritten.

// witnessRig is the shadow rig with the State port the store itself, and
// the pool and per-window State mutations given (zero, the rig's: 400): its
// preflight batches no larger than a window, the store's 16 keys a call at
// most, and the series bounded only by the pool.
func witnessRig(t *testing.T, hosts, padding int, pool, states uint64) *shadowRig {
	t.Helper()
	rig := newShadowRig(t, hosts, padding, shadowRecoveryOff)
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
	return rig
}

// witnessStep runs the rig's Slot at at, which completes.
func witnessStep(t *testing.T, name string, rig *shadowRig, at execution.EvaluationTime) execution.SlotExecutionResult {
	t.Helper()
	result, err := rig.execute(at, nil, nil, false)
	if err != nil || !result.Completed {
		t.Fatalf("%s Slot at %d: result=%+v err=%v", name, at, result, err)
	}
	return result
}

// A Slot read in slices and written window by window, whose steady members
// the segment skip leaves unwritten, holds no witness of a window's keys
// once the window has landed: at its last window's landing the store holds
// none of the Slot's, and the attempt's end has none to drop. The Slot
// completes, every write it makes taking its own.
func TestTheWitnessesOfTheKeysAWindowLeavesUnwrittenGoAsItLands(t *testing.T) {
	rig := witnessRig(t, 24, 40<<10, 0, 0)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 10; round++ {
		at += 60
		witnessStep(t, "warming", rig, at)
	}
	before := rig.skippedSoFar()
	if before == 0 {
		t.Fatal("fixture: the segment skip left no member unwritten in ten rounds")
	}
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
	if rig.skippedSoFar() == before {
		t.Fatal("fixture: the measured Slot left no member unwritten")
	}
	if last := landings[len(landings)-1]; last != 0 {
		t.Fatalf("at its last window's landing the store held %d witnesses of the Slot (by landing %v), want none: "+
			"the keys its windows left unwritten kept theirs", last, landings)
	}
	if left := rig.store.ExecutionStore.RuntimeWitnessStats().Items; left != 0 {
		t.Fatalf("the store holds %d witnesses after the Slot", left)
	}
}

// The writes of a Slot written window by window take the witnesses its
// preflights left: a window lets go of its keys' only once it has landed, so
// none of its writes reads its key again to compare (the sequential path a
// write without a witness goes).
func TestTheWritesOfAWindowedSlotTakeTheWitnessesItsPreflightsLeft(t *testing.T) {
	rig := witnessRig(t, 24, 40<<10, 0, 0)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 3; round++ {
		at += 60
		witnessStep(t, "warming", rig, at)
	}
	reread := rig.store.ExecutionStore.StateReadStats().Keys[state.StateReadSequential]
	rig.observed.mu.Lock()
	written := rig.observed.written
	rig.observed.mu.Unlock()
	at += 60
	result := witnessStep(t, "measured", rig, at)
	rig.observed.mu.Lock()
	wrote := rig.observed.written - written
	rig.observed.mu.Unlock()
	if result.Usage.InputSlices < 2 || wrote == 0 {
		t.Fatalf("fixture: the Slot was read in %d slices and wrote %d keys, want it windowed and writing", result.Usage.InputSlices, wrote)
	}
	if again := rig.store.ExecutionStore.StateReadStats().Keys[state.StateReadSequential] - reread; again != 0 {
		t.Fatalf("%d of the %d keys the Slot wrote were read again by their writes: their witnesses were gone before their windows wrote",
			again, wrote)
	}
}

// A retry whose series its attempt before wrote finds them already applied
// and writes none of them (mergeResumedSeries): their keys, read beside the
// windows' views, let go of their witnesses as the windows they are merged
// into land, as an unwritten member's do.
func TestTheWitnessesOfTheSeriesARetryFindsAppliedGoAsTheirWindowsLand(t *testing.T) {
	rig := witnessRig(t, 24, 40<<10, 0, 0)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 4; round++ {
		at += 60
		witnessStep(t, "warming", rig, at)
	}
	at += 60
	rig.ports.failStage = "progress_commit"
	if result, _ := rig.execute(at, nil, nil, false); result.Completed {
		t.Fatal("fixture: the Slot completed with its Progress failing")
	}
	rig.ports.failStage = ""
	var mu sync.Mutex
	var held []int
	worker.SetWindowLandedForTest(t, func(uint64, uint64, uint64) {
		mu.Lock()
		defer mu.Unlock()
		held = append(held, rig.store.ExecutionStore.RuntimeWitnessStats().Items)
	})
	result := witnessStep(t, "retry", rig, at)
	mu.Lock()
	landings := append([]int(nil), held...)
	mu.Unlock()
	if result.Usage.InputSlices < 2 || len(landings) < 2 {
		t.Fatalf("fixture: the retry was read in %d slices and landed %d windows, want it windowed", result.Usage.InputSlices, len(landings))
	}
	if last := landings[len(landings)-1]; last != 0 {
		t.Fatalf("at the retry's last window's landing the store held %d witnesses (by landing %v), want none: "+
			"the series it found applied kept theirs", last, landings)
	}
}

// With a share small enough that what the Slot holds beside its slices
// decides them, a Slot whose steady members the segment skip leaves
// unwritten is read in the slices the same Slot is read in when every
// member is written, round after round: the witnesses its windows leave go
// as they land, so its slice limit does not fall as it reads.
func TestASlotLeavingItsMembersUnwrittenIsReadInTheSlicesOfOneWritingThem(t *testing.T) {
	const hosts, rounds = 2000, 12
	const pool = 2 << 20
	skipping, writing := witnessRig(t, hosts, 0, pool, 16), witnessRig(t, hosts, 0, pool, 16)
	skipping.skipWord.Store(7)
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
