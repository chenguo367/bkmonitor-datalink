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
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A slice the provider times out with no slice read before it in the Slot
// cannot be put on its size, and completes degraded; the next Slot reads it
// after the others, against which it is judged as any slice is
// (sliceReading.pending). A Slot some of whose slices the provider timed out
// completes COMPLETED_WITH_UNAVAILABLE; its QueryAvailability is
// UNAVAILABLE only when no slice answered (execution QueryAvailability).

// cloudZeroFixture is largeSlicedFixture grouped by cloud and host, cloud
// "0" throughout, whose reads the provider times out from holding series
// on: 24 hosts node-<c>, six in each class of the fixed alphabet.
func cloudZeroFixture(t *testing.T, holding int) *slicedFixture {
	t.Helper()
	fixture := largeSlicedFixture(t)
	for index := range fixture.source.series {
		fixture.source.series[index].cloud = "0"
	}
	fixture.source.facts = cloudHostFacts()
	fixture.source.timeoutBySize, fixture.source.timeoutHolding = true, holding
	return fixture
}

// A Query Group never read, cloud "0" throughout, the provider timing out a
// read of 13 series or more - the whole Query Group, 24, and nothing of six.
// Read whole and timed out, it is read next in a tree cut by prefix on its
// first dimension, no read having delivered a value to cut otherwise (015 C2
// section 4.8): cloud's slice of "0" holds all 24 and is split out of time,
// "0" being its one value its rest holding them still, then host from its
// root under it, and node-0 to node-x a character a Slot - every Slot
// failing out of time, none refused - until node- is cut by its last
// character and a Slot completes, every query answered. The Slots in a row
// that split it so are what that one says (input_slice_timeout_deepening).
func TestAFirstDimensionOfOneValueTimedOutWholeIsReadOnceItsHostsAreCut(t *testing.T) {
	fixture := cloudZeroFixture(t, 13)
	fixture.source.queryMillis = 1_000
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	for slot := 2; slot <= 12; slot++ {
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if reason, refused := worker.InputSliceRefusal(err); refused {
			t.Fatalf("Slot %d refused %s, want it failed out of time or completed", slot, reason)
		}
		if err != nil {
			continue
		}
		if !result.Completed || result.QueryAvailability != execution.QueryAvailabilityAvailable {
			t.Fatalf("Slot %d: result=%+v, want it completed, every query answered", slot, result)
		}
		if deepening := result.Usage.InputSliceTimeoutDeepening; deepening == 0 || deepening > uint64(slot-1) {
			t.Fatalf("Slot %d completed, deepening %d: want the Slots in a row that split it, %d at most", slot, deepening, slot-1)
		}
		t.Logf("completed in Slot %d, deepening %d, %d slices", slot, result.Usage.InputSliceTimeoutDeepening, result.Usage.InputSlices)
		// The shape it leaves is within the leaves a Slot starts in, so the
		// next Slot reads in it rather than dropping it and starting over.
		if kept := worker.SliceMemory(fixture.coordinator).LeavesMax; kept > uint64(inputslice.MaxLeaves-3) {
			t.Fatalf("Slot %d keeps a shape of %d leaves, past the %d a Slot starts in", slot, kept, inputslice.MaxLeaves-3)
		}
		result, err = fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed || result.Usage.InputSliceShapeVoidedMaxLeaves != 0 || result.Usage.InputSliceTimeoutDeepening != 0 {
			t.Fatalf("Slot %d after it: result=%+v err=%v, want it completed in the shape left, none voided, nothing split deeper",
				slot+1, result, err)
		}
		return
	}
	t.Fatal("still failing out of time by Slot 12")
}

// A backend that times out every read, whatever it holds: every slice of
// Slot 2 times out with none or one slow one before it - 30 s against a
// median of 30 s is not twice - and completes degraded, as does Slot 3,
// every slice read last for it, in the same order. Three Slots in a row
// degraded are what the scheduler takes a Query Group into its query
// cooldown on (unavailableThreshold); under it, nothing is split either.
// Nothing is split, kept or discarded at any point, and the slices asked
// stay the same: it stops there.
func TestABackendThatTimesOutEveryReadStopsDegradedUnsplit(t *testing.T) {
	fixture := cloudZeroFixture(t, 0)
	fixture.source.queryMillis = 1_000
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	var asked map[string]bool
	for slot := 2; slot <= 4; slot++ {
		request.QueryCooldownHeld = slot == 4
		first := len(fixture.source.calls)
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed || result.CompletionKind != execution.CompletionUnavailable ||
			result.Usage.InputSliceTimeouts != 0 || result.Usage.InputSliceShapeKept != 0 || result.Usage.InputSlicesDiscarded != 0 {
			t.Fatalf("Slot %d: result=%+v err=%v, want it completed degraded, nothing split, kept or discarded", slot, result, err)
		}
		if slot == 3 && result.Usage.InputSlicesDeferred != 41 {
			t.Fatalf("Slot 3 read %d slices last, want all 41 Slot 2 degraded", result.Usage.InputSlicesDeferred)
		}
		slices := map[string]bool{}
		for _, call := range fixture.source.calls[first:] {
			slices[call.slice.Digest()] = true
		}
		if asked != nil && !equalSets(slices, asked) {
			t.Fatalf("Slot %d asked %d slices, Slot 2 %d: want the same", slot, len(slices), len(asked))
		}
		asked = slices
	}
}

// A Query Group read before - its size known - whose slice the provider
// times out after one that took 20 s: 30 s is not twice that, and the slice
// is not put on its size, read last or not; it completes degraded, unsplit,
// the Slot after reading it last and degrading it again.
func TestAKnownQueryGroupsSliceUnderTwiceTheMedianIsNoSliceTooLarge(t *testing.T) {
	fixture := largeSlicedFixture(t)
	fixture.source.queryMillis = 20_000
	request := windowedRequest()
	if result, err := fixture.coordinator.Execute(context.Background(), request); err != nil || !result.Completed {
		t.Fatalf("the Slot before: result=%+v err=%v", result, err)
	}
	fixture.source.mu.Lock()
	fixture.source.answered, fixture.source.timeoutAfter, fixture.source.timeoutTimes = 0, 1, 1
	fixture.source.mu.Unlock()
	for slot, deferred := range []uint64{0, 1} {
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed || result.CompletionKind != execution.CompletionUnavailable ||
			result.Usage.InputSliceTimeouts != 0 || result.Usage.InputSliceShapeKept != 0 || result.Usage.InputSlicesDeferred != deferred {
			t.Fatalf("Slot %d after: result=%+v err=%v, want it completed degraded, unsplit, %d read last", slot+1, result, err, deferred)
		}
	}
}

func equalSets(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if !right[key] {
			return false
		}
	}
	return true
}
