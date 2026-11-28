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
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// switchedFixture is largeSlicedFixture read whole once and timed out by the
// provider: its next Slot starts in a tree cut by prefix
// (first_dimension_fallback), whose every host - node-0 to node-x - is in
// the slice of n, past a slice's limit.
func switchedFixture(t *testing.T) (*slicedFixture, execution.SlotExecutionRequest) {
	t.Helper()
	fixture := largeSlicedFixture(t)
	fixture.source.timeoutWhole = true
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	fixture.source.timeoutWhole = false
	return fixture, request
}

// landedHosts is every host the completed reads from first on delivered, and
// how many times each was.
func landedHosts(fixture *slicedFixture, first int) map[string]int {
	hosts := map[string]int{}
	for _, call := range fixture.source.calls[first:] {
		if call.completed {
			for _, host := range call.delivered {
				hosts[host]++
			}
		}
	}
	return hosts
}

// A Query Group started in a tree cut by prefix whose slice is let go for its
// size - the sample of a slice cut short is of the start of its stream, which
// only the values' ends sample evenly - is split by its values' ends under
// its prefix in that Slot: it completes, every series read once, nothing
// refused, counted switched_to_suffix. Its next Slot starts cut from the end
// from its root, by the cut that slice's sample made (switched_to_suffix),
// and switches nothing (015 C2 section 4.8).
func TestASlicedLetGoForItsSizeInAPrefixTreeSwitchesTheQueryGroup(t *testing.T) {
	fixture, request := switchedFixture(t)
	first := len(fixture.source.calls)
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("the Slot started cut by prefix: result=%+v err=%v, want it completed", result, err)
	}
	if result.Usage.InputSliceCutOn != observability.InputSliceCutOnFirstDimension {
		t.Fatalf("started cut on %q, want %s: a read whole that delivered nothing starts cut by prefix", result.Usage.InputSliceCutOn, observability.InputSliceCutOnFirstDimension)
	}
	if result.Usage.InputSlicesDiscarded == 0 || result.Usage.InputSliceShapeSwitchedToSuffix != 1 {
		t.Fatalf("usage %+v, want a slice let go for its size and the tree switched once", result.Usage)
	}
	hosts := landedHosts(fixture, first)
	if len(hosts) != len(fixture.source.series) {
		t.Fatalf("the switched Slot read %d hosts of %d", len(hosts), len(fixture.source.series))
	}
	for host, times := range hosts {
		if times != 1 {
			t.Fatalf("host %s read %d times, want once", host, times)
		}
	}
	first = len(fixture.source.calls)
	next, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !next.Completed {
		t.Fatalf("the Slot after the switch: result=%+v err=%v, want it completed", next, err)
	}
	if next.Usage.InputSliceCutOn != observability.InputSliceCutOnSwitchedToSuffix || next.Usage.InputSliceShapeSwitchedToSuffix != 0 {
		t.Fatalf("the Slot after the switch: usage %+v, want it cut from the end by the switch's cut, switching nothing", next.Usage)
	}
	for _, call := range fixture.source.calls[first:] {
		for _, condition := range call.slice.Conditions {
			if !condition.Negated && len(condition.Pattern) > 2 && condition.Pattern[1] != '(' {
				t.Fatalf("the Slot after the switch read %s, want slices cut from the end of the values only", condition.Pattern)
			}
		}
	}
	if hosts := landedHosts(fixture, first); len(hosts) != len(fixture.source.series) {
		t.Fatalf("the Slot after the switch read %d hosts of %d", len(hosts), len(fixture.source.series))
	}
}

// A Slot that switched its tree and failed leaves the Query Group's memory as
// it was: its retry starts in the same root slices, cut by prefix, and
// switches again at the same slice - the slices first-seen and the replays
// hold to (inputslice.Tree.RootCut).
func TestASlotThatSwitchedAndFailedIsRetriedInTheSameRootSlices(t *testing.T) {
	fixture, request := switchedFixture(t)
	rootSlices := func(first int) []string {
		var slices []string
		for _, call := range fixture.source.calls[first:] {
			if len(call.slice.Conditions) == 1 && !call.readAhead {
				slices = append(slices, call.slice.Digest())
			}
		}
		return slices
	}
	first := len(fixture.source.calls)
	// A part of the switch's split - its prefix and a class of its end - fails
	// its read, and the Slot with it.
	fixture.source.mu.Lock()
	fixture.source.failSlice = func(slice execution.InputSlice) error {
		if len(slice.Conditions) == 2 {
			return errors.New("injected: a part of the switched slice fails its read")
		}
		return nil
	}
	fixture.source.mu.Unlock()
	if result, err := fixture.coordinator.Execute(context.Background(), request); err == nil && result.Completed {
		t.Fatalf("the failing Slot completed: %+v", result)
	}
	failed := rootSlices(first)
	fixture.source.mu.Lock()
	fixture.source.failSlice = nil
	fixture.source.mu.Unlock()
	first = len(fixture.source.calls)
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed || result.Usage.InputSliceShapeSwitchedToSuffix != 1 {
		t.Fatalf("the retry: result=%+v err=%v, want it completed, switched again", result, err)
	}
	retried := rootSlices(first)
	for index := range failed {
		if index >= len(retried) || failed[index] != retried[index] {
			t.Fatalf("the retry read root slices %v, the failed Slot %v: want the same ones", retried, failed)
		}
	}
}

// prefixChainShape is the shape of a tree cut by prefix three levels down to
// the slice of "ho" every host-NN is under: the root's split, h's, ho's - 121
// leaves, where the next split by prefix passes the leaves a tree may have.
func prefixChainShape(t *testing.T) inputslice.Shape {
	t.Helper()
	tree, err := inputslice.NewCut([]string{"host"}, 2, inputslice.FixedCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"^h(?s:.*)$", "^ho(?s:.*)$"} {
		split := false
		for _, leaf := range tree.Pending() {
			if len(leaf.Slice.Conditions) == 1 && leaf.Slice.Conditions[0].Pattern == prefix {
				if _, err := tree.SplitKept(leaf.ID, nil); err != nil {
					t.Fatal(err)
				}
				split = true
				break
			}
		}
		if !split {
			t.Fatalf("no slice of %s", prefix)
		}
	}
	if tree.Leaves() != 121 {
		t.Fatalf("the chain has %d leaves, want 121", tree.Leaves())
	}
	return tree.Shape()
}

// A slice the provider times out in a tree cut by prefix at 121 leaves, every
// slice under the splits below the root holding a series - read slower than
// twice the Slot's fastest, the root's empty slices - has no room for its
// split however the Slot reads: it fails out of time once, the slice read last
// the Slot after (deferLeaf), and read last and still without room, that Slot
// is refused for its leaves by name, INPUT_SLICE_LEAVES_EXHAUSTED, not failed
// out of time round after round; the next counts the shape voided (015 C2
// section 4.8).
func TestATimeoutSplitWithNoRoomToMakeIsRefusedForItsLeaves(t *testing.T) {
	const alphabet = "-.0123456789:_abcdefghijklmnopqrstuvwxyz"
	var series []slicedSeries
	for index := 0; index < len(alphabet); index++ {
		if c := alphabet[index]; c != 'o' {
			series = append(series, slicedSeries{host: "h" + string(c) + "x", padding: 1 << 10})
		}
		if c := alphabet[index]; c != 's' {
			series = append(series, slicedSeries{host: "ho" + string(c) + "y", padding: 1 << 10})
		}
	}
	for index := 0; index < 30; index++ {
		series = append(series, slicedSeries{host: fmt.Sprintf("hos%02d", index), padding: 1 << 10})
	}
	fixture := newSlicedFixture(t, series, sliceableFacts(), 400)
	fixture.source.millisOf = func(_ execution.InputSlice, delivered int) uint64 { return 300 + 400*uint64(delivered) }
	fixture.source.timeoutBySize, fixture.source.timeoutHolding = true, 13
	request := windowedRequest()
	worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
		[]string{"host"}, prefixChainShape(t), 121, uint64(len(series))<<10, uint64(len(series)))
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if reason, refused := worker.InputSliceRefusal(err); err == nil || refused || reason != "" {
		t.Fatalf("the first Slot at 121 leaves: result=%+v err=%v, want it failed out of time, the slice read last next", result, err)
	}
	result, err = fixture.coordinator.Execute(context.Background(), request)
	if reason, ok := worker.InputSliceRefusal(err); !ok || reason != contract.ReasonInputSliceLeavesExhausted {
		t.Fatalf("the Slot reading the slice last: result=%+v err=%v, want it refused %s", result, err, contract.ReasonInputSliceLeavesExhausted)
	}
	fixture.source.mu.Lock()
	fixture.source.timeoutBySize = false
	fixture.source.mu.Unlock()
	result, err = fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed || result.Usage.InputSliceShapeVoidedMaxLeaves != 1 {
		t.Fatalf("the Slot after the refusal: result=%+v err=%v, want it completed and the refused shape counted voided once", result, err)
	}
}

// Twenty-four hosts host-00 to host-23, every one under "host-", a read of
// thirteen series or more out of time, and no value ever delivered to cut a
// common prefix by: read in a tree cut by prefix, each Slot splits the slice
// out of time a level deeper - in place, then into its shape as it fails -
// past the 121 leaves three levels of 41 come to by folding what it read empty
// (inputslice.Tree.MakeRoom), until "host-" is cut by its digits, ten hosts a
// slice at most, and the Slot completes. Every Slot before it fails out of
// time and none is refused; the one that completes says how many Slots in a
// row split it so (input_slice_timeout_deepening), and the shape stays within
// the leaves a Slot starts in (015 C2 section 4.8).
func TestASliceOutOfTimeThreeLevelsDownIsSplitPastTheLeavesByFolding(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(24, 1<<10), sliceableFacts(), 400)
	fixture.source.timeoutWhole, fixture.source.queryMillis = true, 1_000
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	fixture.source.mu.Lock()
	fixture.source.timeoutWhole = false
	fixture.source.timeoutBySize, fixture.source.timeoutHolding = true, 13
	fixture.source.mu.Unlock()
	slots := 0
	for ; slots < 8; slots++ {
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if reason, refused := worker.InputSliceRefusal(err); refused {
			t.Fatalf("Slot %d refused %s: there is room to make", slots+2, reason)
		}
		if err != nil {
			t.Logf("Slot %d failed: %v", slots+2, err)
			continue
		}
		if !result.Completed || result.QueryAvailability != execution.QueryAvailabilityAvailable {
			t.Fatalf("Slot %d: result=%+v, want it completed, every query answered", slots+2, result)
		}
		// The shape it keeps, not the slices it read: a slice split in place
		// is read and let go, and its children read after it.
		if kept := worker.SliceMemory(fixture.coordinator).LeavesMax; kept > uint64(inputslice.MaxLeaves-3) {
			t.Fatalf("Slot %d keeps a shape of %d leaves, past the %d a Slot starts in (%d slices read)", slots+2, kept,
				inputslice.MaxLeaves-3, result.Usage.InputSlices)
		}
		if result.Usage.InputSliceTimeoutDeepening != uint64(slots+1) {
			t.Fatalf("Slot %d completed after %d failed: deepening %d, want %d", slots+2, slots, result.Usage.InputSliceTimeoutDeepening, slots+1)
		}
		t.Logf("completed in Slot %d after %d failed, %d slices, folded %d", slots+2, slots, result.Usage.InputSlices, result.Usage.InputSliceShapeFolded)
		return
	}
	t.Fatalf("still failing after %d Slots", slots)
}
