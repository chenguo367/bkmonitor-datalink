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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot started in its Query Group's shape and refused
// INPUT_SLICE_LEAVES_EXHAUSTED - the shape as large as a Slot may start in,
// a slice of it let go and split past the leaves a tree may have - drops the
// shape. The refusal is final and the Slot never reaches what a completed
// Slot remembers, so with the shape kept the next Slot would start in the
// same leaves and be refused the same way, every round. The next Slot starts
// in slices counted instead, completes, and counts the shape voided once;
// the one after counts none.
//
// Twenty-four hosts of a third of a slice's limit each, two to a slice at
// most: twelve leaves at least for them. The shape holds them all in the few
// leaves its first split put them in, every other leaf split, empty, until
// it has MaxLeaves-4 or -3: splitting those few to twelve takes the tree past
// its MaxLeaves.
// Slices counted from the Query Group's input, eight, have room for them.
func TestASlotRefusedForTheLeavesOfItsShapeLeavesTheNextOneSlicesCounted(t *testing.T) {
	series := slicedHosts(24, slicedLimit/3)
	fixture := newSlicedFixture(t, series, sliceableFacts(), 400)
	request := slotRequest(execution.OperationNormal)
	dimensions := []string{"host"}
	shape := crowdedShape(t, dimensions, series)
	worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
		dimensions, shape, 4, uint64(len(series))*slicedLimit/3, uint64(len(series)))

	result, err := fixture.coordinator.Execute(context.Background(), request)
	if reason, ok := worker.InputSliceRefusal(err); !ok || reason != contract.ReasonInputSliceLeavesExhausted {
		t.Fatalf("the Slot in the shape of %d leaves: result=%+v err=%v, want it refused %s", shape.Leaves(), result, err,
			contract.ReasonInputSliceLeavesExhausted)
	}
	result, err = fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("the Slot after the refusal: result=%+v err=%v, want it completed in slices counted", result, err)
	}
	if result.Usage.InputSliceShapeStarted != 0 || result.Usage.InputSliceShapeVoidedMaxLeaves != 1 {
		t.Fatalf("the Slot after the refusal: started in a shape %d, voided_max_leaves %d; want no shape and the refused one counted once",
			result.Usage.InputSliceShapeStarted, result.Usage.InputSliceShapeVoidedMaxLeaves)
	}
	result, err = fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed || result.Usage.InputSliceShapeVoidedMaxLeaves != 0 {
		t.Fatalf("the Slot after that: result=%+v err=%v, want it completed and nothing counted voided again", result, err)
	}
}

// crowdedShape is a shape on dimensions of MaxLeaves-4 or -3 leaves (a Slot
// starts in at most inputslice.MaxLeaves-3), series all in the leaves its first split
// put them in: every split after is of a leaf holding none of them, kept for
// its size as a let-go slice's is (inputslice.Tree.SplitSized).
func crowdedShape(t *testing.T, dimensions []string, series []slicedSeries) inputslice.Shape {
	t.Helper()
	tree, err := inputslice.New(dimensions, 4, dimensions[0])
	if err != nil {
		t.Fatal(err)
	}
	holds := func(leaf inputslice.Leaf) bool {
		for _, one := range series {
			matched, err := leaf.Slice.Matches(map[string]string{dimensions[0]: one.host})
			if err != nil {
				t.Fatal(err)
			}
			if matched {
				return true
			}
		}
		return false
	}
	crowded := 0
	for _, leaf := range tree.Pending() {
		if holds(leaf) {
			crowded++
		}
	}
	for tree.Leaves() < inputslice.MaxLeaves-4 {
		split := false
		for _, leaf := range tree.Pending() {
			if holds(leaf) {
				continue
			}
			if _, err := tree.SplitSized(leaf.ID, nil); err == nil {
				split = true
				break
			}
		}
		if !split {
			t.Fatalf("no empty leaf of the %d could be split", tree.Leaves())
		}
	}
	if tree.Leaves() > inputslice.MaxLeaves-3 {
		t.Fatalf("the shape has %d leaves, past the %d a Slot starts in", tree.Leaves(), inputslice.MaxLeaves-3)
	}
	// Twelve leaves for the hosts at least, from the ones holding them now,
	// past the tree's leaves.
	if needed := tree.Leaves() - crowded + len(series)/2; needed <= inputslice.MaxLeaves {
		t.Fatalf("the hosts in %d of %d leaves need %d leaves read in it, within the tree's %d", crowded, tree.Leaves(),
			needed, inputslice.MaxLeaves)
	}
	shape := tree.Shape()
	if shape.IsZero() || shape.Leaves() != tree.Leaves() {
		t.Fatalf("the shape has %d leaves (zero %t), want the tree's %d", shape.Leaves(), shape.IsZero(), tree.Leaves())
	}
	return shape
}
