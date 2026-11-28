// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inputslice_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// chainTree is a tree cut by prefix three levels down to the slice of "hos",
// every value of population under it: the root's split, h's, ho's - 121
// leaves, the next split by prefix past the leaves a tree may have.
func chainTree(t *testing.T) *inputslice.Tree {
	t.Helper()
	tree, err := inputslice.NewCut([]string{"host"}, 2, inputslice.FixedCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"^h(?s:.*)$", "^ho(?s:.*)$"} {
		var holding *inputslice.Leaf
		for _, leaf := range tree.Pending() {
			if len(leaf.Slice.Conditions) == 1 && leaf.Slice.Conditions[0].Pattern == prefix {
				holding = &leaf
			}
		}
		if holding == nil {
			t.Fatalf("no slice of %s in %s", prefix, describe(leavesOf(tree)))
		}
		if _, err := tree.SplitKept(holding.ID, nil); err != nil {
			t.Fatal(err)
		}
	}
	if tree.Leaves() != 121 {
		t.Fatalf("the chain has %d leaves, want 121", tree.Leaves())
	}
	return tree
}

// rootSlices is the slices of the root's split, h's aside (split further),
// the tree holds: the ones a retry starts in.
func rootSlices(t *testing.T, leaves []execution.InputSlice) map[string]bool {
	t.Helper()
	fresh, err := inputslice.NewCut([]string{"host"}, 2, inputslice.FixedCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]bool{}
	for _, slice := range leaves {
		held[slice.Digest()] = true
	}
	out := map[string]bool{}
	for _, leaf := range fresh.Pending() {
		if held[leaf.Slice.Digest()] {
			out[leaf.Slice.Digest()] = true
		}
	}
	return out
}

// A slice three levels down the provider timed out, under a tree at 121
// leaves, has no room for its split by prefix: the slices this Slot read
// empty under the splits below the root are folded into their rest, the root's
// slices left as they were, and the split is made, every value in exactly one
// leaf.
func TestATimeoutSplitPastTheLeavesFoldsWhatTheSlotReadEmpty(t *testing.T) {
	var population []series
	for index := 0; index < 200; index++ {
		population = append(population, series{"host": fmt.Sprintf("hos%03d", index)})
	}
	probe := append(append([]series(nil), population...), awkward("host")...)
	tree := chainTree(t)
	tr := &tracker{tree: tree}
	before := rootSlices(t, tr.leaves())
	if len(before) != inputslice.PrefixSplitChildren-1 {
		t.Fatalf("fixture: %d of the root's slices held, want all but h's", len(before))
	}
	reads := map[int]inputslice.LeafRead{}
	var slow inputslice.Leaf
	for _, leaf := range tree.Pending() {
		if len(members(t, leaf.Slice, population)) > 0 {
			slow = leaf
			continue
		}
		tr.land(t, leaf)
		reads[leaf.ID] = inputslice.LeafRead{Millis: 300}
	}
	_, err := tree.SplitKept(slow.ID, nil)
	var concentrated *inputslice.ValueConcentratedError
	if !errors.As(err, &concentrated) || concentrated.Reason != inputslice.ReasonMaxLeaves {
		t.Fatalf("the split at 121 leaves: %v, want it refused for the leaves", err)
	}
	folded, fits := tree.MakeRoom(inputslice.PrefixSplitChildren, reads)
	if !fits || folded == 0 {
		t.Fatalf("folded %d, fits %v; want room made", folded, fits)
	}
	if _, err := tree.SplitKept(slow.ID, nil); err != nil {
		t.Fatalf("the split after the room was made: %v", err)
	}
	if tree.Leaves() > inputslice.MaxLeaves {
		t.Fatalf("%d leaves", tree.Leaves())
	}
	if after := rootSlices(t, tr.leaves()); len(after) != len(before) {
		t.Fatalf("%d of the root's slices held after the room was made, %d before: the room was made of them", len(after), len(before))
	}
	assertPartition(t, tr.leaves(), probe)
	if _, err := inputslice.FromShape([]string{"host"}, tree.Shape()); err != nil {
		t.Fatalf("the shape after the room was made does not rebuild: %v", err)
	}
}

// With every slice under the splits below the root holding series - read
// slower than twice the Slot's fastest, the root's empty slices - there is no
// room to make: nothing is folded and the split does not fit, for the Slot to
// be refused for its leaves by name.
func TestNoRoomIsMadeOfSlicesThatHoldSeries(t *testing.T) {
	tree := chainTree(t)
	reads := map[int]inputslice.LeafRead{}
	var below []inputslice.Leaf
	for _, leaf := range tree.Pending() {
		if err := tree.Land(leaf.ID); err != nil {
			t.Fatal(err)
		}
		millis := uint64(300)
		if conditions := leaf.Slice.Conditions; len(conditions) > 0 && !conditions[0].Negated && len(conditions[0].Pattern) > len("^h(?s:.*)$") {
			millis = 2_000
			below = append(below, leaf)
		}
		reads[leaf.ID] = inputslice.LeafRead{Millis: millis}
	}
	if len(below) < inputslice.PrefixSplitChildren {
		t.Fatalf("fixture: %d slices below the root", len(below))
	}
	folded, fits := tree.MakeRoom(inputslice.PrefixSplitChildren, reads)
	if fits || folded != 0 {
		t.Fatalf("folded %d, fits %v; want nothing folded and no room", folded, fits)
	}
}
