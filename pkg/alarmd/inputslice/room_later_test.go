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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// A Slot of a tree cut by prefix on host, of host and pod: the root's slices,
// h's split under them, a slice of h read whole and kept split for its size
// on pod (KeepSizedSplit) - the shape at 121 leaves - and then x's slice the
// provider timed out split in place, 121 leaves in the tree and 161 in the
// shape, past the 125 a Slot starts in. The shape is brought within them
// (CompactForNext) with the slices this Slot read empty, every one read in
// 300 ms but what x's split holds, and neither the slice kept split nor a
// rest it is is folded: its parts were cut from it as it was.
func TestASliceKeptSplitForItsSizeIsNotFoldedAway(t *testing.T) {
	dimensions := []string{"host", "pod"}
	for _, cell := range []struct {
		name string
		// kept is the slice of h kept split: two series of it, of pods p-a
		// and p-z, that no character of the alphabet after it tells apart.
		kept, a, b string
		// xMillis is what x's parts read in: 300 ms is empty.
		xMillis uint64
		// fits is whether the shape comes within the leaves.
		fits bool
	}{
		// x's parts folded, the deepest split first: the splits kept for
		// size counted, the shape compacted to 125.
		{"h's rest kept, x's parts empty", `^h(?s:.*)$`, "hA1", "hB2", 300, true},
		// x's parts holding series, h's split is next: its rest is the slice
		// kept, and no part of h is folded into it.
		{"h's rest kept, x's parts slow", `^h(?s:.*)$`, "hA1", "hB2", 2_000, false},
		// h- kept: h's other empty parts folded into h's rest, h- not.
		{"h- kept, x's parts slow", `^h-(?s:.*)$`, "h-A1", "h-B2", 2_000, true},
	} {
		population := []series{{"host": cell.a, "pod": "p-a"}, {"host": cell.b, "pod": "p-z"}}
		probe := append(append([]series(nil), population...), awkward("host")...)
		tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("host"))
		if err != nil {
			t.Fatal(err)
		}
		find := func(pattern string, rest bool) inputslice.Leaf {
			t.Helper()
			for _, leaf := range tree.Pending() {
				conditions := leaf.Slice.Conditions
				if !rest && len(conditions) == 1 && conditions[0].Pattern == pattern {
					return leaf
				}
				if rest && len(conditions) == 2 && conditions[0].Pattern == pattern && conditions[1].Negated {
					return leaf
				}
			}
			t.Fatalf("%s: no slice of %s (rest %v) in %s", cell.name, pattern, rest, describe(leavesOf(tree)))
			return inputslice.Leaf{}
		}
		if _, err := tree.SplitKept(find(`^h(?s:.*)$`, false).ID, nil); err != nil {
			t.Fatal(err)
		}
		x := find(`^x(?s:.*)$`, false)
		var kept inputslice.Leaf
		if cell.kept == `^h(?s:.*)$` {
			kept = find(cell.kept, true)
		} else {
			kept = find(cell.kept, false)
		}
		reads := map[int]inputslice.LeafRead{}
		for _, leaf := range tree.Pending() {
			if leaf.ID == x.ID {
				continue
			}
			if err := tree.Land(leaf.ID); err != nil {
				t.Fatal(err)
			}
			reads[leaf.ID] = inputslice.LeafRead{Millis: 300}
		}
		if _, _, err := tree.KeepSizedSplit(kept.ID, nil, boundsOf(dimensions, members(t, kept.Slice, population))); err != nil {
			t.Fatalf("%s: kept split for its size: %v", cell.name, err)
		}
		parts, err := tree.SplitKept(x.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range parts {
			if err := tree.Land(part.ID); err != nil {
				t.Fatal(err)
			}
			reads[part.ID] = inputslice.LeafRead{Millis: cell.xMillis}
		}
		folded, fits := tree.CompactForNext(reads)
		built, err := inputslice.FromShape(dimensions, tree.Shape())
		if !cell.fits {
			var refused *inputslice.ShapeRefusedError
			if fits || folded != 0 || !errors.As(err, &refused) || refused.Reason != inputslice.ShapeRefusedMaxLeaves {
				t.Fatalf("%s: folded %d, fits %v, the shape: %v; want nothing folded and the shape a partition past the leaves",
					cell.name, folded, fits, err)
			}
			continue
		}
		if !fits || err != nil {
			t.Fatalf("%s: folded %d, fits %v, the shape: %v; want it compacted and rebuilt", cell.name, folded, fits, err)
		}
		if built.Leaves() > inputslice.MaxLeaves-3 {
			t.Fatalf("%s: the shape compacted has %d leaves, past the %d a Slot starts in", cell.name, built.Leaves(), inputslice.MaxLeaves-3)
		}
		assertPartition(t, leavesOf(built), probe)
	}
}
