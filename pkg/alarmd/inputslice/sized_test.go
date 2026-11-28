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
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

func hosts(n int) inputslice.Sample {
	values := make([]inputslice.SampledValue, 0, n)
	for index := range n {
		values = append(values, inputslice.SampledValue{Value: fmt.Sprintf("host-%03d", index), Series: 1})
	}
	return inputslice.Sample{"host": values}
}

// A slice the tree started in, split for its size as SplitSized splits it, is
// in the Shape - Split's is not - and the tree rebuilt from that Shape starts
// in its parts: the next Slot does not read the slice whole to let it go
// again.
func TestASizedSplitOfAStartingSliceIsTheNextStart(t *testing.T) {
	dimensions := []string{"host"}
	for name, sized := range map[string]bool{"sized": true, "for this round": false} {
		tree, err := inputslice.NewCut(dimensions, 4, inputslice.SuffixCut("host"))
		if err != nil {
			t.Fatal(err)
		}
		// Four asked is one split of the root: classesPerSplit (4) classes
		// of the last character, and the child a split always has of the
		// values it exhausts - at the root, the empty value and the missing
		// label (segment.exhausted). Five slices.
		starting := len(tree.Pending())
		split := tree.Split
		if sized {
			split = tree.SplitSized
		}
		parts, err := split(tree.Pending()[0].ID, hosts(40))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		shape := tree.Shape()
		if !sized {
			if !shape.IsZero() {
				t.Fatalf("%s: a split for this round alone is in the shape", name)
			}
			continue
		}
		next, err := inputslice.FromShape(dimensions, shape)
		if err != nil {
			t.Fatalf("%s: shape does not rebuild: %v", name, err)
		}
		if want := starting - 1 + len(parts); len(next.Pending()) != want || starting != 5 {
			t.Fatalf("%s: next Slot starts in %d slices, want %d", name, len(next.Pending()), want)
		}
	}
}

// A leaf read whole and kept split (KeepSizedSplit) leaves this round's tree
// as it was - nothing pending added - and is in the Shape after the tree's
// own splits; a second split of it is refused.
func TestAKeptSizedSplitChangesOnlyTheShape(t *testing.T) {
	dimensions := []string{"host"}
	tree, err := inputslice.New(dimensions, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	leaf := tree.Pending()[0]
	children, sampled, err := tree.KeepSizedSplit(leaf.ID, hosts(40), nil)
	if err != nil || len(children) < 2 || sampled != 40 {
		t.Fatalf("kept %d parts of %d sampled, err %v; want parts of the 40", len(children), sampled, err)
	}
	if pending := tree.Pending(); len(pending) != 1 || pending[0].ID != leaf.ID {
		t.Fatalf("pending %+v, want the leaf alone, this round unchanged", pending)
	}
	next, err := inputslice.FromShape(dimensions, tree.Shape())
	if err != nil || len(next.Pending()) != len(children) {
		t.Fatalf("next starts in %d slices, err %v; want the %d parts", len(next.Pending()), err, len(children))
	}
	for index, part := range next.Pending() {
		if part.Slice.Digest() != children[index].Slice.Digest() {
			t.Fatalf("part %d is %s, kept %s", index, part.Slice.Digest(), children[index].Slice.Digest())
		}
	}
	if _, _, err := tree.KeepSizedSplit(leaf.ID, hosts(40), nil); err == nil {
		t.Fatal("a leaf kept split twice")
	}
}

// A split kept for time taken back when a leaf it made was itself kept split
// for its size (KeepSizedSplit) this round has no shape without it: the
// later split is of a leaf that is no longer there. Merged says so and
// gives the shape back as it was, both splits in it, rather than the zero
// Shape - every split lost, the next Slot read whole.
func TestAMergeThatWouldNotRebuildKeepsTheShape(t *testing.T) {
	dimensions := []string{"host"}
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.SuffixCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	children, err := tree.SplitKept(tree.Pending()[0].ID, hosts(40))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tree.KeepSizedSplit(children[0].ID, hosts(40), nil); err != nil {
		t.Fatal(err)
	}
	shape := tree.Shape()
	merged, rebuilt := shape.Merged()
	if rebuilt || merged.Leaves() != shape.Leaves() || merged.IsZero() {
		t.Fatalf("merged %d leaves rebuilt %v, want the shape's %d kept as it was and the merge refused",
			merged.Leaves(), rebuilt, shape.Leaves())
	}
	if _, err := inputslice.FromShape(dimensions, merged); err != nil {
		t.Fatalf("the shape kept does not rebuild: %v", err)
	}
}
