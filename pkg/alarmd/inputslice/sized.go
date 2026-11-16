// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inputslice

import (
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A split for a slice's size (Split) is the round's: the next Slot starts in
// the leaves before it, finds the same slice too large again, and pays for it
// again. The two splits here are kept for the next Slot instead, and taken
// back by the series their leaves read (the worker's decay), not by time as a
// split kept for a slice that ran out of time is (SplitKept, Merged).

// laterSplit is a split of a leaf read whole this round, kept for the next
// Slot only: node is the leaf, children its parts.
type laterSplit struct {
	node     int
	children [][]segment
}

// SplitSized is Split for a slice the Slot started in and had to let go for
// its size: the split is in the tree's Shape, so the next Slot starts in its
// children and does not let the same slice go again. It is cut by the sample
// of the part of the slice read before it was let go - a coarse cut, which a
// child that is still too large refines the round after (KeepSizedSplit).
func (tree *Tree) SplitSized(id int, sample Sample) ([]Leaf, error) {
	return tree.splitRead(id, sample, splitSized)
}

// SizedChild is one part of a leaf KeepSizedSplit cut: its slice, and the
// series of the sample its values hold.
type SizedChild struct {
	Slice   execution.InputSlice
	Sampled int
}

// KeepSizedSplit cuts a leaf read whole this round, by the sample of what it
// read, for the next Slot to start in its parts: a leaf larger than a slice
// is meant to be, within its limit, which no split this round made smaller.
// This round's tree is unchanged - its pending leaves, what it lands - and
// the split is in its Shape only, after the tree's own splits. The parts are
// cut as Split cuts, and judged by the same bounds: a split that would leave
// the Shape past MaxLeaves-3 leaves - the most a Slot starts in - or a child
// past ConditionBudget is a ValueConcentratedError, and nothing is kept.
//
// Sampled is the series of the sample in each part, and sampled their sum:
// the share of the leaf's series each part is expected to hold.
func (tree *Tree) KeepSizedSplit(id int, sample Sample) (children []SizedChild, sampled int, err error) {
	if id < 0 || id >= len(tree.nodes) {
		return nil, 0, fmt.Errorf("alarmd inputslice: no slice %d", id)
	}
	target := tree.nodes[id]
	if target.state == stateDiscarded {
		return nil, 0, fmt.Errorf("alarmd inputslice: slice %d already split", id)
	}
	for _, later := range tree.later {
		if later.node == id {
			return nil, 0, fmt.Errorf("alarmd inputslice: slice %d already kept split", id)
		}
	}
	parts, dimension, err := tree.childSegments(target, sample)
	if err != nil {
		return nil, 0, err
	}
	concentrated := func(reason string) error {
		return &ValueConcentratedError{Reason: reason, Dimension: tree.dimensions[dimension], Slice: target.slice}
	}
	leaves := tree.leaves
	for _, later := range tree.later {
		leaves += len(later.children) - 1
	}
	if leaves-1+len(parts) > MaxLeaves-3 {
		return nil, 0, concentrated(ReasonMaxLeaves)
	}
	values := sample[tree.dimensions[dimension]]
	children = make([]SizedChild, 0, len(parts))
	for _, segments := range parts {
		if conditionCost(segments) > ConditionBudget {
			return nil, 0, concentrated(ReasonConditionBudget)
		}
		cut, held := segments[len(segments)-1], 0
		for _, value := range values {
			if cut.holds(value.Value) {
				held += value.Series
			}
		}
		conditions := make([]execution.InputSliceCondition, 0, len(segments))
		for _, s := range segments {
			if s.writesCondition() {
				conditions = append(conditions, s.condition(tree.dimensions))
			}
		}
		children = append(children, SizedChild{Slice: execution.InputSlice{Conditions: conditions}, Sampled: held})
		sampled += held
	}
	tree.later = append(tree.later, laterSplit{node: id, children: parts})
	return children, sampled, nil
}

// LastSized is the leaves the tree's last split kept for a slice's size made,
// by ID, nil when it made none: the leaves MergedSized would make one again.
// A split kept this round for a leaf read whole (KeepSizedSplit) has no
// leaves in this tree, and is not one.
func (tree *Tree) LastSized() []int {
	for index := len(tree.splits) - 1; index >= 0; index-- {
		if tree.splits[index].kind == splitSized {
			return append([]int(nil), tree.splits[index].children...)
		}
	}
	return nil
}

// MergedSized is the shape with its last split kept for a slice's size
// taken back, as Merged takes back the last one kept for its time: its
// children, leaves of the shape, are one leaf again. Whatever split it, a
// shape with no kept split left is the zero Shape. rebuilt is as Merged's.
func (shape Shape) MergedSized() (merged Shape, rebuilt bool) {
	last := -1
	for index, split := range shape.splits {
		if split.kind == splitSized {
			last = index
		}
	}
	if last < 0 {
		return shape, true
	}
	return shape.without(last)
}
