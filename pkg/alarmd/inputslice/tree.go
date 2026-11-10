// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package inputslice is the slicer of decision-020 batch 8 §7.3: the
// adaptive reverse-prefix tree that cuts a Query Group's series for one Slot
// into slices small enough to be read one after another within the Query
// Group's share, refining only the slice that turned out too large.
//
// A node is a path of character classes read from the end of a dimension's
// value backwards. Its children split it by the next character from the end
// into classes that are a partition of every rune, plus a child for the
// values that are exactly as long as the path. Whatever the values are, the
// leaves of the tree are therefore mutually exclusive and together complete:
// every series of the Query Group is in exactly one leaf's slice. Which
// classes a split takes only decides how many splits are needed, never which
// series are read.
package inputslice

import (
	"errors"
	"fmt"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const (
	// MaxLeaves is K_max: the most leaves one Slot's tree may have. Leaves,
	// not slices read: a split slice is discarded and its children replace
	// it, so at most MaxLeaves−1 slices are ever discarded and a query makes
	// at most 2·MaxLeaves−1 round trips.
	MaxLeaves = 64
	// ConditionBudget is M, the bytes a leaf's conditions may take over all
	// the dimensions it is cut on, charged as 10·n + 48·Σd for n dimensions
	// cut to depths d_i. Far below the query length any store accepts, so
	// the slices do not depend on how a deployment set that limit.
	ConditionBudget = 2048
)

// ErrValueConcentrated is what every ValueConcentratedError is, for
// errors.Is: the series of one slice cannot be told apart by the dimensions
// the tree may cut on, so the slice cannot be made smaller.
var ErrValueConcentrated = errors.New("alarmd inputslice: input value concentrated")

// Reasons a ValueConcentratedError names.
const (
	// ReasonMaxLeaves is a split that would leave the tree with more than
	// MaxLeaves leaves.
	ReasonMaxLeaves = "max_leaves"
	// ReasonConditionBudget is a split whose children's conditions would
	// cost more than ConditionBudget.
	ReasonConditionBudget = "condition_budget"
	// ReasonSingleValue is a slice whose sample shows one value, or no
	// further character, on the dimension it is cut on and on every
	// candidate dimension after it: no split would separate its series.
	ReasonSingleValue = "single_value"
)

// ValueConcentratedError is a slice the tree cannot refine, reported as
// INPUT_SLICE_VALUE_CONCENTRATED: the reason, the dimension the slice was
// being cut on and the slice's own conditions. It says the values are
// concentrated, many series under one combination of dimension values, and
// not that the store or the query is at fault.
type ValueConcentratedError struct {
	Reason    string
	Dimension string
	Slice     execution.InputSlice
}

func (err *ValueConcentratedError) Error() string {
	conditions := make([]string, 0, len(err.Slice.Conditions))
	for _, condition := range err.Slice.Conditions {
		operator := "req"
		if condition.Negated {
			operator = "nreq"
		}
		conditions = append(conditions, condition.Dimension+" "+operator+" "+condition.Pattern)
	}
	return fmt.Sprintf("alarmd inputslice: input value concentrated: %s on dimension %q, slice [%s]",
		err.Reason, err.Dimension, strings.Join(conditions, ", "))
}

// Is makes every ValueConcentratedError match ErrValueConcentrated.
func (err *ValueConcentratedError) Is(target error) bool { return target == ErrValueConcentrated }

// segment is a node's path on one dimension. Depth zero and not exhausted is
// that dimension's root, which constrains nothing and writes no condition.
type segment struct {
	dimension int
	// path is P read from the end: path[0] is X_1, the last character's
	// class.
	path []class
	// exhausted is the child of values exactly len(path) characters long; at
	// depth zero, the empty value and the missing label.
	exhausted bool
}

func (s segment) writesCondition() bool { return len(s.path) > 0 || s.exhausted }

func (s segment) condition(dimensions []string) execution.InputSliceCondition {
	name := dimensions[s.dimension]
	if len(s.path) == 0 {
		// The root's exhausted child is everything with no character at
		// all. Written negated so that a missing label is in it, as both the
		// store's matcher and UQ's own filter read nreq.
		return execution.InputSliceCondition{Dimension: name, Negated: true, Pattern: "^(?s:.+)$"}
	}
	var builder strings.Builder
	builder.WriteByte('^')
	if !s.exhausted {
		builder.WriteString("(?s:.*)")
	}
	for index := len(s.path) - 1; index >= 0; index-- {
		builder.WriteString(s.path[index].render())
	}
	builder.WriteByte('$')
	return execution.InputSliceCondition{Dimension: name, Pattern: builder.String()}
}

type nodeState int

const (
	statePending nodeState = iota
	stateLanded
	stateDiscarded
)

// node is a leaf, or a slice that was discarded and split. segments are in
// the order the tree cut on them; only the last one is ever cut further, and
// the ones before it are kept as they were, ANDed in.
type node struct {
	id       int
	segments []segment
	state    nodeState
	slice    execution.InputSlice
}

// Leaf is one slice of the tree waiting to be read: the ID to land or split
// it by, and the conditions every physical query of the Slot runs with.
type Leaf struct {
	ID    int
	Slice execution.InputSlice
}

// Tree is one Slot's slicing on one replica in one round. It is not an
// identity and is never persisted: a slice's shape is valid only for the
// reads it was made for.
//
// Not safe for concurrent use; slices are read one after another.
type Tree struct {
	dimensions []string
	nodes      []*node
	leaves     int
	discarded  int
	maxLeaves  int
}

// New starts a tree that cuts on dimensions, in order: the first one first,
// each later one only when the slices seen show a single value on the ones
// before it. Every dimension has to be a raw group-by label of every physical
// query of the Query Group, which the caller checks.
//
// The tree starts with at least leavesAtLeast leaves (K_0, §7.4): the root is
// split level by level on the first dimension by the fixed alphabet, the
// shallowest leaf first, so the starting slices are as even as an alphabet
// without a sample can make them. One or fewer is the single root leaf, the
// zero slice, which reads the whole Query Group as before slicing. A K_0 the
// tree cannot reach within MaxLeaves is a ValueConcentratedError: the Query
// Group needs more slices than the tree may have.
func New(dimensions []string, leavesAtLeast int) (*Tree, error) {
	return newTree(dimensions, leavesAtLeast, MaxLeaves)
}

func newTree(dimensions []string, leavesAtLeast, maxLeaves int) (*Tree, error) {
	if len(dimensions) == 0 {
		return nil, errors.New("alarmd inputslice: no dimension to slice on")
	}
	seen := map[string]struct{}{}
	for index, dimension := range dimensions {
		if strings.TrimSpace(dimension) == "" {
			return nil, fmt.Errorf("alarmd inputslice: dimension %d is blank", index)
		}
		if _, duplicate := seen[dimension]; duplicate {
			return nil, fmt.Errorf("alarmd inputslice: dimension %q is listed twice", dimension)
		}
		seen[dimension] = struct{}{}
	}
	tree := &Tree{dimensions: append([]string(nil), dimensions...), maxLeaves: maxLeaves}
	tree.add([]segment{{dimension: 0}})
	for tree.leaves < leavesAtLeast {
		target := tree.shallowestCuttable()
		if target == nil {
			return nil, &ValueConcentratedError{Reason: ReasonSingleValue, Dimension: dimensions[0]}
		}
		if _, err := tree.split(target, nil); err != nil {
			return nil, err
		}
	}
	return tree, nil
}

// shallowestCuttable is the pending leaf of the initial tree to split next:
// the first created among those not exhausted, which is the shallowest since
// leaves are created level by level.
func (tree *Tree) shallowestCuttable() *node {
	for _, candidate := range tree.nodes {
		if candidate.state == statePending && !candidate.segments[len(candidate.segments)-1].exhausted {
			return candidate
		}
	}
	return nil
}

func (tree *Tree) add(segments []segment) *node {
	conditions := make([]execution.InputSliceCondition, 0, len(segments))
	for _, s := range segments {
		if s.writesCondition() {
			conditions = append(conditions, s.condition(tree.dimensions))
		}
	}
	created := &node{id: len(tree.nodes), segments: segments, slice: execution.InputSlice{Conditions: conditions}}
	tree.nodes = append(tree.nodes, created)
	tree.leaves++
	return created
}

// Pending is the leaves neither landed nor split, in the order they were
// created, which is the order to read them in.
func (tree *Tree) Pending() []Leaf {
	var pending []Leaf
	for _, candidate := range tree.nodes {
		if candidate.state == statePending {
			pending = append(pending, Leaf{ID: candidate.id, Slice: candidate.slice})
		}
	}
	return pending
}

// Leaves is how many leaves the tree has, landed or pending.
func (tree *Tree) Leaves() int { return tree.leaves }

// Discarded is how many slices were read and then split because they turned
// out too large. Splits made while building the initial tree are not
// counted: those slices were never read.
func (tree *Tree) Discarded() int { return tree.discarded }

// Land records that a pending leaf was read whole. A landed leaf never
// changes again, which is what keeps the leaves split later disjoint from
// the slices already applied.
func (tree *Tree) Land(id int) error {
	target, err := tree.pending(id)
	if err != nil {
		return err
	}
	target.state = stateLanded
	return nil
}

// Split discards a pending leaf whose input reached the slice threshold and
// replaces it by its children, which are returned in reading order and are
// also pending. Landed leaves are untouched.
//
// sample is the dimension values of the series the discarded slice read
// before it was abandoned, by dimension; a series without a dimension counts
// as the empty value. It only steers the split: the leaf is cut by the next
// character from the end into classes of roughly equal count in the sample,
// and when the sample shows a single value on the dimension being cut, or no
// further character, the leaf is cut instead on the next candidate dimension
// on which the sample shows more than one value, keeping the leaf's own
// conditions and ANDing a fresh tree on that dimension. A dimension with no
// values in the sample, or a nil sample, is split by the fixed alphabet.
//
// It never cuts by equality to a sampled value: the sample is only the start
// of the stream, and a series with another value not yet delivered would then
// be in no slice.
//
// A split that cannot separate anything, would take the tree past
// MaxLeaves, or would cost more than ConditionBudget is a
// ValueConcentratedError, and the leaf stays pending as it was.
func (tree *Tree) Split(id int, sample map[string][]string) ([]Leaf, error) {
	target, err := tree.pending(id)
	if err != nil {
		return nil, err
	}
	children, err := tree.split(target, sample)
	if err != nil {
		return nil, err
	}
	tree.discarded++
	leaves := make([]Leaf, 0, len(children))
	for _, child := range children {
		leaves = append(leaves, Leaf{ID: child.id, Slice: child.slice})
	}
	return leaves, nil
}

func (tree *Tree) pending(id int) (*node, error) {
	if id < 0 || id >= len(tree.nodes) {
		return nil, fmt.Errorf("alarmd inputslice: no slice %d", id)
	}
	target := tree.nodes[id]
	switch target.state {
	case stateLanded:
		return nil, fmt.Errorf("alarmd inputslice: slice %d already landed", id)
	case stateDiscarded:
		return nil, fmt.Errorf("alarmd inputslice: slice %d already split", id)
	}
	return target, nil
}

func (tree *Tree) split(target *node, sample map[string][]string) ([]*node, error) {
	children, dimension, err := tree.childSegments(target, sample)
	if err != nil {
		return nil, err
	}
	concentrated := func(reason string) error {
		return &ValueConcentratedError{Reason: reason, Dimension: tree.dimensions[dimension], Slice: target.slice}
	}
	if tree.leaves-1+len(children) > tree.maxLeaves {
		return nil, concentrated(ReasonMaxLeaves)
	}
	for _, segments := range children {
		if conditionCost(segments) > ConditionBudget {
			return nil, concentrated(ReasonConditionBudget)
		}
	}
	target.state = stateDiscarded
	tree.leaves--
	created := make([]*node, 0, len(children))
	for _, segments := range children {
		created = append(created, tree.add(segments))
	}
	return created, nil
}

// conditionCost is what a leaf is charged against ConditionBudget: 10 for
// each dimension that writes a condition and 48 for each level of each path.
// It bounds the bytes the conditions actually take, since no class is wider
// than its level's share.
func conditionCost(segments []segment) int {
	cost := 0
	for _, s := range segments {
		if s.writesCondition() {
			cost += dimensionBytes + levelBytes*len(s.path)
		}
	}
	return cost
}

// childSegments is the children of target and the dimension they are cut on.
func (tree *Tree) childSegments(target *node, sample map[string][]string) ([][]segment, int, error) {
	last := target.segments[len(target.segments)-1]
	if !last.exhausted {
		values := sample[tree.dimensions[last.dimension]]
		histogram, progress := nextHistogram(values, len(last.path))
		if len(values) == 0 || progress {
			kept := target.segments[:len(target.segments)-1]
			return deeper(kept, last, splitClasses(histogram)), last.dimension, nil
		}
	}
	// An exhausted node has no next character on its dimension, and a node
	// whose sample is one value there would only grow deeper around that
	// value: either way the next dimension that tells the sample apart is
	// cut instead, from its root.
	for next := last.dimension + 1; next < len(tree.dimensions); next++ {
		values := sample[tree.dimensions[next]]
		histogram, progress := nextHistogram(values, 0)
		if len(values) > 0 && !progress {
			continue
		}
		kept := target.segments
		if !last.writesCondition() {
			// A root that constrains nothing need not be carried.
			kept = kept[:len(kept)-1]
		}
		return deeper(kept, segment{dimension: next}, splitClasses(histogram)), next, nil
	}
	return nil, last.dimension, &ValueConcentratedError{
		Reason: ReasonSingleValue, Dimension: tree.dimensions[last.dimension], Slice: target.slice,
	}
}

// deeper is the children of the not-exhausted segment parent, each after
// kept: one per class, extending the path by that class, and the exhausted
// child of values exactly as long as the parent's path.
func deeper(kept []segment, parent segment, classes []class) [][]segment {
	children := make([][]segment, 0, len(classes)+1)
	for _, next := range classes {
		path := make([]class, 0, len(parent.path)+1)
		path = append(append(path, parent.path...), next)
		children = append(children, appendSegment(kept, segment{dimension: parent.dimension, path: path}))
	}
	exhausted := segment{dimension: parent.dimension, path: parent.path, exhausted: true}
	return append(children, appendSegment(kept, exhausted))
}

func appendSegment(kept []segment, last segment) []segment {
	segments := make([]segment, 0, len(kept)+1)
	return append(append(segments, kept...), last)
}

// nextHistogram counts the sample's characters at depth+1 from the end and
// reports whether a split there would make progress: more than one distinct
// value, at least one of which has such a character.
func nextHistogram(values []string, depth int) (map[rune]int, bool) {
	histogram := map[rune]int{}
	distinct := map[string]struct{}{}
	for _, value := range values {
		distinct[value] = struct{}{}
		if r, ok := nextRune(value, depth); ok {
			histogram[r]++
		}
	}
	return histogram, len(distinct) > 1 && len(histogram) > 0
}
