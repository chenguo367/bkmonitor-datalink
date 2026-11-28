// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package inputslice is the slicer of decision-020 batch 8 §7.3: the
// adaptive tree that cuts a Query Group's series for one Slot into slices
// small enough to be read one after another within the Query Group's share
// and its time, refining only the slice that turned out too large or too
// slow. A tree is cut one of two ways from its root (015 C2 section 4.8),
// by why its root was split: a root split for its size by a sample of its
// values is cut from the end of the values (this file), a root split for its
// time, or with no sample to cut by, by literal prefixes (prefix.go, and the
// round's judgement of it, rounds.go). A sample is of what a slice
// delivered, and a slice let go for its size delivered the start of its
// stream only, which the store answers in label order: a cut of the
// values' ends is even whatever that start was, a cut of their starts is
// not. A literal prefix is what the store reads a slice by, where a pattern
// led by .* reads every value of the label.
//
// A node is a path of character classes read from the end of a dimension's
// value backwards. Its children split it by the next character from the end
// into classes that are a partition of every rune, plus a child for the
// values that are exactly as long as the path; or, when the next character
// does not tell the slice's series apart, they cut one class of its own path
// into parts that partition that class. Whatever the values are, the
// leaves of the tree are therefore mutually exclusive and together complete:
// every series of the Query Group is in exactly one leaf's slice. Which
// classes a split takes only decides how many splits are needed, never which
// series are read.
package inputslice

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const (
	// MaxLeaves is K_max: the most leaves one Slot's tree may have. Leaves,
	// not slices read: a split slice is discarded and its children replace
	// it, so at most MaxLeaves−1 slices are ever discarded and a query makes
	// at most 2·MaxLeaves−1 round trips. 128: at 64 a Query Group of 500,000
	// series read by two consumers at four cores (015 C1), each slice at half
	// the share, started its second Slot capped at the 61 a Slot may start in
	// and was refused at its first split past them (the 1.0 next release
	// gate); and a Query Group of 8.8 million series was measured to need
	// 87 slices cut by prefix, 109 in a round that splits two at once (015 C2
	// section 4.7).
	MaxLeaves = 128
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
	// further character, on the dimension it is cut on and on every other
	// candidate dimension it is not cut on yet: no split would separate its
	// series.
	ReasonSingleValue = "single_value"
)

// ValueConcentratedError is a slice the tree cannot refine, reported as
// INPUT_SLICE_VALUE_CONCENTRATED: the reason, the dimension the slice was
// being cut on and the slice's own conditions. It says the values are
// concentrated, many series under one combination of dimension values, and
// not that the store or the query is at fault - but for ReasonMaxLeaves,
// reported as INPUT_SLICE_LEAVES_EXHAUSTED: the tree has no leaves left for
// the split, whatever the values are.
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
	// Said as its code is (worker InputSliceRefusal): a split the tree has
	// no leaves left for is INPUT_SLICE_LEAVES_EXHAUSTED, whatever the values.
	what := "input value concentrated"
	if err.Reason == ReasonMaxLeaves {
		what = "split leaves exhausted"
	}
	return fmt.Sprintf("alarmd inputslice: %s: %s on dimension %q, slice [%s]",
		what, err.Reason, err.Dimension, strings.Join(conditions, ", "))
}

// Is makes every ValueConcentratedError match ErrValueConcentrated.
func (err *ValueConcentratedError) Is(target error) bool { return target == ErrValueConcentrated }

// segment is a node's cut on one dimension, of one of two kinds: a path of
// classes from the end of the values, or, byPrefix, a literal prefix less
// what other children of its splits hold (prefix.go). Depth zero and not
// exhausted, of either kind, is that dimension's root, which constrains
// nothing and writes no condition.
type segment struct {
	dimension int
	// path is P read from the end: path[0] is X_1, the last character's
	// class.
	path []class
	// exhausted is the child of values exactly len(path) characters long; at
	// depth zero, the empty value and the missing label.
	exhausted bool
	// byPrefix is a segment cut by literal prefixes: prefix and except say
	// which values (prefix.go), path and exhausted are not used.
	byPrefix bool
	prefix   string
	except   string
}

func (s segment) writesCondition() bool {
	if s.byPrefix {
		return s.prefix != "" || s.except != ""
	}
	return len(s.path) > 0 || s.exhausted
}

// conditionsOf is every condition the segment writes.
func (s segment) conditionsOf(dimensions []string) []execution.InputSliceCondition {
	if s.byPrefix {
		return s.prefixConditions(dimensions)
	}
	return []execution.InputSliceCondition{s.condition(dimensions)}
}

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
	// parent is the node this one was split from, -1 for the root.
	parent int
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
	// splits is every split the tree was made by, in the order made: what
	// its Shape is taken from (shape.go).
	splits []treeSplit
	// later is the splits of leaves read whole that the next Slot starts in
	// (KeepSizedSplit): in the Shape after splits, never in this tree's
	// pending leaves.
	later []laterSplit
	// replayed is how many of splits a tree started in a Shape replayed
	// (FromShape): the ones after are this Slot's own.
	replayed int
	// switched is set once a slice of a tree cut by prefix was let go for
	// its size and split by the classes of its values' ends, ANDed after its
	// prefix (switchSegments): the Query Group's next Slot starts in a tree
	// cut from the end, its Shape none (Switched).
	switched bool
}

// New starts a tree that cuts on dimensions: each split on the one of them
// that splits its slice's sample most evenly (Split). Every dimension has to
// be a raw group-by label of every physical query of the Query Group, which
// the caller checks.
//
// With on one of the dimensions - the one the Query Group's last read was
// cut on - the tree starts with at least leavesAtLeast leaves (K_0, §7.4)
// cut by prefix (FixedCut), as far as MaxLeaves takes it: the root split on
// that dimension by the alphabet, then its leaves level by level, the
// shallowest first. With on empty, nothing chose a dimension, and one or
// fewer leaves asked, it is the single root leaf, the zero slice, which
// reads the whole Query Group as before slicing.
func New(dimensions []string, leavesAtLeast int, on string) (*Tree, error) {
	cut := Cut{}
	if on != "" {
		cut = FixedCut(on)
	}
	return newTree(dimensions, leavesAtLeast, MaxLeaves, cut)
}

// Cut is how a tree's root is cut: the dimension, and either the classes of
// its values' last character, a partition of every rune, or, byPrefix, the
// prefix its split cuts at and the characters it keeps apart after it. A
// tree started on a Cut (NewCut) cuts its root so, and every split of it
// is of that kind (015 C2 section 4.8); the Cut a sample makes (BestCut),
// by classes, is what a read whole let go for its size cuts its root by
// (SplitCut), and the one a tree's root was cut by (RootCut) is what the
// Query Group's next Slot, and this Slot's retry, start in - the same root
// slices. The zero Cut is none.
type Cut struct {
	Dimension string
	classes   []class
	byPrefix  bool
	at, apart string
}

// IsZero reports whether the Cut is none.
func (cut Cut) IsZero() bool { return cut.Dimension == "" }

// ByPrefix reports whether the Cut is by literal prefixes.
func (cut Cut) ByPrefix() bool { return cut.byPrefix }

// FixedCut is the Cut of a dimension at its root by the whole alphabet, by
// prefix: what a tree is cut by with no sample to cut it by - a Query Group
// that ran out of time before its read delivered any, or one split for its
// time.
func FixedCut(dimension string) Cut {
	return Cut{Dimension: dimension, byPrefix: true, apart: alphabet}
}

// SuffixCut is the Cut of a dimension by the fixed classes of its values'
// last character: what a tree that switched from prefix starts cut by when
// the slice that switched it showed no sample (015 C2 section 4.8).
func SuffixCut(dimension string) Cut { return Cut{Dimension: dimension, classes: fixedClasses()} }

// PrefixCutOn is cut's dimension cut by prefix at its root by the whole
// alphabet: a Query Group split for its time on the dimension a sample chose
// (BestCut), cut by prefix as every tree split for its time is.
func PrefixCutOn(cut Cut) Cut { return FixedCut(cut.Dimension) }

// NewCut is New starting on a Cut: the root split on its dimension by it,
// then level by level to at least leavesAtLeast leaves, as far as MaxLeaves
// takes it, the zero Cut the single root leaf.
func NewCut(dimensions []string, leavesAtLeast int, cut Cut) (*Tree, error) {
	return newTree(dimensions, leavesAtLeast, MaxLeaves, cut)
}

func newTree(dimensions []string, leavesAtLeast, maxLeaves int, cut Cut) (*Tree, error) {
	on := cut.Dimension
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
	root := 0
	if on != "" {
		if root = slices.Index(dimensions, on); root < 0 {
			return nil, fmt.Errorf("alarmd inputslice: dimension %q to start on is not one to slice on", on)
		}
	}
	tree := &Tree{dimensions: append([]string(nil), dimensions...), maxLeaves: maxLeaves}
	if cut.byPrefix {
		return tree.startByPrefix(root, leavesAtLeast, cut)
	}
	if on != "" && !partitionOf(class{negated: true}, cut.classes) {
		return nil, fmt.Errorf("alarmd inputslice: the classes to start on %q are no partition of every rune", on)
	}
	tree.add([]segment{{dimension: root}}, -1)
	for on != "" && tree.leaves < leavesAtLeast {
		target := tree.shallowestCuttable()
		if target == nil {
			return nil, &ValueConcentratedError{Reason: ReasonSingleValue, Dimension: on}
		}
		last := target.segments[len(target.segments)-1]
		children := deeper(target.segments[:len(target.segments)-1], last, cut.classes)
		if _, err := tree.splitInto(target, children, last.dimension, splitStarting); err != nil {
			return nil, err
		}
	}
	return tree, nil
}

// startByPrefix is newTree for a Cut by prefix: the root split by the cut,
// then the shallowest leaf with a character left to keep apart split by the
// alphabet at its own prefix, until the leaves asked or as far as the
// leaves a tree may have take it - a split past them would be refused, and
// the tree read in fewer slices than asked.
func (tree *Tree) startByPrefix(root, leavesAtLeast int, cut Cut) (*Tree, error) {
	rootSegment := segment{dimension: root, byPrefix: true}
	if !rootSegment.canSplitAt(cut.at, cut.apart) {
		return nil, fmt.Errorf("alarmd inputslice: the cut to start on %q keeps no literal characters apart", cut.Dimension)
	}
	rootNode := tree.add([]segment{rootSegment}, -1)
	if leavesAtLeast <= 1 {
		return tree, nil
	}
	if _, err := tree.splitInto(rootNode, splitBy(nil, rootSegment, cut.at, cut.apart), root, splitStarting); err != nil {
		return nil, err
	}
	for tree.leaves < leavesAtLeast {
		var target *node
		for _, candidate := range tree.nodes {
			if candidate.state == statePending && candidate.segments[len(candidate.segments)-1].free() != "" {
				target = candidate
				break
			}
		}
		if target == nil {
			break
		}
		last := target.segments[len(target.segments)-1]
		free := last.free()
		if tree.leaves-1+len(free)+1 > tree.maxLeaves {
			break
		}
		if _, err := tree.splitInto(target, splitBy(target.segments[:len(target.segments)-1], last, "", free), last.dimension, splitStarting); err != nil {
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

func (tree *Tree) add(segments []segment, parent int) *node {
	created := &node{id: len(tree.nodes), segments: segments, slice: sliceOf(segments, tree.dimensions), parent: parent}
	tree.nodes = append(tree.nodes, created)
	tree.leaves++
	return created
}

// sliceOf is the slice of a node's segments: every condition each writes.
func sliceOf(segments []segment, dimensions []string) execution.InputSlice {
	conditions := make([]execution.InputSliceCondition, 0, len(segments))
	for _, s := range segments {
		if s.writesCondition() {
			conditions = append(conditions, s.conditionsOf(dimensions)...)
		}
	}
	return execution.InputSlice{Conditions: conditions}
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

// CutOn is the dimension the tree's root was split on, empty while the
// root is a leaf (RootCut).
func (tree *Tree) CutOn() string { return tree.RootCut().Dimension }

// RootCut is the Cut the tree's root was split by - the dimension and the
// classes of its children's last character, or the prefix and the
// characters a split by prefix kept apart - the zero Cut while the root is a
// leaf.
func (tree *Tree) RootCut() Cut {
	for _, split := range tree.splits {
		if split.parent != 0 {
			continue
		}
		if first := tree.nodes[split.children[0]].segments; first[len(first)-1].byPrefix {
			children := make([][]segment, 0, len(split.children))
			for _, child := range split.children {
				children = append(children, tree.nodes[child].segments)
			}
			at, apart, ok := splitOf(tree.nodes[0].segments, children, len(tree.dimensions))
			if !ok {
				return Cut{}
			}
			return Cut{Dimension: tree.dimensions[first[len(first)-1].dimension], byPrefix: true, at: at, apart: apart}
		}
		var cut Cut
		for _, child := range split.children {
			last := tree.nodes[child].segments[len(tree.nodes[child].segments)-1]
			cut.Dimension = tree.dimensions[last.dimension]
			if !last.exhausted && len(last.path) == 1 {
				cut.classes = append(cut.classes, last.path[0])
			}
		}
		return cut
	}
	return Cut{}
}

// SplitCut is Split for the root, a read whole that passed its limit, cut
// by cut (BestCut) rather than by its sample's own split: the cut the Query
// Group's next Slot and this Slot's retry start in (RootCut, NewCut).
func (tree *Tree) SplitCut(id int, cut Cut) ([]Leaf, error) {
	target, err := tree.pending(id)
	if err != nil {
		return nil, err
	}
	dimension := slices.Index(tree.dimensions, cut.Dimension)
	if len(target.segments) != 1 || target.segments[0].writesCondition() || dimension < 0 {
		return nil, fmt.Errorf("alarmd inputslice: slice %d is no root to cut on %q", id, cut.Dimension)
	}
	var segments [][]segment
	switch {
	case cut.byPrefix && (segment{dimension: dimension, byPrefix: true}).canSplitAt(cut.at, cut.apart):
		segments = splitBy(nil, segment{dimension: dimension, byPrefix: true}, cut.at, cut.apart)
	case !cut.byPrefix && partitionOf(class{negated: true}, cut.classes):
		segments = deeper(nil, segment{dimension: dimension}, cut.classes)
	default:
		return nil, fmt.Errorf("alarmd inputslice: slice %d is no root to cut on %q by its classes", id, cut.Dimension)
	}
	children, err := tree.splitInto(target, segments, dimension, splitRead)
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

// Sample is what a slice read before it was let go shows of its series: by
// dimension, each value seen once, with how many of the series sampled
// carried it. A split is judged by the series its parts would hold, not by
// how many values each holds: a dimension of many values, one of which most
// series carry, is no even cut.
type Sample map[string][]SampledValue

// SampledValue is one value of a dimension in a Sample and the series of
// the sample that carried it.
type SampledValue struct {
	Value  string
	Series int
}

// SampleOf is the Sample of values, one entry a series, by dimension: each
// distinct value once, with the entries that carried it.
func SampleOf(values map[string][]string) Sample {
	if values == nil {
		return nil
	}
	sample := make(Sample, len(values))
	for dimension, list := range values {
		index := map[string]int{}
		for _, value := range list {
			if at, seen := index[value]; seen {
				sample[dimension][at].Series++
				continue
			}
			index[value] = len(sample[dimension])
			sample[dimension] = append(sample[dimension], SampledValue{Value: value, Series: 1})
		}
	}
	return sample
}

// BestCut is the Cut to cut the whole Query Group's root by, by sample: of
// the dimensions on which the sample shows more than one value, each cut by
// the classes its sample's last characters split into by the series they
// carry (splitClasses), the one whose largest part holds the fewest series
// sampled, the dimensions in order among equals. Judged by the classes it
// is cut by. A fixed alphabet assumes how values end: values ending in
// digits, most hosts' addresses and numbered names, put nine in ten series
// in its [0-8]. False when no dimension is one: no read chose.
func BestCut(dimensions []string, sample Sample) (Cut, bool) {
	if _, err := New(dimensions, 1, ""); err != nil {
		return Cut{}, false
	}
	best, bestShare, bestClasses := -1, share{}, []class(nil)
	for index, dimension := range dimensions {
		values := sample[dimension]
		histogram, progress := nextHistogram(values, 0)
		if len(values) == 0 || !progress {
			continue
		}
		classes := splitClasses(histogram)
		part := shareOf(deeper(nil, segment{dimension: index}, classes), values)
		if best < 0 || part.below(bestShare) {
			best, bestShare, bestClasses = index, part, classes
		}
	}
	if best < 0 {
		return Cut{}, false
	}
	return Cut{Dimension: dimensions[best], classes: bestClasses}, true
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
// before it was abandoned, by dimension, with the series that carried each
// (Sample); a series without a dimension counts as the empty value. It only steers the split. Each candidate is cut by the
// next character from the end into classes of roughly equal count in the
// sample: the dimension the leaf is cut on, further, and every other
// dimension the leaf is not cut on yet on which the sample shows more than
// one value, from its root, the leaf's own conditions kept and a fresh tree
// on that dimension ANDed. The split taken is the one whose largest part
// holds the smallest share of the series sampled - the most even, judged on
// the series each value carried (Sample), which a dimension of a single value
// never is - the leaf's own dimension first among equals, then the
// dimensions in order. A dimension with no values in the sample, or a nil
// sample, is split by the fixed alphabet.
//
// It never cuts by equality to a sampled value: the sample is only the start
// of the stream, and a series with another value not yet delivered would then
// be in no slice.
//
// A split that cannot separate anything, would take the tree past
// MaxLeaves, or would cost more than ConditionBudget is a
// ValueConcentratedError, and the leaf stays pending as it was.
func (tree *Tree) Split(id int, sample Sample) ([]Leaf, error) {
	return tree.splitRead(id, sample, splitRead)
}

// SplitKept is Split for a slice its Query Group's next Slot is to be read
// split too: one that ran out of time for its size. The split is in the
// tree's Shape, which a slice Split is not.
func (tree *Tree) SplitKept(id int, sample Sample) ([]Leaf, error) {
	return tree.splitRead(id, sample, splitKept)
}

func (tree *Tree) splitRead(id int, sample Sample, kind splitKind) ([]Leaf, error) {
	target, err := tree.pending(id)
	if err != nil {
		return nil, err
	}
	children, err := tree.split(target, sample, kind)
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

// split is target's children, by the kind of its last segment and why it is
// split (015 C2 section 4.8): a leaf cut from the end is cut further so
// (childSegments); a leaf cut by prefix is cut further by prefix for its
// time (prefixChildSegments), and for its size - let go as it was read, its
// sample the start of its stream only - by the classes of its values' ends
// ANDed after its prefix, the tree switched (switchSegments); the root, cut
// neither way yet, by its sample's classes for its size, and by prefix for
// its time or with no sample.
func (tree *Tree) split(target *node, sample Sample, kind splitKind) ([]*node, error) {
	last := target.segments[len(target.segments)-1]
	root := len(target.segments) == 1 && !last.writesCondition()
	forSize := kind == splitRead || kind == splitSized
	var (
		children  [][]segment
		dimension int
		err       error
		switching bool
	)
	switch {
	case root && forSize && sampled(sample):
		children, dimension, err = tree.childSegments(target, sample)
	case root || last.byPrefix && !forSize:
		children, dimension, err = tree.prefixChildSegments(target.segments, target.slice, nil)
	case last.byPrefix:
		children, dimension, err = tree.switchSegments(target, sample)
		switching = true
	default:
		children, dimension, err = tree.childSegments(target, sample)
	}
	if err != nil {
		return nil, err
	}
	created, err := tree.splitInto(target, children, dimension, kind)
	if err == nil && switching {
		tree.switched = true
	}
	return created, err
}

// sampled reports whether the sample shows any value.
func sampled(sample Sample) bool {
	for _, values := range sample {
		if len(values) > 0 {
			return true
		}
	}
	return false
}

// switchSegments is the children of target, a leaf cut by prefix let go for
// its size: of every dimension the sample tells apart, the leaf's own first,
// cut from its root by the classes of its values' last characters
// (splitClasses), ANDed after every segment of the leaf, the most even by the
// sample - a split of the values' ends, which the start of a stream in label
// order samples as evenly as the whole. The leaves under it are cut so as a
// tree cut from the end is.
func (tree *Tree) switchSegments(target *node, sample Sample) ([][]segment, int, error) {
	last := target.segments[len(target.segments)-1]
	var best [][]segment
	bestDimension, bestShare := last.dimension, share{}
	order := append([]int{last.dimension}, slices.DeleteFunc(slices.Clone(rangeOf(len(tree.dimensions))), func(at int) bool { return at == last.dimension })...)
	for _, next := range order {
		values := sample[tree.dimensions[next]]
		histogram, progress := nextHistogram(values, 0)
		if len(values) > 0 && !progress {
			continue
		}
		children := deeper(target.segments, segment{dimension: next}, splitClasses(histogram))
		if part := shareOf(children, values); best == nil || part.below(bestShare) {
			best, bestDimension, bestShare = children, next, part
		}
	}
	if best == nil {
		return nil, last.dimension, &ValueConcentratedError{Reason: ReasonSingleValue, Dimension: tree.dimensions[last.dimension], Slice: target.slice}
	}
	return best, bestDimension, nil
}

func rangeOf(n int) []int {
	out := make([]int, n)
	for index := range out {
		out[index] = index
	}
	return out
}

// prefixChildSegments is the children of the node of segments cut by prefix
// and the dimension they are cut on: on the node's own dimension where its
// bound cuts it (boundedCut), at its own prefix by the alphabet with none;
// failing that, the first other dimension it is not cut on that tells its
// values apart, from its root, the node's segments kept. A dimension whose
// bound is one value tells nothing apart. Neither is a ValueConcentratedError
// of ReasonSingleValue.
func (tree *Tree) prefixChildSegments(segments []segment, slice execution.InputSlice, bounds Bounds) ([][]segment, int, error) {
	last := segments[len(segments)-1]
	boundOf := func(dimension int) *Bound {
		if bound, ok := bounds[tree.dimensions[dimension]]; ok {
			return &bound
		}
		return nil
	}
	own := last
	own.byPrefix = true
	if children, ok := boundedCut(segments[:len(segments)-1], own, boundOf(last.dimension)); ok {
		return children, last.dimension, nil
	}
	kept := segments
	if !last.writesCondition() {
		// A root that constrains nothing need not be carried.
		kept = kept[:len(kept)-1]
	}
	for next := range tree.dimensions {
		if next == last.dimension || cutOn(segments, next) {
			continue
		}
		if children, ok := boundedCut(kept, segment{dimension: next, byPrefix: true}, boundOf(next)); ok {
			return children, next, nil
		}
	}
	return nil, last.dimension, &ValueConcentratedError{Reason: ReasonSingleValue, Dimension: tree.dimensions[last.dimension], Slice: slice}
}

// Switched reports whether a slice of the tree, cut by prefix, was let go for
// its size and split by its values' ends (switchSegments): the Query Group's
// next Slot starts in a tree cut from the end, its Shape none.
func (tree *Tree) Switched() bool { return tree.switched }

// ByPrefix reports whether the tree's root was split by prefix: a tree
// whose splits of a round are judged by Next.
func (tree *Tree) ByPrefix() bool { return tree.RootCut().byPrefix }

// splitInto replaces target by children, the split's kind kept with it.
func (tree *Tree) splitInto(target *node, children [][]segment, dimension int, kind splitKind) ([]*node, error) {
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
	ids := make([]int, 0, len(children))
	for _, segments := range children {
		child := tree.add(segments, target.id)
		created, ids = append(created, child), append(ids, child.id)
	}
	tree.splits = append(tree.splits, treeSplit{parent: target.id, children: ids, kind: kind})
	return created, nil
}

// conditionCost is what a leaf is charged against ConditionBudget: 10 for
// each dimension cut from the end that writes a condition and 48 for each
// level of each path - it bounds the bytes the conditions actually take,
// since no class is wider than its level's share - and the bytes a segment
// cut by prefix writes.
func conditionCost(segments []segment) int {
	cost := 0
	for _, s := range segments {
		switch {
		case !s.writesCondition():
		case s.byPrefix:
			for _, condition := range s.prefixPatterns() {
				cost += len(condition.Pattern)
			}
		default:
			cost += dimensionBytes + levelBytes*len(s.path)
		}
	}
	return cost
}

// childSegments is the children of target and the dimension they are cut
// on: of every candidate split (Split), the most even by the sample.
func (tree *Tree) childSegments(target *node, sample Sample) ([][]segment, int, error) {
	last := target.segments[len(target.segments)-1]
	var best [][]segment
	bestDimension, bestShare := last.dimension, share{}
	consider := func(children [][]segment, dimension int) {
		if part := shareOf(children, sample[tree.dimensions[dimension]]); best == nil || part.below(bestShare) {
			best, bestDimension, bestShare = children, dimension, part
		}
	}
	if children, ok := further(target.segments, sample[tree.dimensions[last.dimension]]); ok {
		consider(children, last.dimension)
	}
	// Another dimension that tells the sample apart is cut from its root:
	// one before the leaf's as well as one after, so a first dimension of a
	// single value is no slice the tree has to cut through first.
	for next := range tree.dimensions {
		if next == last.dimension || cutOn(target.segments, next) {
			continue
		}
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
		consider(deeper(kept, segment{dimension: next}, splitClasses(histogram)), next)
	}
	if best == nil {
		return nil, last.dimension, &ValueConcentratedError{
			Reason: ReasonSingleValue, Dimension: tree.dimensions[last.dimension], Slice: target.slice,
		}
	}
	return best, bestDimension, nil
}

// further is the children of the node of segments cut further on its own
// dimension, by values, the sample's on it, and false when that cannot
// separate anything: an exhausted node has no next character there, and a
// node whose sample is one value would only grow deeper around that value.
func further(segments []segment, values []SampledValue) ([][]segment, bool) {
	last := segments[len(segments)-1]
	kept := segments[:len(segments)-1]
	if last.exhausted {
		return refine(kept, last, values)
	}
	histogram, progress := nextHistogram(values, len(last.path))
	if len(values) == 0 || separatesAt(values, len(last.path)) {
		return deeper(kept, last, splitClasses(histogram)), true
	}
	// The next character does not tell the sample apart. A class an earlier
	// split took from the part of the stream it saw may hold characters that
	// do, and only cutting that class separates them: values that differ only
	// within it share every character before it, and going deeper would only
	// walk that shared prefix to its end.
	if children, refined := refine(kept, last, values); refined {
		return children, true
	}
	if progress {
		return deeper(kept, last, splitClasses(histogram)), true
	}
	return nil, false
}

// cutOn reports whether segments already cut on dimension.
func cutOn(segments []segment, dimension int) bool {
	for _, s := range segments {
		if s.dimension == dimension && s.writesCondition() {
			return true
		}
	}
	return false
}

// share is the series of a split's largest part over all the series it was
// judged on: a split judged on none is as uneven as one that keeps them all
// together.
type share struct{ largest, of int }

// shareOf is how children, each cut on its last segment, part the series
// of values.
func shareOf(children [][]segment, values []SampledValue) share {
	total := 0
	for _, value := range values {
		total += value.Series
	}
	if total == 0 {
		return share{largest: 1, of: 1}
	}
	largest := 0
	for _, child := range children {
		cut, count := child[len(child)-1], 0
		for _, value := range values {
			if cut.holds(value.Value) {
				count += value.Series
			}
		}
		largest = max(largest, count)
	}
	return share{largest: largest, of: total}
}

// below reports whether the share is smaller than other's.
func (s share) below(other share) bool { return s.largest*other.of < other.largest*s.of }

// holds reports whether value is in the segment: its characters from the
// end in the path's classes, and, for an exhausted segment, none before them;
// for a segment cut by prefix, prefixHolds.
func (s segment) holds(value string) bool {
	if s.byPrefix {
		return s.prefixHolds(value)
	}
	for depth, c := range s.path {
		if r, ok := nextRune(value, depth); !ok || !c.contains(r) {
			return false
		}
	}
	if s.exhausted {
		_, more := nextRune(value, len(s.path))
		return !more
	}
	return true
}

// separatesAt reports whether the sample's characters at depth+1 from the end
// tell it apart: two different characters there, or one character beside a
// value that has none (the exhausted child's).
func separatesAt(values []SampledValue, depth int) bool {
	var first rune
	seen, short := false, false
	for _, value := range values {
		r, ok := nextRune(value.Value, depth)
		switch {
		case !ok:
			short = true
		case !seen:
			first, seen = r, true
		case r != first:
			return true
		}
	}
	return seen && short
}

// refine is the children of parent made by cutting one class of its path
// rather than going past it: the class split by the sample's characters at
// that level, each part replacing it, the rest of the path and the
// exhaustion kept. The parts partition the class, so the children partition
// the parent as a deeper split's do. The level taken is the first one at
// which the sample shows two characters, and failing that the first whose
// class is wider than what the sample shows there, which separates the
// series seen from the ones the class holds besides. It reports false when
// no level can be cut, or a part would not fit its level's condition bytes.
func refine(kept []segment, parent segment, values []SampledValue) ([][]segment, bool) {
	level, parts := -1, []class(nil)
	for index := range parent.path {
		histogram := map[rune]int{}
		for _, value := range values {
			if r, ok := nextRune(value.Value, index); ok {
				histogram[r] += value.Series
			}
		}
		if len(histogram) == 0 {
			continue
		}
		cut := cutClass(parent.path[index], splitClasses(histogram))
		if len(cut) < 2 {
			continue
		}
		if level < 0 || len(histogram) > 1 {
			level, parts = index, cut
		}
		if len(histogram) > 1 {
			break
		}
	}
	if level < 0 {
		return nil, false
	}
	children := make([][]segment, 0, len(parts))
	for _, part := range parts {
		if len(part.render()) > levelBytes {
			return nil, false
		}
		path := slices.Clone(parent.path)
		path[level] = part
		children = append(children, appendSegment(kept, segment{dimension: parent.dimension, path: path, exhausted: parent.exhausted}))
	}
	return children, true
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

// nextHistogram counts the sample's series by their characters at depth+1
// from the end and reports whether a split there would make progress: more
// than one distinct value, at least one of which has such a character.
func nextHistogram(values []SampledValue, depth int) (map[rune]int, bool) {
	histogram := map[rune]int{}
	distinct := map[string]struct{}{}
	for _, value := range values {
		distinct[value.Value] = struct{}{}
		if r, ok := nextRune(value.Value, depth); ok {
			histogram[r] += value.Series
		}
	}
	return histogram, len(distinct) > 1 && len(histogram) > 0
}
