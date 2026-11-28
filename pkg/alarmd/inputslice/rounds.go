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
	"maps"
	"slices"
	"strconv"
	"strings"
)

// LeafRead is what one Slot read of one leaf of its tree whole: the time its
// read took, the series it delivered and the input it retained. A leaf
// split while it was read - let go for its size, or out of time - was not
// read whole; one the provider timed out and the Slot split for it
// (SplitKept) gives the time it ran out after under its own ID, what that
// split is judged against the round after.
type LeafRead struct {
	Millis, Series, Bytes uint64
	// TimedOut is a read the provider timed out and that completed
	// degraded, its Millis the backend's limit: it says how slow the slice
	// is and nothing of what it holds, so it splits for time as any slow
	// read does and is judged neither empty nor quick.
	TimedOut bool
	// Bounds is the least and greatest value of each dimension the leaf's
	// read delivered (Bound), for a leaf read slower than the target: where
	// its split for time is cut, at their common prefix. A leaf without them
	// - a read that delivered nothing, as an aggregation past its threshold
	// does - is cut by the alphabet at its own prefix.
	Bounds Bounds
}

// Rules are what Next judges a round by (015 C2 section 4.3).
type Rules struct {
	// TargetMillis is T: a leaf read slower is split for its time; a kept
	// split whose leaves together would read in under half of it is taken
	// back.
	TargetMillis uint64
	// Rounds is N: the rounds in a row a kept split is to be taken back, or
	// a child folded into its rest, before it is.
	Rounds int
	// HalfSeries and HalfBytes are half a slice's series and half the input
	// a slice may retain: a kept split taken back holds no more than both,
	// its leaves together, so that it is not split for its size again.
	HalfSeries, HalfBytes uint64
}

// History is what a Query Group's shape carries from one round to the
// next, by node: where the node is in the tree (nodeKeys), the same node
// whatever its rest excepts as children are folded into it.
type History map[string]NodeHistory

// NodeHistory is one node's.
type NodeHistory struct {
	// Quick is the rounds in a row the node's kept split read, its leaves
	// together, as a slice to be taken back does.
	Quick int
	// Empty is the rounds in a row the leaf read as an empty slice does.
	Empty int
	// SplitFrom is the node's time when it was split for its time, the
	// round before its children are read: what they are judged against.
	SplitFrom uint64
	// HeldAt is the time the node read at when a split of it for its time
	// was found no faster and taken back: it is not split for time again
	// until it reads at twice that, and the mark goes once it reads under
	// the target.
	HeldAt uint64
}

// NextCounts is what one round's Next did.
type NextCounts struct {
	// SplitForTime is the leaves split for their time.
	SplitForTime uint64
	// HeldUniform is the leaves past the target not split: no slower than
	// twice the round's median, a backend slow as a whole.
	HeldUniform uint64
	// Unhelpful is the splits for time taken back for their children reading
	// no faster.
	Unhelpful uint64
	// HeldByMark is the leaves past the target not split for an earlier
	// split of theirs that did not help (NodeHistory.HeldAt).
	HeldByMark uint64
	// Folded is the children folded into their rest.
	Folded uint64
	// Merged is the kept splits taken back.
	Merged uint64
	// MergeRefused is the rounds whose splits to take back would not
	// rebuild without them: the shape kept as it was.
	MergeRefused uint64
	// LeafCap is the leaves to split for their time that the leaves a Slot
	// starts in (MaxLeaves-3) left no room for: not split, read as they are.
	LeafCap uint64
	// Concentrated is the leaves to split for their time that no split
	// separates (ReasonSingleValue).
	Concentrated uint64
	// ConditionBudget is the leaves to split for their time whose children's
	// conditions would pass the budget (ReasonConditionBudget): not split,
	// read as they are - split, the next Slot's tree would refuse the shape.
	ConditionBudget uint64
}

// Next is the Shape the Query Group's next Slot starts in after this one, and
// the history it carries, for a tree cut by prefix (ByPrefix) that did not
// switch (Switched) - a tree cut from the end is judged by its own rules
// (Merged, MergedSized) - judged on this Slot's reads of its leaves by ID
// (LeafRead). From the tree's shape - every split it was started in or
// kept, starting splits included - in this order:
//
//  1. A split for time made the round before whose children read with a
//     median of no less than half the time its node read at is no help -
//     a backend slow as a whole, slow whatever it is asked - and is taken
//     back, the node marked (NodeHistory.HeldAt).
//  2. A kept split whose leaves together would read under half the target
//     - their times less the round's fixed cost, the fastest leaf's time,
//     and that cost once - and hold under half a slice's series and bytes,
//     for Rules.Rounds rounds in a row, is taken back.
//  3. A child kept apart that read within twice the fixed cost - an empty
//     slice's time - for Rules.Rounds rounds in a row is folded into its
//     rest, a leaf too: one read fewer, and a value of it that appears later
//     is read in the rest, split apart again once the rest is slow.
//  4. A leaf that read slower than the target is split a level deeper, for
//     the next Slot, at the common prefix of its values' bounds, by the
//     alphabet at its own prefix with none (prefixChildSegments): when the
//     round read fewer than three leaves whole on
//     the target alone, else only when it read at least twice the round's
//     median - a backend slow as a whole does not split every leaf, every
//     round. Not past MaxLeaves-3 leaves: a leaf there is read as it is,
//     counted LeafCap; nor into children whose conditions would pass
//     ConditionBudget, counted so.
//
// A leaf not read whole this round, or a split any of whose leaves was not,
// is judged on nothing and keeps its counts. A leaf the provider timed out
// (LeafRead.TimedOut) is read as slow as its limit: its split judged so in
// 1, never quick in 2 nor empty in 3. Every split made changes the kind of
// its split to kept, so the Shape holds it.
//
// Each node's history is the one it carried, changed by each step in turn
// where the step judges it - as the parent of a split in 1 and 2, as a child
// kept apart in 3, as a leaf in 4 - and what the steps leave of it is what it
// carries next, a count set to none as much as one set to more; the nodes
// the shape no longer has go.
func (tree *Tree) Next(reads map[int]LeafRead, history History, rules Rules) (Shape, History, NextCounts) {
	var counts NextCounts
	state := maps.Clone(history)
	if state == nil {
		state = History{}
	}
	read := map[string]LeafRead{}
	keys := tree.nodeKeys()
	for _, candidate := range tree.nodes {
		if r, ok := reads[candidate.id]; ok && candidate.state == stateLanded {
			read[keys[candidate.id]] = r
		}
	}
	splits := tree.allSplits()
	current, err := structure(tree.dimensions, splits)
	if err != nil {
		return tree.Shape(), history, counts
	}
	keys = current.nodeKeys()
	// The round's slices: every leaf it read whole, a slice split while it
	// was read for its size among them, by its parts.
	times := make([]uint64, 0, len(read))
	for _, r := range read {
		times = append(times, r.Millis)
	}
	slices.Sort(times)
	fixed, median := uint64(0), uint64(0)
	if len(times) > 0 {
		fixed, median = times[0], times[len(times)/2]
	}

	// 1 and 2: splits taken back, each parent then a leaf that starts over
	// - marked when its split was no help - once the shape rebuilds without
	// them; when it does not, the shape is kept as it was and so is each
	// parent's history, to be judged again.
	merge := map[int]bool{}
	merged := History{}
	for index, split := range current.splits {
		key := keys[split.parent]
		node := state[key]
		children, all := current.childReads(split, read, keys)
		if !all {
			continue
		}
		if node.SplitFrom > 0 {
			if 2*median64(children) >= node.SplitFrom {
				merge[index] = true
				merged[key] = NodeHistory{HeldAt: node.SplitFrom}
				counts.Unhelpful++
				continue
			}
			node.SplitFrom = 0
		}
		if split.kind.keptForNext() {
			if quick(children, fixed, rules) {
				node.Quick++
			} else {
				node.Quick = 0
			}
			if node.Quick >= rules.Rounds {
				merge[index] = true
				merged[key] = NodeHistory{}
				counts.Merged++
				continue
			}
		}
		state[key] = node
	}
	if len(merge) > 0 {
		kept := make([]shapeSplit, 0, len(splits))
		for index, split := range splits {
			if !merge[index] {
				kept = append(kept, split)
			}
		}
		if rebuilt, err := structure(tree.dimensions, kept); err == nil {
			splits, current, keys = kept, rebuilt, rebuilt.nodeKeys()
			maps.Copy(state, merged)
		} else {
			counts.MergeRefused++
			counts.Merged, counts.Unhelpful = 0, 0
		}
	}

	// 3: children folded into their rest.
	for index := range splits {
		split := current.splits[index]
		restID := split.children[len(split.children)-1]
		if current.nodes[restID].state != statePending {
			continue
		}
		if r, ok := read[keys[restID]]; !ok || r.TimedOut {
			continue
		}
		var folded []int
		for _, id := range split.children[:len(split.children)-1] {
			key := keys[id]
			r, ok := read[key]
			if current.nodes[id].state != statePending || !ok || r.TimedOut {
				continue
			}
			node := state[key]
			if r.Millis <= 2*fixed {
				node.Empty++
			} else {
				node.Empty = 0
			}
			if node.Empty >= rules.Rounds {
				folded = append(folded, slices.Index(split.children, id))
				continue
			}
			state[key] = node
		}
		if len(folded) == 0 {
			continue
		}
		splits[index] = foldInto(splits[index], folded)
		counts.Folded += uint64(len(folded))
	}
	splits = slices.DeleteFunc(splits, func(split shapeSplit) bool { return len(split.children) < 2 })
	if rebuilt, err := structure(tree.dimensions, splits); err == nil {
		current, keys = rebuilt, rebuilt.nodeKeys()
	} else {
		return tree.Shape(), history, NextCounts{MergeRefused: 1}
	}

	// 4: leaves split for their time.
	leaves := current.leaves
	for _, leaf := range current.pendingNodes() {
		key := keys[leaf.id]
		r, ok := read[key]
		if !ok {
			continue
		}
		node := state[key]
		if node.HeldAt > 0 {
			if r.Millis <= rules.TargetMillis {
				node.HeldAt = 0
			} else if r.Millis < 2*node.HeldAt {
				counts.HeldByMark++
				state[key] = node
				continue
			}
		}
		if r.Millis <= rules.TargetMillis {
			state[key] = node
			continue
		}
		if len(times) >= 3 && r.Millis < 2*median {
			counts.HeldUniform++
			state[key] = node
			continue
		}
		children, _, err := current.prefixChildSegments(leaf.segments, leaf.slice, r.Bounds)
		switch {
		case err != nil:
			counts.Concentrated++
		case slices.ContainsFunc(children, func(segments []segment) bool { return conditionCost(segments) > ConditionBudget }):
			counts.ConditionBudget++
		case leaves-1+len(children) > MaxLeaves-3:
			counts.LeafCap++
		default:
			splits = append(splits, shapeSplit{parent: slices.Clone(leaf.segments), children: children, kind: splitKept})
			leaves += len(children) - 1
			node.SplitFrom, node.Quick, node.Empty = r.Millis, 0, 0
			counts.SplitForTime++
		}
		state[key] = node
	}

	tree.markTimeoutSplits(reads, state)
	shape := Shape{dimensions: slices.Clone(tree.dimensions), splits: splits}
	final, err := structure(tree.dimensions, splits)
	if err != nil {
		return tree.Shape(), history, NextCounts{MergeRefused: 1}
	}
	next := final.carried(state)
	if shape.IsZero() {
		return Shape{}, next, counts
	}
	return shape, next, counts
}

// KeptHistory is the history a Query Group's next Slot starts with when this
// one failed and the shape of its tree cut by prefix is kept as it is
// (Shape): what it carried, a split this Slot made for a slice the provider
// timed out marked to be judged the round after against the time it ran out
// after - reads by ID, as Next is given them - as Next marks it, for the
// nodes of the shape alone. Nil for a tree cut from the end, which Next does
// not judge.
func (tree *Tree) KeptHistory(reads map[int]LeafRead, history History) History {
	if !tree.ByPrefix() || tree.Switched() {
		return nil
	}
	shape := tree.Shape()
	if shape.IsZero() {
		return nil
	}
	final, err := structure(tree.dimensions, shape.splits)
	if err != nil {
		return nil
	}
	state := maps.Clone(history)
	if state == nil {
		state = History{}
	}
	tree.markTimeoutSplits(reads, state)
	return final.carried(state)
}

// markTimeoutSplits marks every split this Slot made for a slice the
// provider timed out (SplitKept, its parent's read the time it ran out
// after) to be judged the round after against that time.
func (tree *Tree) markTimeoutSplits(reads map[int]LeafRead, state History) {
	keys := tree.nodeKeys()
	for index := tree.replayed; index < len(tree.splits); index++ {
		split := tree.splits[index]
		if r, ok := reads[split.parent]; ok && split.kind == splitKept {
			node := state[keys[split.parent]]
			node.SplitFrom = r.Millis
			state[keys[split.parent]] = node
		}
	}
}

// carried is what of state the tree's nodes carry to the next round: their
// histories but the ones with nothing in them.
func (tree *Tree) carried(state History) History {
	next := History{}
	for _, key := range tree.nodeKeys() {
		if node, ok := state[key]; ok && node != (NodeHistory{}) {
			next[key] = node
		}
	}
	return next
}

// nodeKeys names each node of the tree by where it is in it, as History keeps
// it: its parent's name and its own cut - "~" for the rest of a split, the
// last of its children, which holds what the others do not; the dimension
// and the prefix, or the path from the end, of any other. Not by what a
// rest excepts: that changes as children are folded into it (foldInto,
// MakeRoom), and it is the same node. A node is added after the one it was
// split from, so its parent is named before it.
func (tree *Tree) nodeKeys() map[int]string {
	rest := map[int]bool{}
	for _, split := range tree.splits {
		rest[split.children[len(split.children)-1]] = true
	}
	keys := make(map[int]string, len(tree.nodes))
	for _, candidate := range tree.nodes {
		var builder strings.Builder
		if candidate.parent >= 0 {
			builder.WriteString(keys[candidate.parent])
		}
		builder.WriteByte('/')
		if rest[candidate.id] {
			builder.WriteByte('~')
		} else {
			cut := candidate.segments[len(candidate.segments)-1]
			builder.WriteString(strconv.Itoa(cut.dimension))
			builder.WriteByte(':')
			builder.WriteString(cut.prefix)
			for _, c := range cut.path {
				builder.WriteByte(':')
				builder.WriteString(c.render())
			}
			if cut.exhausted {
				builder.WriteByte('$')
			}
		}
		keys[candidate.id] = builder.String()
	}
	return keys
}

// allSplits is every split of the tree its Shape holds, starting splits
// included whether or not a kept one is among them: what Next judges.
func (tree *Tree) allSplits() []shapeSplit {
	shape := tree.Shape()
	if !shape.IsZero() {
		return shape.splits
	}
	var splits []shapeSplit
	for _, split := range tree.splits {
		if split.kind != splitStarting {
			continue
		}
		children := make([][]segment, 0, len(split.children))
		for _, child := range split.children {
			children = append(children, slices.Clone(tree.nodes[child].segments))
		}
		splits = append(splits, shapeSplit{parent: slices.Clone(tree.nodes[split.parent].segments), children: children, kind: split.kind})
	}
	return splits
}

// structure is the tree of splits, every leaf pending: FromShape, for the
// structure alone.
func structure(dimensions []string, splits []shapeSplit) (*Tree, error) {
	return FromShape(dimensions, Shape{dimensions: dimensions, splits: splits})
}

func (tree *Tree) pendingNodes() []*node {
	var pending []*node
	for _, candidate := range tree.nodes {
		if candidate.state == statePending {
			pending = append(pending, candidate)
		}
	}
	return pending
}

// childReads is the reads of a split's children, by key (nodeKeys), and
// whether every child is a leaf this round read: one the provider timed out
// among them, as slow as its limit.
func (tree *Tree) childReads(split treeSplit, read map[string]LeafRead, keys map[int]string) ([]LeafRead, bool) {
	children := make([]LeafRead, 0, len(split.children))
	for _, id := range split.children {
		r, ok := read[keys[id]]
		if tree.nodes[id].state != statePending || !ok {
			return nil, false
		}
		children = append(children, r)
	}
	return children, true
}

// quick reports whether children read together as a split to be taken back
// does: their times less the fixed cost, and the cost once, under half the
// target, their series and bytes under half a slice's - a split for time as
// much as one for size: taken back over half a slice's bytes, the leaf that
// is left may be let go for its size as it is read, and its tree switch
// from the end for good (Switched) - and none of them one the provider timed
// out, which says nothing of what it holds.
func quick(children []LeafRead, fixed uint64, rules Rules) bool {
	estimate, series, bytes := fixed, uint64(0), uint64(0)
	for _, child := range children {
		if child.TimedOut {
			return false
		}
		estimate += child.Millis - min(child.Millis, fixed)
		series += child.Series
		bytes += child.Bytes
	}
	return 2*estimate < rules.TargetMillis && series <= rules.HalfSeries && bytes <= rules.HalfBytes
}

func median64(children []LeafRead) uint64 {
	times := make([]uint64, 0, len(children))
	for _, child := range children {
		times = append(times, child.Millis)
	}
	slices.Sort(times)
	return times[len(times)/2]
}

// foldInto is split with its children at the positions folded taken back
// into its rest: the rest no longer excepts what each of them held - the
// prefix it was cut at and the character - and excepts again what each
// excepted within it, which other leaves hold. The split is kept from now
// on, its children no longer the ones it started in.
func foldInto(split shapeSplit, folded []int) shapeSplit {
	out := shapeSplit{parent: split.parent, kind: split.kind}
	if out.kind == splitStarting || out.kind == splitRead {
		out.kind = splitKept
	}
	rest := slices.Clone(split.children[len(split.children)-1])
	last := &rest[len(rest)-1]
	excepted := last.excepted()
	for position, child := range split.children[:len(split.children)-1] {
		if !slices.Contains(folded, position) {
			out.children = append(out.children, child)
			continue
		}
		cut := child[len(child)-1]
		head := cut.prefix[len(last.prefix):]
		excepted = slices.DeleteFunc(excepted, func(each string) bool { return each == head })
		for _, within := range cut.excepted() {
			excepted = append(excepted, head+within)
		}
	}
	last.except = ""
	if len(excepted) > 0 {
		last.except = exceptOf(excepted)
	}
	out.children = append(out.children, rest)
	return out
}
