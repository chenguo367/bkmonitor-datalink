// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inputslice

import "slices"

// PrefixSplitChildren is the most children a split by prefix makes: every
// alphabet character kept apart, and the rest.
const PrefixSplitChildren = len(alphabet) + 1

// MakeRoom folds leaves of a tree cut by prefix into their rest, this Slot's
// own, until a split of children more fits within the leaves a tree may have,
// and reports how many it folded and whether it fits now (015 C2 section
// 4.8): a slice the provider timed out has to be split a level deeper, and a
// split by prefix is of up to 41 leaves, so a chain of them passes MaxLeaves
// three levels down, well before folds judged over rounds (Next) would give
// leaves back. A leaf folded is one this Slot read whole (reads, by ID, landed)
// within twice the Slot's fastest read - an empty slice's time - and not timed
// out, kept apart by a split that is not the root's, whose rest the Slot read
// whole too: both already read, so the rest grown to hold it reads nothing
// again, and the root's slices, which a retry starts in, stay as they were.
// Neither is a leaf this Slot kept split for its size (KeepSizedSplit): its
// parts were cut from it as it was, and would not cover it grown, nor be of a
// leaf of the shape folded. A split keeps one child apart at least. Nothing
// is folded when the split fits as it is, or under a tree cut from the end.
func (tree *Tree) MakeRoom(children int, reads map[int]LeafRead) (uint64, bool) {
	return tree.makeRoomWithin(children, tree.maxLeaves, 0, reads)
}

// CompactForNext folds leaves of a tree cut by prefix into their rest, as
// MakeRoom does and by its rules, until the tree is within the leaves a Slot
// starts in (MaxLeaves-3), and reports how many it folded and whether it is
// within them now: the shape it leaves the next Slot. A Slot may split past
// them, up to MaxLeaves, to read a slice the provider timed out a level
// deeper; its shape kept so would be dropped as the next Slot starts in it,
// and a Query Group split a level deeper every Slot would start over every
// time and never come out of it (015 C2 section 4.8). The leaves of the
// splits kept for a leaf's size this Slot (KeepSizedSplit) are the shape's
// too, and counted with the tree's.
func (tree *Tree) CompactForNext(reads map[int]LeafRead) (uint64, bool) {
	return tree.makeRoomWithin(1, MaxLeaves-3, tree.laterLeaves(), reads)
}

// makeRoomWithin is MakeRoom with the leaves the tree is to be within, more
// leaves than its own (later) counted beside them.
func (tree *Tree) makeRoomWithin(children, within, later int, reads map[int]LeafRead) (uint64, bool) {
	fits := func() bool { return tree.leaves+later-1+children <= min(tree.maxLeaves, within) }
	if fits() || !tree.ByPrefix() {
		return 0, fits()
	}
	fastest := uint64(0)
	for id, read := range reads {
		if id >= 0 && id < len(tree.nodes) && tree.nodes[id].state == stateLanded && !read.TimedOut &&
			(fastest == 0 || read.Millis < fastest) {
			fastest = read.Millis
		}
	}
	empty := func(id int) bool {
		read, ok := reads[id]
		return ok && tree.nodes[id].state == stateLanded && !read.TimedOut && read.Millis <= 2*fastest && !tree.keptLater(id)
	}
	var folded uint64
	// The deepest splits first: the latest made, under the slices the chain
	// went down to.
	for index := len(tree.splits) - 1; index >= 0 && !fits(); index-- {
		split := &tree.splits[index]
		if split.parent == 0 || len(split.children) < 3 {
			continue
		}
		restID := split.children[len(split.children)-1]
		rest := tree.nodes[restID]
		last := rest.segments[len(rest.segments)-1]
		if _, read := reads[restID]; !last.byPrefix || !read || rest.state != stateLanded || tree.keptLater(restID) {
			continue
		}
		excepted := last.excepted()
		apart := split.children[:len(split.children)-1]
		kept := make([]int, 0, len(apart)+1)
		here := 0
		for _, id := range apart {
			child := tree.nodes[id]
			cut := child.segments[len(child.segments)-1]
			if fits() || len(apart)-here <= 1 || !empty(id) || !cut.byPrefix || len(child.segments) != len(rest.segments) {
				kept = append(kept, id)
				continue
			}
			head := cut.prefix[len(last.prefix):]
			excepted = slices.DeleteFunc(excepted, func(each string) bool { return each == head })
			for _, within := range cut.excepted() {
				excepted = append(excepted, head+within)
			}
			child.state = stateDiscarded
			tree.leaves--
			here++
		}
		if here == 0 {
			continue
		}
		folded += uint64(here)
		last.except = ""
		if len(excepted) > 0 {
			last.except = exceptOf(excepted)
		}
		segments := slices.Clone(rest.segments)
		segments[len(segments)-1] = last
		rest.segments, rest.slice = segments, sliceOf(segments, tree.dimensions)
		split.children = append(kept, restID)
		if split.kind == splitStarting || split.kind == splitRead {
			split.kind = splitKept
		}
	}
	return folded, fits()
}

// laterLeaves is the leaves the splits kept for a leaf's size this Slot
// (KeepSizedSplit) add to the shape: each its parts less the leaf.
func (tree *Tree) laterLeaves() int {
	leaves := 0
	for _, later := range tree.later {
		leaves += len(later.children) - 1
	}
	return leaves
}

// keptLater reports whether this Slot kept the leaf split for its size
// (KeepSizedSplit).
func (tree *Tree) keptLater(id int) bool {
	for _, later := range tree.later {
		if later.node == id {
			return true
		}
	}
	return false
}
