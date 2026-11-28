// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"errors"
	"maps"
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// shapeTree is the tree a Slot starts in from its Query Group's shape, nil
// when it has none or the shape no longer holds: the query revision it was
// read at changed, it cuts on other dimensions than the Query Group is
// sliced on now, it has more leaves than a starting tree may
// (inputslice.MaxLeaves-3, as sliceReadingFor bounds a count), or its leaves
// are not a partition of every value (inputslice.FromShape). A shape that
// does not hold is dropped from the memory as it is found, and counted once,
// by why, on the Slot that found it; the Slot starts in slices counted as
// though there had been none. A Slot read in a shape is not cut further
// before it is read for the Query Group's size (slicesBySeries, its bytes):
// a Query Group grown since is caught by its slices split for their size as
// they stream, and by a slice split for its time once a round. A shape whose
// Slot was refused for its leaves is dropped too, by the refusal
// (voidRefusedShape).
func (coordinator *SlotExecutionCoordinator) shapeTree(request execution.SlotExecutionRequest, last slicedReading, reading *sliceReading) *inputslice.Tree {
	if last.shape.IsZero() {
		return nil
	}
	dropped := ""
	switch {
	case last.revision != request.Contract.QueryRevision:
		dropped = observability.InputSliceShapeVoidedRevision
	case !slices.Equal(last.shape.Dimensions(), last.dimensions):
		dropped = observability.InputSliceShapeVoidedDimensions
	case last.shape.Leaves() > inputslice.MaxLeaves-3:
		dropped = observability.InputSliceShapeVoidedMaxLeaves
	default:
		tree, err := inputslice.FromShape(last.dimensions, last.shape)
		if err == nil {
			reading.shape.started = true
			if last.seriesMicros > 0 {
				reading.leafPlanned = make(map[string]uint64, len(last.leafSeries))
				for digest, series := range last.leafSeries {
					reading.leafPlanned[digest] = series * last.seriesMicros / 1000
				}
			}
			return tree
		}
		dropped = observability.InputSliceShapeRefusedNotAPartition
		var refused *inputslice.ShapeRefusedError
		if errors.As(err, &refused) {
			dropped = shapeRefusals[refused.Reason]
		}
	}
	reading.shape.dropped = dropped
	coordinator.slicedInput.dropShape(request.Contract.Slot.QueryGroup)
	return nil
}

// shapeRefusals is the outcome each reason FromShape refuses a shape for is
// counted as. A shape cut on other dimensions is found before it is built
// (shapeTree), and counted as voided.
var shapeRefusals = map[string]string{
	inputslice.ShapeRefusedDimensions:    observability.InputSliceShapeVoidedDimensions,
	inputslice.ShapeRefusedNotALeaf:      observability.InputSliceShapeRefusedNotALeaf,
	inputslice.ShapeRefusedNotAPartition: observability.InputSliceShapeRefusedNotAPartition,
	inputslice.ShapeRefusedMaxLeaves:     observability.InputSliceShapeRefusedMaxLeaves,
}

// shapeAfter is the shape a completed Slot leaves its Query Group, and the
// quick rounds it has had. A Slot that split a slice for its time keeps that
// split, its quick rounds starting again. A Slot read in a shape and not
// split so keeps it as it was - a slice split for its size is the round's,
// and is asked again from its bytes - and after sliceDecayRounds rounds in a
// row that read every leaf its last kept split made whole and under half a
// slice's target, that split is taken back: its leaves are one again
// (inputslice.Shape.Merged), the leaves of the splits before it kept, so
// the slice that ran out of time is not read whole again while it is still
// slow. Merging a shape's every kept split back leaves the Query Group
// counted in slices, as before its first. A Slot read whole, or that is no
// longer sliceable on the dimensions its shape cut (sliceable false, or
// cut on others), leaves none.
func (coordinator *SlotExecutionCoordinator) shapeAfter(reading *sliceReading, before slicedReading, current []string, sliceable bool) (inputslice.Shape, int) {
	if reading.tree == nil || reading.unsliceable {
		return inputslice.Shape{}, 0
	}
	shape := reading.tree.Shape()
	if shape.IsZero() {
		return inputslice.Shape{}, 0
	}
	if !sliceable || !slices.Equal(current, shape.Dimensions()) {
		reading.shape.dropped = observability.InputSliceShapeVoidedDimensions
		return inputslice.Shape{}, 0
	}
	if reading.timeoutSplit {
		reading.shape.kept = true
		return shape, 0
	}
	// A leaf this round kept split for its size (keepSizedSplit) may be a
	// leaf of the last split kept for time, read quick but large: taking
	// that split back would leave the later one a split of no leaf. Nothing
	// is taken back by time the round a split is kept, and the count starts
	// again.
	if reading.sizedKept {
		return shape, 0
	}
	// A shape whose kept splits are all for a slice's size has nothing to
	// take back by time: sizedDecay takes those back by their series.
	lastKept := reading.tree.LastKept()
	if len(lastKept) == 0 {
		return shape, 0
	}
	quick := true
	for _, leaf := range lastKept {
		took, read := reading.leafMillis[leaf]
		quick = quick && read && took < sliceQueryTargetMillis/2
	}
	if !quick {
		return shape, 0
	}
	if rounds := before.shapeQuickRounds + 1; rounds < sliceDecayRounds {
		return shape, rounds
	}
	return reading.merged(shape.Merged())
}

// shapeAfterPrefix is shapeAfter for a tree cut by prefix
// (inputslice.Tree.ByPrefix): the shape its Query Group's next Slot starts
// in and the history it carries, the tree judged on the leaves this Slot read
// whole (inputslice.Tree.Next, 015 C2 section 4.3) - a leaf read slower than
// sliceTimeTargetMillis split a level deeper at its values' common prefix, a
// split that did not help taken back and its node held, a child read as an
// empty slice for sliceDecayRounds rounds folded into its rest, a kept split
// whose leaves together read under half the target and hold under half a
// slice's series and bytes for sliceDecayRounds rounds taken back. A slice
// the provider timed out and this Slot split (splitsOnTimeout) gives its time
// under its own leaf, what its split is judged against the round after. A
// Slot no longer sliceable on the dimensions its shape cut leaves none.
func (coordinator *SlotExecutionCoordinator) shapeAfterPrefix(reading *sliceReading, before slicedReading, current []string, sliceable bool, cooldown bool) (inputslice.Shape, inputslice.History) {
	if reading.timeoutSplit {
		reading.shape.kept = true
	}
	if cooldown {
		// Under the query cooldown the shape is held as it was read in: a
		// backend the cooldown protects is not split for its time, and the
		// slices it timed out are asked again as they were (deferred).
		shape := reading.tree.Shape()
		if shape.IsZero() || !sliceable || !slices.Equal(current, shape.Dimensions()) {
			return inputslice.Shape{}, nil
		}
		return shape, before.history
	}
	reading.compactForNext()
	reads := make(map[int]inputslice.LeafRead, len(reading.leafMillis)+len(reading.timedOutSplits))
	for leaf, took := range reading.leafMillis {
		reads[leaf] = inputslice.LeafRead{Millis: took, Series: reading.leafSeriesByID[leaf], Bytes: reading.leafBytesByID[leaf],
			Bounds: reading.slowBounds[leaf], TimedOut: reading.timedOutLeaves[leaf]}
	}
	// A slice split for a timeout is judged by the time it ran out after.
	maps.Copy(reads, reading.timeoutSplitReads())
	limit := coordinator.wholeSliceLimit(max(reading.consumers, 1))
	shape, history, counts := reading.tree.Next(reads, before.history, inputslice.Rules{
		TargetMillis: sliceTimeTargetMillis, Rounds: sliceDecayRounds,
		HalfSeries: sizedSplitSeries() / 2, HalfBytes: limit / 2,
	})
	reading.shape.next = counts
	if shape.IsZero() {
		// Every split taken back: the next Slot reads the Query Group whole,
		// or in the slices its size or a timeout starts it in, and a Query
		// Group read whole is not split for its time - only a tree's leaves
		// are judged here - so a mark on its root has nothing to hold, and
		// the history goes with the shape.
		return inputslice.Shape{}, nil
	}
	if !sliceable || !slices.Equal(current, shape.Dimensions()) {
		reading.shape.dropped = observability.InputSliceShapeVoidedDimensions
		return inputslice.Shape{}, nil
	}
	return shape, history
}

// merged is the shape a split taken back leaves, counted merged; or, when
// the shape would not rebuild without it, the shape as it was, kept and
// counted merge_refused, rather than every split in it lost.
func (reading *sliceReading) merged(shape inputslice.Shape, rebuilt bool) (inputslice.Shape, int) {
	if !rebuilt {
		reading.shape.mergeRefused = true
		return shape, 0
	}
	reading.shape.merged = true
	return shape, 0
}

// sizedDecay is the shape a completed Slot leaves after its last split kept
// for a slice's size (inputslice.Tree.SplitSized, KeepSizedSplit), and the
// rounds it has counted towards taking it back. The split is taken back once
// sliceDecayRounds reads in a row read every leaf it made, together at most
// half a slice's series and at most half the input a slice may retain
// (limit, wholeSliceLimit): merged, the slice is well under a slice's size
// either way, and a split taken back sooner, or for less, would be made again
// the round after - a split for the bytes of a few large series, its series
// well under a slice's, would be taken back to be let go for its bytes again
// every few rounds. A part that rose back over half either starts the count
// again. A
// round that has not read every leaf of it counts nothing yet, and one that
// kept a split this round starts the count again: its leaves are the next
// round's.
func (reading *sliceReading) sizedDecay(shape inputslice.Shape, before slicedReading, limit uint64) (inputslice.Shape, int) {
	if shape.IsZero() || reading.tree == nil || reading.sizedKept || reading.sizedSplit {
		return shape, 0
	}
	leaves := reading.tree.LastSized()
	if len(leaves) == 0 {
		return shape, 0
	}
	together, retained := uint64(0), uint64(0)
	for _, leaf := range leaves {
		series, read := reading.leafSeriesByID[leaf]
		if !read {
			return shape, 0
		}
		together += series
		retained += reading.leafBytesByID[leaf]
	}
	if together > sizedSplitSeries()/2 || retained > limit/2 {
		return shape, 0
	}
	if rounds := before.shapeSizedRounds + 1; rounds < sliceDecayRounds {
		return shape, rounds
	}
	return reading.merged(shape.MergedSized())
}

// keepTimedOutSplit splits the slice whose read ran out of time and failed
// the Slot, when the provider said it ran out (timedOutByProvider: a read
// alarmd's deadline ended is the window's) and its time is put on its size
// (timedOutForSize, as a slice is split and read again for it), by the sample of what it delivered before it
// ran out, the fixed alphabet with none: whether it was not split - the
// cooldown held, the window left was too short - or was split and a child of
// it ran out too. The split is the tree's alone; the Slot is failing, and
// what its stream holds is let go with it.
//
// Judged under the cooldown as a split now is (cooldownHeld): while it holds,
// a slice with no other slice read before it is not put on its size, and a
// Query Group failing so is not made a level deeper each round. Before it
// holds - the scheduler holds it after unavailableThreshold (3) queries in
// a row unavailable - a Slot that fails so splits its shape twice at most:
// the slice split and read again (splitsOnTimeout), and the slice the Slot
// then fails on, split here - another leaf, as children are read after the
// leaves made before them. Three such Slots are six splits of at most
// classesPerSplit+1 (5) leaves: 24 leaves more, and a shape past a starting
// tree's leaves (inputslice.MaxLeaves-3) is dropped.
func (stream *streamedExecution) keepTimedOutSplit(leaf inputslice.Leaf) {
	reading := stream.slices
	if reading.tree == nil || reading.unsliceable || !reading.timedOutByProvider ||
		!reading.timedOutForSize(&stream.slice, reading.timedOutTook, stream.cooldownHeld()) {
		return
	}
	reading.treeMu.Lock()
	defer reading.treeMu.Unlock()
	_, err := reading.tree.SplitKept(leaf.ID, stream.slice.sample)
	if err != nil && reading.makeRoom(err) {
		_, err = reading.tree.SplitKept(leaf.ID, stream.slice.sample)
	}
	if err == nil {
		reading.keptOnFailure = true
		return
	}
	// No room in a tree cut by prefix: the slice is read last the Slot
	// after, every other one read and foldable by then.
	if word, _ := InputSliceRefusal(err); word == contract.ReasonInputSliceLeavesExhausted && reading.byPrefix() {
		reading.deferLeaf(leaf)
	}
}

// timeoutSplitReads is, by the ID of each slice the provider timed out and
// this Slot split for it in place (splitsOnTimeout), the time it ran out
// after: what a split of it is judged against the round after.
func (reading *sliceReading) timeoutSplitReads() map[int]inputslice.LeafRead {
	reads := make(map[int]inputslice.LeafRead, len(reading.leafMillis)+len(reading.timedOutSplits))
	for leaf, took := range reading.timedOutSplits {
		reads[leaf] = inputslice.LeafRead{Millis: took}
	}
	return reads
}

// shapeOnFailure is the shape a Slot that failed with a slice split for its
// size leaves its Query Group: its tree's, brought within the leaves a Slot
// starts in when it is cut by prefix (compactForNext), held to FromShape as
// every shape read in is, none when it does not hold.
func (coordinator *SlotExecutionCoordinator) shapeOnFailure(reading *sliceReading) inputslice.Shape {
	reading.compactForNext()
	shape := reading.tree.Shape()
	if _, err := inputslice.FromShape(shape.Dimensions(), shape); err != nil {
		return inputslice.Shape{}
	}
	return shape
}

// forget takes everything the memory holds of a Query Group out of it - its
// shape, cut, slice count and times - so its next Slot reads it whole.
func (trees *sliceTrees) forget(group execution.QueryGroupIdentity) {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	delete(trees.input, group)
}

// dropShape takes a Query Group's shape out of the memory, and the history of
// its nodes with it, what else it holds kept.
func (trees *sliceTrees) dropShape(group execution.QueryGroupIdentity) {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	if reading, ok := trees.input[group]; ok {
		reading.shape, reading.shapeQuickRounds, reading.history = inputslice.Shape{}, 0, nil
		trees.input[group] = reading
	}
}

// voidRefusedShape drops the shape a Slot started in once the Slot is
// refused INPUT_SLICE_LEAVES_EXHAUSTED: a slice of it let go and split past
// the leaves a tree may have (inputslice.MaxLeaves), its shape near the
// leaves a Slot may start in. The refusal is final and leaves the memory as
// it was - the Slot never reaches rememberSlotInput - so the next Slot would
// start in the same leaves, let the same slice go and be refused again, every
// round, until the process ends. Dropped, the next Slot starts in slices
// counted, as one whose shape grew past a starting tree's leaves does, and is
// split from there. The refused Slot reports no usage: the drop is counted
// voided_max_leaves by the next Slot to complete, as evicted_idle is
// (takeVoided). A Slot not started in a shape, or refused otherwise, drops
// nothing.
func (coordinator *SlotExecutionCoordinator) voidRefusedShape(group execution.QueryGroupIdentity, reading *sliceReading, err error) {
	if reading == nil || !reading.shape.started {
		return
	}
	if word, _ := InputSliceRefusal(err); word != contract.ReasonInputSliceLeavesExhausted {
		return
	}
	coordinator.slicedInput.voidShape(group)
}

// voidShape takes a Query Group's shape out of the memory as dropShape does,
// and counts it for the next Slot to complete (takeVoided).
func (trees *sliceTrees) voidShape(group execution.QueryGroupIdentity) {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	reading, ok := trees.input[group]
	if !ok || reading.shape.IsZero() {
		return
	}
	reading.shape, reading.shapeQuickRounds, reading.shapeSizedRounds, reading.history = inputslice.Shape{}, 0, 0, nil
	trees.input[group] = reading
	trees.voided++
}

// takeVoided is the shapes refused Slots dropped not yet counted, now
// counted, as takeEvicted counts the Query Groups dropped as idle.
func (trees *sliceTrees) takeVoided() uint64 {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	voided := trees.voided
	trees.voided = 0
	return voided
}

// sliceShapeOutcomes is this Slot's shape usage as the counts of
// observability.InputSliceShapeOutcomes, in that order.
func (stream *streamedExecution) sliceShapeOutcomes() [len(observability.InputSliceShapeOutcomes)]uint64 {
	var counts [len(observability.InputSliceShapeOutcomes)]uint64
	if !stream.slicing() {
		return counts
	}
	usage := stream.slices.shape
	for index, outcome := range observability.InputSliceShapeOutcomes {
		switch outcome {
		case observability.InputSliceShapeEvictedIdle:
			counts[index] = stream.coordinator.slicedInput.takeEvicted()
			continue
		case observability.InputSliceShapeVoidedMaxLeaves:
			// The shapes of Slots refused for their leaves (voidRefusedShape),
			// beside this Slot's own.
			counts[index] = stream.coordinator.slicedInput.takeVoided()
		}
		// The round's judgement of a shape cut by prefix counts (Next).
		switch outcome {
		case observability.InputSliceShapeMerged:
			counts[index] += usage.next.Merged
		case observability.InputSliceShapeMergeRefused:
			counts[index] += usage.next.MergeRefused
		case observability.InputSliceShapeRefusedSingleValue:
			counts[index] += usage.next.Concentrated
		case observability.InputSliceShapeRefusedCondition:
			counts[index] += usage.next.ConditionBudget
		case observability.InputSliceShapeSplitTime:
			counts[index] += usage.next.SplitForTime
		case observability.InputSliceShapeSplitHeldUniform:
			counts[index] += usage.next.HeldUniform
		case observability.InputSliceShapeSplitUnhelpful:
			counts[index] += usage.next.Unhelpful
		case observability.InputSliceShapeSplitHeldMark:
			counts[index] += usage.next.HeldByMark
		case observability.InputSliceShapeFolded:
			counts[index] += usage.next.Folded + stream.slices.roomFolded
		case observability.InputSliceShapeLeafCapReached:
			counts[index] += usage.next.LeafCap
		}
		switch {
		case outcome == observability.InputSliceShapeStarted && usage.started,
			outcome == observability.InputSliceShapeKept && usage.kept,
			outcome == observability.InputSliceShapeMerged && usage.merged,
			outcome == observability.InputSliceShapeMergeRefused && usage.mergeRefused,
			outcome == observability.InputSliceShapeSizedSplit && stream.slices.startDiscarded,
			outcome == observability.InputSliceShapeSizedKept && stream.slices.sizedKept,
			outcome == observability.InputSliceShapeSwitchedToSuffix && usage.switched,
			outcome == usage.dropped:
			counts[index]++
		}
	}
	return counts
}
