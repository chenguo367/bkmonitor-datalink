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
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// hostSample is n hosts host-000.. each carrying per series: a slice's sample
// as the stream gave it.
func hostSample(n, per int) inputslice.Sample {
	values := make([]inputslice.SampledValue, 0, n)
	for index := range n {
		values = append(values, inputslice.SampledValue{Value: fmt.Sprintf("host-%03d", index), Series: per})
	}
	return inputslice.Sample{"host": values}
}

// readWhole is a reading of one leaf, the root, read whole with series and
// sample: what noteLeafRead sees once the slice has landed its last batch.
func readWhole(t *testing.T, series uint64, sample inputslice.Sample) (*sliceReading, *sliceRead, inputslice.Leaf) {
	t.Helper()
	tree, err := inputslice.New([]string{"host"}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	reading := &sliceReading{tree: tree, dimensions: []string{"host"}}
	return reading, &sliceRead{series: series, sample: sample}, tree.Pending()[0]
}

// A slice read whole that held 150,000 series - past the 87,000 a slice is
// cut to - is split for the next Slot, by its sample, into parts each within
// a slice's series, each planned at its share of the 150,000. One of 80,000
// is within a slice and is not split.
func TestASliceReadWholePastASlicesSeriesStartsInItsPartsNext(t *testing.T) {
	reading, slice, leaf := readWhole(t, 150_000, hostSample(150, 1000))
	reading.noteLeafRead(slice, leaf, 20_000)
	next, err := inputslice.FromShape([]string{"host"}, reading.tree.Shape())
	if err != nil {
		t.Fatalf("the shape kept does not rebuild: %v", err)
	}
	parts := next.Pending()
	if len(parts) < 2 {
		t.Fatalf("the next Slot starts in %d slices, want the 150,000 cut in parts", len(parts))
	}
	planned := uint64(0)
	for _, part := range parts {
		series, known := reading.leafSeries[part.Slice.Digest()]
		if !known {
			t.Fatalf("part %s has no planned series", part.Slice.Digest())
		}
		if series > sliceSeries {
			t.Fatalf("part planned at %d series, over a slice's %d", series, sliceSeries)
		}
		planned += series
	}
	if planned > 150_000 || planned+uint64(len(parts)) < 150_000 {
		t.Fatalf("parts planned at %d series together, want the slice's 150,000 less rounding", planned)
	}

	within, slice, leaf := readWhole(t, 80_000, hostSample(80, 1000))
	within.noteLeafRead(slice, leaf, 10_000)
	if !within.tree.Shape().IsZero() {
		t.Fatal("a slice of 80,000 series, within a slice's 87,000, was split for the next Slot")
	}
}

// A split that would leave the next Slot past the leaves it may start in
// (inputslice.MaxLeaves-3) keeps nothing, and the read says its shape was
// refused for its leaves; one of a slice whose sample tells none of its
// series apart - one value of one character, none left after the class the
// slice is cut by - keeps nothing either, and is refused for that, not for
// the leaves. Any split of the tree started in MaxLeaves-3 leaves adds one
// at least; the one slice is cut by both samples.
func TestASizedSplitPastTheStartingLeavesIsRefused(t *testing.T) {
	for _, test := range []struct {
		sample inputslice.Sample
		want   string
	}{{hostSample(150, 1000), observability.InputSliceShapeRefusedMaxLeaves}, {inputslice.Sample{"host": {{Value: "x", Series: 150_000}}}, observability.InputSliceShapeRefusedSingleValue}} {
		tree, err := inputslice.New([]string{"host"}, inputslice.MaxLeaves-3, "host")
		if err != nil {
			t.Fatal(err)
		}
		reading := &sliceReading{tree: tree, dimensions: []string{"host"}}
		reading.noteLeafRead(&sliceRead{series: 150_000, sample: test.sample}, tree.Pending()[1], 20_000)
		if reading.sizedKept || reading.shape.dropped != test.want {
			t.Fatalf("kept %v, shape %q: want nothing kept, refused %s", reading.sizedKept, reading.shape.dropped, test.want)
		}
	}
}

// The split is taken back after three reads in a row of all its parts under
// half a slice's series together, 43,500, and under half the input a slice
// may retain: the first two count, the third merges. A part that rises past
// either the second round starts the count again - parts of a few series
// that retain as much as the slice let go for its bytes are not merged, to
// be let go for them again - and a round that did not read every part counts
// nothing.
func TestASizedSplitIsTakenBackAfterThreeQuietRoundsOnly(t *testing.T) {
	cut, slice, leaf := readWhole(t, 150_000, hostSample(150, 1000))
	cut.noteLeafRead(slice, leaf, 20_000)
	shape := cut.tree.Shape()
	const limit = 384 << 10
	partBytes := uint64(0)
	round := func(perPart uint64, skipOne bool) (inputslice.Shape, int, *sliceReading) {
		tree, err := inputslice.FromShape([]string{"host"}, shape)
		if err != nil {
			t.Fatal(err)
		}
		reading := &sliceReading{tree: tree, leafSeriesByID: map[int]uint64{}, leafBytesByID: map[int]uint64{}}
		for index, part := range tree.LastSized() {
			if skipOne && index == 0 {
				continue
			}
			reading.leafSeriesByID[part] = perPart
			reading.leafBytesByID[part] = partBytes
		}
		return shape, len(tree.LastSized()), reading
	}
	_, parts, _ := round(0, false)
	quiet := uint64(sliceSeries/2) / uint64(parts)
	before := slicedReading{}
	for count := 1; count <= 2; count++ {
		_, _, reading := round(quiet, false)
		got, rounds := reading.sizedDecay(shape, before, limit)
		if rounds != count || got.Leaves() != shape.Leaves() {
			t.Fatalf("quiet round %d: %d rounds, %d leaves; want %d counted and the split kept", count, rounds, got.Leaves(), count)
		}
		before.shapeSizedRounds = rounds
	}
	_, _, reading := round(quiet, false)
	if got, rounds := reading.sizedDecay(shape, before, limit); rounds != 0 || !got.IsZero() || !reading.shape.merged {
		t.Fatalf("third quiet round: %d rounds, shape of %d leaves, merged %v; want the split taken back", rounds, got.Leaves(), reading.shape.merged)
	}

	before = slicedReading{shapeSizedRounds: 1}
	_, _, risen := round(quiet+uint64(sliceSeries)/uint64(parts), false)
	if _, rounds := risen.sizedDecay(shape, before, limit); rounds != 0 {
		t.Fatalf("a round past half a slice counted %d, want the count started again", rounds)
	}
	_, _, partial := round(quiet, true)
	if _, rounds := partial.sizedDecay(shape, before, limit); rounds != 0 {
		t.Fatalf("a round that read not every part counted %d, want nothing", rounds)
	}
	// Few series, and together the bytes of the slice let go for them.
	partBytes = limit/uint64(parts) + 1
	_, _, heavy := round(quiet, false)
	if _, rounds := heavy.sizedDecay(shape, before, limit); rounds != 0 {
		t.Fatalf("parts retaining past half a slice's input counted %d, want the count started again", rounds)
	}
}

// The sample keeps the values of the smallest hash, and every value it keeps
// is counted from its first series: each sampled value's series equal the
// stream's, value by value. A stream past the sample's size, in any order,
// gives the same values.
func TestTheSampleKeepsExactCountsOfTheValuesItKeeps(t *testing.T) {
	const distinct = 3 * sliceSampleValues
	stream := make([]string, 0, distinct*4)
	full := map[string]int{}
	for index := range distinct {
		value := fmt.Sprintf("node-%05d", index)
		// Four in ten a heavy value, the rest one series each.
		times := 1
		if index%10 < 4 {
			times = 4
		}
		for range times {
			stream = append(stream, value)
		}
		full[value] += times
	}
	sampleOf := func(order []string) inputslice.Sample {
		sample, seen, keep := inputslice.Sample{}, map[string]int{}, &sampleKeep{}
		for _, value := range order {
			keep.note(&sample, "host", seen, value)
		}
		return sample
	}
	forward := sampleOf(stream)
	if len(forward["host"]) != sliceSampleValues {
		t.Fatalf("sample holds %d values, want %d", len(forward["host"]), sliceSampleValues)
	}
	kept := map[string]bool{}
	for _, value := range forward["host"] {
		if value.Series != full[value.Value] {
			t.Fatalf("%s counted %d series, the stream carried %d", value.Value, value.Series, full[value.Value])
		}
		kept[value.Value] = true
	}
	reversed := make([]string, len(stream))
	for index, value := range stream {
		reversed[len(stream)-1-index] = value
	}
	for _, value := range sampleOf(reversed)["host"] {
		if !kept[value.Value] {
			t.Fatalf("the reversed stream sampled %s, which the forward one did not", value.Value)
		}
	}
}

// A split kept for time whose leaves read quick two rounds would be taken
// back on the third; the round one of its leaves, read quick, held past a
// slice's series and was kept split for its size, it is not: taken back,
// the later split would be of a leaf no longer there, and the shape lost
// whole - every split in it - to be read whole and let go again. The
// leaves are noted as the Slot lands them, then the shape judged, in the
// order a completed Slot takes them (coordinator.go, rememberSlotInput).
func TestATimeMergeTheRoundALeafOfItIsKeptSplitKeepsEverySplit(t *testing.T) {
	dimensions := []string{"host"}
	start, err := inputslice.New(dimensions, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := start.SplitKept(start.Pending()[0].ID, hostSample(150, 1)); err != nil {
		t.Fatal(err)
	}
	tree, err := inputslice.FromShape(dimensions, start.Shape())
	if err != nil {
		t.Fatal(err)
	}
	kept := tree.LastKept()
	reading := &sliceReading{tree: tree, dimensions: dimensions}
	slice := &sliceRead{sample: hostSample(150, 1000)}
	for _, leaf := range tree.Pending() {
		slice.series = 10
		if leaf.ID == kept[0] {
			slice.series = 150_000
		}
		reading.noteLeafRead(slice, leaf, 1)
	}
	if !reading.sizedKept {
		t.Fatal("the leaf past a slice's series was not kept split")
	}
	want := reading.tree.Shape()
	before := slicedReading{shapeQuickRounds: sliceDecayRounds - 1}
	shape, _ := (&SlotExecutionCoordinator{}).shapeAfter(reading, before, dimensions, true)
	if shape.IsZero() || shape.Leaves() != want.Leaves() || shape.Leaves() <= len(kept) {
		t.Fatalf("shape after the round of %d leaves, want both splits kept, %d leaves", shape.Leaves(), want.Leaves())
	}
	if _, err := inputslice.FromShape(dimensions, shape); err != nil {
		t.Fatalf("the shape kept does not rebuild: %v", err)
	}
	if reading.shape.merged || reading.shape.mergeRefused {
		t.Fatalf("merged %v merge refused %v, want nothing taken back the round a split was kept",
			reading.shape.merged, reading.shape.mergeRefused)
	}
}

// A slice the Slot started in and split for its size this round makes the
// last split for size: its count starts at its own first round, not on from
// the two the split before it had counted, which would take it back the
// round it was made - to be let go again the round after.
func TestASizedSplitMadeThisRoundStartsItsOwnCount(t *testing.T) {
	cut, slice, leaf := readWhole(t, 150_000, hostSample(150, 1000))
	cut.noteLeafRead(slice, leaf, 20_000)
	shape := cut.tree.Shape()
	tree, err := inputslice.FromShape([]string{"host"}, shape)
	if err != nil {
		t.Fatal(err)
	}
	reading := &sliceReading{tree: tree, leafSeriesByID: map[int]uint64{}, leafBytesByID: map[int]uint64{}, sizedSplit: true}
	for _, part := range tree.LastSized() {
		reading.leafSeriesByID[part] = 1
	}
	before := slicedReading{shapeSizedRounds: sliceDecayRounds - 1}
	got, rounds := reading.sizedDecay(shape, before, 384<<10)
	if rounds != 0 || got.Leaves() != shape.Leaves() || reading.shape.merged {
		t.Fatalf("%d rounds, %d leaves, merged %v: want the split made this round kept, its count from zero",
			rounds, got.Leaves(), reading.shape.merged)
	}
}

// A split kept for a leaf's size that keeps nothing is named by why: past
// the leaves a Slot starts in, past the condition budget, or failed
// otherwise - none passed over unnamed.
func TestASizedSplitRefusedIsNamedByItsReason(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{&inputslice.ValueConcentratedError{Reason: inputslice.ReasonMaxLeaves}, observability.InputSliceShapeRefusedMaxLeaves},
		{&inputslice.ValueConcentratedError{Reason: inputslice.ReasonConditionBudget}, observability.InputSliceShapeRefusedCondition},
		{&inputslice.ValueConcentratedError{Reason: inputslice.ReasonSingleValue}, observability.InputSliceShapeRefusedSingleValue},
		{&inputslice.ValueConcentratedError{Reason: "another"}, observability.InputSliceShapeRefusedSplitError},
		{fmt.Errorf("wrapped: %w", &inputslice.ValueConcentratedError{Reason: inputslice.ReasonConditionBudget}), observability.InputSliceShapeRefusedCondition},
		{errors.New("alarmd inputslice: slice 3 already kept split"), observability.InputSliceShapeRefusedSplitError},
	} {
		if got := sizedSplitRefusal(test.err); got != test.want {
			t.Fatalf("%v named %q, want %q", test.err, got, test.want)
		}
	}
}
