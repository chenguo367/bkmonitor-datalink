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
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// The rules a tree cut by prefix is judged by in these cases: the worker's
// (015 C2 section 4.3), a merge's series and bytes no bound.
var prefixRules = inputslice.Rules{TargetMillis: 3_000, Rounds: 3, HalfSeries: 1 << 40, HalfBytes: 1 << 40}

// backend is what a read of a slice takes: a fixed cost and a cost a series
// of the slice, as a store reads a slice by its prefix (015 C2 section 4.2),
// or the same whatever the slice holds (flat), a backend slow as a whole.
type backend struct {
	fixed, perSeries uint64
	flat             uint64
	// timeoutAt, set, has a read of a slice of that many series or more
	// time out at the provider: it completes degraded, its time the
	// provider's limit (timedOutMillis), and says nothing of its size.
	timeoutAt int
}

// timedOutMillis is a read the provider timed out: its limit.
const timedOutMillis = 30_000

func (b backend) millis(series int) uint64 {
	if b.flat > 0 {
		return b.flat
	}
	return b.fixed + b.perSeries*uint64(series)
}

// boundsOf is the least and greatest value of each dimension of in, a
// missing label as the empty value, as the worker keeps them for a slice
// read whole.
func boundsOf(dimensions []string, in []series) inputslice.Bounds {
	if len(in) == 0 {
		return nil
	}
	bounds := inputslice.Bounds{}
	for _, dimension := range dimensions {
		for index, values := range in {
			value := values[dimension]
			bound, known := bounds[dimension]
			switch {
			case index == 0 || !known:
				bounds[dimension] = inputslice.Bound{Low: value, High: value}
			case value < bound.Low:
				bound.Low = value
				bounds[dimension] = bound
			case value > bound.High:
				bound.High = value
				bounds[dimension] = bound
			}
		}
	}
	return bounds
}

// readWhole reads every pending leaf of tree whole on b, lands it, and gives
// the reads Next judges, the bounds of each slice when withBounds - a slice
// that delivered its series - and none when not, a read whose aggregation
// delivered nothing.
func readWhole(t *testing.T, tree *inputslice.Tree, dimensions []string, population []series, b backend, withBounds bool) map[int]inputslice.LeafRead {
	t.Helper()
	reads := map[int]inputslice.LeafRead{}
	for _, leaf := range tree.Pending() {
		in := members(t, leaf.Slice, population)
		read := inputslice.LeafRead{Millis: b.millis(len(in)), Series: uint64(len(in))}
		if b.timeoutAt > 0 && len(in) >= b.timeoutAt {
			read = inputslice.LeafRead{Millis: timedOutMillis, TimedOut: true}
		}
		if withBounds {
			read.Bounds = boundsOf(dimensions, in)
		}
		if err := tree.Land(leaf.ID); err != nil {
			t.Fatal(err)
		}
		reads[leaf.ID] = read
	}
	return reads
}

// round is one round of a Query Group read in a tree cut by prefix: its tree,
// built from the shape the round before left (start when none), read whole,
// judged.
type round struct {
	leaves  int
	slowest uint64
	counts  inputslice.NextCounts
}

type rounds struct {
	t          *testing.T
	dimensions []string
	population []series
	probe      []series
	start      func() *inputslice.Tree
	shape      inputslice.Shape
	history    inputslice.History
	withBounds bool
}

func (rs *rounds) next(b backend) round {
	rs.t.Helper()
	tree := rs.start()
	if !rs.shape.IsZero() {
		built, err := inputslice.FromShape(rs.dimensions, rs.shape)
		if err != nil {
			rs.t.Fatalf("the shape the round before left does not rebuild: %v", err)
		}
		tree = built
	}
	assertPartition(rs.t, leavesOf(tree), rs.probe)
	reads := readWhole(rs.t, tree, rs.dimensions, rs.population, b, rs.withBounds)
	var slowest uint64
	for _, read := range reads {
		slowest = max(slowest, read.Millis)
	}
	leaves := tree.Leaves()
	var counts inputslice.NextCounts
	rs.shape, rs.history, counts = tree.Next(reads, rs.history, prefixRules)
	if !rs.shape.IsZero() {
		built, err := inputslice.FromShape(rs.dimensions, rs.shape)
		if err != nil {
			rs.t.Fatalf("the shape Next left does not rebuild: %v", err)
		}
		if built.Leaves() > inputslice.MaxLeaves-3 {
			rs.t.Fatalf("Next left %d leaves, past the %d a Slot starts in", built.Leaves(), inputslice.MaxLeaves-3)
		}
		assertPartition(rs.t, leavesOf(built), rs.probe)
	}
	return round{leaves: leaves, slowest: slowest, counts: counts}
}

func rootOnly(t *testing.T, dimensions []string) func() *inputslice.Tree {
	return func() *inputslice.Tree {
		tree, err := inputslice.New(dimensions, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		return tree
	}
}

// awkward is values no prefix of the alphabet holds past their first
// characters, and the empty value and the missing label, every one of
// which has to be in exactly one leaf of every tree.
func awkward(dimension string) []series {
	var out []series
	for _, value := range []string{"", "A1", "Z", "a/b", "a b", "中文", "\xff\xfe", "a\n", "5", "54", "5\x00", "-", ".x", "_", ":9", "é5"} {
		out = append(out, series{dimension: value})
	}
	return append(out, series{})
}

// A tree cut by prefix writes its leaves as literal prefixes, anchored at both
// ends, and the rest of a split as the non-match of what it keeps apart: a
// class when each is one character, its hyphen escaped, its digits and
// letters as ranges.
func TestAPrefixSplitWritesLiteralPrefixes(t *testing.T) {
	tree, err := inputslice.NewCut([]string{"pod"}, 2, inputslice.FixedCut("pod"))
	if err != nil {
		t.Fatal(err)
	}
	pending := tree.Pending()
	if got, want := len(pending), len("-.0123456789:_abcdefghijklmnopqrstuvwxyz")+1; got != want {
		t.Fatalf("the root split into %d, want %d", got, want)
	}
	for index, want := range map[int]execution.InputSliceCondition{
		0:  {Dimension: "pod", Pattern: "^-(?s:.*)$"},
		1:  {Dimension: "pod", Pattern: `^\.(?s:.*)$`},
		2:  {Dimension: "pod", Pattern: "^0(?s:.*)$"},
		13: {Dimension: "pod", Pattern: "^_(?s:.*)$"},
		39: {Dimension: "pod", Pattern: "^z(?s:.*)$"},
		40: {Dimension: "pod", Negated: true, Pattern: `^[\-.0-9:_a-z](?s:.*)$`},
	} {
		if conditions := pending[index].Slice.Conditions; len(conditions) != 1 || conditions[0] != want {
			t.Errorf("slice %d is %+v, want %+v", index, conditions, want)
		}
	}
	// A child split again keeps its own prefix: its rest is the prefix less
	// what it keeps apart.
	children, err := tree.SplitKept(pending[7].ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	rest := children[len(children)-1].Slice.Conditions
	if len(rest) != 2 || rest[0] != (execution.InputSliceCondition{Dimension: "pod", Pattern: "^5(?s:.*)$"}) ||
		rest[1] != (execution.InputSliceCondition{Dimension: "pod", Negated: true, Pattern: `^5[\-.0-9:_a-z](?s:.*)$`}) {
		t.Fatalf("the rest of 5 is %+v", rest)
	}
	assertPartition(t, leavesOf(tree), append(awkward("pod"), series{"pod": "5a"}, series{"pod": "5"}, series{"pod": "5A"}))
}

// Values with characters past the alphabet - upper-case, separators of no
// address, spaces, other scripts, bytes that are no UTF-8 - and the empty
// value and the missing label are each in exactly one leaf however the tree
// is split and folded, the rest of a split holding what it keeps no child
// for.
func TestValuesPastTheAlphabetAreInTheRest(t *testing.T) {
	dimensions := []string{"pod"}
	var population []series
	for index := 0; index < 400; index++ {
		population = append(population, series{"pod": fmt.Sprintf("5%03d", index)})
	}
	probe := append(append([]series(nil), population...), awkward("pod")...)
	rs := &rounds{t: t, dimensions: dimensions, population: append(population, awkward("pod")...), probe: probe,
		start: rootOnly(t, dimensions), withBounds: true}
	// Slow at first, then fast: the tree is split, its empty children folded
	// into their rest, and its splits taken back.
	for index := 0; index < 12; index++ {
		b := backend{fixed: 300, perSeries: 10}
		if index >= 4 {
			b = backend{fixed: 300}
		}
		rs.next(b)
	}
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("pod"))
	if err != nil {
		t.Fatal(err)
	}
	rest := tree.Pending()[len(tree.Pending())-1]
	for _, values := range awkward("pod") {
		in := members(t, rest.Slice, []series{values})
		first := values["pod"]
		if len(first) > 0 && strings.IndexByte("-.0123456789:_abcdefghijklmnopqrstuvwxyz", first[0]) >= 0 {
			continue
		}
		if len(in) != 1 {
			t.Errorf("value %q is not in the root's rest %s", first, describe([]execution.InputSlice{rest.Slice}))
		}
	}
}

// A slice read whole slower than the target is split at its values' common
// prefix, the least and the greatest value's: device names that share
// "device-" and twenty-one zeros are cut where they differ, in one split,
// and so are addresses that share "10.".
func TestASplitForTimeCutsAtTheValuesCommonPrefix(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  func(int) string
		values int
		at     string
	}{
		{"device names", func(index int) string { return fmt.Sprintf("device-%025d", index) }, 2_000, "device-000000000000000000000"},
		{"addresses", func(index int) string { return fmt.Sprintf("10.%d.%d.%d", index%7, index%11, index) }, 2_000, "10."},
	} {
		t.Run(test.name, func(t *testing.T) {
			dimensions := []string{"target"}
			var population []series
			for index := 0; index < test.values; index++ {
				population = append(population, series{"target": test.value(index)})
			}
			probe := append(append([]series(nil), population...), awkward("target")...)
			rs := &rounds{t: t, dimensions: dimensions, population: population, probe: probe, start: rootOnly(t, dimensions), withBounds: true}
			first := rs.next(backend{fixed: 300, perSeries: 2})
			if first.counts.SplitForTime != 1 {
				t.Fatalf("a slice of %d ms with no other: %+v, want it split", first.slowest, first.counts)
			}
			tree, err := inputslice.FromShape(dimensions, rs.shape)
			if err != nil {
				t.Fatal(err)
			}
			largest := 0
			for _, leaf := range tree.Pending() {
				largest = max(largest, len(members(t, leaf.Slice, population)))
				for _, condition := range leaf.Slice.Conditions {
					if !condition.Negated && !strings.HasPrefix(condition.Pattern, "^"+strings.ReplaceAll(test.at, ".", `\.`)) {
						t.Fatalf("a slice %q is not cut at %q", condition.Pattern, test.at)
					}
				}
			}
			if largest > test.values/2 {
				t.Fatalf("the largest slice holds %d of %d series: %s", largest, test.values, describe(leavesOf(tree)))
			}
		})
	}
}

// A slice of one value - its least and greatest the same - has nothing to
// split: a split for its time is not made and says why (Concentrated, the
// shape's refused_single_value), and a split kept for its size is refused
// by name.
func TestASliceOfOneValueIsNotSplitAndSaysWhy(t *testing.T) {
	dimensions := []string{"host"}
	population := make([]series, 500)
	for index := range population {
		population[index] = series{"host": "h1"}
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("host")...),
		start: rootOnly(t, dimensions), withBounds: true}
	got := rs.next(backend{fixed: 300, perSeries: 10})
	if got.counts.SplitForTime != 0 || got.counts.Concentrated != 1 || !rs.shape.IsZero() {
		t.Fatalf("one value read in %d ms: %+v, shape of %d leaves; want none split and the reason counted", got.slowest, got.counts, rs.shape.Leaves())
	}
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	var holding inputslice.Leaf
	for _, leaf := range tree.Pending() {
		if len(members(t, leaf.Slice, population)) > 0 {
			holding = leaf
		}
	}
	_, _, err = tree.KeepSizedSplit(holding.ID, sampleOf(dimensions, population), boundsOf(dimensions, population))
	assertConcentrated(t, err, inputslice.ReasonSingleValue)
}

// A Query Group read as one slice slower than the target is split: with
// fewer than three slices read the target alone decides, as there is no
// median to say the backend is slow as a whole.
func TestAColdStartOfOneSlowSliceIsSplit(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 3_000; index++ {
		population = append(population, series{"channel": fmt.Sprint(100_000 + index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions)}
	got := rs.next(backend{fixed: 300, perSeries: 2})
	if got.counts.SplitForTime != 1 || rs.shape.IsZero() {
		t.Fatalf("one slice of %d ms: %+v, want it split", got.slowest, got.counts)
	}
}

// A backend slow as a whole - every read the same five seconds, whatever it
// holds - splits a Query Group read in one slice once: its children read
// no faster, the split is taken back the round after and its node marked,
// and from then on it is held, not split again round after round. Once the
// backend reads under the target the mark goes, and a slow read splits it
// again.
func TestABackendSlowAsAWholeIsSplitOnceAndHeld(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions)}
	slow := backend{flat: 5_000}
	if got := rs.next(slow); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want the one slice split", got.counts)
	}
	got := rs.next(slow)
	if got.leaves < 2 || got.counts.Unhelpful != 1 || !rs.shape.IsZero() {
		t.Fatalf("round 2 read %d slices: %+v, shape of %d leaves; want the split taken back, the shape none", got.leaves, got.counts, rs.shape.Leaves())
	}
	for index := 3; index <= 8; index++ {
		got := rs.next(slow)
		if got.leaves != 1 || got.counts.HeldByMark != 1 || got.counts.SplitForTime != 0 {
			t.Fatalf("round %d read %d slices: %+v; want one slice, held by the mark", index, got.leaves, got.counts)
		}
	}
	if got := rs.next(backend{flat: 1_000}); got.counts.SplitForTime != 0 || got.counts.HeldByMark != 0 {
		t.Fatalf("a fast round: %+v", got.counts)
	}
	if got := rs.next(slow); got.counts.SplitForTime != 1 {
		t.Fatalf("slow again after a fast round: %+v, want it split again, the mark gone", got.counts)
	}
}

// A backend slow as a whole once a Query Group is read in several slices -
// every read the same five seconds, past the target, none twice the round's
// median - splits none of them: a slice is split for its time only when it
// is slower than the others, not when every one is (015 C2 section 4.3, the
// relative condition), and the round counts every one held so.
func TestEverySliceSlowAlikeIsHeldNotSplit(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions)}
	if got := rs.next(backend{fixed: 300, perSeries: 20}); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want the one slice split for its time", got.counts)
	}
	got := rs.next(backend{flat: 5_000})
	if got.leaves < 3 || got.counts.SplitForTime != 0 || got.counts.LeafCap != 0 || got.counts.HeldUniform < 3 {
		t.Fatalf("round 2 read %d slices, each 5 s: %+v; want none split, every slow one held as the backend's", got.leaves, got.counts)
	}
}

// A slice the provider timed out says nothing of what it holds: on a backend
// slow throughout, every read sixteen seconds, the provider's thirty no more
// than twice the fastest, the children that time out are not taken for
// empty and folded into their rest with the empty ones - folded, the rest
// would hold them all and time out in turn (015 C2 section 4.8).
func TestASliceTimedOutIsNotFoldedAsEmpty(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions)}
	// Slow enough whole that the children's 16 s is help: the split kept.
	if got := rs.next(backend{fixed: 300, perSeries: 100}); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want the one slice split for its time", got.counts)
	}
	// "1" to "4" hold 111 series each and time out; the rest read in 16 s.
	// Every round's shape: folded once, the Query Group would be read whole,
	// time out, and be split again the round after.
	slow := backend{flat: 16_000, timeoutAt: 100}
	for index := 2; index <= 6; index++ {
		rs.next(slow)
		if rs.shape.IsZero() {
			t.Fatalf("round %d left no shape: the Query Group read whole, every slice the provider timed out folded as empty", index)
		}
		tree, err := inputslice.FromShape(dimensions, rs.shape)
		if err != nil {
			t.Fatal(err)
		}
		for _, leaf := range tree.Pending() {
			if held := len(members(t, leaf.Slice, population)); held > 111 {
				t.Fatalf("round %d left a slice of %d series, past the 111 of the largest slice the provider timed out: "+
					"those were folded as empty", index, held)
			}
		}
	}
}

// A slice slow for the series one of its children holds - every other child
// as fast as an empty slice - is split level by level down to where its
// series part, never taken back for no help nor held: the children's median
// is the empty ones'. With no bounds to cut at - a read whose aggregation
// delivered nothing - each level is one character of the values' shared
// prefix, which is the longest any shape takes to come right: it is never
// refused, never past the leaves a Slot starts in, its empty children
// folded into their rest as it goes, and it comes right within a bounded
// number of rounds or stops counted at the leaves it may have.
func TestAConcentratedSliceIsSplitLevelByLevel(t *testing.T) {
	dimensions := []string{"device"}
	var population []series
	for index := 0; index < 2_000; index++ {
		population = append(population, series{"device": fmt.Sprintf("device-%025d", index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("device")...),
		start: rootOnly(t, dimensions)}
	b := backend{fixed: 300, perSeries: 2}
	came := -1
	var total inputslice.NextCounts
	for index := 1; index <= 400; index++ {
		got := rs.next(b)
		total.SplitForTime += got.counts.SplitForTime
		total.Folded += got.counts.Folded
		total.LeafCap += got.counts.LeafCap
		total.Unhelpful += got.counts.Unhelpful
		total.HeldByMark += got.counts.HeldByMark
		total.Concentrated += got.counts.Concentrated
		if got.counts.Unhelpful != 0 || got.counts.HeldByMark != 0 || got.counts.Concentrated != 0 {
			t.Fatalf("round %d: %+v; a concentrated slice is no backend slow as a whole", index, got.counts)
		}
		if got.slowest <= prefixRules.TargetMillis {
			came = index
			break
		}
	}
	if came < 0 {
		t.Fatalf("still slower than the target after 400 rounds: %+v", total)
	}
	t.Logf("device-%%025d of 2,000 series, no bounds: every slice under the target in round %d; %+v", came, total)
}

// A child kept apart that reads as an empty slice does - within twice the
// round's fastest - for three rounds in a row is folded into its rest, one
// slice fewer; a value of it that appears later is read in the rest.
func TestEmptyChildrenAreFoldedIntoTheirRest(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 3_000; index++ {
		population = append(population, series{"channel": fmt.Sprint(500_000 + index)})
	}
	later := []series{{"channel": "9000"}, {"channel": "a1"}}
	probe := append(append(append([]series(nil), population...), later...), awkward("channel")...)
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: probe, start: rootOnly(t, dimensions)}
	b := backend{fixed: 300, perSeries: 2}
	rs.next(b)
	split := rs.next(b)
	var folded uint64
	for index := 0; index < 3; index++ {
		folded += rs.next(b).counts.Folded
	}
	if folded == 0 {
		t.Fatalf("no child folded in three rounds after a split of %d slices", split.leaves)
	}
	rs.population = append(rs.population, later...)
	rs.next(b)
}

// A kept split is taken back only after its leaves together read under half
// the target for three rounds in a row: not while they read between half the
// target and the target - two children of a thousand series at 1.3 s each,
// 2.3 s together with the fixed cost once - nor after two quick rounds.
func TestAKeptSplitIsTakenBackAfterThreeQuickRounds(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 1_000; index++ {
		population = append(population, series{"channel": fmt.Sprint(1_000 + index)}, series{"channel": fmt.Sprint(2_000 + index)})
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions)}
	if got := rs.next(backend{flat: 5_000}); got.counts.SplitForTime != 1 {
		t.Fatalf("not split: %+v", got.counts)
	}
	between := backend{fixed: 300, perSeries: 1}
	for index := 0; index < 5; index++ {
		got := rs.next(between)
		if got.counts.Merged != 0 || got.counts.Unhelpful != 0 || rs.shape.IsZero() {
			t.Fatalf("round %d between half the target and the target: %+v, shape of %d leaves; want it kept", index+2, got.counts, rs.shape.Leaves())
		}
	}
	quick := backend{flat: 10}
	for index := 1; index <= 2; index++ {
		if got := rs.next(quick); got.counts.Merged != 0 || rs.shape.IsZero() {
			t.Fatalf("taken back after %d quick rounds: %+v", index, got.counts)
		}
	}
	if got := rs.next(quick); got.counts.Merged == 0 || !rs.shape.IsZero() {
		t.Fatalf("not taken back after three quick rounds: %+v, %d leaves", got.counts, rs.shape.Leaves())
	}
}

// At the leaves a Slot starts in, a slow slice is read as it is: not split,
// counted, the leaves together complete still.
func TestTheLeafCapStopsSplittingAndSaysSo(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 4_000; index++ {
		population = append(population, series{"channel": fmt.Sprintf("%d%d", index%3, index)})
	}
	start := func() *inputslice.Tree {
		tree, err := inputslice.NewCut(dimensions, inputslice.MaxLeaves-3, inputslice.FixedCut("channel"))
		if err != nil {
			t.Fatal(err)
		}
		return tree
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...), start: start}
	got := rs.next(backend{fixed: 300, perSeries: 10})
	if got.counts.LeafCap == 0 || got.counts.SplitForTime != 0 {
		t.Fatalf("a tree of %d leaves: %+v, want the slow slices held at the cap and counted", got.leaves, got.counts)
	}
}

// A split this Slot made of a slice the provider timed out is judged the round
// after against the time it ran out after: its children no faster, it is
// taken back and held.
func TestATimeoutSplitIsJudgedTheRoundAfter(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("channel"))
	if err != nil {
		t.Fatal(err)
	}
	pending := tree.Pending()
	timedOut := pending[3]
	children, err := tree.SplitKept(timedOut.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	reads := map[int]inputslice.LeafRead{timedOut.ID: {Millis: 30_000}}
	for _, leaf := range tree.Pending() {
		if err := tree.Land(leaf.ID); err != nil {
			t.Fatal(err)
		}
		reads[leaf.ID] = inputslice.LeafRead{Millis: 400}
	}
	shape, history, _ := tree.Next(reads, nil, prefixRules)
	if len(children) < 2 || shape.IsZero() {
		t.Fatalf("the timeout split is not in the shape")
	}
	next, err := inputslice.FromShape(dimensions, shape)
	if err != nil {
		t.Fatal(err)
	}
	reads = readWhole(t, next, dimensions, population, backend{flat: 29_000}, false)
	_, _, counts := next.Next(reads, history, prefixRules)
	if counts.Unhelpful != 1 {
		t.Fatalf("children of a 30 s slice read at 29 s: %+v, want the split taken back for no help", counts)
	}
}

// A slice of a tree cut by prefix let go for its size is split by the classes
// of its values' ends under its prefix: the tree switches, every series in
// exactly one leaf, its children cut from the end as any tree cut so is, and
// it leaves no shape - its Query Group's next Slot is cut from the end from
// its root.
func TestASliceLetGoForItsSizeSwitchesItsTree(t *testing.T) {
	dimensions := []string{"device"}
	var population []series
	for index := 0; index < 2_000; index++ {
		population = append(population, series{"device": fmt.Sprintf("device-%025d", index)})
	}
	probe := append(append([]series(nil), population...), awkward("device")...)
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("device"))
	if err != nil {
		t.Fatal(err)
	}
	tr := &tracker{tree: tree}
	var holding inputslice.Leaf
	for _, leaf := range tree.Pending() {
		if len(members(t, leaf.Slice, population)) > 0 {
			holding = leaf
		} else {
			tr.land(t, leaf)
		}
	}
	// Let go after the first 300 series, its stream in label order.
	children, err := tree.Split(holding.ID, sampleOf(dimensions, members(t, holding.Slice, population)[:300]))
	if err != nil {
		t.Fatal(err)
	}
	if !tree.Switched() || !tree.Shape().IsZero() {
		t.Fatalf("switched %v, shape of %d leaves; want switched and no shape", tree.Switched(), tree.Shape().Leaves())
	}
	largest := 0
	for _, child := range children {
		if conditions := child.Slice.Conditions; len(conditions) != 2 || conditions[0].Pattern != "^d(?s:.*)$" {
			t.Fatalf("child %s, want its prefix and one class of its end", describe([]execution.InputSlice{child.Slice}))
		}
		largest = max(largest, len(members(t, child.Slice, population)))
	}
	if largest > len(population)/2 {
		t.Fatalf("the largest child holds %d of %d", largest, len(population))
	}
	assertPartition(t, tr.leaves(), probe)
	if _, err := tree.Split(children[0].ID, sampleOf(dimensions, members(t, children[0].Slice, population))); err != nil {
		t.Fatalf("a child of the switch split for its size: %v", err)
	}
	assertPartition(t, tr.leaves(), probe)
}

// A tree cut by prefix built back from its Shape has its leaves, folds and
// all, and its root's cut.
func TestAPrefixShapeRebuildsItsLeaves(t *testing.T) {
	dimensions := []string{"channel", "zone"}
	var population []series
	for index := 0; index < 3_000; index++ {
		population = append(population, series{"channel": fmt.Sprint(700_000 + index), "zone": fmt.Sprint("z", index%4)})
	}
	start := func() *inputslice.Tree {
		tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("channel"))
		if err != nil {
			t.Fatal(err)
		}
		return tree
	}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: start, withBounds: true}
	for index := 0; index < 6; index++ {
		rs.next(backend{fixed: 300, perSeries: 2})
	}
	if rs.shape.IsZero() {
		t.Fatal("no shape")
	}
	built, err := inputslice.FromShape(dimensions, rs.shape)
	if err != nil {
		t.Fatal(err)
	}
	if !built.ByPrefix() || built.RootCut().Dimension != "channel" || !built.RootCut().ByPrefix() {
		t.Fatalf("built tree cut %+v, by prefix %v", built.RootCut(), built.ByPrefix())
	}
	again, err := inputslice.FromShape(dimensions, built.Shape())
	if err != nil {
		t.Fatal(err)
	}
	if again.Leaves() != built.Leaves() {
		t.Fatalf("built twice: %d and %d leaves", again.Leaves(), built.Leaves())
	}
}
