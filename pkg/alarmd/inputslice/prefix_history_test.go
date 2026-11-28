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
	"slices"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// What a node carries from one round to the next (inputslice.History) as
// each step of Next judges it, read from what the rounds after do: a tree
// started in the root's slices, one of them split under it, as a Query Group
// read in slices for its size is and then split for its time.

// nested is a Query Group of a channel dimension and a pod one, read from
// the root's slices of channel (one a character, and their rest): "5" holds
// 500 to 599 - split for its time, its children "50" to "59", ten series
// each, and their rest, which holds none - "1" holds 100 to 109, "7" holds
// 7000, and the root's rest holds A1 and B2, of pods p-a and p-z; every other
// slice of the root holds nothing.
func nested(t *testing.T) *rounds {
	t.Helper()
	dimensions := []string{"channel", "pod"}
	var population []series
	for index := 0; index < 100; index++ {
		population = append(population, series{"channel": fmt.Sprint(500 + index), "pod": "p-5"})
	}
	for index := 0; index < 10; index++ {
		population = append(population, series{"channel": fmt.Sprint(100 + index), "pod": "p-1"})
	}
	population = append(population, series{"channel": "7000", "pod": "p-7"},
		series{"channel": "A1", "pod": "p-a"}, series{"channel": "B2", "pod": "p-z"})
	start := func() *inputslice.Tree {
		tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("channel"))
		if err != nil {
			t.Fatal(err)
		}
		return tree
	}
	return &rounds{t: t, dimensions: dimensions, population: population,
		probe: append(append([]series(nil), population...), awkward("channel")...), start: start, withBounds: true}
}

// nextAs is a round read whole, each slice in the time took gives it by the
// series it holds, its bounds kept.
func (rs *rounds) nextAs(took func(in []series) uint64) round {
	rs.t.Helper()
	return rs.nextRead(func(in []series) inputslice.LeafRead {
		return inputslice.LeafRead{Millis: took(in), Series: uint64(len(in))}
	})
}

// nextRead is a round read whole, each slice read as read gives it by the
// series it holds, its bounds kept but for a read the provider timed out.
func (rs *rounds) nextRead(read func(in []series) inputslice.LeafRead) round {
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
	reads := map[int]inputslice.LeafRead{}
	var slowest uint64
	for _, leaf := range tree.Pending() {
		in := members(rs.t, leaf.Slice, rs.population)
		r := read(in)
		if !r.TimedOut {
			r.Bounds = boundsOf(rs.dimensions, in)
		}
		if err := tree.Land(leaf.ID); err != nil {
			rs.t.Fatal(err)
		}
		reads[leaf.ID] = r
		slowest = max(slowest, r.Millis)
	}
	leaves := tree.Leaves()
	var counts inputslice.NextCounts
	rs.shape, rs.history, counts = tree.Next(reads, rs.history, prefixRules)
	if !rs.shape.IsZero() {
		built, err := inputslice.FromShape(rs.dimensions, rs.shape)
		if err != nil {
			rs.t.Fatalf("the shape Next left does not rebuild: %v", err)
		}
		assertPartition(rs.t, leavesOf(built), rs.probe)
	}
	return round{leaves: leaves, slowest: slowest, counts: counts}
}

// holdingWith reports whether the next round reads the series of channel a in
// the slice it reads b in: in the shape the rounds left, or the slices they
// start in when it is none.
func (rs *rounds) holdingWith(a, b string) bool {
	rs.t.Helper()
	tree := rs.start()
	if !rs.shape.IsZero() {
		built, err := inputslice.FromShape(rs.dimensions, rs.shape)
		if err != nil {
			rs.t.Fatal(err)
		}
		tree = built
	}
	for _, leaf := range tree.Pending() {
		in := members(rs.t, leaf.Slice, rs.population)
		has := func(channel string) bool {
			return slices.ContainsFunc(in, func(s series) bool { return s["channel"] == channel })
		}
		if has(a) {
			return has(b)
		}
	}
	rs.t.Fatalf("no slice of the shape holds %s", a)
	return false
}

// fiveSlow is a round the slice that holds all of 500 to 599 reads in ten
// seconds; the one of 100 to 109 and the root's rest, A1 and B2, in 300 ms;
// every other, none of them, in 100 ms, the round's fastest.
func fiveSlow(in []series) uint64 {
	switch {
	case len(in) == 100:
		return 10_000
	case len(in) > 0:
		return 300
	}
	return 100
}

// A split under a split of the root - "5" of the root's slices, split for its
// time - whose children read quick together for three rounds in a row is
// taken back in the third, not before: each round counts on the one before,
// the parent's count not reset to what it carried in by the root's split,
// which "5" is a child of.
func TestASplitUnderTheRootsSplitIsTakenBackAfterThreeQuickRounds(t *testing.T) {
	rs := nested(t)
	if got := rs.nextAs(fiveSlow); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want 5 split for its time", got.counts)
	}
	// "50" to "59" in 210 ms each, past an empty slice's 200: 1.2 s together
	// with the fixed cost once, under half the target.
	quick := func(in []series) uint64 {
		switch {
		case len(in) == 10 && in[0]["channel"][0] == '5':
			return 210
		case len(in) > 0:
			return 300
		}
		return 100
	}
	for index := 2; index <= 3; index++ {
		if got := rs.nextAs(quick); got.counts.Merged != 0 || got.counts.SplitForTime != 0 || got.counts.Unhelpful != 0 {
			t.Fatalf("round %d, quick round %d: %+v; want the split under the root's kept", index, index-1, got.counts)
		}
	}
	if got := rs.nextAs(quick); got.counts.Merged != 1 {
		t.Fatalf("round 4, the third quick round: %+v; want the split under the root's taken back", got.counts)
	}
	if !rs.holdingWith("500", "599") {
		t.Fatal("500 and 599 in two slices after the split under the root's was taken back")
	}
}

// A split under a split of the root whose children read no faster is taken
// back the round after, and its node is held from then on - each round
// counted held by its mark and not split - not split again the round after
// and back and forth, the mark kept as the root's split judges "5" as its
// child.
func TestASplitUnderTheRootsSplitOfNoHelpIsHeldFromThen(t *testing.T) {
	rs := nested(t)
	if got := rs.nextAs(fiveSlow); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want 5 split for its time", got.counts)
	}
	// A backend slow as a whole: every slice in 6 s, 5's children too,
	// past half the 10 s 5 read in; held as the backend's, not split.
	noFaster := func([]series) uint64 { return 6_000 }
	if got := rs.nextAs(noFaster); got.counts.Unhelpful != 1 {
		t.Fatalf("round 2, 5's children at 6 s against its 10 s: %+v; want the split taken back for no help", got.counts)
	}
	for index := 3; index <= 6; index++ {
		if got := rs.nextAs(fiveSlow); got.counts.HeldByMark != 1 || got.counts.SplitForTime != 0 {
			t.Fatalf("round %d, 5 at 10 s again: %+v; want it held by its mark, not split", index, got.counts)
		}
	}
}

// A child of the root's split that reads as an empty slice does is folded
// into the rest after three such rounds in a row, not three in all: a round
// it reads slower starts the count again.
func TestAChildIsFoldedAfterThreeEmptyRoundsInARowNotInAll(t *testing.T) {
	rs := nested(t)
	// "7", 7000 alone, empty but for its third round; "1" at 300 ms and "5"
	// at 2 s, under the target - so that the root's split, kept once its
	// empty children are folded in round 3, does not read quick and is not
	// taken back whole; every other child of the root as empty as "7", to be
	// folded into the rest in round 3, the rest at 100 ms, the round's
	// fastest.
	sevenAt := func(millis uint64) func(in []series) uint64 {
		return func(in []series) uint64 {
			switch {
			case len(in) == 1 && in[0]["channel"] == "7000":
				return millis
			case len(in) == 100:
				return 2_000
			case len(in) == 10:
				return 300
			}
			return 100
		}
	}
	for index, millis := range []uint64{100, 100, 1_000, 100, 100} {
		rs.nextAs(sevenAt(millis))
		if rs.holdingWith("7000", "A1") {
			t.Fatalf("round %d: 7 folded into the rest, its empty rounds in a row %v", index+1, []uint64{100, 100, 1_000, 100, 100}[:index+1])
		}
	}
	rs.nextAs(sevenAt(100))
	if !rs.holdingWith("7000", "A1") {
		t.Fatal("round 6, 7's third empty round in a row: not folded into the rest")
	}
}

// A child of the root's split held by its mark that reads under the target
// loses the mark: slow again, it is split for its time again.
func TestAMarkOfAChildGoesOnceItReadsUnderTheTarget(t *testing.T) {
	rs := nested(t)
	rs.nextAs(fiveSlow)
	// A backend slow as a whole: every slice in 6 s, 5's children too,
	// past half the 10 s 5 read in; held as the backend's, not split.
	noFaster := func([]series) uint64 { return 6_000 }
	if got := rs.nextAs(noFaster); got.counts.Unhelpful != 1 {
		t.Fatalf("round 2: %+v; want the split taken back for no help", got.counts)
	}
	fast := func(in []series) uint64 {
		if len(in) == 100 {
			return 2_000
		}
		return fiveSlow(in)
	}
	if got := rs.nextAs(fast); got.counts.HeldByMark != 0 || got.counts.SplitForTime != 0 {
		t.Fatalf("round 3, 5 at 2 s: %+v", got.counts)
	}
	if got := rs.nextAs(fiveSlow); got.counts.SplitForTime != 1 || got.counts.HeldByMark != 0 {
		t.Fatalf("round 4, 5 at 10 s after a round under the target: %+v; want it split again, its mark gone", got.counts)
	}
}

// The rest of the root's split the round its empty children are folded into
// it is the same slice as it was read: slower than the target and than twice
// the round's median, it is split for its time that round - on pod, its two
// series' channels A1 and B2 sharing no prefix of the alphabet.
func TestARestReadSlowTheRoundChildrenAreFoldedIntoItIsSplit(t *testing.T) {
	rs := nested(t)
	restAt := func(millis uint64) func(in []series) uint64 {
		return func(in []series) uint64 {
			switch {
			case len(in) == 2:
				return millis
			case len(in) > 0:
				return 300
			}
			return 100
		}
	}
	for index := 1; index <= 2; index++ {
		if got := rs.nextAs(restAt(300)); got.counts.Folded != 0 || got.counts.SplitForTime != 0 {
			t.Fatalf("round %d: %+v", index, got.counts)
		}
	}
	got := rs.nextAs(restAt(10_000))
	if got.counts.Folded == 0 || got.counts.SplitForTime != 1 {
		t.Fatalf("round 3, the empty children's third and the rest at 10 s: %+v; want them folded and the rest split", got.counts)
	}
	if rs.holdingWith("A1", "B2") {
		t.Fatal("A1 and B2 in one slice after the rest was split for its time")
	}
}

// A split for a slice the provider timed out whose children time out in turn
// is no help: a child the provider timed out reads as slow as its limit,
// and the split is judged on it - taken back and held - not passed over.
func TestATimeoutSplitWhoseChildrenTimeOutIsNoHelp(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("channel"))
	if err != nil {
		t.Fatal(err)
	}
	timedOut := tree.Pending()[3]
	if _, err := tree.SplitKept(timedOut.ID, nil); err != nil {
		t.Fatal(err)
	}
	reads := map[int]inputslice.LeafRead{timedOut.ID: {Millis: timedOutMillis}}
	for _, leaf := range tree.Pending() {
		if err := tree.Land(leaf.ID); err != nil {
			t.Fatal(err)
		}
		reads[leaf.ID] = inputslice.LeafRead{Millis: 400}
	}
	shape, history, _ := tree.Next(reads, nil, prefixRules)
	next, err := inputslice.FromShape(dimensions, shape)
	if err != nil {
		t.Fatal(err)
	}
	// Every slice of a series or more times out; the empty ones read in 29 s.
	reads = readWhole(t, next, dimensions, population, backend{flat: 29_000, timeoutAt: 1}, false)
	if _, _, counts := next.Next(reads, history, prefixRules); counts.Unhelpful != 1 {
		t.Fatalf("children of a slice timed out timing out in turn: %+v, want the split taken back for no help", counts)
	}
}

// A Slot that failed with a split it made for a slice the provider timed out
// keeps its tree's shape and what it carries (KeptHistory): the split is
// judged the round after against the time it ran out after, as one of a Slot
// that completed is - without, it is not judged at all.
func TestATimeoutSplitOfAFailedSlotIsJudgedTheRoundAfter(t *testing.T) {
	dimensions := []string{"channel"}
	var population []series
	for index := 0; index < 500; index++ {
		population = append(population, series{"channel": fmt.Sprint(index)})
	}
	failed := func() (*inputslice.Tree, inputslice.History) {
		tree, err := inputslice.NewCut(dimensions, 2, inputslice.FixedCut("channel"))
		if err != nil {
			t.Fatal(err)
		}
		timedOut := tree.Pending()[3]
		if _, err := tree.SplitKept(timedOut.ID, nil); err != nil {
			t.Fatal(err)
		}
		return tree, tree.KeptHistory(map[int]inputslice.LeafRead{timedOut.ID: {Millis: timedOutMillis}}, nil)
	}
	for _, kept := range []bool{true, false} {
		tree, history := failed()
		if !kept {
			history = nil
		}
		next, err := inputslice.FromShape(dimensions, tree.Shape())
		if err != nil {
			t.Fatal(err)
		}
		reads := readWhole(t, next, dimensions, population, backend{flat: 29_000}, false)
		want := map[bool]uint64{true: 1, false: 0}[kept]
		if _, _, counts := next.Next(reads, history, prefixRules); counts.Unhelpful != want {
			t.Fatalf("history kept %v: children of a 30 s slice read at 29 s: %+v, want %d taken back for no help", kept, counts, want)
		}
	}
}

// A kept split one of whose children the provider timed out is not taken
// back as quick, however short the provider's limit - here half a second,
// the children together under half the target with it: a read timed out
// says nothing of what the child holds.
func TestASplitWithAChildTimedOutIsNotTakenBackAsQuick(t *testing.T) {
	rs := nested(t)
	if got := rs.nextAs(fiveSlow); got.counts.SplitForTime != 1 {
		t.Fatalf("round 1: %+v, want 5 split for its time", got.counts)
	}
	oneTimedOut := func(in []series) inputslice.LeafRead {
		switch {
		case len(in) == 10 && strings.HasPrefix(in[0]["channel"], "50"):
			return inputslice.LeafRead{Millis: 500, TimedOut: true}
		case len(in) == 10 && in[0]["channel"][0] == '5':
			return inputslice.LeafRead{Millis: 210, Series: 10}
		case len(in) > 0:
			return inputslice.LeafRead{Millis: 300, Series: uint64(len(in))}
		}
		return inputslice.LeafRead{Millis: 100}
	}
	for index := 2; index <= 5; index++ {
		if got := rs.nextRead(oneTimedOut); got.counts.Merged != 0 {
			t.Fatalf("round %d, 50 timed out at 500 ms, the rest quick: %+v; want the split kept", index, got.counts)
		}
	}
}

// A slice slower than the target whose values share a prefix so long that
// the children it would be split into at it pass the condition budget is
// read as it is and counted so: not split - the next Slot's tree would
// refuse the shape, and the round after split it the same way again.
func TestASplitForTimePastTheConditionBudgetIsNotMade(t *testing.T) {
	dimensions := []string{"channel"}
	long := strings.Repeat("a", inputslice.ConditionBudget)
	population := []series{{"channel": long + "1"}, {"channel": long + "2"}}
	rs := &rounds{t: t, dimensions: dimensions, population: population, probe: append(population, awkward("channel")...),
		start: rootOnly(t, dimensions), withBounds: true}
	got := rs.next(backend{flat: 5_000})
	if got.counts.ConditionBudget != 1 || got.counts.SplitForTime != 0 || !rs.shape.IsZero() {
		t.Fatalf("one slice of 5 s, its values sharing %d characters: %+v; want it not split, counted past the budget", len(long), got.counts)
	}
}
