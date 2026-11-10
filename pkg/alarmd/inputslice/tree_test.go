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
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// series is one series' dimension values; a dimension absent from the map is
// a missing label.
type series map[string]string

// tracker keeps every leaf the tree ever had that was not split: the landed
// ones, which Pending no longer lists, and the pending ones.
type tracker struct {
	tree   *inputslice.Tree
	landed []execution.InputSlice
}

func (tr *tracker) leaves() []execution.InputSlice {
	all := append([]execution.InputSlice(nil), tr.landed...)
	for _, leaf := range tr.tree.Pending() {
		all = append(all, leaf.Slice)
	}
	return all
}

func (tr *tracker) land(t *testing.T, leaf inputslice.Leaf) {
	t.Helper()
	if err := tr.tree.Land(leaf.ID); err != nil {
		t.Fatalf("land %d: %v", leaf.ID, err)
	}
	tr.landed = append(tr.landed, leaf.Slice)
}

// promMatches reads a slice the way the store does: a Prometheus matcher
// anchors its expression again as ^(?:…)$, has no (?s) of its own, and reads
// a missing label as the empty value.
func promMatches(t *testing.T, slice execution.InputSlice, values series) bool {
	t.Helper()
	for _, condition := range slice.Conditions {
		pattern, err := regexp.Compile("^(?:" + condition.Pattern + ")$")
		if err != nil {
			t.Fatalf("pattern %q does not compile as a matcher: %v", condition.Pattern, err)
		}
		if pattern.MatchString(values[condition.Dimension]) == condition.Negated {
			return false
		}
	}
	return true
}

// assertPartition checks the property the tree exists for, independently of
// how it built its leaves: every series is in exactly one leaf, read either
// way.
func assertPartition(t *testing.T, leaves []execution.InputSlice, population []series) {
	t.Helper()
	for index, slice := range leaves {
		if err := slice.Validate(); err != nil {
			t.Fatalf("leaf %d does not validate: %v", index, err)
		}
		if slice.ConditionBytes() > inputslice.ConditionBudget {
			t.Fatalf("leaf %d takes %d bytes", index, slice.ConditionBytes())
		}
	}
	for _, values := range population {
		goMatches, storeMatches := 0, 0
		for _, slice := range leaves {
			matched, err := slice.Matches(values)
			if err != nil {
				t.Fatalf("match: %v", err)
			}
			if matched {
				goMatches++
			}
			if promMatches(t, slice, values) {
				storeMatches++
			}
		}
		if goMatches != 1 || storeMatches != 1 {
			t.Fatalf("series %q is in %d leaves by Matches and %d by the store's matcher, want 1 and 1; leaves %s",
				values, goMatches, storeMatches, describe(leaves))
		}
	}
}

func describe(leaves []execution.InputSlice) string {
	var lines []string
	for _, slice := range leaves {
		var parts []string
		for _, condition := range slice.Conditions {
			parts = append(parts, fmt.Sprintf("%s neg=%v %q", condition.Dimension, condition.Negated, condition.Pattern))
		}
		lines = append(lines, "{"+strings.Join(parts, " & ")+"}")
	}
	return strings.Join(lines, " ")
}

// members is the series of population in slice.
func members(t *testing.T, slice execution.InputSlice, population []series) []series {
	t.Helper()
	var in []series
	for _, values := range population {
		matched, err := slice.Matches(values)
		if err != nil {
			t.Fatal(err)
		}
		if matched {
			in = append(in, values)
		}
	}
	return in
}

// sampleOf is the sample a discarded slice hands Split: each dimension's
// values, a missing label read as empty.
func sampleOf(dimensions []string, in []series) map[string][]string {
	sample := map[string][]string{}
	for _, dimension := range dimensions {
		for _, values := range in {
			sample[dimension] = append(sample[dimension], values[dimension])
		}
	}
	return sample
}

// awkwardRunes are characters that mean something in a bracket expression or
// in a regular expression, are not printable, are multi-byte, or are the
// replacement character itself.
var awkwardRunes = []string{
	"]", "[", "\\", "^", "-", ":", ".", "$", "*", "(", ")", "{", "}", "|", "+", "?",
	"\n", "\r", "\t", "\x00", "\x7f", " ", "中", "é", "😀", "�", "\U0010FFFF", "​",
}

// invalidUTF8 are byte strings that are not UTF-8, each of which Go's regexp
// reads as one U+FFFD per byte.
var invalidUTF8 = []string{"\xff", "\xe4\xb8", "\x80", "\xc0\xaf", "\xed\xa0\x80", "\xf0\x9f\x98"}

func randomValue(rng *rand.Rand) string {
	switch rng.Intn(10) {
	case 0:
		return ""
	case 1:
		return fmt.Sprintf("svc-%d.svc", rng.Intn(1000))
	case 2:
		return fmt.Sprintf("api-%d-prod", rng.Intn(1000))
	case 3:
		return fmt.Sprintf("%d0", rng.Intn(10000))
	case 4:
		return invalidUTF8[rng.Intn(len(invalidUTF8))] + fmt.Sprint(rng.Intn(10)) + invalidUTF8[rng.Intn(len(invalidUTF8))]
	default:
		var builder strings.Builder
		length := 1 + rng.Intn(8)
		for index := 0; index < length; index++ {
			switch rng.Intn(3) {
			case 0:
				builder.WriteString(awkwardRunes[rng.Intn(len(awkwardRunes))])
			default:
				builder.WriteByte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[rng.Intn(62)])
			}
		}
		return builder.String()
	}
}

func randomPopulation(rng *rand.Rand, dimensions []string, size int) []series {
	population := make([]series, 0, size)
	for index := 0; index < size; index++ {
		values := series{}
		for _, dimension := range dimensions {
			if rng.Intn(12) == 0 {
				continue // missing label
			}
			values[dimension] = randomValue(rng)
		}
		population = append(population, values)
	}
	return population
}

// TestEveryValueInExactlyOneLeafAfterAnySplits drives the tree from its public
// entry with random landings and splits, samples that are the whole slice, a
// prefix of it as a stream would deliver, or nothing, and checks after every
// step that the leaves partition both the series the samples came from and
// series no sample ever showed.
func TestEveryValueInExactlyOneLeafAfterAnySplits(t *testing.T) {
	dimensions := []string{"service", "pod", "zone"}
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			population := randomPopulation(rng, dimensions, 300)
			unseen := randomPopulation(rng, dimensions, 300)
			probe := append(append([]series(nil), population...), unseen...)
			tree, err := inputslice.New(dimensions, 1+rng.Intn(12))
			if err != nil {
				t.Fatal(err)
			}
			tr := &tracker{tree: tree}
			assertPartition(t, tr.leaves(), probe)
			for step := 0; step < 120; step++ {
				pending := tree.Pending()
				if len(pending) == 0 {
					break
				}
				leaf := pending[rng.Intn(len(pending))]
				if rng.Intn(4) == 0 {
					tr.land(t, leaf)
					continue
				}
				in := members(t, leaf.Slice, population)
				var sample map[string][]string
				switch rng.Intn(5) {
				case 0:
				case 1:
					sample = sampleOf(dimensions, in[:rng.Intn(len(in)+1)])
				default:
					sample = sampleOf(dimensions, in)
				}
				before := tree.Leaves()
				children, err := tree.Split(leaf.ID, sample)
				if err != nil {
					if !errors.Is(err, inputslice.ErrValueConcentrated) {
						t.Fatalf("split: %v", err)
					}
					if tree.Leaves() != before {
						t.Fatalf("a refused split changed the leaves from %d to %d", before, tree.Leaves())
					}
					tr.land(t, leaf)
					continue
				}
				if tree.Leaves() != before-1+len(children) {
					t.Fatalf("leaves went from %d to %d with %d children", before, tree.Leaves(), len(children))
				}
				if tree.Leaves() > inputslice.MaxLeaves {
					t.Fatalf("tree has %d leaves", tree.Leaves())
				}
				if tree.Discarded() > tree.Leaves()-1 {
					t.Fatalf("%d discarded with %d leaves", tree.Discarded(), tree.Leaves())
				}
				assertPartition(t, tr.leaves(), probe)
			}
		})
	}
}

// TestRootSplitConditions pins the conditions of a split without a sample
// to the forms of §7.3, worked out by hand: the fixed alphabet's 36
// characters in four groups of nine, the last group left to the complement,
// and the root's exhausted child negated.
func TestRootSplitConditions(t *testing.T) {
	tree, err := inputslice.New([]string{"pod"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	pending := tree.Pending()
	if len(pending) != 1 || !pending[0].Slice.IsZero() {
		t.Fatalf("a tree of one leaf is the zero slice, got %+v", pending)
	}
	children, err := tree.Split(pending[0].ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []execution.InputSliceCondition{
		{Dimension: "pod", Pattern: "^(?s:.*)[0-8]$"},
		{Dimension: "pod", Pattern: "^(?s:.*)[9a-h]$"},
		{Dimension: "pod", Pattern: "^(?s:.*)[i-q]$"},
		{Dimension: "pod", Pattern: "^(?s:.*)[^0-9a-q]$"},
		{Dimension: "pod", Negated: true, Pattern: "^(?s:.+)$"},
	}
	if len(children) != len(want) {
		t.Fatalf("got %d children, want %d", len(children), len(want))
	}
	for index, child := range children {
		if len(child.Slice.Conditions) != 1 || child.Slice.Conditions[0] != want[index] {
			t.Fatalf("child %d is %+v, want %+v", index, child.Slice.Conditions, want[index])
		}
	}
	// The next level puts the new class in front of the one it refines, and
	// an exhausted child at depth one is the class alone, anchored.
	grandchildren, err := tree.Split(children[0].ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := grandchildren[1].Slice.Conditions[0].Pattern; got != "^(?s:.*)[9a-h][0-8]$" {
		t.Fatalf("second grandchild is %q", got)
	}
	if got := grandchildren[len(grandchildren)-1].Slice.Conditions[0].Pattern; got != "^[0-8]$" {
		t.Fatalf("exhausted grandchild is %q", got)
	}
	if tree.Discarded() != 2 {
		t.Fatalf("discarded %d, want 2", tree.Discarded())
	}
}

// TestSampledClassesEscapeBracketCharacters splits on a sample whose next
// characters are the ones that mean something in a bracket expression, and
// checks each child holds only its own share of them.
func TestSampledClassesEscapeBracketCharacters(t *testing.T) {
	var population []series
	for _, r := range []string{"]", "\\", "^", "-", "[", ":", "\n", "\x00", "中", "\xff", "a"} {
		for copyIndex := 0; copyIndex < 3; copyIndex++ {
			population = append(population, series{"pod": fmt.Sprintf("p%d%s", copyIndex, r)})
		}
	}
	population = append(population, series{}, series{"pod": ""})
	tree, err := inputslice.New([]string{"pod"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	root := tree.Pending()[0]
	children, err := tree.Split(root.ID, sampleOf([]string{"pod"}, population))
	if err != nil {
		t.Fatal(err)
	}
	tr := &tracker{tree: tree}
	assertPartition(t, tr.leaves(), population)
	nonEmpty := 0
	for _, child := range children {
		if len(members(t, child.Slice, population)) > 0 {
			nonEmpty++
		}
	}
	// Eleven distinct last characters and the empty values: every class and
	// the exhausted child should hold some of the sample.
	if nonEmpty != len(children) {
		t.Fatalf("only %d of %d children hold sampled series: %s", nonEmpty, len(children), describe(tr.leaves()))
	}
}

// TestFixedSuffixMakesProgress is values sharing a long suffix: each split
// on the slice holding them moves one character further from the end, and
// within the suffix's length plus one the series are spread over more than
// one child.
func TestFixedSuffixMakesProgress(t *testing.T) {
	for _, suffix := range []string{".svc", "-prod", "0"} {
		t.Run(suffix, func(t *testing.T) {
			var population []series
			for index := 0; index < 200; index++ {
				population = append(population, series{"pod": fmt.Sprintf("name-%d%s", index, suffix)})
			}
			tree, err := inputslice.New([]string{"pod"}, 1)
			if err != nil {
				t.Fatal(err)
			}
			tr := &tracker{tree: tree}
			leaf := tree.Pending()[0]
			for splits := 1; ; splits++ {
				if splits > len(suffix)+1 {
					t.Fatalf("still one slice holds every series after %d splits", splits-1)
				}
				children, err := tree.Split(leaf.ID, sampleOf([]string{"pod"}, members(t, leaf.Slice, population)))
				if err != nil {
					t.Fatal(err)
				}
				assertPartition(t, tr.leaves(), population)
				largest, largestCount := children[0], -1
				for _, child := range children {
					if count := len(members(t, child.Slice, population)); count > largestCount {
						largest, largestCount = child, count
					}
				}
				if largestCount < len(population) {
					break
				}
				leaf = largest
			}
		})
	}
}

// TestSingleValueSwitchesDimension is a slice whose series all share one
// value of the dimension being cut: the split moves to the next dimension,
// keeps the slice's own condition, and never compares to the shared value.
func TestSingleValueSwitchesDimension(t *testing.T) {
	dimensions := []string{"cluster", "pod"}
	var population []series
	for index := 0; index < 60; index++ {
		population = append(population,
			series{"cluster": "a1", "pod": fmt.Sprintf("pod-%d", index)},
			series{"cluster": "b1", "pod": fmt.Sprintf("pod-%d", index)})
	}
	population = append(population, series{"pod": "x"}, series{"cluster": "a1"}, series{})
	tree, err := inputslice.New(dimensions, 1)
	if err != nil {
		t.Fatal(err)
	}
	tr := &tracker{tree: tree}
	leaf := tree.Pending()[0]
	// Cut on cluster until one slice holds only cluster a1.
	for {
		in := members(t, leaf.Slice, population)
		children, err := tree.Split(leaf.ID, sampleOf(dimensions, in))
		if err != nil {
			t.Fatal(err)
		}
		assertPartition(t, tr.leaves(), population)
		var next *inputslice.Leaf
		for index := range children {
			child := children[index]
			got := members(t, child.Slice, population)
			if len(got) > 0 && got[0]["cluster"] == "a1" {
				next = &child
			}
		}
		if next == nil {
			t.Fatal("no child holds cluster a1")
		}
		leaf = *next
		only := true
		for _, values := range members(t, leaf.Slice, population) {
			if values["cluster"] != "a1" {
				only = false
			}
		}
		if only {
			break
		}
	}
	clusterCondition := leaf.Slice.Conditions[len(leaf.Slice.Conditions)-1]
	in := members(t, leaf.Slice, population)
	children, err := tree.Split(leaf.ID, sampleOf(dimensions, in))
	if err != nil {
		t.Fatal(err)
	}
	assertPartition(t, tr.leaves(), population)
	sawPodExhausted := false
	for _, child := range children {
		conditions := child.Slice.Conditions
		if len(conditions) != 2 || conditions[0] != clusterCondition || conditions[1].Dimension != "pod" {
			t.Fatalf("child after the switch is %s, want %+v and a pod condition", describe([]execution.InputSlice{child.Slice}), clusterCondition)
		}
		for _, condition := range conditions {
			if strings.Contains(condition.Pattern, "a1") {
				t.Fatalf("condition compares to the sampled value: %q", condition.Pattern)
			}
		}
		if conditions[1].Negated && conditions[1].Pattern == "^(?s:.+)$" {
			sawPodExhausted = true
		}
	}
	if !sawPodExhausted {
		t.Fatal("the switch did not include the new dimension's root exhausted child")
	}
	spread := 0
	for _, child := range children {
		if len(members(t, child.Slice, population)) > 0 {
			spread++
		}
	}
	if spread < 2 {
		t.Fatalf("the switch put every series in one child")
	}
}

// TestSwitchFromRootCarriesNoEmptyCondition is a Query Group whose first
// dimension has one value throughout: the root is cut on the second
// dimension straight away, and the first writes no condition.
func TestSwitchFromRootCarriesNoEmptyCondition(t *testing.T) {
	dimensions := []string{"cluster", "pod"}
	var population []series
	for index := 0; index < 40; index++ {
		population = append(population, series{"cluster": "c", "pod": fmt.Sprintf("p%d", index)})
	}
	tree, err := inputslice.New(dimensions, 1)
	if err != nil {
		t.Fatal(err)
	}
	children, err := tree.Split(tree.Pending()[0].ID, sampleOf(dimensions, population))
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		if len(child.Slice.Conditions) != 1 || child.Slice.Conditions[0].Dimension != "pod" {
			t.Fatalf("child is %s, want a single pod condition", describe([]execution.InputSlice{child.Slice}))
		}
	}
	probe := append(append([]series(nil), population...), series{}, series{"cluster": "d"}, series{"pod": ""})
	assertPartition(t, (&tracker{tree: tree}).leaves(), probe)
}

// TestConcentratedOnEveryDimensionIsNamed is a slice whose series share one
// value on every dimension: no split can separate them, so the split is
// refused by name and the leaf stays as it was.
func TestConcentratedOnEveryDimensionIsNamed(t *testing.T) {
	dimensions := []string{"cluster", "pod"}
	population := []series{{"cluster": "c", "pod": "p"}, {"cluster": "c", "pod": "p"}}
	tree, err := inputslice.New(dimensions, 1)
	if err != nil {
		t.Fatal(err)
	}
	root := tree.Pending()[0]
	_, err = tree.Split(root.ID, sampleOf(dimensions, population))
	assertConcentrated(t, err, inputslice.ReasonSingleValue)
	if pending := tree.Pending(); len(pending) != 1 || pending[0].ID != root.ID || tree.Leaves() != 1 || tree.Discarded() != 0 {
		t.Fatalf("a refused split changed the tree: pending %+v, leaves %d, discarded %d", pending, tree.Leaves(), tree.Discarded())
	}
}

// TestMaxLeavesIsNamed splits without samples until the tree would pass
// MaxLeaves.
func TestMaxLeavesIsNamed(t *testing.T) {
	tree, err := inputslice.New([]string{"pod", "zone"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for {
		pending := tree.Pending()
		_, err := tree.Split(pending[0].ID, nil)
		if err != nil {
			assertConcentrated(t, err, inputslice.ReasonMaxLeaves)
			break
		}
		if tree.Leaves() > inputslice.MaxLeaves {
			t.Fatalf("tree has %d leaves", tree.Leaves())
		}
	}
	if tree.Discarded() > tree.Leaves()-1 || 2*inputslice.MaxLeaves-1 < tree.Discarded()+tree.Leaves() {
		t.Fatalf("discarded %d with %d leaves", tree.Discarded(), tree.Leaves())
	}
}

// TestInitialTreeReachesK0 builds the starting tree for every K_0 the tree
// can hold and checks it has at least that many leaves, all unread and
// together complete; a K_0 past what MaxLeaves allows is refused by name.
func TestInitialTreeReachesK0(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	dimensions := []string{"pod"}
	probe := randomPopulation(rng, dimensions, 400)
	reachable := 0
	for k0 := 0; k0 <= inputslice.MaxLeaves; k0++ {
		tree, err := inputslice.New(dimensions, k0)
		if err != nil {
			assertConcentrated(t, err, inputslice.ReasonMaxLeaves)
			if k0 <= 2 {
				t.Fatalf("K_0 %d refused", k0)
			}
			continue
		}
		reachable = k0
		if tree.Leaves() < k0 || tree.Leaves() > inputslice.MaxLeaves {
			t.Fatalf("K_0 %d gave %d leaves", k0, tree.Leaves())
		}
		if len(tree.Pending()) != tree.Leaves() || tree.Discarded() != 0 {
			t.Fatalf("K_0 %d: %d pending of %d leaves, %d discarded", k0, len(tree.Pending()), tree.Leaves(), tree.Discarded())
		}
		assertPartition(t, (&tracker{tree: tree}).leaves(), probe)
	}
	// Each split without a sample replaces one leaf by five, so the largest
	// tree reachable within 64 leaves has 1 + 4·15 = 61.
	if reachable != 61 {
		t.Fatalf("largest K_0 reached is %d, want 61", reachable)
	}
}

// TestLandAndSplitRefuseSettledLeaves is the rule that only unlanded leaves
// change.
func TestLandAndSplitRefuseSettledLeaves(t *testing.T) {
	tree, err := inputslice.New([]string{"pod"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	pending := tree.Pending()
	if err := tree.Land(pending[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := tree.Land(pending[0].ID); err == nil {
		t.Fatal("landed a leaf twice")
	}
	if _, err := tree.Split(pending[0].ID, nil); err == nil {
		t.Fatal("split a landed leaf")
	}
	if _, err := tree.Split(0, nil); err == nil {
		t.Fatal("split the already split root")
	}
	if _, err := tree.Split(1000, nil); err == nil {
		t.Fatal("split a leaf that does not exist")
	}
	if _, err := inputslice.New(nil, 1); err == nil {
		t.Fatal("built a tree with no dimension")
	}
	if _, err := inputslice.New([]string{"pod", "pod"}, 1); err == nil {
		t.Fatal("built a tree with a dimension twice")
	}
}

func assertConcentrated(t *testing.T, err error, reason string) {
	t.Helper()
	if !errors.Is(err, inputslice.ErrValueConcentrated) {
		t.Fatalf("got %v, want a value-concentrated error", err)
	}
	var concentrated *inputslice.ValueConcentratedError
	if !errors.As(err, &concentrated) || concentrated.Reason != reason {
		t.Fatalf("got %v, want reason %s", err, reason)
	}
}

// TestAStreamPrefixSampleNeverLumpsDistinctValuesForGood is a numbered fleet
// read in order, host-00 to host-23, each slice let go at the series that
// passes its room, as a slice is: the sample of every split is the front of
// the stream, never the whole slice. A class an early split took from such
// a sample lumps characters it did not see - [^0] at the tens digit holds
// both the 1x and the 2x hosts - and every character before it is "-tsoh"
// for all of them, so going deeper never separates them. The tree cuts that
// class instead, and every slice comes to fit with the series in exactly one
// slice, instead of distinct hosts being refused as one value. Rooms from
// four to nine series, as a slice limit shrinks while the slices before it
// hold their share.
func TestAStreamPrefixSampleNeverLumpsDistinctValuesForGood(t *testing.T) {
	var population []series
	for index := 0; index < 24; index++ {
		population = append(population, series{"host": fmt.Sprintf("host-%02d", index)})
	}
	for room := 4; room <= 9; room++ {
		t.Run(fmt.Sprint(room), func(t *testing.T) {
			tree, err := inputslice.New([]string{"host"}, 1)
			if err != nil {
				t.Fatal(err)
			}
			tr := &tracker{tree: tree}
			for reads := 0; len(tree.Pending()) > 0; reads++ {
				if reads > 2*inputslice.MaxLeaves-1 {
					t.Fatalf("still reading after %d slices: %s", reads, describe(tr.leaves()))
				}
				leaf := tree.Pending()[0]
				in := members(t, leaf.Slice, population)
				if len(in) <= room {
					tr.land(t, leaf)
					continue
				}
				if _, err := tree.Split(leaf.ID, sampleOf([]string{"host"}, in[:room+1])); err != nil {
					t.Fatalf("split of %d series: %v", len(in), err)
				}
				assertPartition(t, tr.leaves(), population)
			}
			assertPartition(t, tr.leaves(), population)
		})
	}
}
