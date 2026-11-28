// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// Consumed by two streams side by side, a Slot of a Plan with series and one
// without, read one slice at a time, writes what it writes consumed by one:
// the same State byte for byte, the same events, gap statements and
// completion, and the same verdict of every series (015 C1, invariant 6; the
// scenarios with no barrier, step 3.2). The slices it reads are its own: each of two consumers cuts its
// slices within a quarter of the share (015 C1 4.1), so the Slot reads more
// of them, smaller.
func TestTwoConsumersOfASlotsSlicesWriteWhatOneDoes(t *testing.T) {
	// Query permits give the read width: one slice at a time, two read side
	// by side, four (coordinator.sliceReadWidth). Two consumers read two
	// slices themselves: with four in flight, two more are read ahead.
	for name, permits := range map[string]int{"one slice at a time": 2, "two read side by side": 0, "four read side by side": 16} {
		t.Run(name, func(t *testing.T) { twoConsumersWriteWhatOneDoes(t, permits) })
	}
}

func twoConsumersWriteWhatOneDoes(t *testing.T, permits int) {
	var replayed uint64
	run := func(k int) c1K1Output {
		worker.SetSliceConsumersForTest(t, k)
		fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
		fixture.queryPermits = permits
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		plans, requirements, request := withCompletionOnlyPlan(t)
		fixture.source.plans, fixture.source.requirements = plans, requirements
		output, result := c1K1RunResult(t, fixture, request, false)
		if k > 1 {
			replayed = result.Usage.InputSlicesReadAhead
		}

		return output
	}
	// Each series' verdict, without the series its Plan counted so far -
	// read so far, which the slices' bounds set - and without the lines that
	// merge a batch's routine evaluations of a Plan, which the batches set:
	// every count read from them is the same however they are merged.
	verdicts := func(lines []string) []string {
		var kept []string
		for _, line := range lines {
			fields := strings.Fields(line)
			if strings.HasSuffix(fields[0], "/") {
				continue
			}
			kept = append(kept, strings.Join(fields[:len(fields)-1], " "))
		}
		sort.Strings(kept)
		return kept
	}
	one := run(1)
	var consumed []int
	worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
	two := run(2)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 {
		t.Fatalf("the two consumers consumed %v slices, want both some: the Slot was not consumed side by side", consumed)
	}
	// Read ahead: the slices no consumer has, as many as the width leaves
	// beside the two, each replayed by the consumer that took it.
	if readAhead := permits == 16; readAhead != (replayed > 0) {
		t.Fatalf("two consumers replayed %d slices read ahead, want some only with the width past two", replayed)
	}
	for _, field := range []struct {
		name     string
		one, two any
	}{
		{"completed", one.Completed, two.Completed}, {"kind", one.Kind, two.Kind}, {"State", one.States, two.States},
		{"event count", one.EventCount, two.EventCount}, {"events", one.Events, two.Events}, {"gap statements", one.Gaps, two.Gaps},
		{"completion", string(one.Completion), string(two.Completion)}, {"series' verdicts", verdicts(one.Lines), verdicts(two.Lines)},
	} {
		if !reflect.DeepEqual(field.one, field.two) {
			t.Errorf("two consumers wrote %s\n%v\nwhere one wrote\n%v", field.name, field.two, field.one)
		}
	}
}

// sideBySideSame fails on any of the outputs invariant 6 holds k streams to
// that differ between one and two: State, events, gap statements, the
// completion and every series' verdict (015 C1).
func sideBySideSame(t *testing.T, name string, one, two c1K1Output) {
	t.Helper()
	verdicts := func(lines []string) []string {
		var kept []string
		for _, line := range lines {
			fields := strings.Fields(line)
			if strings.HasSuffix(fields[0], "/") {
				continue
			}
			kept = append(kept, strings.Join(fields[:len(fields)-1], " "))
		}
		sort.Strings(kept)
		return kept
	}
	for _, field := range []struct {
		name     string
		one, two any
	}{
		{"completed", one.Completed, two.Completed}, {"kind", one.Kind, two.Kind}, {"State", one.States, two.States},
		{"event count", one.EventCount, two.EventCount}, {"events", one.Events, two.Events}, {"gap statements", one.Gaps, two.Gaps},
		{"completion", string(one.Completion), string(two.Completion)}, {"series' verdicts", verdicts(one.Lines), verdicts(two.Lines)},
	} {
		if !reflect.DeepEqual(field.one, field.two) {
			t.Errorf("%s: two consumers wrote %s\n%v\nwhere one wrote\n%v", name, field.name, field.two, field.one)
		}
	}
}

// A Plan new to its activation - one never run, and one stopped and started
// again, a series of it written before in a later slice - read in slices,
// warms up however its slices are cut and consumed: one stream, two
// consumers, two consumers of slices half as large. A Slot read in slices
// judges no Plan first-seen (first_seen.go), so every one of them writes the
// same events and Level verdicts, and counts the Plan warmed up, sliced_read.
// Read whole, the Plan never run is judged first-seen and written that Slot,
// the one run before is found to have run.
func TestTwoConsumersWarmUpAPlanNewToItsActivationAsOneDoes(t *testing.T) {
	var usage execution.SlotBudgetUsage
	run := func(k int, pool uint64, prior []string) (*slicedFixture, c1K1Output) {
		worker.SetSliceConsumersForTest(t, k)
		fixture, seen := newSlicedFirstSeenFixtureOf(t, slicedHosts(24, 40<<10), prior, 200)
		fixture.queryPermits, fixture.pool = 2, pool
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: seen, State: seen,
			GapGuard: &markerStore{t: t, ports: fixture.ports}, Events: fixture.ports}, 200)
		output, result := c1K1RunResult(t, fixture, windowedRequest(), false)
		usage = result.Usage
		return fixture, output
	}
	learn, _ := run(1, 0, nil)
	var held []string
	for _, slice := range learn.source.landed() {
		if len(slice.delivered) != 0 {
			held = append(held, slice.delivered[0])
		}
	}
	if len(held) < 3 {
		t.Fatalf("fixture: %d slices hold series, want three at least", len(held))
	}
	for name, prior := range map[string][]string{"never run": nil, "stopped and started again": {held[2]}} {
		t.Run(name, func(t *testing.T) {
			_, one := run(1, 0, prior)
			if usage.InputSlices < 3 || usage.FirstSeenSlicedRead != 1 || usage.FirstSeenJudged != 0 || one.EventCount != 0 {
				t.Fatalf("one stream read %d slices, counted %d Plans warmed up read in slices and %d first-seen, raised %d "+
					"events; want the one Plan warmed up, no event", usage.InputSlices, usage.FirstSeenSlicedRead,
					usage.FirstSeenJudged, one.EventCount)
			}
			for _, cut := range []struct {
				name string
				pool uint64
			}{{"two consumers", 0}, {"two consumers of slices half as large", slicedPool / 2}} {
				var consumed []int
				worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
				_, two := run(2, cut.pool, prior)
				if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 {
					t.Fatalf("%s consumed %v slices, want both some", cut.name, consumed)
				}
				if usage.FirstSeenSlicedRead != 1 || usage.FirstSeenJudged != 0 {
					t.Fatalf("%s counted %d Plans warmed up read in slices and %d first-seen, want the one warmed up",
						cut.name, usage.FirstSeenSlicedRead, usage.FirstSeenJudged)
				}
				sideBySideSame(t, name+", "+cut.name, one, two)
			}
			_, whole := run(1, wholePool, prior)
			judged, ranBefore, events := uint64(1), uint64(0), 24
			if prior != nil {
				judged, ranBefore, events = 0, 1, 0
			}
			if usage.InputSlices != 1 || usage.FirstSeenJudged != judged || usage.FirstSeenRanBefore != ranBefore ||
				usage.FirstSeenSlicedRead != 0 || whole.EventCount != events {
				t.Fatalf("read whole in %d slices, the Plan counted %d first-seen, %d run before, %d warmed up read in slices, "+
					"%d events raised; want one slice, %d, %d, none and %d", usage.InputSlices, usage.FirstSeenJudged,
					usage.FirstSeenRanBefore, usage.FirstSeenSlicedRead, whole.EventCount, judged, ranBefore, events)
			}
		})
	}
}

// spreadHosts is count series whose hosts' last two characters fall in
// different classes of the fixed alphabet a Slot's starting slices are cut
// by (inputslice.SuffixCut), so most of the slices it starts in hold some:
// slicedHosts' host-NN all end in a digit, and start in two.
func spreadHosts(count, padding int) []slicedSeries {
	ends := []string{"00", "91", "i2", "r3", "0a", "9b", "ic", "rd", "xi", "xj", "xr", "xs"}
	series := make([]slicedSeries, count)
	for index := range series {
		series[index] = slicedSeries{host: fmt.Sprintf("h%02d-%s", index, ends[index%len(ends)]), padding: padding}
	}
	return series
}

// startingLeaves is how many slices a Slot whose last read left twelve on
// host starts in: the tree cut level by level to at least that many.
func startingLeaves(t *testing.T) int {
	t.Helper()
	tree, err := inputslice.NewCut([]string{"host"}, 12, inputslice.SuffixCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	return len(tree.Pending())
}

// On the same slices, one of them answering PARTIAL, two consumers write
// what one stream does: the marker the slice's head grows, the windows of
// three, every series' verdict - and, with a Plan of no series beside, what
// the Slot finishes it on: the slice numbered last's own completion, as one
// stream finishes on the slice it read last, not the Slot's completion
// folded, which takes the PARTIAL of another slice. Which slice answers
// short follows what it holds - the one with a given host - not when it was
// read, so it is the same slice on both sides (the golden's own scenario
// picks it by when, which k = 2's order moves).
func TestTwoConsumersOnTheSameSlicesOneOfThemPartialWriteWhatOneDoes(t *testing.T) {
	for _, plans := range []struct {
		name           string
		completionOnly bool
	}{{"one Plan", false}, {"a Plan of no series beside", true}} {
		t.Run(plans.name, func(t *testing.T) { sameSlicesOnePartial(t, plans.completionOnly) })
	}
}

func sameSlicesOnePartial(t *testing.T, completionOnly bool) {
	leaves := startingLeaves(t)
	run := func(k int, partialHost string) (*slicedFixture, c1K1Output) {
		worker.SetSliceConsumersForTest(t, k)
		fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 3)
		fixture.queryPermits = 2
		fixture.ports.gapMissing = true
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 3)
		fixture.source.completenessOf = func(delivered []string) execution.Completeness {
			if partialHost != "" && slices.Contains(delivered, partialHost) {
				return execution.CompletenessPartial
			}
			return execution.CompletenessFull
		}
		request := windowedRequest()
		if completionOnly {
			var plans []execution.DuePlan
			var requirements []execution.DataRequirement
			plans, requirements, request = withCompletionOnlyPlan(t)
			fixture.source.plans, fixture.source.requirements = plans, requirements
		}
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		return fixture, c1K1Run(t, fixture, request, false)
	}
	learn, _ := run(1, "")
	var held []string
	for _, slice := range learn.source.landed() {
		if len(slice.delivered) != 0 {
			held = append(held, slice.delivered[0])
		}
	}
	if landed := len(learn.source.landed()); landed != leaves || len(held) < 3 {
		t.Fatalf("fixture: one stream read %d slices, %d holding series; want the %d it starts in, three holding some at least",
			landed, len(held), leaves)
	}
	partialHost := held[len(held)/2]
	oneFixture, one := run(1, partialHost)
	var consumed []int
	worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
	twoFixture, two := run(2, partialHost)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 {
		t.Fatalf("the two consumers consumed %v slices, want both some", consumed)
	}
	for name, fixture := range map[string]*slicedFixture{"one stream": oneFixture, "two consumers": twoFixture} {
		if landed := len(fixture.source.landed()); landed != leaves || !fixture.source.partial[partialHost] {
			t.Fatalf("fixture: %s read %d slices, the one holding %s PARTIAL: %v; want the %d it starts in, that one short",
				name, landed, partialHost, fixture.source.partial[partialHost], leaves)
		}
	}
	sideBySideSame(t, "a slice PARTIAL", one, two)
}

// A process of four cores or more consumes a Slot's slices two at a time by
// default (015 C1 section 9), from the Slot's second slice - its first read
// on its own stream, the header saying the read width - and writes what one
// stream does, on the same slices.
func TestAProcessOfFourCoresConsumesASlotsSlicesSideBySideByDefault(t *testing.T) {
	run := func(byCores bool) (c1K1Output, []int) {
		if byCores {
			worker.SetSliceConsumersByCoresForTest(t, 4)
		} else {
			worker.SetSliceConsumersForTest(t, 1)
		}
		fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
		fixture.queryPermits = 16
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		request := windowedRequest()
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		var consumed []int
		worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
		return c1K1Run(t, fixture, request, false), consumed
	}
	one, _ := run(false)
	two, consumed := run(true)
	leaves := startingLeaves(t)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 || consumed[0]+consumed[1] != leaves-1 {
		t.Fatalf("by four cores two consumers consumed %v of %d slices, want both some and every slice but the first, "+
			"read on the Slot's own stream", consumed, leaves)
	}
	sideBySideSame(t, "k by four cores", one, two)
}

// A slice split after a slice past it was handed, a slice past both
// answering PARTIAL: two consumers write what one stream does (015 C1 3.2,
// the deviation it left to check). The Slot starts in the same slices on
// both sides; one holds four hosts too large together for the limit, each
// fits alone. A slice is split by the values its read sampled before it
// went past its limit, so the two have the same limit - two consumers a pool
// twice one stream's, a quarter of the share each as one stream has half -
// and split it into the same children. One
// stream reads the children before the slices after; with two consumers the
// slice after it is handed first, and the children are numbered after it -
// out of what that slice's head counts so far, the slice after them being
// the one short.
func TestTwoConsumersWithASliceSplitAfterTheOnePastItWasHandedWriteWhatOneDoes(t *testing.T) {
	series := spreadHosts(24, 4<<10)
	// The four whose last character is in one class (i to q), each its
	// second-last in another: one starting slice, one child each.
	for index, name := range map[int]string{8: "h08-0i", 9: "h09-9j", 20: "h20-ik", 21: "h21-rl"} {
		series[index] = slicedSeries{host: name, padding: 100 << 10}
	}
	// What a read delivers is its series by digest.
	partialHost := series[10].digest()
	run := func(k int) (*slicedFixture, c1K1Output) {
		worker.SetSliceConsumersForTest(t, k)
		fixture := newSlicedFixture(t, series, sliceableFacts(), 200)
		fixture.queryPermits = 2
		fixture.ports.gapMissing = true
		fixture.pool = uint64(k) * slicedPool
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		fixture.source.completenessOf = func(delivered []string) execution.Completeness {
			if slices.Contains(delivered, partialHost) {
				return execution.CompletenessPartial
			}
			return execution.CompletenessFull
		}
		request := windowedRequest()
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		return fixture, c1K1Run(t, fixture, request, false)
	}
	read := func(fixture *slicedFixture) []string {
		var landed []string
		for _, call := range fixture.source.landed() {
			landed = append(landed, call.slice.Digest()+":"+strings.Join(call.delivered, ","))
		}
		sort.Strings(landed)
		return landed
	}
	oneFixture, one := run(1)
	var consumed []int
	worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
	twoFixture, two := run(2)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 {
		t.Fatalf("the two consumers consumed %v slices, want both some", consumed)
	}
	// Split once, into its four classes and the slice of none: four slices
	// more than the Slot starts in, on both sides.
	if a, b := read(oneFixture), read(twoFixture); len(a) != startingLeaves(t)+4 || !reflect.DeepEqual(a, b) {
		t.Fatalf("fixture: one stream landed the slices %v, two consumers %v; want the same, the starting slices with one split",
			a, b)
	}
	if !oneFixture.source.partial[partialHost] || !twoFixture.source.partial[partialHost] {
		t.Fatalf("fixture: the slice holding %s did not answer PARTIAL on both sides", series[10].host)
	}
	sideBySideSame(t, "a slice split after the one past it was handed", one, two)
}

// A Slot that starts in the slices its last read left, on a dimension its
// Query Group is not grouped by - cloud, the Query Group grouped by host -
// is read whole by two consumers as by one stream: the first Begin finds
// the dimension gone, and the Slot reads whole on its own stream, the other
// consumers' reads of slices it no longer has let go with nothing of them
// kept, none begun once the dimensions were forgotten - begun and consumed,
// one would be read beside the whole read.
func TestTwoConsumersOfASlotWhoseSliceDimensionWentReadItWholeAsOneDoes(t *testing.T) {
	run := func(k int) (*slicedFixture, c1K1Output) {
		worker.SetSliceConsumersForTest(t, k)
		fixture := newSlicedFixture(t, slicedHosts(24, 4<<10), sliceableFacts(), 200)
		fixture.queryPermits = 2
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		request := windowedRequest()
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"cloud"}, inputslice.Shape{}, 12, 1, 24)
		return fixture, c1K1Run(t, fixture, request, false)
	}
	wholeOnce := func(name string, fixture *slicedFixture) {
		t.Helper()
		landed := fixture.source.landed()
		if len(landed) != 1 || len(landed[0].slice.Conditions) != 0 || len(landed[0].delivered) != 24 {
			t.Fatalf("%s landed %d reads, want the Slot read whole once, every series in it", name, len(landed))
		}
	}
	oneFixture, one := run(1)
	wholeOnce("one stream", oneFixture)
	twoFixture, two := run(2)
	wholeOnce("two consumers", twoFixture)
	sideBySideSame(t, "the slices' dimension gone", one, two)
}

// A Slot one of whose consumers' reads fails ends with every consumer
// stopped (015 C1 section 7): the Slot fails, and no consumer's goroutine
// outlives it - one left running would wait on the dispatch for good.
func TestASlotWhoseConsumersReadFailsStopsEveryConsumer(t *testing.T) {
	worker.SetSliceConsumersForTest(t, 2)
	fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
	fixture.queryPermits = 2
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
		Events: fixture.ports}, 200)
	failed := errors.New("injected: the slice's query failed")
	fixture.source.failSlice = func(slice execution.InputSlice) error {
		if in, err := slice.Matches(map[string]string{"host": spreadHosts(24, 0)[13].host}); err == nil && in && !slice.IsZero() {
			return failed
		}
		return nil
	}
	request := windowedRequest()
	worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
		[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
	if _, err := fixture.coordinator.Execute(context.Background(), request); err == nil {
		t.Fatal("a Slot one of whose slices' reads failed completed")
	}
	testwait.Until(t, "every consumer of the failed Slot ended", func() bool {
		stacks := make([]byte, 1<<20)
		return !bytes.Contains(stacks[:runtime.Stack(stacks, true)], []byte("(*sliceConsumer).run"))
	})
}

// Two consumers of a Slot whose per-Slot State cap is four - two State
// writes a window for one stream, one for each of two consumers (015 C1 4.2)
// - write what one stream does. Whether a merge comes past the cap side by
// side follows how fast the windows land; the barrier it is judged again
// behind is held to its own case.
func TestTwoConsumersPastTheSlotsCapWriteWhatOneDoes(t *testing.T) {
	run := func(k int) c1K1Output {
		worker.SetSliceConsumersForTest(t, k)
		fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 4)
		fixture.queryPermits = 2
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 4)
		return c1K1Run(t, fixture, windowedRequest(), false)
	}
	one := run(1)
	var consumed []int
	worker.SetSlicesBySubstreamForTest(t, func(by []int, _ uint64) { consumed = by })
	two := run(2)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 {
		t.Fatalf("the two consumers consumed %v slices, want both some", consumed)
	}
	sideBySideSame(t, "a cap of four", one, two)
}
