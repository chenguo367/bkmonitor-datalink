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
	"cmp"
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// wholePool is a pool the first-seen fixture's 24 series of 40 KiB, about
// 1 MiB, fit in whole: a share of 6 MiB, the Slot read in one read.
const wholePool = 8 << 20

// firstSeenSlot runs one attempt of a Slot of a Plan new to its activation,
// without a marker, over the fixture's 24 series, the State of prior written
// before it, on a pool of pool (slicedPool when zero): what it sent as
// events and State, and its result - retrying PLAN_REACTIVATED when the
// Plan warms up, its marker written.
func firstSeenSlot(t *testing.T, pool uint64, prior []string) (events, states int, result execution.SlotExecutionResult,
	fixture *slicedFixture) {
	t.Helper()
	events, states, result, fixture, _ = firstSeenSlotReading(t, pool, 200, prior)
	return events, states, result, fixture
}

// firstSeenSlotReading is firstSeenSlot with windows of maxStates State
// writes at most, and the State reads the Slot made ahead of its own
// preflight, the first-seen pre-pass's.
func firstSeenSlotReading(t *testing.T, pool, maxStates uint64, prior []string) (events, states int, result execution.SlotExecutionResult,
	fixture *slicedFixture, ahead int64) {
	t.Helper()
	fixture, seen := newSlicedFirstSeenFixtureOf(t, slicedHosts(24, 40<<10), prior, maxStates)
	fixture.queryPermits, fixture.pool = 2, pool
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: seen, State: seen,
		GapGuard: &markerStore{t: t, ports: fixture.ports}, Events: fixture.ports}, maxStates)
	result, err := fixture.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil {
		t.Fatalf("Execute() err=%v", err)
	}
	return fixture.ports.eventCount, len(fixture.state.saved), result, fixture, seen.aheadReads.Load()
}

// A Plan new to its activation that ran before - one series' State written
// before the Slot - warms up, read in slices however they are cut and
// whichever slice holds that series: nothing of it is written, no State and
// no event. Judged first-seen by the slices read before the one holding it,
// the series of those slices were written without warming and raised events
// the warm-up holds back, as many as the cut put before it - none to sixteen
// of twenty-four here. Read whole, the same Plan never run raises every
// series' event: the case sees events where there are some.
func TestAPlanThatRanWarmsUpWhicheverSliceItsStateFromBeforeIsIn(t *testing.T) {
	if events, _, result, _ := firstSeenSlot(t, wholePool, nil); events != 24 || result.Usage.InputSlices != 1 {
		t.Fatalf("a Plan that never ran, read whole, raised %d events in %d slices, want every series' in one",
			events, result.Usage.InputSlices)
	}
	_, _, _, learn := firstSeenSlot(t, 0, nil)
	var order []string
	for _, slice := range learn.source.landed() {
		order = append(order, slice.delivered...)
	}
	if len(order) != 24 {
		t.Fatalf("fixture: %d series read, want 24", len(order))
	}
	for _, pool := range []uint64{0, slicedPool / 2} {
		for _, at := range []int{0, 6, 12, 18} {
			events, states, _, fixture := firstSeenSlot(t, pool, []string{order[at]})
			if events != 0 || states != 0 {
				t.Errorf("pool %d, State from before in series %d of 24 (%d slices): %d events and %d series' State written; "+
					"want none, the Plan ran and warms up", cmp.Or(pool, slicedPool), at, len(fixture.source.landed()), events, states)
			}
		}
	}
}

// A Plan new to its activation that never ran warms up too when its Slot is
// read in slices: whether it ran is known only once its every series' State
// is read, which a Slot read in slices has not done when it writes its first
// window, so it writes nothing this Slot and detects from the next - a
// period late, as before first-seen. The completion row counts it under its
// verdict, sliced_read, and no State is read ahead of the Slot's own
// preflight for a judgement it does not make - as the same Slot read whole
// and written in windows of three does, to judge it before its first.
func TestAPlanNewToItsActivationReadInSlicesWarmsUp(t *testing.T) {
	if _, _, _, _, ahead := firstSeenSlotReading(t, wholePool, 3, nil); ahead == 0 {
		t.Fatal("fixture: a Slot read whole and written in windows read no State ahead of its first window")
	}
	events, states, result, fixture, ahead := firstSeenSlotReading(t, 0, 200, nil)
	if ahead != 0 {
		t.Fatalf("the Slot read in slices read State %d times ahead of its preflight, for no judgement", ahead)
	}
	if result.Usage.InputSlices < 3 {
		t.Fatalf("fixture: the Slot was read in %d slices, want three at least", result.Usage.InputSlices)
	}
	if events != 0 || states != 0 || satisfiedMarkers(fixture.ports.gapMutations) != 0 {
		t.Fatalf("a Plan never run, read in slices, raised %d events, wrote %d series' State and %d first-seen markers; "+
			"want none, it warms up", events, states, satisfiedMarkers(fixture.ports.gapMutations))
	}
	if usage := result.Usage; usage.FirstSeenSlicedRead != 1 || usage.FirstSeenJudged != 0 || usage.FirstSeenRanBefore != 0 {
		t.Fatalf("the completion row counts %d Plans first-seen, %d run before and %d warmed up read in slices; want the one "+
			"warmed up read in slices", usage.FirstSeenJudged, usage.FirstSeenRanBefore, usage.FirstSeenSlicedRead)
	}
}

// Read whole, a Plan new to its activation that never ran is still judged
// first-seen: every one of its series is in the one read, its State read
// before anything is written, and it writes them all this Slot, recording
// that it ran with a satisfied marker. The completion row counts it judged
// first-seen; the same Plan with its State from before read whole is
// counted as run before.
func TestAPlanNewToItsActivationReadWholeIsJudgedFirstSeen(t *testing.T) {
	events, states, result, fixture := firstSeenSlot(t, wholePool, nil)
	if result.Usage.InputSlices != 1 {
		t.Fatalf("fixture: the Slot was read in %d slices, want one", result.Usage.InputSlices)
	}
	if events != 24 || states != 24 || satisfiedMarkers(fixture.ports.gapMutations) != 1 {
		t.Fatalf("a Plan never run, read whole, raised %d events, wrote %d series' State and %d first-seen markers; "+
			"want 24, 24 and one", events, states, satisfiedMarkers(fixture.ports.gapMutations))
	}
	if usage := result.Usage; usage.FirstSeenJudged != 1 || usage.FirstSeenRanBefore != 0 || usage.FirstSeenSlicedRead != 0 {
		t.Fatalf("the completion row counts %d Plans first-seen, %d run before and %d warmed up read in slices; want the one "+
			"first-seen", usage.FirstSeenJudged, usage.FirstSeenRanBefore, usage.FirstSeenSlicedRead)
	}
	_, _, _, learn := firstSeenSlot(t, wholePool, nil)
	events, states, result, _ = firstSeenSlot(t, wholePool, learn.source.landed()[0].delivered[:1])
	if usage := result.Usage; events != 0 || states != 0 || usage.FirstSeenRanBefore != 1 || usage.FirstSeenJudged != 0 {
		t.Fatalf("a Plan that ran, read whole, raised %d events and wrote %d series' State, counted %d first-seen and %d run "+
			"before; want nothing written, the one run before", events, states, usage.FirstSeenJudged, usage.FirstSeenRanBefore)
	}
}
