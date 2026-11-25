// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// shortReplay is a replay short of time's read of qg's Slot at evaluation, of
// slice, ready now, its normal round's budget budget and its frozen schedule's
// next Slot interval on.
func shortReplay(f *fixture, evaluation int64, slice execution.InputSlice, budget, interval time.Duration) Query {
	q := query("qg", evaluation, minute, sourceLog)
	q.Operation, q.ReplayShortOfTime, q.Slice = execution.OperationReplay, true, slice
	q.ReadyAt = f.clock.now()
	q.NormalDeadline = q.ReadyAt.Add(budget)
	q.FollowingSlot = execution.EvaluationTime(evaluation + int64(interval/time.Second))
	return q
}

// readFor begins q, moves the clock took on and completes it whole.
func readFor(t *testing.T, f *fixture, q Query, took time.Duration) {
	t.Helper()
	read := f.engine.Begin(q)
	if read == nil {
		t.Fatalf("the replay short of time of the Slot at %d was not timed", q.Contract.Slot.EvaluationTime)
	}
	f.clock.set(f.clock.now().Add(took))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
}

// A replay short of time read in slices is timed by its round, from its
// first slice's send, against its normal round's budget (014 next9a, the
// boundary of B' and E). A round within it leaves the round's reading; a slice
// that takes the round past it takes back the reading the earlier slices left,
// and the Slot's later slices leave none: a part of the round within the
// budget is no reading of what the round took, and kept, it would divert the
// next round to a replay that reads as long, the group never running the
// normal round the degraded pool judges it by. Before, only the slice past the
// budget was left out, and the reading the first slice left stayed.
func TestAReplayShortOfTimeReadInSlicesIsTimedByItsRound(t *testing.T) {
	f := newFixture(t)
	start := f.clock.now().Truncate(time.Minute)
	f.clock.set(start)
	const budget = 20 * time.Second
	readFor(t, f, shortReplay(f, start.Unix(), sliceOf("a*"), budget, time.Minute), 8*time.Second)
	readFor(t, f, shortReplay(f, start.Unix(), sliceOf("b*"), budget, time.Minute), 5*time.Second)
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 13*time.Second {
		t.Fatalf("a round of 13 s within its 20 s: the reading is %v %v, want the round's 13 s", got, ok)
	}

	next := start.Add(time.Minute)
	f.clock.set(next)
	readFor(t, f, shortReplay(f, next.Unix(), sliceOf("a*"), budget, time.Minute), 15*time.Second)
	readFor(t, f, shortReplay(f, next.Unix(), sliceOf("b*"), budget, time.Minute), 10*time.Second)
	readFor(t, f, shortReplay(f, next.Unix(), sliceOf("c*"), budget, time.Minute), time.Second)
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 13*time.Second {
		t.Fatalf("a round past its 20 s, its first slice of 15 s within it: the reading is %v %v, want the earlier round's 13 s "+
			"alone", got, ok)
	}
	state := f.engine.groups["qg"]
	if state.firstReads.count != 1 {
		t.Fatalf("the group keeps %d readings, want the earlier round's one", state.firstReads.count)
	}
}

// A replay short of time's reading ages by its Slot's frozen schedule, as a
// normal first read's does: sixteen of its rounds, by the interval to its next
// Slot, not sixteen of its query's steps. A group whose Slots are twenty
// steps apart and that no normal round has read here keeps a replay's reading
// to its next round. Before, such a group's readings aged by its step, each
// before the next round came: that round ran on the fallback.
func TestAReplayShortOfTimesReadingAgesByItsSlotsInterval(t *testing.T) {
	f := newFixture(t)
	start := f.clock.now().Truncate(time.Minute)
	f.clock.set(start)
	const interval = 20 * time.Minute
	readFor(t, f, shortReplay(f, start.Unix(), execution.InputSlice{}, 30*time.Second, interval), 5*time.Second)
	read := f.clock.now()

	f.clock.set(read.Add(interval))
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 5*time.Second {
		t.Fatalf("its next round, %v on: the reading is %v %v, want the replay's 5 s", interval, got, ok)
	}
	f.clock.set(read.Add(readingsKept*interval + time.Second))
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 0 {
		t.Fatalf("past sixteen of its rounds: the reading is %v %v, want none, the group measured", got, ok)
	}
}

// A replay short of time slower than its normal round's budget leaves no
// reading, read whole or in slices, as no normal round could have read it.
func TestAReplayShortOfTimeSlowerThanItsNormalRoundLeavesNoReading(t *testing.T) {
	f := newFixture(t)
	start := f.clock.now().Truncate(time.Minute)
	f.clock.set(start)
	readFor(t, f, shortReplay(f, start.Unix(), execution.InputSlice{}, 20*time.Second, time.Minute), 21*time.Second)
	readFor(t, f, shortReplay(f, start.Unix()+60, sliceOf("a*"), 20*time.Second, time.Minute), 21*time.Second)
	if got, ok := f.engine.QueryDuration("qg"); ok {
		t.Fatalf("the group's reading is %v, want none", got)
	}
}

// A Slot read in slices that takes its round past its normal round's budget
// takes its reading back from a ring already full - a group's ring is full
// once it has read sixteen rounds, which is most groups - and the next reading
// takes that place: no other reading of the group is written over for it, and
// every place is still read. Before, the place was cleared and the next
// reading wrote over the oldest real one, the ring one reading short for
// fifteen more.
func TestAReadingTakenBackFromAFullRingLeavesItsPlaceToTheNextReading(t *testing.T) {
	f := newFixture(t)
	start := f.clock.now().Truncate(time.Minute)
	for round := range readingsKept {
		took := time.Duration(1+round%5) * time.Second
		switch round {
		case 1:
			took = 8 * time.Second
		case readingsKept - 1:
			took = 9 * time.Second
		}
		f.clock.set(start.Add(time.Duration(round) * time.Minute))
		wholeFirstRead(t, f, start.Unix()+int64(round)*60, took)
	}
	state := f.engine.groups["qg"]
	if state.firstReads.count != readingsKept {
		t.Fatalf("fixture: %d readings, want the ring full", state.firstReads.count)
	}

	replay := start.Add(readingsKept * time.Minute)
	f.clock.set(replay)
	readFor(t, f, shortReplay(f, replay.Unix(), sliceOf("a*"), 20*time.Second, time.Minute), 15*time.Second)
	readFor(t, f, shortReplay(f, replay.Unix(), sliceOf("b*"), 20*time.Second, time.Minute), 10*time.Second)
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 9*time.Second {
		t.Fatalf("the round past its budget taken back: the reading is %v %v, want the longest left, the last normal round's 9 s", got, ok)
	}

	next := replay.Add(time.Minute)
	f.clock.set(next)
	wholeFirstRead(t, f, next.Unix(), 2*time.Second)
	kept := false
	for index := range state.firstReads.count {
		kept = kept || state.firstReads.values[index] == 8*time.Second
	}
	if !kept || state.firstReads.count != readingsKept {
		t.Fatalf("the next reading wrote over another (round 1's 8 s kept: %t, %d readings): the ring holds %v", kept,
			state.firstReads.count, state.firstReads.values)
	}
}
