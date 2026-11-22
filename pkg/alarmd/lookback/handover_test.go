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
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// respecs answers a restored Slot's planning with the plan given, and says
// what it was asked.
type respecs struct {
	mu    sync.Mutex
	plan  RestoredPlan
	err   error
	asked []HandoverSlot
}

func (planned *respecs) respec(_ context.Context, queryGroup execution.QueryGroupIdentity, evaluation execution.EvaluationTime,
	readHoldMillis int64) (RestoredPlan, error) {
	planned.mu.Lock()
	defer planned.mu.Unlock()
	planned.asked = append(planned.asked, HandoverSlot{Evaluation: evaluation, ReadHoldMillis: readHoldMillis, Source: string(queryGroup)})
	return planned.plan, planned.err
}

// wholeRead is a restored plan of the Slot's query read whole, keeping every
// series.
func wholeRead(slot int64) RestoredPlan {
	return RestoredPlan{Specs: []execution.PhysicalQuerySpec{query("qg", slot, minute, sourceLog).Spec}, Slices: []execution.InputSlice{{}},
		Shaped: true}
}

// slicesAsked records the slice each reread asked for, wrapping f's recheck.
func slicesAsked(f *fixture) *[]execution.InputSlice {
	var mu sync.Mutex
	asked := &[]execution.InputSlice{}
	recheck := f.engine.options.Recheck
	f.engine.options.Recheck = func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		mu.Lock()
		*asked = append(*asked, execution.InputSliceFrom(ctx))
		mu.Unlock()
		return recheck(ctx, spec, sink)
	}
	return asked
}

// A Slot on a series_late Query Group's reread ladder, handed over with the
// Query Group, is read again by its next owner at its rung's delay from its
// first read: its queries planned again with its read hold - its first read
// was not there - and read in the slices the Query Group is read in there,
// whole here, and every series of the read handed to the supplement, which
// tells those decided already. The series_late state goes with it. The
// supplement's facts say how it told them: by the segment, by the State's
// version, or read and decided.
func TestASlotHandedOverIsReadAgainAndEverySeriesSupplemented(t *testing.T) {
	previous, _ := directedFixture(t, SupplementOutcome{})
	slot := previous.clock.now().Unix()
	previous.firstRead(slot, execution.CompletenessFull)
	readAt := previous.clock.now()
	handover := previous.engine.ForgetHandingOver("qg", ForgottenMoved)
	if handover == nil || len(handover.Slots) != 1 || handover.Slots[0].Evaluation != execution.EvaluationTime(slot) ||
		handover.Slots[0].ReadAtMillis != readAt.UnixMilli() || handover.SeriesLate == nil {
		t.Fatalf("handed over %+v, want the Slot on its ladder and the series_late state", handover)
	}
	if stats := previous.engine.Stats(); stats.GroupsForgotten[ForgottenMoved] != 1 {
		t.Fatalf("forgotten %v, want the Query Group forgotten as moved", stats.GroupsForgotten)
	}

	next := newFixture(t)
	next.clock.set(readAt)
	recorder := &supplementRecorder{outcome: SupplementOutcome{Ran: true, Facts: execution.SupplementFacts{Candidates: 3,
		CrossedT: 2, CoveredT: 1, Unreached: 1, Admitted: 1, Points: 1}}}
	next.engine.options.Supplement = recorder.run
	planned := &respecs{plan: wholeRead(slot)}
	next.engine.options.Respec = planned.respec
	next.engine.Restore("qg", handover)
	if stats := next.engine.Stats(); stats.HandoverSlots[HandoverSlotRestored] != 1 || stats.HandoverLate[HandoverLateRestored] != 1 {
		t.Fatalf("restored %v and %v, want the Slot and the series_late state", stats.HandoverSlots, stats.HandoverLate)
	}
	next.clock.set(readAt.Add(rungDelay(handover.Slots[0].Rung, minute)))
	next.answers <- delivered(point(slot, "1"), dataset("h2", map[int64]string{slot - 60: "5"}))
	next.engine.Step(context.Background())
	stats := next.waitFor(func(stats Stats) bool { return stats.Sources[sourceLog].SupplementWindows[DirectedSupplemented] == 1 })
	planned.mu.Lock()
	asked := append([]HandoverSlot(nil), planned.asked...)
	planned.mu.Unlock()
	if len(asked) != 1 || asked[0].Evaluation != execution.EvaluationTime(slot) || asked[0].ReadHoldMillis != handover.Slots[0].ReadHoldMillis {
		t.Fatalf("planned %+v, want the Slot planned again with its read hold", asked)
	}
	jobs := recorder.taken()
	if len(jobs) != 1 || !jobs[0].All || len(jobs[0].Series) != 0 || jobs[0].EvaluationTime != execution.EvaluationTime(slot) {
		t.Fatalf("jobs %+v, want one supplement of every series the read returned", jobs)
	}
	sink := &replaySink{}
	if _, err := jobs[0].Read.Replay(context.Background(), planned.plan.Specs[0], sink); err != nil || len(sink.batches) != 2 {
		t.Fatalf("the read replayed %d series (%v), want both it read - h1 too, no first read to leave it by", len(sink.batches), err)
	}
	if want := map[string]uint64{RestoredDedupSegment: 1, RestoredDedupPersistedVersion: 1, RestoredDedupRead: 1}; !reflect.DeepEqual(stats.RestoredDedup, want) {
		t.Fatalf("restored dedup %v, want %v", stats.RestoredDedup, want)
	}
}

// A Slot handed over past its rung's window when it comes is let go; one of
// a Query Group read directed on neither the previous owner's word nor its
// own samples here has no ladder to go on; a series_late state handed to a
// Query Group whose own samples here decide it is set aside. Each counted.
func TestWhatIsHandedOverIsTakenOnlyWhereItStillStands(t *testing.T) {
	f := newFixture(t)
	now := f.clock.now()
	f.engine.Restore("qg", &Handover{Slots: []HandoverSlot{
		{Evaluation: 1000, Rung: 0, StepMillis: minute.Milliseconds(), ReadAtMillis: now.Add(-24 * time.Hour).UnixMilli()},
		{Evaluation: 1060, Rung: 0, StepMillis: minute.Milliseconds(), ReadAtMillis: now.UnixMilli()},
	}})
	if stats := f.engine.Stats(); stats.HandoverSlots[HandoverSlotExpired] != 1 || stats.HandoverSlots[HandoverSlotNotSeriesLate] != 1 ||
		stats.HandoverSlots[HandoverSlotRestored] != 0 {
		t.Fatalf("slots %v, want one past its window and one with no ladder", stats.HandoverSlots)
	}
	f.engine.mu.Lock()
	f.engine.groups["qg"].sampled = true
	f.engine.mu.Unlock()
	f.engine.Restore("qg", &Handover{SeriesLate: &HandoverSeriesLate{Rung: 1, SinceMillis: now.UnixMilli()}})
	f.engine.mu.Lock()
	late := f.engine.groups["qg"].seriesLate
	f.engine.mu.Unlock()
	if stats := f.engine.Stats(); stats.HandoverLate[HandoverLateKeptLocal] != 1 || late != nil {
		t.Fatalf("series_late %v, state %+v; want the handed state set aside for the samples here", stats.HandoverLate, late)
	}
}

// A restored Slot whose queries cannot be planned again is read nowhere,
// counted unobserved for it.
func TestARestoredSlotThatCannotBePlannedAgainIsUnobserved(t *testing.T) {
	previous, _ := directedFixture(t, SupplementOutcome{})
	slot := previous.clock.now().Unix()
	previous.firstRead(slot, execution.CompletenessFull)
	readAt := previous.clock.now()
	handover := previous.engine.ForgetHandingOver("qg", ForgottenShutdown)

	next := newFixture(t)
	next.clock.set(readAt)
	next.engine.options.Supplement = (&supplementRecorder{}).run
	next.engine.options.Respec = (&respecs{err: errors.New("no contract")}).respec
	next.engine.Restore("qg", handover)
	next.clock.set(readAt.Add(rungDelay(handover.Slots[0].Rung, minute)))
	next.engine.Step(context.Background())
	next.waitFor(func(stats Stats) bool {
		return stats.Sources[sourceLog].SupplementUnobserved[UnobservedRestoredUnplanned] == 1
	})
}

// A restored Slot is read again in the slices this process reads its Query
// Group in now, one request a slice, each with its slice's conditions, as
// the previous owner reread its first read's slices; and its reread keeps
// only the series its Plans' segments do not cover - the others dropped as
// they stream, counted decided by their segment.
func TestARestoredSlotIsReadInTheQueryGroupsSlicesKeepingOnlyWhatItsSegmentsDoNotCover(t *testing.T) {
	previous, _ := directedFixture(t, SupplementOutcome{})
	slot := previous.clock.now().Unix()
	previous.firstRead(slot, execution.CompletenessFull)
	readAt := previous.clock.now()
	handover := previous.engine.ForgetHandingOver("qg", ForgottenMoved)

	next := newFixture(t)
	next.clock.set(readAt)
	recorder := &supplementRecorder{outcome: SupplementOutcome{Ran: true, Facts: execution.SupplementFacts{Candidates: 1, Unreached: 1,
		Admitted: 1, Points: 1}}}
	next.engine.options.Supplement = recorder.run
	halves := []execution.InputSlice{
		{Conditions: []execution.InputSliceCondition{{Dimension: "host", Pattern: "h[0-4].*"}}},
		{Conditions: []execution.InputSliceCondition{{Dimension: "host", Pattern: "h[0-4].*", Negated: true}}},
	}
	plan := wholeRead(slot)
	plan.Slices = halves
	plan.Covered = func(series execution.SeriesIdentityDigest, _ int64) bool { return series == "digest-h1" }
	next.engine.options.Respec = (&respecs{plan: plan}).respec
	asked := slicesAsked(next)
	next.engine.Restore("qg", handover)
	next.clock.set(readAt.Add(rungDelay(handover.Slots[0].Rung, minute)))
	next.answers <- delivered(point(slot, "1"))
	next.answers <- delivered(dataset("h2", map[int64]string{slot - 60: "5"}))
	next.engine.Step(context.Background())
	stats := next.waitFor(func(stats Stats) bool { return stats.Sources[sourceLog].SupplementWindows[DirectedSupplemented] == 1 })
	if !reflect.DeepEqual(*asked, halves) {
		t.Fatalf("reread in %+v, want one request a slice, each with its conditions", *asked)
	}
	jobs := recorder.taken()
	sink := &replaySink{}
	if len(jobs) != 1 {
		t.Fatalf("jobs %+v", jobs)
	}
	if _, err := jobs[0].Read.Replay(context.Background(), plan.Specs[0], sink); err != nil || len(sink.batches) != 1 {
		t.Fatalf("the read kept %d series (%v), want h2 alone: h1 its segment covers", len(sink.batches), err)
	}
	if stats.RestoredDedup[RestoredDedupSegment] != 1 || stats.RestoredDedup[RestoredDedupRead] != 1 {
		t.Fatalf("restored dedup %v, want h1 by its segment and h2 read", stats.RestoredDedup)
	}
}

// A restored Slot whose Query Group no read here has said the slices of
// waits for them where it is on its ladder - not planned, its segments not
// read, while this process knows no shape; not read whole while the plan
// says none - and is let go by name once its deepest rung's window is over,
// not before.
func TestARestoredSlotWithNoSliceShapeWaitsAndIsLetGoPastTheDeepestRungByName(t *testing.T) {
	for _, known := range []bool{false, true} {
		previous, _ := directedFixture(t, SupplementOutcome{})
		slot := previous.clock.now().Unix()
		previous.firstRead(slot, execution.CompletenessFull)
		readAt := previous.clock.now()
		handover := previous.engine.ForgetHandingOver("qg", ForgottenMoved)
		// On the ladder's first rung, so its deepest is not its own.
		handover.Slots[0].Rung, handover.SeriesLate.Rung = 0, 0

		next := newFixture(t)
		next.clock.set(readAt)
		next.engine.options.Supplement = (&supplementRecorder{}).run
		plan := wholeRead(slot)
		plan.Slices, plan.Shaped = nil, false
		planned := &respecs{plan: plan}
		next.engine.options.Respec = planned.respec
		next.engine.options.ShapeKnown = func(execution.QueryGroupIdentity) bool { return known }
		asked := slicesAsked(next)
		next.engine.Restore("qg", handover)
		deepest := len(RungSteps) - 1
		idle := func() bool {
			next.engine.mu.Lock()
			defer next.engine.mu.Unlock()
			for _, state := range next.engine.groups {
				for _, directed := range state.directed {
					if directed.running {
						return false
					}
				}
			}
			return true
		}
		// At each moment the Slot is judged once with nothing of it in
		// flight: a Step, the planning it starts done, and a Step again.
		for _, at := range []time.Duration{rungDelay(handover.Slots[0].Rung, minute), rungDelay(deepest, minute),
			rungDelay(deepest, minute) + rungWindow(deepest, minute)} {
			next.clock.set(readAt.Add(at))
			for step := 0; step < 2; step++ {
				next.engine.Step(context.Background())
				testwait.Until(t, "the Slot's planning done", idle)
			}
		}
		if stats := next.engine.Stats(); stats.HandoverSlots[HandoverSlotNoSliceShape] != 0 {
			t.Fatalf("known %t: let go %v at its deepest rung's window's end, want it waiting until past it", known, stats.HandoverSlots)
		}
		planned.mu.Lock()
		planning := len(planned.asked)
		planned.mu.Unlock()
		if known && planning == 0 || !known && planning != 0 {
			t.Fatalf("known %t: planned %d times, want it planned only once the shape is known", known, planning)
		}
		next.clock.set(readAt.Add(rungDelay(deepest, minute) + rungWindow(deepest, minute) + time.Second))
		next.engine.Step(context.Background())
		stats := next.waitFor(func(stats Stats) bool { return stats.HandoverSlots[HandoverSlotNoSliceShape] == 1 })
		if len(*asked) != 0 || stats.Sources[sourceLog].SupplementUnobserved[UnobservedNoSliceShape] != 1 {
			t.Fatalf("known %t: %d rereads, unobserved %v; want none read and the Slot let go as having no slice shape",
				known, len(*asked), stats.Sources[sourceLog].SupplementUnobserved)
		}
	}
}

// A restored Slot waiting for its Query Group's slices holds every later
// Slot of the group back: the new owner's own Slot after it, due at its rung,
// is not supplemented before it. The Step its slices are known, the restored
// Slot is read and supplemented, its rung's delay long past, and then the
// later one - the oldest first, one at a time, as every Slot of a group.
func TestALaterSlotWaitsForARestoredSlotWaitingForItsSlices(t *testing.T) {
	previous, _ := directedFixture(t, SupplementOutcome{})
	slot := previous.clock.now().Unix()
	previous.firstRead(slot, execution.CompletenessFull)
	readAt := previous.clock.now()
	handover := previous.engine.ForgetHandingOver("qg", ForgottenMoved)

	next := newFixture(t)
	next.clock.set(readAt)
	recorder := &supplementRecorder{outcome: SupplementOutcome{Ran: true, Facts: execution.SupplementFacts{Candidates: 1,
		Unreached: 1, Admitted: 1, Points: 1}}}
	next.engine.options.Supplement = recorder.run
	var known atomic.Bool
	next.engine.options.ShapeKnown = func(execution.QueryGroupIdentity) bool { return known.Load() }
	next.engine.options.Respec = (&respecs{plan: wholeRead(slot)}).respec
	next.engine.Restore("qg", handover)
	// The new owner's own first Slot, a step on, read without a series late.
	next.clock.set(readAt.Add(minute))
	later := slot + 60
	next.firstRead(later, execution.CompletenessFull)
	next.clock.set(readAt.Add(minute + rungDelay(handover.Slots[0].Rung, minute)))
	// The later Slot is due and its reread could be answered: it is held
	// back all the same.
	for step := 0; step < 20; step++ {
		for len(next.answers) < cap(next.answers) {
			next.answers <- delivered(point(later, "1"), dataset("h2", map[int64]string{later - 60: "5"}))
		}
		next.engine.Step(context.Background())
		time.Sleep(time.Millisecond) // fixed-wait: lets a dispatched reread run before the next Step looks
	}
	if jobs := recorder.taken(); len(jobs) != 0 {
		t.Fatalf("supplemented %+v while the restored Slot waits for its slices, want nothing", jobs)
	}
	known.Store(true)
	// The Query Group's own samples read from the same answers: kept full,
	// so no read waits on another's.
	testwait.Until(t, "both Slots supplemented", func() bool {
		for len(next.answers) < cap(next.answers) {
			next.answers <- delivered(point(later, "1"), dataset("h2", map[int64]string{slot - 60: "5"}))
		}
		next.engine.Step(context.Background())
		return len(recorder.taken()) == 2
	})
	jobs := recorder.taken()
	if jobs[0].EvaluationTime != execution.EvaluationTime(slot) || jobs[1].EvaluationTime != execution.EvaluationTime(later) {
		t.Fatalf("supplemented %d then %d, want the restored Slot %d first", jobs[0].EvaluationTime, jobs[1].EvaluationTime, slot)
	}
}
