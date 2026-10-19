// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// A held Query Group run through the production Worker - its Slots frozen with the hold, dispatched by its Runner,
// executed by access, the lookback stepped as its loop steps it - reserves each earlier-read trial at the Slot's
// first attempt, not a hold later at its readiness retry.
//
// Before, the lookback judged the group's rest at the moment Prepare ran: a rest that ended between a held Slot's
// first attempt and its readiness retry - most of them, on a hold near the period - left the first attempt to skip
// and the retry to reserve the trial a hold past its own candidate point, overtaken before it read. Here that was
// every trial, prepared a whole hold late.
//
// What this does not show is a trial completing. With the data step equal to the period, as a strategy's
// agg_interval makes it, a hold under two steps lowers to no hold and leaves the trial no time before its point,
// and a longer one has its Slot's first attempt behind the previous Slot's held read, past the point. Those are
// the trial's design, not when it is prepared: TestAHeldGroupsTrialOfNoHoldCompletesAndTheHoldComesDown and
// TestAHoldPastThePeriodIsTriedFromThePreviousSlotAndComesDown.
func TestAHeldSlotReservesItsEarlierReadTrialAtItsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	engine := f.bundle.dependencies.Lookback
	if engine == nil {
		t.Fatal("fixture: the bundle runs no lookback")
	}
	holds := f.bundle.dependencies.ReadHolds
	// The group measures its data landing a minute late, as the lookback reports a window read early: its hold
	// rises near the period, and its next Slots are frozen held.
	last := f.progress(ctx).LastCompletion
	if last == nil {
		t.Fatal("fixture: no Slot completed")
	}
	if err := holds.controller.Observe(ctx, readhold.Evidence{Contract: last.Contract, ArrivalAge: time.Minute,
		Rung: lookback.RungNames[0], Buckets: []int64{int64(last.Contract.Slot.EvaluationTime)}}); err != nil {
		t.Fatal(err)
	}
	const step = 5 * time.Second
	var hold time.Duration
	trials := func() (prepared uint64, lateMax float64) {
		for _, source := range engine.Stats().Sources {
			for _, count := range source.EarlierReads {
				prepared += count
			}
			lateMax = max(lateMax, source.EarlierPreparedLateMaxSeconds)
		}
		return prepared, lateMax
	}
	for deadline := f.now().Add(3 * time.Hour); f.now().Before(deadline); f.clock.Store(f.now().Add(step).UnixMilli()) {
		if prepared, _ := trials(); prepared >= 3 {
			break
		}
		if !f.runner.NextReadyAt().After(f.now()) {
			if _, _, err := f.runner.RunOne(ctx); err != nil {
				t.Logf("RunOne at %s: %v", f.now().UTC().Format(time.TimeOnly), err)
			}
		}
		engine.StepEarly(ctx)
		hold = max(hold, holds.controller.ReadHold(f.queryGroup))
	}
	prepared, lateMax := trials()
	if hold < 30*time.Second || prepared == 0 {
		t.Fatalf("fixture: hold %v, %d trials; want a held group whose trials are reserved", hold, prepared)
	}
	// Reserved at the first attempt, a trial is late at most by when the Runner got to the Slot - here within the
	// loop's step - never by the hold.
	if lateMax*float64(time.Second) >= float64(hold/2) {
		t.Fatalf("trials reserved up to %.1f s past their candidate point on a %v hold: reserved at the readiness retry", lateMax, hold)
	}
}

// heldTrialRun drives a held group's Slots through the production Worker, the clock taken to each Slot's first
// attempt as the dispatcher wakes it and stepped between, the lookback stepped as its loop steps it, for up to
// span or until done says so. The group's data is measured landing arrival late, which raises its hold. It
// returns the group's lowest hold after it was raised, the trials' outcomes, what overtook the overtaken ones,
// and the raised hold.
func heldTrialRun(t *testing.T, f *cutoverStallFixture, arrival, span time.Duration, trialPartial bool,
	done func(elapsed, lowest, raised time.Duration, outcomes map[string]uint64) bool,
) (lowest, raised time.Duration, outcomes, overtaken map[string]uint64) {
	t.Helper()
	ctx := context.Background()
	engine := f.bundle.dependencies.Lookback
	holds := f.bundle.dependencies.ReadHolds
	last := f.progress(ctx).LastCompletion
	if engine == nil || last == nil {
		t.Fatal("fixture: no lookback, or no Slot completed")
	}
	// The group measures its data landing arrival late, as the lookback reports a window read early.
	if err := holds.controller.Observe(ctx, readhold.Evidence{Contract: last.Contract, ArrivalAge: arrival,
		Rung: lookback.RungNames[0], Buckets: []int64{int64(last.Contract.Slot.EvaluationTime)}}); err != nil {
		t.Fatal(err)
	}
	raised = holds.controller.ReadHold(f.queryGroup)
	lowest = raised
	// When asked to, a read of a window before its held readiness answers partial - its data was not whole yet -
	// and one at or after it whole: a trial's read comes back incomplete, the formal read complete.
	if trialPartial {
		f.uqPartial = func(end int64) bool { return f.now().Before(time.Unix(end, 0).Add(raised)) }
	}
	const step = 5 * time.Second
	immediate := 0
	outcomesNow := func() map[string]uint64 {
		counts := map[string]uint64{}
		for _, source := range engine.Stats().Sources {
			for outcome, count := range source.EarlierReads {
				counts[outcome] += count
			}
		}
		return counts
	}
	started := f.now()
	for deadline := started.Add(span); f.now().Before(deadline); {
		if done(f.now().Sub(started), lowest, raised, outcomesNow()) {
			break
		}
		if next := f.runner.NextReadyAt(); !next.After(f.now()) {
			if _, _, err := f.runner.RunOne(ctx); err != nil {
				t.Logf("RunOne at %s: %v", f.now().UTC().Format(time.TimeOnly), err)
			}
		}
		engine.StepEarly(ctx)
		// The trial reads on its own goroutine: let it answer before the clock moves.
		for settle := time.Now().Add(20 * time.Millisecond); time.Now().Before(settle); {
			time.Sleep(time.Millisecond)
		}
		if hold := holds.controller.ReadHold(f.queryGroup); hold < lowest {
			lowest = hold
		}
		// A Slot due now runs now, as the dispatcher runs it: the clock moves only when none is due, or when the
		// Runner stays due without moving on.
		wake := f.runner.NextReadyAt()
		if !wake.After(f.now()) && immediate < 5 {
			immediate++
			continue
		}
		immediate = 0
		next := f.now().Add(step)
		if wake.After(f.now()) && wake.Before(next) {
			next = wake.Add(time.Millisecond)
		}
		f.clock.Store(next.UnixMilli())
	}
	outcomes, overtaken = map[string]uint64{}, map[string]uint64{}
	for _, source := range engine.Stats().Sources {
		for outcome, count := range source.EarlierReads {
			outcomes[outcome] += count
		}
		for path, count := range source.EarlierOvertaken {
			if count > 0 {
				overtaken[path] += count
			}
		}
	}
	t.Logf("hold %v -> lowest %v; trials %v, overtaken by %v", raised, lowest, outcomes, overtaken)
	return lowest, raised, outcomes, overtaken
}

// A held group's trial of no hold reads before the Slot's readiness without the hold - the reread bound before
// it - finds the data the formal read later reads, and after the matches and quiet the controller asks for, the
// hold comes down. Before, a trial of no hold was confined to that readiness itself and had no time to read.
func TestAHeldGroupsTrialOfNoHoldCompletesAndTheHoldComesDown(t *testing.T) {
	t.Parallel()
	f := startCutoverFixture(t, nil)
	// Half a minute late: the hold rises under two steps, so a trial tries no hold at all, and short of the
	// period, as the seeds are.
	lowest, raised, outcomes, _ := heldTrialRun(t, f, 30*time.Second, 6*time.Hour, false,
		func(_, lowest, raised time.Duration, _ map[string]uint64) bool { return lowest < raised })
	if raised < 20*time.Second {
		t.Fatalf("fixture: hold raised to %v", raised)
	}
	if outcomes[lookback.EarlierEqual] == 0 {
		t.Fatalf("no trial matched its formal read: %v", outcomes)
	}
	if lowest >= raised {
		t.Fatalf("hold stayed at %v (lowest %v) after %d matching trials", raised, lowest, outcomes[lookback.EarlierEqual])
	}
}

// Reading earlier moves only when the trial looks, never what proves a lower hold: a trial whose read comes back
// incomplete supplies no evidence, and the hold does not come down.
func TestAnIncompleteTrialOfNoHoldLeavesTheHold(t *testing.T) {
	t.Parallel()
	f := startCutoverFixture(t, nil)
	// Long enough that the matches and quiet the controller asks for would have lowered it had they counted.
	lowest, raised, outcomes, _ := heldTrialRun(t, f, 30*time.Second, 6*time.Hour, true,
		func(elapsed, _, _ time.Duration, outcomes map[string]uint64) bool {
			return elapsed >= 2*time.Hour && outcomes[lookback.EarlierReadFailed] > readhold.EarlierMatchesRequired
		})
	if outcomes[lookback.EarlierReadFailed] == 0 {
		t.Fatalf("fixture: no trial read came back incomplete: %v", outcomes)
	}
	if outcomes[lookback.EarlierEqual] != 0 || lowest < raised {
		t.Fatalf("an incomplete trial lowered the hold %v -> %v: %v", raised, lowest, outcomes)
	}
}

// A hold past the period less the reread bound leaves a Slot's first attempt behind the previous Slot's held read,
// past where its own trial had to start: under two steps, a trial of no hold whose point is the readiness without
// the hold; from two steps, a trial of half the hold. Reserved from the previous Slot's first attempt, the trial
// reads in time, matches the formal read, and the hold comes down; none is prepared after its point.
func TestAHoldPastThePeriodIsTriedFromThePreviousSlotAndComesDown(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name    string
		arrival time.Duration
	}{
		{"under two steps", 100 * time.Second},
		{"two steps and more", 160 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := startCutoverFixture(t, nil)
			lowest, raised, outcomes, overtaken := heldTrialRun(t, f, c.arrival, 6*time.Hour, false,
				func(_, lowest, raised time.Duration, _ map[string]uint64) bool { return lowest < raised })
			if raised <= time.Minute-lookback.RecheckTimeout {
				t.Fatalf("fixture: hold raised to %v, want past the period less the reread bound", raised)
			}
			if outcomes[lookback.EarlierEqual] == 0 {
				t.Fatalf("no trial matched its formal read: %v, overtaken by %v", outcomes, overtaken)
			}
			if lowest >= raised {
				t.Fatalf("hold stayed at %v after %d matching trials", raised, outcomes[lookback.EarlierEqual])
			}
			if overtaken[lookback.OvertakenPreparedLate] != 0 {
				t.Fatalf("%d trials prepared past their point", overtaken[lookback.OvertakenPreparedLate])
			}
		})
	}
}

// Reserved from the previous Slot, a trial still proves nothing when its read comes back incomplete: the hold
// does not come down.
func TestAnIncompleteTrialFromThePreviousSlotLeavesTheHold(t *testing.T) {
	t.Parallel()
	f := startCutoverFixture(t, nil)
	lowest, raised, outcomes, _ := heldTrialRun(t, f, 100*time.Second, 6*time.Hour, true,
		func(elapsed, _, _ time.Duration, outcomes map[string]uint64) bool {
			return elapsed >= 3*time.Hour && outcomes[lookback.EarlierReadFailed] > readhold.EarlierMatchesRequired
		})
	if outcomes[lookback.EarlierReadFailed] == 0 {
		t.Fatalf("fixture: no trial read came back incomplete: %v", outcomes)
	}
	if outcomes[lookback.EarlierEqual] != 0 || lowest < raised {
		t.Fatalf("an incomplete trial lowered the hold %v -> %v: %v", raised, lowest, outcomes)
	}
}
