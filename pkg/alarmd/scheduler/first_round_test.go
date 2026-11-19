// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// firstRoundRead is when access reads this fixture's Slot, by the call access
// itself makes, in milliseconds: the fixture's offset is its interval and its
// reserve the five seconds the source is built with.
func firstRoundRead(interval int64, slot execution.EvaluationTime) int64 {
	budget := time.Duration(interval)*time.Second - 5*time.Second
	return int64(slot)*1000 + execution.ReadWaitWithinQueryBudget("query-group-1", budget, 30*time.Second).Milliseconds()
}

func firstRoundNext(t *testing.T, interval int64, load execution.ProgressLoadResult, at time.Time) (bool, SlotDueFacts, *fakeSlotCatalog) {
	t.Helper()
	schedule := schedulerSchedule(t, interval, execution.EvaluationTime(interval*1000), nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, load, at)
	_, due, facts, err := source.Next(context.Background(), "query-group-1")
	if err != nil {
		t.Fatalf("Next() at %s error = %v", at, err)
	}
	return due, facts, catalog
}

// A Slot of a minute or more has its first round at its read: before it, nothing
// is frozen and nothing is asked of the store beyond the cursor. The not-due
// answer names the read, and its bound is the second before the read's, so the
// due index offers the Query Group ahead of the read and never after it.
func TestASlotOfAMinuteOrMoreHasItsFirstRoundAtItsRead(t *testing.T) {
	for _, interval := range []int64{60, 300, 3600} {
		slot := execution.EvaluationTime(interval * 1000)
		read := firstRoundRead(interval, slot)
		if read/1000 <= int64(slot) {
			t.Fatalf("interval %d: read at %d is no later than the Slot's second, so this cannot tell the two apart", interval, read)
		}

		// At the grid point, and in the last millisecond before the read.
		for _, at := range []time.Time{time.Unix(int64(slot), 0), time.UnixMilli(read - 1)} {
			due, facts, catalog := firstRoundNext(t, interval, missingProgress(), at)
			if due {
				t.Fatalf("interval %d at %s: due", interval, at)
			}
			if facts.DueAtUnixMilli != read || facts.NotDueUntilUnix != read/1000-1 ||
				facts.NextSlotUnix != int64(slot) || facts.IntervalSeconds != interval {
				t.Fatalf("interval %d at %s: facts = %+v, want the read %d, the second before its own and the Slot %d",
					interval, at, facts, read, slot)
			}
			if len(catalog.requests) != 0 {
				t.Fatalf("interval %d at %s: froze %d Slots before the read", interval, at, len(catalog.requests))
			}
		}

		due, _, catalog := firstRoundNext(t, interval, missingProgress(), time.UnixMilli(read))
		if !due || len(catalog.requests) != 1 {
			t.Fatalf("interval %d at the read: due=%t froze %d, want the Slot frozen once", interval, due, len(catalog.requests))
		}
	}
}

// The source judges a first round by the millisecond: due at its read, not at
// the whole second after it, which with the dispatcher's tick was up to two
// seconds of the window. The bound it gives the due index ends before the read's
// second begins, so a dispatch the index predicts due is never one the source
// answers not due for a second the index held it back.
func TestTheFirstRoundIsJudgedByTheMillisecondItsReadIsAt(t *testing.T) {
	const interval = 60
	slot := execution.EvaluationTime(interval * 1000)
	read := firstRoundRead(interval, slot)
	if read%1000 == 0 {
		t.Fatalf("the read at %d falls on a second, so this cannot tell the two apart", read)
	}
	if due, facts, _ := firstRoundNext(t, interval, missingProgress(), time.UnixMilli(read)); !due {
		t.Fatalf("at the read itself: not due, facts=%+v; want the round there, not at the next second", facts)
	}
	due, facts, _ := firstRoundNext(t, interval, missingProgress(), time.UnixMilli(read-1))
	if due || (facts.NotDueUntilUnix+1)*1000 > read {
		t.Fatalf("a millisecond before the read: due=%t facts=%+v, want not due, bounded before the read's second", due, facts)
	}
}

// A Runner reports the read its next Slot's first round waits for as its
// readiness only once it is near. Right after the not-due answer, most of a
// period away, it reports none: the dispatcher would otherwise park the Query
// Group in its recovery queue for the period. Within the lead it reports the
// read, so the recovery queue starts the round at it. The round that asks again
// spends it.
func TestARunnerReportsItsFirstRoundsReadAsItsReadinessOnceItIsNear(t *testing.T) {
	read := time.UnixMilli(76_345)
	source := &fakeSlotSource{facts: SlotDueFacts{NotDueUntilUnix: 75, DueAtUnixMilli: read.UnixMilli(), NextSlotUnix: 60, IntervalSeconds: 60}}
	fence := execution.OwnerFence{QueryGroup: "query-group-1", OwnerID: "worker-1", OwnerEpoch: 1, LeaseToken: "token-1"}
	at := time.Unix(61, 0)
	runner, err := NewRunner("query-group-1", &fakeSession{fence: fence}, source, &blockingExecutor{}, NewFlightCoordinator(),
		func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runner.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runner.NextReadyAt(); !got.IsZero() {
		t.Fatalf("a period before the read NextReadyAt() = %s, want none", got)
	}
	if bound := runner.DueBound(); bound.NotDueUntilUnix != 75 || bound.Deferred {
		t.Fatalf("bound = %+v, want the schedule's, the second before the read's", bound)
	}
	at = read.Add(-1500 * time.Millisecond)
	if got := runner.NextReadyAt(); !got.Equal(read) {
		t.Fatalf("a second and a half before the read NextReadyAt() = %s, want the read %s", got, read)
	}
	source.facts = SlotDueFacts{NotDueUntilUnix: 120, NextSlotUnix: 120, IntervalSeconds: 60}
	if _, _, err := runner.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runner.NextReadyAt(); !got.IsZero() {
		t.Fatalf("after a round that named no read NextReadyAt() = %s, want none", got)
	}
}

// productionFirstRoundNext is firstRoundNext on the schedule a period derives in
// production (execution.DeriveScheduleSpec), whose ten- and fifteen-second
// periods complete thirty seconds after their grid point.
func productionFirstRoundNext(t *testing.T, interval int64, at time.Time) (bool, SlotDueFacts, *fakeSlotCatalog) {
	t.Helper()
	slot := execution.EvaluationTime(interval * 1000)
	schedule := schedulerSchedule(t, interval, slot, nil, "snapshot-1", 1)
	spec := execution.DeriveScheduleSpec(interval)
	schedule.Plans[0].Spec, schedule.Plans[0].ScheduleRevision = spec, mustPlanScheduleRevision(t, spec)
	revision, err := execution.DeriveQueryGroupScheduleRevision(schedule.Plans)
	if err != nil {
		t.Fatal(err)
	}
	schedule.Segment.ScheduleRevision = revision
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), at)
	_, due, facts, err := source.Next(context.Background(), "query-group-1")
	if err != nil {
		t.Fatalf("Next() at %s error = %v", at, err)
	}
	return due, facts, catalog
}

// productionFirstRoundRead is when access reads the production schedule's Slot,
// by the call access itself makes: the budget is the period's completion offset
// less the five-second reserve the source is built with.
func productionFirstRoundRead(interval int64, slot execution.EvaluationTime) int64 {
	budget := time.Duration(execution.DeriveScheduleSpec(interval).CompletionOffsetSeconds())*time.Second - 5*time.Second
	return int64(slot)*1000 + execution.ReadWaitWithinQueryBudget("query-group-1", budget, 30*time.Second).Milliseconds()
}

// A period under a minute waits for its read as a minute does: not due at its
// grid point, which access would only turn away, nor a millisecond before its
// read, and due at the read, named on the not-due answer and bounded the second
// before its own. Named by the interval and not the short-period cohorts, which
// only list some.
func TestAPeriodUnderAMinuteHasItsFirstRoundAtItsRead(t *testing.T) {
	for _, interval := range []int64{59, 30, 20, 15, 10} {
		slot := execution.EvaluationTime(interval * 1000)
		read := productionFirstRoundRead(interval, slot)
		if read <= int64(slot)*1000 {
			t.Fatalf("interval %d: read at %d is no later than the grid point, so this cannot tell the two apart", interval, read)
		}
		for _, at := range []time.Time{time.Unix(int64(slot), 0), time.UnixMilli(read - 1)} {
			due, facts, catalog := productionFirstRoundNext(t, interval, at)
			if due || facts.DueAtUnixMilli != read || facts.NotDueUntilUnix != read/1000-1 || facts.NextSlotUnix != int64(slot) {
				t.Fatalf("interval %d at %s: due=%t facts=%+v, want not due, the read %d named and the second before its own",
					interval, at, due, facts, read)
			}
			if len(catalog.requests) != 0 {
				t.Fatalf("interval %d at %s: froze %d Slots before the read", interval, at, len(catalog.requests))
			}
		}
		due, _, catalog := productionFirstRoundNext(t, interval, time.UnixMilli(read))
		if !due || len(catalog.requests) != 1 {
			t.Fatalf("interval %d at its read: due=%t froze %d, want the Slot frozen once", interval, due, len(catalog.requests))
		}
	}
}

// A period too short for the settling wait and the reserve reads past its query
// deadline. It keeps its grid point, named as the instant its round is due at,
// where the round is bound unavailable as it was.
func TestAPeriodWhoseReadIsPastItsDeadlineKeepsItsGridPoint(t *testing.T) {
	const interval = 12
	slot := execution.EvaluationTime(interval * 1000)
	deadline := (int64(slot)+execution.DeriveScheduleSpec(interval).CompletionOffsetSeconds())*1000 - 5000
	if read := productionFirstRoundRead(interval, slot); read < deadline {
		t.Fatalf("the read at %d is before the deadline %d, so this cannot tell the two apart", read, deadline)
	}
	due, facts, _ := productionFirstRoundNext(t, interval, time.UnixMilli(int64(slot)*1000-1))
	if due || facts.DueAtUnixMilli != int64(slot)*1000 || facts.NotDueUntilUnix != int64(slot)-1 {
		t.Fatalf("a millisecond before its grid point: due=%t facts=%+v, want the grid point named", due, facts)
	}
	if due, _, catalog := productionFirstRoundNext(t, interval, time.Unix(int64(slot), 0)); !due || len(catalog.requests) != 1 {
		t.Fatalf("at its grid point: due=%t froze %d, want the Slot frozen once", due, len(catalog.requests))
	}
}

// A ten-second period, waiting for its read, keeps at least nine seconds of its
// query window after it, whatever its phase: ten seconds to settle and at most
// six of phase, against a query deadline twenty-five seconds after its grid
// point.
func TestATenSecondPeriodKeepsNineSecondsAfterItsRead(t *testing.T) {
	const interval = 10
	budget := time.Duration(execution.DeriveScheduleSpec(interval).CompletionOffsetSeconds())*time.Second - 5*time.Second
	for index := range 512 {
		queryGroup := execution.QueryGroupIdentity(fmt.Sprintf("query-group-%d", index))
		if left := budget - execution.ReadWaitWithinQueryBudget(queryGroup, budget, 30*time.Second); left < 9*time.Second {
			t.Fatalf("%s keeps %s after its read, want at least 9s", queryGroup, left)
		}
	}
	slot := execution.EvaluationTime(interval * 1000)
	_, facts, _ := productionFirstRoundNext(t, interval, time.Unix(int64(slot), 0))
	deadline := (int64(slot)+execution.DeriveScheduleSpec(interval).CompletionOffsetSeconds())*1000 - 5000
	if left := deadline - facts.DueAtUnixMilli; facts.DueAtUnixMilli == 0 || left < 9000 {
		t.Fatalf("the source's round is due at %d, %d ms before the query deadline %d, want at least 9000", facts.DueAtUnixMilli, left, deadline)
	}
}

// A Slot already begun is a recovery: its round does not wait for a read that the
// projection already has a contract for.
func TestASlotAlreadyBegunIsNotHeldForItsRead(t *testing.T) {
	const interval = 60
	slot := execution.EvaluationTime(interval * 1000)
	schedule := schedulerSchedule(t, interval, slot, nil, "snapshot-1", 1)
	frozenCatalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	frozen, due, _, err := newProductionSlotSourceForTest(t, frozenCatalog, missingProgress(), time.Unix(int64(slot)+40, 0)).
		Next(context.Background(), "query-group-1")
	if err != nil || !due {
		t.Fatalf("freezing the Slot: due=%t error=%v", due, err)
	}
	load := foundProgress(slot, 0)
	load.Progress.UnfinishedSlot = &execution.UnfinishedSlotProjection{
		Contract: frozen.Contract, DuePlanTargets: frozen.DuePlanTargets.Clone(),
		EarliestQueryDeadlineUnixMilli: frozen.EarliestQueryDeadlineUnixMilli, KeepUntilUnixMilli: frozen.KeepUntilUnixMilli,
	}
	// Past the grid point and before the read: a Slot not begun waits here.
	due, facts, _ := firstRoundNext(t, interval, foundProgress(slot, 0), time.Unix(int64(slot)+1, 0))
	if due || facts.NotDueUntilUnix <= int64(slot)+1 {
		t.Fatalf("a Slot not begun: due=%t facts=%+v, want it held for its read", due, facts)
	}
	due, _, _ = firstRoundNext(t, interval, load, time.Unix(int64(slot)+1, 0))
	if !due {
		t.Fatal("a Slot already begun was held for its read")
	}
	// Begun means this Slot: a projection of any other says nothing of it.
	other := foundProgress(slot, 0)
	other.Progress.UnfinishedSlot = &execution.UnfinishedSlotProjection{
		Contract: frozen.Contract, DuePlanTargets: frozen.DuePlanTargets.Clone(),
		EarliestQueryDeadlineUnixMilli: frozen.EarliestQueryDeadlineUnixMilli, KeepUntilUnixMilli: frozen.KeepUntilUnixMilli,
	}
	other.Progress.UnfinishedSlot.Contract.Slot.EvaluationTime = slot + interval
	source := newProductionSlotSourceForTest(t, frozenCatalog, other, time.Unix(int64(slot)+1, 0))
	if dueAt := source.firstRoundAt(schedule, slot, other.Progress); dueAt <= (int64(slot)+1)*1000 {
		t.Fatalf("a projection of another Slot left the first round at %d, want the read", dueAt)
	}
}

// A source that was not told the settling wait has no read to wait for.
func TestASourceWithoutASettlingWaitKeepsTheGridPoint(t *testing.T) {
	const interval = 60
	slot := execution.EvaluationTime(interval * 1000)
	schedule := schedulerSchedule(t, interval, slot, nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(int64(slot)-1, 0))
	source.settlingWait = 0
	_, due, facts, err := source.Next(context.Background(), "query-group-1")
	if err != nil || due || facts.NotDueUntilUnix != int64(slot)-1 || facts.DueAtUnixMilli != int64(slot)*1000 {
		t.Fatalf("Next() due=%t facts=%+v error=%v, want not due until the grid point, named, bounded the second before it", due, facts, err)
	}
}

// A schedule whose deadline cannot be derived keeps the grid point, and the round
// there says what is wrong with it, as it did: holding the Slot for a read nobody
// can work out would put that off to a time made up from the broken deadline.
func TestAScheduleWithoutADeadlineKeepsTheGridPoint(t *testing.T) {
	const interval = 60
	slot := execution.EvaluationTime(interval * 1000)
	schedule := schedulerSchedule(t, interval, slot, nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(int64(slot)-1, 0))
	// A reserve past the completion leaves no query deadline.
	source.queryReserve = time.Hour
	_, due, facts, err := source.Next(context.Background(), "query-group-1")
	if err != nil || due || facts.NotDueUntilUnix != int64(slot)-1 || facts.DueAtUnixMilli != int64(slot)*1000 {
		t.Fatalf("before the grid point: due=%t facts=%+v error=%v, want not due until the grid point, named, bounded the second before it", due, facts, err)
	}
}

// A Slot's deadline is reckoned from its grid point, not from when its first round
// is: reckoned from the round, a Slot waiting for its read would rank behind Slots
// that expire before it. A bound that names no Slot falls back on itself.
func TestTheDeadlineOfASlotWaitingForItsReadIsFromItsGridPoint(t *testing.T) {
	for name, test := range map[string]struct {
		facts SlotDueFacts
		want  int64
	}{
		"waiting for its read":  {SlotDueFacts{NotDueUntilUnix: 76, NextSlotUnix: 60, IntervalSeconds: 60}, 120},
		"waiting for its point": {SlotDueFacts{NotDueUntilUnix: 60, NextSlotUnix: 60, IntervalSeconds: 60}, 120},
		"naming no Slot":        {SlotDueFacts{NotDueUntilUnix: 76, IntervalSeconds: 60}, 136},
	} {
		t.Run(name, func(t *testing.T) {
			source := &fakeSlotSource{facts: test.facts}
			fence := execution.OwnerFence{QueryGroup: "query-group-1", OwnerID: "worker-1", OwnerEpoch: 1, LeaseToken: "token-1"}
			at := time.Unix(50, 0)
			runner, err := NewRunner("query-group-1", &fakeSession{fence: fence}, source, &blockingExecutor{}, NewFlightCoordinator(),
				func() time.Time { return at })
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := runner.RunOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			if bound := runner.DueBound(); bound.Verdict != DueVerdictNotDue || bound.Deferred {
				t.Fatalf("bound = %+v, want a not-due schedule bound", bound)
			}
			if got := runner.NextDeadline().Unix(); got != test.want {
				t.Fatalf("NextDeadline() = %d, want %d", got, test.want)
			}
		})
	}
}

// A hold is not in the first round's second: a held Slot still comes round at its
// read without the hold, is frozen with the hold there, and access turns it away
// by name. That round is where the lookback reserves its earlier read.
func TestAHeldSlotComesRoundAtItsReadWithoutTheHold(t *testing.T) {
	const interval = 60
	slot := execution.EvaluationTime(interval * 1000)
	bound := (firstRoundRead(interval, slot) + 999) / 1000
	schedule := schedulerSchedule(t, interval, slot, nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(bound, 0))
	source.readHolds = &testReadHolds{hold: 30 * time.Second}
	frozen, due, _, err := source.Next(context.Background(), "query-group-1")
	if err != nil || !due || frozen.Contract.ReadHoldMillis != 30_000 {
		t.Fatalf("Next() at the read without the hold = %+v due=%t error=%v, want the Slot frozen with its hold", frozen.Contract, due, err)
	}
}
