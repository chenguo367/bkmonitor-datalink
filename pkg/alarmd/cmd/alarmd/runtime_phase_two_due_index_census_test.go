// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// The census counts every entry by where it is in its cycle, with the grace
// of one period: past due within a period is late, past it is overdue. Objects
// with no entry are the ones nothing has been evaluated for since takeover.
func TestDueIndexCensusCountsEveryEntryByItsCyclePosition(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(10_000, 0)}
	dispatcher := dueIndexDispatcher(clock, metric.NewRecorder(metric.BuildInfo{}), 8, 8,
		map[execution.QueryGroupIdentity]walkRunner{
			"waiting": {}, "cooling": {}, "late": {}, "late-edge": {}, "overdue": {}, "no-period": {},
		})
	index := dispatcher.dueIndex
	record := func(queryGroup execution.QueryGroupIdentity, dueAt, interval int64, cooling bool) {
		index.Record(queryGroup, dispatcher.bundle.runners[queryGroup], index.versionEpoch,
			scheduler.RunnerDueBound{NotDueUntilUnix: dueAt, IntervalSeconds: interval, QueryCooldown: cooling},
			time.Unix(dueAt-1, 0))
	}
	record("waiting", 10_040, 60, false)
	record("cooling", 10_300, 60, true)
	record("late", 9_990, 60, false)      // 10 s past due, period 60: late
	record("late-edge", 9_940, 60, false) // exactly one period past due: still late, not overdue
	record("overdue", 9_800, 60, false)   // 200 s past due: a turn missed
	record("no-period", 9_000, 0, false)

	// Seven objects owned, six with entries: one never evaluated.
	census := index.Census(clock.at, 7)
	want := fleet.ScheduleCensus{Waiting: 2, Cooling: 1, Late: 3, Overdue: 1, Never: 1,
		OldestLateSeconds: 1000, MissingPeriod: 1}
	if census != want {
		t.Errorf("census = %+v, want %+v", census, want)
	}
	// The wake facts for one object are the entry as it stands; an object with
	// no entry is Known false, which is a different answer from a zero bound.
	if wake := index.WakeOf("overdue"); !wake.Known || wake.DueAt.Unix() != 9_800 || wake.IntervalSeconds != 60 {
		t.Errorf("WakeOf(overdue) = %+v", wake)
	}
	if wake := index.WakeOf("cooling"); !wake.Cooling {
		t.Errorf("WakeOf(cooling) = %+v, want Cooling", wake)
	}
	if wake := index.WakeOf("never"); wake.Known {
		t.Errorf("WakeOf(never) = %+v, want Known false", wake)
	}
}

// Every executed round is judged against the bound it fell due by, and the
// two windows add up separately. Only executed rounds count, only rounds with
// a period can be judged, and a round that returns before its bound (a
// publication pulled the bound forward) is on time and not late.
func TestDueIndexCountsCompletionsAgainstTheBoundTheyFellDueBy(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(100_000, 0)}
	dispatcher := dueIndexDispatcher(clock, metric.NewRecorder(metric.BuildInfo{}), 8, 8,
		map[execution.QueryGroupIdentity]walkRunner{"a": {}, "b": {}, "c": {}, "d": {}})
	index := dispatcher.dueIndex
	epoch := index.versionEpoch
	lifecycle := func(name execution.QueryGroupIdentity) *phaseTwoQueryGroupLifecycle {
		return dispatcher.bundle.runners[name]
	}
	// First rounds establish bounds; they are not judged (nothing to judge
	// against).
	for _, name := range []execution.QueryGroupIdentity{"a", "b", "c", "d"} {
		index.Record(name, lifecycle(name), epoch,
			scheduler.RunnerDueBound{NotDueUntilUnix: 100_060, IntervalSeconds: 60, Executed: true},
			time.Unix(100_000, 0))
	}
	if census := index.Census(time.Unix(100_000, 0), 4); census.Completed1h != 0 {
		t.Fatalf("first rounds were judged: %+v", census)
	}
	// a: on time (returned 10 s after falling due, period 60).
	index.Record("a", lifecycle("a"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_130, IntervalSeconds: 60, Executed: true}, time.Unix(100_070, 0))
	// b: pushed back by the dispatcher, and still on time (returned 50 s after
	// falling due).
	index.MarkHeldBack("b")
	index.Record("b", lifecycle("b"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_170, IntervalSeconds: 60, Executed: true}, time.Unix(100_110, 0))
	// c: pushed back and missed its turn (returned 130 s after falling due).
	index.MarkHeldBack("c")
	index.Record("c", lifecycle("c"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_250, IntervalSeconds: 60, Executed: true}, time.Unix(100_190, 0))
	// d: returned without executing. Not a completion.
	index.Record("d", lifecycle("d"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_120, IntervalSeconds: 60, Executed: false}, time.Unix(100_100, 0))
	// d again, executed exactly one period after falling due: on time. The
	// deadline is the next turn, and returning as it falls due is meeting it.
	index.Record("d", lifecycle("d"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_240, IntervalSeconds: 60, Executed: true}, time.Unix(100_180, 0))
	// Marking an object nothing has been evaluated for marks nothing.
	index.MarkHeldBack("never-seen")

	census := index.Census(time.Unix(100_200, 0), 4)
	if census.Completed1h != 4 || census.OnTime1h != 3 {
		t.Errorf("1h window = %d completed / %d on time, want 4 / 3", census.Completed1h, census.OnTime1h)
	}
	if census.HeldBack1h != 2 || census.HeldBackOnTime1h != 1 {
		t.Errorf("held back = %d, of which on time %d, want 2 and 1", census.HeldBack1h, census.HeldBackOnTime1h)
	}
	// The mark is consumed by the return it was made before: b's next round
	// is not held back unless the dispatcher says so again.
	index.Record("b", lifecycle("b"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_260, IntervalSeconds: 60, Executed: true}, time.Unix(100_200, 0))
	if census := index.Census(time.Unix(100_200, 0), 4); census.HeldBack1h != 2 {
		t.Errorf("a mark survived the return it was made before: held back = %d, want 2", census.HeldBack1h)
	}
	if census.Completed6h != 4 || census.OnTime6h != 3 {
		t.Errorf("6h window = %d / %d, want 4 / 3", census.Completed6h, census.OnTime6h)
	}
	// Two hours on, the hour window has forgotten all five returns and the
	// six-hour one has not; seven hours on, both have.
	later := index.Census(time.Unix(100_200+2*3600, 0), 4)
	if later.Completed1h != 0 || later.Completed6h != 5 {
		t.Errorf("after two hours: 1h = %d, 6h = %d, want 0 and 5", later.Completed1h, later.Completed6h)
	}
	if gone := index.Census(time.Unix(100_200+7*3600, 0), 4); gone.Completed6h != 0 {
		t.Errorf("after seven hours the six-hour window still holds %d", gone.Completed6h)
	}
	// A bucket six hours old is recognised by its minute, not its slot: a
	// completion now lands in a fresh bucket, not on top of the stale one.
	index.Record("a", lifecycle("a"), epoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 100_200 + 7*3600 + 60, IntervalSeconds: 60, Executed: true},
		time.Unix(100_200+7*3600, 0))
	if fresh := index.Census(time.Unix(100_200+7*3600, 0), 4); fresh.Completed1h != 1 || fresh.Completed6h != 1 {
		t.Errorf("after the ring wrapped: 1h = %d, 6h = %d, want 1 and 1", fresh.Completed1h, fresh.Completed6h)
	}
}

// The publisher puts the census on the snapshot and the wake facts on every
// listed row, from the same index at the same instant.
func TestFleetPublisherCarriesTheCensusAndWakeFacts(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(20_000, 0)}
	dispatcher := dueIndexDispatcher(clock, metric.NewRecorder(metric.BuildInfo{}), 8, 8,
		map[execution.QueryGroupIdentity]walkRunner{"query-group-a": {}, "query-group-b": {}})
	index := dispatcher.dueIndex
	index.Record("query-group-a", dispatcher.bundle.runners["query-group-a"], index.versionEpoch,
		scheduler.RunnerDueBound{NotDueUntilUnix: 19_900, IntervalSeconds: 60}, time.Unix(19_850, 0))

	tracker := fleet.NewTracker(nil, "replica-1", clock.now)
	// Two degraded rounds make an anomaly of each object; b has no entry in
	// the index.
	for _, name := range []string{"query-group-a", "query-group-b"} {
		for i := 0; i < fleet.DefaultDegradedRounds; i++ {
			tracker.Observe(context.Background(), observability.Observation{
				ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE",
				Trace:                  observability.TraceFields{QueryGroupKey: name, StrategyID: "1", BusinessID: "2"},
			})
		}
	}
	publisher := fleetPublisher{
		tracker: tracker, replica: "replica-1", now: clock.now,
		owned: func() []execution.QueryGroupIdentity {
			return []execution.QueryGroupIdentity{"query-group-a", "query-group-b"}
		},
		schedule: index,
	}
	snapshot := publisher.snapshot(context.Background())
	if snapshot.Schedule == nil {
		t.Fatal("the snapshot carried no schedule census")
	}
	if snapshot.Schedule.Overdue != 1 || snapshot.Schedule.Never != 1 {
		t.Errorf("census = %+v, want one overdue (a, 100 s past a 60 s period) and one never (b)", *snapshot.Schedule)
	}
	wakes := map[string]*fleet.WakeFacts{}
	for _, anomaly := range snapshot.Anomalies {
		wakes[anomaly.QueryGroup] = anomaly.Wake
	}
	if wake := wakes["query-group-a"]; wake == nil || !wake.Known || wake.DueAt.Unix() != 19_900 {
		t.Errorf("row a carries wake %+v, want the index's bound", wake)
	}
	if wake := wakes["query-group-b"]; wake == nil || wake.Known {
		t.Errorf("row b carries wake %+v, want Known false: nothing evaluated since takeover", wake)
	}
	// A publisher with no schedule source publishes neither, rather than zeroes
	// and Known-false rows that would read as "never evaluated".
	bare := publisher
	bare.schedule = nil
	plain := bare.snapshot(context.Background())
	if plain.Schedule != nil {
		t.Error("a publisher with no schedule source published a census")
	}
	for _, anomaly := range plain.Anomalies {
		if anomaly.Wake != nil {
			t.Errorf("row %s carries wake facts from no source", anomaly.QueryGroup)
		}
	}
}

// The dispatcher marks the objects it pushes back on the due index at the
// line that makes the decision, so their next return can say whether being
// pushed back cost the deadline. Driven through fillQueues with a ready queue
// of one, which is the branch a live deployment reaches under load.
func TestHeldBackObjectsAreMarkedAtTheDispatchersDecision(t *testing.T) {
	dispatcher := walkDispatcher(1, 4, map[execution.QueryGroupIdentity]time.Time{
		"query-group-a": {}, "query-group-b": {}, "query-group-c": {},
	})
	index := dispatcher.dueIndex
	if index == nil {
		t.Fatal("the walk dispatcher has no due index; the mark has nowhere to land")
	}
	at := time.Unix(50_000, 0)
	for name := range dispatcher.bundle.runners {
		index.Record(name, dispatcher.bundle.runners[name], index.versionEpoch,
			scheduler.RunnerDueBound{NotDueUntilUnix: at.Unix() - 1, IntervalSeconds: 60, Executed: true}, at.Add(-time.Minute))
	}
	runners, revision := dispatcher.bundle.snapshotScheduledRunners()
	// One place in the ready queue: the first object is queued and the walk
	// stops at the second, which is pushed back.
	dispatcher.fillQueues(runners, revision)
	if dispatcher.rotation.deferredQueueFull == 0 {
		t.Fatal("the full ready queue turned nothing away; the branch under test did not run")
	}
	held := 0
	for _, entry := range index.entries {
		if entry.heldBack {
			held++
		}
	}
	if held != 1 {
		t.Fatalf("%d entries marked held back, want the one the full queue turned away", held)
	}
	// Its next return counts as held back, on time or not.
	for name, entry := range index.entries {
		if !entry.heldBack {
			continue
		}
		index.Record(name, dispatcher.bundle.runners[name], index.versionEpoch,
			scheduler.RunnerDueBound{NotDueUntilUnix: at.Unix() + 60, IntervalSeconds: 60, Executed: true}, at.Add(10*time.Second))
	}
	census := index.Census(at.Add(10*time.Second), 3)
	if census.HeldBack1h != 1 || census.HeldBackOnTime1h != 1 {
		t.Errorf("census = %+v, want one held-back round that was still on time", census)
	}
}
