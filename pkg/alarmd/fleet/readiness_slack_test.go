// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// slotTiming feeds the tracker an object's Slot as production reports it:
// the access layer's readiness arrival, the scheduler's start with what its
// dispatch waited for an execution slot, and its completion with its wall
// time, under operation.
func slotTiming(tracker *Tracker, queryGroup string, slot int64, operation observability.Operation,
	readiness observability.SlotReadinessFacts, waitMillis float64, wall time.Duration) {
	trace := observability.TraceFields{QueryGroupKey: queryGroup, EvaluationTime: slot}
	ctx := context.Background()
	if operation == observability.OperationNormal {
		tracker.Observe(ctx, observability.Observation{Component: observability.ComponentAccess,
			Stage: observability.StageSlotReadinessArrival, Result: observability.ResultSuccess, Operation: operation,
			SlotReadiness: &readiness, Trace: trace})
	}
	tracker.Observe(ctx, observability.Observation{Component: observability.ComponentScheduler, Stage: observability.StageSlotStarted,
		Result: observability.ResultStarted, Operation: operation, Trace: trace,
		ExecutionSlotWait: &observability.ExecutionSlotWaitFacts{WaitMillis: waitMillis, Cohort: "60"}})
	tracker.Observe(ctx, observability.Observation{Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
		Result: observability.ResultSuccess, Operation: operation, Duration: wall, DurationKnown: true, Trace: trace})
}

func unified(slack float64) observability.SlotReadinessFacts {
	return observability.SlotReadinessFacts{Boundary: observability.ReadinessBoundaryUnified, SlackSeconds: slack, Slack: true}
}

func budgeted(tracker *Tracker, queryGroup string, interval int64) {
	tracker.Observe(context.Background(), observability.Observation{Component: observability.ComponentAccess,
		Stage: observability.StageQueryBudgetResolved, Result: observability.ResultSuccess, Operation: observability.OperationNormal,
		QueryTiming: &observability.QueryTimingFacts{IntervalSeconds: interval}, Trace: observability.TraceFields{QueryGroupKey: queryGroup}})
}

func slackNames(rows []ReadinessSlackFacts) string {
	var order []string
	for _, row := range rows {
		order = append(order, row.QueryGroup)
	}
	return strings.Join(order, " ")
}

// The held objects whose normal Slots read their data with slack past 2 s
// most often over the last hour are named first, then by their largest
// slack, then by name: each with its Slots past 0.5 s and 2 s, its largest
// slack and the bound of the bucket its p99 falls in, the boundary its
// Slots read under, its period - its queries' interval, else the least
// distance between two of its Slots, none with one Slot - the series its
// Plans bound, its longest wall time and its normal dispatches' waits for an
// execution slot. An object not held has no row, a retry's wait and wall
// time are not counted, and when the process has run less than the hour
// each row says since when. A bucket an hour old is out of the window and
// is started afresh when written again.
func TestTheObjectsWithTheMostSlackPast2sAreNamedFirst(t *testing.T) {
	base := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
	at := &clock{at: base}
	tracker := newTracker(t, at)
	budgeted(tracker, "qg-a", 60)
	matched := 7
	tracker.Observe(observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-a"}),
		observability.Observation{Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
			Result: observability.ResultSuccess, PlanSeriesMatched: &matched,
			Trace: observability.TraceFields{StrategyID: "s1", BusinessID: "2", EvaluationTime: 1000}})
	for index, slack := range []float64{0.3, 0.7, 2.5, 3, 0.1, 2.1} {
		wait, wall := float64(0), 2*time.Second
		switch index {
		case 1:
			wait = 1500
		case 2:
			wait, wall = 500, 7*time.Second
		}
		// A Slot every other period: the period is still its queries' 60 s.
		slotTiming(tracker, "qg-a", 1000+int64(index)*120, observability.OperationNormal, unified(slack), wait, wall)
	}
	slotTiming(tracker, "qg-a", 1000, observability.OperationRetry, observability.SlotReadinessFacts{}, 9000, time.Minute)
	slotTiming(tracker, "qg-b", 2000, observability.OperationNormal, unified(10), 0, time.Second)
	slotTiming(tracker, "qg-b", 2600, observability.OperationNormal, unified(12), 0, time.Second)
	for index := 0; index < 99; index++ {
		slotTiming(tracker, "qg-p99", 3000+int64(index)*10, observability.OperationNormal, unified(0.3), 0, time.Second)
	}
	slotTiming(tracker, "qg-p99", 4000, observability.OperationNormal, unified(40), 0, time.Second)
	slotTiming(tracker, "qg-c", 5000, observability.OperationNormal, observability.SlotReadinessFacts{Boundary: observability.ReadinessBoundaryMixed}, 0, time.Second)
	slotTiming(tracker, "qg-d", 5000, observability.OperationNormal, observability.SlotReadinessFacts{Boundary: observability.ReadinessBoundaryNone}, 0, time.Second)
	slotTiming(tracker, "qg-e", 5000, observability.OperationNormal, unified(0.05), 0, time.Second)
	slotTiming(tracker, "qg-x", 5000, observability.OperationNormal, unified(99), 0, time.Second)
	held := []string{"qg-a", "qg-b", "qg-c", "qg-d", "qg-e", "qg-p99", "qg-none"}

	rows := tracker.ReadinessSlack(held, base.Add(-10*time.Minute), time.Hour)
	if got := slackNames(rows); got != "qg-a qg-b qg-p99 qg-e qg-c qg-d" {
		t.Fatalf("order %s, want qg-a qg-b qg-p99 qg-e qg-c qg-d", got)
	}
	a := rows[0]
	if a.Slots != 6 || a.SlotsOverHalf != 4 || a.SlotsOver2 != 3 || a.SlackMaxSeconds != 3 || a.SlackP99Seconds != 3 ||
		a.Boundary != observability.ReadinessBoundaryUnified || a.WallMaxSeconds != 7 || a.SlotWaitSeconds != 2 || a.SlotWaits != 2 ||
		a.Series != 7 {
		t.Fatalf("qg-a %+v, want 6 Slots, 4 past 0.5 s, 3 past 2 s, slack 3 at most and at p99, unified, 7 s wall, 2 s over 2 waits, 7 series", a)
	}
	if a.PeriodSeconds == nil || *a.PeriodSeconds != 60 || a.WindowStart == nil || !a.WindowStart.Equal(base.Add(-10*time.Minute)) {
		t.Fatalf("qg-a period %v since %v, want its queries' 60 s and the process's start", a.PeriodSeconds, a.WindowStart)
	}
	if b := rows[1]; b.PeriodSeconds == nil || *b.PeriodSeconds != 600 || b.SlotsOver2 != 2 || b.SlackMaxSeconds != 12 {
		t.Fatalf("qg-b %+v, want the 600 s between its two Slots, both past 2 s, 12 s at most", b)
	}
	if p := rows[2]; p.Slots != 100 || p.SlackP99Seconds != 0.5 || p.SlackMaxSeconds != 40 || p.SlotsOver2 != 1 {
		t.Fatalf("qg-p99 %+v, want 100 Slots, p99 in the 0.5 s bucket, 40 s at most", p)
	}
	if c, d := rows[4], rows[5]; c.Boundary != observability.ReadinessBoundaryMixed || c.PeriodSeconds != nil || c.SlackMaxSeconds != 0 ||
		c.SlackP99Seconds != 0 || d.Boundary != observability.ReadinessBoundaryNone {
		t.Fatalf("qg-c %+v and qg-d %+v, want mixed with no period and no slack, and none", c, d)
	}
	// A process up more than the hour: no row says since when.
	for _, row := range tracker.ReadinessSlack(held, base.Add(-2*time.Hour), time.Hour) {
		if row.WindowStart != nil {
			t.Fatalf("%s says since %v though the process has run the hour", row.QueryGroup, row.WindowStart)
		}
	}
	// An object no longer owned is forgotten with its Slots; the rest stand.
	tracker.Forget(map[string]struct{}{"qg-b": {}, "qg-c": {}, "qg-d": {}, "qg-e": {}, "qg-p99": {}})
	if got := slackNames(tracker.ReadinessSlack(held, time.Time{}, time.Hour)); got != "qg-b qg-p99 qg-e qg-c qg-d" {
		t.Fatalf("after qg-a was forgotten, %s", got)
	}
	// An hour on, every Slot seen is out of the window, and the bucket a new
	// Slot lands in - the same one of the twelve - holds it alone.
	at.at = base.Add(time.Hour)
	if rows := tracker.ReadinessSlack(held, time.Time{}, time.Hour); len(rows) != 0 {
		t.Fatalf("an hour on, %d rows: %+v", len(rows), rows)
	}
	slotTiming(tracker, "qg-b", 6200, observability.OperationNormal, unified(0.2), 0, time.Second)
	if rows := tracker.ReadinessSlack(held, time.Time{}, time.Hour); len(rows) != 1 || rows[0].Slots != 1 ||
		rows[0].SlotsOver2 != 0 || rows[0].SlackMaxSeconds != 0.2 {
		t.Fatalf("an hour on, one new Slot reads %+v", rows)
	}
}

// At most MaxReadinessSlack objects are named, the rest left out by the
// same order.
func TestTheObjectsNamedByTheirSlackAreBounded(t *testing.T) {
	at := &clock{at: time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)}
	tracker := newTracker(t, at)
	var held []string
	for index := 0; index < MaxReadinessSlack+8; index++ {
		queryGroup := fmt.Sprintf("qg-%02d", index)
		held = append(held, queryGroup)
		slotTiming(tracker, queryGroup, 1000, observability.OperationNormal, unified(3+float64(index)), 0, time.Second)
	}
	rows := tracker.ReadinessSlack(held, time.Time{}, time.Hour)
	if len(rows) != MaxReadinessSlack || rows[0].QueryGroup != "qg-39" || rows[MaxReadinessSlack-1].QueryGroup != "qg-08" {
		t.Fatalf("%d rows from %s to %s, want %d from qg-39 to qg-08", len(rows), rows[0].QueryGroup, rows[len(rows)-1].QueryGroup, MaxReadinessSlack)
	}
}

// The deployment's health names every replica's objects by their slack
// together, read from the summaries as published: the most Slots past 2 s
// first, the same by largest slack, then by replica and name, each naming
// its replica, cut to MaxReadinessSlack across the replicas; the public
// summary leaves them out.
func TestTheDeploymentsHealthNamesTheObjectsWithTheMostSlackAcrossReplicas(t *testing.T) {
	at := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
	var ids []string
	period := int64(60)
	snapshot := func(replica, prefix string) Snapshot {
		s := Snapshot{Replica: replica, TakenAt: at, Owned: 20, Determined: 20}
		for index := 0; index < 20; index++ {
			qg := fmt.Sprintf("%s-%02d", prefix, index)
			ids = append(ids, qg)
			s.OwnedObjects = append(s.OwnedObjects, qg)
			s.ReadinessSlack = append(s.ReadinessSlack, ReadinessSlackFacts{QueryGroup: qg, PeriodSeconds: &period, Slots: 60,
				SlotsOver2: uint32(20 - index), SlackMaxSeconds: 9, Boundary: observability.ReadinessBoundaryUnified})
		}
		return s
	}
	health := healthFromSummaries(t, []Snapshot{snapshot("pod-b", "qg-b"), snapshot("pod-a", "qg-a")},
		Expectation{QueryGroups: 40, Known: true, IDs: ids}, []string{"pod-a", "pod-b"}, at, time.Minute)
	encoded, err := json.Marshal(health)
	if err != nil {
		t.Fatal(err)
	}
	var read struct {
		ReadinessSlack []ReadinessSlackFacts `json:"readiness_slack"`
	}
	if err := json.Unmarshal(encoded, &read); err != nil {
		t.Fatal(err)
	}
	got := read.ReadinessSlack
	if len(got) != MaxReadinessSlack {
		t.Fatalf("health names %d objects, want %d of the 40", len(got), MaxReadinessSlack)
	}
	for index, row := range got {
		want := fmt.Sprintf("pod-a/qg-a-%02d", index/2)
		if index%2 == 1 {
			want = fmt.Sprintf("pod-b/qg-b-%02d", index/2)
		}
		if name := row.Replica + "/" + row.QueryGroup; name != want || row.PeriodSeconds == nil || *row.PeriodSeconds != 60 {
			t.Fatalf("row %d is %s (%+v), want %s with its period", index, name, row, want)
		}
	}
	if public, _ := json.Marshal(PublicHealth(health)); strings.Contains(string(public), "readiness_slack") {
		t.Fatalf("the public summary names Query Groups: %s", public)
	}
}
