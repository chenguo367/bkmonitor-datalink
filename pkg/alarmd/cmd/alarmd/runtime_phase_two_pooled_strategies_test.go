// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// scriptedSchedules answers a frozen schedule read per Query Group, counting
// the reads; held, when set, holds every read until it is closed.
type scriptedSchedules struct {
	mu      sync.Mutex
	reads   map[execution.QueryGroupIdentity]int
	answers map[execution.QueryGroupIdentity]func(execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error)
	held    chan struct{}
}

func (schedules *scriptedSchedules) ReadFrozenSchedule(_ context.Context, queryGroup execution.QueryGroupIdentity, at execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error) {
	schedules.mu.Lock()
	schedules.reads[queryGroup]++
	answer, held := schedules.answers[queryGroup], schedules.held
	schedules.mu.Unlock()
	if held != nil {
		<-held
	}
	if answer == nil {
		return execution.FrozenQueryGroupSchedule{}, errors.New("no such schedule")
	}
	return answer(at)
}

func (schedules *scriptedSchedules) readsOf(queryGroup string) int {
	schedules.mu.Lock()
	defer schedules.mu.Unlock()
	return schedules.reads[execution.QueryGroupIdentity(queryGroup)]
}

// scheduleOf is a frozen schedule of one Segment, its Plans of the
// strategies named.
func scheduleOf(revision string, start int64, strategies ...string) func(execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error) {
	return func(execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error) {
		schedule := execution.FrozenQueryGroupSchedule{Segment: execution.ScheduleSegmentFact{
			ScheduleRevision: execution.ScheduleRevision(revision), Start: execution.EvaluationTime(start)}}
		for _, strategy := range strategies {
			schedule.Plans = append(schedule.Plans, execution.FrozenPlanSchedule{
				Identity: execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: strategy}})
		}
		return schedule, nil
	}
}

// pooledFixture is a replica's publisher over a real tracker, its pooled rows
// read from scripted schedules.
type pooledFixture struct {
	t         *testing.T
	at        time.Time
	tracker   *fleet.Tracker
	owned     []execution.QueryGroupIdentity
	schedules *scriptedSchedules
	publisher *fleetPublisher
}

func newPooledFixture(t *testing.T, budget int, bound time.Duration) *pooledFixture {
	f := &pooledFixture{t: t, at: time.Unix(1_800_000_000, 0),
		schedules: &scriptedSchedules{reads: map[execution.QueryGroupIdentity]int{},
			answers: map[execution.QueryGroupIdentity]func(execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error){}}}
	now := func() time.Time { return f.at }
	f.tracker = fleet.NewTracker(nil, "replica-1", now)
	pooled := newPooledStrategies(f.schedules)
	pooled.budget, pooled.bound = budget, bound
	f.publisher = &fleetPublisher{tracker: f.tracker, replica: "replica-1", now: now, pooled: pooled,
		owned: func() []execution.QueryGroupIdentity { return f.owned }}
	return f
}

// restored is the pool line a Runner writes when it reads its Query Group's
// pool record back: the schedule the pool holds it under, no strategy.
func (f *pooledFixture) restored(queryGroup, revision string, start int64) {
	f.owned = append(f.owned, execution.QueryGroupIdentity(queryGroup))
	f.tracker.Observe(context.Background(), observability.Observation{
		Component: observability.ComponentScheduler, Stage: observability.StageQueryCooldown,
		Result: observability.ResultDegraded, ReasonCode: "QUERY_TARGET_MISSING",
		Trace: observability.TraceFields{QueryGroupKey: queryGroup, OwnerEpoch: 3},
		QueryCooldown: &observability.QueryCooldownFacts{Event: "restored", Source: "restored", EnteredAt: f.at.Add(-time.Hour),
			Until: f.at.Add(time.Hour), ScheduleRevision: revision, SegmentStart: start},
	})
}

func (f *pooledFixture) publish() map[string]fleet.Anomaly {
	f.t.Helper()
	rows := map[string]fleet.Anomaly{}
	for _, row := range f.publisher.snapshot(context.Background()).Demoted {
		rows[row.QueryGroup] = row
	}
	return rows
}

// A Query Group restored into the pool is listed with the strategies of the
// Plans its pool's schedule serves, each once, read once while it is
// pooled; a schedule of another revision, one that could not be read and a
// pool line naming no schedule each say so and name no strategy, and a
// row whose rounds named its strategies is not read. The schedule itself is
// not published.
func TestARestoredPoolsRowNamesTheStrategiesItsScheduleServes(t *testing.T) {
	f := newPooledFixture(t, pooledStrategiesBudget, time.Second)
	f.schedules.answers["qg-read"] = scheduleOf("schedule-r1", 900, "11", "12", "12")
	f.schedules.answers["qg-moved"] = scheduleOf("schedule-r2", 900, "21")
	f.schedules.answers["qg-shifted"] = scheduleOf("schedule-r1", 960, "22")
	f.schedules.answers["qg-observed"] = scheduleOf("schedule-r1", 900, "31")
	f.restored("qg-read", "schedule-r1", 900)
	f.restored("qg-moved", "schedule-r1", 900)
	f.restored("qg-shifted", "schedule-r1", 900)
	f.restored("qg-unread", "schedule-r1", 900)
	f.restored("qg-unknown", "", 0)
	f.tracker.Observe(context.Background(), observability.Observation{ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE",
		ProgressCompletionReason: "QUERY_TARGET_MISSING", Trace: observability.TraceFields{QueryGroupKey: "qg-observed", StrategyID: "39", BusinessID: "2"}})
	f.restored("qg-observed", "schedule-r1", 900)
	for round := 0; round < 2; round++ {
		rows := f.publish()
		if read := rows["qg-read"]; read.StrategiesFrom != fleet.StrategiesFromSchedule || read.StrategiesTotal != 2 ||
			fmt.Sprint(read.Strategies) != fmt.Sprint([]fleet.StrategyRef{{StrategyID: "11", BusinessID: "2"}, {StrategyID: "12", BusinessID: "2"}}) {
			t.Fatalf("publish %d: read row %+v, want 11 and 12 from its schedule", round, read)
		}
		for queryGroup, reason := range map[string]string{"qg-moved": fleet.StrategiesFromScheduleRevisionMoved,
			"qg-shifted": fleet.StrategiesFromScheduleRevisionMoved,
			"qg-unread":  fleet.StrategiesFromScheduleUnread, "qg-unknown": fleet.StrategiesFromScheduleRevisionUnknown} {
			if row := rows[queryGroup]; row.StrategiesFrom != reason || len(row.Strategies) != 0 || row.StrategiesTotal != 0 {
				t.Fatalf("publish %d: %s %+v, want %s and no strategy", round, queryGroup, row, reason)
			}
		}
		if observed := rows["qg-observed"]; observed.StrategiesFrom != "" || fmt.Sprint(observed.Strategies) != "[{39 2}]" {
			t.Fatalf("publish %d: observed row %+v, want its round's strategy and nothing read", round, observed)
		}
	}
	// A schedule read once; a failed read read again on the next publish.
	if f.schedules.readsOf("qg-read") != 1 || f.schedules.readsOf("qg-moved") != 1 || f.schedules.readsOf("qg-unread") != 2 ||
		f.schedules.readsOf("qg-unknown") != 0 || f.schedules.readsOf("qg-observed") != 0 {
		t.Fatalf("reads %v, want each schedule once, the failed one each publish, none for the unknown and the observed", f.schedules.reads)
	}
	encoded, err := json.Marshal(f.publisher.snapshot(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	if text := string(encoded); strings.Contains(text, "schedule-r1") || !strings.Contains(text, `"strategies_from":"schedule"`) {
		t.Fatalf("the snapshot writes the pool's schedule, or not where its strategies came from: %.400s", text)
	}
}

// A pool moved to another schedule - its Segment at the same start
// rewritten - is read for that schedule, not served the one read before.
func TestAPoolMovedToAnotherScheduleIsReadForIt(t *testing.T) {
	f := newPooledFixture(t, pooledStrategiesBudget, time.Second)
	f.schedules.answers["qg-a"] = scheduleOf("schedule-r1", 900, "11")
	f.restored("qg-a", "schedule-r1", 900)
	if row := f.publish()["qg-a"]; fmt.Sprint(row.Strategies) != "[{11 2}]" {
		t.Fatalf("under schedule-r1: %+v, want 11", row)
	}
	f.schedules.answers["qg-a"] = scheduleOf("schedule-r2", 900, "21")
	f.restored("qg-a", "schedule-r2", 900)
	if row := f.publish()["qg-a"]; fmt.Sprint(row.Strategies) != "[{21 2}]" || f.schedules.readsOf("qg-a") != 2 {
		t.Fatalf("moved to schedule-r2: %+v after %d reads, want 21 read for it", row, f.schedules.readsOf("qg-a"))
	}
}

// A catalog that does not answer holds no snapshot: the row is unread for
// as long as the read is out, one read of it at a time, and the read that
// comes back serves the next publish.
func TestAStalledScheduleReadLeavesItsRowUnreadAndTheSnapshotOnTime(t *testing.T) {
	f := newPooledFixture(t, pooledStrategiesBudget, 50*time.Millisecond)
	f.schedules.held = make(chan struct{})
	f.schedules.answers["qg-slow"] = scheduleOf("schedule-r1", 900, "11")
	f.restored("qg-slow", "schedule-r1", 900)
	// How long a publish took is what the case measures: one that waited out
	// the read would take its whole read timeout.
	onTime := func(publish string) {
		t.Helper()
		started := time.Now()
		row := f.publish()["qg-slow"]
		if took := time.Since(started); took > 5*time.Second || row.StrategiesFrom != fleet.StrategiesFromScheduleUnread {
			t.Fatalf("%s publish took %v with %+v, want the bound and the row unread", publish, took, row)
		}
	}
	onTime("first")
	onTime("second")
	if f.schedules.readsOf("qg-slow") != 1 {
		t.Fatalf("%d reads of a schedule still out, want one", f.schedules.readsOf("qg-slow"))
	}
	close(f.schedules.held)
	f.publisher.pooled.bound = 10 * time.Second
	if row := f.publish()["qg-slow"]; row.StrategiesFrom != fleet.StrategiesFromSchedule || f.schedules.readsOf("qg-slow") != 1 {
		t.Fatalf("after the read came back: %+v with %d reads, want its strategies from that one read", row, f.schedules.readsOf("qg-slow"))
	}
}

// Past a publish's reads the rest of the pool waits, named, for the next;
// a schedule read is not read again, and one let go with its row - the
// Query Group no longer this replica's - is read anew if it comes back.
func TestPooledRowsPastAPublishsReadsWaitAndAReleasedOneIsReadAnew(t *testing.T) {
	f := newPooledFixture(t, 2, time.Second)
	for _, queryGroup := range []string{"qg-a", "qg-b", "qg-c"} {
		f.schedules.answers[execution.QueryGroupIdentity(queryGroup)] = scheduleOf("schedule-r1", 900, "11")
		f.restored(queryGroup, "schedule-r1", 900)
	}
	counts := func(rows map[string]fleet.Anomaly) map[string]int {
		by := map[string]int{}
		for _, row := range rows {
			by[row.StrategiesFrom]++
		}
		return by
	}
	if first := counts(f.publish()); first[fleet.StrategiesFromSchedule] != 2 || first[fleet.StrategiesFromSchedulePending] != 1 {
		t.Fatalf("first publish %v, want two read and one pending", first)
	}
	if second := counts(f.publish()); second[fleet.StrategiesFromSchedule] != 3 {
		t.Fatalf("second publish %v, want all three read", second)
	}
	for _, queryGroup := range []string{"qg-a", "qg-b", "qg-c"} {
		if f.schedules.readsOf(queryGroup) != 1 {
			t.Fatalf("reads %v, want each schedule read once", f.schedules.reads)
		}
	}
	f.owned = f.owned[1:]
	f.publish()
	f.restored("qg-a", "schedule-r1", 900)
	if row := f.publish()["qg-a"]; row.StrategiesFrom != fleet.StrategiesFromSchedule || f.schedules.readsOf("qg-a") != 2 {
		t.Fatalf("qg-a back %+v after %d reads, want its schedule read again", row, f.schedules.readsOf("qg-a"))
	}
}
