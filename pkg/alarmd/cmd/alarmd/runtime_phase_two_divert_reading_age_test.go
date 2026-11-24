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
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// agingClock is the lookback's clock, which the case moves.
type agingClock struct {
	mu sync.Mutex
	at time.Time
}

func (clock *agingClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.at
}

func (clock *agingClock) set(at time.Time) {
	clock.mu.Lock()
	clock.at = at
	clock.mu.Unlock()
}

// readingLookback is a lookback on clock that has read qg's query once: a
// whole first read of a Slot whose frozen schedule has the next a minute on,
// which took took and ended at clock's time then.
func readingLookback(t *testing.T, qg execution.QueryGroupIdentity, clock *agingClock, took time.Duration) *lookback.Engine {
	t.Helper()
	engine, err := lookback.New(lookback.Options{Now: clock.now,
		Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, nil
		},
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	query := sampledQuery(qg)
	query.FollowingSlot = query.Contract.Slot.EvaluationTime + 60
	read := engine.Begin(query)
	if read == nil {
		t.Fatal("the lookback took no read of the group's first read")
	}
	clock.set(clock.now().Add(took))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if got, ok := engine.QueryDuration(qg); !ok || got != took {
		t.Fatalf("the lookback's reading is %v %v, want %v", got, ok, took)
	}
	return engine
}

func noReadHoldRecords() *runtimeTestReadHoldControl {
	return &runtimeTestReadHoldControl{values: map[execution.QueryGroupIdentity][]byte{}}
}

// A group whose own reading - one first read of 30 s - leaves its round 7 s
// short of it is diverted to replay, and takes no reading while no read of it
// completes whole - here its replays are never read. Within sixteen of its
// rounds since that reading the round is diverted by it; past them the
// reading has aged, and the round runs as a normal round on the 5 s fallback,
// through the slot source the bundle wires to the lookback's reading (014
// next8). Before readings aged, the group was diverted every round for good.
// A replay short of time that reads whole is timed as the group's first read
// (014 next9a): TestAGroupDivertedEveryRoundWhoseReplaysReadAsSlowlyStaysDiverted.
func TestAGroupDivertedEveryRoundByItsReadingRunsAsANormalRoundOnceTheReadingAged(t *testing.T) {
	fact, schedule, deadline := inheritedDivertGroup(t)
	qg := schedule.Segment.QueryGroup
	clock := &agingClock{at: time.Unix(1_700_000_060, 0)}
	engine := readingLookback(t, qg, clock, 30*time.Second)
	read := clock.now()

	clock.set(read.Add(15 * time.Minute))
	facts, _, operation := successorRoundWith(t, noReadHoldRecords(), fact, schedule, deadline, 0, lookbackQueryLatency(engine))
	if operation != execution.OperationReplay || len(facts) != 1 || facts[0].Outcome != observability.ReplayTakeoverDiverted ||
		facts[0].FloorSource != observability.TakeoverFloorObserved || facts[0].FloorSeconds != 30 {
		t.Fatalf("fifteen rounds after the reading: %q, takeover facts %+v; want it diverted by its own 30 s reading", operation, facts)
	}

	clock.set(read.Add(16*time.Minute + time.Second))
	facts, _, operation = successorRoundWith(t, noReadHoldRecords(), fact, schedule, deadline, 0, lookbackQueryLatency(engine))
	if operation != execution.OperationNormal {
		t.Fatalf("sixteen rounds after the reading the round reached the executor as %q, want it run as a normal round", operation)
	}
	for _, got := range facts {
		if got.Outcome == observability.ReplayTakeoverDiverted {
			t.Fatalf("its reading aged, the round was diverted again: %+v", got)
		}
	}
}

// A group diverted round after round by its own reading - one first read of
// 30 s, each round left 7 s - whose replays read as slowly, each whole, stays
// diverted, judged by its own reading round after round: a replay short of
// time is the normal round in its place, and its first read is timed into
// the group's reading as that round's would be (014 next9a, the boundary of
// B' and E), so the reading does not age while the group is read. The
// replays' reads are timed here as access times them (lookbackQuery, Begin
// after the permit, Complete). Before, a replay took no reading: sixteen
// rounds on the reading aged, and the round ran as a normal round on the 5 s
// fallback with 7 s left for a 30 s read - cut at its deadline, a cut first
// read timing nothing, and so every round after.
func TestAGroupDivertedEveryRoundWhoseReplaysReadAsSlowlyStaysDiverted(t *testing.T) {
	fact, schedule, deadline := inheritedDivertGroup(t)
	qg := schedule.Segment.QueryGroup
	clock := &agingClock{at: time.Unix(1_700_000_060, 0)}
	const took = 30 * time.Second
	engine := readingLookback(t, qg, clock, took)
	read := clock.now()

	for round := 1; round <= 20; round++ {
		clock.set(read.Add(time.Duration(round) * time.Minute))
		facts, _, operation := successorRoundWith(t, noReadHoldRecords(), fact, schedule, deadline, 0, lookbackQueryLatency(engine))
		if operation != execution.OperationReplay || len(facts) != 1 || facts[0].Outcome != observability.ReplayTakeoverDiverted ||
			facts[0].FloorSource != observability.TakeoverFloorObserved || facts[0].FloorSeconds != 30 {
			t.Fatalf("round %d after the first read: %q, takeover facts %+v; want it diverted by its own 30 s reading", round,
				operation, facts)
		}
		// The round's replay: a replay short of time's first read, whole in
		// 30 s, inside the minute its normal round has from its readiness.
		query := sampledQuery(qg)
		query.Contract.Slot.EvaluationTime += execution.EvaluationTime(60 * round)
		query.Operation, query.ReplayShortOfTime = execution.OperationReplay, true
		query.ReadyAt = time.Unix(int64(query.Contract.Slot.EvaluationTime), 0)
		query.NormalDeadline = query.ReadyAt.Add(time.Minute)
		replay := engine.Begin(query)
		clock.set(clock.now().Add(took))
		replay.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	}
}

// A group measured here whose readings have aged is judged by the fallback,
// not by the reading its previous owner left in its read-hold record: an
// inherited reading stands in only until the group is measured here
// (014 next8). The record keeps 9 s, the round has 7 s left: by the record it
// would be diverted, as by its own 8 s reading before readings aged.
func TestAGroupMeasuredHereIsNotJudgedByAnInheritedReadingOnceItsOwnAged(t *testing.T) {
	fact, schedule, deadline := inheritedDivertGroup(t)
	qg := schedule.Segment.QueryGroup
	store := noReadHoldRecords()
	raw, err := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 30_000})
	if err != nil {
		t.Fatal(err)
	}
	store.values[qg] = raw
	ownerMeasures(t, store, schedule, time.Unix(int64(schedule.Segment.Start)+1, 0))
	clock := &agingClock{at: time.Unix(1_700_000_060, 0)}
	engine := readingLookback(t, qg, clock, 8*time.Second)
	clock.set(clock.now().Add(16*time.Minute + time.Second))

	facts, holds, operation := successorRoundWith(t, store, fact, schedule, deadline, 30*time.Second, lookbackQueryLatency(engine))
	if got, ok := holds.controller.InheritedQueryDuration(qg); !ok || got != 9*time.Second {
		t.Fatalf("fixture: the record keeps %v (%v), want the previous owner's 9 s", got, ok)
	}
	if operation != execution.OperationNormal {
		t.Fatalf("the round reached the executor as %q, takeover facts %+v; want it run as a normal round on the fallback",
			operation, facts)
	}
	for _, got := range facts {
		if got.Outcome == observability.ReplayTakeoverDiverted {
			t.Fatalf("the round was diverted: %+v", got)
		}
	}
}
