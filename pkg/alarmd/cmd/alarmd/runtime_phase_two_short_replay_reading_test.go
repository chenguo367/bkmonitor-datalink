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
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// tappedRound is what a Slot reached the bundle's executor as: its operation
// and the divert decision it was let run as a normal round on, if any.
type tappedRound struct {
	operation execution.Operation
	decision  *observability.DivertDecisionFacts
}

// decisionTap is the bundle's executor, keeping what each Slot reached it as.
type decisionTap struct {
	next scheduler.Executor
	mu   sync.Mutex
	seen map[execution.EvaluationTime]tappedRound
}

func (tap *decisionTap) Execute(ctx context.Context, request execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
	round := tappedRound{operation: request.Operation}
	if decision := observability.DivertDecisionFromContext(ctx); decision != nil {
		kept := *decision
		round.decision = &kept
	}
	tap.mu.Lock()
	tap.seen[request.Contract.Slot.EvaluationTime] = round
	tap.mu.Unlock()
	return tap.next.Execute(ctx, request)
}

func (tap *decisionTap) round(slot execution.EvaluationTime) (tappedRound, bool) {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	round, ok := tap.seen[slot]
	return round, ok
}

// A group diverted round after round by its reading - its first read took
// 20 s, each round after is left 7 s - whose replays read in 1 s, whole,
// returns to normal rounds judged by its replays' reading (014 next9a, the
// boundary of B' and E). Each replay short of time is the normal round in its
// place, and access times its first read into the group's reading as it
// would that round's: the Slot request's replay distance and age reach the
// lookback through the coordinator's query request and access. The 20 s read
// stays the group's longest until it ages, sixteen rounds on, as before; the
// round it returns on is judged by the 1 s its replays read, not the 5 s
// fallback. Before, a replay took no reading: the group came back on the
// fallback, and a round with less left than the fallback stayed diverted.
func TestAGroupWhoseShortReplaysReadQuicklyReturnsToNormalByTheirReading(t *testing.T) {
	tap := &decisionTap{seen: map[execution.EvaluationTime]tappedRound{}}
	fixture := startCutoverFixtureOpened(t, nil, observability.Discard(observability.ComponentRuntime), nil, func(bundle *phaseTwoWorkerBundle) {
		production := bundle.dependencies.Ownership.(*productionPhaseTwoOwnership)
		tap.next, production.dependencies.Executor = production.dependencies.Executor, tap
	})
	ctx := context.Background()
	engine, qg := fixture.bundle.dependencies.Lookback, fixture.queryGroup
	if engine == nil {
		t.Fatal("fixture: the bundle runs no lookback")
	}
	// Each Slot's first read takes took, a read of a window read before -
	// the lookback's own - nothing.
	var mu sync.Mutex
	took, last := 20*time.Second, int64(0)
	takes := func(end int64) time.Duration {
		mu.Lock()
		defer mu.Unlock()
		if end <= last {
			return 0
		}
		last = end
		return took
	}
	fixture.uqTakes.Store(&takes)

	first := execution.EvaluationTime(fixture.base + 60)
	if ended := runSlot(t, fixture, qg, fixture.runner, first); ended != fullEnding {
		t.Fatalf("fixture: the group's first Slot ended %q, want FULL", ended)
	}
	normal, reached := tap.round(first)
	if !reached || normal.operation != execution.OperationNormal || normal.decision == nil {
		t.Fatalf("fixture: the first Slot reached the executor as %+v (%t), want a normal round with its decision", normal, reached)
	}
	if got, measured := engine.QueryDuration(qg); !measured || got != 20*time.Second {
		t.Fatalf("fixture: the group's reading is %v (%t), want its first read's 20 s", got, measured)
	}
	mu.Lock()
	took = time.Second
	mu.Unlock()
	// Where the query deadline is from the evaluation time.
	offset := normal.decision.FrozenDeadlineUnixMilli - int64(first)*1000

	for round := 1; round <= 24; round++ {
		slot := first + execution.EvaluationTime(60*round)
		fixture.clock.Store(int64(slot)*1000 + offset - 7_000)
		for attempt := 0; attempt < 4; attempt++ {
			if _, done := endingsOf(fixture, qg)[slot]; done {
				break
			}
			if _, attempted, err := fixture.runner.RunOne(ctx); err != nil || !attempted {
				t.Fatalf("round %d: attempted=%t error=%v", round, attempted, err)
			}
		}
		ran, reached := tap.round(slot)
		if !reached {
			t.Fatalf("round %d never reached the executor; endings %v", round, endingsOf(fixture, qg))
		}
		if ran.operation == execution.OperationReplay {
			if ended := endingsOf(fixture, qg)[slot]; ended != fullEnding {
				t.Fatalf("round %d's replay ended %q, want FULL", round, ended)
			}
			continue
		}
		if round == 1 {
			t.Fatalf("fixture: the round after the 20 s read ran as %+v with 7 s left, want it diverted", ran)
		}
		if ran.operation != execution.OperationNormal || ran.decision == nil ||
			ran.decision.BoundSource != observability.DivertBoundQueryDuration || ran.decision.BoundMillis != 1_000 {
			t.Fatalf("round %d ran as %q on %+v; want a normal round judged by its replays' 1 s reading, not the fallback",
				round, ran.operation, ran.decision)
		}
		t.Logf("diverted %d rounds, then a normal round on %+v", round-1, *ran.decision)
		return
	}
	t.Fatalf("the group never returned to normal rounds; endings %v", endingsOf(fixture, qg))
}
