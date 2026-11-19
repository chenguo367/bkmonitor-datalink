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
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// slotWaitRunner is one Query Group of the execution slot wait cases, its
// round starting one Slot of operation through the production Slot executor -
// the one that writes slot_started - which runs execute. readyAt is what its
// Runner reports as its readiness: none (zero) for a round due now, as a
// scheduler Runner reports it, which the walk puts in the ready queue; an
// instant for one deferred to it, which waits in the recovery queue.
type slotWaitRunner struct {
	queryGroup execution.QueryGroupIdentity
	readyAt    time.Time
	operation  execution.Operation
	execute    func()
}

// newSlotWaitBundle is a dispatcher bundle on a clock the case moves, its
// Query Groups' Slots started through observedProductionSlotExecutor with
// the recorder as its observer, so a dispatch's wait goes the production way
// from the dispatcher to the counters. Each Query Group runs once.
func newSlotWaitBundle(t *testing.T, slots, readyQueue int, clock *atomic.Int64, runners []slotWaitRunner) (*phaseTwoWorkerBundle, *metric.Recorder, chan execution.QueryGroupIdentity) {
	t.Helper()
	cfg := validGoAccessRuntimeConfig()
	cfg.PhaseTwo.Scheduler.ProcessQueryPermits = 1
	cfg.PhaseTwo.Scheduler.RecoveryQueryPermits = 0
	cfg.PhaseTwo.Scheduler.ActiveExecutionLimit = slots
	if readyQueue > 0 {
		cfg.PhaseTwo.Scheduler.ReadyQueueCapacity = readyQueue
	}
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	recorder := metric.NewRecorder(metric.BuildInfo{})
	started := make(chan execution.QueryGroupIdentity, len(runners))
	bundle := &phaseTwoWorkerBundle{
		dependencies: phaseTwoWorkerBundleDependencies{Config: cfg, Now: now, Recorder: recorder},
		runners:      make(map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle),
	}
	for _, runner := range runners {
		runner := runner
		var ran atomic.Bool
		executor := observedProductionSlotExecutor{
			next: slotExecutorFunc(func(context.Context, execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
				if runner.execute != nil {
					runner.execute()
				}
				return execution.SlotExecutionResult{}, nil
			}),
			observer: recorder,
		}
		bundle.runners[runner.queryGroup] = &phaseTwoQueryGroupLifecycle{runner: &callbackPhaseTwoQueryGroup{
			operation: runner.operation,
			nextReadyAt: func() time.Time {
				if ran.Load() {
					return now().Add(time.Hour)
				}
				return runner.readyAt
			},
			dueBound: func() scheduler.RunnerDueBound { return scheduler.RunnerDueBound{IntervalSeconds: 60} },
			run: func(ctx context.Context) (execution.SlotExecutionResult, bool, error) {
				ran.Store(true)
				_, err := executor.Execute(ctx, execution.SlotExecutionRequest{Operation: runner.operation})
				// After the Slot started and returned, so its slot_started
				// line has been counted when the case reads the counters.
				started <- runner.queryGroup
				return execution.SlotExecutionResult{}, false, err
			},
		}}
	}
	return bundle, recorder, started
}

func slotWaits(t *testing.T, recorder *metric.Recorder, operation, waited string) float64 {
	t.Helper()
	return counterValue(t, recorder, "alarmd_scheduler_execution_slot_waits_total",
		map[string]string{"operation": operation, "waited": waited, "cohort": "60s"})
}

func slotWaitSeconds(t *testing.T, recorder *metric.Recorder, operation string) float64 {
	t.Helper()
	return counterValue(t, recorder, "alarmd_scheduler_execution_slot_wait_seconds_total",
		map[string]string{"operation": operation, "cohort": "60s"})
}

// One execution slot, held by a Slot that found it free. A normal Slot ready
// alongside it, and a replay that became ready two seconds into the hold,
// wait until the hold ends five seconds in; the dispatcher sleeps the whole
// time, nothing waking it until the held Slot returns. The normal Slot
// waited the five seconds the slot was full after it could go, the replay
// the three after its readiness - not the five since it was queued - and the
// replay is counted under replay, apart from the normal Slots. The five
// seconds pass between the holder's dispatch and the pass after it: the
// clock moves at the snapshot the dispatching pass takes once it has read
// the clock and before it dispatches, so the hold is counted from the
// dispatch, not from the pass that comes after it.
func TestASlotReadyWhileEveryExecutionSlotIsInUseCountsTheFullTimeSinceItCouldGo(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	release := make(chan struct{})
	holding := make(chan struct{})
	bundle, recorder, started := newSlotWaitBundle(t, 1, 0, &clock, []slotWaitRunner{
		{queryGroup: "query-group-a-holder", operation: execution.OperationNormal,
			execute: func() { close(holding); <-release }},
		{queryGroup: "query-group-b-normal", operation: execution.OperationNormal},
		{queryGroup: "query-group-c-replay", readyAt: start.Add(2 * time.Second), operation: execution.OperationReplay},
	})
	// The dispatching pass's snapshot is the one with all three queued -
	// the two ready, the replay waiting for its readiness - and nothing yet
	// executing.
	var moved atomic.Bool
	bundle.dependencies.Observer = observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		facts := observation.Dispatcher
		if observation.Stage != observability.StageDispatcherSnapshot || facts == nil {
			return
		}
		if facts.Active == 0 && facts.Ready == 2 && facts.Delayed == 1 && moved.CompareAndSwap(false, true) {
			clock.Store(start.Add(5 * time.Second).UnixNano())
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- bundle.runScheduler(ctx, wake, nil, false) }()
	wake <- struct{}{}

	select {
	case <-holding:
	case <-testwait.Expired(t):
		t.Fatal("the holder's Slot never started")
	}
	if !moved.Load() {
		t.Fatal("the clock did not move at the dispatching pass's snapshot; the case is about a hold counted from the dispatch")
	}
	// The replay's readiness came while the slot was held, and the hold goes
	// on past it; the loop is asleep for all of it.
	close(release)
	collectDispatches(t, started, 3)
	stopDeadlineDispatcher(t, cancel, done)

	if got := slotWaits(t, recorder, "normal", "false"); got != 1 {
		t.Fatalf("normal Slots that found a slot free = %v, want 1: the holder", got)
	}
	if got := slotWaits(t, recorder, "normal", "true"); got != 1 {
		t.Fatalf("normal Slots that waited for an execution slot = %v, want 1", got)
	}
	if got := slotWaitSeconds(t, recorder, "normal"); got != 5 {
		t.Fatalf("normal wait = %v s, want 5: ready when the holder took the only slot, it waited the "+
			"whole hold, slept through by the dispatcher", got)
	}
	if got := slotWaits(t, recorder, "replay", "true"); got != 1 {
		t.Fatalf("replays that waited = %v, want 1, counted under replay", got)
	}
	if got := slotWaitSeconds(t, recorder, "replay"); got != 3 {
		t.Fatalf("replay wait = %v s, want 3: from its readiness two seconds in, not from when it was queued", got)
	}
}

// With a slot free for every Slot, nothing waits: each dispatch is counted,
// under waited=false, with no seconds.
func TestASlotThatFindsAnExecutionSlotFreeIsCountedAsNotHavingWaited(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	bundle, recorder, started := newSlotWaitBundle(t, 2, 0, &clock, []slotWaitRunner{
		{queryGroup: "query-group-a", operation: execution.OperationNormal},
		{queryGroup: "query-group-b", operation: execution.OperationNormal},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- bundle.runScheduler(ctx, wake, nil, false) }()
	wake <- struct{}{}
	collectDispatches(t, started, 2)
	clock.Store(start.Add(5 * time.Second).UnixNano())
	stopDeadlineDispatcher(t, cancel, done)

	if got := slotWaits(t, recorder, "normal", "false"); got != 2 {
		t.Fatalf("normal Slots that found a slot free = %v, want 2", got)
	}
	if got := slotWaits(t, recorder, "normal", "true"); got != 0 {
		t.Fatalf("normal Slots that waited = %v, want 0", got)
	}
	if got := slotWaitSeconds(t, recorder, "normal"); got != 0 {
		t.Fatalf("normal wait = %v s, want 0", got)
	}
}

// A dispatch's wait goes to the first Slot its round starts; a later Slot of
// the round ran on the slot the dispatch already held, and a Slot no
// dispatch started carries no wait at all.
func TestADispatchsWaitGoesToTheFirstSlotItStarts(t *testing.T) {
	ctx := withDispatchSlotWait(context.Background(), 2*time.Second, "60s")
	first, second := takeDispatchSlotWait(ctx), takeDispatchSlotWait(ctx)
	if first == nil || first.WaitMillis != 2000 || first.Cohort != "60s" {
		t.Fatalf("first Slot's wait = %+v, want 2000 ms under 60s", first)
	}
	if second == nil || second.Waited() || second.Cohort != "60s" {
		t.Fatalf("second Slot's wait = %+v, want none waited, under the same cohort", second)
	}
	if undispatched := takeDispatchSlotWait(context.Background()); undispatched != nil {
		t.Fatalf("a Slot no dispatch started carries %+v, want nothing", undispatched)
	}
}

// A Query Group the full ready queue held back is not waiting for an
// execution slot: the walk takes it up again once a dispatch frees a place,
// and its wait starts there. With one slot and a ready queue of one, the
// holder takes the slot, the second Query Group the place the holder's
// dispatch frees, and the third is turned away; five seconds later the hold
// ends, the second goes having waited the five, and the third is queued only
// then, finds the slot free once the second returns, and did not wait.
func TestAQueryGroupTheFullReadyQueueHeldBackWaitsOnlyFromWhenItIsQueued(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	release := make(chan struct{})
	holding := make(chan struct{})
	bundle, recorder, started := newSlotWaitBundle(t, 1, 1, &clock, []slotWaitRunner{
		{queryGroup: "query-group-a-holder", operation: execution.OperationNormal,
			execute: func() { close(holding); <-release }},
		{queryGroup: "query-group-b", operation: execution.OperationNormal},
		{queryGroup: "query-group-c", operation: execution.OperationNormal},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- bundle.runScheduler(ctx, wake, nil, false) }()
	wake <- struct{}{}
	select {
	case <-holding:
	case <-testwait.Expired(t):
		t.Fatal("the holder's Slot never started")
	}
	// The second is turned away while the holder waits in the ready queue,
	// and takes the place the holder's dispatch frees; the third, behind it
	// on the same walk, is then turned away. Only once both have happened is
	// the second queued before the hold goes on.
	heldBack := func() float64 {
		return counterValue(t, recorder, "alarmd_dispatch_queue_turnaways_total",
			map[string]string{"outcome": "normal_queue_full", "cohort": "60s"})
	}
	testwait.Until(t, "the full ready queue turning the second away and then the third", func() bool { return heldBack() >= 2 })
	clock.Store(start.Add(5 * time.Second).UnixNano())
	close(release)
	collectDispatches(t, started, 3)
	stopDeadlineDispatcher(t, cancel, done)
	if got := slotWaits(t, recorder, "normal", "true"); got != 1 {
		t.Fatalf("normal Slots that waited for an execution slot = %v, want 1: the second", got)
	}
	if got := slotWaitSeconds(t, recorder, "normal"); got != 5 {
		t.Fatalf("normal wait = %v s, want 5, the second's alone: the third was held back by the queue, "+
			"not by the slot, and queued when the hold had ended", got)
	}
	if got := slotWaits(t, recorder, "normal", "false"); got != 2 {
		t.Fatalf("normal Slots that found a slot free = %v, want 2: the holder and the third", got)
	}
}
