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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// A Slot attempt that executes past half its Query Group's period is said on
// a slot_execute_slow line beside its completion and counted, a retry's and
// a replay's as a normal round's, through the production Slot executor: the
// period its dispatch knew, the threshold it gave, where the time went - the
// phases and the query waits, bodies and slices of its usage, its dispatch's
// wait for an execution slot, and its queries' waits for permits as the
// scheduler marks them on its context - and its completion unchanged. An
// attempt a millisecond under the threshold says nothing and is not counted.
func TestASlotAttemptPastHalfItsPeriodIsSaidAndCounted(t *testing.T) {
	recorder := metric.NewRecorder(metric.BuildInfo{})
	var said []observability.Observation
	observer := observability.ObserverFunc(func(ctx context.Context, observation observability.Observation) {
		said = append(said, observation)
		recorder.Observe(ctx, observation)
	})
	now := time.Unix(1_700_000_000, 0)
	var took time.Duration
	executor := observedProductionSlotExecutor{
		next: slotExecutorFunc(func(ctx context.Context, request execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
			capture := observability.BudgetCaptureFromContext(ctx)
			capture.MarkPermitWait(700 * time.Millisecond)
			capture.MarkPermitWait(300 * time.Millisecond)
			now = now.Add(took)
			return execution.SlotExecutionResult{
				Usage: execution.SlotBudgetUsage{QueryWaitMillis: 6000, QueryBodyMillis: 1500, QueryWaitMillisMax: 4000,
					QueryBodyMillisMax: 900, InputSlices: 3, InputSliceTailMillis: 800, InputSliceTailMillisMax: 500},
				Timing: execution.SlotTiming{Slot: 11000, Input: 7000, Apply: 2000},
			}, nil
		}),
		observer: observer,
		now:      func() time.Time { return now },
	}
	attempt := func(operation execution.Operation, elapsed time.Duration) {
		t.Helper()
		took = elapsed
		ctx := withDispatchSlotWait(context.Background(), 1500*time.Millisecond, "60s", 20)
		if _, err := executor.Execute(ctx, execution.SlotExecutionRequest{Operation: operation}); err != nil {
			t.Fatal(err)
		}
	}
	for _, operation := range []execution.Operation{execution.OperationNormal, execution.OperationRetry, execution.OperationReplay} {
		said = nil
		attempt(operation, 10*time.Second-time.Millisecond)
		attempt(operation, 11*time.Second)
		var slow, completed []observability.Observation
		for _, observation := range said {
			switch observation.Stage {
			case observability.StageSlotExecuteSlow:
				slow = append(slow, observation)
			case observability.StageSlotCompleted:
				completed = append(completed, observation)
			}
		}
		if len(slow) != 1 || len(completed) != 2 {
			t.Fatalf("%s: %d slow lines and %d completions over an attempt just under the threshold and one past it, want 1 and 2",
				operation, len(slow), len(completed))
		}
		facts := slow[0].SlotExecuteSlow
		want := observability.SlotExecuteSlowFacts{PeriodSeconds: 20, ThresholdMillis: 10000,
			Timing:          observability.SlotTimingFacts{Slot: 11000, Input: 7000, Apply: 2000},
			QueryWaitMillis: 6000, QueryBodyMillis: 1500, QueryWaitMillisMax: 4000, QueryBodyMillisMax: 900, InputSlices: 3,
			InputSliceTailMillis: 800, InputSliceTailMillisMax: 500, ExecutionSlotWaitMillis: 1500, QueryPermitWaitMillis: 1000,
			QueryPermitWaitMillisMax: 700}
		if slow[0].Operation != observability.Operation(operation) || slow[0].Duration != 11*time.Second ||
			slow[0].Result != observability.Result(observability.ResultDegraded) || slow[0].ReasonCode != observability.ReasonSlotExecuteSlow ||
			facts == nil || *facts != want {
			t.Fatalf("%s: the slow line %+v (facts %+v), want an 11 s attempt of %s with %+v", operation, slow[0], facts, operation, want)
		}
		for _, completion := range completed {
			if completion.SlotExecuteSlow != nil || completion.Result != observability.Result(observability.ResultSuccess) {
				t.Fatalf("%s: a completion %+v carries the slow line's facts or changed its result", operation, completion)
			}
		}
		if completed[1].Duration != 11*time.Second {
			t.Fatalf("%s: the completion of the slow attempt says %s, want 11s", operation, completed[1].Duration)
		}
		if got := counterValue(t, recorder, "alarmd_slot_execute_slow_total", map[string]string{"operation": string(operation)}); got != 1 {
			t.Fatalf("%s: %v slow attempts counted, want the one", operation, got)
		}
	}
}

// A query's wait for its permit reaches its attempt through the production
// permit acquirer, which hands the coordinator the attempt's context as
// access hands it: with every permit held, a normal query waits on the
// process permits and a replay's on the recovery channels, and each wait is
// in the budget capture of the context it asked under. A context lost on
// the way would leave every slow line's permit waits at zero.
func TestAPermitWaitReachesItsAttemptThroughTheProductionAcquirer(t *testing.T) {
	flights, err := scheduler.NewFlightCoordinatorWithRecovery(scheduler.RecoveryLimits{
		ProcessQueryPermits: 2, RecoveryQueryPermits: 1,
		ReadyQueueCapacity: 8, RecoveryQueueCapacity: 8,
		MaxQueuedItemsPerQG: 2,
		MaxReplaySlots:      3, MaxReplayAge: 10 * time.Minute,
		RetryMinDelay: time.Second, RetryMaxDelay: 8 * time.Second,
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	acquirer := productionQueryPermitAcquirer{flights: flights}
	deadline := time.Now().Add(time.Minute)
	const held = 50 * time.Millisecond

	var normalHeld []access.QueryPermit
	for _, group := range []execution.QueryGroupIdentity{"a", "a2"} {
		permit, err := acquirer.AcquireQueryPermit(context.Background(), execution.SlotIdentity{QueryGroup: group, EvaluationTime: 60},
			execution.OperationNormal, deadline)
		if err != nil {
			t.Fatal(err)
		}
		normalHeld = append(normalHeld, permit)
	}
	normalCtx, normal := observability.ContextWithBudgetCapture(context.Background())
	granted := make(chan access.QueryPermit, 1)
	go func() {
		permit, err := acquirer.AcquireQueryPermit(normalCtx, execution.SlotIdentity{QueryGroup: "b", EvaluationTime: 60},
			execution.OperationNormal, deadline)
		if err != nil {
			t.Error(err)
		}
		granted <- permit
	}()
	testwait.Until(t, "a normal query queued for a permit", func() bool { return flights.QueryPermitOccupancy().Waiting["normal"] > 0 })
	time.Sleep(held) // fixed-wait: held a known time, the least the queued query waits
	for _, permit := range normalHeld {
		permit.Release()
	}
	if permit := <-granted; permit != nil {
		permit.Release()
	}
	if total, longest := normal.PermitWaits(); total < held || longest != total {
		t.Fatalf("the normal query's attempt waited %s (longest %s) for its permit, want its one wait of at least %s", total, longest, held)
	}

	replay := execution.SlotIdentity{QueryGroup: "c", EvaluationTime: 60}
	channelsHeld, err := acquirer.AcquireRecoveryChannels(context.Background(), execution.SlotIdentity{QueryGroup: "d", EvaluationTime: 60},
		execution.OperationReplay, deadline, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	replayCtx, replayed := observability.ContextWithBudgetCapture(context.Background())
	channels := make(chan access.RecoveryChannels, 1)
	go func() {
		got, err := acquirer.AcquireRecoveryChannels(replayCtx, replay, execution.OperationReplay, deadline, 1, nil)
		if err != nil {
			t.Error(err)
		}
		channels <- got
	}()
	testwait.Until(t, "a replay queued for the recovery channels", func() bool { return flights.QueryPermitOccupancy().Waiting["recovery"] > 0 })
	time.Sleep(held) // fixed-wait: held a known time, the least the queued query waits
	channelsHeld.Release()
	got := <-channels
	if got == nil {
		t.Fatal("fixture: the replay was given no channels")
	}
	permit, err := got.AcquireQueryPermit(replayCtx, replay, execution.OperationReplay, deadline)
	if err != nil {
		t.Fatal(err)
	}
	permit.Release()
	got.Release()
	if total, longest := replayed.PermitWaits(); total < held || longest < held {
		t.Fatalf("the replay's attempt waited %s (longest %s) for its channels and permit, want at least the %s the channels were held",
			total, longest, held)
	}
}
