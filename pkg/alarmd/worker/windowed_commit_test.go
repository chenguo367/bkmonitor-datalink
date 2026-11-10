// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The watchdog's grace (cmd/alarmd executionPastDeadlineGrace) and a window's
// bound (worker windowStepBound), restated: both are a minute.
const (
	watchdogGrace = time.Minute
	windowBound   = time.Minute
)

// hookedEvaluator runs hook before the at-th series it evaluates, which in
// the windowed fixture (a b | c d | e f) is a point between two windows when
// at is four: the first window landed when the third series' result would
// have overfilled it, before the fourth was evaluated.
type hookedEvaluator struct {
	*recordingPorts
	calls, at int
	hook      func()
}

func (evaluator *hookedEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	evaluator.calls++
	if evaluator.calls == evaluator.at && evaluator.hook != nil {
		evaluator.hook()
	}
	return evaluator.recordingPorts.Evaluate(ctx, request)
}

// hookedWindowedSlot is windowedSlot with the evaluator hooked.
func hookedWindowedSlot(t *testing.T) (*recordingPorts, *persistedRetryState, *hookedEvaluator, *worker.SlotExecutionCoordinator) {
	t.Helper()
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	state := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	evaluator := &hookedEvaluator{recordingPorts: ports, at: 4}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: evaluator, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
		Hosts: worker.SharedHostBusiness, Events: ports, State: state, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {})},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 2, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	return ports, state, evaluator, coordinator
}

// markedContext is an execution running under deadline, before its commit.
func markedContext(deadline time.Time) (context.Context, *execution.StageMarker) {
	marker := &execution.StageMarker{}
	marker.Begin(execution.SlotStageEvaluate)
	marker.Extend(deadline, deadline)
	return execution.WithStageMarker(context.Background(), marker), marker
}

// 2.2 step 6 a: a Slot applied window by window leaves its commit once a
// window's State is applied, so the watchdog can claim it between two
// windows. Claimed there, the next window writes nothing - no event, no
// State - and the attempt ends declined. The next holder's attempt resumes the
// landed window's series without sending them again, sends the rest once,
// and writes the tail and Progress.
func TestADeclineBetweenTwoWindowsWritesNothingMoreAndTheNextAttemptFinishesTheSlot(t *testing.T) {
	ports, state, evaluator, coordinator := hookedWindowedSlot(t)
	ctx, marker := markedContext(time.Now())
	claimed := false
	evaluator.hook = func() {
		_, _, claimed = marker.Decline(time.Now().Add(24*time.Hour), watchdogGrace)
	}
	request := windowedRequest()
	if _, err := coordinator.Execute(ctx, request); !errors.Is(err, execution.ErrExecutionDeclined) {
		t.Fatalf("first attempt err = %v, want it declined", err)
	}
	if !claimed {
		t.Fatal("the watchdog could not claim the Slot between two windows")
	}
	series := windowedSeries("a", "b", "c", "d", "e", "f")
	a, b, c, d, e, f := series[0], series[1], series[2], series[3], series[4], series[5]
	if got, want := sortedBatches(ports.eventBatches, 0), [][]string{{a, b}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first attempt event batches = %v, want only the window before the claim", got)
	}
	if len(state.saved) != 2 || countStage(*ports.trace, "progress_commit") != 0 {
		t.Fatalf("first attempt wrote State for %d series and %d Progress commits, want the first window's two and none",
			len(state.saved), countStage(*ports.trace, "progress_commit"))
	}

	evaluator.hook = nil
	batches := len(ports.eventBatches)
	result, err := coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("next attempt result=%+v err=%v", result, err)
	}
	if got, want := sortedBatches(ports.eventBatches, batches), [][]string{{c, d}, {e, f}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("next attempt event batches = %v, want the unlanded windows once and nothing of the landed one", got)
	}
	if len(state.saved) != 6 || countStage(*ports.trace, "progress_commit") != 1 {
		t.Fatalf("after the next attempt State for %d series and %d Progress commits, want all six and its one",
			len(state.saved), countStage(*ports.trace, "progress_commit"))
	}
}

// 2.2 step 6 b: each window boundary moves the Slot's deadline a window's
// bound ahead. A Slot already past its query deadline by more than the grace
// but landing windows is not claimed between them; a window that takes longer
// than its bound and the grace after the boundary before it is.
func TestAProgressingWindowedSlotIsNotDeclinedAndAStuckWindowIs(t *testing.T) {
	for _, test := range []struct {
		name    string
		tickIn  time.Duration
		claimed bool
	}{
		{"progressing", 0, false},
		{"stuck", windowBound + watchdogGrace + time.Second, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, evaluator, coordinator := hookedWindowedSlot(t)
			// Past its own deadline by more than the grace: claimed at the
			// first tick if the boundary had not moved the deadline.
			ctx, marker := markedContext(time.Now().Add(-90 * time.Second))
			claimed := false
			evaluator.hook = func() {
				_, _, claimed = marker.Decline(time.Now().Add(test.tickIn), watchdogGrace)
			}
			result, err := coordinator.Execute(ctx, windowedRequest())
			if claimed != test.claimed {
				t.Fatalf("claimed between the windows = %v, want %v", claimed, test.claimed)
			}
			if test.claimed {
				if !errors.Is(err, execution.ErrExecutionDeclined) {
					t.Fatalf("a claimed Slot ended with %v, want it declined", err)
				}
				return
			}
			if err != nil || !result.Completed {
				t.Fatalf("a progressing Slot result=%+v err=%v, want it completed", result, err)
			}
		})
	}
}
