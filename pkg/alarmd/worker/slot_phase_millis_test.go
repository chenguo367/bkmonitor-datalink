// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slowPorts is the fixture's ports with a delay in one named place, so a case
// can move one phase's clock and no other.
type slowPorts struct {
	*recordingPorts
	preflight time.Duration
	evaluate  time.Duration
	input     time.Duration
}

// slowConsumer is the Slot's own consumer with a wait between Begin and the
// first record, which is what a query that takes its time looks like from
// inside the Slot.
type slowConsumer struct {
	execution.QueryExecutionConsumer
	wait time.Duration
}

func (consumer slowConsumer) Begin(ctx context.Context, header execution.InternalExecutionHeader) error {
	if err := consumer.QueryExecutionConsumer.Begin(ctx, header); err != nil {
		return err
	}
	time.Sleep(consumer.wait)
	return nil
}

func (ports *slowPorts) Execute(ctx context.Context, request execution.QueryExecutionRequest, consumer execution.QueryExecutionConsumer) (execution.QueryExecutionCompletion, error) {
	if ports.input > 0 {
		consumer = slowConsumer{QueryExecutionConsumer: consumer, wait: ports.input}
	}
	return ports.recordingPorts.Execute(ctx, request, consumer)
}

func (ports *slowPorts) LoadRuntime(ctx context.Context, request execution.StatePreflightRequest) (execution.StatePreflightResult, error) {
	time.Sleep(ports.preflight)
	return ports.recordingPorts.LoadRuntime(ctx, request)
}

func (ports *slowPorts) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	time.Sleep(ports.evaluate)
	return ports.recordingPorts.Evaluate(ctx, request)
}

func runSlowSlot(t *testing.T, input, preflight, evaluate time.Duration) execution.SlotTiming {
	t.Helper()
	result, err := executeSlowSlot(t, "", input, preflight, evaluate)
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v error=%v", result, err)
	}
	return result.Timing
}

func executeSlowSlot(t *testing.T, failStage string, input, preflight, evaluate time.Duration) (execution.SlotExecutionResult, error) {
	t.Helper()
	trace := make([]string, 0, len(fullTrace))
	ports := &slowPorts{recordingPorts: &recordingPorts{trace: &trace, ready: true, failStage: failStage},
		input: input, preflight: preflight, evaluate: evaluate}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports,
		Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: ports, Admission: ports, GapGuard: ports,
		NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: ports, State: ports, Progress: ports,
		Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {}),
	}, worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 100, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatalf("NewSlotExecutionCoordinator() error: %v", err)
	}
	return coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
}

// A completed Slot says where its wall clock went, and each phase answers for
// its own work only.
//
// The three cases delay one place each and read two numbers, because the
// failure this pins is not "the clock is zero" -- it is one duration reported
// under several names, which a case that slows everything at once cannot tell
// from a real split. Every phase here is already on its own line per call;
// what no line could answer is what a Slot spent in each in total, which is
// the question asked of a Slot that overran its period.
func TestACompletedSlotSaysWhichPhaseSpentItsTime(t *testing.T) {
	const slow = 40 * time.Millisecond
	// Wide of the delay in both directions: the assertion is that the clock
	// followed the delay into its own phase, not that a test machine is
	// punctual.
	const moved, still = 30, 20

	for name, testCase := range map[string]struct {
		input, preflight, evaluate time.Duration
		phase                      func(execution.SlotTiming) (string, uint64, string, uint64)
	}{
		"records that take their time": {input: slow, phase: func(timing execution.SlotTiming) (string, uint64, string, uint64) {
			return "input", timing.Input, "preflight", timing.Preflight
		}},
		"a slow State read": {preflight: slow, phase: func(timing execution.SlotTiming) (string, uint64, string, uint64) {
			return "preflight", timing.Preflight, "evaluate", timing.Evaluate
		}},
		"a slow evaluator": {evaluate: slow, phase: func(timing execution.SlotTiming) (string, uint64, string, uint64) {
			return "evaluate", timing.Evaluate, "input", timing.Input
		}},
	} {
		t.Run(name, func(t *testing.T) {
			timing := runSlowSlot(t, testCase.input, testCase.preflight, testCase.evaluate)
			delayed, delayedMillis, other, otherMillis := testCase.phase(timing)
			t.Logf("slot=%d input=%d preflight=%d evaluate=%d",
				timing.Slot, timing.Input, timing.Preflight, timing.Evaluate)

			if delayedMillis < moved {
				t.Fatalf("%s_millis = %d after %v was spent there, want at least %d: the phase did not charge its own work",
					delayed, delayedMillis, slow, moved)
			}
			if otherMillis > still {
				t.Fatalf("%s_millis = %d while the delay was in %s, want at most %d: one duration is being reported "+
					"under more than one name", other, otherMillis, delayed, still)
			}
			// The total holds the parts. They do not sum to it -- the
			// completion does more than these three -- so this is the only
			// relation between them that always holds, and it is the one that
			// catches a phase measured against a different clock.
			if sum := timing.Input + timing.Preflight + timing.Evaluate; timing.Slot < sum {
				t.Fatalf("slot_millis = %d under its own phases' %d: the parts are being measured against a clock "+
					"the total is not", timing.Slot, sum)
			}
		})
	}
}

// A Slot that did not finish reports no timing at all, rather than the part of
// one it got through.
//
// The same rule the usage beside it already follows, and for the same reason:
// a Slot that ends anywhere but its two completion exits has no reading to
// report, and a partial one would be indistinguishable on the row from a fast
// Slot. The distinction is the whole value of the number -- a Slot that spent
// four hundred milliseconds and a Slot that died before it spent anything must
// not arrive as the same row.
func TestASlotThatDidNotFinishReportsNoTiming(t *testing.T) {
	// The failing Slot is made slow on purpose. A fixture that fails in under
	// a millisecond reports zeros whether the rule holds or not, and a case
	// that cannot tell those apart proves nothing about either: the first
	// version of this test passed with the rule removed.
	result, err := executeSlowSlot(t, "query", 40*time.Millisecond, 0, 0)
	if err == nil {
		t.Fatalf("this case needs a Slot that failed: result=%+v", result)
	}
	if (result.Timing != execution.SlotTiming{}) {
		t.Fatalf("a Slot that failed at its query reported timing %+v: a partial reading on the row cannot be told "+
			"from a Slot that was quick", result.Timing)
	}
	if (result.Usage != execution.SlotBudgetUsage{}) {
		t.Fatalf("a Slot that failed at its query reported usage %+v: the timing follows this rule, so the two "+
			"must not disagree about what an unfinished Slot says", result.Usage)
	}
}
