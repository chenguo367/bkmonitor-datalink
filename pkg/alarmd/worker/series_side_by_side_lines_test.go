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
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// evaluationLines keeps the evaluation lines a Slot says, in the order said,
// each with its duration taken out: the one field that differs from run to
// run.
type evaluationLines struct {
	mu    sync.Mutex
	lines []observability.Observation
}

func (lines *evaluationLines) Observe(_ context.Context, observation observability.Observation) {
	if observation.Stage != observability.StageEvaluationCompleted {
		return
	}
	observation.Duration = 0
	lines.mu.Lock()
	defer lines.mu.Unlock()
	lines.lines = append(lines.lines, observation)
}

// failingEvaluator is the recording evaluator, but for one series, whose
// evaluation fails.
type failingEvaluator struct {
	*recordingPorts
	failing string
}

func (evaluator *failingEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	if len(request.State.Items) > 0 && string(request.State.Items[0].Identity.SeriesIdentityDigest) == evaluator.failing {
		return execution.EvaluationResult{}, errors.New("the series' evaluation fails")
	}
	return evaluator.recordingPorts.Evaluate(ctx, request)
}

// sideBySideLines runs one Slot of series with procs cores, the series
// failing given its evaluation failing, and returns the evaluation lines it
// said and whether it completed.
func sideBySideLines(t *testing.T, procs int, series []string, failing string) ([]observability.Observation, bool) {
	t.Helper()
	previous := runtime.GOMAXPROCS(procs)
	defer runtime.GOMAXPROCS(previous)
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: series}
	saved := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	lines := &evaluationLines{}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: &failingEvaluator{recordingPorts: ports, failing: failing}, Admission: ports,
		GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness, Events: ports,
		State: &readCountingState{persistedRetryState: saved, evaluated: func() int64 { return 0 }}, Progress: ports,
		ExecutionEvidence: newMemoryEvidenceStore(), Observer: lines},
		worker.ProvisionalBudget{MaxSeries: 1000, MaxRetainedBytes: 64 << 20, MaxStateMutations: 1000, MaxEvents: 1000, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := coordinator.Execute(context.Background(), windowedRequest())
	lines.mu.Lock()
	defer lines.mu.Unlock()
	return append([]observability.Observation(nil), lines.lines...), result.Completed
}

// The evaluation lines a batch evaluated side by side says - made beside the
// evaluations from each job alone (014 next9 3, (ii-a)) - are those the same
// Slot says on one core, line for line and in their order; and the first
// series that fails ends the batch as before, no line said of a series after
// it, though the lines of those evaluated beside it were made.
func TestABatchSaysTheEvaluationLinesItSaysOnOneCoreAndNoneAfterAFailure(t *testing.T) {
	series := make([]string, 0, 99)
	for index := range 99 {
		series = append(series, fmt.Sprintf("%064x", index+1))
	}
	one, completed := sideBySideLines(t, 1, series, "")
	if !completed || len(one) == 0 {
		t.Fatalf("one core: completed %t with %d evaluation lines", completed, len(one))
	}
	four, completed := sideBySideLines(t, 4, series, "")
	if !completed || !reflect.DeepEqual(four, one) {
		t.Fatalf("four cores said %d evaluation lines (completed %t), one core %d: want them line for line", len(four), completed, len(one))
	}

	const failing = 40
	after := map[string]bool{}
	for _, name := range series[failing+1:] {
		after[name] = true
	}
	oneFailed, _ := sideBySideLines(t, 1, series, series[failing])
	fourFailed, completed := sideBySideLines(t, 4, series, series[failing])
	if completed {
		t.Fatal("the Slot whose series failed completed")
	}
	for _, line := range fourFailed {
		if after[line.Trace.DimensionIdentityDigest] {
			t.Fatalf("a line was said of the series %s, after the one that failed", line.Trace.DimensionIdentityDigest)
		}
	}
	if !reflect.DeepEqual(fourFailed, oneFailed) {
		t.Fatalf("with a series failing, four cores said %d lines and one core %d: want them line for line", len(fourFailed),
			len(oneFailed))
	}
}
