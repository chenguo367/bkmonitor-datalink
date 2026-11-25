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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// An attempt applied window by window is counted by how it ended, through
// Execute and scraped as the bundle binds it (015 O1): the window pipeline's
// phase seconds are the completed ones' alone, so the completed count is
// their denominator, and the failed ones are what the pipeline did on the
// attempts that did not complete. One applied whole is in none of them.
func TestAnAttemptAppliedWindowByWindowIsCountedByHowItEnded(t *testing.T) {
	_, _, coordinator, _ := windowedSlot(t, windowedSeries("a", "b", "d", "e", "f"))
	result, err := coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed || result.Usage.Windows < 2 {
		t.Fatalf("six series at two a window (the fixture's own and five more): result=%+v err=%v, want it completed in windows", result, err)
	}
	if got := worker.WindowedSlots(coordinator); got != (worker.WindowedSlotsReading{Completed: 1}) {
		t.Fatalf("after the windowed Slot completed: %+v, want one completed", got)
	}
	// The same attempt, its second window's State write failing.
	ports, _, failing, _ := windowedSlot(t, windowedSeries("a", "b", "d", "e", "f"))
	ports.failStateApplyAt = 2
	if _, err := failing.Execute(context.Background(), windowedRequest()); err == nil {
		t.Fatal("the attempt whose second window failed completed")
	}
	if got := worker.WindowedSlots(failing); got != (worker.WindowedSlotsReading{Failed: 1}) {
		t.Fatalf("after the windowed attempt failed: %+v, want one failed", got)
	}
	// A Slot of one series is applied whole: counted nowhere.
	_, _, whole, _ := windowedSlot(t, nil)
	if result, err := whole.Execute(context.Background(), windowedRequest()); err != nil || !result.Completed || result.Usage.Windows != 0 {
		t.Fatalf("the one-series Slot: result=%+v err=%v, want it applied whole", result, err)
	}
	if got := worker.WindowedSlots(whole); got != (worker.WindowedSlotsReading{}) {
		t.Fatalf("a Slot applied whole counted %+v, want nothing", got)
	}
	// Scraped as the bundle binds it.
	recorder := metric.NewRecorder(metric.BuildInfo{})
	if err := recorder.BindWindowedSlots(func() metric.WindowedSlotsReading {
		reading := worker.WindowedSlots(coordinator)
		return metric.WindowedSlotsReading{Completed: reading.Completed, Failed: reading.Failed, Retried: reading.Retried}
	}); err != nil {
		t.Fatal(err)
	}
	families, err := recorder.ScrapeGatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	scraped := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "alarmd_slot_windowed_total" {
			continue
		}
		for _, sample := range family.GetMetric() {
			scraped[sample.GetLabel()[0].GetValue()] = sample.GetCounter().GetValue()
		}
	}
	if len(scraped) != 3 || scraped["completed"] != 1 || scraped["failed"] != 0 || scraped["retried"] != 0 {
		t.Fatalf("scraped slot_windowed_total %v, want the three outcomes with the one completed", scraped)
	}
}

// An attempt that fails after it handed its first window over is counted
// failed (015 O1). The window it handed lands only when the apply is joined,
// after the attempt is counted: counted by the windows landed, it read as
// one applied whole and was counted nowhere - the shape of a sliced group
// whose second slice's read times out. Six series at two a window, a b | c d
// | e f: the first window is handed over at c, its State write held until
// the attempt has said its failure, and the attempt fails evaluating e,
// before the second is cut - so the first cannot land before the attempt is
// counted, as the last window handed before a failed read does not. The
// evaluation fails as failingEvaluator fails one series.
func TestAnAttemptFailingAfterItHandedItsFirstWindowIsCountedFailed(t *testing.T) {
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	persisted := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	gate := &heldState{persistedRetryState: persisted, released: make(chan struct{}), expired: testwait.Expired(t)}
	var releasedOnFailure atomic.Bool
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: &failingEvaluator{recordingPorts: ports, failing: strings.Repeat("e", 64)},
		Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness, Events: ports,
		State: gate, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			if observation.QueryFailure != nil || observation.Err != nil {
				releasedOnFailure.Store(true)
				gate.release.Do(func() { close(gate.released) })
			}
		})},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 4, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(context.Background(), windowedRequest()); err == nil {
		t.Fatal("the attempt whose evaluation of e failed completed")
	}
	if !gate.returned.Load() || !releasedOnFailure.Load() {
		t.Fatalf("first window's State write returned %v, released on the failure %v: want the window handed over, "+
			"held until the attempt said its failure", gate.returned.Load(), releasedOnFailure.Load())
	}
	if got := worker.WindowedSlots(coordinator); got != (worker.WindowedSlotsReading{Failed: 1}) {
		t.Fatalf("after the attempt that handed its first window and then failed: %+v, want one failed", got)
	}
}
