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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slotCensus is one attempt's census as its one observation reports it.
type slotCensus struct {
	reports    int
	admitted   int64
	written    int64
	notWritten map[observability.StateNotWrittenReason]int64
}

func censusOf(t *testing.T, observations []observability.Observation) slotCensus {
	t.Helper()
	census := slotCensus{notWritten: map[observability.StateNotWrittenReason]int64{}}
	for _, observation := range observations {
		facts := observation.FrozenStateRenewal
		if observation.Stage != observability.StageFrozenStateRenewed || facts == nil {
			continue
		}
		census.reports++
		census.admitted += facts.Admitted()
		census.written += int64(facts.Written)
		for reason, count := range facts.NotWritten {
			census.notWritten[reason] += count
		}
	}
	return census
}

// assertSlotCensus holds the Slot's reading - the classes sum to what it
// wrote plus what it did not - and that the attempt reported it once, with
// the numbers this case built.
func assertSlotCensus(t *testing.T, census slotCensus, admitted, written int64, notWritten map[observability.StateNotWrittenReason]int64) {
	t.Helper()
	if census.reports != 1 {
		t.Fatalf("the attempt reported its census %d times, want once whichever way it ended", census.reports)
	}
	total := int64(0)
	for _, count := range census.notWritten {
		total += count
	}
	if census.admitted != census.written+total {
		t.Fatalf("classes %d != written %d + not written %d (%v): the Slot's reading does not reconcile",
			census.admitted, census.written, total, census.notWritten)
	}
	if census.admitted != admitted || census.written != written {
		t.Fatalf("admitted %d written %d, want %d and %d", census.admitted, census.written, admitted, written)
	}
	for _, reason := range observability.AllStateNotWrittenReasons() {
		if census.notWritten[reason] != notWritten[reason] {
			t.Fatalf("not written %v, want %v", census.notWritten, notWritten)
		}
	}
}

// A Slot whose writes all land: every class it admitted is a key it wrote.
func TestAWrittenSlotsClassesAreWhatItWrote(t *testing.T) {
	fixture := newFixture(t, true, "")
	fixture.ports.reverseStateReceipts = true
	if _, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 2, nil)
}

// An output the sink did not acknowledge holds back the Plan's State: the
// two series it admitted are output_withheld, and nothing is written.
func TestAnUnacknowledgedOutputCountsItsSeriesAsWithheld(t *testing.T) {
	fixture := newFixture(t, true, "event_ack")
	fixture.ports.reverseStateReceipts = true
	_, _ = fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 0,
		map[observability.StateNotWrittenReason]int64{observability.StateNotWrittenOutputWithheld: 2})
}

// The State admission refuses one of two series by name: that one is
// state_admission_rejected, the other is written.
func TestAnAdmissionRefusalCountsTheRefusedSeriesAlone(t *testing.T) {
	fixture := newFixture(t, true, "")
	fixture.ports.reverseStateReceipts = true
	fixture.ports.stateAdmissionDeterministic = true
	if _, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 1,
		map[observability.StateNotWrittenReason]int64{observability.StateNotWrittenAdmissionRejected: 1})
}

// The store fails the write: the attempt fails, and still reports its census
// at the Slot's end, the two series it sent as state_apply_failed.
func TestAFailedStateWriteIsCountedOnTheAttemptThatFailed(t *testing.T) {
	fixture := newFixture(t, true, "state_apply")
	fixture.ports.reverseStateReceipts = true
	if _, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err == nil {
		t.Fatal("the failed State write did not fail the attempt")
	}
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 0,
		map[observability.StateNotWrittenReason]int64{observability.StateNotWrittenApplyFailed: 2})
}

// The admission itself fails, by no exit the write path names: what the
// attempt admitted is slot_failed.
func TestAnAttemptThatFailsBeforeItsWritesCountsWhatItAdmittedAsSlotFailed(t *testing.T) {
	fixture := newFixture(t, true, "state_admission")
	fixture.ports.reverseStateReceipts = true
	if _, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err == nil {
		t.Fatal("the failed State admission did not fail the attempt")
	}
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 0,
		map[observability.StateNotWrittenReason]int64{observability.StateNotWrittenSlotFailed: 2})
}

// The attempt fails after its State landed, on the gap write that follows
// it: its census still counts the two keys it wrote. Before, a failed attempt
// reported no census at all, and the writes it had made went uncounted.
func TestAnAttemptThatFailsAfterItsWritesStillCountsThemWritten(t *testing.T) {
	fixture := newFixture(t, true, "gap_after")
	fixture.ports.reverseStateReceipts = true
	if _, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err == nil {
		t.Fatal("the failed gap write did not fail the attempt")
	}
	assertSlotCensus(t, censusOf(t, *fixture.observations), 2, 2, nil)
}

// Written window by window, two series a window: the first window lands, the
// second window's State write fails, and the third is never admitted. The
// attempt's one census says so.
func TestAWindowedSlotsCensusCountsEachWindowAsItLeft(t *testing.T) {
	trace := make([]string, 0)
	observations := make([]observability.Observation, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	state := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: ports, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
		Hosts: worker.SharedHostBusiness, Events: ports, State: state, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			observations = append(observations, observability.NormalizeObservation(observation))
		})},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 2, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	ports.failStateApplyAt = 2
	if _, err := coordinator.Execute(context.Background(), windowedRequest()); err == nil {
		t.Fatal("the second window's failed State write did not fail the attempt")
	}
	if len(state.saved) != 2 {
		t.Fatalf("the attempt wrote %d series, want the first window's two", len(state.saved))
	}
	assertSlotCensus(t, censusOf(t, observations), 4, 2,
		map[observability.StateNotWrittenReason]int64{observability.StateNotWrittenApplyFailed: 2})
}
