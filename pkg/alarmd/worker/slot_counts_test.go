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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slotCounts is a SlotCounter that keeps every word it was told, in order.
type slotCounts struct{ begins, notCommitted []string }

func (counts *slotCounts) RecordSlotBegin(result string) {
	counts.begins = append(counts.begins, result)
}

func (counts *slotCounts) RecordSlotNotCommitted(reason string) {
	counts.notCommitted = append(counts.notCommitted, reason)
}

// newCountingFixture is newFixture with the Slot counter wired, as the
// bundle wires the metric recorder.
func newCountingFixture(t *testing.T) (fixture, *slotCounts) {
	t.Helper()
	trace := make([]string, 0, len(fullTrace))
	observations := make([]observability.Observation, 0, len(fullTrace)+1)
	observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		observations = append(observations, observability.NormalizeObservation(observation))
	})
	ports := &recordingPorts{trace: &trace, ready: true}
	counts := &slotCounts{}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports,
		Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: ports, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: ports, State: ports, Progress: ports,
		Observer: observer, SlotCounts: counts,
	}, worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 100, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatalf("NewSlotExecutionCoordinator() error: %v", err)
	}
	return fixture{trace: &trace, observations: &observations, ports: ports, coordinator: coordinator}, counts
}

// Every BeginSlot that returned a status is counted under it, and an
// attempt that began its Slot and did not write it down is counted once, by
// the exit it took: a readiness deferral is QUERY_NOT_READY, a finalization
// that asks for the attempt again is its mode, and every other return -
// a failed query, a Progress commit that conflicted - is other. A Slot that
// committed, or one that never began, is not counted as not committed.
func TestSlotBeginsAndBegunSlotsNotCommittedAreCountedByExit(t *testing.T) {
	readiness := func(ports *recordingPorts) {
		ports.executeOverride = func(context.Context, execution.QueryExecutionRequest, execution.QueryExecutionConsumer) (execution.QueryExecutionCompletion, error) {
			return execution.QueryExecutionCompletion{}, fmt.Errorf("wrapped readiness: %w", coordinatorReadinessDeferredError{readyAt: time.Unix(1_800_000_030, 0)})
		}
	}
	for _, exit := range []struct {
		name         string
		mutate       func(*recordingPorts)
		begin        string
		notCommitted []string
	}{
		{name: "committed", mutate: func(*recordingPorts) {}, begin: "committed"},
		{name: "readiness deferral", mutate: readiness, begin: "committed", notCommitted: []string{execution.SlotNotCommittedQueryNotReady}},
		{name: "snapshot retry", mutate: func(ports *recordingPorts) { ports.finalizationMode = execution.FinalizationSnapshotRetry },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedSnapshotRetry}},
		{name: "exact set blocked", mutate: func(ports *recordingPorts) { ports.finalizationMode = execution.FinalizationExactSetBlocked },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedExactSetBlocked}},
		{name: "query failed", mutate: func(ports *recordingPorts) { ports.failStage = "query" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedOther}},
		{name: "commit conflicted", mutate: func(ports *recordingPorts) { ports.progressConflict = true },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedOther}},
		{name: "begin conflicted", mutate: func(ports *recordingPorts) { ports.beginStatus = execution.ProgressConflict }, begin: "conflict"},
		{name: "begin stale owner", mutate: func(ports *recordingPorts) { ports.beginStatus = execution.ProgressStaleOwner }, begin: "stale_owner"},
		{name: "begin retryable", mutate: func(ports *recordingPorts) { ports.beginStatus = execution.ProgressRetryableIO }, begin: "retryable_io"},
		{name: "begin content moved", mutate: func(ports *recordingPorts) { ports.beginStatus = execution.ProgressContentMoved }, begin: "content_moved"},
	} {
		fixture, counts := newCountingFixture(t)
		exit.mutate(fixture.ports)
		result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
		if exit.name == "committed" && (err != nil || !result.Completed) {
			t.Fatalf("%s: result=%+v err=%v, want the Slot completed", exit.name, result, err)
		}
		if !slices.Equal(counts.begins, []string{exit.begin}) {
			t.Errorf("%s: begins counted %v, want [%s]", exit.name, counts.begins, exit.begin)
		}
		if !slices.Equal(counts.notCommitted, exit.notCommitted) {
			t.Errorf("%s: not committed counted %v, want %v (result=%+v err=%v)", exit.name, counts.notCommitted, exit.notCommitted, result, err)
		}
		if !slices.Contains(execution.SlotBeginResults, exit.begin) {
			t.Errorf("%s: %s is not a word of SlotBeginResults %v", exit.name, exit.begin, execution.SlotBeginResults)
		}
	}
}
