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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// permitDeadlineTestError is a physical query permit's wait that reached the
// query's deadline, as access names it.
type permitDeadlineTestError struct{}

func (permitDeadlineTestError) Error() string {
	return "acquire physical query permit: context deadline exceeded"
}
func (permitDeadlineTestError) Unwrap() error { return context.DeadlineExceeded }
func (permitDeadlineTestError) QueryFailure() (string, string) {
	return observability.QueryFailureCategoryAdmission, "QUERY_PERMIT_DEADLINE"
}

// shareRefusedTestError is a Slot refused for its share, deterministically.
type shareRefusedTestError struct{}

func (shareRefusedTestError) Error() string { return "over its share" }
func (shareRefusedTestError) DeterministicShareRefusal() (execution.ShareRefusal, bool) {
	return execution.ShareRefusal{LimitBytes: 1}, true
}

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
// the exit it took (execution.SlotNotCommittedReasons): a readiness deferral
// is QUERY_NOT_READY, a finalization that asks for the attempt again is its
// mode; an error by what it is - a fence lost, a cancellation or a deadline
// (while the finalization is read too: a rollout stops Slots there), the
// finalization, the watchdog, a share, a State conflict - and otherwise by the step it
// failed in, a Progress commit inside the Slot's apply by the commit; a
// result by its reason and kind; a panic as no result. A Slot that committed,
// or one that never began, is not counted as not committed.
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
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedRetryPending}},
		{name: "query failed", mutate: func(ports *recordingPorts) { ports.failStage = "query" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedQueryFailed}},
		{name: "commit conflicted", mutate: func(ports *recordingPorts) { ports.progressConflict = true },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedProgressCommitFailed}},
		{name: "commit failed", mutate: func(ports *recordingPorts) { ports.failStage = "progress_commit" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedProgressCommitFailed}},
		{name: "commit fence lost", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "progress_commit", fmt.Errorf("commit: %w", ownership.ErrStaleFence)
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedOwnerRefused}},
		{name: "state apply failed", mutate: func(ports *recordingPorts) { ports.failStage = "state_apply" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedApplyFailed}},
		{name: "state apply conflicted", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "state_apply", &worker.StateConflictError{Status: string(execution.StateVersionConflict)}
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedStoreConflict}},
		{name: "evaluation failed", mutate: func(ports *recordingPorts) { ports.failStage = "evaluate" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedCompleteFailed}},
		{name: "output ack unknown", mutate: func(ports *recordingPorts) { ports.failStage = "event_ack" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedOutputRetryPending}},
		{name: "output failed", mutate: func(ports *recordingPorts) { ports.failStage, ports.eventAckPlain = "event_ack", true },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedOutputFailed}},
		{name: "cancelled", mutate: func(ports *recordingPorts) { ports.failStage, ports.failErr = "query", context.Canceled },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedCancelled}},
		{name: "permit deadline", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "query", fmt.Errorf("alarmd access: acquire physical query permit: %w", permitDeadlineTestError{})
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedQueryPermitFailed}},
		{name: "deadline", mutate: func(ports *recordingPorts) { ports.failStage, ports.failErr = "query", context.DeadlineExceeded },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedDeadlineExceeded}},
		{name: "declined", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "query", fmt.Errorf("enter commit: %w", execution.ErrExecutionDeclined)
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedDeclined}},
		{name: "share refused", mutate: func(ports *recordingPorts) { ports.failStage, ports.failErr = "query", shareRefusedTestError{} },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedCapacityRefused}},
		{name: "finalization invalid", mutate: func(ports *recordingPorts) { ports.finalizationMode = "NOT_A_MODE" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedFinalizationFailed}},
		{name: "finalization unread", mutate: func(ports *recordingPorts) { ports.failStage = "finalization" },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedFinalizationFailed}},
		{name: "cancelled while finalizing", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "finalization", fmt.Errorf("read the frozen Plan: %w", context.Canceled)
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedCancelled}},
		{name: "deadline while finalizing", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "finalization", fmt.Errorf("read the frozen Plan: %w", context.DeadlineExceeded)
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedDeadlineExceeded}},
		{name: "activation unreadable", mutate: func(ports *recordingPorts) { ports.activationErrorAt = 1 },
			begin: "committed", notCommitted: []string{execution.SlotNotCommittedActivationReadFailed}},
		{name: "activation moved", mutate: func(ports *recordingPorts) {
			ports.activationChangeAt, ports.activationSelection, ports.persistActivatedGaps = 2, execution.ActivationPending, true
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedActivationChanged}},
		{name: "plain error from a step not named", mutate: func(ports *recordingPorts) {
			ports.failStage, ports.failErr = "query", errors.New("plain")
		}, begin: "committed", notCommitted: []string{execution.SlotNotCommittedQueryFailed}},
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

// An attempt that began its Slot and panicked returned nothing at all: it is
// counted as no_result, the one exit that leaves no result, and the panic
// goes on up as it was.
func TestABegunSlotThatPanicsIsCountedAsNoResult(t *testing.T) {
	fixture, counts := newCountingFixture(t)
	fixture.ports.executeOverride = func(context.Context, execution.QueryExecutionRequest, execution.QueryExecutionConsumer) (execution.QueryExecutionCompletion, error) {
		panic("query port panicked")
	}
	recovered := func() (value any) {
		defer func() { value = recover() }()
		_, _ = fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
		return nil
	}()
	if recovered != "query port panicked" {
		t.Fatalf("recovered %v, want the port's panic passed on", recovered)
	}
	if !slices.Equal(counts.notCommitted, []string{execution.SlotNotCommittedNoResult}) {
		t.Fatalf("not committed counted %v, want [%s]", counts.notCommitted, execution.SlotNotCommittedNoResult)
	}
}
