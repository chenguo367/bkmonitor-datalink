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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot whose every commit the record refused for its cursor, returned as
// the coordinator returns it - the query-free finalization's step around
// the commit's, the store's refusal inside - reaches the fleet row through
// the executor's slot_completed line: COMMIT, CONTRACT, NONE, under the
// refusal's word, the step on the row. Two strategies stood so for a day,
// their rows UNLOCATED on every axis under "error".
func TestARefusedCommitIsLocatedOnTheFleetRowThroughTheSlotCompletedLine(t *testing.T) {
	const slot = execution.EvaluationTime(1_795_620_120)
	refused := &worker.SlotStepError{Step: execution.SlotNotCommittedQueryFreeFailed, Err: fmt.Errorf("alarmd worker: finalize query-free Slot: %w",
		&worker.SlotStepError{Step: execution.SlotNotCommittedProgressCommitFailed, Err: fmt.Errorf("alarmd worker: %w",
			&worker.ProgressRefusalError{Slot: slot, Result: execution.ProgressCommitResult{Status: execution.ProgressConflict,
				Conflict: execution.SkipRefusalCursorMoved, RecordNext: slot - 10, StoredNext: slot}})})}
	at := time.Unix(int64(slot)+600, 0)
	tracker := fleet.NewTracker(nil, "replica-1", func() time.Time { return at })
	executor := observedProductionSlotExecutor{next: slotExecutorFunc(func(context.Context, execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
		return execution.SlotExecutionResult{}, refused
	}), observer: tracker}
	request := execution.SlotExecutionRequest{Contract: execution.FrozenExecutionContractRef{
		Slot: execution.SlotIdentity{QueryGroup: "qg-stalled", EvaluationTime: slot}}}
	for round := 0; round < fleet.DefaultDegradedRounds; round++ {
		if _, err := executor.Execute(context.Background(), request); !errors.Is(err, refused) {
			t.Fatalf("round %d returned %v, want the refusal", round, err)
		}
		at = at.Add(30 * time.Second)
	}
	rows := tracker.Anomalies()
	fleet.Attribute(rows, at)
	if len(rows) != 1 || rows[0].Blocked == nil {
		t.Fatalf("rows = %+v, want the one stalled object, blocked", rows)
	}
	row, blocked := rows[0], rows[0].Blocked
	if row.FailedStep != execution.SlotNotCommittedProgressCommitFailed || row.ProgressRefusal != "CURSOR_MOVED" {
		t.Fatalf("the row carries step %q and refusal %q, want the commit's step and CURSOR_MOVED", row.FailedStep, row.ProgressRefusal)
	}
	if blocked.Code != "CURSOR_MOVED" || blocked.Stage != fleet.StageCommit || blocked.Class != fleet.ClassContract ||
		blocked.Dependency != fleet.DependencyNone {
		t.Fatalf("blocked = %+v, want CURSOR_MOVED at COMMIT / CONTRACT / NONE", blocked)
	}
}

// A Slot whose BeginSlot the record refused returns a retry carrying the
// word, no error; the executor's slot_completed line takes it from the
// result, and the fleet row names it, where it said only
// PROGRESS_BEGIN_REJECTED.
func TestARefusedBeginSlotIsNamedOnTheFleetRowThroughTheSlotCompletedLine(t *testing.T) {
	const slot = execution.EvaluationTime(1_795_620_120)
	at := time.Unix(int64(slot)+600, 0)
	tracker := fleet.NewTracker(nil, "replica-1", func() time.Time { return at })
	executor := observedProductionSlotExecutor{next: slotExecutorFunc(func(context.Context, execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
		return execution.SlotExecutionResult{Result: observability.ResultRetrying, ReasonCode: "PROGRESS_BEGIN_REJECTED",
			ProgressRefusal: "CURSOR_MOVED"}, nil
	}), observer: tracker}
	request := execution.SlotExecutionRequest{Contract: execution.FrozenExecutionContractRef{
		Slot: execution.SlotIdentity{QueryGroup: "qg-begin-refused", EvaluationTime: slot}}}
	for round := 0; round < fleet.DefaultDegradedRounds; round++ {
		if _, err := executor.Execute(context.Background(), request); err != nil {
			t.Fatalf("round %d returned %v, want the retry", round, err)
		}
		at = at.Add(30 * time.Second)
	}
	rows := tracker.Anomalies()
	fleet.Attribute(rows, at)
	if len(rows) != 1 || rows[0].Blocked == nil || rows[0].ProgressRefusal != "CURSOR_MOVED" {
		t.Fatalf("rows = %+v, want the one object, blocked, carrying the refusal", rows)
	}
	if blocked := rows[0].Blocked; blocked.Code != "CURSOR_MOVED" || blocked.Class != fleet.ClassContract || blocked.Dependency != fleet.DependencyNone {
		t.Fatalf("blocked = %+v, want CURSOR_MOVED, CONTRACT, NONE", blocked)
	}
}
