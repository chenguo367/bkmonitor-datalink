// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"errors"
	"testing"
	"time"

	model "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A round that returned an error no code read is located by the step of
// Execute it failed in, and a Progress commit the store refused by what
// refused it: a commit the record refused for its cursor is the commit's,
// this deployment's own state in conflict with itself, nothing it depends
// on failing. The row read UNLOCATED on every axis under the executor's
// catch-all word, for an object whose every commit the record refused.
func TestAFailedRoundIsLocatedByTheStepItFailedIn(t *testing.T) {
	for _, test := range []struct {
		name, step, refusal, text string
		code                      string
		stage                     Stage
		class                     Class
		dependency                Dependency
	}{
		{name: "a commit refused for its cursor", step: model.SlotNotCommittedProgressCommitFailed, refusal: "CURSOR_MOVED",
			text: "alarmd worker: finalize query-free Slot: alarmd worker: progress not committed: CONFLICT: CURSOR_MOVED: the " +
				"record's completed facts derive 110 as next, it stores 120, the commit is of 120",
			code: "CURSOR_MOVED", stage: StageCommit, class: ClassContract, dependency: DependencyNone},
		{name: "a commit refused for a write under it", step: model.SlotNotCommittedProgressCommitFailed, refusal: "CAS_CONFLICT",
			text: "alarmd worker: progress not committed: CONFLICT: CAS_CONFLICT",
			code: "CAS_CONFLICT", stage: StageCommit, class: ClassContract, dependency: DependencyNone},
		{name: "a commit the store did not answer", step: model.SlotNotCommittedProgressCommitFailed, refusal: "RETRYABLE_IO",
			text: "alarmd worker: progress not committed: RETRYABLE_IO",
			code: "RETRYABLE_IO", stage: StageCommit, class: ClassUnavailable, dependency: DependencyRedis},
		{name: "a step alone says the stage", step: model.SlotNotCommittedCompleteFailed, text: "alarmd worker: complete Slot: boom",
			code: model.SlotNotCommittedCompleteFailed, stage: StageEvaluate, class: ClassUnlocated, dependency: DependencyUnlocated},
		{name: "no step says nothing", text: "boom", code: "error", stage: StageUnlocated, class: ClassUnlocated,
			dependency: DependencyUnlocated},
	} {
		t.Run(test.name, func(t *testing.T) {
			at := &clock{at: now}
			tracker := newTracker(t, at)
			for round := 0; round < DefaultDegradedRounds; round++ {
				tracker.Observe(context.Background(), observability.Observation{
					ExecuteOutcome: "error", SlotStep: test.step, ProgressRefusal: test.refusal, Err: errors.New(test.text),
					Trace: observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: 1_795_620_120},
				})
				at.at = at.at.Add(30 * time.Second)
			}
			rows := tracker.Anomalies()
			Attribute(rows, at.at)
			if len(rows) != 1 || rows[0].Blocked == nil {
				t.Fatalf("rows = %+v, want one blocked row", rows)
			}
			blocked := rows[0].Blocked
			if blocked.Code != test.code || blocked.Stage != test.stage || blocked.Class != test.class || blocked.Dependency != test.dependency {
				t.Fatalf("blocked = %+v, want code %s at %s / %s / %s", blocked, test.code, test.stage, test.class, test.dependency)
			}
			if rows[0].FailedStep != test.step || rows[0].ProgressRefusal != test.refusal {
				t.Fatalf("the row carries step %q and refusal %q, want %q and %q", rows[0].FailedStep, rows[0].ProgressRefusal,
					test.step, test.refusal)
			}
		})
	}
}

// A round whose BeginSlot the record refused as a conflict returns a retry,
// no error and so no step, under PROGRESS_BEGIN_REJECTED; the word its result
// carries names the fact on the row, as a refused commit's does. The row
// said only PROGRESS_BEGIN_REJECTED, the same for every refusal.
func TestARoundWhoseBeginSlotWasRefusedIsNamedOnItsRow(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	for round := 0; round < DefaultDegradedRounds; round++ {
		tracker.Observe(context.Background(), observability.Observation{
			ExecuteOutcome: "retrying", Result: observability.ResultRetrying, ReasonCode: "PROGRESS_BEGIN_REJECTED",
			ProgressRefusal: "CURSOR_MOVED",
			Trace:           observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: 1_795_620_120},
		})
		at.at = at.at.Add(30 * time.Second)
	}
	rows := tracker.Anomalies()
	Attribute(rows, at.at)
	if len(rows) != 1 || rows[0].Blocked == nil {
		t.Fatalf("rows = %+v, want one blocked row", rows)
	}
	blocked := rows[0].Blocked
	if blocked.Code != "CURSOR_MOVED" || blocked.Class != ClassContract || blocked.Dependency != DependencyNone || blocked.Stage == StageUnlocated {
		t.Fatalf("blocked = %+v, want CURSOR_MOVED, CONTRACT, NONE at a located stage", blocked)
	}
}
