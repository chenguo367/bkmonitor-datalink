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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// refusingCommits is the fixture's Progress store with every commit refused
// as refused says.
type refusingCommits struct {
	execution.ProgressStore
	refused execution.ProgressCommitResult
}

func (store refusingCommits) CommitProgress(context.Context, execution.ProgressCommitRequest) (execution.ProgressCommitResult, error) {
	return store.refused, nil
}

// An attempt whose commit the record refused returns, through Execute, the
// step it failed in and what refused the commit - what its slot_completed
// line carries for the fleet to read the row by - and its error names the
// fact; for a cursor not at the Slot, the Slot the record's completed facts
// derive and the cursor it stores, the two numbers a reader needs to tell
// which of them moved.
func TestAnAttemptWhoseCommitTheRecordRefusedSaysWhereAndWhy(t *testing.T) {
	header, batches := workerG4StreamFixture(t, strategy.DetectorKindSimpleRingRatio)
	for _, test := range []struct {
		name    string
		refused execution.ProgressCommitResult
		word    string
		text    string
	}{
		{name: "cursor moved", refused: execution.ProgressCommitResult{Status: execution.ProgressConflict,
			Conflict: execution.SkipRefusalCursorMoved, RecordNext: 1_000, StoredNext: 1_060}, word: "CURSOR_MOVED",
			text: "CONFLICT: CURSOR_MOVED: the record's completed facts derive 1000 as next, it stores 1060, the commit is of"},
		{name: "a write under it", refused: execution.ProgressCommitResult{Status: execution.ProgressConflict,
			Conflict: execution.SkipRefusalCASConflict}, word: "CAS_CONFLICT", text: "progress not committed: CONFLICT: CAS_CONFLICT"},
		{name: "no answer", refused: execution.ProgressCommitResult{Status: execution.ProgressRetryableIO,
			ReasonCode: execution.ReasonCode(contract.ReasonRedisUnavailable)}, word: "RETRYABLE_IO", text: "progress not committed: RETRYABLE_IO"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ports, _, coordinator := workerG4CoordinatorWith(t, observability.ObserverFunc(func(context.Context, observability.Observation) {}),
				newMemoryEvidenceStore(), func(wired *worker.Ports) {
					wired.Progress = refusingCommits{ProgressStore: wired.Progress, refused: test.refused}
				})
			ports.executeOverride = streamExecution(header, batches, fullCompletion(t, header, batches))
			_, err := coordinator.Execute(context.Background(), liveSlotRequest(header.Contract))
			if err == nil {
				t.Fatal("an attempt whose commit was refused completed")
			}
			if step := worker.SlotStepOf(err); step != execution.SlotNotCommittedProgressCommitFailed {
				t.Fatalf("the step is %q, want %q (%v)", step, execution.SlotNotCommittedProgressCommitFailed, err)
			}
			if word, refused := worker.ProgressRefusalOf(err); !refused || word != test.word {
				t.Fatalf("the refusal is (%q, %v), want %q (%v)", word, refused, test.word, err)
			}
			if !strings.Contains(err.Error(), test.text) {
				t.Fatalf("the error says %q, want it to say %q", err, test.text)
			}
		})
	}
}
