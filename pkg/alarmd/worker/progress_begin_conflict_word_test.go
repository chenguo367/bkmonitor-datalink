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
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// refusingBegins is the fixture's Progress store with every BeginSlot
// refused as refused says, begun at the attempt's start.
type refusingBegins struct {
	execution.ProgressStore
	refused execution.ProgressBeginResult
}

func (store refusingBegins) BeginSlot(context.Context, execution.ProgressBeginRequest) (execution.ProgressBeginResult, error) {
	return store.refused, nil
}

// deferredRefusingBegins is refusingBegins put off to the attempt's first
// write, as the production store puts it off (014 ⑩ F5).
type deferredRefusingBegins struct{ refusingBegins }

func (deferredRefusingBegins) BeginDeferrable(execution.ProgressBeginRequest) bool { return true }

// An attempt whose BeginSlot the record refused as a conflict returns a
// retry, no error, under PROGRESS_BEGIN_REJECTED as before; its progress line
// now names the fact that refused it - for a cursor not at the Slot, the
// Slot the record's completed facts derive and the cursor it stores - and
// the retry carries the word to the slot_completed line. A conflict its path
// returned without a word is UNNAMED there, not lost. The same whether the
// BeginSlot was made at the attempt's start or put off to its first write.
func TestAnAttemptWhoseBeginSlotTheRecordRefusedSaysWhy(t *testing.T) {
	header, batches := workerG4StreamFixture(t, strategy.DetectorKindSimpleRingRatio)
	for _, test := range []struct {
		name     string
		refused  execution.ProgressBeginResult
		word     string
		lineWord observability.ReasonCode
		text     string
	}{
		{name: "cursor moved", refused: execution.ProgressBeginResult{Status: execution.ProgressConflict,
			Conflict: execution.SkipRefusalCursorMoved, RecordNext: 1_000, StoredNext: 1_060}, word: "CURSOR_MOVED",
			lineWord: observability.CursorRefusalCursorMoved,
			text:     "progress not begun: CONFLICT: CURSOR_MOVED: the record's completed facts derive 1000 as next, it stores 1060, the Slot asked is"},
		{name: "unnamed", refused: execution.ProgressBeginResult{Status: execution.ProgressConflict}, word: "UNNAMED",
			lineWord: observability.ReasonCode(contract.ReasonProgressBeginRejected), text: "progress not begun: CONFLICT: UNNAMED"},
	} {
		for _, deferred := range []bool{false, true} {
			name := test.name + "/at the start"
			if deferred {
				name = test.name + "/put off"
			}
			t.Run(name, func(t *testing.T) {
				var mu sync.Mutex
				var lines []observability.Observation
				observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					if observation.Component == observability.ComponentProgress && observation.Stage == observability.StageProgressCommitted &&
						observation.Err != nil {
						mu.Lock()
						lines = append(lines, observation)
						mu.Unlock()
					}
				})
				ports, _, coordinator := workerG4CoordinatorWith(t, observer, newMemoryEvidenceStore(), func(wired *worker.Ports) {
					refusing := refusingBegins{ProgressStore: wired.Progress, refused: test.refused}
					if deferred {
						wired.Progress = deferredRefusingBegins{refusing}
					} else {
						wired.Progress = refusing
					}
				})
				ports.executeOverride = streamExecution(header, batches, fullCompletion(t, header, batches))
				result, err := coordinator.Execute(context.Background(), liveSlotRequest(header.Contract))
				if err != nil {
					t.Fatalf("an attempt whose BeginSlot was refused returned %v, want a retry", err)
				}
				if result.Result != observability.ResultRetrying || result.ReasonCode != execution.ReasonCode(contract.ReasonProgressBeginRejected) ||
					result.ProgressRefusal != test.word {
					t.Fatalf("result = %s %s refusal %q, want a retry under PROGRESS_BEGIN_REJECTED refused by %s", result.Result,
						result.ReasonCode, result.ProgressRefusal, test.word)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(lines) != 1 || lines[0].ReasonCode != test.lineWord || !strings.Contains(lines[0].Err.Error(), test.text) {
					t.Fatalf("progress lines = %+v, want one under %s saying %q", lines, test.lineWord, test.text)
				}
			})
		}
	}
}
