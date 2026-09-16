// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// answeringNoDataStore answers every write with one chosen status.
type answeringNoDataStore struct{ status execution.NoDataApplyStatus }

func (store *answeringNoDataStore) LoadNoData(
	_ context.Context, request execution.NoDataLoadRequest,
) (execution.NoDataLoadResult, error) {
	result := execution.NoDataLoadResult{Items: make([]execution.NoDataMemorySnapshot, len(request.Items))}
	for index, item := range request.Items {
		result.Items[index] = execution.NoDataMemorySnapshot{
			Identity: item.Identity, Status: execution.NoDataMemoryMissing,
		}
	}
	return result, nil
}

func (store *answeringNoDataStore) ApplyNoData(
	_ context.Context, request execution.NoDataApplyRequest,
) (execution.NoDataApplyResult, error) {
	result := execution.NoDataApplyResult{Items: make([]execution.NoDataApplyItemResult, len(request.Items))}
	for index, mutation := range request.Items {
		result.Items[index] = execution.NoDataApplyItemResult{Identity: mutation.Identity, Status: store.status}
	}
	return result, nil
}

// Every outcome the store can return is reported, the ones that worked
// included, and each mutation lands on exactly one of the two stages.
//
// The reading this exists for is "is this Plan's memory being kept", and it is
// a question about now. With only the failures reported it has to be answered
// from an absence of them, which reads the same whether the Plan recovered,
// stopped being evaluated, or started losing races instead of being refused.
func TestEveryNoDataMemoryWriteOutcomeIsReportedOnExactlyOneStage(t *testing.T) {
	stored := map[execution.NoDataApplyStatus]bool{
		execution.NoDataApplied:        true,
		execution.NoDataAlreadyApplied: true,
	}
	for _, status := range execution.NoDataApplyStatuses {
		t.Run(string(status), func(t *testing.T) {
			observed := make([]observability.Observation, 0, 2)
			coordinator := &SlotExecutionCoordinator{ports: Ports{
				NoData: &answeringNoDataStore{status: status}, Hosts: SharedHostBusiness,
				Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					observed = append(observed, observability.NormalizeObservation(observation))
				}),
			}}
			if err := coordinator.applyNoDataMemory(context.Background(), execution.SlotExecutionRequest{
				Operation: execution.OperationNormal,
			}, []execution.PlanNoDataMutation{refusedMemoryMutation(t)}); err != nil {
				t.Fatalf("applyNoDataMemory() error: %v", err)
			}

			var writes, refusals int
			for _, observation := range observed {
				switch observation.Stage {
				case observability.StageNoDataMemoryWritten:
					writes++
					facts := observation.NoDataMemoryWrite
					if facts == nil {
						t.Fatalf("write line carried no facts: %+v", observation)
					}
					if facts.Outcome != string(status) {
						t.Fatalf("outcome = %q, want the store's own word %q", facts.Outcome, status)
					}
					// Taken from a table written here rather than from the
					// function under test: asking NoDataWriteStored what it
					// thinks and then checking the line agrees with it proves
					// only that one call site reads one function.
					if facts.Stored != stored[status] {
						t.Fatalf("stored = %v for %s, want %v", facts.Stored, status, stored[status])
					}
					if observation.Trace.StrategyID != "8946" {
						t.Fatalf("write line strategy = %q", observation.Trace.StrategyID)
					}
				case observability.StageNoDataMemoryRefused:
					refusals++
				}
			}
			wantWrites, wantRefusals := 1, 0
			if status == execution.NoDataRejected {
				wantWrites, wantRefusals = 0, 1
			}
			if writes != wantWrites || refusals != wantRefusals {
				t.Fatalf("%s produced %d write lines and %d refusal lines, want %d and %d: "+
					"every mutation lands on exactly one of the two, or the two families do not add up "+
					"to what the store was asked for",
					status, writes, refusals, wantWrites, wantRefusals)
			}
		})
	}
}

// A write that lost a race is not reported as success.
//
// STALE_VERSION is the outcome that reads like one and is not: the store
// answered, nothing failed, and what this round learned was dropped because a
// newer record won. A page that counted it as stored would show a Plan whose
// memory is being kept while its absence clocks stand still.
func TestAWriteThatLostToANewerRecordIsNotReportedAsStored(t *testing.T) {
	for status, wantStored := range map[execution.NoDataApplyStatus]bool{
		execution.NoDataApplied:        true,
		execution.NoDataAlreadyApplied: true,
		execution.NoDataStale:          false,
		execution.NoDataConflict:       false,
		execution.NoDataRetryable:      false,
	} {
		if got := execution.NoDataWriteStored(status); got != wantStored {
			t.Fatalf("NoDataWriteStored(%s) = %v, want %v", status, got, wantStored)
		}
	}
	// And the two that count are the two that leave the record carrying this
	// round's own version, which is what the next round reads.
	if !execution.NoDataWriteStored(execution.NoDataAlreadyApplied) {
		t.Fatal("a record that already holds this round's write is a record that was kept")
	}
}

// The refusal reason is on the refusal line and the outcome is on the write
// line, and a Plan never gets both.
func TestARefusedWriteIsNotAlsoReportedAsAWrite(t *testing.T) {
	store := &refusingNoDataStore{reason: execution.ReasonCode(contract.ReasonStateCorrupt)}
	coordinator, observed := noDataRefusalFixture(store)
	if err := coordinator.applyNoDataMemory(context.Background(), execution.SlotExecutionRequest{
		Operation: execution.OperationNormal,
	}, []execution.PlanNoDataMutation{refusedMemoryMutation(t)}); err != nil {
		t.Fatalf("applyNoDataMemory() error: %v", err)
	}
	for _, observation := range *observed {
		if observation.Stage == observability.StageNoDataMemoryWritten {
			t.Fatalf("a refused write was also counted as a write: %+v", observation.NoDataMemoryWrite)
		}
	}
}
