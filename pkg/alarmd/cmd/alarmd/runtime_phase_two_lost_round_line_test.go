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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A round let run as a normal one that then times out says on its own
// completion line what it was judged on and what window its read had: the
// divert decision the Runner carried and the binding query budget access
// recorded, here for a sixty-second plan. A round that succeeds writes
// neither.
func TestALostRoundExplainsItselfOnItsCompletionLine(t *testing.T) {
	decision := observability.DivertDecisionFacts{ClassifiedAtUnixMilli: 1_700_000_023_000, FrozenDeadlineUnixMilli: 1_700_000_055_000,
		LiveDeadlineUnixMilli: 1_700_000_045_000, BoundMillis: 5_000, BoundSource: observability.DivertBoundFallback, RemainingMillis: 22_000}
	budget := observability.QueryBudgetFacts{ConsumerDeadlineUnixMilli: 1_700_000_060_000, FrozenReserveMillis: 5_000, LiveReserveMillis: 15_000,
		ReadyAtUnixMilli: 1_700_000_044_700, FrozenQueryDeadlineUnixMilli: 1_700_000_055_000, LiveQueryDeadlineUnixMilli: 1_700_000_045_000}
	request := execution.SlotExecutionRequest{AttemptNo: 1, Operation: execution.OperationNormal}
	request.Contract.Slot.EvaluationTime = execution.EvaluationTime(1_700_000_000)
	for _, tc := range []struct {
		name   string
		result execution.SlotExecutionResult
		lost   bool
	}{
		{"timed out", execution.SlotExecutionResult{Completed: true, CompletionKind: execution.CompletionUnavailable,
			Result: observability.ResultDegraded, ReasonCode: execution.ReasonCode(contract.ReasonQueryTimeout)}, true},
		{"succeeded", execution.SlotExecutionResult{Completed: true, CompletionKind: execution.CompletionFull, Result: observability.ResultSuccess}, false},
	} {
		var completion *observability.Observation
		executor := observedProductionSlotExecutor{
			next: slotExecutorFunc(func(ctx context.Context, _ execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
				observability.BudgetCaptureFromContext(ctx).Record(budget)
				return tc.result, nil
			}),
			observer: observability.ObserverFunc(func(_ context.Context, o observability.Observation) {
				if o.Stage == observability.StageSlotCompleted {
					completion = &o
				}
			}),
			decisionLag: func(time.Duration) {},
		}
		ctx := observability.ContextWithDivertDecision(context.Background(), decision)
		if _, err := executor.Execute(ctx, request); err != nil {
			t.Fatal(err)
		}
		if completion == nil {
			t.Fatalf("%s: no completion line", tc.name)
		}
		if !tc.lost {
			if completion.DivertDecision != nil || completion.QueryBudget != nil {
				t.Fatalf("%s: a successful round wrote %+v %+v", tc.name, completion.DivertDecision, completion.QueryBudget)
			}
			continue
		}
		if completion.DivertDecision == nil || *completion.DivertDecision != decision || completion.QueryBudget == nil || *completion.QueryBudget != budget {
			t.Fatalf("%s: decision %+v budget %+v, want %+v %+v", tc.name, completion.DivertDecision, completion.QueryBudget, decision, budget)
		}
	}
}
