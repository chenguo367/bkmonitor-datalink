// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The results the coordinator's fixture cannot reach as easily read by their
// reason and kind: a degraded result is incomplete, a lease expiring is the
// output retried, a successful result that did not commit names nothing;
// and of two steps the innermost names the error.
func TestNotCommittedResultsAndStepsAreNamed(t *testing.T) {
	query := execution.FinalizationQueryRequired
	for _, test := range []struct {
		name   string
		result execution.SlotExecutionResult
		err    error
		want   string
	}{
		{name: "degraded", result: execution.SlotExecutionResult{Result: observability.ResultDegraded}, want: execution.SlotNotCommittedIncomplete},
		{name: "lease expiring", result: execution.SlotExecutionResult{Result: observability.ResultRetrying,
			ReasonCode: execution.ReasonCode(contract.ReasonOutputLeaseExpiring)}, want: execution.SlotNotCommittedOutputRetryPending},
		{name: "segments not landed", result: execution.SlotExecutionResult{Result: observability.ResultRetrying,
			ReasonCode: execution.ReasonCode(contract.ReasonSegmentWriteFailed)}, want: execution.SlotNotCommittedSegmentWriteFailed},
		{name: "plan reactivated", result: execution.SlotExecutionResult{Result: observability.ResultRetrying,
			ReasonCode: execution.ReasonCode(contract.ReasonPlanReactivated)}, want: execution.SlotNotCommittedActivationChanged},
		{name: "plan not active", result: execution.SlotExecutionResult{Result: observability.ResultDegraded,
			ReasonCode: execution.ReasonCode(contract.ReasonPlanNotActive)}, want: execution.SlotNotCommittedActivationChanged},
		{name: "success not committed", result: execution.SlotExecutionResult{Result: observability.ResultSuccess}, want: execution.SlotNotCommittedOther},
		{name: "innermost step", err: inSlotStep(execution.SlotNotCommittedApplyFailed,
			fmt.Errorf("apply: %w", inSlotStep(execution.SlotNotCommittedOutputFailed, errors.New("ack")))), want: execution.SlotNotCommittedOutputFailed},
		{name: "no step", err: errors.New("before any step"), want: execution.SlotNotCommittedOther},
		{name: "segments unread", err: inSlotStep(execution.SlotNotCommittedQueryFailed, fmt.Errorf("query: %w",
			segmentsUnreadError{why: "the read failed"})), want: execution.SlotNotCommittedSegmentReadFailed},
	} {
		if got := notCommittedReason(query, test.result, test.err); got != test.want {
			t.Errorf("%s: %q, want %q", test.name, got, test.want)
		}
	}
	if step := inSlotStep(execution.SlotNotCommittedQueryFailed, errors.New("the query's own words")); step.Error() != "the query's own words" {
		t.Fatalf("a step keeps the error's text: %q", step.Error())
	}
	if inSlotStep(execution.SlotNotCommittedQueryFailed, nil) != nil {
		t.Fatal("no error in a step is still no error")
	}
}
