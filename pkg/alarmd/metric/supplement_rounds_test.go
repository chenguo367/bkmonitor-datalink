// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The metrics that count rounds leave supplements out: an executor return, a
// Slot's own timings, a completed Slot near its retained share, and the
// progress timestamps a stall is read from -- which a supplement kept fresh
// while the rounds had stopped. The same observations as a round's are
// counted, which shows each metric is reached.
func TestTheRoundMetricsLeaveSupplementsOut(t *testing.T) {
	for operation, counted := range map[observability.Operation]bool{observability.OperationSupplement: false, observability.OperationNormal: true} {
		r := NewRecorder(BuildInfo{})
		completed := observability.Observation{Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
			Operation: operation, ExecuteOutcome: "incomplete", Result: observability.ResultSuccess, Duration: time.Second,
			SlotBudgetUsage: &observability.SlotBudgetUsageFacts{RetainedBytes: 99, RetainedShareBytes: 100}}
		evaluated := observability.Observation{Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
			Operation: operation, Result: observability.ResultSuccess}
		r.Observe(context.Background(), completed)
		r.Observe(context.Background(), evaluated)
		got := map[string]bool{
			"execute_return_total":                         len(gatherFamily(t, r, "bkmonitor_alarmd_execute_return_total")) > 0,
			"slot_operation_duration_seconds":              len(gatherFamily(t, r, "bkmonitor_alarmd_slot_operation_duration_seconds")) > 0,
			"state_retained_share_approaching_slots_total": gatherFamily(t, r, "bkmonitor_alarmd_state_retained_share_approaching_slots_total")[0].GetCounter().GetValue() > 0,
			"last_progress_timestamp_seconds":              len(gatherFamily(t, r, "bkmonitor_alarmd_last_progress_timestamp_seconds")) > 0,
		}
		for name, present := range got {
			if present != counted {
				t.Errorf("operation %s: %s counted %v, want %v", operation, name, present, counted)
			}
		}
	}
}
