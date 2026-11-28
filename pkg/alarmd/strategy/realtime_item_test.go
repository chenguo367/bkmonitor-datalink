// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package strategy

import (
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// A Plan whose realtime part holds compiles as the queried Plan it sits on
// does; one whose part does not - a regular expression that does not
// compile here - is refused as an invalid Plan at realtime, with what was
// wrong, wherever the Plan is read, rather than run.
func TestAPlanWhoseRealtimePartDoesNotHoldIsRefusedWhereItIsRead(t *testing.T) {
	plan := validPlan()
	plan.Realtime = &contract.RealtimeItemV2{Item: contract.RealtimeItemKey{StrategyID: 1001, ItemID: 1, QueryMD5: "query-1"}, Metric: "cpu_usage", Conditions: [][]contract.RealtimeConditionV2{
		{{Key: "ip", Method: contract.RealtimeConditionEq, Values: []string{"127.0.0.1"}}}}}
	if terminal := mustCompileResult(t, newTestCompiler(t), plan).PlanTerminal(); terminal != nil {
		t.Fatalf("a realtime part that holds refused: %#v", terminal)
	}
	plan.Realtime.Conditions[0][0] = contract.RealtimeConditionV2{Key: "module", Method: contract.RealtimeConditionReg, Values: []string{"db-("}}
	terminal := mustCompileResult(t, newTestCompiler(t), plan).PlanTerminal()
	if terminal == nil || terminal.ReasonCode != contract.ReasonPlanInvalid || terminal.FieldPath != "realtime" ||
		!strings.Contains(terminal.Detail, "regular expression") {
		t.Fatalf("plan terminal = %#v, want PLAN_INVALID at realtime with the expression refused", terminal)
	}
}
