// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// multiDetectFrozenExecution is frozenExecution for an item detected over two
// inputs: its PRIMARY, the first input, and a second input of a query of its
// own (another field of the same table), read as the control plane writes it
// - over the PRIMARY's own window, step and alignment.
func multiDetectFrozenExecution(t *testing.T) (execution.FrozenExecutionContractRef, FrozenPlan) {
	t.Helper()
	contractRef, frozen := frozenExecution(t)
	primary := frozen.Requirements[0]
	facts := frozen.QueryFacts[primary.LogicalQueryRef]
	other := facts
	other.QueryRevision = ""
	other.QueryList = append([]execution.QueryClause(nil), facts.QueryList...)
	other.QueryList[0].FieldName = "usage_limit"
	built, err := execution.BuildQueryPlanFacts(other)
	if err != nil {
		t.Fatal(err)
	}
	ref := execution.LogicalQueryRef(built.QueryRevision)
	peer := primary
	peer.RequirementID, peer.DatasetName, peer.Role, peer.LogicalQueryRef = "input:limit", "input:limit", execution.InputRoleMultiDetectInput, ref
	frozen.Requirements = []execution.DataRequirement{primary, peer}
	frozen.QueryFacts[ref] = built
	return bindFrozenDueDigest(t, contractRef, frozen), frozen
}

// The inputs of an item detected over several are asked the same window:
// each a query of its own, both over one logical window, one provider range
// and one accepted range, on one step - the points they answer lie on one
// grid and are judged side by side.
func TestMultiDetectInputsAskOneWindow(t *testing.T) {
	contractRef, frozen := multiDetectFrozenExecution(t)
	prepared, err := Prepare(contractRef, frozen, time.Second)
	if err != nil {
		t.Fatalf("Prepare() error=%v", err)
	}
	if len(prepared.Queries) != 2 {
		t.Fatalf("queries %+v, want one for each input", prepared.Queries)
	}
	first, second := prepared.Queries[0].Spec, prepared.Queries[1].Spec
	if first.Digest == second.Digest || first.PlanFacts.QueryRevision == second.PlanFacts.QueryRevision {
		t.Fatal("the two inputs were asked as one query")
	}
	if first.LogicalWindow != second.LogicalWindow || first.ProviderRange != second.ProviderRange || first.AcceptedRange != second.AcceptedRange {
		t.Fatalf("windows %+v/%+v/%+v and %+v/%+v/%+v, want one window for both inputs",
			first.LogicalWindow, first.ProviderRange, first.AcceptedRange, second.LogicalWindow, second.ProviderRange, second.AcceptedRange)
	}
	if first.PlanFacts.RequestStepMillis() != second.PlanFacts.RequestStepMillis() ||
		execution.RequestEndTrimEligible(first.PlanFacts) != execution.RequestEndTrimEligible(second.PlanFacts) {
		t.Fatal("the two inputs are asked on another step or end")
	}
	at := int64(contractRef.Slot.EvaluationTime)
	if want := (execution.QueryWindow{Start: at - 60, End: at}); first.LogicalWindow != want {
		t.Fatalf("window %+v, want the PRIMARY's own %+v", first.LogicalWindow, want)
	}
}
