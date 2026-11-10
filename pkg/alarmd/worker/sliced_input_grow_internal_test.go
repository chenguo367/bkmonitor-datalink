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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// openStatementFor is a Plan's open statement at one Slot's version over
// scopes, expecting revision.
func openStatementFor(t *testing.T, revision uint64, scopes ...execution.GapScopeMutation) execution.PlanGapMutation {
	t.Helper()
	statement, err := execution.BuildPlanGapMutation(execution.PlanGapMutation{
		Identity:               execution.PlanGapIdentity{Plan: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}, StateGeneration: "state-v1"},
		ExpectedMarkerRevision: revision, ScheduleRevision: "plan-schedule-v1",
		ApplyVersion: execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 1_788_000_000, SlotDigest: "slot"},
		Scopes:       scopes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return statement
}

func levelOpen(level uint32, reason string, required uint32) execution.GapScopeMutation {
	return execution.GapScopeMutation{Scope: execution.GapScope{LevelID: level, HasLevel: true}, Kind: execution.GapOpen,
		ReasonCode: execution.ReasonCode(reason), RequiredFullSlots: required}
}

// B3's growth is a second statement at the Slot's version, so it is judged
// by the rule the store extends a marker at its own version by
// (execution.ExtendSameSlotGap), not by the fold of the reasons: a slice
// that opens a scope the marker lacks grows it, one revision on, the scopes
// it held kept as they were; a slice that only answers a held scope worse
// writes nothing, since the store would refuse the changed reason as a
// CONFLICT and the scope already guards it; a statement of another marker
// is refused by name.
func TestASliceGrowsTheMarkerOnlyByWhatTheStoreExtendsItWith(t *testing.T) {
	previous := openStatementFor(t, 3, levelOpen(5, contract.ReasonQueryPartial, 1))
	grown, grows, err := growOpenStatement(previous, openStatementFor(t, 3, levelOpen(6, contract.ReasonQueryUnavailable, 1)))
	if err != nil || !grows {
		t.Fatalf("grow = %v, %v; want the new scope added", grows, err)
	}
	if grown.ExpectedMarkerRevision != 4 || grown.ValidateDigest() != nil || len(grown.Scopes) != 2 ||
		grown.Scopes[0] != previous.Scopes[0] || grown.Scopes[1].Scope.LevelID != 6 {
		t.Fatalf("grown statement %+v, want both scopes, the held one unchanged, at revision 4", grown)
	}
	held := []execution.GapScopeState{{Scope: previous.Scopes[0].Scope, Status: execution.GapStatusGapped,
		ReasonCode: previous.Scopes[0].ReasonCode, RequiredFullSlots: 1}}
	if _, extended := execution.ExtendSameSlotGap(held, grown.Scopes); !extended {
		t.Fatal("the store would not extend the marker with the grown statement")
	}

	if _, grows, err := growOpenStatement(previous, openStatementFor(t, 3, levelOpen(5, contract.ReasonQueryUnavailable, 1))); err != nil || grows {
		t.Fatalf("a worse answer on a held scope grows = %v, %v; want nothing written", grows, err)
	}
	other := openStatementFor(t, 3, levelOpen(6, contract.ReasonQueryPartial, 1))
	other.Identity.StateGeneration = "state-v2"
	if _, _, err := growOpenStatement(previous, other); err == nil {
		t.Fatal("grew a marker with another marker's statement")
	}
}
