// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A prior attempt already counted a full observation for this same Slot.
// Finalization must not erase that evidence or leave Progress permanently stuck.
func TestQueryFreeFinalizationPreservesObservedProtection(t *testing.T) {
	for _, required := range []uint32{9, 10} {
		activation := activePlanResult("state-v2", 2)
		activation.Facts[0].Selected.RequiredFullSlots = 9
		selected := activation.Facts[0].Selected
		fixture := newQueryFreeFixture(t, []execution.PlanActivationResult{activation})
		marker := queryFreeGapMarker(t, selected, currentQueryFreeApplyVersion(t), selected.ScheduleRevision,
			[]execution.GapScopeState{{Scope: execution.GapScope{}, Status: execution.GapStatusGapped,
				ReasonCode: execution.ReasonCode(contract.ReasonGapSkipped), RequiredFullSlots: required, ObservedFullSlots: 1}})
		before := cloneGapGuardSnapshot(marker)
		fixture.ports.markers[marker.Identity] = marker
		result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationReplay))
		if err != nil || !result.Completed || fixture.ports.progressCalls != 1 {
			t.Fatalf("required=%d: result=%+v err=%v progress=%d", required, result, err, fixture.ports.progressCalls)
		}
		if fixture.ports.applyCalls != 0 || !reflect.DeepEqual(before, fixture.ports.markers[marker.Identity]) {
			t.Fatal("reusing same-Slot protection changed persisted evidence")
		}
	}
}

// A level-only marker cannot be reused as Plan-wide protection. The coordinator
// must request an additional Plan scope rather than repeatedly rejecting the Slot.
// Preservation of stored level scopes is checked with ExecutionStore separately.
func TestQueryFreeFinalizationAddsMissingPlanProtection(t *testing.T) {
	activation := activePlanResult("state-v2", 2)
	activation.Facts[0].Selected.RequiredFullSlots = 9
	selected := activation.Facts[0].Selected
	fixture := newQueryFreeFixture(t, []execution.PlanActivationResult{activation})
	marker := queryFreeGapMarker(t, selected, currentQueryFreeApplyVersion(t), selected.ScheduleRevision,
		[]execution.GapScopeState{
			{Scope: execution.GapScope{HasLevel: true, LevelID: 1}, Status: execution.GapStatusGapped, ReasonCode: execution.ReasonCode(contract.ReasonHistoryGapped), RequiredFullSlots: 3},
			{Scope: execution.GapScope{HasLevel: true, LevelID: 2}, Status: execution.GapStatusWarming, ReasonCode: execution.ReasonCode(contract.ReasonHistoryGapped), RequiredFullSlots: 4, ObservedFullSlots: 1},
		})
	fixture.ports.markers[marker.Identity] = marker
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationReplay))
	if err != nil || !result.Completed || fixture.ports.progressCalls != 1 {
		t.Fatalf("result=%+v err=%v progress=%d", result, err, fixture.ports.progressCalls)
	}
	if len(fixture.ports.mutations) != 1 {
		t.Fatalf("mutations=%d", len(fixture.ports.mutations))
	}
	m := fixture.ports.mutations[0]
	if m.ExpectedMarkerRevision != marker.MarkerRevision || m.ApplyVersion != marker.PersistedApplyVersion || len(m.Scopes) != 1 || m.Scopes[0].Scope.HasLevel || m.Scopes[0].RequiredFullSlots != 9 {
		t.Fatalf("unexpected extension: %+v", m)
	}
	commits := 0
	for _, observation := range *fixture.observations {
		if observation.Stage != observability.StageGapGuardCommitted {
			continue
		}
		commits++
		if len(observation.GapExtensions) != 1 {
			t.Fatalf("missing extension evidence: %+v", observation)
		}
		facts := observation.GapExtensions[0]
		if facts.MarkerRevision != marker.MarkerRevision || facts.StrategyID != selected.Identity.StrategyID || len(facts.Persisted) != 2 || len(facts.Proposed) != 1 || facts.Persisted[1].Observed != 1 {
			t.Fatalf("incomplete extension evidence: %+v", facts)
		}
	}
	if commits != 1 {
		t.Fatalf("commit observations=%d, want one (no duplicate metric event)", commits)
	}
}
