// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
)

// What the Slot resolved is handed to the source for admission and to the
// no-data round for absence from one holder: an unavailable resolution is
// left out of the memberships (its filter then admits nothing) and read by
// absence as unavailable; a complete or incomplete one is both; nothing
// resolved is nil on both sides.
func TestOneResolutionServesAdmissionAndAbsence(t *testing.T) {
	complete := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "1"}
	incomplete := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "2"}
	unavailable := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "3"}
	stream := &streamedExecution{targetResolutions: map[execution.PlanIdentity]*resolvedTarget{
		complete:    {absence: nodata.TargetResolution{State: nodata.TargetResolutionComplete, Members: []string{"101"}}, members: map[string]struct{}{"101": {}}},
		incomplete:  {absence: nodata.TargetResolution{State: nodata.TargetResolutionIncomplete, Members: []string{"101"}}, members: map[string]struct{}{"101": {}}},
		unavailable: {absence: nodata.TargetResolution{State: nodata.TargetResolutionUnavailable}, members: map[string]struct{}{"101": {}}},
	}}
	memberships := stream.ResolvedTargets()
	if len(memberships) != 2 || memberships[complete] == nil || memberships[incomplete] == nil || memberships[unavailable] != nil {
		t.Fatalf("memberships = %v, want the complete and incomplete resolutions only", memberships)
	}
	if !memberships[complete].Contains("101") || memberships[complete].Contains("102") {
		t.Fatal("membership does not answer from the resolved members")
	}
	if view := stream.targetResolutions[unavailable].absenceView(); view == nil || view.State != nodata.TargetResolutionUnavailable {
		t.Fatalf("absence view of the unavailable resolution = %+v", view)
	}
	if view := stream.targetResolutions[execution.PlanIdentity{StrategyID: "none"}].absenceView(); view != nil {
		t.Fatalf("absence view of nothing = %+v, want nil", view)
	}
	if (&streamedExecution{}).ResolvedTargets() != nil {
		t.Fatal("a Slot that resolved nothing hands out memberships")
	}
}
