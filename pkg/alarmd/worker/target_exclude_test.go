// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

func TestExcludedMembersAreAbsentFromAdmissionAndNoDataTogether(t *testing.T) {
	resolution := &targetplan.Resolution{Static: map[string]struct{}{"101": {}, "102": {}}, Excluded: map[string]struct{}{"101": {}}}
	resolution.Compose()
	target := newResolvedTarget(resolution)
	if target.Contains("101") || !target.Contains("102") || !target.Definitive() || !reflect.DeepEqual(target.absenceView().Members, []string{"102"}) {
		t.Fatalf("target views disagree: %+v", target)
	}
	resolution.ExclusionUnavailable = true
	resolution.Selectors = []targetplan.SelectorResult{{Kind: targetplan.SelectorKindExclude, ID: "cw-Host", State: targetplan.SelectorUnavailable, Reason: targetplan.ReasonModelUnresolved}}
	resolution.Compose()
	target = newResolvedTarget(resolution)
	if target.Contains("102") || target.Definitive() || target.absenceView().State != nodata.TargetResolutionUnavailable || len(target.absenceView().Members) != 0 {
		t.Fatalf("unknown exclusion treated as an empty or complete target: %+v", target)
	}
}
