// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
)

// resolvedTarget is one Plan's target plan as this Slot resolved it: the
// member set the admission filter asks about, and the reduced view absence
// reads. Both come out of one resolution, which is the point of holding
// them together.
type resolvedTarget struct {
	absence nodata.TargetResolution
	members map[string]struct{}
}

// Contains is the admission filter's question.
func (target *resolvedTarget) Contains(key string) bool {
	if target == nil {
		return false
	}
	_, found := target.members[key]
	return found
}

// absenceView is what the no-data round reads; nil when nothing resolved.
func (target *resolvedTarget) absenceView() *nodata.TargetResolution {
	if target == nil {
		return nil
	}
	view := target.absence
	return &view
}

// ResolvedTargets hands the source what Begin resolved, so the records it
// admits are filtered against the same members absence is judged against.
// A Plan whose resolution is unavailable is left out: its filter then admits
// nothing, which is what "the target is not known" has to mean for records.
func (stream *streamedExecution) ResolvedTargets() execution.TargetMemberships {
	if len(stream.targetResolutions) == 0 {
		return nil
	}
	targets := make(execution.TargetMemberships, len(stream.targetResolutions))
	for identity, resolution := range stream.targetResolutions {
		if resolution == nil || resolution.absence.State == nodata.TargetResolutionUnavailable {
			continue
		}
		targets[identity] = resolution
	}
	return targets
}
