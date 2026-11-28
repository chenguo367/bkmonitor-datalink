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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// localOwnershipView is what a Runner's local ownership check reads of the
// Worker's view client: a Query Group's installed entry, and whether the
// Leader was heard from within a bound.
type localOwnershipView interface {
	Entry(execution.QueryGroupIdentity) (viewstream.Entry, bool)
	HeardWithin(time.Duration) bool
}

// productionLocalOwnership is one Runner's local ownership check: its
// Session judged against the Query Group's installed entry (015 R1).
type productionLocalOwnership struct {
	session     *ownership.Session
	queryGroup  execution.QueryGroupIdentity
	view        localOwnershipView
	heardWithin time.Duration
}

func (local productionLocalOwnership) ValidateLocally(at time.Time) (execution.OwnerFence, ownership.AssignmentRecord, string, error) {
	entry, named := local.view.Entry(local.queryGroup)
	return local.session.ValidateLocally(at, localOwnershipOf(local.queryGroup, entry, named, local.view.HeardWithin(local.heardWithin)))
}

func (local productionLocalOwnership) NoteFenceRefused(at time.Time) {
	local.session.NoteFenceRefused(at)
}

// localOwnershipOf is the Assignment record an installed entry rebuilds:
// every field a fence check reads back, on the same clocks - the record's
// instants are Redis's, and so are the entry's. ObservedAt stays zero: no
// fence check read it.
func localOwnershipOf(queryGroup execution.QueryGroupIdentity, entry viewstream.Entry, named, heard bool) ownership.LocalOwnership {
	assignment := entry.Assignment
	record := ownership.AssignmentRecord{
		QueryGroup: queryGroup, DesiredWorkerID: assignment.DesiredWorkerID,
		AssignmentGeneration: assignment.AssignmentGeneration, RecordRevision: assignment.Revision,
		ControlEpoch: assignment.ControlEpoch, PlacementReason: ownership.PlacementReason(assignment.PlacementReason),
		ContentScope: assignment.ContentScope, PendingContentScope: assignment.PendingContentScope,
		TimelineRecordRevision: assignment.TimelineRecordRevision,
		SegmentSkip:            assignment.SegmentSkip, SegmentSkipNoData: assignment.SegmentSkipNoData,
	}
	if assignment.AssignedAtMs > 0 {
		record.AssignedAt = time.UnixMilli(assignment.AssignedAtMs)
	}
	if assignment.EffectiveAtMs > 0 {
		record.EffectiveAt = time.UnixMilli(assignment.EffectiveAtMs)
	}
	return ownership.LocalOwnership{Named: named && entry.QueryGroup == queryGroup, Complete: assignment.Complete(), Record: record, HeardWithin: heard}
}

var _ scheduler.LocalOwnership = productionLocalOwnership{}
