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
	"context"
	"errors"
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// reconcileSegmentSkip restates the segment skip word (014 B3 stage 3) on
// the records the round settled: this Leader's epoch on every record while
// the values ask for it and every ready worker declares the contract, none
// otherwise. A record that already says what the round wants is left
// alone; the word is written by the publish script from the fence it
// checks, so it is the epoch of the view this Leader publishes, and a new
// Leader restates every record once. A conflict - another write moved the
// record since the round read it - is counted and restated next round, as
// is any other failure; a lost fence ends the round.
func (runtime *productionPhaseTwoOwnership) reconcileSegmentSkip(
	ctx context.Context,
	authority ownership.PublicationAuthority,
	workers []ownership.WorkerRegistration,
	records map[execution.QueryGroupIdentity]ownership.AssignmentRecord,
	at time.Time,
) (observability.SegmentSkipFacts, error) {
	gate := ownership.SegmentSkipAdmission(workers)
	asked := runtime.dependencies.SegmentSkip
	facts := observability.SegmentSkipFacts{Asked: asked}
	if asked {
		facts.Ready, facts.Unaware = gate.Ready, gate.Unaware
		if !gate.Admitted {
			facts.Held = ownership.SegmentSkipHeld
		}
	}
	want := asked && gate.Admitted
	var wanted uint64
	if want {
		wanted = authority.Fence.OwnerEpoch
	}
	queryGroups := make([]execution.QueryGroupIdentity, 0, len(records))
	for queryGroup, record := range records {
		if record.SegmentSkip != wanted {
			queryGroups = append(queryGroups, queryGroup)
		}
	}
	sort.Slice(queryGroups, func(left, right int) bool { return queryGroups[left] < queryGroups[right] })
	for _, queryGroup := range queryGroups {
		record := records[queryGroup]
		_, err := runtime.records.PublishAssignment(ctx, authority, ownership.AssignmentDecision{
			QueryGroup: queryGroup, DesiredWorkerID: record.DesiredWorkerID, ExpectedRecordRevision: record.RecordRevision,
			PlacementReason: record.PlacementReason, DecidedAt: at, SetSegmentSkip: want, WithdrawSegmentSkip: !want,
		})
		switch {
		case err == nil && want:
			facts.Set++
		case err == nil:
			facts.Withdrawn++
		case errors.Is(err, ownership.ErrStaleFence):
			return facts, err
		case ctx.Err() != nil:
			return facts, ctx.Err()
		default:
			facts.Failed++
		}
	}
	return facts, nil
}
