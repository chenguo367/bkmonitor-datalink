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
	"slices"
	"sort"
	"strconv"
	"strings"
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
// is any other failure; a lost fence ends the round. The no-data family's
// word (014 next9 ⑥) is restated with it: this Leader's epoch only beside
// the segment skip word and only while every ready worker declares the
// no-data family's contract as well, none otherwise; turning the skip off
// takes both back.
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
	noDataGate := ownership.SegmentSkipNoDataAdmission(workers)
	wantNoData := want && noDataGate.Admitted
	var wanted, wantedNoData uint64
	if want {
		wanted = authority.Fence.OwnerEpoch
	}
	if wantNoData {
		wantedNoData = authority.Fence.OwnerEpoch
	}
	queryGroups := make([]execution.QueryGroupIdentity, 0, len(records))
	for queryGroup, record := range records {
		if record.SegmentSkip != wanted || record.SegmentSkipNoData != wantedNoData {
			queryGroups = append(queryGroups, queryGroup)
		}
	}
	sort.Slice(queryGroups, func(left, right int) bool { return queryGroups[left] < queryGroups[right] })
	for _, queryGroup := range queryGroups {
		record := records[queryGroup]
		_, err := runtime.records.PublishAssignment(ctx, authority, ownership.AssignmentDecision{
			QueryGroup: queryGroup, DesiredWorkerID: record.DesiredWorkerID, ExpectedRecordRevision: record.RecordRevision,
			PlacementReason: record.PlacementReason, DecidedAt: at, SetSegmentSkip: want, WithdrawSegmentSkip: !want,
			SetSegmentSkipNoData: wantNoData, WithdrawSegmentSkipNoData: !wantNoData,
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
	facts.Changed = runtime.segmentSkipGateChanged(authority.Fence.OwnerEpoch, facts, noDataGate)
	return facts, nil
}

// segmentSkipGate is the gate a Leader's latest round found: under which
// epoch, and the gate itself - asked, held, the ready replicas not declaring
// the contract.
type segmentSkipGate struct {
	epoch uint64
	key   string
}

// segmentSkipGateChanged says the round's segment skip gate is news, for its
// rebalance_planned line to be written: the round set, took back or failed
// to write a word, or the gate is not what this Leader's previous round
// found. A Leader's first round always is: a gate held from the start by a
// replica that never declares the contract is said once, by name, when the
// Leader takes over, and again whenever it moves. The no-data family's gate
// moving is news too. It keeps this round's for the next.
func (runtime *productionPhaseTwoOwnership) segmentSkipGateChanged(epoch uint64, facts observability.SegmentSkipFacts,
	noDataGate ownership.ShardSplitGate) bool {
	unaware := slices.Clone(facts.Unaware)
	slices.Sort(unaware)
	noDataUnaware := slices.Clone(noDataGate.Unaware)
	slices.Sort(noDataUnaware)
	key := strconv.FormatBool(facts.Asked) + "\x00" + facts.Held + "\x00" + strings.Join(unaware, "\x00") + "\x00\x00" +
		strconv.FormatBool(noDataGate.Admitted) + "\x00" + strings.Join(noDataUnaware, "\x00")
	previous := runtime.segmentSkipGate
	runtime.segmentSkipGate = segmentSkipGate{epoch: epoch, key: key}
	return facts.Set > 0 || facts.Withdrawn > 0 || facts.Failed > 0 || previous.epoch != epoch || previous.key != key
}
