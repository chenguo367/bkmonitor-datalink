// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func openPlanRecord(plan execution.PlanIdentity, shard *execution.ShardRef, generation execution.StateGeneration) PlanActivationRecord {
	return PlanActivationRecord{Fact: execution.PlanActivationFact{Plan: plan, Shard: shard,
		Selected: execution.ActivatedPlan{StateGeneration: generation}}}
}

// The open Segment's Plans are each piece of a split strategy apart, under
// the generation the open Segment runs it by - not a closed Segment's -
// each once, in Plan order and then piece, bounded with the count beside;
// a timeline whose Segments are all closed has none.
func TestTheOpenSegmentsPlansAreEachPieceUnderItsGeneration(t *testing.T) {
	plain := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "1001"}
	split := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "1002"}
	// The piece numbered first has the later digest: pieces go by number.
	first := execution.ShardRef{Dimension: "bk_target_ip", Index: 0, Count: 2, MatcherDigest: strings.Repeat("b", 64)}
	second := execution.ShardRef{Dimension: "bk_target_ip", Index: 1, Count: 2, MatcherDigest: strings.Repeat("a", 64)}
	closed := execution.EvaluationTime(600)
	timeline := persistedScheduleTimeline{Segments: []persistedScheduleSegment{
		{Schedule: execution.FrozenQueryGroupSchedule{Segment: execution.ScheduleSegmentFact{End: &closed}},
			Plans: []PlanActivationRecord{openPlanRecord(plain, nil, "old")}},
		{Plans: []PlanActivationRecord{openPlanRecord(split, &second, "g2"), openPlanRecord(plain, nil, "g1"),
			openPlanRecord(split, &first, "g2"), openPlanRecord(plain, nil, "g1")}},
	}}
	plans, total, err := openPlansOf(timeline, MaxPlanCarries)
	want := []OpenPlan{{Plan: plain, StateGeneration: "g1"}, {Plan: split, Shard: first, StateGeneration: "g2"},
		{Plan: split, Shard: second, StateGeneration: "g2"}}
	if err != nil || total != 3 || !reflect.DeepEqual(plans, want) {
		t.Fatalf("open plans = (%+v, %d, %v), want %+v", plans, total, err, want)
	}
	if plans, total, err := openPlansOf(timeline, 2); err != nil || total != 3 || !reflect.DeepEqual(plans, want[:2]) {
		t.Fatalf("open plans by two = (%+v, %d, %v), want the first two of three", plans, total, err)
	}
	timeline.Segments = timeline.Segments[:1]
	if _, _, err := openPlansOf(timeline, MaxPlanCarries); !errors.Is(err, ErrScheduleUnavailable) {
		t.Fatalf("a timeline of closed Segments = %v, want ErrScheduleUnavailable", err)
	}
}
