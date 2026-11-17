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
	"context"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// OpenPlan is one Plan of a Query Group's open schedule Segment - its piece
// of a split strategy, zero for one not split - under the generation the
// Segment runs it by: what the Plan's stable segments are keyed by.
type OpenPlan struct {
	Plan            execution.PlanIdentity
	Shard           execution.ShardRef
	StateGeneration execution.StateGeneration
}

// OpenPlans reads the Plans of a Query Group's open schedule Segment, each
// piece once, in Plan order and then piece: at most limit (at most
// MaxPlanCarries), and how many the Segment runs. ErrScheduleUnavailable
// when the timeline holds no open Segment.
func (repository *RedisCatalogRepository) OpenPlans(ctx context.Context, queryGroup execution.QueryGroupIdentity, limit int) ([]OpenPlan, int, error) {
	if limit <= 0 || limit > MaxPlanCarries {
		limit = MaxPlanCarries
	}
	timeline, err := repository.loadScheduleTimelineHinted(ctx, queryGroup)
	if err != nil {
		return nil, 0, err
	}
	return openPlansOf(timeline, limit)
}

// openPlansOf is OpenPlans of a timeline read.
func openPlansOf(timeline persistedScheduleTimeline, limit int) ([]OpenPlan, int, error) {
	for index := len(timeline.Segments) - 1; index >= 0; index-- {
		if timeline.Segments[index].Schedule.Segment.End != nil {
			continue
		}
		seen := map[OpenPlan]bool{}
		var plans []OpenPlan
		for _, record := range timeline.Segments[index].Plans {
			plan := OpenPlan{Plan: record.Fact.Plan, Shard: execution.ShardOf(record.Fact.Shard),
				StateGeneration: record.Fact.Selected.StateGeneration}
			if !seen[plan] {
				seen[plan] = true
				plans = append(plans, plan)
			}
		}
		sort.Slice(plans, func(i, j int) bool {
			if plans[i].Plan != plans[j].Plan {
				return lessPlanIdentity(plans[i].Plan, plans[j].Plan)
			}
			if plans[i].Shard.Index != plans[j].Shard.Index {
				return plans[i].Shard.Index < plans[j].Shard.Index
			}
			if plans[i].Shard.MatcherDigest != plans[j].Shard.MatcherDigest {
				return plans[i].Shard.MatcherDigest < plans[j].Shard.MatcherDigest
			}
			return plans[i].StateGeneration < plans[j].StateGeneration
		})
		return plans[:min(len(plans), limit)], len(plans), nil
	}
	return nil, 0, ErrScheduleUnavailable
}
