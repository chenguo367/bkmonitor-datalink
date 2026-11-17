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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// openPlansReader is the catalog's read of a Query Group's open Segment's
// Plans (controlplane OpenPlans).
type openPlansReader interface {
	OpenPlans(ctx context.Context, queryGroup execution.QueryGroupIdentity, limit int) ([]controlplane.OpenPlan, int, error)
}

// planSegmentsReader is the State store's read of Plans' segment marks
// (state PlanSegments).
type planSegmentsReader interface {
	PlanSegments(ctx context.Context, group execution.QueryGroupIdentity, keys []segment.PlanKey) (segment.PlansReading, error)
}

// readPlanSegments is plan_segments on an object detail: the Plans its open
// Segment runs, at most MaxPlanCarries of them, and each one's mark - its
// piece's, for a piece of a split strategy - under the generation the
// Segment runs it by, with what this replica last did with it.
func readPlanSegments(ctx context.Context, plans openPlansReader, store planSegmentsReader, queryGroup execution.QueryGroupIdentity) (segment.PlansReading, error) {
	open, total, err := plans.OpenPlans(ctx, queryGroup, controlplane.MaxPlanCarries)
	if err != nil {
		return segment.PlansReading{}, err
	}
	keys := make([]segment.PlanKey, len(open))
	for index, plan := range open {
		keys[index] = segment.PlanKey{Plan: plan.Plan, Shard: plan.Shard, Generation: plan.StateGeneration}
	}
	reading, err := store.PlanSegments(ctx, queryGroup, keys)
	if err != nil {
		return segment.PlansReading{}, err
	}
	reading.Total = total
	return reading, nil
}
