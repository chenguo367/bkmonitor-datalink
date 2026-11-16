// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import "context"

// StageSeriesNotEvaluated names a completed Slot that bound PRIMARY series to
// a due Plan and never took them up: answered by a query, they reached no
// evaluation, no resume and no skip (SeriesNotEvaluated). The count is in
// series_not_evaluated_total by family; this line says which Query Group and
// which Plans.
const StageSeriesNotEvaluated = "series_not_evaluated"

// ReasonSeriesNotEvaluated is the series_not_evaluated line's reason.
const ReasonSeriesNotEvaluated ReasonCode = "SERIES_NOT_EVALUATED"

// MaxSeriesNotEvaluatedPlans is the most Plans one series_not_evaluated line
// names; the rest are counted in PlansTruncated.
const MaxSeriesNotEvaluatedPlans = 32

// SeriesNotEvaluatedPlan is one due Plan's series lost under one family: a
// Plan of several families is named once under each, as the counter counts
// it.
type SeriesNotEvaluatedPlan struct {
	TenantID   string `json:"tenant_id"`
	BusinessID string `json:"business_id"`
	StrategyID string `json:"strategy_id"`
	Family     string `json:"family"`
	Missing    uint64 `json:"missing"`
}

// SeriesNotEvaluatedFacts is the Plans of one completed Slot that lost
// series, in the order the Slot's header lists its due Plans.
//
// SuppressedSlots and SuppressedSeries are what the Query Group's lines held
// back since its last one: the Slots that lost series and wrote no line, and
// the series they lost together (the worker writes a Query Group's line at
// most once in ten minutes).
type SeriesNotEvaluatedFacts struct {
	Plans            []SeriesNotEvaluatedPlan `json:"plans"`
	PlansTruncated   int                      `json:"plans_truncated,omitempty"`
	SuppressedSlots  uint64                   `json:"suppressed_slots,omitempty"`
	SuppressedSeries uint64                   `json:"suppressed_series,omitempty"`
}

// ObserveSeriesNotEvaluated writes the one series_not_evaluated line of a
// completed Slot, at WARN and outside every sample: a Plan that loses series
// is a defect of this deployment's own, and a sampled line would name the
// first Query Group it met and none after. The worker writes a Query
// Group's line at most once in ten minutes, the first at once, each after it
// with what was held back since (SuppressedSlots, SuppressedSeries): even a
// loss on every round of every Query Group is the Query Groups' count of
// lines every ten minutes. Nothing is written for a Slot that lost none.
func ObserveSeriesNotEvaluated(ctx context.Context, observer Observer, trace TraceFields, operation Operation, facts SeriesNotEvaluatedFacts) {
	if observer == nil || len(facts.Plans) == 0 {
		return
	}
	if len(facts.Plans) > MaxSeriesNotEvaluatedPlans {
		facts.PlansTruncated += len(facts.Plans) - MaxSeriesNotEvaluatedPlans
		facts.Plans = facts.Plans[:MaxSeriesNotEvaluatedPlans]
	}
	// Observability is a fail-open side channel, as at every other boundary.
	defer func() { _ = recover() }()
	observer.Observe(ctx, Observation{
		Component: ComponentScheduler, Stage: StageSeriesNotEvaluated, Result: ResultDegraded,
		ReasonCode: ReasonSeriesNotEvaluated, Operation: operation, Trace: trace, SeriesNotEvaluated: &facts,
	})
}
