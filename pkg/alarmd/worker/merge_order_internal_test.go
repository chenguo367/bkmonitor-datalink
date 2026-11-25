// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Slot's batches merged in any order, each at its place in merge order -
// the slice its series were read in, then its turn - merge to what they do
// in that order, which is the order a Slot read one slice at a time merges
// them in: the reason of equal rank the first batch said, the Plans in the
// order they first came, the disposition of equal rank the first batch of a
// Plan gave it, its no-data end updates in their batches' order, and the
// completion fold alike. Merged as they come instead, the last batches'
// reasons and the last-seen Plans would lead (015 C1 appendix A 6). What a
// Plan's results list - its series' State results, Level outcomes and
// no-data end updates - is the windows' to take, each window its own
// series', and is not compared here.
func TestASlotsResultsMergeToWhatTheyDoInMergeOrder(t *testing.T) {
	contract := execution.FrozenExecutionContractRef{QueryRevision: "query-v1"}
	plan := func(id string, disposition execution.PlanDisposition, reason string, ends ...string) execution.PlanEvaluationResult {
		result := execution.PlanEvaluationResult{Plan: execution.PlanIdentity{StrategyID: id}, Disposition: disposition,
			ReasonCode: execution.ReasonCode(reason)}
		for _, end := range ends {
			result.NoDataEnds = append(result.NoDataEnds, execution.NoDataEndUpdate{Entry: execution.NoDataEndEntry{Series: execution.SeriesIdentityDigest(end)}})
		}
		return result
	}
	batches := []execution.EvaluationResult{
		{Contract: contract, Result: observability.ResultDegraded, ReasonCode: "QUERY_TIMEOUT",
			Plans: []execution.PlanEvaluationResult{plan("1", execution.PlanUnavailable, "QUERY_TIMEOUT", "a")}},
		{Contract: contract, Result: observability.ResultDegraded, ReasonCode: "QUERY_UNAVAILABLE",
			Plans: []execution.PlanEvaluationResult{plan("2", execution.PlanDecided, ""),
				plan("1", execution.PlanReadinessGap, "QUERY_NOT_READY", "b")}},
		{Contract: contract, Result: observability.ResultSuccess,
			Plans: []execution.PlanEvaluationResult{plan("3", execution.PlanDecided, ""), plan("1", execution.PlanDecided, "", "c")}},
	}
	orders := []execution.MergeOrder{{Slice: 0, Seq: 1}, {Slice: 2, Seq: 2}, {Slice: 3, Seq: 3}}
	merge := func(sequence []int) (execution.EvaluationResult, slotEvaluationSummary) {
		var merged execution.EvaluationResult
		var summary slotEvaluationSummary
		for _, index := range sequence {
			if err := appendProvisionalAt(&merged, batches[index], orders[index], &summary.results); err != nil {
				t.Fatal(err)
			}
			summary.addAt(batches[index], nil, orders[index])
		}
		return merged, summary
	}
	headers := func(result execution.EvaluationResult) []string {
		var heads []string
		for _, plan := range result.Plans {
			heads = append(heads, plan.Plan.StrategyID+" "+string(plan.Disposition)+" "+string(plan.ReasonCode))
		}
		return heads
	}
	inOrder, inOrderSummary := merge([]int{0, 1, 2})
	for _, sequence := range [][]int{{2, 1, 0}, {1, 2, 0}, {2, 0, 1}} {
		merged, summary := merge(sequence)
		if merged.Result != inOrder.Result || merged.ReasonCode != inOrder.ReasonCode || !reflect.DeepEqual(headers(merged), headers(inOrder)) {
			t.Fatalf("merged in %v: %s %s %v, want what merge order gives, %s %s %v", sequence, merged.Result, merged.ReasonCode,
				headers(merged), inOrder.Result, inOrder.ReasonCode, headers(inOrder))
		}
		var plans, wantPlans []execution.PlanIdentity
		var ends, wantEnds [][]execution.NoDataEndUpdate
		for index := range summary.plans {
			plans, ends = append(plans, summary.plans[index].plan), append(ends, summary.plans[index].noDataEnds)
			wantPlans, wantEnds = append(wantPlans, inOrderSummary.plans[index].plan), append(wantEnds, inOrderSummary.plans[index].noDataEnds)
		}
		if !reflect.DeepEqual(plans, wantPlans) || !reflect.DeepEqual(ends, wantEnds) ||
			!reflect.DeepEqual(summary.completion.Dispositions(), inOrderSummary.completion.Dispositions()) {
			t.Fatalf("summary merged in %v: Plans %v ends %v dispositions %v, want %v %v %v", sequence, plans, ends,
				summary.completion.Dispositions(), wantPlans, wantEnds, inOrderSummary.completion.Dispositions())
		}
	}
	if inOrder.ReasonCode != "QUERY_TIMEOUT" || inOrder.Plans[0].Plan.StrategyID != "1" || inOrder.Plans[0].Disposition != execution.PlanUnavailable ||
		inOrder.Plans[1].Plan.StrategyID != "2" || inOrder.Plans[2].Plan.StrategyID != "3" {
		t.Fatalf("merge order gave %s %+v, want the first batch's reason and Plan 1's disposition, Plans 1, 2, 3", inOrder.ReasonCode, inOrder.Plans)
	}
}
