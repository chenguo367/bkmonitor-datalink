// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// requestEnd is the end, in seconds, a query asks the provider for, and why.
//
// The provider evaluates a range query at its start's grid points up to and
// including its end (unify-query v3.9.4910 and v3.9.4892: QueryTs.ToTime
// leaves the end as asked, and the range query runs from the aligned start to
// it); a window ends before its end (AcceptedRange, half-open). The point at
// the end came back only to be let go here (outside_range_after) - and it is
// the next round's first: every one-step round read each point twice.
//
// It is asked a second earlier. Every grid point is a whole second - the
// step is, and so is the start it is aligned from - so a second before the
// end leaves out the point at the end and no other: the window's last point,
// at or before end - step, is asked for as before, its value from its own
// look-back. Not for a step that is not whole seconds, whose grid need not
// be; not for a query not aligned (not_time_align), held back until the first
// readings; and not for a source not confirmed to value each point from its
// own look-back alone (eligibleForTrim).
func requestEnd(spec execution.PhysicalQuerySpec) (int64, string) {
	end := spec.ProviderRange.End
	if !eligibleForTrim(spec.PlanFacts) {
		return end, observability.QueryRequestEndSourceNotEligible
	}
	if spec.PlanFacts.NotTimeAlign {
		return end, observability.QueryRequestEndNotTimeAlign
	}
	// A provider range is never empty (QueryWindow.Validate: its start before
	// its end), so a second earlier never passes its start.
	if step := spec.PlanFacts.RequestStepMillis(); step <= 0 || step%1000 != 0 {
		return end, observability.QueryRequestEndStepNotWholeSeconds
	}
	return end - 1, observability.QueryRequestEndTrimmed
}

// requestEndOf is the end requestEnd asks for.
func requestEndOf(spec execution.PhysicalQuerySpec) int64 {
	end, _ := requestEnd(spec)
	return end
}

// metricTimeSeriesSources is the clause data sources that are a metric time
// series: the compilers' empty one and the two names unify-query normalizes
// to bkmonitor. Confirmed by the provider's owners for v3.9.4910 and
// v3.9.4892, the versions deployed: such a table has a VmRt and must-vm-query
// is on by default, so the query is read by DirectQueryRange - VictoriaMetrics
// valuing each point from its own look-back, never through the engine - and
// asking its end a second or a step earlier answered the same points and
// values on a production deployment. No influxdb traffic was read on any of
// them.
var metricTimeSeriesSources = map[string]bool{"": true, "bkmonitor": true, "bk_monitor": true}

// eligibleForTrim is whether every clause of a structured query is a metric
// time series (metricTimeSeriesSources) with no query string. A PromQL query,
// a query with no clause, and every other source - logs, events, APM, bk_data,
// custom names - are not: providers that bucket each point [t, t+step) and
// filter their data by the range's end (Elasticsearch's Lte(end), influxdb's
// GROUP BY time, bk_sql's FLOOR) would lose the end of the window's last
// bucket.
func eligibleForTrim(facts execution.QueryPlanFacts) bool {
	if facts.PromQL != nil || len(facts.QueryList) == 0 {
		return false
	}
	for _, clause := range facts.QueryList {
		if !metricTimeSeriesSources[clause.DataSource] || clause.QueryString != "" {
			return false
		}
	}
	return true
}
