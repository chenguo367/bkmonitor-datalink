// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

// requestEndTrimSources is the clause data sources that are a metric time
// series: the compilers' empty one and the two names unify-query normalizes
// to bkmonitor. Confirmed by the provider's owners for v3.9.4910 and
// v3.9.4892, the versions deployed: such a table has a VmRt and must-vm-query
// is on by default, so the query is read by DirectQueryRange - VictoriaMetrics
// valuing each point from its own look-back, never through the engine - and
// asking its end a second or a step earlier answered the same points and
// values on a production deployment. No influxdb traffic was read on any of
// them.
var requestEndTrimSources = map[string]bool{"": true, "bkmonitor": true, "bk_monitor": true}

// RequestEndTrimEligible is whether every clause of a structured query is a metric
// time series (requestEndTrimSources) with no query string. A PromQL query,
// a query with no clause, and every other source - logs, events, APM, bk_data,
// custom names - are not: providers that bucket each point [t, t+step) and
// filter their data by the range's end (Elasticsearch's Lte(end), influxdb's
// GROUP BY time, bk_sql's FLOOR) would lose the end of the window's last
// bucket. Such a query is asked its end a second earlier
// (access/uq requestEnd); one that is not is asked its end as it is.
func RequestEndTrimEligible(facts QueryPlanFacts) bool {
	if facts.PromQL != nil || len(facts.QueryList) == 0 {
		return false
	}
	for _, clause := range facts.QueryList {
		if !requestEndTrimSources[clause.DataSource] || clause.QueryString != "" {
			return false
		}
	}
	return true
}
