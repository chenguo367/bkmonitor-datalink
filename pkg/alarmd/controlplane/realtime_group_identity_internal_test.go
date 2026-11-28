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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A realtime Query Group is its whole input (016, A5): the items of one
// table read over one channel and period are one group, whatever metrics
// their Plans reference; another table, cluster, topic, source kind or
// period is another group, and so is the tenant's queried group. A retained
// Plan still carrying the input it was compiled with is so a group of its
// own, never one group with two revisions.
func TestARealtimeQueryGroupIsItsWholeInput(t *testing.T) {
	facts := func(change func(*execution.RealtimeInputFacts)) execution.QueryPlanFacts {
		input := execution.RealtimeInputFacts{ResultTableID: "2_bkmonitor_time_series_1", Cluster: "inner", Topic: "realtime_2_1",
			SourceKind: "custom_report", PeriodSeconds: 60}
		if change != nil {
			change(&input)
		}
		return execution.QueryPlanFacts{Provider: execution.ProviderRealtime, TenantID: "tenant-a", BusinessID: "2", Realtime: &input}
	}
	identity := func(facts execution.QueryPlanFacts) execution.QueryGroupIdentity {
		t.Helper()
		derived, err := deriveQueryGroupIdentity(facts)
		if err != nil {
			t.Fatal(err)
		}
		return derived
	}
	base := identity(facts(nil))
	if again := identity(facts(nil)); again != base {
		t.Fatalf("the same input is two groups, %s and %s", base, again)
	}
	for _, other := range []struct {
		name  string
		facts execution.QueryPlanFacts
	}{
		{"another table", facts(func(input *execution.RealtimeInputFacts) { input.ResultTableID = "2_bkmonitor_time_series_2" })},
		{"another cluster", facts(func(input *execution.RealtimeInputFacts) { input.Cluster = "inner-2" })},
		{"another topic", facts(func(input *execution.RealtimeInputFacts) { input.Topic = "realtime_2_1_rebuilt" })},
		{"another source kind", facts(func(input *execution.RealtimeInputFacts) { input.SourceKind = "k8s" })},
		{"another period", facts(func(input *execution.RealtimeInputFacts) { input.PeriodSeconds = 120 })},
		{"the tenant's queried group", execution.QueryPlanFacts{Provider: execution.ProviderUQ, TenantID: "tenant-a", BusinessID: "2"}},
	} {
		if got := identity(other.facts); got == base {
			t.Fatalf("%s is the same group %s", other.name, base)
		}
	}
}
