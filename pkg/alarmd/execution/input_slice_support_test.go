// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution_test

import (
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// groupedFacts is a structured metric query grouped by dimensions, every
// function aggregating by them.
func groupedFacts(dimensions ...string) execution.QueryPlanFacts {
	return execution.QueryPlanFacts{Provider: execution.ProviderUQ, QueryList: []execution.QueryClause{{
		TableID: "system.cpu_summary", Dimensions: dimensions,
		Functions: []execution.QueryFunction{{Method: "mean", Dimensions: dimensions}},
	}}}
}

// The dimensions a Slot can be sliced on are the raw group-by labels every
// query of it shares, in the order the first query groups by; the reasons a
// Slot cannot be sliced at all are named.
func TestSliceableDimensionsAreTheRawGroupByLabelsEveryQueryShares(t *testing.T) {
	aliased := groupedFacts("host", "region")
	aliased.Normalization.DimensionAliases = map[string]string{"dimensions.region": "region"}
	without := groupedFacts("host", "region")
	without.QueryList[0].Functions = append(without.QueryList[0].Functions, execution.QueryFunction{Method: "sum", Without: true, Dimensions: []string{"host"}})
	promql := groupedFacts("host")
	promql.PromQL = &execution.PromQLQuery{Expression: "sum by (host) (up)"}
	bkdata := groupedFacts("host")
	bkdata.QueryList[0].DataSource = "bkdata"
	events := groupedFacts("host")
	events.SourceSemantics = []string{"custom/event"}
	elastic := groupedFacts("host")
	elastic.TSDBMap = map[string][]execution.QueryStorage{"a": {{StorageType: "elasticsearch"}}}
	custom := groupedFacts("host")
	custom.SourceSemantics = []string{"custom/time_series"}
	for _, test := range []struct {
		name   string
		facts  []execution.QueryPlanFacts
		want   []string
		reason string
	}{
		{name: "shared_in_first_order", facts: []execution.QueryPlanFacts{groupedFacts("region", "host", "zone"), groupedFacts("host", "region")},
			want: []string{"region", "host"}},
		{name: "custom_time_series", facts: []execution.QueryPlanFacts{custom}, want: []string{"host"}},
		{name: "aliased_left_out", facts: []execution.QueryPlanFacts{aliased}, want: []string{"host"}},
		{name: "aggregated_without", facts: []execution.QueryPlanFacts{without}, want: []string{"region"}},
		{name: "none_shared", facts: []execution.QueryPlanFacts{groupedFacts("host"), groupedFacts("region")}, reason: "dimension_not_grouped"},
		{name: "promql_first", facts: []execution.QueryPlanFacts{bkdata, promql}, reason: "promql"},
		{name: "bkdata", facts: []execution.QueryPlanFacts{groupedFacts("host"), bkdata}, reason: "storage_dialect"},
		{name: "events", facts: []execution.QueryPlanFacts{events}, reason: "storage_dialect"},
		{name: "elasticsearch", facts: []execution.QueryPlanFacts{elastic}, reason: "storage_dialect"},
		{name: "no_queries", reason: "storage_dialect"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := execution.SliceableDimensions(test.facts...)
			if test.reason != "" {
				if reason, ok := execution.InputSliceUnsupportedOf(err); !ok || reason != test.reason || got != nil {
					t.Fatalf("SliceableDimensions() = %v, %v; want refused as %s", got, err, test.reason)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("SliceableDimensions() = %v, %v; want %v", got, err, test.want)
			}
		})
	}
}

// A slice is checked against the Slot's dimensions: on one of them it can
// be read, on any other it is refused, and the zero slice is always read.
func TestCheckInputSliceNamesADimensionOutsideTheGroupBy(t *testing.T) {
	facts := []execution.QueryPlanFacts{groupedFacts("host", "region"), groupedFacts("host")}
	on := func(dimension string) execution.InputSlice {
		return execution.InputSlice{Conditions: []execution.InputSliceCondition{{Dimension: dimension, Pattern: "^(?s:.*)a$"}}}
	}
	if err := execution.CheckInputSlice(on("host"), facts...); err != nil {
		t.Fatalf("CheckInputSlice(host) = %v, want it read", err)
	}
	if reason, ok := execution.InputSliceUnsupportedOf(execution.CheckInputSlice(on("region"), facts...)); !ok || reason != "dimension_not_grouped" {
		t.Fatalf("CheckInputSlice(region) refused as %q, want dimension_not_grouped", reason)
	}
	promql := groupedFacts("host")
	promql.PromQL = &execution.PromQLQuery{Expression: "up"}
	if err := execution.CheckInputSlice(execution.InputSlice{}, promql); err != nil {
		t.Fatalf("CheckInputSlice(zero) = %v, want the whole Query Group read", err)
	}
}

// The header's dimensions are its requirements' query facts, the primary
// first; a requirement whose facts it does not carry leaves the store
// undetermined.
func TestTheHeadersSliceableDimensionsReadItsRequirementsFacts(t *testing.T) {
	header := execution.InternalExecutionHeader{
		Requirements: []execution.DataRequirement{
			{Role: execution.InputRoleAlgorithmDependency, LogicalQueryRef: "dependency"},
			{Role: execution.InputRolePrimary, LogicalQueryRef: "primary"},
		},
		QueryFacts: map[execution.LogicalQueryRef]execution.QueryPlanFacts{
			"primary": groupedFacts("host", "region"), "dependency": groupedFacts("region", "host"),
		},
	}
	if got, err := header.SliceableDimensions(); err != nil || !reflect.DeepEqual(got, []string{"host", "region"}) {
		t.Fatalf("SliceableDimensions() = %v, %v; want the primary's order host, region", got, err)
	}
	delete(header.QueryFacts, "dependency")
	if reason, ok := execution.InputSliceUnsupportedOf(func() error { _, err := header.SliceableDimensions(); return err }()); !ok || reason != "storage_dialect" {
		t.Fatalf("SliceableDimensions() without the dependency's facts refused as %q, want storage_dialect", reason)
	}
}
