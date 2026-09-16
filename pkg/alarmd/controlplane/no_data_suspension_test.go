// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
)

// A no-data setting this build cannot compile suspends no-data detection and
// leaves the strategy detecting thresholds.
//
// It used to withhold the whole Plan. The argument was that half a Plan makes
// "is this strategy covered" unanswerable -- but the answer to that is to say
// which half, not to switch off both. A strategy whose threshold detection
// stops because of its no-data settings has lost the detection somebody was
// watching, over a half they may not have known was configured, and the only
// sign is a count of withheld objects.
//
// Driven from the stored document through BuildCatalog, because that chain is
// where the trade was made and each link of it looked reasonable on its own.
func TestANoDataSettingThatCannotCompileSuspendsOnlyNoData(t *testing.T) {
	catalog := buildSuspensionCatalog(t, `{"is_enabled":true,"continuous":"many"}`, nil)

	plan := onlyPlan(t, catalog)
	if plan.Plan.NoData != nil {
		t.Fatalf("no-data is attached: %+v; a section this build cannot compile must not travel to a Slot",
			plan.Plan.NoData)
	}
	if plan.NoDataSuspended != "NO_DATA_CONFIG_INVALID" {
		t.Fatalf("NoDataSuspended = %q, want the reason named. Empty here is how a suspended Plan "+
			"becomes indistinguishable from one that never asked for no-data", plan.NoDataSuspended)
	}
	if len(plan.Plan.StrategyIR.Levels) == 0 {
		t.Fatal("the Plan has no level: the thresholds went with the no-data section, which is the " +
			"whole thing this stops doing")
	}
	wantNotWithheld(t, catalog)
}

// The same for a target shape whose expected set this build cannot derive.
func TestARosterShapeThisBuildCannotDeriveSuspendsOnlyNoData(t *testing.T) {
	// Two host conditions: the backend enumerates only the first condition of
	// the first group, so this is a target alarmd refuses to guess at.
	scope := map[string]any{"target": []any{[]any{
		map[string]any{"field": "bk_target_ip", "method": "eq", "value": []any{map[string]any{"bk_target_ip": "10.0.0.1", "bk_target_cloud_id": "0"}}},
		map[string]any{"field": "bk_target_ip", "method": "eq", "value": []any{map[string]any{"bk_target_ip": "10.0.0.2", "bk_target_cloud_id": "0"}}},
	}}}
	catalog := buildSuspensionCatalog(t,
		`{"is_enabled":true,"continuous":3,"agg_dimension":["bk_target_ip","bk_target_cloud_id"]}`, scope)

	plan := onlyPlan(t, catalog)
	if plan.Plan.NoData != nil || plan.NoDataSuspended != "NO_DATA_ROSTER_UNSUPPORTED" {
		t.Fatalf("no_data=%+v suspended=%q, want the section dropped and the reason named",
			plan.Plan.NoData, plan.NoDataSuspended)
	}
	if len(plan.Plan.StrategyIR.Levels) == 0 {
		t.Fatal("the thresholds went with the roster this build cannot derive")
	}
	wantNotWithheld(t, catalog)
}

// The three states a Plan's no-data detection can be in partition the Plans,
// and each is read from its own field.
//
// This is the assertion that keeps the change honest. A suspended Plan carries
// no no-data section, so deciding "never configured" by the section being
// absent would file every suspended Plan as one that never asked: the
// published family would still add up, and the objects this change exists to
// make visible would be the ones that vanished.
func TestTheNoDataStatesPartitionEveryPlan(t *testing.T) {
	cases := []struct {
		section string
		scope   map[string]any
		want    nodata.RosterSource
	}{
		// No target: the expected set comes from what the item has been seen
		// reporting, which is the history roster.
		{section: `{"is_enabled":true,"continuous":3}`, want: nodata.RosterHistory},
		{section: `{"is_enabled":true,"continuous":"many"}`, want: controlplane.SuspendedNoDataConfigInvalid},
		{section: `null`},
	}
	var plans, configured int
	composition := controlplane.CatalogComposition{}
	for _, test := range cases {
		catalog := buildSuspensionCatalog(t, test.section, test.scope)
		one := controlplane.ComposeCatalog(catalog)
		plans += len(onlyGroup(t, catalog).Plans)
		if test.want != "" {
			configured++
			if got := one.NoDataPlans[test.want]; got != 1 {
				t.Fatalf("section %s landed as %+v, want one under %q", test.section, one.NoDataPlans, test.want)
			}
		}
		composition.NoDataNotConfigured += one.NoDataNotConfigured
		if composition.NoDataPlans == nil {
			composition.NoDataPlans = map[nodata.RosterSource]int{}
		}
		for source, count := range one.NoDataPlans {
			composition.NoDataPlans[source] += count
		}
	}

	var counted int
	for _, count := range composition.NoDataPlans {
		counted += count
	}
	if counted != configured {
		t.Fatalf("the published family holds %d Plans, want the %d that asked for no-data", counted, configured)
	}
	if counted+composition.NoDataNotConfigured != plans {
		t.Fatalf("the three states hold %d Plans and there are %d. A state that is inferred rather "+
			"than read loses objects here and nowhere else",
			counted+composition.NoDataNotConfigured, plans)
	}
}

func buildSuspensionCatalog(t *testing.T, noDataSection string, scope map[string]any) controlplane.Catalog {
	t.Helper()
	item := map[string]any{
		"id": 11, "query_md5": "m", "expression": "a", "name": "item",
		"query_configs": []any{map[string]any{
			"data_source_label": "bk_monitor", "data_type_label": "time_series",
			"result_table_id": "system.cpu", "metric_field": "usage", "agg_method": "AVG",
			"agg_interval": 60, "alias": "a", "agg_dimension": []any{"host"},
		}},
		"algorithms":     []any{map[string]any{"level": 1, "type": "Threshold", "config": []any{map[string]any{"method": "gte", "threshold": 1}}}},
		"no_data_config": json.RawMessage(noDataSection),
	}
	if scope != nil {
		item["target"] = scope["target"]
	}
	document := map[string]any{
		"id": 1001, "bk_biz_id": 2, "update_time": 1700000000, "name": "s", "scenario": "os",
		"detects": []any{map[string]any{"level": 1, "trigger_config": map[string]any{"check_window": 1, "count": 1}}},
		"items":   []any{item},
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := controlplane.NewLegacyPrimaryQueryCompiler("uq-primary-v1", "UTC", testLegacyQueryRuntimeFacts())
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Strategies: []controlplane.SourceStrategy{{SourceID: "1001", Document: encoded,
			Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}}},
		Planner: planner,
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func onlyGroup(t *testing.T, catalog controlplane.Catalog) controlplane.QueryGroup {
	t.Helper()
	if len(catalog.QueryGroups) != 1 {
		t.Fatalf("groups = %d, dispositions = %+v; the strategy did not compile at all",
			len(catalog.QueryGroups), catalog.Dispositions)
	}
	return catalog.QueryGroups[0]
}

func onlyPlan(t *testing.T, catalog controlplane.Catalog) controlplane.FrozenPlan {
	t.Helper()
	group := onlyGroup(t, catalog)
	if len(group.Plans) != 1 {
		t.Fatalf("plans = %d, want one", len(group.Plans))
	}
	return group.Plans[0]
}

func wantNotWithheld(t *testing.T, catalog controlplane.Catalog) {
	t.Helper()
	for _, disposition := range catalog.Dispositions {
		if disposition.Disposition != controlplane.DispositionAccepted {
			t.Fatalf("the strategy is filed as %+v; a suspended no-data half is not a withheld object, "+
				"and counting it as one is what took the thresholds down with it", disposition)
		}
	}
}
