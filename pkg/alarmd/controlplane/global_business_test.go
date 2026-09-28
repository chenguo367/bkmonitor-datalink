// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// editedDocument returns the fixture document with edits applied to its
// decoded top level and to its first item.
func editedDocument(t *testing.T, document json.RawMessage, top func(map[string]any), item func(map[string]any)) json.RawMessage {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(document, &value); err != nil {
		t.Fatal(err)
	}
	if top != nil {
		top(value)
	}
	if item != nil {
		items, ok := value["items"].([]any)
		if !ok || len(items) == 0 {
			t.Fatal("fixture strategy carries no items")
		}
		first, ok := items[0].(map[string]any)
		if !ok {
			t.Fatal("fixture item is not an object")
		}
		item(first)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// is_global_strategy is read strictly: absent is false, a JSON boolean is
// itself, and anything else refuses the strategy by name - never read as
// false, which would run a global strategy scoped to one business's space
// with nothing to show for it. A refused strategy does not take its
// healthy sibling with it. The switch is the strategy's own field: the
// business-level name an earlier draft used is not read as it.
func TestTheStrategySourceReadsTheGlobalSwitchStrictly(t *testing.T) {
	for name, test := range map[string]struct {
		key     string
		value   any
		present bool
		global  bool
		refused bool
	}{
		"absent":             {},
		"true":               {value: true, present: true, global: true},
		"false":              {value: false, present: true},
		"null":               {value: nil, present: true, refused: true},
		"string":             {value: "true", present: true, refused: true},
		"number":             {value: 1, present: true, refused: true},
		"business-level key": {key: "is_global_biz", value: true, present: true},
	} {
		t.Run(name, func(t *testing.T) {
			client := newControlplaneRedis(t)
			ctx := context.Background()
			documents := realThresholdDocuments(t)
			tested := withWireIdentity(t, documents[0], "tenant-a", "bkcc__2")
			if test.present {
				key := test.key
				if key == "" {
					key = "is_global_strategy"
				}
				tested = editedDocument(t, tested, func(top map[string]any) { top[key] = test.value }, nil)
			}
			healthy := withWireIdentity(t, documents[1], "tenant-a", "bkcc__2")
			for id, payload := range map[string]json.RawMessage{"1001": tested, "1002": healthy} {
				if err := client.Set(ctx, "bkmonitor.cache.strategy_"+id, string(payload), 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
			if err != nil {
				t.Fatal(err)
			}
			strategies, err := source.Strategies(ctx, []string{"1001", "1002"})
			if err != nil || len(strategies) != 2 {
				t.Fatalf("Strategies() = (%d, %v)", len(strategies), err)
			}
			got := strategies[0]
			if test.refused {
				if got.SourceDisposition == nil || got.SourceDisposition.Disposition != controlplane.DispositionConfigRejected ||
					got.SourceDisposition.Reason != controlplane.ReasonGlobalStrategyInvalid || got.SourceDisposition.FieldPath != "is_global_strategy" {
					t.Fatalf("disposition = %+v, want %s at is_global_strategy", got.SourceDisposition, controlplane.ReasonGlobalStrategyInvalid)
				}
			} else {
				want := controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2", GlobalBusiness: test.global}
				if got.SourceDisposition != nil || got.Identity != want {
					t.Fatalf("strategy = %+v, want identity %+v", got, want)
				}
			}
			if strategies[1].SourceDisposition != nil || strategies[1].Identity.GlobalBusiness {
				t.Fatalf("the healthy sibling = %+v", strategies[1])
			}
		})
	}
}

func globalBusinessPlanner(t *testing.T) controlplane.PrimaryQueryCompiler {
	t.Helper()
	planner, err := controlplane.NewLegacyPrimaryQueryCompiler("uq-primary-v1", "UTC", testLegacyQueryRuntimeFacts())
	if err != nil {
		t.Fatal(err)
	}
	return planner
}

// A global business Plan and an ordinary Plan of the same business that ask
// the same query are two queries: one skips the space and one is scoped to
// it. They build two Query Groups, each Plan in its own, and each carries
// the flag it was read with - the query facts and the frozen Plan.
func TestBuildCatalogSeparatesAGlobalBusinessPlanFromAnOrdinaryOne(t *testing.T) {
	strategies := twoThresholdStrategiesWithDelay(t, 0)
	for index := range strategies {
		// A revision, so both publish the standard event: without one the
		// global Plan would publish the compatible event and be refused.
		strategies[index].Document = editedDocument(t, strategies[index].Document, func(top map[string]any) { top["strategy_revision"] = 7 }, nil)
	}
	strategies[1].Identity.GlobalBusiness = true
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Planner: globalBusinessPlanner(t), Strategies: strategies})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.QueryGroups) != 2 {
		t.Fatalf("groups = %d, want the global and the ordinary query apart", len(catalog.QueryGroups))
	}
	seen := map[string]bool{}
	for _, group := range catalog.QueryGroups {
		if len(group.Plans) != 1 {
			t.Fatalf("group %s holds %d Plans", group.Identity, len(group.Plans))
		}
		plan := group.Plans[0]
		global := plan.Identity.StrategyID == "1002"
		if group.QueryPlan.GlobalBusiness != global || plan.Plan.GlobalBusiness != global {
			t.Fatalf("strategy %s: query facts global=%t, Plan global=%t, want %t",
				plan.Identity.StrategyID, group.QueryPlan.GlobalBusiness, plan.Plan.GlobalBusiness, global)
		}
		if plan.Identity.BusinessID != "2" || group.QueryPlan.SpaceScope != "bkcc__2" {
			t.Fatalf("strategy %s: identity %+v, space %q: the Plan's business and space stay its own",
				plan.Identity.StrategyID, plan.Identity, group.QueryPlan.SpaceScope)
		}
		seen[plan.Identity.StrategyID] = true
	}
	if !seen["1001"] || !seen["1002"] {
		t.Fatalf("Plans built: %v", seen)
	}
}

// Each condition a global business strategy must meet refuses it by name,
// with the condition in the detail; the same strategy as an ordinary one
// compiles, so the refusal is the global flag's and not the fixture's.
func TestAGlobalBusinessStrategyIsWithheldWhereItCannotRunAsOne(t *testing.T) {
	for name, test := range map[string]struct {
		item     func(map[string]any)
		protocol string
		reason   string
	}{
		"an old-form target": {
			item: func(item map[string]any) {
				item["target"] = []any{[]any{map[string]any{"field": "bk_target_ip", "method": "eq",
					"value": []any{map[string]any{"bk_target_ip": "192.0.2.1", "bk_target_cloud_id": 0}}}}}
			},
			reason: controlplane.GlobalBusinessLegacyTarget,
		},
		"a PromQL query": {
			item: func(item map[string]any) {
				item["query_configs"] = []any{map[string]any{"data_source_label": "prometheus", "data_type_label": "time_series",
					"promql": "sum by (host) (up)", "agg_interval": 60}}
			},
			reason: controlplane.GlobalBusinessQueryKind,
		},
		"an event query": {
			item: func(item map[string]any) {
				item["query_configs"] = []any{map[string]any{"data_source_label": "custom", "data_type_label": "event",
					"result_table_id": "k8s_event", "custom_event_name": "PodError", "agg_interval": 60, "agg_dimension": []any{"namespace"}}}
			},
			reason: controlplane.GlobalBusinessQueryKind,
		},
		"a query naming no table": {
			item: func(item map[string]any) {
				configs := item["query_configs"].([]any)
				configs[0].(map[string]any)["result_table_id"] = ""
			},
			reason: controlplane.GlobalBusinessQueryTable,
		},
		"the compatible output": {
			protocol: "legacy",
			reason:   controlplane.GlobalBusinessOutputProtocol,
		},
	} {
		t.Run(name, func(t *testing.T) {
			base := twoThresholdStrategiesWithDelay(t, 0)[0]
			base.Document = editedDocument(t, base.Document, func(top map[string]any) { top["strategy_revision"] = 7 }, test.item)
			build := func(global bool) controlplane.Catalog {
				strategy := base
				strategy.Identity.GlobalBusiness = global
				catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
					Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{strategy}, OutputProtocol: test.protocol})
				if err != nil {
					t.Fatal(err)
				}
				return catalog
			}
			ordinary := build(false)
			if len(ordinary.QueryGroups) != 1 {
				t.Fatalf("the ordinary strategy did not compile: %+v", ordinary.Dispositions)
			}
			global := build(true)
			if len(global.QueryGroups) != 0 {
				t.Fatalf("the global strategy compiled into %d groups", len(global.QueryGroups))
			}
			found := false
			for _, disposition := range global.Dispositions {
				if disposition.Reason == controlplane.ReasonGlobalBusinessUnsupported {
					found = true
					if disposition.Disposition != controlplane.DispositionUnsupported || disposition.Detail != "reason="+test.reason {
						t.Fatalf("disposition = %+v, want UNSUPPORTED with reason=%s", disposition, test.reason)
					}
				}
			}
			if !found {
				t.Fatalf("dispositions %+v name no %s", global.Dispositions, controlplane.ReasonGlobalBusinessUnsupported)
			}
		})
	}
}

// What a global business strategy may be: no target or a target plan, a
// structured time series query over the metric router - the platform's own
// or a custom one - naming its table.
func TestAGlobalBusinessStrategyThatMeetsTheConditionsCompiles(t *testing.T) {
	for name, item := range map[string]func(map[string]any){
		"no target": nil,
		"a Kubernetes target plan": func(item map[string]any) {
			item["target_plan"] = map[string]any{"schema_version": 1, "model_id": "cw-K8s_Cluster", "target_rule": "k8s_cluster",
				"failure_policy": "no_match", "dynamic_groups": []any{}, "dynamic_topologies": []any{},
				"static_targets": []any{map[string]any{"model_id": "cw-K8s_Cluster", "model_inst_id": "cluster-a",
					"match": map[string]any{"bcs_cluster_id": "cluster-a"}, "bk_biz_id": 11}}}
		},
		"a custom time series": func(item map[string]any) {
			configs := item["query_configs"].([]any)
			configs[0].(map[string]any)["data_source_label"] = "custom"
			configs[0].(map[string]any)["result_table_id"] = "2_bkmonitor_time_series_100.__default__"
		},
	} {
		t.Run(name, func(t *testing.T) {
			strategy := twoThresholdStrategiesWithDelay(t, 0)[0]
			strategy.Document = editedDocument(t, strategy.Document, func(top map[string]any) { top["strategy_revision"] = 7 }, item)
			strategy.Identity.GlobalBusiness = true
			catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
				Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{strategy}})
			if err != nil {
				t.Fatal(err)
			}
			if len(catalog.QueryGroups) != 1 {
				t.Fatalf("groups = %d, dispositions %+v", len(catalog.QueryGroups), catalog.Dispositions)
			}
			plan := catalog.QueryGroups[0].Plans[0].Plan
			if !plan.GlobalBusiness || !catalog.QueryGroups[0].QueryPlan.GlobalBusiness {
				t.Fatalf("Plan global=%t, facts global=%t", plan.GlobalBusiness, catalog.QueryGroups[0].QueryPlan.GlobalBusiness)
			}
			if strings.HasPrefix(name, "a Kubernetes") {
				want := map[string]string{"cluster-a": "11"}
				if plan.TargetPlan == nil || !reflect.DeepEqual(plan.TargetPlan.StaticBusinesses, want) {
					t.Fatalf("target plan = %+v, want the static target's business frozen", plan.TargetPlan)
				}
			}
		})
	}
}
