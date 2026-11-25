// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// multiDetectStrategy is the fixture strategy turned into the container case
// of an item detected over several inputs: CPU used over its limit and
// memory used over its limit, each a percentage, each its own threshold at
// level 1, joined on the host, with its frozen revision.
func multiDetectStrategy(t *testing.T) controlplane.SourceStrategy {
	t.Helper()
	strategy := twoThresholdStrategiesWithDelay(t, 0)[0]
	query := func(alias, field string) map[string]any {
		return map[string]any{"data_source_label": "bk_monitor", "data_type_label": "time_series", "agg_method": "avg",
			"agg_interval": 60, "agg_dimension": []any{"host"}, "agg_condition": []any{}, "result_table_id": "container.usage",
			"metric_field": field, "alias": alias, "functions": []any{}, "unit": "percent"}
	}
	threshold := func(ref string, limit int) map[string]any {
		return map[string]any{"level": 1, "type": "Threshold", "unit_prefix": "", "input_ref": ref,
			"config": []any{[]any{map[string]any{"method": "gte", "threshold": limit}}}}
	}
	// A frozen revision, as the writer stamps every strategy it projects:
	// the item goes out as the standard raw event.
	strategy.Document = editedDocument(t, strategy.Document, func(top map[string]any) { top["strategy_revision"] = 7 }, func(item map[string]any) {
		item["query_configs"] = []any{query("a", "cpu_used"), query("b", "cpu_limit"), query("c", "memory_used"), query("d", "memory_limit")}
		item["query_output_config"] = map[string]any{"response_contract": "multi_detect/v1", "common_dimensions": []any{"host"},
			"inputs": []any{
				map[string]any{"ref": "cpu", "expression": "a / b * 100", "interval": 60, "unit": "percent"},
				map[string]any{"ref": "memory", "expression": "c / d * 100", "interval": 60, "unit": "percent"},
			}}
		item["algorithms"] = []any{threshold("cpu", 90), threshold("memory", 80)}
	})
	return strategy
}

// An item detected over several inputs compiles whole - its inputs, its one
// level of a MultiDetectThreshold for each input, joined strictly - and is
// withheld as a capability this build has
// not turned on, naming what it compiled to; it is no Plan of the Catalog,
// and nothing else is said of it.
func TestAMultiDetectStrategyCompilesWholeAndIsWithheld(t *testing.T) {
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{multiDetectStrategy(t)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range catalog.QueryGroups {
		if len(group.Plans) != 0 {
			t.Fatalf("a withheld multi-input strategy is a Plan of group %s", group.Identity)
		}
	}
	var withheld []controlplane.ObjectDisposition
	for _, disposition := range catalog.Dispositions {
		if disposition.SourceID == "1001" && disposition.Disposition != controlplane.DispositionAccepted {
			withheld = append(withheld, disposition)
		}
	}
	if len(withheld) != 1 {
		t.Fatalf("dispositions %+v, want the strategy withheld once", withheld)
	}
	got := withheld[0]
	if got.Disposition != controlplane.DispositionUnsupported || got.Reason != "MULTI_DETECT_NOT_ENABLED" ||
		got.FieldPath != "items[0].query_output_config" {
		t.Fatalf("disposition %+v, want MULTI_DETECT_NOT_ENABLED on the query output config", got)
	}
	if !strings.Contains(got.Detail, "inputs cpu,memory compiled") || !strings.Contains(got.Detail, "level 1 STRICT_AND MultiDetectThreshold+MultiDetectThreshold") {
		t.Fatalf("detail %q, want both inputs compiled to one strict level of a MultiDetectThreshold each", got.Detail)
	}
	if !strings.Contains(got.Detail, "held: no ready worker is known yet") {
		t.Fatalf("detail %q, want it held for no ready set known", got.Detail)
	}
}

// The item enters the Catalog on a round that admits it - every ready worker
// declares the capability - as a Plan of its first input's Query Group, item
// expression written or not (the writer writes none: each input is compiled
// from its own); on a round with a ready worker that does not declare it, it
// is withheld naming that worker, and the same document is admitted again the
// round after, from the same compiled candidate.
func TestAMultiDetectStrategyIsAdmittedOnlyWhileEveryReadyWorkerDeclaresIt(t *testing.T) {
	strategy := multiDetectStrategy(t)
	strategy.Document = editedDocument(t, strategy.Document, nil, func(item map[string]any) {
		delete(item, "expression")
		delete(item, "expression_unit")
	})
	cache := controlplane.NewCandidateCache()
	build := func(admission controlplane.MultiDetectAdmission) controlplane.Catalog {
		t.Helper()
		catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
			Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{strategy}, Cache: cache, MultiDetect: admission})
		if err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	plans := func(catalog controlplane.Catalog) int {
		count := 0
		for _, group := range catalog.QueryGroups {
			count += len(group.Plans)
		}
		return count
	}
	withheld := func(catalog controlplane.Catalog) []controlplane.ObjectDisposition {
		var found []controlplane.ObjectDisposition
		for _, disposition := range catalog.Dispositions {
			if disposition.SourceID == "1001" && disposition.Reason == "MULTI_DETECT_NOT_ENABLED" {
				found = append(found, disposition)
			}
		}
		return found
	}
	admitted := build(controlplane.MultiDetectAdmission{Admitted: true, Known: true})
	if plans(admitted) != 1 || len(withheld(admitted)) != 0 {
		t.Fatalf("admitted: %d Plans, withheld %+v; want the item's Plan and nothing withheld", plans(admitted), withheld(admitted))
	}
	held := build(controlplane.MultiDetectAdmission{Known: true, Unaware: []string{"worker-old"}})
	if got := withheld(held); plans(held) != 0 || len(got) != 1 ||
		!strings.Contains(got[0].Detail, "held: 1 of the ready workers do not declare multi-detect.v1: worker-old") {
		t.Fatalf("held: %d Plans, withheld %+v; want no Plan and the worker named", plans(held), got)
	}
	if again := build(controlplane.MultiDetectAdmission{Admitted: true, Known: true}); plans(again) != 1 {
		t.Fatalf("admitted again: %d Plans, want the item's Plan back", plans(again))
	}
}

// The same item bound to its inputs but written under another contract is
// refused for the binding, and an item of named outputs is untouched by it.
func TestAnInputRefOutsideAMultiDetectItemIsRefused(t *testing.T) {
	strategy := multiDetectStrategy(t)
	strategy.Document = editedDocument(t, strategy.Document, nil, func(item map[string]any) {
		delete(item, "query_output_config")
		item["query_configs"] = item["query_configs"].([]any)[:1]
		item["expression"] = "a"
	})
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{strategy}})
	if err != nil {
		t.Fatal(err)
	}
	for _, disposition := range catalog.Dispositions {
		if disposition.SourceID == "1001" && disposition.Reason == "ALGORITHM_INPUT_REF_UNEXPECTED" &&
			disposition.Disposition == controlplane.DispositionConfigRejected {
			return
		}
	}
	t.Fatalf("dispositions %+v, want ALGORITHM_INPUT_REF_UNEXPECTED", catalog.Dispositions)
}

// Under the Python-compatible output - every event one value the platform's
// pipeline compares with each threshold - the item is held whatever the
// fleet declares, saying why; the same item goes out as the standard raw
// event, admitted.
func TestAMultiDetectStrategyIsHeldUnderTheCompatibleOutput(t *testing.T) {
	admitted := controlplane.MultiDetectAdmission{Admitted: true, Known: true}
	build := func(protocol string) controlplane.Catalog {
		t.Helper()
		catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
			Planner: globalBusinessPlanner(t), Strategies: []controlplane.SourceStrategy{multiDetectStrategy(t)},
			OutputProtocol: protocol, MultiDetect: admitted})
		if err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	held := false
	for _, disposition := range build("legacy").Dispositions {
		if disposition.SourceID == "1001" && disposition.Reason == "MULTI_DETECT_NOT_ENABLED" &&
			strings.Contains(disposition.Detail, "Python-compatible output carries one value an event") {
			held = true
		}
	}
	if !held {
		t.Fatal("the item under the compatible output was not held saying why")
	}
	accepted := false
	for _, disposition := range build("native").Dispositions {
		if disposition.SourceID == "1001" && disposition.Disposition == controlplane.DispositionAccepted {
			accepted = true
		}
	}
	if !accepted {
		t.Fatal("the item under the standard raw event was not admitted")
	}
}
