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
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// multiDetectQuery is one PromQL query config of a multi-input item: a part
// of an expression (expression_mode promql), aggregated by namespace and pod
// at interval.
func multiDetectQuery(alias, promql string, interval int, dimensions ...string) json.RawMessage {
	if len(dimensions) == 0 {
		dimensions = []string{"pod", "namespace"}
	}
	encoded, _ := json.Marshal(map[string]any{"data_source_label": "prometheus", "data_type_label": "time_series",
		"expression_mode": "promql", "alias": alias, "promql": promql, "agg_interval": interval, "agg_dimension": dimensions})
	return encoded
}

// multiDetectSource is the container case a multi-input item is made for:
// CPU used over its limit, and memory used over its limit, each a
// percentage of its own two queries, joined on namespace and pod.
func multiDetectSource(config string) PrimaryQuerySource {
	return PrimaryQuerySource{
		Identity: SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}, StrategyID: "106",
		ItemID: "1", QueryMD5: "query-106",
		QueryConfigs: []json.RawMessage{
			multiDetectQuery("a", "sum by (namespace, pod) (cpu_used)", 60), multiDetectQuery("b", "sum by (namespace, pod) (cpu_limit)", 60),
			multiDetectQuery("c", "sum by (namespace, pod) (memory_used)", 60), multiDetectQuery("d", "sum by (namespace, pod) (memory_limit)", 60),
		},
		QueryOutputConfig: json.RawMessage(config),
	}
}

const multiDetectContainerConfig = `{"response_contract":"multi_detect/v1","common_dimensions":["namespace","pod"],"inputs":[
	{"ref":"cpu_limit_usage","display_name":"CPU limit 使用率","expression":"a / b * 100","interval":60,"unit":"percent"},
	{"ref":"mem_limit_usage","expression":"c / d * 100","interval":60,"unit":"percent"}]}`

// Each input is its own query - the query configs its expression names and
// the expression over them - in the order written, on one grid; an input the
// writer gave no display name is shown by its reference name.
func TestAMultiDetectItemCompilesEachInputAsItsOwnQuery(t *testing.T) {
	detected, err := namedOutputsTestCompiler(t).CompileMultiDetect(context.Background(), multiDetectSource(multiDetectContainerConfig))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(detected.CommonDimensions, []string{"namespace", "pod"}) || len(detected.Inputs) != 2 {
		t.Fatalf("detected %+v, want two inputs joined on namespace and pod", detected)
	}
	cpu, memory := detected.Inputs[0], detected.Inputs[1]
	if cpu.Ref != "cpu_limit_usage" || cpu.DisplayName != "CPU limit 使用率" || cpu.Unit != "percent" ||
		memory.Ref != "mem_limit_usage" || memory.DisplayName != "mem_limit_usage" || memory.Unit != "percent" {
		t.Fatalf("inputs %+v / %+v, want each as written, the second shown by its reference name", cpu, memory)
	}
	for _, input := range detected.Inputs {
		if err := input.Facts.Validate(); err != nil {
			t.Fatalf("input %s: compiled facts do not validate: %v", input.Ref, err)
		}
		if input.Facts.PromQL == nil || input.Facts.NamedOutputs != nil {
			t.Fatalf("input %s: facts %+v, want one PromQL of its own and no named outputs", input.Ref, input.Facts)
		}
	}
	if got := cpu.Facts.PromQL.Expression; !strings.Contains(got, "cpu_used") || !strings.Contains(got, "cpu_limit") || strings.Contains(got, "memory") {
		t.Fatalf("the CPU input asks %q, want its own two queries and only those", got)
	}
	if got := memory.Facts.PromQL.Expression; !strings.Contains(got, "memory_used") || !strings.Contains(got, "memory_limit") || strings.Contains(got, "cpu") {
		t.Fatalf("the memory input asks %q, want its own two queries and only those", got)
	}
	if !sameMultiDetectStep(cpu.Facts, memory.Facts) || cpu.Facts.StepMillis != 60_000 {
		t.Fatalf("steps %d / %d, want both inputs on one 60 s grid", cpu.Facts.StepMillis, memory.Facts.StepMillis)
	}
	if cpu.Facts.QueryRevision == memory.Facts.QueryRevision {
		t.Fatal("the two inputs compiled to one query")
	}
}

// An item the contract does not admit is refused by the name of what is
// wrong with it, before any of it is detected.
func TestAMultiDetectItemIsRefusedByName(t *testing.T) {
	input := func(ref, expression string, interval int) string {
		return `{"ref":"` + ref + `","expression":"` + expression + `","interval":` + itoa(interval) + `,"unit":"percent"}`
	}
	config := func(common string, inputs ...string) string {
		return `{"response_contract":"multi_detect/v1","common_dimensions":` + common + `,"inputs":[` + strings.Join(inputs, ",") + `]}`
	}
	both := []string{input("x", "a / b * 100", 60), input("y", "c / d * 100", 60)}
	cases := []struct {
		name, config, reason string
		edit                 func(*PrimaryQuerySource)
	}{
		{"another contract", strings.Replace(config(`["namespace","pod"]`, both...), "multi_detect/v1", "multi_detect/v2", 1), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"one input", config(`["namespace","pod"]`, input("x", "a / b + c / d", 60)), "MULTI_DETECT_INPUT_COUNT", nil},
		{"five inputs", config(`["namespace","pod"]`, input("v", "a", 60), input("w", "b", 60), input("x", "c", 60), input("y", "d", 60), input("z", "a + b", 60)), "MULTI_DETECT_INPUT_COUNT", nil},
		{"an input interval other than its queries'", config(`["namespace","pod"]`, input("x", "a / b * 100", 120), both[1]), "MULTI_DETECT_INTERVAL_MISMATCH", nil},
		{"inputs on two intervals", config(`["namespace","pod"]`, both...), "MULTI_DETECT_INTERVAL_MISMATCH", func(source *PrimaryQuerySource) {
			source.QueryConfigs[2] = multiDetectQuery("c", "sum by (namespace, pod) (memory_used)", 120)
			source.QueryConfigs[3] = multiDetectQuery("d", "sum by (namespace, pod) (memory_limit)", 120)
			source.QueryOutputConfig = json.RawMessage(config(`["namespace","pod"]`, both[0], input("y", "c / d * 100", 120)))
		}},
		{"common dimensions other than the queries'", config(`["namespace","pod","container"]`, both...), "MULTI_DETECT_DIMENSIONS_MISMATCH", nil},
		{"a query aggregated otherwise", config(`["namespace","pod"]`, both...), "MULTI_DETECT_DIMENSIONS_MISMATCH", func(source *PrimaryQuerySource) {
			source.QueryConfigs[1] = multiDetectQuery("b", "sum by (namespace) (cpu_limit)", 60, "namespace")
		}},
		{"no common dimensions", config(`[]`, both...), "MULTI_DETECT_DIMENSIONS_MISMATCH", nil},
		{"common dimensions not a list", config(`"pod"`, both...), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"a common dimension twice", config(`["pod","pod","namespace"]`, both...), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"one reference twice", config(`["namespace","pod"]`, input("x", "a / b * 100", 60), input("x", "c / d * 100", 60)), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"a reference that is no name", config(`["namespace","pod"]`, input("1x", "a / b * 100", 60), both[1]), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"an input without its expression", config(`["namespace","pod"]`, input("x", "", 60), both[1]), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"an input that names no query", config(`["namespace","pod"]`, input("x", "e * 100", 60), both[1]), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"a query no input reads", config(`["namespace","pod"]`, input("x", "a / b * 100", 60), input("y", "c", 60)), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"an expression over one PromQL query", config(`["namespace","pod"]`, input("x", "a / b * 100", 60), input("y", "c * 100", 60), input("z", "d", 60)), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"an interval that is no number", strings.Replace(config(`["namespace","pod"]`, both...), `"interval":60`, `"interval":"60"`, 1), "MULTI_DETECT_CONFIG_INVALID", nil},
		{"item functions", config(`["namespace","pod"]`, both...), "MULTI_DETECT_CONFIG_INVALID", func(source *PrimaryQuerySource) {
			source.Functions = []json.RawMessage{json.RawMessage(`{"id":"abs","params":[]}`)}
		}},
	}
	compiler := namedOutputsTestCompiler(t)
	for _, test := range cases {
		source := multiDetectSource(test.config)
		if test.edit != nil {
			test.edit(&source)
		}
		_, err := compiler.CompileMultiDetect(context.Background(), source)
		var refusal *QueryPlanCompileError
		if !errors.As(err, &refusal) || refusal.Reason != test.reason || refusal.Disposition != DispositionConfigRejected {
			t.Errorf("%s: err = %v, want %s", test.name, err, test.reason)
		}
	}
}

func itoa(value int) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// multiDetectItem is the container case's item with its algorithms: one
// Threshold for each input at level 1.
func multiDetectItem() legacyItem {
	threshold := json.RawMessage(`[[{"method":"gt","threshold":90}]]`)
	return legacyItem{ID: 1, Algorithms: []legacyAlgorithm{
		{Level: 1, Type: "Threshold", Config: threshold, InputRef: "cpu_limit_usage"},
		{Level: 1, Type: "Threshold", Config: threshold, InputRef: "mem_limit_usage"},
	}}
}

// A valid multi-input item is read and checked whole, and comes back as its
// inputs: what the Plan compile reads next (buildCandidate, which then
// withholds it as MULTI_DETECT_NOT_ENABLED).
func TestAValidMultiDetectItemComesBackAsItsInputs(t *testing.T) {
	detected, refusal, err := compileMultiDetectItem(context.Background(), namedOutputsTestCompiler(t), "106", multiDetectItem(),
		multiDetectSource(multiDetectContainerConfig))
	if err != nil || refusal != nil {
		t.Fatalf("refusal %+v err %v, want the item read whole", refusal, err)
	}
	if len(detected.Inputs) != 2 || detected.Inputs[0].Ref != "cpu_limit_usage" || detected.Inputs[1].Ref != "mem_limit_usage" {
		t.Fatalf("inputs %+v, want the two as written", detected.Inputs)
	}
}

// The algorithms of a multi-input item are checked against its inputs: one
// level, thresholds only, each bound to an input and every input bound, and
// no no-data detection. An algorithm bound to an input on an item of
// another contract is refused too.
func TestTheAlgorithmsOfAMultiDetectItemAreCheckedAgainstItsInputs(t *testing.T) {
	cases := []struct {
		name, reason string
		edit         func(*legacyItem)
	}{
		{"two levels", "MULTI_DETECT_LEVELS_UNSUPPORTED", func(item *legacyItem) { item.Algorithms[1].Level = 2 }},
		{"another algorithm", "MULTI_DETECT_ALGORITHM_UNSUPPORTED", func(item *legacyItem) { item.Algorithms[0].Type = "SimpleRingRatio" }},
		{"an algorithm bound to nothing", "MULTI_DETECT_ALGORITHM_UNBOUND", func(item *legacyItem) { item.Algorithms[0].InputRef = "" }},
		{"an algorithm bound to no input", "MULTI_DETECT_ALGORITHM_UNBOUND", func(item *legacyItem) { item.Algorithms[0].InputRef = "disk_usage" }},
		{"an input no algorithm detects", "MULTI_DETECT_ALGORITHM_UNBOUND", func(item *legacyItem) { item.Algorithms = item.Algorithms[:1] }},
		{"two thresholds for one input", "MULTI_DETECT_ALGORITHM_UNSUPPORTED", func(item *legacyItem) {
			item.Algorithms = append(item.Algorithms, legacyAlgorithm{Level: 1, Type: "Threshold", Config: item.Algorithms[0].Config, InputRef: "cpu_limit_usage"})
		}},
		{"no-data detection", "MULTI_DETECT_NO_DATA_UNSUPPORTED", func(item *legacyItem) {
			item.NoDataConfig = json.RawMessage(`{"is_enabled":true,"continuous":5}`)
		}},
	}
	compiler := namedOutputsTestCompiler(t)
	for _, test := range cases {
		item := multiDetectItem()
		test.edit(&item)
		_, refusal, err := compileMultiDetectItem(context.Background(), compiler, "106", item, multiDetectSource(multiDetectContainerConfig))
		if err == nil || refusal == nil || refusal.Reason != test.reason || refusal.Disposition != DispositionConfigRejected {
			t.Errorf("%s: refusal %+v err %v, want %s", test.name, refusal, err, test.reason)
		}
	}
	other := multiDetectItem()
	if refusal := algorithmInputRefRefusal("107", other); refusal == nil || refusal.Reason != "ALGORITHM_INPUT_REF_UNEXPECTED" ||
		refusal.FieldPath != "items[0].algorithms[0].input_ref" {
		t.Fatalf("refusal %+v, want ALGORITHM_INPUT_REF_UNEXPECTED on the first algorithm", refusal)
	}
	other.Algorithms[0].InputRef, other.Algorithms[1].InputRef = "", ""
	if refusal := algorithmInputRefRefusal("107", other); refusal != nil {
		t.Fatalf("an item without input_ref refused: %+v", refusal)
	}
}

// The contract is read off the config only as far as its name: an item of
// named outputs, of none, or of a config that does not decode, is not a
// multi-input item, and the named outputs compile answers for it as before.
func TestOnlyAMultiDetectConfigIsReadAsOne(t *testing.T) {
	for config, want := range map[string]string{
		multiDetectContainerConfig:                                         execution.MultiDetectContract,
		`{"response_contract":" multi_detect/v1 "}`:                        execution.MultiDetectContract,
		`{"response_contract":"named_outputs/v1","legacy_output_ref":"A"}`: execution.NamedOutputsContract,
		``:                        "",
		`null`:                    "",
		`{"response_contract":7}`: "",
		`[`:                       "",
	} {
		if got := queryOutputContractOf(json.RawMessage(config)); got != want {
			t.Errorf("config %q read as %q, want %q", config, got, want)
		}
	}
}

// An item whose queries are not all time series is refused: an event or log
// count reads from earlier than its window, and another input would not be
// read over the same one.
func TestAMultiDetectItemOfEventsOrLogsIsRefused(t *testing.T) {
	item := multiDetectItem()
	item.QueryConfigs = []json.RawMessage{json.RawMessage(`{"data_type_label":"time_series"}`), json.RawMessage(`{"data_type_label":"event"}`)}
	_, refusal, err := compileMultiDetectItem(context.Background(), namedOutputsTestCompiler(t), "106", item,
		multiDetectSource(multiDetectContainerConfig))
	if err == nil || refusal == nil || refusal.Reason != "MULTI_DETECT_DATA_TYPE_UNSUPPORTED" ||
		refusal.FieldPath != "items[0].query_configs[1].data_type_label" {
		t.Fatalf("refusal %+v err %v, want MULTI_DETECT_DATA_TYPE_UNSUPPORTED on the event query", refusal, err)
	}
}

// Two inputs read on one grid only when they also agree on the end they ask:
// a metric time series asks its end a second early, a PromQL its end as it
// is, and the two would not be asked one end.
func TestInputsAskingAnotherEndAreNotOnOneGrid(t *testing.T) {
	detected, err := namedOutputsTestCompiler(t).CompileMultiDetect(context.Background(), multiDetectSource(multiDetectContainerConfig))
	if err != nil {
		t.Fatal(err)
	}
	promql := detected.Inputs[0].Facts
	structured := promql
	structured.PromQL = nil
	structured.QueryList = []execution.QueryClause{{DataSource: "bkmonitor", ReferenceName: "a"}}
	if !sameMultiDetectStep(promql, detected.Inputs[1].Facts) {
		t.Fatal("two PromQL inputs at one interval are not on one grid")
	}
	if execution.RequestEndTrimEligible(promql) == execution.RequestEndTrimEligible(structured) || sameMultiDetectStep(promql, structured) {
		t.Fatal("a PromQL input and a metric time series input were taken as asking one end")
	}
}

// The inputs after the first are read as requirements of their own, each a
// query of its own over the PRIMARY's own window, step and alignment, and
// each query is among the Plan's.
func TestTheInputsAfterTheFirstAreReadOverThePrimarysWindow(t *testing.T) {
	detected, err := namedOutputsTestCompiler(t).CompileMultiDetect(context.Background(), multiDetectSource(multiDetectContainerConfig))
	if err != nil {
		t.Fatal(err)
	}
	inputs := compiledPlanInputs{primary: detected.Inputs[0].Facts, multiDetect: detected.Inputs[1:]}
	templates, err := inputs.buildRequirements(strategy.DetectorKindThreshold, 1, 60, 60, execution.InputProjection{ValueFields: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(templates) != 2 {
		t.Fatalf("requirements %+v, want the PRIMARY and the second input", templates)
	}
	primary, peer := templates[0], templates[1]
	if primary.Role != execution.InputRolePrimary || peer.Role != execution.InputRoleMultiDetectInput ||
		peer.DatasetName != "input:mem_limit_usage" {
		t.Fatalf("requirements %+v / %+v, want the PRIMARY and the memory input as a peer", primary, peer)
	}
	if peer.RelativeWindow != primary.RelativeWindow || peer.StepMillis != primary.StepMillis || peer.AlignmentMillis != primary.AlignmentMillis ||
		peer.ReadinessClass != primary.ReadinessClass || peer.ResultWindowPolicy != primary.ResultWindowPolicy {
		t.Fatalf("the peer reads %+v, the PRIMARY %+v: want one window, step and alignment", peer, primary)
	}
	peerRef := execution.LogicalQueryRef(detected.Inputs[1].Facts.QueryRevision)
	if peer.LogicalQueryRef != peerRef || inputs.queryPlans[peerRef].QueryRevision != detected.Inputs[1].Facts.QueryRevision ||
		peer.LogicalQueryRef == primary.LogicalQueryRef {
		t.Fatalf("the peer reads %s, want its own query %s among the Plan's", peer.LogicalQueryRef, peerRef)
	}
}

// A Slot of an item detected over several inputs is sliced on a dimension
// every input groups by, so a key's series of each input land in one slice:
// its candidates are within the common dimensions (015 F1 section 8). Every
// query of every input is aggregated by exactly them, so the intersection the
// slicer takes is in them already.
func TestAMultiDetectItemIsSlicedOnlyOnItsCommonDimensions(t *testing.T) {
	query := func(alias, field string) json.RawMessage {
		encoded, _ := json.Marshal(map[string]any{"data_source_label": "bk_monitor", "data_type_label": "time_series",
			"agg_method": "avg", "agg_interval": 60, "agg_dimension": []string{"namespace", "pod"}, "agg_condition": []any{},
			"result_table_id": "container.usage", "metric_field": field, "alias": alias, "functions": []any{}, "unit": "percent"})
		return encoded
	}
	source := multiDetectSource(multiDetectContainerConfig)
	source.QueryConfigs = []json.RawMessage{query("a", "cpu_used"), query("b", "cpu_limit"), query("c", "memory_used"), query("d", "memory_limit")}
	detected, err := namedOutputsTestCompiler(t).CompileMultiDetect(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	facts := make([]execution.QueryPlanFacts, 0, len(detected.Inputs))
	for _, input := range detected.Inputs {
		facts = append(facts, input.Facts)
	}
	candidates, err := execution.SliceableDimensions(facts...)
	if err != nil {
		t.Fatalf("the item's Slot cannot be sliced: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("no candidate dimension, want the common ones")
	}
	for _, dimension := range candidates {
		if !slices.Contains(detected.CommonDimensions, dimension) {
			t.Fatalf("candidates %v, want them within the common dimensions %v", candidates, detected.CommonDimensions)
		}
	}
}
