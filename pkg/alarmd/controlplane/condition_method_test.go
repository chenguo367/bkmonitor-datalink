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
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const conditionMethodBKDataConfig = `{"data_source_label":"bk_data","data_type_label":"time_series","result_table_id":"2_system_cpu","metric_field":"usage","alias":"a","agg_method":"AVG","agg_interval":60,"agg_dimension":["host"],"agg_condition":%s}`

// A metric query is read by VictoriaMetrics or InfluxDB, whose renderer
// takes an operator it does not know for equality (UQ
// query/structured/condition_field.go ToPromOperator): a condition written
// with gt would select the series equal to its value, and the strategy would
// run on the wrong series with nothing said. Every method the strategy page
// offers has its operator (eq, neq, include, exclude, reg, nreg), and UQ's
// own operator names the renderer reads go through as written, as the
// platform sends them (strategies in service write req); any other - a
// comparison, an existence check, a word misspelt - refuses the strategy by
// name.
func TestAMetricConditionWithAMethodTheStoreWouldReadAsEqualityIsRefused(t *testing.T) {
	for _, method := range []string{"gt", "gte", "lt", "lte", "exists", "nexisted", "is", "EQ", "regex"} {
		_, err := compileWithConditions(t, emptyConditionMetricConfig, fmt.Sprintf(`[{"key":"host","method":%q,"value":["a"]}]`, method))
		var failure *QueryPlanCompileError
		if !errors.As(err, &failure) || failure.Disposition != DispositionConfigRejected || failure.Reason != "QUERY_CONDITION_METHOD_UNSUPPORTED" {
			t.Errorf("method %q: %v, want the strategy refused as QUERY_CONDITION_METHOD_UNSUPPORTED", method, err)
		}
		if operator, known := MetricConditionOperator(method); known {
			t.Errorf("MetricConditionOperator(%q) = %q, a method the compiler refuses", method, operator)
		}
	}
	for method, operator := range map[string]string{"eq": "contains", "neq": "ncontains", "include": "req", "exclude": "nreq", "reg": "req", "nreg": "nreq",
		"ne": "ne", "req": "req", "nreq": "nreq", "contains": "contains", "ncontains": "ncontains"} {
		facts, err := compileWithConditions(t, emptyConditionMetricConfig, fmt.Sprintf(`[{"key":"host","method":%q,"value":["a"]}]`, method))
		if err != nil {
			t.Fatalf("method %q: %v, want it compiled", method, err)
		}
		if got := compiledOperators(facts); len(got) != 1 || got[0] != operator {
			t.Errorf("method %q compiled to %v, want [%s]", method, got, operator)
		}
		// The table read outside the compiler is the one it compiles by.
		if got, known := MetricConditionOperator(method); !known || got != operator {
			t.Errorf("MetricConditionOperator(%q) = %q, %v; the compiler compiles it to %s", method, got, known, operator)
		}
	}
}

// The computing platform's tables are read by BkSql, which compares with
// gt, gte, lt and lte as written (UQ tsdb/bksql/sql_expr/base.go): those are
// its operators too, and a method neither knows is refused the same way.
func TestABKDataConditionComparesAndRefusesAMethodNoStoreKnows(t *testing.T) {
	// A computing platform query compiles with an expression: one read
	// whole is the platform's local-time form, not migrated
	// (TestPollingBKDataLocalTimeBoundary).
	compile := func(conditions string) (execution.QueryPlanFacts, error) {
		planner, err := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
		if err != nil {
			t.Fatal(err)
		}
		source := pollingTestSource(fmt.Sprintf(conditionMethodBKDataConfig, conditions))
		source.Expression = "a * 100"
		return planner.CompilePrimaryQuery(context.Background(), source)
	}
	for _, method := range []string{"gt", "gte", "lt", "lte"} {
		facts, err := compile(fmt.Sprintf(`[{"key":"cpu","method":%q,"value":["1"]}]`, method))
		if err != nil {
			t.Fatalf("bk_data method %q: %v, want it compiled", method, err)
		}
		if got := compiledOperators(facts); len(got) != 1 || got[0] != method {
			t.Errorf("bk_data method %q compiled to %v, want [%s]", method, got, method)
		}
	}
	_, err := compile(`[{"key":"cpu","method":"exists","value":["1"]}]`)
	var failure *QueryPlanCompileError
	if !errors.As(err, &failure) || failure.Reason != "QUERY_CONDITION_METHOD_UNSUPPORTED" {
		t.Fatalf("bk_data method exists: %v, want QUERY_CONDITION_METHOD_UNSUPPORTED", err)
	}
}

// A condition with no value is dropped before its method is judged, as UQ
// drops it whatever its operator and as the platform's strategy runs: one
// with a method the store does not read compiles as if it were not written.
// An existence check is refused with no value too: UQ keeps it.
func TestAConditionWithNoValueIsDroppedWhateverItsMethod(t *testing.T) {
	for _, tc := range []struct{ name, with, without string }{
		{"alone", `[{"key":"host","method":"gt","value":[]}]`, `[]`},
		{"beside an equality", `[{"key":"host","method":"eq","value":["a"]},{"condition":"and","key":"cpu","method":"gt","value":[]}]`,
			`[{"key":"host","method":"eq","value":["a"]}]`},
		// The control: an empty condition with a method the store reads.
		{"an empty equality beside an equality", `[{"key":"host","method":"eq","value":["a"]},{"condition":"and","key":"cpu","method":"eq","value":[]}]`,
			`[{"key":"host","method":"eq","value":["a"]}]`},
	} {
		with, err := compileWithConditions(t, emptyConditionMetricConfig, tc.with)
		if err != nil {
			t.Fatalf("%s: %v, want the empty condition dropped", tc.name, err)
		}
		without, err := compileWithConditions(t, emptyConditionMetricConfig, tc.without)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := compiledOperators(with), compiledOperators(without); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s compiled to %v, without the empty condition %v", tc.name, got, want)
		}
	}
	_, err := compileWithConditions(t, emptyConditionMetricConfig, `[{"key":"host","method":"nexisted","value":[]}]`)
	var failure *QueryPlanCompileError
	if !errors.As(err, &failure) || failure.Reason != "QUERY_CONDITION_METHOD_UNSUPPORTED" {
		t.Fatalf("nexisted with no value: %v, want QUERY_CONDITION_METHOD_UNSUPPORTED", err)
	}
}

// A computing platform comparison takes one value: BkSql refuses more at
// every query, so the strategy is refused once, by name. Every other
// method maps on the computing platform's tables as on the other metric
// sources.
func TestABKDataComparisonTakesOneValueAndTheOtherMethodsMapAsMetrics(t *testing.T) {
	compile := func(conditions string) (execution.QueryPlanFacts, error) {
		planner, err := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
		if err != nil {
			t.Fatal(err)
		}
		source := pollingTestSource(fmt.Sprintf(conditionMethodBKDataConfig, conditions))
		source.Expression = "a * 100"
		return planner.CompilePrimaryQuery(context.Background(), source)
	}
	_, err := compile(`[{"key":"cpu","method":"gt","value":["1","2"]}]`)
	var failure *QueryPlanCompileError
	if !errors.As(err, &failure) || failure.Reason != "QUERY_CONDITION_INVALID" {
		t.Fatalf("gt with two values: %v, want QUERY_CONDITION_INVALID", err)
	}
	for method, operator := range metricConditions.operators {
		facts, err := compile(fmt.Sprintf(`[{"key":"cpu","method":%q,"value":["1"]}]`, method))
		if err != nil {
			t.Fatalf("bk_data method %q: %v", method, err)
		}
		if got := compiledOperators(facts); len(got) != 1 || got[0] != operator {
			t.Errorf("bk_data method %q compiled to %v, want [%s] as on the other metric sources", method, got, operator)
		}
	}
}

// compiledOperators is the operators of the first query's conditions, in
// order.
func compiledOperators(facts execution.QueryPlanFacts) []string {
	if len(facts.QueryList) == 0 {
		return nil
	}
	operators := make([]string, 0, len(facts.QueryList[0].Conditions.Fields))
	for _, field := range facts.QueryList[0].Conditions.Fields {
		operators = append(operators, field.Operator)
	}
	return operators
}
