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
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const apmConfig = `{"data_source_label":"bk_monitor","data_type_label":"time_series","metric_field":"bk_apm_count","result_table_id":"2_bkapm_metric_cross_bank_trade.__default__","agg_method":"SUM","agg_interval":60,"agg_dimension":["service_name"],"agg_condition":[],"alias":"a"}`

// An APM query asks the provider for app_name beside the strategy's own
// dimensions -- in the clause, the columns it keeps and the aggregation --
// and nothing about the query alarmd froze changes: the spec, its digest and
// its revision are the ones the request was built from.
func TestAnAPMQueryAsksForItsApplicationWithoutChangingTheFrozenQuery(t *testing.T) {
	spec := compiledSpec(t, apmConfig)
	before, _ := json.Marshal(spec)
	_, wire, err := buildWireRequest(spec)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		QueryList []struct {
			Dimensions  []string `json:"dimensions"`
			KeepColumns []string `json:"keep_columns"`
			Functions   []struct {
				Dimensions []string `json:"dimensions"`
			} `json:"function"`
		} `json:"query_list"`
	}
	if err := json.Unmarshal(wire, &body); err != nil || len(body.QueryList) != 1 {
		t.Fatalf("wire %s: %v", wire, err)
	}
	clause := body.QueryList[0]
	if !reflect.DeepEqual(clause.Dimensions, []string{"service_name", "app_name"}) || len(clause.Functions) == 0 ||
		!reflect.DeepEqual(clause.Functions[0].Dimensions, []string{"service_name", "app_name"}) ||
		(len(clause.KeepColumns) > 0 && !strings.Contains(strings.Join(clause.KeepColumns, ","), "app_name")) {
		t.Fatalf("wire %s, want app_name in the clause, its aggregation and its columns", wire)
	}
	after, _ := json.Marshal(spec)
	digest, err := execution.DerivePhysicalQueryDigest(spec)
	if string(before) != string(after) || err != nil || digest != spec.Digest {
		t.Fatalf("building the request changed the frozen query: digest %s vs %s (%v)", digest, spec.Digest, err)
	}
	// Without the carry the request is the one the strategy alone asks for.
	_, plain, err := buildWireRequestCarrying(spec, false)
	if err != nil || strings.Contains(string(plain), "app_name") {
		t.Fatalf("uncarried wire %s (%v)", plain, err)
	}
}

// The application is carried only when every clause reads the same APM
// table: never for another table, nor for clauses on different tables,
// whose join on group keys an extra key would change.
func TestTheApplicationIsCarriedOnlyForOneAPMTable(t *testing.T) {
	spec := compiledSpec(t, apmConfig)
	if !carriesAPMApp(spec) {
		t.Fatal("an APM metric table is not carried")
	}
	other := spec
	other.PlanFacts.QueryList = append([]execution.QueryClause(nil), spec.PlanFacts.QueryList...)
	other.PlanFacts.QueryList[0].TableID = "2_system.cpu_summary"
	if carriesAPMApp(other) {
		t.Fatal("a non-APM table is carried")
	}
	mixed := spec
	second := spec.PlanFacts.QueryList[0]
	second.TableID, second.ReferenceName = "2_bkapm_metric_cross_bank_counter.__default__", "b"
	mixed.PlanFacts.QueryList = append(append([]execution.QueryClause(nil), spec.PlanFacts.QueryList...), second)
	if carriesAPMApp(mixed) {
		t.Fatal("clauses on two APM tables are carried")
	}
	promql := spec
	promql.PlanFacts.PromQL = &execution.PromQLQuery{Expression: "up"}
	if carriesAPMApp(promql) {
		t.Fatal("a PromQL query is carried")
	}
}

// The series identity, the record ids and so every state key and alert
// fingerprint built from them are the same whether the provider's series
// carries app_name or not; the record carries it beside them.
func TestACarriedApplicationLeavesTheSeriesIdentityAsItWas(t *testing.T) {
	spec := compiledSpec(t, apmConfig)
	plain, _, err := normalizeSeries(spec, "provider-result", identityTestSeries([]string{"service_name"}, []string{"transfer"}), 1_700_123_500)
	if err != nil {
		t.Fatal(err)
	}
	carried, _, err := normalizeSeries(spec, "provider-result", identityTestSeries([]string{"service_name", "app_name"}, []string{"transfer", "cross-bank-trade"}), 1_700_123_500)
	if err != nil {
		t.Fatal(err)
	}
	a, b := plain.Dataset.Records(), carried.Dataset.Records()
	if len(a) != 1 || len(b) != 1 || a[0].RecordID != b[0].RecordID || !reflect.DeepEqual(a[0].DimensionIdentity, b[0].DimensionIdentity) {
		t.Fatalf("identity differs: %+v vs %+v", a, b)
	}
	if string(b[0].Dimensions["app_name"]) != `"cross-bank-trade"` {
		t.Fatalf("carried record dimensions %v, want app_name as written", b[0].Dimensions)
	}
}
