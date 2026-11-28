// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// A queried item's Plan serializes exactly as it did before the realtime
// part existed: the field is absent, not null. A realtime item's carries its
// metric and its condition groups, and reads back the same.
func TestTheRealtimePartIsAbsentFromEveryOtherPlan(t *testing.T) {
	queried := contract.EvaluationPlanV2{PlanID: "7"}
	encoded, err := json.Marshal(queried)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"realtime"`) {
		t.Fatalf("a queried item's Plan carries a realtime part: %s", encoded)
	}
	item := contract.RealtimeItemV2{Item: contract.RealtimeItemKey{StrategyID: 1001, ItemID: 1, QueryMD5: "query-1"}, Metric: "cpu_usage", Conditions: [][]contract.RealtimeConditionV2{
		{{Key: "ip", Method: contract.RealtimeConditionEq, Values: []string{"127.0.0.1", "127.0.0.2"}}},
		{{Key: "module", Method: contract.RealtimeConditionReg, Values: []string{"^db-.*"}},
			{Key: "env", Method: contract.RealtimeConditionNeq, Values: []string{"test"}}},
	}}
	realtime := contract.EvaluationPlanV2{PlanID: "7", Realtime: &item}
	if encoded, err = json.Marshal(realtime); err != nil {
		t.Fatal(err)
	}
	var decoded contract.EvaluationPlanV2
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Realtime, &item) {
		t.Fatalf("realtime part read back as %+v, want %+v", decoded.Realtime, item)
	}
}

// The methods a realtime condition is judged by are the six the strategy
// page offers, in its own words, each once: a method outside them is no
// realtime condition (REALTIME_CONDITION_UNSUPPORTED as the item compiles).
func TestTheRealtimeConditionMethodsAreThePagesSix(t *testing.T) {
	want := map[string]bool{"eq": true, "neq": true, "include": true, "exclude": true, "reg": true, "nreg": true}
	got := contract.RealtimeConditionMethods()
	if len(got) != len(want) {
		t.Fatalf("methods %v, want the six of %v", got, want)
	}
	seen := map[string]bool{}
	for _, method := range got {
		if !want[method] || seen[method] {
			t.Fatalf("methods %v: %q unknown or twice", got, method)
		}
		seen[method] = true
	}
}

// A realtime part holds when it names its metric and every condition of
// every group names its label, one of the six methods and its values, a
// regular expression's compiling as the whole-string match it is judged by;
// each of these broken alone is refused.
func TestARealtimePartHoldsWithItsMetricAndWellFormedConditions(t *testing.T) {
	item := func(change func(*contract.RealtimeItemV2)) contract.RealtimeItemV2 {
		part := contract.RealtimeItemV2{Item: contract.RealtimeItemKey{StrategyID: 1001, ItemID: 1, QueryMD5: "query-1"}, Metric: "cpu_usage", Conditions: [][]contract.RealtimeConditionV2{
			{{Key: "ip", Method: contract.RealtimeConditionEq, Values: []string{"127.0.0.1"}}},
			{{Key: "module", Method: contract.RealtimeConditionInclude, Values: []string{"db-.*", "cache"}}},
		}}
		if change != nil {
			change(&part)
		}
		return part
	}
	if err := item(nil).Validate(); err != nil {
		t.Fatalf("a part that holds refused: %v", err)
	}
	if err := item(func(part *contract.RealtimeItemV2) { part.Conditions = nil }).Validate(); err != nil {
		t.Fatalf("a part with no condition, every series, refused: %v", err)
	}
	for _, cell := range []struct {
		name   string
		change func(*contract.RealtimeItemV2)
	}{
		{"no strategy", func(part *contract.RealtimeItemV2) { part.Item.StrategyID = 0 }},
		{"no item", func(part *contract.RealtimeItemV2) { part.Item.ItemID = 0 }},
		{"no query digest", func(part *contract.RealtimeItemV2) { part.Item.QueryMD5 = "" }},
		{"no metric", func(part *contract.RealtimeItemV2) { part.Metric = "" }},
		{"an empty group", func(part *contract.RealtimeItemV2) { part.Conditions = append(part.Conditions, nil) }},
		{"no label", func(part *contract.RealtimeItemV2) { part.Conditions[0][0].Key = "" }},
		{"no values", func(part *contract.RealtimeItemV2) { part.Conditions[0][0].Values = nil }},
		{"a method the page does not offer", func(part *contract.RealtimeItemV2) { part.Conditions[0][0].Method = "gt" }},
		{"an expression that does not compile", func(part *contract.RealtimeItemV2) { part.Conditions[1][0].Values = []string{"db-("} }},
	} {
		if err := item(cell.change).Validate(); err == nil {
			t.Fatalf("%s: held", cell.name)
		}
	}
}

// A regular-expression condition is judged by its values joined as written
// and anchored at both ends - a whole-string match, not a substring - and an
// equality condition has no expression.
func TestARealtimeConditionsPatternIsTheWholeStringMatch(t *testing.T) {
	pattern, err := contract.RealtimeConditionV2{Key: "module", Method: contract.RealtimeConditionReg, Values: []string{"db-.*", "cache"}}.Pattern()
	if err != nil {
		t.Fatal(err)
	}
	for value, want := range map[string]bool{"db-1": true, "cache": true, "a-cache": false, "cache-1": false, "": false} {
		if got := pattern.MatchString(value); got != want {
			t.Fatalf("%q matched %v, want %v", value, got, want)
		}
	}
	if pattern, err := (contract.RealtimeConditionV2{Key: "ip", Method: contract.RealtimeConditionEq, Values: []string{"a"}}).Pattern(); pattern != nil || err != nil {
		t.Fatalf("an equality condition has pattern %v, %v", pattern, err)
	}
}
