// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package admission

import (
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

func realtimeConditions(t *testing.T, groups ...[]contract.RealtimeConditionV2) *RealtimeConditions {
	t.Helper()
	conditions, err := NewRealtimeConditions(groups)
	if err != nil {
		t.Fatal(err)
	}
	return conditions
}

func condition(key, method string, values ...string) contract.RealtimeConditionV2 {
	return contract.RealtimeConditionV2{Key: key, Method: method, Values: values}
}

// Each method judges a label's value as the design's table writes it (016
// section 5.1) - eq a value equal to one of the values, exactly; neq equal to
// none; include and reg the whole value matching the values joined, not a
// substring of it; exclude and nreg not - a missing label read as the empty
// string. The values carry the metacharacters an exact match must take
// literally.
func TestEachRealtimeConditionMethodJudgesALabelAsTheTableSays(t *testing.T) {
	for _, cell := range []struct {
		name      string
		condition contract.RealtimeConditionV2
		passes    map[string]bool
	}{
		{"eq one value", condition("ip", "eq", "192.0.2.1"),
			map[string]bool{"192.0.2.1": true, "192.0.2.10": false, "192a0a2a1": false, "": false}},
		{"eq values, a dot taken literally", condition("ip", "eq", "192.0.2.1", "192.0.2.2"),
			map[string]bool{"192.0.2.1": true, "192.0.2.2": true, "192x0x2x2": false, "": false}},
		{"eq the empty value", condition("ip", "eq", ""), map[string]bool{"": true, "192.0.2.1": false}},
		{"neq values", condition("env", "neq", "test", "dev"),
			map[string]bool{"prod": true, "test": false, "dev": false, "": true}},
		{"include a whole-string expression", condition("module", "include", "db-.*", "cache"),
			map[string]bool{"db-1": true, "cache": true, "a-cache": false, "cache-1": false, "": false}},
		{"reg as include", condition("module", "reg", "db-[0-9]+"),
			map[string]bool{"db-12": true, "db-x": false, "mydb-12": false}},
		{"include that matches the empty value", condition("module", "include", "db-.*", ""),
			map[string]bool{"": true, "db-1": true, "web": false}},
		{"exclude", condition("module", "exclude", "db-.*"),
			map[string]bool{"db-1": false, "web-1": true, "": true}},
		{"nreg as exclude", condition("module", "nreg", "a|b"),
			map[string]bool{"a": false, "b": false, "ab": true, "": true}},
	} {
		conditions := realtimeConditions(t, []contract.RealtimeConditionV2{cell.condition})
		for value, want := range cell.passes {
			got := conditions.Pass(func(key string) string {
				if key != cell.condition.Key {
					t.Fatalf("%s: read label %q", cell.name, key)
				}
				return value
			})
			if got != want {
				t.Fatalf("%s: %q passed %v, want %v", cell.name, value, got, want)
			}
		}
	}
}

// A series passes when it passes every condition of any one group: or
// between groups, and within one. No group is every series.
func TestRealtimeConditionGroupsAreOrsOfAnds(t *testing.T) {
	conditions := realtimeConditions(t,
		[]contract.RealtimeConditionV2{condition("env", "eq", "prod"), condition("module", "include", "db-.*")},
		[]contract.RealtimeConditionV2{condition("ip", "eq", "192.0.2.9")},
	)
	for _, cell := range []struct {
		labels map[string]string
		want   bool
	}{
		{map[string]string{"env": "prod", "module": "db-1"}, true},
		{map[string]string{"env": "prod", "module": "web"}, false},
		{map[string]string{"env": "test", "module": "db-1"}, false},
		{map[string]string{"env": "test", "ip": "192.0.2.9"}, true},
		{map[string]string{}, false},
	} {
		if got := conditions.Pass(func(key string) string { return cell.labels[key] }); got != cell.want {
			t.Fatalf("%v passed %v, want %v", cell.labels, got, cell.want)
		}
	}
	every := realtimeConditions(t)
	if !every.Pass(func(string) string { return "" }) {
		t.Fatal("an item with no conditions turned a series away")
	}
	var none *RealtimeConditions
	if !none.Pass(func(string) string { return "" }) {
		t.Fatal("no conditions turned a series away")
	}
}

// Conditions that do not hold are not compiled.
func TestARealtimeItemThatDoesNotHoldIsNotCompiled(t *testing.T) {
	if _, err := NewRealtimeConditions([][]contract.RealtimeConditionV2{{condition("module", "reg", "db-(")}}); err == nil {
		t.Fatal("an expression that does not compile was compiled")
	}
}

// The filter admits every series for a Plan with no realtime conditions,
// and judges a realtime Plan's by its labels as the series carries them - a
// string's content, a number's own text, a null or a missing label as the
// empty string - turning away one that passes no group as condition_rejected.
func TestTheRealtimeConditionFilterJudgesTheLabelsAsTheSeriesCarriesThem(t *testing.T) {
	filter := RealtimeConditionFilter{}
	facts := func(dimensions map[string]string) *Facts {
		raw := make(map[string]json.RawMessage, len(dimensions))
		for key, value := range dimensions {
			raw[key] = json.RawMessage(value)
		}
		return &Facts{Dimensions: raw}
	}
	if decision := filter.Admit(PlanContext{}, facts(map[string]string{"env": `"test"`})); !decision.Admit {
		t.Fatalf("a Plan with no realtime conditions turned a series away: %+v", decision)
	}
	plan := PlanContext{RealtimeConditions: realtimeConditions(t,
		[]contract.RealtimeConditionV2{condition("replica", "eq", "1"), condition("env", "neq", "test")})}
	for _, cell := range []struct {
		dimensions map[string]string
		admit      bool
	}{
		{map[string]string{"replica": `"1"`, "env": `"prod"`}, true},
		{map[string]string{"replica": `1`, "env": `"prod"`}, true},
		{map[string]string{"replica": `1`, "env": `null`}, true},
		{map[string]string{"replica": `1`}, true},
		{map[string]string{"replica": `"1"`, "env": `"test"`}, false},
		{map[string]string{"replica": `1.0`}, false},
	} {
		decision := filter.Admit(plan, facts(cell.dimensions))
		if decision.Admit != cell.admit || !cell.admit && decision.Reason != RealtimeConditionRejected {
			t.Fatalf("%v: %+v, want admitted %v", cell.dimensions, decision, cell.admit)
		}
	}
	// A label the series does not have is read as the empty string, and so
	// is a null one: equal to the empty value, as a matcher of it selects a
	// series without the label.
	empty := PlanContext{RealtimeConditions: realtimeConditions(t, []contract.RealtimeConditionV2{condition("env", "eq", "")})}
	for _, cell := range []struct {
		dimensions map[string]string
		admit      bool
	}{
		{map[string]string{"replica": `1`}, true},
		{map[string]string{"env": `null`}, true},
		{map[string]string{"env": `""`}, true},
		{map[string]string{"env": `"prod"`}, false},
	} {
		if decision := filter.Admit(empty, facts(cell.dimensions)); decision.Admit != cell.admit {
			t.Fatalf("env equal to the empty value, %v: %+v, want admitted %v", cell.dimensions, decision, cell.admit)
		}
	}
}
