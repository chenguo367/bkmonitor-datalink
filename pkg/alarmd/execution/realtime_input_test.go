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
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func realtimeFacts() execution.RealtimeInputFacts {
	return execution.RealtimeInputFacts{ResultTableID: "2_bkmonitor_time_series_1", Cluster: "inner", Topic: "realtime_2_1",
		SourceKind: "custom_report", PeriodSeconds: 60}
}

// A realtime input names its result table, cluster and topic, one of the
// four source kinds, and the one period a realtime Slot is; any other is
// refused by name.
func TestARealtimeInputIsItsTableTopicSourceAndPeriod(t *testing.T) {
	if err := realtimeFacts().Validate(); err != nil {
		t.Fatalf("valid realtime input refused: %v", err)
	}
	for _, source := range []string{"k8s", "custom_report", "plugin_flat", "plugin_nested"} {
		facts := realtimeFacts()
		facts.SourceKind = source
		if err := facts.Validate(); err != nil {
			t.Fatalf("source kind %q refused: %v", source, err)
		}
	}
	for _, cell := range []struct {
		name   string
		broken func(*execution.RealtimeInputFacts)
	}{
		{"no table", func(facts *execution.RealtimeInputFacts) { facts.ResultTableID = "" }},
		{"no cluster", func(facts *execution.RealtimeInputFacts) { facts.Cluster = "" }},
		{"no topic", func(facts *execution.RealtimeInputFacts) { facts.Topic = "" }},
		{"a source not resolved", func(facts *execution.RealtimeInputFacts) { facts.SourceKind = "" }},
		{"a source no rule writes", func(facts *execution.RealtimeInputFacts) { facts.SourceKind = "bk_standard" }},
		{"a period of a minute and a half", func(facts *execution.RealtimeInputFacts) { facts.PeriodSeconds = 90 }},
		{"no period", func(facts *execution.RealtimeInputFacts) { facts.PeriodSeconds = 0 }},
	} {
		facts := realtimeFacts()
		cell.broken(&facts)
		if err := facts.Validate(); err == nil {
			t.Fatalf("%s: accepted %+v", cell.name, facts)
		}
	}
}

// A Query Group read from a query provider serializes its facts exactly as
// it did before the realtime input existed: the field is absent, not null.
// A realtime one carries the input whole, and reads back the same.
func TestTheRealtimeInputIsAbsentFromEveryOtherQueryGroupsFacts(t *testing.T) {
	queried := execution.QueryPlanFacts{Provider: execution.ProviderUQ, TenantID: "tenant", BusinessID: "2", StepMillis: 60_000}
	encoded, err := json.Marshal(queried)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "Realtime") {
		t.Fatalf("a queried group's facts carry the realtime input: %s", encoded)
	}
	input := realtimeFacts()
	realtime := execution.QueryPlanFacts{Provider: execution.ProviderRealtime, TenantID: "tenant", BusinessID: "2", Realtime: &input}
	encoded, err = json.Marshal(realtime)
	if err != nil {
		t.Fatal(err)
	}
	var decoded execution.QueryPlanFacts
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Provider != execution.ProviderRealtime || !reflect.DeepEqual(decoded.Realtime, &input) {
		t.Fatalf("realtime facts read back as %+v (input %+v), want %+v", decoded, decoded.Realtime, input)
	}
}

// A realtime Query Group's facts are built, and read back, as the realtime
// provider's with its input and nothing a query provider is asked: the
// provider and the input go together, the input holds, the tenant and
// business are named. Each is refused alone; the facts that hold get their
// revision, which Validate derives again.
func TestARealtimeQueryGroupsFactsAreTheRealtimeProvidersWithItsInputAlone(t *testing.T) {
	realtime := func(change func(*execution.QueryPlanFacts)) execution.QueryPlanFacts {
		input := realtimeFacts()
		facts := execution.QueryPlanFacts{Provider: execution.ProviderRealtime, TenantID: "tenant", BusinessID: "2", Realtime: &input}
		if change != nil {
			change(&facts)
		}
		return facts
	}
	built, err := execution.BuildQueryPlanFacts(realtime(nil))
	if err != nil {
		t.Fatalf("realtime facts refused: %v", err)
	}
	if built.QueryRevision == "" || built.Validate() != nil {
		t.Fatalf("realtime facts built as %+v, revision %q, Validate %v", built, built.QueryRevision, built.Validate())
	}
	for _, cell := range []struct {
		name  string
		facts execution.QueryPlanFacts
	}{
		{"the realtime provider with no input", realtime(func(facts *execution.QueryPlanFacts) { facts.Realtime = nil })},
		{"an input under the query provider", realtime(func(facts *execution.QueryPlanFacts) { facts.Provider = execution.ProviderUQ })},
		{"an input that does not hold", realtime(func(facts *execution.QueryPlanFacts) { facts.Realtime.Topic = "" })},
		{"no tenant", realtime(func(facts *execution.QueryPlanFacts) { facts.TenantID = "" })},
		{"no business", realtime(func(facts *execution.QueryPlanFacts) { facts.BusinessID = "" })},
		{"a provider route", realtime(func(facts *execution.QueryPlanFacts) { facts.ProviderRouteRef = "uq-primary-v1" })},
		{"a query list", realtime(func(facts *execution.QueryPlanFacts) { facts.QueryList = []execution.QueryClause{{Driver: "vm"}} })},
		{"a metric merge", realtime(func(facts *execution.QueryPlanFacts) { facts.MetricMerge = "a" })},
		{"a PromQL", realtime(func(facts *execution.QueryPlanFacts) { facts.PromQL = &execution.PromQLQuery{Expression: "up"} })},
	} {
		if _, err := execution.BuildQueryPlanFacts(cell.facts); err == nil {
			t.Fatalf("%s: built", cell.name)
		}
	}
}
