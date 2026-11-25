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
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The two inputs of an item detected over several - each the ratio of its
// own two metric time series, compiled as the control plane compiles them -
// asked over the window they share, send the provider the same start, end
// and step: the end a second early for both, as every metric time series
// asks it. Only their queries differ.
func TestMultiDetectInputsSendOneStartEndAndStep(t *testing.T) {
	query := func(alias, field string) json.RawMessage {
		encoded, _ := json.Marshal(map[string]any{"data_source_label": "bk_monitor", "data_type_label": "time_series",
			"result_table_id": "container.usage", "metric_field": field, "agg_method": "AVG", "agg_interval": 60,
			"alias": alias, "agg_dimension": []string{"namespace", "pod"}})
		return encoded
	}
	planner, err := controlplane.NewLegacyPrimaryQueryCompiler("uq", "Asia/Shanghai", controlplane.LegacyQueryRuntimeFacts{})
	if err != nil {
		t.Fatal(err)
	}
	detected, err := planner.CompileMultiDetect(context.Background(), controlplane.PrimaryQuerySource{
		Identity:   controlplane.SourceIdentity{TenantID: "tenant", BusinessID: "2", SpaceScope: "bkcc__2"},
		StrategyID: "15", ItemID: "1", QueryMD5: "multi",
		QueryConfigs: []json.RawMessage{query("a", "cpu_used"), query("b", "cpu_limit"), query("c", "memory_used"), query("d", "memory_limit")},
		QueryOutputConfig: json.RawMessage(`{"response_contract":"multi_detect/v1","common_dimensions":["namespace","pod"],"inputs":[
			{"ref":"cpu","expression":"a / b * 100","interval":60,"unit":"percent"},
			{"ref":"memory","expression":"c / d * 100","interval":60,"unit":"percent"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	window := execution.QueryWindow{Start: 1_700_123_940, End: 1_700_124_000}
	type asked struct {
		Start  string `json:"start_time"`
		End    string `json:"end_time"`
		Step   string `json:"step"`
		Merge  string `json:"metric_merge"`
		Fields []struct {
			Field string `json:"field_name"`
		} `json:"query_list"`
	}
	bodies := make([]asked, 0, len(detected.Inputs))
	for _, input := range detected.Inputs {
		spec, err := execution.BuildPhysicalQuerySpec(execution.PhysicalQuerySpec{PlanFacts: input.Facts, LogicalWindow: window,
			ProviderRange: window, AcceptedRange: window, RequiredColumns: []string{"value"}})
		if err != nil {
			t.Fatal(err)
		}
		_, wire, err := buildWireRequest(spec)
		if err != nil {
			t.Fatal(err)
		}
		var body asked
		if err := json.Unmarshal(wire, &body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	cpu, memory := bodies[0], bodies[1]
	if cpu.Start != memory.Start || cpu.End != memory.End || cpu.Step != memory.Step {
		t.Fatalf("asked %s..%s by %s and %s..%s by %s, want one start, end and step", cpu.Start, cpu.End, cpu.Step,
			memory.Start, memory.End, memory.Step)
	}
	if cpu.Start != "1700123940" || cpu.End != "1700123999" || cpu.Step != "60s" {
		t.Fatalf("asked %s..%s by %s, want the window with its end a second early", cpu.Start, cpu.End, cpu.Step)
	}
	if cpu.Merge == memory.Merge || len(cpu.Fields) != 2 || len(memory.Fields) != 2 ||
		cpu.Fields[0].Field != "cpu_used" || memory.Fields[0].Field != "memory_used" {
		t.Fatalf("queries %+v and %+v, want each input's own two series", cpu, memory)
	}
}
