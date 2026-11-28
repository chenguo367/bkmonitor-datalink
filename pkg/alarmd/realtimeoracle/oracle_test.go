// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeoracle

import (
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

func cond(key, method string, values ...string) contract.RealtimeConditionV2 {
	return contract.RealtimeConditionV2{Key: key, Method: method, Values: values}
}

// A realtime item's conditions select, in process, the series
// VictoriaMetrics selects for the same conditions on the queried path: for
// every method with one value and several, values carrying the regular
// expression metacharacters an exact match must take literally and an
// expression must not, a missing label and an empty one, and or and and
// mixed. The expectation is the queried path's: the compiler's operators,
// unify-query's VMString, MetricsQL's parse of the selector, judged by
// Prometheus' matchers - never this tree's judgement.
func TestARealtimeItemsConditionsSelectTheSeriesVictoriaMetricsSelects(t *testing.T) {
	cases := [][][]contract.RealtimeConditionV2{
		{{cond("ip", "eq", "192.0.2.1")}},
		{{cond("ip", "eq", "192.0.2.1", "192.0.2.2")}},
		{{cond("module", "eq", "a.b", "c*d", "e+f", "(g)", "h|i")}},
		{{cond("module", "eq", "")}},
		{{cond("ip", "neq", "192.0.2.1")}},
		{{cond("module", "neq", "a.b", "h|i")}},
		{{cond("module", "neq", "")}},
		{{cond("module", "include", "db-.*")}},
		{{cond("module", "include", "db-.*", "cache")}},
		{{cond("host", "include", `.*\.example\.com`)}},
		{{cond("module", "include", "a", "")}},
		{{cond("module", "exclude", "db-.*")}},
		{{cond("module", "exclude", "db-.*", "cache")}},
		{{cond("module", "reg", "h|i")}},
		{{cond("module", "nreg", "(g)|a.b")}},
		{{cond("env", "eq", "prod"), cond("module", "include", "db-.*")}, {cond("ip", "eq", "192.0.2.9")}},
		{{cond("env", "neq", "test")}, {cond("module", "exclude", "db-.*")}},
		{{cond("env", "eq", "prod", "test"), cond("ip", "neq", "192.0.2.1")}},
	}
	series := []map[string]string{
		{}, {"ip": ""}, {"ip": "192.0.2.1"}, {"ip": "192.0.2.2"}, {"ip": "192x0x2x1"}, {"ip": "192.0.2.10"},
		{"ip": "192.0.2.9", "env": "test", "module": "db-1"}, {"env": "prod", "module": "db-1"}, {"env": "prod", "module": "web"},
		{"module": ""}, {"module": "cache"}, {"module": "a-cache"}, {"module": "a"}, {"module": "a.b"}, {"module": "axb"},
		{"module": "c*d"}, {"module": "ccd"}, {"module": "e+f"}, {"module": "eef"}, {"module": "(g)"}, {"module": "g"},
		{"module": "h|i"}, {"module": "h"}, {"module": "i"}, {"host": "www.example.com"}, {"host": "www.exampleXcom"},
		{"env": "test", "ip": "192.0.2.1"}, {"env": "prod", "ip": "192.0.2.2"},
	}
	selected, rejected := 0, 0
	for index, groups := range cases {
		conditions, err := admission.NewRealtimeConditions(groups)
		if err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
		selector, err := uqSelector(groups)
		if err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
		for _, labels := range series {
			want := selects(selector, labels)
			got := conditions.Pass(func(key string) string { return labels[key] })
			if got != want {
				t.Fatalf("case %d %s, series %v: judged %v in process, VictoriaMetrics selects %v", index, describe(groups), labels, got, want)
			}
			if want {
				selected++
			} else {
				rejected++
			}
		}
	}
	if selected == 0 || rejected == 0 {
		t.Fatalf("the cases selected %d series and turned away %d: one side was never compared", selected, rejected)
	}
}

func describe(groups [][]contract.RealtimeConditionV2) string {
	return fmt.Sprintf("%+v", groups)
}
