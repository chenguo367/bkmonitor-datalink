// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package linkdoutput

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The content of an item detected over several inputs words each input as
// its shown name, value and unit, in the order the item lists them, joined
// by 且, then the time and the window (015 F1 section 7); without a version 2
// snapshot - the item's dropped for size, or one query's event - its values
// are named by their references as any event's are.
func TestTheContentOfAnItemDetectedOverSeveralInputsWordsEachInput(t *testing.T) {
	snapshot := json.RawMessage(`{"version":2,"inputs":{` +
		`"mem":{"state":"SUCCESS","value":86.000000,"unit":"percent","hit":true,"display_name":"内存 limit 使用率"},` +
		`"cpu":{"state":"SUCCESS","value":95.200000,"unit":"percent","hit":true,"display_name":"CPU limit 使用率"},` +
		`"rss":{"state":"SUCCESS","value":1073741824.000000,"unit":"bytes","hit":true,"display_name":"RSS"}},` +
		`"levels":[{"level":1,"algorithms":[{"type":"Threshold","input_ref":"cpu"},{"type":"Threshold","input_ref":"mem"},{"type":"Threshold","input_ref":"rss"}],"window":{"required":1,"observed":1,"size":1}}]}`)
	event := &contract.TriggerEventV1{EventKind: contract.TriggerEventAbnormal,
		RecordRef:         contract.TriggerRecordRefV1{SourceTime: 1_700_000_000},
		Observed:          contract.TriggerObservedV1{Values: map[string]json.RawMessage{"cpu": json.RawMessage(`95.2`), "mem": json.RawMessage(`86.1`)}},
		DetectionSnapshot: snapshot}
	primary := contract.LevelResultV1{LevelID: 1, DecisionWindow: contract.DecisionWindowV1{
		Trigger: contract.TriggerWindowEvidenceV1{WindowSize: 1, RequiredAnomalies: 1, ObservedAnomalies: 1}}}
	got := content(event, primary, false)
	// The values as judged, at the threshold's precision, read without it.
	if !strings.HasPrefix(got, "CPU limit 使用率=95.2% 且 内存 limit 使用率=86% 且 RSS=1073741824 bytes at ") || !strings.Contains(got, "1 of 1 points in the window were anomalous") {
		t.Fatalf("content %q, want each input by its shown name, value and unit, in order, joined by 且", got)
	}
	for value, want := range map[string]string{"86.000000": "86", "95.200000": "95.2", "-0.000000": "0", "100": "100", "1e-7": "1e-7"} {
		if got := trimmedDecimal(value); got != want {
			t.Errorf("trimmedDecimal(%s) = %s, want %s", value, got, want)
		}
	}
	event.DetectionSnapshot = nil
	if got := content(event, primary, false); !strings.HasPrefix(got, "cpu=95.2, mem=86.1 at ") {
		t.Fatalf("content without the snapshot %q, want the values by reference", got)
	}
}
