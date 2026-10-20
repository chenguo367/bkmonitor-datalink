// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package trigger

import (
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// namedOutputsAt is a table of outputs A, B, C and D - C the legacy one -
// with one record: A answered 1, B nothing there, C the record's own 50.1,
// and D a point in a state that failed.
func namedOutputsAt(recordID string) *execution.NamedOutputRecords {
	return &execution.NamedOutputRecords{Refs: []string{"A", "B", "C", "D"}, Records: map[string][]execution.NamedOutputValue{
		recordID: {
			{Value: json.RawMessage("1"), State: execution.NamedOutputSuccess},
			{State: execution.NamedOutputMissing},
			{Value: json.RawMessage("50.1"), State: execution.NamedOutputSuccess},
			{Value: json.RawMessage("2"), State: execution.NamedOutputError},
		}}}
}

// abnormalRequest decides level 5 ABNORMAL beside level 6, which stays
// NORMAL.
func abnormalRequest(t *testing.T) EvaluationRequestV2 {
	t.Helper()
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 1, 1, 1, nil), levelV2(6, 2, 1, 1, 1, nil)})
	history := []LevelHistory{{LevelID: 5, View: pointHistory{step: 60, points: map[int64]bool{300: true}}},
		{LevelID: 6, View: pointHistory{step: 60, points: map[int64]bool{300: false}}}}
	return requestV2(t, plan, 300, []DetectionFact{factV2(plan.Levels().At(0), DetectionAnomalous), factV2(plan.Levels().At(1), DetectionNormal)},
		history, activeFactsV2(t, plan, 300))
}

// An ABNORMAL event of a Plan with named outputs carries what its detection
// decided on: each output at the record, a value only where it answered -
// not where it failed, though a point was there - and the level that
// decided it, not the one that stayed normal - its algorithm with the matched OR group, the
// group's condition and the value it was judged at, its window, when its
// anomalies began and the unit. Its id and semantic digest are what they
// are without it.
func TestAnAbnormalEventCarriesItsDetectionSnapshot(t *testing.T) {
	request := abnormalRequest(t)
	plain, err := EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	request.NamedOutputs = namedOutputsAt(request.Record.RecordID)
	named, err := EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	if plain.TriggerEvent.DetectionSnapshot != nil {
		t.Fatalf("an event of a Plan without named outputs carries %s", plain.TriggerEvent.DetectionSnapshot)
	}
	want := `{"version":1,"ref_values":{"A":{"value":1,"state":"SUCCESS"},"B":{"state":"MISSING"},"C":{"value":50.1,"state":"SUCCESS"},"D":{"state":"ERROR"}},` +
		`"levels":[{"level":5,"algorithms":[{"type":"Threshold","facts":{"value":50.100000,"group":0,"method":"gte","threshold":50.000000}}],` +
		`"window":{"required":1,"observed":1,"size":1},"anomaly_begin_time":300,"unit":"%"}]}`
	if got := string(named.TriggerEvent.DetectionSnapshot); got != want {
		t.Fatalf("snapshot\n%s\nwant\n%s", got, want)
	}
	if named.TriggerEvent.EventID != plain.TriggerEvent.EventID || named.TriggerEvent.EventSemanticDigest != plain.TriggerEvent.EventSemanticDigest {
		t.Fatal("the snapshot moved the event's identity")
	}
	encoded, err := contract.EncodeTriggerEventV1(named.TriggerEvent)
	if err != nil {
		t.Fatal(err)
	}
	if plainEncoded, _ := contract.EncodeTriggerEventV1(plain.TriggerEvent); string(encoded) != string(plainEncoded) {
		t.Fatal("the snapshot is in the event's own encoding")
	}
}

// A record not in the table - or a no-data point, which is never read -
// carries no ref_values; its levels it still carries.
func TestAnEventOfARecordNotReadCarriesNoRefValues(t *testing.T) {
	for name, mutate := range map[string]func(*EvaluationRequestV2){
		"not in the table": func(request *EvaluationRequestV2) { request.NamedOutputs = namedOutputsAt("another-record") },
		"no data": func(request *EvaluationRequestV2) {
			request.NamedOutputs = namedOutputsAt(request.Record.RecordID)
			request.RecordRef.Dimensions = map[string]json.RawMessage{contract.NoDataDimensionTag: json.RawMessage("true")}
		},
	} {
		request := abnormalRequest(t)
		mutate(&request)
		result, err := EvaluateV2(request)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot contract.DetectionSnapshotV1
		if err := json.Unmarshal(result.TriggerEvent.DetectionSnapshot, &snapshot); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if snapshot.RefValues != nil || len(snapshot.Levels) != 1 {
			t.Errorf("%s: %s", name, result.TriggerEvent.DetectionSnapshot)
		}
	}
}

// A RECOVERY event carries its levels and their windows, and no
// ref_values: no reader needs the outputs once the alert recovers.
func TestARecoveryEventCarriesItsLevelsAndNoRefValues(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 9, 3, 2, 2, nil)})
	request := requestV2(t, plan, 360, []DetectionFact{factV2(plan.Levels().At(0), DetectionAnomalous)}, []LevelHistory{{
		LevelID: 5, View: pointHistory{step: 60, points: map[int64]bool{180: false, 240: false, 300: false, 360: true}},
	}}, activeFactsV2(t, plan, 360))
	request.NamedOutputs = namedOutputsAt(request.Record.RecordID)
	result, err := EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.TriggerEvent == nil || result.TriggerEvent.EventKind != contract.TriggerEventRecovery {
		t.Fatalf("result %+v", result)
	}
	var snapshot contract.DetectionSnapshotV1
	if err := json.Unmarshal(result.TriggerEvent.DetectionSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.RefValues != nil || len(snapshot.Levels) != 1 || snapshot.Levels[0].Level != 5 ||
		snapshot.Levels[0].Window != (contract.DetectionWindowV1{Required: 2, Observed: 1, Size: 3}) ||
		len(snapshot.Levels[0].Algorithms) != 1 || snapshot.Levels[0].Algorithms[0].Type != "Threshold" {
		t.Fatalf("snapshot %s", result.TriggerEvent.DetectionSnapshot)
	}
}

// The largest snapshot a realistic named strategy decides, built as the
// trigger builds it: four outputs under long reference names, three levels
// all ABNORMAL, each of three Threshold algorithms whose matched OR group
// has three conditions. It is whole, well under its bound; its size is
// logged for the hand-over.
func TestTheLargestRealisticNamedStrategySnapshotIsWhole(t *testing.T) {
	threshold := map[string]any{
		"value_field": "value", "data_unit": "percent", "threshold_unit_prefix": "", "precision": map[string]any{"decimal_places": 6, "rounding": "HALF_EVEN"},
		"groups": []any{map[string]any{"conditions": []any{
			map[string]any{"operator": "GTE", "threshold_decimal": "50.125"},
			map[string]any{"operator": "LT", "threshold_decimal": "99.875"},
			map[string]any{"operator": "NEQ", "threshold_decimal": "75.5"},
		}}},
	}
	levels := make([]contract.LevelIRV2, 0, 3)
	for id := uint32(1); id <= 3; id++ {
		level := levelV2(id, id, 5, 3, 1, nil)
		level.Connector = contract.LevelConnectorOR
		level.DetectPlan.Algorithms = nil
		for range 3 {
			level.DetectPlan.Algorithms = append(level.DetectPlan.Algorithms, contract.AlgorithmIRV2{Type: "Threshold", Version: 1, Config: mustJSONV2(threshold)})
		}
		levels = append(levels, level)
	}
	plan := compilePlanV2(t, levels)
	histories := make([]LevelHistory, 0, 3)
	facts := make([]DetectionFact, 0, 3)
	for index, level := range plan.Levels().All() {
		histories = append(histories, LevelHistory{LevelID: level.Definition().LevelID,
			View: pointHistory{step: 60, points: map[int64]bool{120: true, 180: true, 240: true, 300: true}}})
		fact := factV2(level, DetectionAnomalous)
		algorithm := uint32(index)
		fact.Evidence.MatchedAlgorithmOrdinal = &algorithm
		facts = append(facts, fact)
	}
	request := requestV2(t, plan, 300, facts, histories, activeFactsV2(t, plan, 300))
	refs := []string{"REQUEST_SUCCESS_COUNT_FROM_GATEWAY", "REQUEST_FAILURE_COUNT_FROM_GATEWAY",
		"REQUEST_TOTAL_COUNT_FROM_UPSTREAMS", "REQUEST_SUCCESS_RATE_PERCENTAGE_RESULT"}
	values := make([]execution.NamedOutputValue, 0, len(refs))
	for range refs {
		values = append(values, execution.NamedOutputValue{Value: json.RawMessage("123456789.123456"), State: execution.NamedOutputSuccess})
	}
	request.NamedOutputs = &execution.NamedOutputRecords{Refs: refs, Records: map[string][]execution.NamedOutputValue{request.Record.RecordID: values}}
	result, err := EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.TriggerEvent == nil || result.Snapshot == nil {
		t.Fatalf("result %+v", result)
	}
	size := len(result.TriggerEvent.DetectionSnapshot)
	t.Logf("largest realistic named strategy snapshot: %d bytes", size)
	var snapshot contract.DetectionSnapshotV1
	if err := json.Unmarshal(result.TriggerEvent.DetectionSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.Truncated || size > contract.MaxDetectionSnapshotBytes || len(snapshot.Levels) != 3 || len(snapshot.RefValues) != 4 {
		t.Fatalf("%d bytes, truncated %v: %s", size, result.Snapshot.Truncated, result.TriggerEvent.DetectionSnapshot)
	}
	if facts := snapshot.Levels[2].Algorithms[2].Facts; len(facts) != 8 {
		t.Fatalf("the third level's deciding algorithm has %d facts, want value, group and three conditions: %s", len(facts), result.TriggerEvent.DetectionSnapshot)
	}
}
