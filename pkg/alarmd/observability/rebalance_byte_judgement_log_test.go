// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// The rebalance line names how the byte constraint judged and why - the
// judgement, the Workers that made it the sum of peaks, the all-over mode,
// a new Leader's first hour and the Query Groups past every share - zeros
// included, so a round's line reads the same as its fleet snapshot.
func TestTheRebalanceLineNamesTheByteJudgement(t *testing.T) {
	render := func(facts ByteConstraintFacts) map[string]any {
		t.Helper()
		var output bytes.Buffer
		renderTo(&output, Observation{Component: ComponentOwnership, Stage: StageRebalancePlanned, Result: ResultSuccess,
			Rebalance: &RebalanceFacts{ReadyWorkers: 2, Bytes: &facts}})
		var event map[string]any
		if err := json.Unmarshal(output.Bytes(), &event); err != nil {
			t.Fatalf("decode: %v; log=%s", err, output.String())
		}
		return event
	}
	event := render(ByteConstraintFacts{Judgement: "peak_sum", NotReporting: []string{"w2"}, AllOver: true, Held: true, OverShare: 3})
	for field, value := range map[string]any{
		"byte_constraint_judgement": "peak_sum", "byte_constraint_not_reporting": []any{"w2"},
		"byte_constraint_all_over": true, "byte_constraint_held": true, "byte_constraint_over_share": float64(3),
	} {
		if !reflect.DeepEqual(event[field], value) {
			t.Fatalf("event[%q] = %#v, want %#v; event=%#v", field, event[field], value, event)
		}
	}
	event = render(ByteConstraintFacts{Judgement: "occupancy"})
	for field, value := range map[string]any{
		"byte_constraint_judgement": "occupancy", "byte_constraint_all_over": false, "byte_constraint_held": false, "byte_constraint_over_share": float64(0),
	} {
		if !reflect.DeepEqual(event[field], value) {
			t.Fatalf("event[%q] = %#v, want %#v present at its zero", field, event[field], value)
		}
	}
}
