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
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// steppedCompatiblePlanV2 is a Plan detected every minute over four-minute
// windows, publishing the compatibility protocol with its context.
func steppedCompatiblePlanV2(t *testing.T, levels []contract.LevelIRV2) *strategy.CompiledPlan {
	t.Helper()
	plan := compilePlanV2WithOutput(t, levels, func(p *contract.EvaluationPlanV2) {
		p.StrategyIR.ExecutionSemantics.AggregationInterval = 240
		p.WireFormat = contract.WireFormatPythonCompatible
		p.LegacyOutput = &contract.LegacyOutputContext{
			Strategy:        json.RawMessage(`{"id":1001,"bk_biz_id":2,"update_time":1756684800}`),
			DimensionFields: []string{"host"}, ItemID: "1",
		}
	})
	if !plan.PublishesCompatibleProtocol() || !plan.CompatibleOffBoundary(300) || plan.CompatibleOffBoundary(480) {
		t.Fatal("the fixture is not a stepped Plan publishing the compatibility protocol")
	}
	return plan
}

func anomalousRequestV2(t *testing.T, plan *strategy.CompiledPlan, source int64, points map[int64]bool) EvaluationRequestV2 {
	t.Helper()
	return requestV2(t, plan, source, []DetectionFact{factV2(plan.Levels().At(0), DetectionAnomalous)}, []LevelHistory{{
		LevelID: 5, View: pointHistory{step: 60, points: points},
	}}, activeFactsV2(t, plan, source))
}

// The detections of a stepped Plan between two aggregation boundaries have no
// message on the compatibility protocol: decided and not built. The one on
// the boundary is built, and names the anomalies at windows the consumer
// counts - those on the boundaries - while it decided on all of them.
func TestASteppedCompatiblePlanSendsOnlyItsBoundaryDetections(t *testing.T) {
	levels := []contract.LevelIRV2{levelV2(5, 9, 3, 2, 2, nil)}
	plan := steppedCompatiblePlanV2(t, levels)

	off, err := EvaluateV2(anomalousRequestV2(t, plan, 420, map[int64]bool{300: true, 360: true, 420: true}))
	if err != nil {
		t.Fatal(err)
	}
	if off.RecordResult != contract.LevelResultAbnormal || off.TriggerEvent != nil || off.WithoutMessageFormat != contract.WireFormatPythonCompatible {
		t.Fatalf("between boundaries: record %q event %+v format %q, want an anomaly decided and not built", off.RecordResult, off.TriggerEvent, off.WithoutMessageFormat)
	}

	on, err := EvaluateV2(anomalousRequestV2(t, plan, 480, map[int64]bool{360: true, 420: true, 480: true}))
	if err != nil {
		t.Fatal(err)
	}
	if on.RecordResult != contract.LevelResultAbnormal || on.TriggerEvent == nil || on.TriggerEvent.LegacyOutput == nil || on.WithoutMessageFormat != "" {
		t.Fatalf("on the boundary: record %q event %+v, want the anomaly built with its context", on.RecordResult, on.TriggerEvent)
	}
	if got := on.TriggerEvent.LegacyOutput.AnomalyTimestamps; !reflect.DeepEqual(got, []int64{480}) {
		t.Fatalf("anomaly timestamps %v, want only the one on the boundary", got)
	}

	// A standard Plan detected every step sends every detection.
	standard := compilePlanV2WithOutput(t, levels, func(p *contract.EvaluationPlanV2) {
		p.StrategyIR.ExecutionSemantics.AggregationInterval = 240
		p.WireFormat = contract.WireFormatStandardRawEvent
		p.StrategyRef.SnapshotRevision = 1
		p.StrategyIR.StrategyRef.SnapshotRevision = 1
	})
	native, err := EvaluateV2(anomalousRequestV2(t, standard, 420, map[int64]bool{300: true, 360: true, 420: true}))
	if err != nil {
		t.Fatal(err)
	}
	if native.TriggerEvent == nil || native.WithoutMessageFormat != "" {
		t.Fatalf("a standard Plan's detection between boundaries: event %+v format %q, want it sent", native.TriggerEvent, native.WithoutMessageFormat)
	}
}
