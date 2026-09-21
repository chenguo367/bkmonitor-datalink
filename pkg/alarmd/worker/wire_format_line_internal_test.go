// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// countingSink takes or refuses every batch, as told.
type countingSink struct{ err error }

func (sink countingSink) WriteBatch(context.Context, []contract.TriggerEventV1) error {
	return sink.err
}

// The event_acked line says how many of the batch went out as each wire
// format, from the word each event carries, whether or not the sink took
// the batch: a batch the broker refused still was what it was. An event
// with no word is counted under the fold's name, never under an empty key.
func TestTheACKLineCountsTheBatchByWireFormat(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "taken", err: nil},
		{name: "refused", err: errors.New("broker refused")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorded := []observability.Observation{}
			coordinator := &SlotExecutionCoordinator{ports: Ports{
				Events: countingSink{err: testCase.err},
				Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					recorded = append(recorded, observation)
				}),
			}}
			events := []contract.TriggerEventV1{
				{WireFormat: contract.WireFormatPythonCompatible, PlanRef: contract.RuntimePlanRefV1{StrategyID: "4101"}},
				{WireFormat: contract.WireFormatPythonCompatible, PlanRef: contract.RuntimePlanRefV1{StrategyID: "4101"}},
				{WireFormat: contract.WireFormatStandardRawEvent, PlanRef: contract.RuntimePlanRefV1{StrategyID: "4102"}},
				{PlanRef: contract.RuntimePlanRefV1{StrategyID: "4103"}},
			}
			err := coordinator.writeEvents(context.Background(), execution.OperationNormal, events)
			if (err != nil) != (testCase.err != nil) {
				t.Fatalf("writeEvents() error = %v, want an error iff the sink refused", err)
			}
			var acked *observability.Observation
			for index := range recorded {
				if recorded[index].Stage == observability.StageEventACKed {
					acked = &recorded[index]
				}
			}
			if acked == nil {
				t.Fatalf("no event_acked observation among %+v", recorded)
			}
			want := observability.OutputWireFormatCounts{
				contract.WireFormatPythonCompatible: 2, contract.WireFormatStandardRawEvent: 1, observability.WireFormatOther: 1,
			}
			if len(acked.OutputWireFormats) != len(want) {
				t.Fatalf("counts = %v, want %v", acked.OutputWireFormats, want)
			}
			for format, count := range want {
				if acked.OutputWireFormats[format] != count {
					t.Fatalf("counts = %v, want %v", acked.OutputWireFormats, want)
				}
			}
		})
	}
}

// The evaluation line names the format the Plan's events go out as, resolved
// the way the sink resolves it: a Plan with no revision and no word is
// Python-compatible, and the historical word resolves to the standard raw
// event rather than being printed as itself.
func TestTheEvaluationLineNamesTheResolvedWireFormat(t *testing.T) {
	plan := noDataWiredPlan(t)
	if got := planWireFormat(plan); got != contract.WireFormatPythonCompatible {
		t.Fatalf("an unrevisioned Plan with no word resolves to %q, want %q", got, contract.WireFormatPythonCompatible)
	}
	if got := planWireFormat(execution.DuePlan{}); got != "" {
		t.Fatalf("a due Plan without a compiled Plan names %q, want nothing", got)
	}
	recorded := []observability.Observation{}
	stream := noDataWiredStream(t, plan, &emptyNoDataStore{})
	stream.coordinator.ports.Observer = observability.ObserverFunc(
		func(_ context.Context, observation observability.Observation) {
			recorded = append(recorded, observation)
		})
	stream.observeCompletionOnlyPlan(context.Background(), plan, execution.EvaluationResult{Result: observability.ResultSuccess})
	if len(recorded) != 1 || recorded[0].Stage != observability.StageEvaluationCompleted ||
		recorded[0].OutputWireFormat != contract.WireFormatPythonCompatible || recorded[0].Trace.StrategyID != plan.Identity.StrategyID {
		t.Fatalf("evaluation line = %+v, want the Plan's resolved wire format beside its strategy", recorded)
	}
}
