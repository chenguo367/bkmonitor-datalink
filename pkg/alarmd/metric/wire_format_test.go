// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Every wire format is a series from startup, each ACK line adds its counts
// to its cells, a word the build does not name folds to _other, and lines
// that are not ACKs add nothing.
func TestOutputEventsByWireFormatAreSeriesFromStartupAndSumTheACKLines(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	const family = "bkmonitor_alarmd_output_events_by_wire_format_total"
	if before := gatherFamily(t, r, family); len(before) != len(observability.WireFormats) {
		t.Fatalf("%d series before any ACK, want every format (%d) so a standard_raw_event nobody sent reads as zero",
			len(before), len(observability.WireFormats))
	}
	ctx := context.Background()
	r.Observe(ctx, observability.Observation{
		Component: observability.ComponentOutput, Stage: observability.StageEventACKed, Result: observability.ResultSuccess,
		OutputWireFormats: observability.OutputWireFormatCounts{contract.WireFormatPythonCompatible: 12, contract.WireFormatStandardRawEvent: 1},
	})
	r.Observe(ctx, observability.Observation{
		Component: observability.ComponentOutput, Stage: observability.StageEventACKed, Result: observability.ResultDegraded,
		OutputWireFormats: observability.OutputWireFormatCounts{contract.WireFormatPythonCompatible: 3, "some_future_word": 2},
	})
	// The same counts on a line that is not an ACK are not this family's.
	r.Observe(ctx, observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
		OutputWireFormats: observability.OutputWireFormatCounts{contract.WireFormatStandardRawEvent: 100},
	})
	count := func(format string) float64 {
		return testutil.ToFloat64(r.phaseTwo.outputEventsByWireFormat.WithLabelValues(format))
	}
	for format, want := range map[string]float64{
		contract.WireFormatPythonCompatible: 15, contract.WireFormatStandardRawEvent: 1, observability.WireFormatOther: 2,
	} {
		if got := count(format); got != want {
			t.Fatalf("%s = %v, want %v", format, got, want)
		}
	}
	if after := gatherFamily(t, r, family); len(after) != len(observability.WireFormats) {
		t.Fatalf("%d series after the lines, want the same %d: an unknown word folds rather than creating a cell", len(after), len(observability.WireFormats))
	}
}

// The leader's Catalog composition publishes its Plans by wire format, every
// format present even at zero, from the composition the source hands it.
func TestCatalogPlansByWireFormatArePublishedFromTheComposition(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	composition := &controlplane.CatalogComposition{PlansByWireFormat: map[string]int{
		contract.WireFormatPythonCompatible: 12, contract.WireFormatStandardRawEvent: 0, observability.WireFormatOther: 0,
	}}
	r.SetCatalogCompositionSource(func() *controlplane.CatalogComposition { return composition })
	const family = "bkmonitor_alarmd_catalog_plans_by_wire_format"
	series := gatherFamily(t, r, family)
	if len(series) != len(observability.WireFormats) {
		t.Fatalf("%d series, want one per format (%d) with the zeros published", len(series), len(observability.WireFormats))
	}
	values := map[string]float64{}
	for _, metric := range series {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "format" {
				values[label.GetValue()] = metric.GetGauge().GetValue()
			}
		}
	}
	if values[contract.WireFormatPythonCompatible] != 12 || values[contract.WireFormatStandardRawEvent] != 0 {
		t.Fatalf("values = %v, want 12 python_compatible and a published 0 standard_raw_event", values)
	}
}
