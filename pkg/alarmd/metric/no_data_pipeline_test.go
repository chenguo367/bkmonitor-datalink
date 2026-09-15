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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The no-data observation reaches the counters through the recorder the process
// actually installs, not by being handed to the counter.
//
// Everything between the worker and the series is a place the observation can
// be dropped without anything failing. NormalizeComponentStage folds a pair it
// does not know to (_other, _other), which is a whitelist over an open input,
// and a counter fed from observations then sits at a computed zero forever --
// the same reading as a worker with nothing to report. The unit tests below
// hand the observation straight to the metric struct and cannot see any of it.
//
// So this goes in the front door: Recorder.Observe, the same call the runtime
// makes, and reads the series out of the registry the scrape reads.
func TestNoDataObservationsReachTheCountersThroughTheRecorder(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.2.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	ctx := context.Background()

	recorder.Observe(ctx, observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageNoDataDecided,
		Direction: observability.DirectionInternal, Result: observability.ResultSuccess,
		NoDataCensus: &observability.NoDataCensusFacts{Plans: 9},
	})
	recorder.Observe(ctx, observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageNoDataDecided,
		Direction: observability.DirectionInternal, Result: observability.ResultSuccess,
		NoDataSlot: &observability.NoDataSlotFacts{Outcome: string(nodata.OutcomeEvaluated), Plans: 9},
	})

	if got := recorderCounterValue(t, recorder, "bkmonitor_alarmd_worker_no_data_plans_seen_total"); got != 9 {
		t.Fatalf("census = %v through the recorder, want 9. Between the worker and the series the "+
			"observation passes a component/stage whitelist that folds what it does not know to "+
			"(_other, _other); a counter fed from observations then reads as a computed zero, which is "+
			"exactly what a worker with nothing to report reads as", got)
	}
	series := noDataSlotSeriesFrom(t, recorder)
	if series[string(nodata.OutcomeEvaluated)] != 9 {
		t.Fatalf("EVALUATED = %v through the recorder, want 9", series[string(nodata.OutcomeEvaluated)])
	}
}

// The pair the worker emits is one the observation catalog names.
//
// Stated against the normalizer itself, so the failure says which of the two
// halves is unregistered rather than only that a counter did not move.
func TestTheNoDataComponentStageSurvivesNormalisation(t *testing.T) {
	component, stage := observability.NormalizeComponentStage(
		observability.ComponentEvaluation, observability.StageNoDataDecided,
	)
	if component != observability.ComponentEvaluation || stage != observability.StageNoDataDecided {
		t.Fatalf("(%q, %q) normalised to (%q, %q): the pair the worker emits is not in the observation "+
			"catalog, so every no-data observation is folded away before it reaches a counter or a log",
			observability.ComponentEvaluation, observability.StageNoDataDecided, component, stage)
	}
}

// A Slot with no such Plan reports its zero.
//
// Skipping the census on an empty Slot would put the number back where it
// started: absent and zero would mean the same thing, and the question this
// answers is exactly which of the two a worker is in.
func TestASlotWithNoNoDataPlansStillReportsItsCensus(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.2.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	recorder.Observe(context.Background(), observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageNoDataDecided,
		Direction: observability.DirectionInternal, Result: observability.ResultSuccess,
		NoDataCensus: &observability.NoDataCensusFacts{Plans: 0},
	})

	if _, reported := recorderCounterSeries(t, recorder, "bkmonitor_alarmd_worker_no_data_plans_seen_total"); !reported {
		t.Fatal("no census series after a Slot that found no no-data Plan; absent and zero then mean the " +
			"same thing, which is the reading this counter exists to split")
	}
}

func recorderCounterSeries(t *testing.T, recorder *Recorder, name string) (float64, bool) {
	t.Helper()
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, series := range family.GetMetric() {
			return series.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

func recorderCounterValue(t *testing.T, recorder *Recorder, name string) float64 {
	t.Helper()
	value, reported := recorderCounterSeries(t, recorder, name)
	if !reported {
		t.Fatalf("no series named %s", name)
	}
	return value
}

// noDataSlotSeriesFrom reads the outcome family out of a recorder's registry.
func noDataSlotSeriesFrom(t *testing.T, recorder *Recorder) map[string]float64 {
	t.Helper()
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	read := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "bkmonitor_alarmd_worker_no_data_slot_plans_total" {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, pair := range series.GetLabel() {
				if pair.GetName() == "outcome" {
					read[pair.GetValue()] = series.GetCounter().GetValue()
				}
			}
		}
	}
	return read
}
