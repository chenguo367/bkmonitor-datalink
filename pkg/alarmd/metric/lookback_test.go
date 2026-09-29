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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// lookbackSources is how many source labels a process counts: every source a
// query can be compiled from, and mixed and other.
func lookbackSources() int { return len(controlplane.SupportedSourceSemantics) + 2 }

// lookbackSeriesUpperBounds is each lookback family's series count once
// bound, by the family's short name.
func lookbackSeriesUpperBounds() map[string]int {
	sources, rungs := lookbackSources(), len(lookback.RungNames)
	return map[string]int{
		"lookback_first_reads_total":                 sources,
		"lookback_samples_total":                     sources * len(lookback.SampleOutcomes),
		"lookback_rechecks_total":                    sources * rungs * len(lookback.RecheckOutcomes),
		"lookback_changed_windows_total":             sources * rungs,
		"lookback_changes_total":                     sources * rungs * len(lookback.Changes),
		"lookback_completion_total":                  sources * len(lookback.AgeBuckets),
		"lookback_probes_total":                      sources * len(lookback.ProbeOutcomes),
		"lookback_empty_first_reads_total":           sources * len(lookback.EmptyFirstReadOutcomes),
		"lookback_empty_first_read_completion_total": sources * len(lookback.AgeBuckets),
		"lookback_completion_max_seconds":            sources,
		"lookback_groups":                            sources * len(lookback.DepthLabels),
		"lookback_rest_seconds":                      sources,
		"lookback_first_read_bytes_total":            sources,
		"lookback_recheck_bytes_total":               sources,
		"lookback_unknown_lookback_total":            sources,
		"lookback_coverage":                          2,
		"lookback_pending":                           2,
		"lookback_preemptions_total":                 sources * rungs,
		"lookback_yield_releases_total":              sources,
		"lookback_yield_release_seconds_total":       sources,
		"lookback_yield_release_max_seconds":         sources,
		// Every reason the scheduler refuses with, and other.
		"lookback_permit_refusals_total": len(scheduler.LookbackRefusals) + 1,
		"lookback_faults_total":          len(lookback.Faults),
	}
}

// Not running emits nothing; running emits every cell of a production
// engine's counts, zero included, and each count under its own words.
func TestTheLookbackCollectorEmitsEveryCellOnceBound(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	if series := gatherFamily(t, r, "bkmonitor_alarmd_lookback_samples_total"); len(series) != 0 {
		t.Fatalf("a process not running the lookback emitted %v", series)
	}
	engine, err := lookback.New(lookback.Options{Refusals: scheduler.LookbackRefusals,
		Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, nil
		},
		Permit: func() (func(), <-chan struct{}, string) { return nil, nil, scheduler.LookbackRefusedWaiting },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 7 }})
	if err != nil {
		t.Fatal(err)
	}
	stats := engine.Stats()
	logs := controlplane.SupportedSourceSemantics[1]
	stats.Sources[logs].Samples[lookback.OutcomeCaptured] = 4
	stats.Sources[logs].Changes[lookback.RungNames[1]][lookback.ChangePointsAdded] = 7
	stats.Sources[logs].Completion["le_300s"] = 2
	stats.Sources[logs].Preempted[lookback.RungNames[0]] = 5
	stats.Sources[logs].Probes[lookback.ProbeChanged] = 6
	stats.Sources[logs].EmptyFirstReads[lookback.EmptyArrived] = 8
	stats.Sources[logs].EmptyFirstReadCompletion["le_600s"] = 3
	entry := stats.Sources[logs]
	entry.RecheckBytes = 2048
	stats.Sources[logs] = entry
	stats.PermitRefusals[scheduler.LookbackRefusedWaiting] = 9
	stats.Pending, stats.PendingBytes = 3, 4096
	r.SetLookbackSource(func() lookback.Stats { return stats })
	for name, n := range lookbackSeriesUpperBounds() {
		if got := len(gatherFamily(t, r, "bkmonitor_alarmd_"+name)); got != n {
			t.Fatalf("%s has %d series, want every cell: %d", name, got, n)
		}
	}
	value := func(family string, want map[string]string) float64 {
		for _, m := range gatherFamily(t, r, family) {
			matched := true
			for _, label := range m.GetLabel() {
				if expected, named := want[label.GetName()]; named && expected != label.GetValue() {
					matched = false
				}
			}
			if matched {
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue()
				}
				return m.GetGauge().GetValue()
			}
		}
		t.Fatalf("%s has no series %v", family, want)
		return 0
	}
	if got := value("bkmonitor_alarmd_lookback_samples_total", map[string]string{"source": logs, "outcome": lookback.OutcomeCaptured}); got != 4 {
		t.Fatalf("captured = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_changes_total", map[string]string{"source": logs, "rung": lookback.RungNames[1], "class": lookback.ChangePointsAdded}); got != 7 {
		t.Fatalf("points added = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_completion_total", map[string]string{"source": logs, "age": "le_300s"}); got != 2 {
		t.Fatalf("completion = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_probes_total", map[string]string{"source": logs, "outcome": lookback.ProbeChanged}); got != 6 {
		t.Fatalf("deep rechecks changed = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_empty_first_reads_total", map[string]string{"source": logs, "outcome": lookback.EmptyArrived}); got != 8 {
		t.Fatalf("empty first reads arrived = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_empty_first_read_completion_total", map[string]string{"source": logs, "age": "le_600s"}); got != 3 {
		t.Fatalf("empty first read completion = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_pending", map[string]string{"what": "bytes"}); got != 4096 {
		t.Fatalf("pending bytes = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_preemptions_total", map[string]string{"source": logs, "rung": lookback.RungNames[0]}); got != 5 {
		t.Fatalf("preemptions = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_permit_refusals_total", map[string]string{"reason": scheduler.LookbackRefusedWaiting}); got != 9 {
		t.Fatalf("refusals = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_coverage", map[string]string{"what": "owned"}); got != 7 {
		t.Fatalf("owned = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_groups", map[string]string{"source": logs, "depth": "1"}); got != 0 {
		t.Fatalf("groups at depth 1 = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_recheck_bytes_total", map[string]string{"source": logs}); got != 2048 {
		t.Fatalf("recheck bytes = %v", got)
	}
}
