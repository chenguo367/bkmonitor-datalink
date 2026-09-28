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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
)

// lookbackSeriesUpperBounds is each lookback family's series count once
// bound, by the family's short name.
func lookbackSeriesUpperBounds() map[string]int {
	sources, tiers := len(lookback.Sources), len(lookback.TierNames)
	return map[string]int{
		"lookback_samples_total":          sources * len(lookback.SampleOutcomes),
		"lookback_rechecks_total":         sources * tiers * len(lookback.RecheckOutcomes),
		"lookback_compared_buckets_total": sources * tiers,
		"lookback_compared_windows_total": sources * tiers * 2,
		"lookback_differences_total":      sources * tiers * len(lookback.Differences),
		"lookback_judgments_total":        sources * tiers * len(lookback.Judgments),
		"lookback_series_total":           sources * tiers * 2,
		"lookback_windows_by_age_total":   sources * len(lookback.AgeBuckets) * 2,
		"lookback_pending":                3,
	}
}

// Not running emits nothing; running emits every cell, zero included, and
// each count under its own words.
func TestTheLookbackCollectorEmitsEveryCellOnceBound(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	if series := gatherFamily(t, r, "bkmonitor_alarmd_lookback_samples_total"); len(series) != 0 {
		t.Fatalf("a process not running the lookback emitted %v", series)
	}
	stats := lookback.Stats{
		Samples:     map[string]map[string]uint64{lookback.SourceLogSearch: {lookback.OutcomeCaptured: 4}},
		Differences: map[string]map[string]map[string]uint64{lookback.SourceCollectorLog: {"t210": {lookback.DiffZeroToNonzero: 7}}},
		ByAge:       map[string]map[string]map[string]uint64{lookback.SourceLogSearch: {"le_240s": {"yes": 2}}},
		Pending:     3, PendingBytes: 4096, MemoryBytes: 1 << 20,
	}
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
	if got := value("bkmonitor_alarmd_lookback_samples_total", map[string]string{"source": lookback.SourceLogSearch, "outcome": lookback.OutcomeCaptured}); got != 4 {
		t.Fatalf("captured = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_differences_total", map[string]string{"source": lookback.SourceCollectorLog, "tier": "t210", "class": lookback.DiffZeroToNonzero}); got != 7 {
		t.Fatalf("zero_to_nonzero = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_windows_by_age_total", map[string]string{"source": lookback.SourceLogSearch, "age": "le_240s", "differed": "yes"}); got != 2 {
		t.Fatalf("by age = %v", got)
	}
	if got := value("bkmonitor_alarmd_lookback_pending", map[string]string{"what": "bytes"}); got != 4096 {
		t.Fatalf("pending bytes = %v", got)
	}
}
