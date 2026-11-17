// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import "testing"

// A cost lands in the band of its milliseconds a second, a band's edge in the
// band above; every band is a cell from the start, read at the scrape, and
// the source is bound once.
func TestTheComputeCostIsBandedAndReadAtTheScrape(t *testing.T) {
	for milli, want := range map[uint64]string{0: "none", 1: "lt_10ms", 9: "lt_10ms", 10: "lt_100ms", 99: "lt_100ms",
		100: "lt_1s", 999: "lt_1s", 1000: "ge_1s"} {
		if got := ComputeCostBand(milli); got != want {
			t.Errorf("ComputeCostBand(%d) = %s, want %s", milli, got, want)
		}
	}
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	reading := ComputeCostReading{}
	if err := recorder.BindComputeCost(func() ComputeCostReading { return reading }); err != nil {
		t.Fatal(err)
	}
	for _, band := range ComputeCostBands {
		if samples := redisSamples(t, recorder, "alarmd_worker_compute_cost", map[string]string{"band": band}); len(samples) != 1 ||
			samples[0].GetGauge().GetValue() != 0 {
			t.Fatalf("band %s: %d samples, want one at zero", band, len(samples))
		}
	}
	reading = ComputeCostReading{Groups: map[string]uint64{"lt_1s": 3}, Seconds: map[string]float64{"lt_1s": 1.25}}
	if samples := redisSamples(t, recorder, "alarmd_worker_compute_cost", map[string]string{"band": "lt_1s"}); samples[0].GetGauge().GetValue() != 1.25 {
		t.Fatalf("lt_1s cost %v, want 1.25 at the scrape", samples)
	}
	if samples := redisSamples(t, recorder, "alarmd_worker_compute_cost_query_groups", map[string]string{"band": "lt_1s"}); samples[0].GetGauge().GetValue() != 3 {
		t.Fatalf("lt_1s groups %v, want 3", samples)
	}
	if err := recorder.BindComputeCost(func() ComputeCostReading { return reading }); err == nil {
		t.Fatal("bound twice")
	}
}
