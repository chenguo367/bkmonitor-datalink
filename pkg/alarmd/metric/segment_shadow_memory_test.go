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

// The shadow's books are read at the scrape, every kind a cell - zero while
// nothing is held - and bound once.
func TestTheSegmentShadowMemoryIsReadAtTheScrapeEveryKindPresent(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	reading := SegmentShadowMemoryReading{}
	if err := recorder.BindSegmentShadowMemory(func() SegmentShadowMemoryReading { return reading }); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"plans", "entries_open", "entries_closed", "bytes_estimated"} {
		if samples := redisSamples(t, recorder, "alarmd_worker_segment_shadow_memory", map[string]string{"kind": kind}); len(samples) != 1 ||
			samples[0].GetGauge().GetValue() != 0 {
			t.Fatalf("kind %s: %d samples, want one at zero with nothing held", kind, len(samples))
		}
	}
	reading = SegmentShadowMemoryReading{Plans: 2, Open: 280, Closed: 20, Bytes: 48_512}
	for kind, want := range map[string]float64{"plans": 2, "entries_open": 280, "entries_closed": 20, "bytes_estimated": 48_512} {
		if samples := redisSamples(t, recorder, "alarmd_worker_segment_shadow_memory", map[string]string{"kind": kind}); len(samples) != 1 ||
			samples[0].GetGauge().GetValue() != want {
			t.Fatalf("kind %s read %v, want %v at the scrape", kind, samples, want)
		}
	}
	if err := recorder.BindSegmentShadowMemory(func() SegmentShadowMemoryReading { return reading }); err == nil {
		t.Fatal("bound twice")
	}
}
