// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// The segments' families are read from the store at the scrape, every cell
// present from the start - a zero before anything is read - and bound once.
func TestTheSegmentStoreFamiliesAreReadAtTheScrapeEveryCellPresent(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	stats := state.SegmentStats{}
	if err := recorder.BindSegmentStore(func() state.SegmentStats { return stats }); err != nil {
		t.Fatal(err)
	}
	cells := []map[string]string{{"source": segment.SourceCache, "reason": segment.ReadHit}}
	for _, reason := range segment.ReadReasons {
		cells = append(cells, map[string]string{"source": segment.SourceRedis, "reason": reason})
	}
	for _, cell := range cells {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_read_total", cell); len(samples) != 1 || samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("read cell %v: %d samples, want one at zero", cell, len(samples))
		}
	}
	for _, result := range segment.WriteResults {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_write_total", map[string]string{"result": result}); len(samples) != 1 {
			t.Fatalf("write cell %s: %d samples", result, len(samples))
		}
	}
	for _, extension := range segment.Extensions {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_extend_total", map[string]string{"outcome": string(extension)}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("extend cell %s: %v, want one at zero", extension, samples)
		}
	}
	stats = state.SegmentStats{Reads: map[[2]string]uint64{{segment.SourceRedis, segment.ReadEvicted}: 4},
		Writes: map[string]uint64{segment.WriteUnknown: 2}, Drift: 1, Plans: 3, Pages: 5, Bytes: 9_000,
		Extends: map[segment.Extension]uint64{segment.Extended: 7, segment.RefusedOffGrid: 2}}
	for extension, want := range map[segment.Extension]float64{segment.Extended: 7, segment.RefusedOffGrid: 2, segment.Within: 0} {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_extend_total", map[string]string{"outcome": string(extension)}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != want {
			t.Fatalf("extend cell %s: %v, want %v at the scrape", extension, samples, want)
		}
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_read_total", map[string]string{"source": segment.SourceRedis,
		"reason": segment.ReadEvicted}); len(samples) != 1 || samples[0].GetCounter().GetValue() != 4 {
		t.Fatalf("evicted reads %v, want 4 at the scrape", samples)
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_write_total", map[string]string{"result": segment.WriteUnknown}); samples[0].GetCounter().GetValue() != 2 {
		t.Fatalf("unknown writes %v", samples)
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_cache_drift_total", nil); len(samples) != 1 || samples[0].GetCounter().GetValue() != 1 {
		t.Fatalf("drift %v", samples)
	}
	for kind, want := range map[string]float64{"plans": 3, "pages": 5, "bytes": 9_000} {
		if samples := redisSamples(t, recorder, "alarmd_worker_segment_shadow_memory", map[string]string{"kind": kind}); len(samples) != 1 ||
			samples[0].GetGauge().GetValue() != want {
			t.Fatalf("kind %s read %v, want %v", kind, samples, want)
		}
	}
	if err := recorder.BindSegmentStore(func() state.SegmentStats { return stats }); err == nil {
		t.Fatal("bound twice")
	}
}
