// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// Through the production processes, the value a round writes is kept in the
// State cache compact (014 next9 1, the first step of writes kept compact):
// the quiet series' first round writes it and the cache holds it compact,
// nothing as its bytes, and each round after is served it from the cache and
// reads nothing of it back. Before, a written value was kept as its bytes,
// some four times its compact form.
func TestTheValueARoundWritesIsKeptCompactAndServedTheRoundsAfter(t *testing.T) {
	asked := true
	full := openSegmentSkipFull(t, &asked)
	cell := func(family, label, value string) float64 {
		t.Helper()
		got, served := servedSample(t, full.recorder, family, map[string]string{label: value})
		if !served {
			t.Fatalf("/metrics serves no %s{%s=%q}", family, label, value)
		}
		return got
	}
	fallbacks := func() float64 {
		sum := 0.0
		for _, reason := range state.StateCacheFallbackReasons {
			sum += cell("alarmd_state_cache_fallback_mget_total", "reason", reason)
		}
		return sum
	}
	full.round()
	if compact, bytes := cell("alarmd_state_cache_entries", "kind", state.StateCacheEntryCompact),
		cell("alarmd_state_cache_entries", "kind", state.StateCacheEntryWritten); compact != 1 || bytes != 0 {
		t.Fatalf("after the round that wrote the quiet series the cache holds %v compact and %v as bytes, want it compact",
			compact, bytes)
	}
	for round := 0; round < 4; round++ {
		hits, read := cell("alarmd_state_cache_lookups_total", "result", state.StateCacheHit), fallbacks()
		full.round()
		if got := cell("alarmd_state_cache_lookups_total", "result", state.StateCacheHit); got != hits+1 || fallbacks() != read {
			t.Fatalf("round %d: %v hits and %v members read back, want the quiet series served from the cache", round, got-hits,
				fallbacks()-read)
		}
	}
}
