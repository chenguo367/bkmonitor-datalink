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

// Through the production processes, a stable member its rounds leave
// unwritten is read back once and served from the State cache on every round
// after: its cache let go as a takeover lets it go, the next round reads it
// back for nothing kept and keeps it, compact, and each round after finds
// it in the cache and reads nothing of it back.
func TestAStableMemberItsRoundsLeaveUnwrittenIsServedFromTheCacheRoundAfterRound(t *testing.T) {
	asked := true
	full := openSegmentSkipFull(t, &asked)
	for round := 0; round < 8 && full.skipped() == 0; round++ {
		full.round()
	}
	if full.skipped() == 0 {
		t.Fatal("no round left the quiet series' write out")
	}
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
	// Let go and read back again, past the day's sampled round should the
	// round meet it: it writes its member whole and reads it back besides.
	// Twice again at most - the round is once a day; a build that samples
	// every round fails here rather than spinning.
	var kept, absent float64
	for tries, sampled := 0, -1.0; sampled != full.sampled(); tries++ {
		if tries > 2 {
			t.Fatalf("the round after the cache was let go ran as the day's sampled round %d times running, "+
				"want one at most: a sampled round is once a day", tries)
		}
		for _, queryGroup := range full.worker.ownedQueryGroups() {
			full.worker.dependencies.DropStateCache(queryGroup)
		}
		kept, absent = cell("alarmd_state_cache_readback_inserted_total", "result", state.StateCacheReadbackKept),
			cell("alarmd_state_cache_fallback_mget_total", "reason", state.StateCacheFallbackAbsent)
		sampled = full.sampled()
		full.round()
	}
	if got := cell("alarmd_state_cache_readback_inserted_total", "result", state.StateCacheReadbackKept); got != kept+1 ||
		cell("alarmd_state_cache_fallback_mget_total", "reason", state.StateCacheFallbackAbsent) != absent+1 {
		t.Fatalf("the round after the cache was let go kept %v values read back, read %v back for nothing kept; want the quiet series'",
			got-kept, cell("alarmd_state_cache_fallback_mget_total", "reason", state.StateCacheFallbackAbsent)-absent)
	}
	passed := 0
	for round := 0; round < 6; round++ {
		skipped, hits, read := full.skipped(), cell("alarmd_state_cache_lookups_total", "result", state.StateCacheHit), fallbacks()
		sampled := full.sampled()
		full.round()
		if full.sampled() != sampled {
			// The day's sampled round: its member written whole, by design.
			// Once at most: a build sampling every round would pass a case
			// that held no round to the skip.
			if passed++; passed > 1 {
				t.Fatalf("round %d ran as the day's sampled round, the %d-th of the case's: want one at most, a sampled round is once a day",
					round, passed)
			}
			continue
		}
		if full.skipped() != skipped+1 {
			t.Fatalf("round %d left %v writes out, want the quiet series'", round, full.skipped()-skipped)
		}
		if got := cell("alarmd_state_cache_lookups_total", "result", state.StateCacheHit); got != hits+1 || fallbacks() != read {
			t.Fatalf("round %d: %v hits and %v members read back, want the quiet series served from the cache", round, got-hits,
				fallbacks()-read)
		}
	}
	if entries := cell("alarmd_state_cache_entries", "kind", state.StateCacheEntryCompact); entries != 1 ||
		cell("alarmd_state_cache_entries", "kind", state.StateCacheEntryReadback) != 0 {
		t.Fatalf("%v compact entries held, want the quiet series' kept compact", entries)
	}
}
