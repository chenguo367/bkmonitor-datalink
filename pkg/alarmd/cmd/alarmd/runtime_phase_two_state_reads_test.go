// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import "testing"

// Through the production processes, a Worker's /metrics says what its State
// store read Redis for (014 next9 1): a round's preflight of the series the
// cache does not hold, under runtime, and its Plan's gap key, under gap_load,
// a key a read, every round. Before, nothing told the reads apart.
func TestAWorkersStateReadsAreServedByPurpose(t *testing.T) {
	asked := true
	full := openSegmentSkipFull(t, &asked)
	read := func(family, purpose string) float64 {
		t.Helper()
		got, served := servedSample(t, full.recorder, family, map[string]string{"command": "mget", "purpose": purpose})
		if !served {
			t.Fatalf("/metrics serves no %s{command=\"mget\",purpose=%q}", family, purpose)
		}
		return got
	}
	full.round()
	if runtime := read("alarmd_state_redis_reads_total", "runtime"); runtime == 0 {
		t.Fatal("the first round's preflight read nothing under runtime, want the series the cache held none of")
	}
	gaps, keys := read("alarmd_state_redis_reads_total", "gap_load"), read("alarmd_state_redis_read_keys_total", "gap_load")
	full.round()
	if reads, keyed := read("alarmd_state_redis_reads_total", "gap_load")-gaps,
		read("alarmd_state_redis_read_keys_total", "gap_load")-keys; reads == 0 || keyed != reads {
		t.Fatalf("a round read the gap key %v times, %v keys; want a read and a key each", reads, keyed)
	}
}
