// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// The worker's end-to-end rig writes State under its lease's fence, as
// production configures the store (014 next9 1): a round's writes are taken
// into the State cache, and the next round's preflight is served from it,
// through Execute on the real State store. Before, the rig's store had no
// fence keys: its writes went unfenced, the cache kept none of them, and no
// case of this package reached a value written back into the cache.
func TestARigRoundsWritesAreKeptAndServeTheRoundAfter(t *testing.T) {
	const hosts = 40
	rig := newThresholdShadowRig(t, hosts, 16<<20)
	// From the round after a sampled one: a sampled round reads every member
	// back whatever the cache holds.
	at := sampledAfter(t, frozenContract().Slot.EvaluationTime)
	round := func() state.StateCacheStats {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
		return rig.store.StateCacheStats()
	}
	first := round()
	if kept := first.Entries[state.StateCacheEntryWritten] + first.Entries[state.StateCacheEntryCompact]; kept != hosts {
		t.Fatalf("after a round writing %d hosts the cache holds %v (departures %v); want every value written kept", hosts,
			first.Entries, first.Departures)
	}
	second := round()
	if served := second.Lookups[state.StateCacheHit] - first.Lookups[state.StateCacheHit]; served < hosts {
		t.Fatalf("the round after served %d of its %d members from the cache (lookups %v), want all", served, hosts, second.Lookups)
	}
}
