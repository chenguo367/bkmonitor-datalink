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

// A process whose one Query Group's round is larger than the State cache's
// floor, the cache holding nothing, sizes the budget for that round at the
// first resize after it (014 next9 1): through Execute on the real State
// store, the group's rounds kept out past the floor and read back, the
// once-a-minute resize after them sets the budget to the round they were
// priced at, and the next round comes in whole, the one after served from
// the cache. Before, the budget was the series held at the mean of the
// entries held, the floor while there were none: no entry of the group ever
// came in to be measured, and it was read back every round for good.
func TestAGroupLargerThanTheFloorIsSizedForAtTheFirstResizeWithNothingHeld(t *testing.T) {
	const hosts = 300
	// Far under the round of 300 members, a page and a mark.
	const floor = 16 << 10
	rig := newKeptOutRigWithin(t, hosts, floor)
	rig.warm(hosts)
	stats := rig.step(keptOutRoomOpen)
	if stats.BudgetBytes != floor || stats.Entries[state.StateCacheEntryCompact] != 0 ||
		stats.Lookups[state.StateCacheMissOverBudget] == 0 {
		t.Fatalf("fixture: budget %d, entries %v, lookups %v; want the group kept out over the floor %d, read back",
			stats.BudgetBytes, stats.Entries, stats.Lookups, floor)
	}

	rig.store.SizeStateCache(0)
	if sized := rig.store.StateCacheStats().BudgetBytes; sized <= floor {
		t.Fatalf("the resize after the group's rounds left the budget at %d, the floor %d; want it sized for the group's round", sized, floor)
	}
	// The round that comes in looks its members up kept out still, and is
	// taken in at its first write.
	if stats = rig.step(keptOutRoomOpen); stats.Entries[state.StateCacheEntryCompact] != hosts {
		t.Fatalf("with the budget sized the group holds %v (budget %d, departures %v); want all %d members in", stats.Entries,
			stats.BudgetBytes, stats.Departures, hosts)
	}
	before := stats
	stats = rig.step(keptOutRoomOpen)
	if served := stats.Lookups[state.StateCacheHit] - before.Lookups[state.StateCacheHit]; served < hosts {
		t.Fatalf("the round after it came in served %d members from the cache, want all %d", served, hosts)
	}
	missed := stats.Lookups[state.StateCacheMissOverBudget] - before.Lookups[state.StateCacheMissOverBudget]
	left := stats.Departures[state.StateCacheDepartOverBudget] - before.Departures[state.StateCacheDepartOverBudget]
	if missed != 0 || left != 0 {
		t.Fatalf("the round after it came in: %d lookups over the budget, %d departures over it; want none", missed, left)
	}
}
