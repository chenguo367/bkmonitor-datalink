// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The State cache lets go first what no round reads any more (014 next9 1):
// a Query Group whose latest whole round left most of its values untouched
// holds them until another group's write does not fit beside them, and then
// they go - reclaimed - and the write is taken in, the values the first
// group's rounds read staying, the second group served the round after.
// Before, the write did not fit and its group was put out over the budget,
// read back by MGET every round, while values no round read kept the room
// until they aged out.
func TestValuesNoRoundReadsAreLetGoBeforeAWritingGroupIsPutOut(t *testing.T) {
	const held, read, writing = 30, 5, 20
	// A value's charge, read off a cache that takes the round in whole.
	probe := newCacheRig(t, 1<<20)
	probe.round(first, held, 0)
	charge := probe.store.StateCacheStats().Bytes / held
	// Room for the first group's round and two values more: not for both
	// groups' rounds, and for the second's beside the values the first reads.
	rig := newCacheRig(t, charge*(held+2))
	rig.round(first, held, 0)
	if stats := rig.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact]+stats.Entries[StateCacheEntryWritten] != held {
		t.Fatalf("fixture: the first group holds %v, want all %d values", stats.Entries, held)
	}
	for _, at := range []execution.EvaluationTime{120, 180} {
		rig.now = rig.now.Add(time.Minute)
		rig.load(slot{at: at, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, read)
	}

	rig.now = rig.now.Add(time.Minute)
	other := frozenRef()
	other.Slot.QueryGroup, other.Slot.EvaluationTime = "q-other", 240
	fence := execution.OwnerFence{QueryGroup: other.Slot.QueryGroup, OwnerID: "worker-1", OwnerEpoch: 3, LeaseToken: "lease-token"}
	version := execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 240, SlotDigest: "slot"}
	writeOtherGroup(t, rig, other, fence, version, writing)
	stats := rig.store.StateCacheStats()
	// The departure word as the design names it, not as the constant says.
	if got := stats.StateDepartures["reclaimed"]; got != held-read {
		t.Fatalf("the second group's write let go %d values reclaimed (departures %v), want the %d no round read", got,
			stats.StateDepartures, held-read)
	}
	if kept := stats.Entries[StateCacheEntryCompact] + stats.Entries[StateCacheEntryWritten]; stats.QueryGroups != 2 || kept != read+writing ||
		stats.StateDepartures[StateCacheDepartOverBudget] != 0 {
		t.Fatalf("after the second group's write: %d groups, %v entries, departures %v; want both groups, the %d read and the %d written",
			stats.QueryGroups, stats.Entries, stats.StateDepartures, read, writing)
	}

	// The second group's round after, at its next Slot.
	rig.now = rig.now.Add(time.Minute)
	next, nextVersion := other, version
	next.Slot.EvaluationTime, nextVersion.EvaluationTime = 300, 300
	hits := rig.lookups(StateCacheHit)
	items := otherItems(nextVersion, writing)
	if _, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: next, Items: items,
		OwnerFence: fence}); err != nil {
		t.Fatal(err)
	}
	if served := rig.lookups(StateCacheHit) - hits; served != writing {
		t.Fatalf("the second group's next read was served %d of its %d values from the cache", served, writing)
	}
}

// otherItems is the second group's series, apart from the first's.
func otherItems(version execution.ApplyVersion, count int) []execution.StatePreflightItem {
	items := make([]execution.StatePreflightItem, count)
	for index := range items {
		items[index] = execution.StatePreflightItem{Identity: seriesIdentity(1000 + index), ApplyVersion: version}
	}
	return items
}

// writeOtherGroup reads, then writes, count series of the group the contract
// names, under fence.
func writeOtherGroup(t *testing.T, rig *cacheRig, contract execution.FrozenExecutionContractRef, fence execution.OwnerFence,
	version execution.ApplyVersion, count int) {
	t.Helper()
	ctx := context.Background()
	items := otherItems(version, count)
	mutations := make([]execution.StateMutation, count)
	for index := range mutations {
		mutations[index] = seriesMutation(t, items[index].Identity, version, 0, "")
	}
	if _, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: contract, Items: items, OwnerFence: fence}); err != nil {
		t.Fatal(err)
	}
	apply := testApplyFence()
	apply.Fence = fence
	result, err := rig.store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: contract, Retention: testRetention(),
		Items: mutations}, apply)
	if err != nil {
		t.Fatal(err)
	}
	requireAllStatus(t, result, execution.StateApplied)
}
