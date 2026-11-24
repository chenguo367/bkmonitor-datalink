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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// Half the Query Groups moved away, no group that stays is put out whole
// (014 next9 1): the once-a-minute resize may come while the moved groups'
// values are still held - the process lets them go when it hands them over -
// and a group that stays writes a value then; once they are let go the
// budget comes down with what they held, and a group that stays writes again.
// Before, the budget was the series held, which stopped counting a moved
// group at once: a resize in that minute set it under what the cache still
// held, and the next group to write was put out over the budget.
func TestHalfTheGroupsMovedAwayPutNoGroupThatStaysOut(t *testing.T) {
	const per = 15
	staying := []execution.QueryGroupIdentity{"q-a", "q-b"}
	moving := []execution.QueryGroupIdentity{"q-c", "q-d"}
	groups := append(append([]execution.QueryGroupIdentity(nil), staying...), moving...)
	base := func(group execution.QueryGroupIdentity) int {
		for index, of := range groups {
			if of == group {
				return 1000 * (index + 1)
			}
		}
		return 0
	}
	writer := newCacheRig(t, 0)
	for _, group := range groups {
		writeGroup(t, writer, group, 60, base(group), per, 0)
	}
	readBack := func(rig *cacheRig, group execution.QueryGroupIdentity, at execution.EvaluationTime) {
		t.Helper()
		indexes := make([]int, per)
		for index := range indexes {
			indexes[index] = base(group) + index
		}
		contract, fence, version := groupSlot(group, at)
		ctx := segment.WithSource(context.Background(), readbackCover(rig.now, true, indexes...))
		rig.loadRequest(ctx, execution.StatePreflightRequest{Contract: contract, Items: groupItems(version, base(group), per),
			OwnerFence: fence})
	}
	// What one group's members take compact.
	probe := segmentRig(t, writer.backend, 1<<20, nil)
	readBack(probe, staying[0], 120)
	one := probe.store.StateCacheStats().Bytes
	// A floor one group fits, and rounds until the budget has taken all four.
	rig := segmentRig(t, writer.backend, one*11/10, nil)
	at := execution.EvaluationTime(60)
	for round := 0; round < 3 && rig.store.StateCacheStats().QueryGroups < len(groups); round++ {
		rig.now, at = rig.now.Add(time.Minute), at+60
		for _, group := range groups {
			readBack(rig, group, at)
		}
		rig.store.SizeStateCache(0) // held: all four groups
	}
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != len(groups) {
		t.Fatalf("fixture: %d groups held of %d (budget %d, bytes %d)", stats.QueryGroups, len(groups), stats.BudgetBytes, stats.Bytes)
	}
	before := rig.store.StateCacheStats()

	// The resize before the moved groups are let go, and a group that stays
	// writing a value that changed.
	rig.now, at = rig.now.Add(time.Minute), at+60
	rig.store.SizeStateCache(0) // held: the two that stay
	writeGroup(t, rig, staying[0], at, base(staying[0]), 1, 1)
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != len(groups) ||
		stats.StateDepartures[StateCacheDepartOverBudget] != before.StateDepartures[StateCacheDepartOverBudget] {
		t.Fatalf("a group that stays wrote a value, the moved ones still held: %d groups, departures %v; want none put out",
			stats.QueryGroups, stats.StateDepartures)
	}

	// The moved groups let go, the budget down with them, and another write.
	for _, group := range moving {
		rig.store.DropStateCacheQueryGroup(group)
	}
	rig.now, at = rig.now.Add(time.Minute), at+60
	rig.store.SizeStateCache(0) // held: the two that stay
	if lowered := rig.store.StateCacheStats().BudgetBytes; lowered >= before.BudgetBytes {
		t.Fatalf("the budget with half the groups let go is %d, want it below the %d of all four", lowered, before.BudgetBytes)
	}
	writeGroup(t, rig, staying[1], at, base(staying[1]), 1, 1)
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != len(staying) ||
		stats.StateDepartures[StateCacheDepartOverBudget] != before.StateDepartures[StateCacheDepartOverBudget] {
		t.Fatalf("a group that stays wrote a value, the budget down: %d groups, departures %v; want both held, none put out",
			stats.QueryGroups, stats.StateDepartures)
	}
}

// groupSlot is group's Slot at at: its contract, owner fence and apply
// version.
func groupSlot(group execution.QueryGroupIdentity, at execution.EvaluationTime) (execution.FrozenExecutionContractRef,
	execution.OwnerFence, execution.ApplyVersion) {
	contract := frozenRef()
	contract.Slot.QueryGroup, contract.Slot.EvaluationTime = group, at
	return contract, execution.OwnerFence{QueryGroup: group, OwnerID: "worker-1", OwnerEpoch: 3, LeaseToken: "lease-token"},
		execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: at, SlotDigest: "slot"}
}

// groupItems is count series of a group, from the series index from on.
func groupItems(version execution.ApplyVersion, from, count int) []execution.StatePreflightItem {
	items := make([]execution.StatePreflightItem, count)
	for index := range items {
		items[index] = execution.StatePreflightItem{Identity: seriesIdentity(from + index), ApplyVersion: version}
	}
	return items
}

// writeGroup reads, then writes at revision, count series of group's Slot at
// at, from the series index from on.
func writeGroup(t *testing.T, rig *cacheRig, group execution.QueryGroupIdentity, at execution.EvaluationTime, from, count int,
	revision uint64) {
	t.Helper()
	ctx := context.Background()
	contract, fence, version := groupSlot(group, at)
	items := groupItems(version, from, count)
	mutations := make([]execution.StateMutation, count)
	for index := range mutations {
		mutations[index] = seriesMutation(t, items[index].Identity, version, revision, "")
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
