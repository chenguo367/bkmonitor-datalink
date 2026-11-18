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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// writeStateValues writes count Runtime State values of the frozen Slot
// through the store, as a Slot does: read, then a fenced write.
func writeStateValues(t *testing.T, store *ExecutionStore, count int) {
	t.Helper()
	ctx := context.Background()
	mutations := make([]execution.StateMutation, count)
	items := make([]execution.StatePreflightItem, count)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), applyVersion(), 0, "")
		items[index] = execution.StatePreflightItem{Identity: mutations[index].Identity, ApplyVersion: applyVersion()}
	}
	if _, err := store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: frozenRef(), Items: items,
		OwnerFence: testApplyFence().Fence}); err != nil {
		t.Fatalf("LoadRuntime() error = %v", err)
	}
	applied, err := store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: frozenRef(), Retention: testRetention(),
		Items: mutations}, testApplyFence())
	if err != nil {
		t.Fatalf("ApplyRuntimeFenced() error = %v", err)
	}
	requireAllStatus(t, applied, execution.StateApplied)
}

// A Runtime State value and a Plan's stable segment share the cache's budget
// and are each counted under their own kind: the segment's mark and page are
// the segments' share of the charge, the State values the rest; released,
// each kind's entries leave under it.
func TestTheStateCacheCountsStateValuesAndSegmentsApart(t *testing.T) {
	store := newSegmentRedis(t).store(t, 1<<20)
	writeStateValues(t, store, 3)
	values := store.StateCacheStats()
	if values.Bytes == 0 || values.SegmentBytes != 0 {
		t.Fatalf("after three State values: charge %d, the segments' %d; want the values' alone", values.Bytes, values.SegmentBytes)
	}
	key := segmentPlanOf("9")
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, key, map[int][]byte{0: []byte("page")}, 1, 0, 120)); results[0] != segment.WriteApplied {
		t.Fatalf("the segment write = %v", results)
	}
	both := store.StateCacheStats()
	if both.SegmentBytes == 0 || both.Bytes-both.SegmentBytes != values.Bytes {
		t.Fatalf("with a segment: charge %d, the segments' %d; want the values' %d beside the segment's", both.Bytes, both.SegmentBytes, values.Bytes)
	}
	store.DropStateCacheQueryGroup(frozenRef().Slot.QueryGroup)
	released := store.StateCacheStats()
	if released.Departures[StateCacheDepartReleased] != 5 || released.SegmentDepartures[StateCacheDepartReleased] != 2 ||
		released.StateDepartures[StateCacheDepartReleased] != 3 || released.SegmentBytes != 0 || released.Bytes != 0 {
		t.Fatalf("released: %d left, %d of them the segment's and %d State's, charge %d, the segments' %d; want five, the mark and "+
			"the page the segment's, the three values State's, none held", released.Departures[StateCacheDepartReleased],
			released.SegmentDepartures[StateCacheDepartReleased], released.StateDepartures[StateCacheDepartReleased], released.Bytes,
			released.SegmentBytes)
	}
}

// A segment the budget cannot take in is counted over_budget under the
// segments' kind, and nothing of it is charged.
func TestASegmentTheBudgetCannotTakeIsCountedUnderItsKind(t *testing.T) {
	store := newSegmentRedis(t).store(t, 64)
	key := segmentPlanOf("9")
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, key, map[int][]byte{0: []byte("page")}, 1, 0, 120)); results[0] != segment.WriteApplied {
		t.Fatalf("the segment write = %v", results)
	}
	stats := store.StateCacheStats()
	if stats.SegmentDepartures[StateCacheDepartOverBudget] == 0 || stats.StateDepartures[StateCacheDepartOverBudget] != 0 ||
		stats.SegmentDepartures[StateCacheDepartOverBudget] != stats.Departures[StateCacheDepartOverBudget] || stats.SegmentBytes != 0 {
		t.Fatalf("over the budget: %d left over_budget, %d of them the segment's, its charge %d; want the segment's alone, nothing held",
			stats.Departures[StateCacheDepartOverBudget], stats.SegmentDepartures[StateCacheDepartOverBudget], stats.SegmentBytes)
	}
}
