// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// PinSliceConsumersForTests has every Slot read in slices consume them on
// its own stream, k = 1, unless a case sets k: this package's cases are one
// stream's but those that set it (TestMain).
func PinSliceConsumersForTests() { sliceConsumersForTest.Store(1) }

// SetSliceConsumersByCoresForTest has every Slot read in slices take k as a
// process of procs cores takes it (sliceConsumersByCores, within the read
// width, from its second slice) until t ends.
func SetSliceConsumersByCoresForTest(t testing.TB, procs int) {
	t.Helper()
	before, beforeProcs := sliceConsumersForTest.Swap(0), sliceConsumerProcsForTest.Swap(int32(procs))
	t.Cleanup(func() {
		sliceConsumersForTest.Store(before)
		sliceConsumerProcsForTest.Store(beforeProcs)
	})
}

// SetSliceConsumersForTest has every Slot read in slices consume them with k
// streams side by side until t ends (sliceConsumersForTest, 015 C1).
func SetSliceConsumersForTest(t testing.TB, k int) {
	t.Helper()
	before := sliceConsumersForTest.Swap(int32(k))
	t.Cleanup(func() { sliceConsumersForTest.Store(before) })
}

// SetBarrierAskedForTest tells note, until t ends, each time a consumer asks
// for its Slot's barrier.
func SetBarrierAskedForTest(t testing.TB, note func()) {
	t.Helper()
	before := barrierAskedForTest.Swap(&note)
	t.Cleanup(func() { barrierAskedForTest.Store(before) })
}

// SetSlicesBySubstreamForTest tells note, until t ends, how many slices each
// consumer of a Slot consumed, and how many barriers they held, as its
// consumers end.
func SetSlicesBySubstreamForTest(t testing.TB, note func(consumed []int, barriers uint64)) {
	t.Helper()
	before := slicesBySubstreamForTest.Swap(&note)
	t.Cleanup(func() { slicesBySubstreamForTest.Store(before) })
}

// SetSliceReadTakenForTest tells note, until t ends, the slice of each read
// a Slot's dispatch takes back from its consumers, before it judges it.
func SetSliceReadTakenForTest(t testing.TB, note func(slice execution.InputSlice)) {
	t.Helper()
	before := sliceReadTakenForTest.Swap(&note)
	t.Cleanup(func() { sliceReadTakenForTest.Store(before) })
}

// SetConsumersBeganForTest tells note, until t ends, as a Slot's consumers
// are about to begin.
func SetConsumersBeganForTest(t testing.TB, note func()) {
	t.Helper()
	before := consumersBeganForTest.Swap(&note)
	t.Cleanup(func() { consumersBeganForTest.Store(before) })
}

// SetSliceRoomHeldForTest has a Slot's k chosen by its room, until t ends, as
// if its dispatch held bytes more than it does.
func SetSliceRoomHeldForTest(t testing.TB, bytes uint64) {
	t.Helper()
	before := sliceRoomHeldForTest.Swap(bytes)
	t.Cleanup(func() { sliceRoomHeldForTest.Store(before) })
}

// SliceShareForTest is a Query Group's share on the coordinator and the floor
// of a slice consumed side by side, L_min (consumersForRoom).
func SliceShareForTest(coordinator *SlotExecutionCoordinator) (share, floor uint64) {
	share = coordinator.qgShareBytes()
	return share, share / sliceFloorDivisor
}
