// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "testing"

// SetApplySeriallyForTest has every Slot's windows written one after another
// until t ends, the Slot's goroutine waiting on each as before the window
// pipeline (applySerially): the side the comparison cases read the pipeline
// against.
func SetApplySeriallyForTest(t testing.TB) {
	t.Helper()
	before := applySerially.Swap(true)
	t.Cleanup(func() { applySerially.Store(before) })
}

// ApplyGoroutinesForTest is how many Slots' apply goroutines are running.
func ApplyGoroutinesForTest() int64 { return applyGoroutines.Load() }

// EffectsReservedForTest is the State, event and gap counts the coordinator
// holds for its Slots.
func EffectsReservedForTest(coordinator *SlotExecutionCoordinator) (states, events, gaps uint64) {
	coordinator.reservations.mu.Lock()
	defer coordinator.reservations.mu.Unlock()
	return coordinator.reservations.states, coordinator.reservations.events, coordinator.reservations.gaps
}

// PoolHeldForTest is what the process's pool holds reserved now, by counter:
// series, retained bytes, State mutations, events, gap mutations, gap facts.
func PoolHeldForTest(coordinator *SlotExecutionCoordinator) [6]uint64 {
	coordinator.reservations.mu.Lock()
	defer coordinator.reservations.mu.Unlock()
	held := &coordinator.reservations
	return [6]uint64{held.series, held.retainedBytes, held.states, held.events, held.gaps, held.gapFacts}
}

// SetWindowLandedForTest has note told, until t ends, what each window gave
// back as it landed: of its state, output and input, in that order.
func SetWindowLandedForTest(t testing.TB, note func(state, output, input uint64)) {
	t.Helper()
	hook := func(given [retainPhaseCount]uint64) {
		note(given[retainPhaseState], given[retainPhaseOutput], given[retainPhaseInput])
	}
	before := windowLandedForTest.Swap(&hook)
	t.Cleanup(func() { windowLandedForTest.Store(before) })
}
