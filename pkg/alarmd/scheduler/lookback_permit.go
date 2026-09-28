// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import "sync"

// The lookback reads a finished Slot's window again, later, to see what
// arrived after the formal read. It spends the same query budget as
// detection and must never be the reason a formal query waits. A permit for
// it is therefore granted only on a pool with room to spare and never
// queued for:
//
//   - nobody is waiting for a query permit, normal or recovery;
//   - the lookback holds fewer than LookbackPermitLimit permits;
//   - after this one is granted, at least LookbackPermitLimit permits are
//     still free for the next formal queries.
//
// A lookback permit is not an Operation. It has its own inflight count and
// held seconds, so the normal and recovery readings the capacity is sized on
// never include it, while the process total does.

// LookbackPermitLimit is the most lookback permits one process holds at once:
// an eighth of its query permits, at least one. A batch of first reads is
// sampled together and comes due together; the limit keeps that batch from
// taking every free permit in the moment it finds them.
func LookbackPermitLimit(processPermits int) int {
	return max(1, processPermits/8)
}

// Why a lookback permit was not granted, closed.
const (
	LookbackRefusedDisabled = "disabled"
	LookbackRefusedWaiting  = "waiters"
	LookbackRefusedLimit    = "lookback_limit"
	LookbackRefusedHeadroom = "headroom"
)

// LookbackPermit is one lookback query's share of the query budget.
type LookbackPermit struct {
	coordinator *FlightCoordinator
	id          uint64
	once        sync.Once
}

// Release returns the permit; a second call does nothing.
func (permit *LookbackPermit) Release() {
	if permit == nil || permit.coordinator == nil {
		return
	}
	permit.once.Do(func() { permit.coordinator.releaseLookbackPermit(permit.id) })
}

// TryAcquireLookbackPermit grants a lookback permit now or refuses with the
// reason; it never waits and never queues.
func (coordinator *FlightCoordinator) TryAcquireLookbackPermit() (*LookbackPermit, string) {
	if coordinator == nil || !coordinator.recoveryEnabled {
		return nil, LookbackRefusedDisabled
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.expireWaitersLocked(&coordinator.normalWaiters)
	coordinator.expireWaitersLocked(&coordinator.recoveryWaiters)
	if len(coordinator.normalWaiters) > 0 || len(coordinator.recoveryWaiters) > 0 || len(coordinator.recoveryChannelWaiters) > 0 {
		return nil, LookbackRefusedWaiting
	}
	limit := LookbackPermitLimit(coordinator.limits.ProcessQueryPermits)
	if coordinator.lookbackInflight >= limit {
		return nil, LookbackRefusedLimit
	}
	if coordinator.limits.ProcessQueryPermits-coordinator.queryInflight-1 < limit {
		return nil, LookbackRefusedHeadroom
	}
	coordinator.queryInflight++
	coordinator.lookbackInflight++
	coordinator.permitSequence++
	permit := &LookbackPermit{coordinator: coordinator, id: coordinator.permitSequence}
	if coordinator.lookbackHeld == nil {
		coordinator.lookbackHeld = make(map[uint64]heldPermit)
	}
	coordinator.lookbackHeld[permit.id] = heldPermit{since: coordinator.now()}
	return permit, ""
}

func (coordinator *FlightCoordinator) releaseLookbackPermit(id uint64) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.queryInflight > 0 {
		coordinator.queryInflight--
	}
	if coordinator.lookbackInflight > 0 {
		coordinator.lookbackInflight--
	}
	if held, ok := coordinator.lookbackHeld[id]; ok {
		delete(coordinator.lookbackHeld, id)
		if elapsed := coordinator.now().Sub(held.since); elapsed > 0 {
			coordinator.lookbackSeconds += elapsed.Seconds()
		}
	}
	// The permit freed room in the shared pool: a formal query waiting for it
	// gets it now, not at the next release.
	coordinator.dispatchQueryPermitsLocked()
}
