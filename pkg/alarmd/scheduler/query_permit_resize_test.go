// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func acquireNormal(t *testing.T, flights *FlightCoordinator, group string, deadline time.Time) *QueryPermit {
	t.Helper()
	permit, err := flights.AcquireQueryPermit(context.Background(), execution.SlotIdentity{QueryGroup: execution.QueryGroupIdentity(group), EvaluationTime: 60},
		execution.OperationNormal, deadline)
	if err != nil {
		t.Fatal(err)
	}
	return permit
}

// The query budget follows the work: raising it grants the queries already
// waiting at once, and lowering it takes nothing back - permits already held
// run to their release, and no new one is granted until the held fall under
// the new budget.
func TestTheQueryBudgetIsResizedInPlace(t *testing.T) {
	clock := newMutableClock(time.Unix(300, 0))
	limits := testRecoveryLimits()
	flights, err := NewFlightCoordinatorWithRecovery(limits, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	deadline := clock.Now().Add(time.Minute)
	var held []*QueryPermit
	for i := 0; i < limits.ProcessQueryPermits; i++ {
		held = append(held, acquireNormal(t, flights, string(rune('a'+i)), deadline))
	}
	granted := make(chan *QueryPermit, 1)
	go func() { granted <- acquireNormal(t, flights, "late", deadline) }()
	for flights.queryPermitSnapshot().NormalWaiting == 0 {
		time.Sleep(time.Millisecond)
	}
	raised := limits
	raised.ProcessQueryPermits++
	if err := flights.ResizeQueryLimits(raised); err != nil {
		t.Fatal(err)
	}
	late := <-granted
	if occupancy := flights.QueryPermitOccupancy(); occupancy.Budget != raised.ProcessQueryPermits {
		t.Fatalf("budget %d after raising to %d", occupancy.Budget, raised.ProcessQueryPermits)
	}

	lowered := limits
	lowered.ProcessQueryPermits = limits.RecoveryQueryPermits + 1
	if err := flights.ResizeQueryLimits(lowered); err != nil {
		t.Fatal(err)
	}
	if snapshot := flights.queryPermitSnapshot(); snapshot.Inflight != len(held)+1 {
		t.Fatalf("lowering the budget took back permits: %d held", snapshot.Inflight)
	}
	go func() { granted <- acquireNormal(t, flights, "later", deadline) }()
	for flights.queryPermitSnapshot().NormalWaiting == 0 {
		time.Sleep(time.Millisecond)
	}
	late.Release()
	for _, permit := range held[:len(held)-lowered.ProcessQueryPermits] {
		permit.Release()
	}
	select {
	case <-granted:
		t.Fatalf("a permit was granted with %d held against a budget of %d", lowered.ProcessQueryPermits, lowered.ProcessQueryPermits)
	case <-time.After(50 * time.Millisecond):
	}
	held[len(held)-1].Release()
	(<-granted).Release()
	for _, permit := range held[len(held)-lowered.ProcessQueryPermits : len(held)-1] {
		permit.Release()
	}
}

// A shape the coordinator cannot run under is refused whole, and the one in
// force stays.
func TestAnInvalidQueryBudgetIsRefusedAndTheOldOneStays(t *testing.T) {
	clock := newMutableClock(time.Unix(300, 0))
	limits := testRecoveryLimits()
	flights, err := NewFlightCoordinatorWithRecovery(limits, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	invalid := limits
	invalid.RecoveryQueryPermits = invalid.ProcessQueryPermits
	if err := flights.ResizeQueryLimits(invalid); err == nil {
		t.Fatal("recovery permits equal to the process budget were accepted")
	}
	changed := limits
	changed.MaxReplaySlots++
	if err := flights.ResizeQueryLimits(changed); err == nil {
		t.Fatal("a resize changed the replay limits, which are not the budget's")
	}
	if occupancy := flights.QueryPermitOccupancy(); occupancy.Budget != limits.ProcessQueryPermits || occupancy.RecoveryBudget != limits.RecoveryQueryPermits {
		t.Fatalf("budget %d/%d after a refused resize, want %d/%d", occupancy.Budget, occupancy.RecoveryBudget, limits.ProcessQueryPermits, limits.RecoveryQueryPermits)
	}
}

// The hold a query permit is sized against is the service's answer time when
// it is not under pressure: the lowest hourly median of the normal queries'
// holds over the last day, so an hour of slow answers does not raise the
// budget and put more pressure on a service that is already slow.
func TestTheHoldReferenceIsTheLowestHourlyMedian(t *testing.T) {
	clock := newMutableClock(time.Unix(3600*100, 0))
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := flights.HoldReference(); got != 0 {
		t.Fatalf("hold reference %v before any hold, want none", got)
	}
	hold := func(d time.Duration, n int) {
		for i := 0; i < n; i++ {
			permit := acquireNormal(t, flights, "a", clock.Now().Add(time.Hour))
			clock.Advance(d)
			permit.Release()
		}
	}
	hold(100*time.Millisecond, 200)
	first := flights.HoldReference()
	if first < 70*time.Millisecond || first > 145*time.Millisecond {
		t.Fatalf("hold reference %v after an hour of 100 ms holds", first)
	}
	clock.Advance(time.Hour)
	hold(2*time.Second, 200)
	if got := flights.HoldReference(); got != first {
		t.Fatalf("an hour of 2 s holds moved the reference from %v to %v", first, got)
	}
	clock.Advance(25 * time.Hour)
	hold(2*time.Second, 200)
	if got := flights.HoldReference(); got < 1400*time.Millisecond {
		t.Fatalf("a day after the fast hour the reference is still %v", got)
	}
}
