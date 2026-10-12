// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

func shaperForTest(t *testing.T, clock *atomic.Int64) (*schedulerShaper, *scheduler.FlightCoordinator, config.Config) {
	t.Helper()
	cfg := validGoAccessRuntimeConfig()
	now := func() time.Time { return time.UnixMilli(clock.Load()) }
	flights, err := scheduler.NewFlightCoordinatorWithRecovery(cfg.PhaseTwo.Scheduler.RecoveryLimits(), now)
	if err != nil {
		t.Fatal(err)
	}
	return newSchedulerShaper(flights, cfg, now), flights, cfg
}

// The shaper sizes the coordinator from what the last minute asked of it:
// the busiest second of permit requests and of Slots made ready, the measured
// hold and execution length, the owned groups. The coordinator takes the
// derived budget, and the capacity reports it with its inputs.
func TestTheShaperSizesTheBudgetFromTheMeasuredWork(t *testing.T) {
	clock := &atomic.Int64{}
	clock.Store(time.Unix(3600*100, 0).UnixMilli())
	shaper, flights, cfg := shaperForTest(t, clock)
	if shape := shaper.shape(); shape.ProcessQueryPermits != cfg.PhaseTwo.Scheduler.ProcessQueryPermits {
		t.Fatalf("before any derivation the shape is %+v, want the startup one", shape)
	}
	// Twelve queries asked for in one second, each held 100 ms; eight Slots
	// made ready in that second, each running 2 s.
	deadline := time.UnixMilli(clock.Load()).Add(time.Hour)
	var held []*scheduler.QueryPermit
	for i := 0; i < 12; i++ {
		permit, err := flights.AcquireQueryPermit(context.Background(), execution.SlotIdentity{QueryGroup: "g", EvaluationTime: 60},
			execution.OperationNormal, deadline)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, permit)
	}
	for i := 0; i < 8; i++ {
		shaper.recordReady()
		shaper.recordExecution(2 * time.Second)
	}
	clock.Add(100)
	for _, permit := range held {
		permit.Release()
	}
	reading := shaper.derive(700)
	want := config.DeriveSchedulerShape(shaper.ceiling, config.SchedulerDemand{PeakQueriesPerSecond: 12, PeakSlotsPerSecond: 8,
		Hold: flights.HoldReference(), Execution: reading.Demand.Execution, OwnedQueryGroups: 700,
		MaxQueuedItemsPerQG: cfg.PhaseTwo.Scheduler.MaxQueuedItemsPerQG})
	if !reading.Applied || reading.Shape != want || reading.Demand.PeakQueriesPerSecond != 12 || reading.Demand.PeakSlotsPerSecond != 8 {
		t.Fatalf("derived %+v from %+v, want %+v applied", reading.Shape, reading.Demand, want)
	}
	if reading.Demand.Execution < 1400*time.Millisecond || reading.Demand.Execution > 2900*time.Millisecond {
		t.Fatalf("execution length %v from 2 s executions", reading.Demand.Execution)
	}
	if occupancy := flights.QueryPermitOccupancy(); occupancy.Budget != want.ProcessQueryPermits || occupancy.RecoveryBudget != want.RecoveryQueryPermits {
		t.Fatalf("coordinator budget %d/%d, derived %d/%d", occupancy.Budget, occupancy.RecoveryBudget, want.ProcessQueryPermits, want.RecoveryQueryPermits)
	}
	if shaper.shape() != want {
		t.Fatalf("shape in force %+v, derived %+v", shaper.shape(), want)
	}
	facts := shaper.facts()
	if facts == nil || facts.ProcessQueryPermits != want.ProcessQueryPermits || facts.PeakQueriesPerSecond != 12 ||
		facts.OwnedQueryGroups != 700 || facts.HoldMillis != flights.HoldReference().Milliseconds() {
		t.Fatalf("capacity reports %+v for %+v", facts, want)
	}
	if shaper.due() {
		t.Fatal("a derivation is due again at once")
	}
	clock.Add(time.Minute.Milliseconds())
	if !shaper.due() {
		t.Fatal("no derivation due a minute later")
	}
	// A minute with no work leaves only the smallest shape that runs.
	if idle := shaper.derive(700); idle.Shape.ProcessQueryPermits != 2 || idle.Demand.PeakQueriesPerSecond != 0 {
		t.Fatalf("an idle minute derived %+v from %+v", idle.Shape, idle.Demand)
	}
}

// Without a coordinator the bundle keeps the startup shape and reports none.
func TestABundleWithoutACoordinatorKeepsTheStartupShape(t *testing.T) {
	var shaper *schedulerShaper
	if shaper.due() || shaper.facts() != nil || shaper.shape() != (config.DerivedScheduler{}) {
		t.Fatal("a nil shaper derived something")
	}
	shaper.recordReady()
	shaper.recordExecution(time.Second)
}

// The dispatcher offers work only while fewer than the derived bound
// execute, not up to its goroutines: with the shape derived down to its
// smallest, a fifth active Slot is past the bound though the ceiling's
// slots are free.
func TestTheDispatcherOffersWorkOnlyUnderTheDerivedBound(t *testing.T) {
	clock := &atomic.Int64{}
	clock.Store(time.Unix(3600*100, 0).UnixMilli())
	shaper, _, cfg := shaperForTest(t, clock)
	bundle := &phaseTwoWorkerBundle{dependencies: phaseTwoWorkerBundleDependencies{Config: cfg}, shaper: shaper}
	dispatcher := &phaseTwoRunnerDispatcher{bundle: bundle, active: map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{}}
	reading := shaper.derive(1)
	if reading.Shape.ActiveExecutions >= cfg.PhaseTwo.Scheduler.ActiveExecutionCeiling || phaseTwoDispatcherFanout(bundle) != cfg.PhaseTwo.Scheduler.ActiveExecutionCeiling {
		t.Fatalf("derived %d slots, fanout %d, ceiling %d: the case needs a bound under the goroutines",
			reading.Shape.ActiveExecutions, phaseTwoDispatcherFanout(bundle), cfg.PhaseTwo.Scheduler.ActiveExecutionCeiling)
	}
	for i := 0; i < reading.Shape.ActiveExecutions; i++ {
		if dispatcher.slotsFull() {
			t.Fatalf("%d active of %d slots read as full", i, reading.Shape.ActiveExecutions)
		}
		dispatcher.active[execution.QueryGroupIdentity(fmt.Sprintf("g%d", i))] = &phaseTwoQueryGroupLifecycle{}
	}
	if !dispatcher.slotsFull() {
		t.Fatalf("%d active of %d slots read as free", len(dispatcher.active), reading.Shape.ActiveExecutions)
	}
}
