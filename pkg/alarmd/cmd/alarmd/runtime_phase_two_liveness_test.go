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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// livenessClock is a clock a test moves by hand.
type livenessClock struct{ at atomic.Int64 }

func newLivenessClock() *livenessClock {
	clock := &livenessClock{}
	clock.at.Store(time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixNano())
	return clock
}

func (clock *livenessClock) Now() time.Time              { return time.Unix(0, clock.at.Load()).UTC() }
func (clock *livenessClock) advance(delta time.Duration) { clock.at.Add(int64(delta)) }

func stalledLoops(liveness *phaseTwoLiveness) []string {
	var loops []string
	for _, stall := range liveness.Stalls() {
		loops = append(loops, stall.Loop)
	}
	return loops
}

func sameLoops(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// A started loop that has not finished a turn within its bound is stalled;
// one second either side of each bound is tested, for both loops.
func TestALoopIsStalledOnlyPastItsBound(t *testing.T) {
	for _, loop := range []struct {
		name  string
		bound time.Duration
	}{{livenessLoopControl, controlLoopStallBound}, {livenessLoopDispatch, dispatchLoopStallBound}} {
		t.Run(loop.name, func(t *testing.T) {
			clock := newLivenessClock()
			liveness := newPhaseTwoLiveness(clock.Now, nil)
			liveness.start(loop.name, loop.bound)
			clock.advance(loop.bound - time.Second)
			if got := stalledLoops(liveness); len(got) != 0 {
				t.Fatalf("a second inside the bound: stalled %v", got)
			}
			clock.advance(2 * time.Second)
			if got := stalledLoops(liveness); !sameLoops(got, loop.name) {
				t.Fatalf("a second past the bound: stalled %v, want %s", got, loop.name)
			}
			liveness.turned(loop.name, clock.Now())
			if got := stalledLoops(liveness); len(got) != 0 {
				t.Fatalf("after a turn: stalled %v", got)
			}
		})
	}
}

// The two loops are judged apart: one past its bound does not make the other
// stalled, and a loop that never started is never late.
func TestLoopsAreJudgedApartAndOnlyOnceStarted(t *testing.T) {
	clock := newLivenessClock()
	liveness := newPhaseTwoLiveness(clock.Now, nil)
	clock.advance(time.Hour)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("nothing started (a replica waiting on a startup dependency): stalled %v", got)
	}
	liveness.start(livenessLoopControl, controlLoopStallBound)
	liveness.start(livenessLoopDispatch, dispatchLoopStallBound)
	clock.advance(dispatchLoopStallBound + time.Second)
	liveness.turned(livenessLoopControl, clock.Now())
	if got := stalledLoops(liveness); !sameLoops(got, livenessLoopDispatch) {
		t.Fatalf("stalled %v, want only dispatch", got)
	}
	liveness.stopJudging()
	clock.advance(time.Hour)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("a draining replica: stalled %v", got)
	}
}

// Every slot held past its deadline by the grace, with nothing returning, for
// longer than the bound is a stall; one second either side of the bound is
// tested. A free slot, a return, or an execution with no deadline ends it.
func TestEveryExecutionSlotStuckPastItsDeadlineIsAStall(t *testing.T) {
	setup := func() (*livenessClock, *phaseTwoLiveness, time.Time) {
		clock := newLivenessClock()
		liveness := newPhaseTwoLiveness(clock.Now, nil)
		liveness.setSlots(2)
		deadline := clock.Now().Add(time.Minute)
		liveness.executionStarted(deadline)
		liveness.executionStarted(deadline.Add(-30 * time.Second))
		return clock, liveness, deadline
	}
	clock, liveness, deadline := setup()
	crossed := deadline.Add(executionPastDeadlineGrace)
	clock.advance(crossed.Sub(clock.Now()) + executionsStuckBound - time.Second)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("a second inside the bound: stalled %v", got)
	}
	if reading := liveness.reading(); reading.ExecutionsPastDeadline != 2 {
		t.Fatalf("executions past deadline = %d, want 2", reading.ExecutionsPastDeadline)
	}
	clock.advance(2 * time.Second)
	if got := stalledLoops(liveness); !sameLoops(got, livenessExecutions) {
		t.Fatalf("a second past the bound: stalled %v, want executions", got)
	}

	// A free slot: the replica still detects on it.
	clock, liveness, _ = setup()
	liveness.setSlots(3)
	clock.advance(time.Hour)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("with a free slot: stalled %v", got)
	}

	// A return inside the window: the condition has not held across it, even
	// when the slot is refilled at once by work whose deadline has passed.
	clock, liveness, deadline = setup()
	clock.advance(deadline.Add(executionPastDeadlineGrace).Sub(clock.Now()) + 4*time.Minute)
	token := liveness.executionStarted(deadline)
	liveness.executionReturned(token - 1)
	clock.advance(2 * time.Minute)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("two minutes after a return: stalled %v", got)
	}

	// An execution with no deadline cannot be judged past it.
	clock = newLivenessClock()
	liveness = newPhaseTwoLiveness(clock.Now, nil)
	liveness.setSlots(1)
	liveness.executionStarted(time.Time{})
	clock.advance(time.Hour)
	if got := stalledLoops(liveness); len(got) != 0 {
		t.Fatalf("an execution without a deadline: stalled %v", got)
	}
}

// The metric carries only started loops, and their age.
func TestLivenessReadingCarriesOnlyStartedLoops(t *testing.T) {
	clock := newLivenessClock()
	recorder := metric.NewRecorder(metric.BuildInfo{})
	liveness := newPhaseTwoLiveness(clock.Now, recorder)
	liveness.start(livenessLoopControl, controlLoopStallBound)
	clock.advance(90 * time.Second)
	reading := liveness.reading()
	if len(reading.TurnAge) != 1 || reading.TurnAge[livenessLoopControl] != 90*time.Second {
		t.Fatalf("reading = %+v, want control at 90s only", reading)
	}
	began := clock.Now()
	clock.advance(3 * time.Second)
	liveness.turned(livenessLoopControl, began)
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var age, count float64 = -1, -1
	for _, family := range families {
		for _, sample := range family.GetMetric() {
			switch family.GetName() {
			case "bkmonitor_alarmd_loop_turn_age_seconds":
				age = sample.GetGauge().GetValue()
			case "bkmonitor_alarmd_loop_turn_duration_seconds":
				for _, label := range sample.GetLabel() {
					if label.GetValue() == livenessLoopControl {
						count = float64(sample.GetHistogram().GetSampleCount())
					}
				}
			}
		}
	}
	if age != 0 || count != 1 {
		t.Fatalf("loop_turn_age_seconds = %v, loop_turn_duration_seconds count = %v; want 0 and 1", age, count)
	}
}

// blockingAssignmentOwnership holds the control loop inside a store read
// once armed, the way a read that ignores its deadline would.
type blockingAssignmentOwnership struct {
	*fakePhaseTwoOwnership
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (owner *blockingAssignmentOwnership) AssignedQueryGroups(
	ctx context.Context, queryGroups []execution.QueryGroupIdentity,
) ([]execution.QueryGroupIdentity, error) {
	if owner.armed.Load() {
		owner.once.Do(func() { close(owner.entered) })
		<-owner.release
	}
	return owner.fakePhaseTwoOwnership.AssignedQueryGroups(ctx, queryGroups)
}

// The shape the probe exists for: the control loop stuck inside one call.
// The dispatch loop keeps turning, so only the control loop is named; the
// probe fails once the bound passes and recovers when the call returns.
func TestAControlLoopStuckInACallFailsLivenessAndRecovers(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	cfg.PhaseTwo.Scheduler.TickInterval = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Control.RefreshInterval = config.Duration(time.Hour)
	cfg.PhaseTwo.Control.ReconcileInterval = config.Duration(2 * time.Millisecond)
	queryGroup := execution.QueryGroupIdentity("query-group-1")
	runner := newFakePhaseTwoQueryGroup()
	owner := &blockingAssignmentOwnership{
		fakePhaseTwoOwnership: &fakePhaseTwoOwnership{assigned: []execution.QueryGroupIdentity{queryGroup}, runner: runner},
		entered:               make(chan struct{}), release: make(chan struct{}),
	}
	clock := newLivenessClock()
	bundle, err := newPhaseTwoWorkerBundle(phaseTwoWorkerBundleDependencies{
		Config: cfg, Health: newPhaseTwoApplicationHealth(),
		Control: &fakePhaseTwoControl{queryGroups: []execution.QueryGroupIdentity{queryGroup}}, Ownership: owner,
		Observer: observability.NopObserver{}, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bundle.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	waitSignal(t, runner.leaseStarted, "owned query-group lease")

	owner.armed.Store(true)
	waitSignal(t, owner.entered, "the control loop enters the held call")
	clock.advance(controlLoopStallBound + time.Second)
	waitUntil(t, 5*time.Second, "only the control loop is named", func() bool {
		return sameLoops(stalledLoops(bundle.liveness), livenessLoopControl)
	})

	owner.armed.Store(false)
	close(owner.release)
	waitUntil(t, 5*time.Second, "the probe recovers once the call returns", func() bool {
		return len(stalledLoops(bundle.liveness)) == 0
	})
}
