// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// hungQueryGroup runs a Slot that ignores its cancellation until the test
// lets it go, and says which stage it is in and the Slot's deadline.
type hungQueryGroup struct {
	*callbackPhaseTwoQueryGroup
	stage    string
	deadline atomic.Int64
	released atomic.Int32
}

func (runner *hungQueryGroup) InFlight() (string, time.Time) {
	return runner.stage, time.UnixMilli(runner.deadline.Load())
}

func (runner *hungQueryGroup) Release(context.Context) error {
	runner.released.Add(1)
	return nil
}

func newHungQueryGroup(started chan<- struct{}, letGo <-chan struct{}) *hungQueryGroup {
	var once atomic.Bool
	return &hungQueryGroup{stage: execution.SlotStageOutput, callbackPhaseTwoQueryGroup: &callbackPhaseTwoQueryGroup{
		run: func(context.Context) (execution.SlotExecutionResult, bool, error) {
			if once.CompareAndSwap(false, true) {
				close(started)
				<-letGo // the context is not watched
			}
			return execution.SlotExecutionResult{Completed: true}, true, nil
		},
	}}
}

func lastDeclined(owner *fakePhaseTwoOwnership) []ownership.DeclinedQueryGroup {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.registrations) == 0 {
		return nil
	}
	return owner.registrations[len(owner.registrations)-1].Declined
}

// An execution past its Slot deadline by more than the grace has ignored its
// cancellation (design 02 section 6.5). Its Query Group is let go at once -
// detached, its lease released without waiting on the hung Slot - and named
// on the replica's registration with the stage it hangs in; the replica does
// not open it again while the execution hangs, and stays ready for the rest.
// When the hung execution returns at last the decline is lifted. A replica
// started afresh declines nothing.
func TestAnExecutionHungPastItsDeadlineIsDeclinedUntilItReturns(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	queryGroup := execution.QueryGroupIdentity("query-group-hung")
	control := &fakePhaseTwoControl{queryGroups: []execution.QueryGroupIdentity{queryGroup}}
	started, letGo := make(chan struct{}), make(chan struct{})
	runner := newHungQueryGroup(started, letGo)
	owner := &fakePhaseTwoOwnership{assigned: []execution.QueryGroupIdentity{queryGroup}, runner: runner}
	health := newPhaseTwoApplicationHealth()
	bundle := mustPhaseTwoWorkerBundle(t, cfg, health, control, owner)
	if err := bundle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bundle.Shutdown(context.Background()) }()
	var finished atomic.Bool
	defer func() {
		if !finished.Load() {
			close(letGo)
		}
	}()
	ran := make(chan error, 1)
	go func() { ran <- runScheduledOnceSettled(context.Background(), bundle) }()
	waitSignal(t, started, "the Slot that will hang")

	now := time.Now()
	// Inside the grace: nothing happens.
	runner.deadline.Store(now.Add(-executionPastDeadlineGrace / 2).UnixMilli())
	bundle.declineHungExecutions(now)
	if runner.released.Load() != 0 || heldRunner(bundle, queryGroup) == nil {
		t.Fatal("an execution inside its grace was let go")
	}
	// Past it: declined.
	runner.deadline.Store(now.Add(-executionPastDeadlineGrace - time.Second).UnixMilli())
	bundle.declineHungExecutions(now)
	deadline := time.Now().Add(signalWaitBound)
	for runner.released.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if released := runner.released.Load(); released != 1 {
		t.Fatalf("release calls = %d, want the lease released once without waiting on the hung Slot", released)
	}
	if heldRunner(bundle, queryGroup) != nil {
		t.Fatal("the declined Query Group is still a current runner")
	}
	for time.Now().Before(deadline) && len(lastDeclined(owner)) == 0 {
		time.Sleep(time.Millisecond)
	}
	if declined := lastDeclined(owner); len(declined) != 1 || declined[0] != (ownership.DeclinedQueryGroup{
		QueryGroup: string(queryGroup), Stage: execution.SlotStageOutput,
	}) {
		t.Fatalf("registration declines %+v, want the Query Group with the stage it hangs in", declined)
	}
	// Still desired here until a leader moves it: not opened again, and the
	// replica stays ready.
	if err := bundle.refreshAndReconcile(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if heldRunner(bundle, queryGroup) != nil {
		t.Fatal("the reconcile opened the declined Query Group again")
	}
	if snapshot := health.HealthSnapshot(); snapshot.State != observability.HealthReady {
		t.Fatalf("health with a declined Query Group still assigned = %+v, want ready", snapshot)
	}
	// The hung execution returns: the decline is lifted.
	finished.Store(true)
	close(letGo)
	<-ran
	if declined := bundle.declinedRegistration(); declined != nil {
		t.Fatalf("declines after the hung execution returned = %+v, want none", declined)
	}
	// A replica started afresh carries nothing over.
	fresh := mustPhaseTwoWorkerBundle(t, cfg, newPhaseTwoApplicationHealth(), control,
		&fakePhaseTwoOwnership{assigned: []execution.QueryGroupIdentity{queryGroup}, runner: newFakePhaseTwoQueryGroup()})
	if declined := fresh.declinedRegistration(); declined != nil {
		t.Fatalf("a fresh replica declines %+v, want none", declined)
	}
}
