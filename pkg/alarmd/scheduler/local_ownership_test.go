// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// countingSession is fakeSession counting the fence checks the Runner asks
// the store for.
type countingSession struct {
	*fakeSession
	checks int
}

func (session *countingSession) ValidateCurrentWithAssignment(ctx context.Context, at time.Time) (execution.OwnerFence, ownership.AssignmentRecord, error) {
	session.checks++
	return session.fakeSession.ValidateCurrentWithAssignment(ctx, at)
}

// scriptedLocal answers every local check with reason and err, and records
// the refusals it is told of.
type scriptedLocal struct {
	fence   execution.OwnerFence
	reason  string
	err     error
	asked   int
	refused []time.Time
}

func (local *scriptedLocal) ValidateLocally(time.Time) (execution.OwnerFence, ownership.AssignmentRecord, string, error) {
	local.asked++
	if local.err != nil || local.reason != ownership.LocalCheckPass {
		return execution.OwnerFence{}, ownership.AssignmentRecord{}, local.reason, local.err
	}
	return local.fence, ownership.AssignmentRecord{}, local.reason, nil
}

func (local *scriptedLocal) NoteFenceRefused(at time.Time) { local.refused = append(local.refused, at) }

func newLocalOwnershipRunner(t *testing.T, clock *mutableClock, executor Executor, local *scriptedLocal) (*Runner, *countingSession, *[][2]string) {
	t.Helper()
	slot := frozenSlot("query-group-1")
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	session := &countingSession{fakeSession: &fakeSession{fence: slot.Dispatch.OwnerFence}}
	runner, err := NewRunner("query-group-1", session, &fakeSlotSource{slot: slot}, executor, flights, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	counted := &[][2]string{}
	if local != nil {
		local.fence = slot.Dispatch.OwnerFence
		runner.WithLocalOwnership(local, func(outcome, reason string) { *counted = append(*counted, [2]string{outcome, reason}) })
	}
	return runner, session, counted
}

// An attempt the local check admits runs without asking the store; one it
// sends to the store asks once, as every attempt did before; one whose lease
// refused itself is the round's ownership rejection without the store; and
// a Runner without a local check asks the store every attempt. Each check is
// counted once, under its outcome and reason.
func TestARunnersOwnershipCheckIsLocalWhenItCanBe(t *testing.T) {
	for _, cell := range []struct {
		name      string
		local     *scriptedLocal
		checks    int
		attempted bool
		counted   [2]string
	}{
		{"admitted locally", &scriptedLocal{reason: ownership.LocalCheckPass}, 0, true, [2]string{ownership.OwnershipCheckLocalPass, ownership.LocalCheckPass}},
		{"sent to the store", &scriptedLocal{reason: ownership.LocalCheckViewSilent}, 1, true, [2]string{ownership.OwnershipCheckFallbackRedis, ownership.LocalCheckViewSilent}},
		{"refused by its lease", &scriptedLocal{reason: ownership.LocalCheckLeaseRefused, err: ownership.ErrStaleFence}, 0, false,
			[2]string{ownership.OwnershipCheckLocalRefused, ownership.LocalCheckLeaseRefused}},
		{"without a local check", nil, 1, true, [2]string{}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			clock := newMutableClock(time.Unix(200, 0))
			executor := &failingExecutor{}
			runner, session, counted := newLocalOwnershipRunner(t, clock, executor, cell.local)
			_, attempted, err := runner.RunOne(context.Background())
			if attempted != cell.attempted || (cell.local != nil && cell.local.err != nil) != (err != nil) {
				t.Fatalf("RunOne attempted=%v err=%v, want attempted=%v", attempted, err, cell.attempted)
			}
			if cell.local != nil && cell.local.err != nil && !errors.Is(err, ownership.ErrStaleFence) {
				t.Fatalf("a lease that refused itself = %v, want its stale fence", err)
			}
			if session.checks != cell.checks {
				t.Fatalf("the store was asked %d fence checks, want %d", session.checks, cell.checks)
			}
			if executor.Calls() != map[bool]int{true: 1, false: 0}[cell.attempted] {
				t.Fatalf("the executor ran %d times", executor.Calls())
			}
			if cell.local == nil {
				if len(*counted) != 0 {
					t.Fatalf("a Runner without a local check counted %v", *counted)
				}
				return
			}
			if len(*counted) != 1 || (*counted)[0] != cell.counted {
				t.Fatalf("counted %v, want one %v", *counted, cell.counted)
			}
		})
	}
}

// A begin the owner fence refused - a stale owner or a moved content scope -
// tells the local check, at the Runner's clock, so the next attempt asks the
// store; one that ran does not.
func TestABeginTheFenceRefusedTellsTheLocalCheck(t *testing.T) {
	for _, refusal := range []*execution.SlotBeginRefusedError{
		{Status: execution.ProgressStaleOwner, Err: ownership.ErrStaleFence},
		{Status: execution.ProgressContentMoved, Err: ownership.ErrContentScopeMoved},
	} {
		clock := newMutableClock(time.Unix(200, 0))
		local := &scriptedLocal{reason: ownership.LocalCheckPass}
		runner, _, _ := newLocalOwnershipRunner(t, clock, &failingExecutor{err: refusal, failures: 1}, local)
		_, _, _ = runner.RunOne(context.Background())
		if len(local.refused) != 1 || !local.refused[0].Equal(clock.Now()) {
			t.Fatalf("%s at begin: the local check was told %v, want once at %v", refusal.Status, local.refused, clock.Now())
		}
	}
	clock := newMutableClock(time.Unix(200, 0))
	local := &scriptedLocal{reason: ownership.LocalCheckPass}
	runner, _, _ := newLocalOwnershipRunner(t, clock, &failingExecutor{}, local)
	if _, attempted, err := runner.RunOne(context.Background()); !attempted || err != nil {
		t.Fatalf("RunOne = (%v, %v), want a Slot that ran", attempted, err)
	}
	if len(local.refused) != 0 {
		t.Fatalf("a Slot that ran told the local check of a refusal: %v", local.refused)
	}
}

// A write after the begin that the fence refused - a State or a Progress
// write answering a stale owner, a Query Group no longer desired here, a
// content scope moved - tells the local check as a refused begin does, so
// the next attempt asks the store; a failure that is not the fence's does
// not.
func TestAWriteAfterTheBeginTheFenceRefusedTellsTheLocalCheck(t *testing.T) {
	for _, cell := range []struct {
		err  error
		told int
	}{
		{fmt.Errorf("apply state: %w", ownership.ErrStaleFence), 1},
		{fmt.Errorf("commit progress: %w", ownership.ErrNotDesired), 1},
		{fmt.Errorf("apply state: %w", ownership.ErrContentScopeMoved), 1},
		{errors.New("commit progress: redis timeout"), 0},
	} {
		clock := newMutableClock(time.Unix(200, 0))
		local := &scriptedLocal{reason: ownership.LocalCheckPass}
		runner, _, _ := newLocalOwnershipRunner(t, clock, &failingExecutor{err: cell.err, failures: 1}, local)
		_, _, _ = runner.RunOne(context.Background())
		if len(local.refused) != cell.told {
			t.Fatalf("%v: the local check was told %d times, want %d", cell.err, len(local.refused), cell.told)
		}
	}
}

// Every cell the counter is created with is one a check can be counted
// under, and every reason a check can give is in one.
func TestEveryOwnershipCheckWordHasItsCell(t *testing.T) {
	cells := map[[2]string]bool{}
	for _, cell := range ownership.OwnershipCheckCells() {
		cells[cell] = true
	}
	for _, reason := range ownership.LocalCheckReasons {
		outcome := ownership.OwnershipCheckFallbackRedis
		switch reason {
		case ownership.LocalCheckPass:
			outcome = ownership.OwnershipCheckLocalPass
		case ownership.LocalCheckLeaseRefused:
			outcome = ownership.OwnershipCheckLocalRefused
		}
		if !cells[[2]string{outcome, reason}] {
			t.Errorf("reason %s has no cell under %s", reason, outcome)
		}
	}
	if !cells[[2]string{ownership.LocalCheckOther, ownership.LocalCheckOther}] {
		t.Fatal("no other/other cell: a check under words without a cell would be dropped")
	}
	if len(cells) != len(ownership.LocalCheckReasons)+1 {
		t.Fatalf("%d cells for %d reasons and other", len(cells), len(ownership.LocalCheckReasons))
	}
}
