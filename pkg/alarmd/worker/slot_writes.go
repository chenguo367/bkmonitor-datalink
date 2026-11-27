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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// What stands before each write of a Slot attempt (014 ⑩).
//
// The owner fence (F1). A write whose script runs the fence itself - the
// State, the segments, Progress - needs no check before it: the script
// judges the owner at the moment it writes. The writes whose scripts do not
// - a gap statement, an event, the no-data memory and its ends - are each
// preceded, within the step they belong to, by one check of the fence
// (beforeWrite). A step begins where a Plan is admitted (admitPlan, which
// reads its activation only), and where the no-data memory, the no-data
// ends and a grown head statement are written, each a step of its own. A
// check made in the step stands for the step's later writes: it was made
// after the step began, so no write is further from its check than it was
// from the admission that checked the fence before. A step with nothing of
// that kind to write checks nothing, which is the steady round of a small
// Query Group. The fence check stays where it was for an attempt without
// this ledger (a supplement, which admits with Check, the fence and the
// activation together).
//
// The Slot's beginning (F5). An attempt whose BeginSlot would only write
// its unfinished projection onto a record already at its Slot
// (progress.Store.BeginDeferrable) puts the BeginSlot off until its first
// write, the frozen-State renewal aside (it only lengthens a key's life):
// the unfinished projection is what a retry holds its frozen contract to,
// so it lands before anything the attempt writes. A round that writes
// nothing commits without it, the one fenced write the round makes. An
// attempt that leaves without writing its Slot down - turned away by its
// read hold, stopped in its read, retried - makes it on the way out
// (beginOnExit): what the next attempt and a successor read of it, its
// frozen contract with its hold and its carried reason, is left as the
// BeginSlot at its start left it. A BeginSlot that does not begin the Slot
// is what the attempt returns, as it was at its start, and every write
// after it is refused.
type slotWrites struct {
	coordinator *SlotExecutionCoordinator
	request     execution.SlotExecutionRequest

	mu sync.Mutex
	// fenced is that the owner fence was confirmed in this step: checked,
	// or a BeginSlot put off landed under it, which its script ran.
	fenced bool
	// refused is the fence check that refused the owner: every unfenced
	// write after it returns it rather than asking the store again.
	refused error
	// owed is the BeginSlot put off and not yet made; deferred is that one
	// was put off, made since or not.
	owed     *execution.ProgressBeginRequest
	deferred bool
	// stoodIn is that the attempt's commit was sent in the place of the
	// BeginSlot it owed: an empty round's (commitStandsIn).
	stoodIn bool
	// failed is the BeginSlot put off that did not begin the Slot.
	failed *slotNotBegun
}

// slotBeginOnExitTimeout bounds the BeginSlot an attempt leaving uncommitted
// makes on its way out, on a context of its own: the attempt's may be
// cancelled already, a stop in its read being one of the ways out that
// leave the Slot to the next attempt, as the execution evidence's write is
// (executionEvidenceWriteTimeout).
const slotBeginOnExitTimeout = 2 * time.Second

// slotNotBegun is a put-off BeginSlot that did not begin the Slot: what the
// attempt returns, a result or an error, exactly as a BeginSlot at its start
// would have had it return.
type slotNotBegun struct {
	result execution.SlotExecutionResult
	err    error
}

func (notBegun *slotNotBegun) Error() string {
	if notBegun.err != nil {
		return "alarmd worker: Slot not begun: " + notBegun.err.Error()
	}
	return "alarmd worker: Slot not begun: " + string(notBegun.result.ReasonCode)
}

func (notBegun *slotNotBegun) Unwrap() error { return notBegun.err }

type slotWritesContextKey struct{}

func withSlotWrites(ctx context.Context, coordinator *SlotExecutionCoordinator, request execution.SlotExecutionRequest) (
	context.Context, *slotWrites) {
	writes := &slotWrites{coordinator: coordinator, request: request}
	return context.WithValue(ctx, slotWritesContextKey{}, writes), writes
}

func slotWritesFrom(ctx context.Context) *slotWrites {
	writes, _ := ctx.Value(slotWritesContextKey{}).(*slotWrites)
	return writes
}

// fenceStep begins a step: a write after it whose script does not run the
// fence stands behind a check made after it.
func fenceStep(ctx context.Context) {
	if writes := slotWritesFrom(ctx); writes != nil {
		writes.mu.Lock()
		writes.fenced = false
		writes.mu.Unlock()
	}
}

// deferBegin puts request's BeginSlot off until the attempt's first write.
func (writes *slotWrites) deferBegin(request execution.ProgressBeginRequest) {
	writes.mu.Lock()
	defer writes.mu.Unlock()
	writes.owed, writes.deferred = &request, true
}

// unbegun is whether the attempt put its BeginSlot off and has not made it:
// its commit carries no unfinished projection.
func (writes *slotWrites) unbegun() bool {
	if writes == nil {
		return false
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	return writes.owed != nil
}

// commitStandsIn marks the BeginSlot owed spent on the commit about to be
// sent without it: whatever the commit's answer, no BeginSlot follows.
func (writes *slotWrites) commitStandsIn() {
	writes.mu.Lock()
	defer writes.mu.Unlock()
	writes.owed, writes.stoodIn = nil, true
}

// standsIn is whether the attempt's commit was sent in the place of its
// BeginSlot.
func (writes *slotWrites) standsIn() bool {
	if writes == nil {
		return false
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	return writes.stoodIn
}

// commitRefusedUnbegun is a commit sent in the BeginSlot's place that the
// record refused (status): the attempt is counted and returns as that
// BeginSlot at its start would have, result or err.
func (writes *slotWrites) commitRefusedUnbegun(status execution.ProgressCommitStatus, result execution.SlotExecutionResult, err error) {
	writes.coordinator.countSlotBegin(status)
	writes.mu.Lock()
	defer writes.mu.Unlock()
	writes.failed = &slotNotBegun{result: result, err: err}
}

// beginOnExit makes the BeginSlot an attempt still owes as it leaves
// without writing its Slot down, on a context of its own. An attempt the
// owner fence turned away (exitErr, ownership's refusal) makes none: the
// BeginSlot would be refused the same, and the attempt is counted and
// returns as one refused at its start, its own refusal its error. committed
// is that the attempt wrote its Slot down.
func (writes *slotWrites) beginOnExit(ctx context.Context, exitErr error, committed bool) {
	if writes == nil || committed {
		return
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	if writes.owed == nil || writes.failed != nil {
		return
	}
	if reason, refused := ownership.RefusalReason(exitErr); refused {
		status := execution.ProgressStaleOwner
		if reason == contract.ReasonContentScopeMoved {
			status = execution.ProgressContentMoved
		}
		writes.coordinator.countSlotBegin(status)
		writes.owed = nil
		writes.failed = &slotNotBegun{err: exitErr}
		return
	}
	exitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), slotBeginOnExitTimeout)
	defer cancel()
	_ = writes.beginLocked(exitCtx)
}

// notBegun is the put-off BeginSlot that did not begin the Slot, nil when
// there is none.
func (writes *slotWrites) notBegun() *slotNotBegun {
	if writes == nil {
		return nil
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	return writes.failed
}

// writeClearance is what beforeWrite cleared a write with, on the context
// the write is sent with: unfenced, that the fence was confirmed for a write
// whose script does not run it.
type writeClearance struct{ unfenced bool }

type writeClearanceContextKey struct{}

// beforeWrite is what a write needs before it is sent: the Slot begun, when
// the attempt put its BeginSlot off, and, for a write whose script does not
// run the owner fence (unfenced), the fence confirmed in this step. Every
// write of a Slot attempt calls it, the frozen-State renewal alone does not,
// and is sent with the context it returns, which says it was cleared.
func (coordinator *SlotExecutionCoordinator) beforeWrite(ctx context.Context, unfenced bool) (context.Context, error) {
	writes := slotWritesFrom(ctx)
	if writes == nil {
		return ctx, nil
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	if err := writes.beginLocked(ctx); err != nil {
		return ctx, err
	}
	cleared := context.WithValue(ctx, writeClearanceContextKey{}, writeClearance{unfenced: unfenced})
	if !unfenced || writes.fenced {
		return cleared, nil
	}
	if writes.refused != nil {
		return ctx, writes.refused
	}
	request := writes.request
	started := time.Now()
	err := coordinator.ports.Admission.CheckOwnerFence(ctx, request.Contract, request.OwnerFence)
	var reason execution.ReasonCode
	if named, ok := ownership.RefusalReason(err); ok {
		reason = execution.ReasonCode(named)
	}
	// The fence check's line, as the admission's was while it checked the
	// fence: it is what fence_checked and its refusals are read from.
	coordinator.observe(ctx, observability.ComponentState, observability.StageSideEffectAdmission, request.Operation, started, "", reason, err)
	if err != nil {
		writes.refused = fmt.Errorf("alarmd worker: side-effect admission: %w", err)
		return ctx, writes.refused
	}
	writes.fenced = true
	return cleared, nil
}

// beginLocked makes the BeginSlot the attempt put off, once; nil when it
// owes none. Under writes.mu.
func (writes *slotWrites) beginLocked(ctx context.Context) error {
	if writes.failed != nil {
		return writes.failed
	}
	if writes.owed == nil {
		return nil
	}
	request := *writes.owed
	coordinator, attempt := writes.coordinator, writes.request
	started := time.Now()
	begin, err := coordinator.ports.Progress.BeginSlot(ctx, request)
	observability.ObserveSlotWait(ctx, coordinator.ports.Observer, observability.SlotWaitProgressBegin,
		observability.Operation(attempt.Operation), started, time.Now)
	if err != nil {
		writes.failed = &slotNotBegun{result: coordinator.progressBeginFailure(ctx, attempt, started, err)}
		return writes.failed
	}
	coordinator.countSlotBegin(begin.Status)
	if err := begin.Validate(); err != nil {
		writes.failed = &slotNotBegun{err: fmt.Errorf("alarmd worker: invalid Progress BeginSlot result: %w", err)}
		return writes.failed
	}
	if begin.Status != execution.ProgressCommitted {
		if refused := progressBeginRefused(begin.Status); refused != nil {
			writes.failed = &slotNotBegun{err: refused}
			return writes.failed
		}
		writes.failed = &slotNotBegun{result: coordinator.progressBeginNotCommitted(ctx, attempt, started, begin)}
		return writes.failed
	}
	writes.owed = nil
	// Its script ran the fence as it wrote: the step's unfenced writes
	// stand behind it as behind a check.
	writes.fenced = true
	return nil
}

// watchedPorts, when set, wraps the ports of every coordinator made after
// it: this package's tests, watching that each write of every Slot attempt
// they run is sent as beforeWrite cleared it. Nil outside them.
var watchedPorts func(Ports) Ports

// progressBeginDeferrer is a Progress store that can tell a BeginSlot that
// may wait for the Slot's first write (progress.Store).
type progressBeginDeferrer interface {
	BeginDeferrable(execution.ProgressBeginRequest) bool
}
