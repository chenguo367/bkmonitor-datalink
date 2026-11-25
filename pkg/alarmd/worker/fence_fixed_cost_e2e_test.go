// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// 014 ⑩ through Execute, with the fence and Progress wired as production
// wires them (cmd/alarmd/runtime_phase_two_bundle.go): the ownership store's
// admitter, its fence read from Redis; the Progress store over the
// ownership store's fenced compare-and-set; the gap markers, the State and
// the segments in the execution store; the Query Group assigned and leased.
// The fence checks, the activation reads and the fenced compare-and-sets
// are counted where they reach the stores.

// countedFences is the ownership store's fence check, counted.
type countedFences struct {
	store  *ownership.RedisStore
	checks atomic.Int64
}

func (fences *countedFences) CheckFence(ctx context.Context, fence execution.OwnerFence) error {
	fences.checks.Add(1)
	return fences.store.CheckFence(ctx, fence)
}

// countedActivation answers every Plan active, but the read inactiveAt,
// counted from the first, which answers it not: an activation that moved
// under the round.
type countedActivation struct {
	reads      atomic.Int64
	inactiveAt atomic.Int64
}

func (activation *countedActivation) IsPlanActive(context.Context, execution.FrozenExecutionContractRef, execution.PlanKey,
	execution.StateApplyEpoch) (bool, error) {
	return activation.reads.Add(1) != activation.inactiveAt.Load(), nil
}

// countedControl is the ownership store as Progress's control store, its
// fenced compare-and-sets counted.
type countedControl struct {
	*ownership.RedisStore
	cas atomic.Int64
}

func (control *countedControl) FencedCompareAndSet(ctx context.Context, request ownership.FencedCASRequest) (ownership.FencedCASStatus, error) {
	control.cas.Add(1)
	return control.RedisStore.FencedCompareAndSet(ctx, request)
}

// slotAttemptCounts is the coordinator's slot_begin_total and
// slot_not_committed_total.
type slotAttemptCounts struct {
	mu           sync.Mutex
	begins       map[string]int
	notCommitted map[string]int
}

func (counts *slotAttemptCounts) RecordSlotBegin(result string) {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	counts.begins[result]++
}

func (counts *slotAttemptCounts) RecordSlotNotCommitted(reason string) {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	counts.notCommitted[reason]++
}

func (counts *slotAttemptCounts) begun(result string) int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.begins[result]
}

type fixedCostRig struct {
	*shadowRig
	fences     *countedFences
	activation *countedActivation
	control    *countedControl
	progress   *progress.Store
	counts     *slotAttemptCounts
	at         execution.EvaluationTime
	prefix     string
}

// fixedCostNow is the Progress store's clock: a record committed twice is
// the same bytes.
var fixedCostNow = time.Unix(1_788_100_000, 0).UTC()

func newFixedCostRig(t *testing.T, hosts int, prefix string, wire ...func(*fixedCostRig, *worker.Ports)) *fixedCostRig {
	t.Helper()
	rig := newShadowRig(t, hosts, 0, shadowRecoveryOff)
	fixed := &fixedCostRig{shadowRig: rig, fences: &countedFences{store: rig.owners}, activation: &countedActivation{},
		control: &countedControl{RedisStore: rig.owners},
		counts:  &slotAttemptCounts{begins: map[string]int{}, notCommitted: map[string]int{}},
		at:      frozenContract().Slot.EvaluationTime, prefix: prefix}
	admitter, err := ownership.NewAdmitter(fixed.fences, fixed.activation)
	if err != nil {
		t.Fatal(err)
	}
	fixed.progress, err = progress.NewStore(progress.StoreOptions{Prefix: prefix, Control: fixed.control,
		Slots: g3aFixedSlotResolver{}, Now: func() time.Time { return fixedCostNow }})
	if err != nil {
		t.Fatal(err)
	}
	ports := rig.wiring
	ports.Admission, ports.Progress, ports.GapGuard, ports.SlotCounts = admitter, fixed.progress, rig.store, fixed.counts
	for _, wiring := range wire {
		wiring(fixed, &ports)
	}
	if rig.coordinator, err = worker.NewSlotExecutionCoordinator(ports, rig.budget); err != nil {
		t.Fatal(err)
	}
	return fixed
}

// fixedCostRound is what one Slot attempt did, as the stores saw it.
type fixedCostRound struct {
	result          execution.SlotExecutionResult
	err             error
	fenceChecks     int64
	activationReads int64
	fencedCAS       int64
	trace           []string
}

func (round fixedCostRound) wrote(stages ...string) bool {
	for _, written := range round.trace {
		for _, stage := range stages {
			if written == stage {
				return true
			}
		}
	}
	return false
}

// round runs the Slot interval after the last, or again the last when again.
func (fixed *fixedCostRig) round(t *testing.T, again bool, absent []string, value map[string]string) fixedCostRound {
	t.Helper()
	if !again {
		fixed.at += 60
	}
	fences, reads, cas := fixed.fences.checks.Load(), fixed.activation.reads.Load(), fixed.control.cas.Load()
	traced := len(*fixed.ports.trace)
	result, err := fixed.execute(fixed.at, absent, value, false)
	return fixedCostRound{result: result, err: err, fenceChecks: fixed.fences.checks.Load() - fences,
		activationReads: fixed.activation.reads.Load() - reads, fencedCAS: fixed.control.cas.Load() - cas,
		trace: append([]string(nil), (*fixed.ports.trace)[traced:]...)}
}

// warm runs rounds until one writes no gap marker and sends no event, the
// round a steady Query Group's every round is.
func (fixed *fixedCostRig) warm(t *testing.T) {
	t.Helper()
	for round := 0; round < 12; round++ {
		ran := fixed.round(t, false, nil, nil)
		if ran.err != nil || !ran.result.Completed {
			t.Fatalf("warming round %d: result=%+v err=%v trace=%v", round, ran.result, ran.err, ran.trace)
		}
		if round >= 2 && !ran.wrote("event_ack") && ran.fenceChecks == 0 {
			return
		}
	}
	t.Fatal("no round of twelve was steady: every one checked the fence or sent events")
}

// A steady round - its gap markers settled, nothing anomalous - writes its
// State and segments, whose scripts run the fence, and nothing whose script
// does not: Execute checks the fence not once, and reads each Plan's
// activation where it did, so an activation that moves under the round
// still stops it (014 ⑩ §7 case 1).
func TestASteadyRoundChecksTheFenceNotOnceAndItsActivationAsBefore(t *testing.T) {
	fixed := newFixedCostRig(t, 6, "alarmd-fixed-cost-steady")
	fixed.warm(t)
	steady := fixed.round(t, false, nil, nil)
	if steady.err != nil || !steady.result.Completed {
		t.Fatalf("steady round: result=%+v err=%v trace=%v", steady.result, steady.err, steady.trace)
	}
	if steady.wrote("event_ack") {
		t.Fatalf("precondition: the steady round sent events: %v", steady.trace)
	}
	if steady.fenceChecks != 0 {
		t.Fatalf("a steady round checked the fence %d times, want none: its writes' scripts run it", steady.fenceChecks)
	}
	if steady.activationReads == 0 {
		t.Fatal("a steady round read no Plan's activation")
	}
	// The next round's first activation read answers the Plan not active.
	fixed.activation.inactiveAt.Store(fixed.activation.reads.Load() + 1)
	drifted := fixed.round(t, false, nil, nil)
	if drifted.result.Completed {
		t.Fatalf("an activation that moved under the round completed it: %+v", drifted.result)
	}
	if said := drifted.result.ReasonCode; !strings.Contains(errorText(drifted.err)+string(said), contract.ReasonConfigDrift) {
		t.Fatalf("the moved activation stopped the round with result=%+v err=%v, want CONFIG_DRIFT", drifted.result, drifted.err)
	}
}

// A steady Query Group's anomalous round puts its BeginSlot off to its
// first write, its events; the BeginSlot's script runs the fence as it
// writes, so the events stand behind it and no check is made of its own:
// two fenced writes, the BeginSlot and the commit, and no fence check
// (014 ⑩ F1, F5). An attempt that began at its start checks the fence
// before its events instead (coordinator_test.go, fullTrace).
func TestAnAnomalousRoundsEventsStandBehindItsPutOffBeginSlot(t *testing.T) {
	fixed := newFixedCostRig(t, 6, "alarmd-fixed-cost-events")
	fixed.warm(t)
	anomalous := fixed.round(t, false, nil, map[string]string{"host-01": "10"}) // a drop past the floor
	if anomalous.err != nil || !anomalous.result.Completed {
		t.Fatalf("anomalous round: result=%+v err=%v trace=%v", anomalous.result, anomalous.err, anomalous.trace)
	}
	if !anomalous.wrote("event_ack") {
		t.Fatalf("precondition: the anomalous round sent no event: %v", anomalous.trace)
	}
	if anomalous.fenceChecks != 0 || anomalous.fencedCAS != 2 {
		t.Fatalf("an anomalous round checked the fence %d times in %d fenced writes, want none in its BeginSlot and its commit",
			anomalous.fenceChecks, anomalous.fencedCAS)
	}
}

// A round that writes nothing - no series answered, its markers settled -
// makes one fenced write: its commit, the BeginSlot put off and never made.
// What it leaves in Redis is the record a BeginSlot and a commit would
// have left, byte for byte; the attempt is counted deferred, and the begin
// results still sum to the attempts (014 ⑩ §7 cases 4 and 7).
func TestAnEmptyRoundCommitsItsSlotWithOneFencedWriteAndTheSameRecord(t *testing.T) {
	deferred := newFixedCostRig(t, 0, "alarmd-fixed-cost-empty")
	deferred.warm(t)
	before := deferred.counts.begun(execution.SlotBeginDeferred)
	empty := deferred.round(t, false, nil, nil)
	if empty.err != nil || !empty.result.Completed {
		t.Fatalf("empty round: result=%+v err=%v trace=%v", empty.result, empty.err, empty.trace)
	}
	if empty.fencedCAS != 1 {
		t.Fatalf("an empty round made %d fenced compare-and-sets, want its commit alone", empty.fencedCAS)
	}
	if got := deferred.counts.begun(execution.SlotBeginDeferred) - before; got != 1 {
		t.Fatalf("an empty round was counted deferred %d times, want once", got)
	}
	// The same rounds on a store that begins every Slot: its record after
	// them is the record the deferred rounds left.
	begun := newFixedCostRig(t, 0, "alarmd-fixed-cost-begun")
	begun.coordinator = coordinatorBeginningEverySlot(t, begun)
	for begun.at < deferred.at {
		if ran := begun.round(t, false, nil, nil); ran.err != nil || !ran.result.Completed {
			t.Fatalf("round at %d on the beginning store: result=%+v err=%v", begun.at, ran.result, ran.err)
		}
	}
	if got, want := storedProgress(t, deferred), storedProgress(t, begun); !bytes.Equal(got, want) {
		t.Fatalf("the deferred round left\n%s\nwhere begin and commit leave\n%s", got, want)
	}
	attempts := 0
	for _, result := range execution.SlotBeginResults {
		attempts += deferred.counts.begun(result)
	}
	if rounds := int((deferred.at - frozenContract().Slot.EvaluationTime) / 60); attempts != rounds {
		t.Fatalf("slot_begin_total sums to %d over %d attempts", attempts, rounds)
	}
}

// An attempt that put its BeginSlot off and leaves without writing its Slot
// down - here an activation moved under it, turned away before any write -
// begins the Slot on its way out: what it leaves is the record a BeginSlot
// at its start left, byte for byte, its unfinished projection for the next
// attempt and a successor to read, and it is counted as one that began and
// returned (014 ⑩ F5). An empty round that commits makes no BeginSlot.
func TestAnAttemptLeavingUncommittedLeavesTheSlotBegunAsAtItsStart(t *testing.T) {
	deferred := newFixedCostRig(t, 6, "alarmd-fixed-cost-exit")
	deferred.warm(t)
	committed := deferred.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted))
	deferred.activation.inactiveAt.Store(deferred.activation.reads.Load() + 1)
	turned := deferred.round(t, false, nil, nil)
	if turned.result.Completed || turned.wrote("event_ack", "gap_before", "gap_after") {
		t.Fatalf("precondition: the round was to be turned away before any write: result=%+v err=%v trace=%v", turned.result, turned.err, turned.trace)
	}
	if turned.fencedCAS != 1 {
		t.Fatalf("a round turned away made %d fenced compare-and-sets, want its BeginSlot on the way out", turned.fencedCAS)
	}
	if got := deferred.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted)) - committed; got != 1 {
		t.Fatalf("a round turned away was counted begun %d times, want once", got)
	}
	deferred.counts.mu.Lock()
	notCommitted := 0
	for _, count := range deferred.counts.notCommitted {
		notCommitted += count
	}
	reasons := fmt.Sprint(deferred.counts.notCommitted)
	deferred.counts.mu.Unlock()
	if notCommitted != 1 {
		t.Fatalf("a round turned away was counted not committed %s, want once", reasons)
	}

	// The same rounds on a store that begins every Slot at its start, sent
	// the same requests: the deadlines a request carries, which its
	// unfinished projection keeps, are the rig's, taken from the clock as
	// it was made.
	begun := newFixedCostRig(t, 6, "alarmd-fixed-cost-exit-begun")
	begun.coordinator = coordinatorBeginningEverySlot(t, begun)
	begun.base.EarliestQueryDeadlineUnixMilli = deferred.base.EarliestQueryDeadlineUnixMilli
	begun.base.KeepUntilUnixMilli = deferred.base.KeepUntilUnixMilli
	for begun.at < deferred.at-60 {
		if ran := begun.round(t, false, nil, nil); ran.err != nil || !ran.result.Completed {
			t.Fatalf("round at %d on the beginning store: result=%+v err=%v", begun.at, ran.result, ran.err)
		}
	}
	begun.activation.inactiveAt.Store(begun.activation.reads.Load() + 1)
	if ran := begun.round(t, false, nil, nil); ran.result.Completed {
		t.Fatalf("the beginning store's turned-away round completed: %+v", ran.result)
	}
	if got, want := storedProgress(t, deferred), storedProgress(t, begun); !bytes.Equal(got, want) {
		t.Fatalf("the round turned away left\n%s\nwhere a BeginSlot at its start leaves\n%s", got, want)
	}
}

// An earlier attempt's unfinished Slot is its evidence: the next attempt
// begins as before, so the BeginSlot judges it, rather than a round
// committed over it erasing it unread (014 ⑩ §7 case 5).
func TestAnUnfinishedSlotLeftBehindIsBegunAsBefore(t *testing.T) {
	fixed := newFixedCostRig(t, 6, "alarmd-fixed-cost-unfinished")
	fixed.warm(t)
	// An attempt that begins its Slot and whose events are not acknowledged:
	// retried, its Slot left unfinished.
	fixed.ports.failStage = "event_ack"
	failed := fixed.round(t, false, nil, map[string]string{"host-01": "10"})
	fixed.ports.failStage = ""
	if failed.result.Completed || !failed.wrote("event_ack") {
		t.Fatalf("precondition: the first attempt was to send its events and fail: result=%+v err=%v trace=%v", failed.result, failed.err, failed.trace)
	}
	committed := fixed.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted))
	retry := fixed.round(t, true, nil, map[string]string{"host-01": "10"})
	if retry.err != nil || !retry.result.Completed {
		t.Fatalf("the retry: result=%+v err=%v trace=%v", retry.result, retry.err, retry.trace)
	}
	if begun := fixed.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted)) - committed; begun != 1 || retry.fencedCAS != 2 {
		t.Fatalf("the retry over an unfinished Slot was begun %d times in %d fenced writes, want its BeginSlot and its commit",
			begun, retry.fencedCAS)
	}

	// An empty round's retry over the unfinished Slot its first attempt
	// left, turned away by an activation that moved and begun on its way
	// out: begun at its start, its commit not sent over that Slot.
	empty := newFixedCostRig(t, 0, "alarmd-fixed-cost-unfinished-empty")
	empty.warm(t)
	empty.activation.inactiveAt.Store(empty.activation.reads.Load() + 1)
	if turned := empty.round(t, false, nil, nil); turned.result.Completed || turned.activationReads == 0 || turned.fencedCAS != 1 {
		t.Fatalf("precondition: the empty round was to be turned away by its activation and begun on its way out: result=%+v err=%v "+
			"activation reads %d, fenced writes %d", turned.result, turned.err, turned.activationReads, turned.fencedCAS)
	}
	committed = empty.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted))
	deferred := empty.counts.begun(execution.SlotBeginDeferred)
	again := empty.round(t, true, nil, nil)
	if again.err != nil || !again.result.Completed {
		t.Fatalf("the empty retry: result=%+v err=%v trace=%v", again.result, again.err, again.trace)
	}
	if begun := empty.counts.begun(execution.SlotBeginResult(execution.ProgressCommitted)) - committed; begun != 1 || again.fencedCAS != 2 ||
		empty.counts.begun(execution.SlotBeginDeferred) != deferred {
		t.Fatalf("the empty retry over an unfinished Slot was begun %d times in %d fenced writes, deferred %d; want its BeginSlot and its commit",
			begun, again.fencedCAS, empty.counts.begun(execution.SlotBeginDeferred)-deferred)
	}
}

// An owner whose lease ended after its round was dispatched is refused at
// its first fenced write, as the owner's refusal: an error that is
// ownership's, no backoff (014 ⑩ §7 case 3). A round that put its BeginSlot
// off is refused at that BeginSlot, made before its first write - or, an
// empty round, at its commit - as a BeginSlot at its start was: nothing of
// it was written, it began no Slot. One whose BeginSlot landed is refused at
// its State as before, owner_refused.
func TestAStaleOwnerIsRefusedAtItsFirstFencedWriteAsTheOwners(t *testing.T) {
	t.Run("put-off BeginSlot at the first write", func(t *testing.T) {
		fixed := newFixedCostRig(t, 6, "alarmd-fixed-cost-stale-begin")
		fixed.warm(t)
		takeLease(t, fixed)
		stale := fixed.round(t, false, nil, nil)
		assertRefusedAsNotBegun(t, fixed, stale)
		// The very error a BeginSlot refused at the start returns, whatever
		// step the put-off one was made in.
		if _, exact := stale.err.(*execution.SlotBeginRefusedError); !exact {
			t.Fatalf("a put-off BeginSlot refused returned %T %v, want the BeginSlot's own refusal", stale.err, stale.err)
		}
	})
	t.Run("empty round at its commit", func(t *testing.T) {
		fixed := newFixedCostRig(t, 0, "alarmd-fixed-cost-stale-empty")
		fixed.warm(t)
		takeLease(t, fixed)
		deferred := fixed.counts.begun(execution.SlotBeginDeferred)
		staleBegins := fixed.counts.begun(execution.SlotBeginResult(execution.ProgressStaleOwner))
		assertRefusedAsNotBegun(t, fixed, fixed.round(t, false, nil, nil))
		// Counted as a BeginSlot refused at its start: by the refusal, not
		// deferred, and not among the attempts that began and returned.
		if got := fixed.counts.begun(execution.SlotBeginResult(execution.ProgressStaleOwner)) - staleBegins; got != 1 {
			t.Fatalf("the refused empty round was counted stale_owner %d times, want once", got)
		}
		if got := fixed.counts.begun(execution.SlotBeginDeferred) - deferred; got != 0 {
			t.Fatalf("the refused empty round was counted deferred %d times, want none", got)
		}
	})
	t.Run("State after the Slot was begun", func(t *testing.T) {
		var taking atomic.Bool
		fixed := newFixedCostRig(t, 6, "alarmd-fixed-cost-stale-state", func(fixed *fixedCostRig, ports *worker.Ports) {
			ports.Events = leaseTakingEvents{EventSink: ports.Events, take: func() {
				if taking.Load() {
					takeLease(t, fixed)
				}
			}}
		})
		fixed.warm(t)
		taking.Store(true)
		stale := fixed.round(t, false, nil, map[string]string{"host-01": "10"}) // anomalous: events, then State
		var notBegun *execution.SlotBeginRefusedError
		if !errors.Is(stale.err, ownership.ErrStaleFence) || errors.As(stale.err, &notBegun) {
			t.Fatalf("a begun round's stale State write returned result=%+v err=%v, want ErrStaleFence, not a BeginSlot's", stale.result, stale.err)
		}
		fixed.counts.mu.Lock()
		refused := fixed.counts.notCommitted[execution.SlotNotCommittedOwnerRefused]
		fixed.counts.mu.Unlock()
		if refused != 1 {
			t.Fatalf("the stale State write was counted owner_refused %d times, want once", refused)
		}
	})
}

// assertRefusedAsNotBegun is a round the fence refused as a BeginSlot at its
// start would have been, nothing of it written; notCommitted is what it is
// counted not committed under, none for a BeginSlot made and refused.
func assertRefusedAsNotBegun(t *testing.T, fixed *fixedCostRig, stale fixedCostRound, notCommitted ...string) {
	t.Helper()
	var notBegun *execution.SlotBeginRefusedError
	if !errors.As(stale.err, &notBegun) || !errors.Is(stale.err, ownership.ErrStaleFence) {
		t.Fatalf("a stale owner's put-off BeginSlot returned result=%+v err=%v, want the BeginSlot's refusal", stale.result, stale.err)
	}
	if stale.wrote("event_ack", "gap_before", "gap_after") {
		t.Fatalf("a round not begun wrote: %v", stale.trace)
	}
	want := map[string]int{}
	for _, reason := range notCommitted {
		want[reason]++
	}
	fixed.counts.mu.Lock()
	defer fixed.counts.mu.Unlock()
	if !reflect.DeepEqual(fixed.counts.notCommitted, want) {
		t.Fatalf("the refused round was counted not committed %v, want %v", fixed.counts.notCommitted, want)
	}
}

// A round that put its BeginSlot off and meets a record moved past its Slot
// - another writer committed it - returns as a BeginSlot at its start would
// have: a retry, PROGRESS_BEGIN_REJECTED, and no error (014 ⑩ F5). The
// empty round finds it at its commit.
func TestAPutOffBeginSlotMeetingAMovedRecordReturnsAsAtItsStart(t *testing.T) {
	fixed := newFixedCostRig(t, 0, "alarmd-fixed-cost-moved")
	fixed.warm(t)
	// The next Slot run and committed by a coordinator whose Progress store
	// keeps no record of the owner's: the rig's record is one Slot behind.
	own := fixed.coordinator
	other, err := progress.NewStore(progress.StoreOptions{Prefix: fixed.prefix, Control: fixed.owners,
		Slots: g3aFixedSlotResolver{}, Now: func() time.Time { return fixedCostNow }})
	if err != nil {
		t.Fatal(err)
	}
	fixed.coordinator = coordinatorOver(t, fixed, other)
	if ran := fixed.round(t, false, nil, nil); ran.err != nil || !ran.result.Completed {
		t.Fatalf("the other coordinator's round: result=%+v err=%v", ran.result, ran.err)
	}
	fixed.coordinator = own
	moved := fixed.round(t, true, nil, nil)
	if moved.err != nil || moved.result.Completed || moved.result.ReasonCode != execution.ReasonCode(contract.ReasonProgressBeginRejected) {
		t.Fatalf("a put-off BeginSlot over a moved record returned result=%+v err=%v, want a retry, PROGRESS_BEGIN_REJECTED", moved.result, moved.err)
	}
}

// coordinatorOver is the rig's coordinator over store as its Progress.
func coordinatorOver(t *testing.T, fixed *fixedCostRig, store execution.ProgressStore) *worker.SlotExecutionCoordinator {
	t.Helper()
	ports := fixed.wiring
	admitter, err := ownership.NewAdmitter(fixed.fences, fixed.activation)
	if err != nil {
		t.Fatal(err)
	}
	ports.Admission, ports.Progress, ports.GapGuard, ports.SlotCounts = admitter, store, fixed.store, fixed.counts
	coordinator, err := worker.NewSlotExecutionCoordinator(ports, fixed.budget)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// takeLease ends the rig's lease, as its expiry does: the rig's fence is
// stale from here.
func takeLease(t *testing.T, fixed *fixedCostRig) {
	t.Helper()
	if err := fixed.owners.Release(context.Background(), fixed.base.OwnerFence); err != nil {
		t.Fatal(err)
	}
}

// coordinatorBeginningEverySlot is the rig's coordinator over a Progress
// store that cannot put a BeginSlot off.
func coordinatorBeginningEverySlot(t *testing.T, fixed *fixedCostRig) *worker.SlotExecutionCoordinator {
	t.Helper()
	return coordinatorOver(t, fixed, beginningProgress{fixed.progress})
}

// beginningProgress is the Progress store with nothing but its port: no
// BeginSlot put off.
type beginningProgress struct{ execution.ProgressStore }

func storedProgress(t *testing.T, fixed *fixedCostRig) []byte {
	t.Helper()
	raw, _, err := fixed.owners.ReadControl(context.Background(), frozenContract().Slot.QueryGroup, fixed.prefix+":progress")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// leaseTakingEvents takes the rig's lease as its batch is written: the
// round's writes after it are a stale owner's.
type leaseTakingEvents struct {
	execution.EventSink
	take func()
}

func (sink leaseTakingEvents) WriteBatch(ctx context.Context, events []contract.TriggerEventV1) error {
	err := sink.EventSink.WriteBatch(ctx, events)
	sink.take()
	return err
}
