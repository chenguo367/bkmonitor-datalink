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
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// degradedRig is a coordinator over a memory store holding one Plan whose
// member joined and was compared, its clock at the rounds', dispatching
// with a segment skip word its view follows.
type degradedRig struct {
	t           *testing.T
	store       *memorySegments
	observed    *shadowObservations
	coordinator *SlotExecutionCoordinator
	due         execution.DuePlan
	key         segment.PlanKey
	member      execution.SeriesIdentityDigest
	at          int64
}

func newDegradedRig(t *testing.T) *degradedRig {
	t.Helper()
	rig := &degradedRig{t: t, store: newMemorySegments(), observed: &shadowObservations{}, member: shadowSeries(0), at: 1440,
		key: segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}}
	runShadowFor(t, rig.store, shadowDue, []shadowRoundOf{steady(t, rig.member, 1320)}, false)
	runShadowFor(t, rig.store, shadowDue, []shadowRoundOf{steady(t, rig.member, 1380)}, false)
	rig.due = shadowDue
	rig.due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
	rig.coordinator = &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: rig.store},
		Now: func() time.Time { return time.Unix(rig.at, 0) }, Observer: rig.observed,
		InstalledControlEpoch: func() (uint64, bool) { return 7, true }}}
	return rig
}

// request is the Slot at the rig's time, its attempt and word as given.
func (rig *degradedRig) request(operation execution.Operation, attempt uint32, word uint64) execution.SlotExecutionRequest {
	request := shadowRequest(operation, rig.at)
	request.AttemptNo, request.SegmentSkip = attempt, word
	return request
}

func (rig *degradedRig) load(request execution.SlotExecutionRequest) (*segmentSlot, string, error) {
	return rig.coordinator.loadSegments(context.Background(), request, execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{rig.due}})
}

// degraded is what the rig's Slots counted degraded so far, by why.
func (rig *degradedRig) degraded() map[string]int64 {
	counted := map[string]int64{}
	for _, observation := range rig.observed.observed {
		if observation.SegmentShadow != nil {
			for why, count := range observation.SegmentShadow.Degraded {
				counted[why] += count
			}
		}
	}
	return counted
}

// round runs the member's round at the rig's time under a first normal
// attempt with the word, and moves the clock on a round: whether the member
// was left, and the Plan's write's skip mode.
func (rig *degradedRig) round() (bool, string) {
	rig.t.Helper()
	request := rig.request(execution.OperationNormal, 1, 7)
	defer func() { rig.at += 60 }()
	slot, _, err := rig.load(request)
	if err != nil || slot == nil {
		rig.t.Fatalf("the round at %d read (%v, %v)", rig.at, slot != nil, err)
	}
	round := newSegmentShadowRound(request, slot)
	series := steady(rig.t, rig.member, rig.at)
	left := round.leaves(rig.due, series.view, series.write, series.class, false)
	if !left {
		round.observe(rig.due, series.view, series.write, series.class, false)
	}
	rig.coordinator.foldSegmentShadow(context.Background(), request, round, []execution.DuePlan{rig.due}, nil)
	return left, rig.store.written[rig.key].SkipMode
}

// A Slot dispatched with a segment skip word whose segments did not read -
// the read failing whole, or its Plan's not answering - fails its first
// attempt by SEGMENT_READ_FAILED; its retry, an attempt numbered past one
// or a retry operation, reads them as stored, counted unread once for the
// Slot; without a word the Slot reads them as stored and fails nothing, as
// in the second stage.
func TestASlotWithAWordWhoseSegmentsDidNotReadFailsItsFirstAttemptAndItsRetryReadsThemAsStored(t *testing.T) {
	for _, shape := range []string{"read failed", "its Plan's unanswered"} {
		t.Run(shape, func(t *testing.T) {
			rig := newDegradedRig(t)
			if shape == "read failed" {
				rig.store.readFails = true
			} else {
				rig.store.unread = map[segment.PlanKey]bool{rig.key: true}
			}
			_, _, err := rig.load(rig.request(execution.OperationNormal, 1, 7))
			if word, named := SegmentReadFailedReason(err); !errors.Is(err, ErrSegmentsUnread) || !named || word != "SEGMENT_READ_FAILED" {
				t.Fatalf("the first attempt = %v, want SEGMENT_READ_FAILED", err)
			}
			if notCommittedReason(execution.FinalizationQueryRequired, execution.SlotExecutionResult{}, err) !=
				execution.SlotNotCommittedSegmentReadFailed {
				t.Fatal("the first attempt's failure is not counted segment_read_failed")
			}
			for _, retry := range []execution.SlotExecutionRequest{rig.request(execution.OperationRetry, 2, 7),
				rig.request(execution.OperationNormal, 2, 7)} {
				slot, _, err := rig.load(retry)
				if err != nil {
					t.Fatalf("the retry (%s, attempt %d) = %v, want its segments read as stored", retry.Operation, retry.AttemptNo, err)
				}
				if shape != "read failed" && (slot == nil || !slot.plans[rig.key].fresh) {
					t.Fatal("the retry covers the Plan's members with a book it did not read")
				}
			}
			if got := rig.degraded(); got[observability.SegmentDegradedUnread] != 1 || got[observability.SegmentDegradedStopped] != 0 {
				t.Fatalf("degraded %v, want the Plan unread once for the Slot", got)
			}
			if _, _, err := rig.load(rig.request(execution.OperationNormal, 1, 0)); err != nil {
				t.Fatalf("without a word: %v, want its segments read as stored", err)
			}
			if got := rig.degraded(); got[observability.SegmentDegradedUnread] != 1 {
				t.Fatalf("without a word degraded %v, want nothing more", got)
			}
		})
	}
}

// A Plan degraded in a second Slot of its owner's term is stopped: counted
// so, its later Slots no longer fail first for its segments not reading,
// and once they read, it leaves no member - its writes say stopped - a
// decodable read not clearing it. A new owner's term, or the Query Group
// released, starts over.
func TestAPlanDegradedInASecondSlotIsStoppedUntilItsOwnerChanges(t *testing.T) {
	rig := newDegradedRig(t)
	rig.store.readFails = true
	for slot := 0; slot < 2; slot++ {
		if _, _, err := rig.load(rig.request(execution.OperationNormal, 1, 7)); !errors.Is(err, ErrSegmentsUnread) {
			t.Fatalf("Slot %d's first attempt = %v, want SEGMENT_READ_FAILED", slot, err)
		}
		if _, _, err := rig.load(rig.request(execution.OperationRetry, 2, 7)); err != nil {
			t.Fatalf("Slot %d's retry = %v", slot, err)
		}
		rig.at += 60
	}
	if got := rig.degraded(); got[observability.SegmentDegradedUnread] != 2 || got[observability.SegmentDegradedStopped] != 1 {
		t.Fatalf("degraded %v, want unread twice and the Plan stopped", got)
	}
	if _, _, err := rig.load(rig.request(execution.OperationNormal, 1, 7)); err != nil {
		t.Fatalf("a stopped Plan's Slot still fails first: %v", err)
	}
	rig.at += 60
	rig.store.readFails = false
	for round := 0; round < 3; round++ {
		if left, mode := rig.round(); left || mode != segment.SkipModeStopped {
			t.Fatalf("a stopped Plan's round left %v, its write says %q", left, mode)
		}
	}
	moved := rig.request(execution.OperationNormal, 1, 7)
	moved.OwnerFence.OwnerEpoch++
	if rig.coordinator.segmentDegradations.stopped(moved, rig.key) {
		t.Fatal("a new owner's term finds the Plan stopped")
	}
	// The new term counts afresh: its first degradation does not stop the
	// Plan, its second does.
	for slot := 1; slot <= 2; slot++ {
		moved.Contract.Slot.EvaluationTime += 60
		rig.coordinator.segmentDegradations.degrade(moved, rig.key)
		if stopped := rig.coordinator.segmentDegradations.stopped(moved, rig.key); stopped != (slot == 2) {
			t.Fatalf("after %d degradations in the new term stopped is %v", slot, stopped)
		}
	}
	ForgetQueryGroup(rig.coordinator, moved.Contract.Slot.QueryGroup)
	if rig.coordinator.segmentDegradations.stopped(rig.request(execution.OperationNormal, 1, 7), rig.key) {
		t.Fatal("the Query Group released, the Plan is still stopped")
	}
}

// The pool refusing the books of a Slot dispatched with a word fails its
// first attempt; the retry runs without them, counted book_refused; the
// Plan refused so in a second Slot is stopped, and the Slot after does not
// fail first.
func TestBooksTheRetainedPoolKeepsRefusingStopThePlanAfterTwoRetries(t *testing.T) {
	rig := newDegradedRig(t)
	books := segment.BookBytes(1)
	rig.coordinator.budget = ProvisionalBudget{MaxSeries: 1000, MaxRetainedBytes: books - 1}
	hold := func(request execution.SlotExecutionRequest) (*segmentSlot, error) {
		t.Helper()
		stream := &streamedExecution{coordinator: rig.coordinator, request: request, began: true}
		return stream.holdSegments(context.Background(), shadowSlot(t, rig.store, request, shadowDue), "")
	}
	for slot := 0; slot < 2; slot++ {
		if _, err := hold(rig.request(execution.OperationNormal, 1, 7)); !errors.Is(err, ErrSegmentsUnread) {
			t.Fatalf("Slot %d's first attempt = %v, want SEGMENT_READ_FAILED", slot, err)
		}
		if held, err := hold(rig.request(execution.OperationRetry, 2, 7)); held != nil || err != nil {
			t.Fatalf("Slot %d's retry = (%v, %v), want no books and no failure", slot, held != nil, err)
		}
		rig.at += 60
	}
	if got := rig.degraded(); got[observability.SegmentDegradedBookRefused] != 2 || got[observability.SegmentDegradedStopped] != 1 {
		t.Fatalf("degraded %v, want book_refused twice and the Plan stopped", got)
	}
	if held, err := hold(rig.request(execution.OperationNormal, 1, 7)); held != nil || err != nil {
		t.Fatalf("the stopped Plan's next Slot = (%v, %v), want it run without its books, not failed", held != nil, err)
	}
	if held, err := hold(rig.request(execution.OperationNormal, 1, 0)); held != nil || err != nil {
		t.Fatalf("without a word = (%v, %v), want it run without its books", held != nil, err)
	}
}

// Segments that alternately do not decode: the first time the Plan starts
// over and is skipped again once its member has joined back; the second
// time it is stopped, and no round after leaves its member, though every
// read decodes.
func TestAPlanWhoseSegmentsDoNotDecodeASecondTimeIsStopped(t *testing.T) {
	rig := newDegradedRig(t)
	if left, _ := rig.round(); !left {
		t.Fatal("the member is not left before anything went wrong")
	}
	corrupt := func() { rig.store.marks[rig.key] = []byte("not a mark") }
	corrupt()
	resumed := false
	for round := 0; round < 4; round++ {
		if left, _ := rig.round(); left {
			resumed = true
		}
	}
	if got := rig.degraded(); !resumed || got[observability.SegmentDegradedUndecodable] != 1 || got[observability.SegmentDegradedStopped] != 0 {
		t.Fatalf("after one undecodable read: skipping resumed %v, degraded %v", resumed, got)
	}
	corrupt()
	for round := 0; round < 5; round++ {
		if left, mode := rig.round(); left || (round > 0 && mode != segment.SkipModeStopped) {
			t.Fatalf("round %d after the second: left %v, its write says %q", round, left, mode)
		}
	}
	if got := rig.degraded(); got[observability.SegmentDegradedUndecodable] != 2 || got[observability.SegmentDegradedStopped] != 1 {
		t.Fatalf("degraded %v, want undecodable twice and the Plan stopped", got)
	}
}

// A Plan whose round's write cannot be made - no lifetime to give its keys
// - leaves no member and fails no Slot, is counted write_absent each Slot
// and stopped at the second.
func TestAPlanWhoseWriteIsNotMadeIsStoppedAtTheSecondSlot(t *testing.T) {
	rig := newDegradedRig(t)
	rig.store.noLife = true
	for round := 0; round < 3; round++ {
		request := rig.request(execution.OperationNormal, 1, 7)
		slot, _, err := rig.load(request)
		if err != nil || slot == nil {
			t.Fatalf("round %d read (%v, %v)", round, slot != nil, err)
		}
		shadow := newSegmentShadowRound(request, slot)
		series := steady(t, rig.member, rig.at)
		if shadow.leaves(rig.due, series.view, series.write, series.class, false) {
			t.Fatalf("round %d left a member of a Plan whose write cannot be made", round)
		}
		shadow.observe(rig.due, series.view, series.write, series.class, false)
		if rig.coordinator.foldSegmentShadow(context.Background(), request, shadow, []execution.DuePlan{rig.due}, nil) {
			t.Fatalf("round %d failed its Slot", round)
		}
		rig.at += 60
	}
	if got := rig.degraded(); got[observability.SegmentDegradedWriteAbsent] != 3 || got[observability.SegmentDegradedStopped] != 1 {
		t.Fatalf("degraded %v, want write_absent each Slot and the Plan stopped once", got)
	}
}

// The generation a Plan carries from not answering, in a Slot with a word,
// fails its first attempt as its own segments would, and its retry reads the
// carried series as stored, counted unread against the Plan; a carried
// generation that does not decode is counted undecodable against it.
func TestTheGenerationAPlanCarriesFromNotReadingDegradesThePlan(t *testing.T) {
	for _, shape := range []string{"unanswered", "undecodable"} {
		t.Run(shape, func(t *testing.T) {
			rig := newDegradedRig(t)
			from := rig.key
			rig.due.StateGeneration, rig.due.StateCarry = "g1", &execution.StateCarry{From: shadowDue.StateGeneration}
			own := segment.PlanKey{Plan: rig.due.Identity, Generation: "g1"}
			if shape == "unanswered" {
				rig.store.unread = map[segment.PlanKey]bool{from: true}
				if _, _, err := rig.load(rig.request(execution.OperationNormal, 1, 7)); !errors.Is(err, ErrSegmentsUnread) {
					t.Fatalf("the first attempt = %v, want SEGMENT_READ_FAILED", err)
				}
			} else {
				rig.store.marks[from] = []byte("not a mark")
			}
			slot, _, err := rig.load(rig.request(execution.OperationRetry, 2, 7))
			if err != nil || slot == nil || len(slot.carried) != 0 {
				t.Fatalf("the retry = (%v, %v), want the Plan's own segments and no carried book", slot != nil, err)
			}
			want := map[string]string{"unanswered": observability.SegmentDegradedUnread, "undecodable": observability.SegmentDegradedUndecodable}[shape]
			if got := rig.degraded(); got[want] != 1 || len(got) != 1 {
				t.Fatalf("degraded %v, want the Plan %s once", got, want)
			}
			rig.coordinator.segmentDegradations.mu.Lock()
			times := rig.coordinator.segmentDegradations.byGroup[rig.request(execution.OperationNormal, 1, 7).Contract.Slot.QueryGroup].times[own]
			rig.coordinator.segmentDegradations.mu.Unlock()
			if times != 1 {
				t.Fatalf("the Plan's own generation noted %d times, want once", times)
			}
		})
	}
}
