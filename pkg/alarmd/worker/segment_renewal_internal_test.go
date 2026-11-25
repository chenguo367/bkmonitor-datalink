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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// renewalRig drives one Plan's rounds as the third stage will run them:
// only the series that are not open members are written - the members are
// kept alive by their pages' renewals alone - and the wall clock the keys
// expire by runs apart from the evaluation time. After every round it holds
// the invariant R (014 B3 3.5): every open member's key expires no sooner
// than its page's renewal time plus the lifetime that renewal gave, and has
// not expired.
type renewalRig struct {
	t     *testing.T
	store *memorySegments
	wall  int64
	key   segment.PlanKey
	total observability.SegmentShadowFacts
	// due is the Plan's due entry, shadowDue's one-minute Plan unless set;
	// operation the rounds', normal unless set.
	due       execution.DuePlan
	operation execution.Operation
}

func newRenewalRig(t *testing.T, ttl time.Duration) *renewalRig {
	t.Helper()
	rig := &renewalRig{t: t, store: newMemorySegments(), wall: time.Unix(1_800_000_000, 0).UnixMilli(),
		key: segment.PlanKey{Plan: shadowDue.Identity, Generation: "g"}, due: shadowDue}
	rig.store.ttl = ttl
	rig.store.wall = func() int64 { return rig.wall }
	rig.store.at = 1140
	return rig
}

// round runs the round after step of wall clock, every series of present
// there and steady, and holds R after it.
func (rig *renewalRig) round(step time.Duration, present []int) observability.SegmentShadowFacts {
	rig.t.Helper()
	rig.wall += step.Milliseconds()
	open := map[execution.SeriesIdentityDigest]bool{}
	if rig.store.marks[rig.key] != nil {
		rig.store.book(rig.t, rig.key.Plan, rig.key.Generation).Each(func(entry segment.Entry) {
			if entry.Open() {
				open[entry.Digest] = true
			}
		})
	}
	at := rig.store.at + 60
	var written []execution.SeriesIdentityDigest
	var rounds []shadowRoundOf
	for _, index := range present {
		digest := shadowSeries(index)
		rounds = append(rounds, steady(rig.t, digest, at-60))
		if !open[digest] {
			written = append(written, digest)
		}
	}
	rig.store.writeState(rig.key, written)
	operation := rig.operation
	if operation == "" {
		operation = execution.OperationNormal
	}
	facts := runShadowForAs(rig.t, rig.store, rig.due, rounds, false, operation)
	for result, count := range facts.Renewals {
		rig.total.Renewal(result, count)
	}
	for _, elapsed := range facts.RenewScripts {
		rig.total.RenewScript(elapsed)
	}
	rig.holdR()
	return facts
}

func (rig *renewalRig) mark() segment.Mark {
	rig.t.Helper()
	mark, err := segment.DecodeMark(rig.store.marks[rig.key])
	if err != nil {
		rig.t.Fatal(err)
	}
	return mark
}

func (rig *renewalRig) holdR() {
	rig.t.Helper()
	mark := rig.mark()
	pages := rig.store.book(rig.t, rig.key.Plan, rig.key.Generation).Pages()
	if len(pages) != mark.Pages {
		rig.t.Fatalf("the book lays out over %d pages, the mark names %d", len(pages), mark.Pages)
	}
	for page, entries := range pages {
		renewal := mark.Renewed[page]
		for _, entry := range entries {
			if !entry.Open() {
				continue
			}
			expires := rig.store.expiry[rig.key][entry.Digest]
			if expires < renewal.At+renewal.TTL {
				rig.t.Fatalf("page %d: a member's key expires at %d, before its renewal %d + %d (R)", page, expires,
					renewal.At, renewal.TTL)
			}
			if expires <= rig.wall {
				rig.t.Fatalf("page %d: an open member's key expired at %d, the wall clock at %d", page, expires, rig.wall)
			}
		}
	}
}

func series(count int) []int {
	indexes := make([]int, count)
	for index := range indexes {
		indexes[index] = index
	}
	return indexes
}

// Members left unwritten round after round, for four lifetimes, stay found:
// their page is renewed every quarter of its lifetime, and nothing else
// keeps their keys alive.
func TestMembersRenewedByTheirPageAloneStayFoundPastTheirLifetime(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	for round := 0; round < 40; round++ {
		rig.round(time.Minute, series(3))
	}
	if rig.total.Renewals[observability.SegmentRenewalRenewed] < 10 || rig.total.Renewals[observability.SegmentRenewalFailed] != 0 ||
		rig.total.Renewals[observability.SegmentRenewalMissing] != 0 || rig.total.Renewals[observability.SegmentRenewalFallbackWrite] != 0 {
		t.Fatalf("renewals over four lifetimes %v, want one every quarter and nothing else", rig.total.Renewals)
	}
	// Each renewed page's script reaches the round's facts with its own run
	// time, for the renewal script histogram.
	if int64(len(rig.total.RenewScripts)) != rig.total.Renewals[observability.SegmentRenewalRenewed] {
		t.Fatalf("%d script run times for %d renewed pages, want one each", len(rig.total.RenewScripts), rig.total.Renewals[observability.SegmentRenewalRenewed])
	}
}

// A renewal that fails - any of its segments - leaves the page's renewal
// time where it was, and the page is renewed whole the round after: R holds
// across, every key renewed or still within the time it had.
func TestARenewalThatFailsLeavesItsTimeAndIsRenewedWholeNextRound(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	failing := false
	rig.store.failPage = func(segment.PlanKey, int) bool { return failing }
	for rig.total.Renewals[observability.SegmentRenewalRenewed] == 0 {
		rig.round(time.Minute, series(3))
	}
	before := rig.mark().Renewed[0]
	for rig.mark().Renewed[0] == before || rig.total.Renewals[observability.SegmentRenewalFailed] == 0 {
		failing = rig.total.Renewals[observability.SegmentRenewalFailed] == 0
		facts := rig.round(time.Minute, series(3))
		if facts.Renewals[observability.SegmentRenewalFailed] == 1 && rig.mark().Renewed[0] != before {
			t.Fatalf("a failed renewal moved its page's time from %+v to %+v", before, rig.mark().Renewed[0])
		}
		if facts.Renewals[observability.SegmentRenewalRenewed] == 1 && rig.total.Renewals[observability.SegmentRenewalFailed] == 0 {
			t.Fatal("renewed before the failing round")
		}
	}
	if renewed := rig.mark().Renewed[0]; renewed.At != rig.wall {
		t.Fatalf("the round after the failure renewed at %d, want the wall clock %d", renewed.At, rig.wall)
	}
}

// A lifetime that grows, and one that shrinks, keep R: a page is due by the
// shorter of the lifetime its renewal gave and this round's, and a shorter
// one lowers the page's at once, so a member joining with it holds R too.
func TestRenewalsKeepRAsTheLifetimeGrowsAndShrinks(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	for round := 0; round < 12; round++ {
		rig.round(time.Minute, series(3))
	}
	rig.store.ttl = 30 * time.Minute
	for round := 0; round < 12; round++ {
		rig.round(time.Minute, series(3))
	}
	rig.store.ttl = 4 * time.Minute
	rig.round(time.Minute, series(4))
	if renewal := rig.mark().Renewed[0]; renewal.TTL != (4 * time.Minute).Milliseconds() {
		t.Fatalf("the round the lifetime shrank left the page's at %d, want lowered to %d", renewal.TTL, (4 * time.Minute).Milliseconds())
	}
	for round := 0; round < 12; round++ {
		rig.round(time.Minute, series(4))
	}
}

// A membership that outgrows its page doubles the count: every page of the
// new count carries its source's renewal - the earliest time, the shortest
// life - and R holds over the members it moved.
func TestARebucketingKeepsRForTheMembersItMoves(t *testing.T) {
	if testing.Short() {
		t.Skip("thousands of series a round")
	}
	rig := newRenewalRig(t, 10*time.Minute)
	for round := 0; round < 5; round++ {
		rig.round(time.Minute, series(3000))
	}
	rig.round(time.Minute, series(segment.PageFill+50))
	if pages := rig.mark().Pages; pages != 2 {
		t.Fatalf("%d members over %d pages, want the count doubled", segment.PageFill+50, pages)
	}
	for round := 0; round < 12; round++ {
		rig.round(time.Minute, series(segment.PageFill+50))
	}
}

// A page whose renewals keep failing is counted fallback_write once half
// its lifetime has passed since it was last renewed - its members still
// alive, as the third stage writes them whole there.
func TestAPageUnrenewedForHalfItsLifetimeIsCountedForTheFallback(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	rig.round(time.Minute, series(3))
	rig.store.failPage = func(segment.PlanKey, int) bool { return true }
	for rig.total.Renewals[observability.SegmentRenewalFallbackWrite] == 0 {
		rig.round(time.Minute, series(3))
		if rig.wall-rig.mark().Renewed[0].At > (10 * time.Minute).Milliseconds() {
			t.Fatal("no fallback within the page's lifetime")
		}
	}
	if since := rig.wall - rig.mark().Renewed[0].At; since < (5 * time.Minute).Milliseconds() {
		t.Fatalf("fallback counted %d ms after the page's renewal, before half its lifetime", since)
	}
	if short := rig.total.Renewals[observability.SegmentRenewalFallbackUnrenewable]; short != 0 {
		t.Fatalf("a one-minute Plan with ten minutes of life counted %d short-lived fallbacks", short)
	}
	// Every round settled, its position a step on: not stalled.
	if stalled := rig.total.Renewals[observability.SegmentRenewalFallbackStalled]; stalled != 0 {
		t.Fatalf("a Plan settling every round counted %d stalled fallbacks", stalled)
	}
}

// A Plan whose pages live no more than two of its intervals - two-hourly,
// its keys given three hours, as a Plan keeping one point is - goes half a
// lifetime between two rounds: every round its page is a fallback, by
// design and with every renewal answering, and is counted
// fallback_unrenewable, never fallback_write, which the bar reads.
func TestAPlanLivingNoMoreThanTwoIntervalsCountsItsFallbackApart(t *testing.T) {
	rig := newRenewalRig(t, 3*time.Hour)
	rig.due.ScheduleSpec.EvaluationIntervalSeconds = 7200
	rig.round(2*time.Hour, series(3))
	for round := 1; round <= 4; round++ {
		before := rig.total.Renewals[observability.SegmentRenewalFallbackUnrenewable]
		rig.round(2*time.Hour, series(3))
		if got := rig.total.Renewals[observability.SegmentRenewalFallbackUnrenewable] - before; got != 1 {
			t.Fatalf("round %d counted %d short-lived fallbacks, want its one page", round, got)
		}
	}
	if rig.total.Renewals[observability.SegmentRenewalFallbackWrite] != 0 || rig.total.Renewals[observability.SegmentRenewalFailed] != 0 {
		t.Fatalf("renewals %v, want no fallback_write and none failed", rig.total.Renewals)
	}
}

// A Query Group paused for 0.6 of a lifetime catches up a Slot every few
// seconds of wall clock while its evaluation time advances a minute a Slot:
// renewals judge by the wall clock the keys expire by, so the first round
// after the pause renews and the members stay found. By evaluation time the
// pages would not look due, and the keys would run out.
func TestAQueryGroupCatchingUpAfterAPauseKeepsItsMembersFound(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	for round := 0; round < 5; round++ {
		rig.round(time.Minute, series(3))
	}
	renewed := rig.total.Renewals[observability.SegmentRenewalRenewed]
	rig.round(6*time.Minute, series(3))
	if rig.total.Renewals[observability.SegmentRenewalRenewed] != renewed+1 {
		t.Fatalf("the round after a pause of 0.6 of a lifetime renewed %v, want its page", rig.total.Renewals)
	}
	for round := 0; round < 30; round++ {
		rig.round(5*time.Second, series(3))
	}
}

// A round whose joins the pool does not take joins nobody: they are written
// as today and join a later round, counted book_refused.
func TestARoundWhoseJoinsThePoolRefusesJoinsNobody(t *testing.T) {
	store := newMemorySegments()
	request := shadowRequest(execution.OperationNormal, 1260)
	round := newSegmentShadowRound(request, shadowSlot(t, store, request, shadowDue))
	for index := 0; index < 3; index++ {
		written := steady(t, shadowSeries(index), 1200)
		round.observe(shadowDue, segment.FamilyReal, written.view, written.write, written.class, false)
	}
	asked := 0
	facts, folded := foldSegmentShadowRound(round, []execution.DuePlan{shadowDue}, nil, func(joins int) bool {
		asked = joins
		return false
	})
	if asked != 3 || facts.Skipped[observability.SegmentShadowSkippedBookRefused] != 1 || member(facts, observability.SegmentShadowJoined) != 0 {
		t.Fatalf("asked %d, facts %+v: want the three joins asked for, refused, none joined", asked, facts)
	}
	if len(folded) != 1 || folded[0].state.book.Len() != 0 {
		t.Fatalf("the book holds %d after refused joins", folded[0].state.book.Len())
	}
}

// The joins a Slot's round takes in are reserved as its books are: against
// the share and the pool, under the State phase, held to the Slot's end.
func TestTheJoinsARoundTakesInAreReservedAsItsBooksAre(t *testing.T) {
	for _, room := range []uint64{1 << 30, 10*segment.BookMemberBytes - 1} {
		coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxSeries: 1000, MaxRetainedBytes: room}}
		stream := &streamedExecution{coordinator: coordinator, request: shadowRequest(execution.OperationNormal, 1260), began: true}
		admit := coordinator.segmentJoinAdmission(withRetainedOwner(context.Background(), stream))
		taken := admit(10)
		want := uint64(10 * segment.BookMemberBytes)
		if room > want {
			if !taken || stream.retainedByPhase[retainPhaseState] != want || coordinator.reservations.retainedBytes != want {
				t.Fatalf("with room: taken %v, State phase %d, pool %d; want %d reserved", taken,
					stream.retainedByPhase[retainPhaseState], coordinator.reservations.retainedBytes, want)
			}
			continue
		}
		if taken || coordinator.reservations.retainedBytes != 0 {
			t.Fatalf("without room: taken %v, pool %d", taken, coordinator.reservations.retainedBytes)
		}
	}
	if admit := (&SlotExecutionCoordinator{}).segmentJoinAdmission(context.Background()); admit != nil {
		t.Fatal("a Slot holding nothing reserves its joins")
	}
}

// A new page is made staggered, its renewal up to a quarter lifetime earlier.
// A Plan run hourly whose keys live two hours and a little - under four
// intervals, past two - can find its new page half its lifetime unrenewed the
// round after it was made, no round between to renew it: counted
// fallback_unrenewable, not fallback_write.
func TestANewPageNoRoundCouldRenewBeforeHalfItsLifetimeIsCountedUnrenewable(t *testing.T) {
	const ttl = 7300 * time.Second
	rig := newRenewalRig(t, ttl)
	rig.due.ScheduleSpec.EvaluationIntervalSeconds = 3600
	rig.round(time.Hour, series(3))
	// The stagger is the page's: the hash of its Plan and number.
	stagger := time.Duration(rig.wall-rig.mark().Renewed[0].At) * time.Millisecond
	if need := ttl/2 - time.Hour; stagger < need {
		t.Fatalf("the new page was made %v earlier, short of the %v its next round needs to be past half its lifetime", stagger, need)
	}
	unrenewable, written := rig.total.Renewals[observability.SegmentRenewalFallbackUnrenewable],
		rig.total.Renewals[observability.SegmentRenewalFallbackWrite]
	rig.round(time.Hour, series(3))
	unrenewable = rig.total.Renewals[observability.SegmentRenewalFallbackUnrenewable] - unrenewable
	written = rig.total.Renewals[observability.SegmentRenewalFallbackWrite] - written
	if unrenewable != 1 || written != 0 {
		t.Fatalf("the round after the page was made counted %d unrenewable and %d fallback_write, want the one page unrenewable", unrenewable,
			written)
	}
}

// A Plan whose rounds stopped - not active for a while, no round settled -
// and came back with its pages past half their lifetime is counted
// fallback_stalled, not fallback_write: the Slot's evaluation time is past
// the round its mark was written in by more than its interval. Six minutes
// of a ten-minute lifetime is past the interval and the quarter lifetime,
// and past half of it.
func TestAPlanThatStoppedFallsBackStalledNotWrite(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	rig.round(time.Minute, series(3))
	rig.round(time.Minute, series(3))
	stalled, written := rig.total.Renewals[observability.SegmentRenewalFallbackStalled], rig.total.Renewals[observability.SegmentRenewalFallbackWrite]
	rig.store.at += 5 * 60
	rig.round(6*time.Minute, series(3))
	stalled = rig.total.Renewals[observability.SegmentRenewalFallbackStalled] - stalled
	written = rig.total.Renewals[observability.SegmentRenewalFallbackWrite] - written
	if stalled != 1 || written != 0 {
		t.Fatalf("the round after six minutes without one counted %d stalled and %d fallback_write, want the page stalled", stalled, written)
	}
}

// A Plan that stopped and is caught up by replays - its first replay's
// position a step past its mark, as if nothing stopped - is counted
// fallback_stalled too: the round is a replay, late for the time it was due.
func TestAReplayAfterAStopFallsBackStalledNotWrite(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	rig.round(time.Minute, series(3))
	rig.round(time.Minute, series(3))
	stalled, written := rig.total.Renewals[observability.SegmentRenewalFallbackStalled], rig.total.Renewals[observability.SegmentRenewalFallbackWrite]
	rig.operation = execution.OperationReplay
	rig.round(6*time.Minute, series(3))
	stalled = rig.total.Renewals[observability.SegmentRenewalFallbackStalled] - stalled
	written = rig.total.Renewals[observability.SegmentRenewalFallbackWrite] - written
	if stalled != 1 || written != 0 {
		t.Fatalf("the replay six minutes on counted %d stalled and %d fallback_write, want the page stalled", stalled, written)
	}
}

// A Plan none of whose members is present the round it comes back - its
// position where its mark left it - shows its stop by the Slot's
// evaluation time: its page is counted fallback_stalled, not fallback_write.
func TestAPlanThatStoppedWithNoMemberBackFallsBackStalledNotWrite(t *testing.T) {
	rig := newRenewalRig(t, 10*time.Minute)
	rig.round(time.Minute, series(3))
	rig.round(time.Minute, series(3))
	stalled, written := rig.total.Renewals[observability.SegmentRenewalFallbackStalled], rig.total.Renewals[observability.SegmentRenewalFallbackWrite]
	rig.store.at += 5 * 60
	rig.round(6*time.Minute, nil)
	stalled = rig.total.Renewals[observability.SegmentRenewalFallbackStalled] - stalled
	written = rig.total.Renewals[observability.SegmentRenewalFallbackWrite] - written
	if stalled == 0 || written != 0 {
		t.Fatalf("the round six minutes on with no member back counted %d stalled and %d fallback_write, want its page stalled", stalled,
			written)
	}
}
