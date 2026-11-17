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
}

func newRenewalRig(t *testing.T, ttl time.Duration) *renewalRig {
	t.Helper()
	rig := &renewalRig{t: t, store: newMemorySegments(), wall: time.Unix(1_800_000_000, 0).UnixMilli(),
		key: segment.PlanKey{Plan: shadowDue.Identity, Generation: "g"}}
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
	facts := runShadow(rig.t, rig.store, rounds, false)
	for result, count := range facts.Renewals {
		rig.total.Renewal(result, count)
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
		round.observe(shadowDue, written.view, written.write, written.class, false)
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
