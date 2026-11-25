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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A series is covered by the book of its Plan under its generation that
// holds it - one of the Plan's pieces - with the position of the mark that
// book was written under and the Plan's grid; a series no book holds, one of
// another generation, and a Plan whose book was not stored or did not
// decode cover nothing. A Slot's stream hands the store its segments through
// its context; a stream that read none, and no stream, hand none.
func TestASeriesIsCoveredByTheBookOfItsPieceTheSlotRead(t *testing.T) {
	plan := shadowDue.Identity
	member, stranger := shadowSeries(0), shadowSeries(1)
	piece := func(index int, digests ...execution.SeriesIdentityDigest) *segmentShadowPlan {
		var entries []segment.Entry
		for _, digest := range digests {
			entries = append(entries, segment.Entry{Digest: digest, Start: 60})
		}
		book, err := segment.NewBook(entries)
		if err != nil {
			t.Fatal(err)
		}
		return &segmentShadowPlan{key: segment.PlanKey{Plan: plan, Generation: "g1", Shard: execution.ShardRef{Index: index, Count: 2}},
			book: book, hadMark: true, position: 240, interval: 60, retention: 5}
	}
	first, second := piece(0, shadowSeries(2)), piece(1, member)
	slot := &segmentSlot{pieces: map[segmentPlanGeneration][]*segmentShadowPlan{{plan: plan, generation: "g1"}: {first, second}}}
	identity := execution.StateKeyIdentity{Plan: plan, StateGeneration: "g1", SeriesIdentityDigest: member}
	coverage, covered := slot.Coverage(identity)
	if !covered || coverage.Entry.Digest != member || coverage.Position != 240 || coverage.Interval != 60 || coverage.Retention != 5 {
		t.Fatalf("the member's coverage = (%+v, %v), want its piece's entry under the mark at 240", coverage, covered)
	}
	for name, other := range map[string]execution.StateKeyIdentity{
		"a series no book holds": {Plan: plan, StateGeneration: "g1", SeriesIdentityDigest: stranger},
		"another generation":     {Plan: plan, StateGeneration: "g0", SeriesIdentityDigest: member},
		"another Plan": {Plan: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "10"},
			StateGeneration: "g1", SeriesIdentityDigest: member},
	} {
		if _, covered := slot.Coverage(other); covered {
			t.Errorf("%s is covered", name)
		}
	}
	second.fresh = true
	if _, covered := slot.Coverage(identity); covered {
		t.Error("a book read fresh covers its member")
	}
	second.fresh, second.hadMark = false, false
	if _, covered := slot.Coverage(identity); covered {
		t.Error("a book with no stored mark covers its member")
	}
	second.hadMark = true
	if _, covered := (*segmentSlot)(nil).Coverage(identity); covered {
		t.Error("no segments cover a series")
	}

	stream := &streamedExecution{segments: slot}
	if _, covered := segment.SourceFrom(withRetainedOwner(context.Background(), stream)).Coverage(identity); !covered {
		t.Fatal("the Slot's context does not hand the store the segments it read")
	}
	if _, covered := segment.SourceFrom(withRetainedOwner(context.Background(), &streamedExecution{})).Coverage(identity); covered {
		t.Fatal("a stream that read no segments covers a series")
	}
	if segment.SourceFrom(withRetainedOwner(context.Background(), nil)) != nil {
		t.Fatal("no stream hands the store segments")
	}
}

// Every operation reads its Plans' segments, a supplement's and a probe's
// too: a member's State they read is extended by them as any other read is.
// Neither completes a round of the shadow.
func TestEveryOperationReadsItsPlansSegments(t *testing.T) {
	due := shadowDue
	due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
	coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: newMemorySegments()}}}
	for _, operation := range []execution.Operation{execution.OperationNormal, execution.OperationRetry, execution.OperationReplay,
		execution.OperationSupplement, execution.OperationProbe} {
		request := shadowRequest(operation, 120)
		slot, skipped, _ := coordinator.loadSegments(context.Background(), request, execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{due}})
		if slot == nil || skipped != "" || len(slot.plans) != 1 || len(slot.pieces) != 1 {
			t.Fatalf("a %s read segments (%+v, %q), want its Plan's", operation, slot, skipped)
		}
		round := newSegmentShadowRound(request, slot)
		if shadowed := operation != execution.OperationSupplement && operation != execution.OperationProbe; (round != nil) != shadowed {
			t.Fatalf("a %s has a shadow round: %v, want %v", operation, round != nil, shadowed)
		}
	}
}

// A Plan whose activation carries from another generation reads that
// generation's book beside its own: a series read under the generation it
// carries from is covered by it - materialized before the carry, 014 B3
// 3.2 - and that book is held for the Slot, outside the shadow's own books.
// A generation carried from with no stored book covers nothing.
func TestASlotReadsTheBookOfTheGenerationItsPlanCarriesFrom(t *testing.T) {
	store := newMemorySegments()
	if facts := runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, shadowSeries(0), 1320)}, false); member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("the previous generation's round %+v, want the series joined", facts)
	}
	due := shadowDue
	due.StateGeneration, due.StateCarry = "g1", &execution.StateCarry{From: shadowDue.StateGeneration}
	request := shadowRequest(execution.OperationNormal, store.at+60)
	slot := loadSegmentSlot(context.Background(), store, request, []segmentDue{{due: due, retention: 5,
		lifetime: segment.Lifetime{Interval: time.Minute}}})
	if slot == nil {
		t.Fatal("no segments read")
	}
	carried := execution.StateKeyIdentity{Plan: due.Identity, StateGeneration: shadowDue.StateGeneration, SeriesIdentityDigest: shadowSeries(0)}
	if coverage, covered := slot.Coverage(carried); !covered || coverage.Position <= 0 || coverage.Entry.Digest != shadowSeries(0) {
		t.Fatalf("the carried read's coverage = (%+v, %v), want the previous generation's entry under its mark", coverage, covered)
	}
	if len(slot.plans) != 1 || len(slot.carried) != 1 {
		t.Fatalf("%d own books and %d carried, want one of each", len(slot.plans), len(slot.carried))
	}
	for key := range slot.plans {
		if key.Generation != due.StateGeneration {
			t.Fatalf("the shadow's books hold generation %s, want only the Plan's own", key.Generation)
		}
	}
	var own uint64
	for _, state := range slot.plans {
		own += segment.BookBytes(state.book.Len())
	}
	if want := own + segment.BookBytes(slot.carried[0].book.Len()); slot.bookBytes() != want {
		t.Fatalf("the Slot holds %d bytes of books, want %d with the carried one's", slot.bookBytes(), want)
	}
	unread := shadowDue
	unread.StateGeneration, unread.StateCarry = "g1", &execution.StateCarry{From: "g-none"}
	none := loadSegmentSlot(context.Background(), store, request, []segmentDue{{due: unread, retention: 5,
		lifetime: segment.Lifetime{Interval: time.Minute}}})
	if none == nil || len(none.carried) != 0 {
		t.Fatalf("a generation with no stored book carried %+v", none)
	}
}

// A round skips only for a normal operation of a Slot that follows its word:
// a retry and a replay write whole, a supplement and a probe have no round;
// a Slot that does not follow the word skips under no operation.
func TestOnlyANormalOperationOfASlotFollowingItsWordSkips(t *testing.T) {
	for _, follows := range []bool{true, false} {
		slot := &segmentSlot{skip: follows}
		for _, operation := range []execution.Operation{execution.OperationNormal, execution.OperationRetry, execution.OperationReplay,
			execution.OperationSupplement, execution.OperationProbe} {
			round := newSegmentShadowRound(execution.SlotExecutionRequest{Operation: operation}, slot)
			skips := round != nil && round.skip
			if want := follows && operation == execution.OperationNormal; skips != want {
				t.Errorf("following %v, a %s skips %v, want %v", follows, operation, skips, want)
			}
		}
	}
}

// The fold of a round sets the bit on the Slot's segments, and a coverage
// asked after it is counted.
func TestACoverageAskedAfterTheFoldIsCounted(t *testing.T) {
	store := newMemorySegments()
	request := shadowRequest(execution.OperationNormal, 120)
	round := newSegmentShadowRound(request, shadowSlot(t, store, request, shadowDue))
	coordinator := &SlotExecutionCoordinator{ports: Ports{Now: time.Now}}
	coordinator.foldSegmentShadow(context.Background(), request, round, []execution.DuePlan{shadowDue}, nil)
	if !round.slot.folded.Load() {
		t.Fatal("the fold left the Slot's segments unmarked")
	}
	before := segmentCoverageAfterFold.Load()
	round.slot.Coverage(execution.StateKeyIdentity{Plan: shadowDue.Identity, StateGeneration: shadowDue.StateGeneration,
		SeriesIdentityDigest: shadowSeries(0)})
	if segmentCoverageAfterFold.Load() != before+1 {
		t.Fatal("a coverage asked after the fold is not counted")
	}
}

// A round that skips leaves a series only when every condition holds: an
// open member, its round not retried, no exception - events sent among
// them - and a made read matching the write. Each condition alone keeps
// the series written.
func TestTheRoundLeavesOnlyAnOpenMemberWithNoExceptionWhoseMadeReadMatches(t *testing.T) {
	member, departed := shadowSeries(0), shadowSeries(1)
	store := newMemorySegments()
	// Both join, both are compared a round, and the second departs.
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1320), steady(t, departed, 1320)}, false)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, store.at+60), steady(t, departed, store.at+60)}, false)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, store.at+60)}, false)
	request := shadowRequest(execution.OperationNormal, store.at+60)
	fresh := func() *segmentShadowRound {
		t.Helper()
		slot := shadowSlot(t, store, request, shadowDue)
		slot.skip = true
		return newSegmentShadowRound(request, slot)
	}
	// The round's point is the next on the grid after the mark.
	var at int64
	for _, state := range fresh().slot.plans {
		at = state.position + 60
	}
	series := steady(t, member, at)
	if !fresh().leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
		t.Fatal("an open member, steady on the grid, whose made read matches is not left")
	}
	if fresh().leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, true) {
		t.Error("a member that sent events is left")
	}
	mismatched := steady(t, member, at)
	mismatched.write.Points[0].RecordID += "-other"
	if fresh().leaves(shadowDue, segment.FamilyReal, mismatched.view, mismatched.write, mismatched.class, false) {
		t.Error("a member whose made read does not match is left")
	}
	retried := fresh()
	retried.retried(shadowDue)
	if retried.leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
		t.Error("a member of a retried round is left")
	}
	back := steady(t, departed, at)
	if fresh().leaves(shadowDue, segment.FamilyReal, back.view, back.write, back.class, false) {
		t.Error("a member back after it departed is left")
	}
	// The Plan's write decides on the same facts before admission (014 n8
	// (i)): the member left when it sent none, written when it sent events,
	// the decision noting nothing on the round either way.
	for sent, wantLeft := range map[bool]int{false: 1, true: 0} {
		round := fresh()
		apply := &slotApply{shadow: round}
		decided, writing, leaving := apply.decideLeaving(shadowDue, []execution.StateMutation{series.write},
			map[execution.StateKeyIdentity]shadowed{series.write.Identity: {view: series.view, class: series.class, eventsSent: sent}})
		if len(leaving) != wantLeft || len(writing) != 1-wantLeft || decided[series.write.Identity].leave != (wantLeft == 1) {
			t.Errorf("events sent %t: %d left, %d written, want %d left", sent, len(leaving), len(writing), wantLeft)
		}
		if len(round.plans) != 0 {
			t.Errorf("events sent %t: the decision started %d Plan rounds, want it to note nothing", sent, len(round.plans))
		}
	}
}

// A Plan's members decided together before admission build the round the
// shadow would start for the Plan once, not once a member (014 n8 (i)): the
// decision keeps nothing of it, and its building - a round's tallies and the
// sampled round's hash of the piece's name - is the cost the skip removes
// from the apply. Member for member, deciding twenty more of them together
// allocates less than deciding each alone by half a round's building at
// least, that building measured here.
func TestAPlansMembersDecidedTogetherBuildTheRoundItWouldStartOnce(t *testing.T) {
	const many = 21
	store := newMemorySegments()
	roundOf := func(at int64) []shadowRoundOf {
		written := make([]shadowRoundOf, many)
		for index := range written {
			written[index] = steady(t, shadowSeries(index), at)
		}
		return written
	}
	// They join, and are compared two rounds, as the case of a member left
	// above.
	runShadowFor(t, store, shadowDue, roundOf(1320), false)
	runShadowFor(t, store, shadowDue, roundOf(store.at+60), false)
	runShadowFor(t, store, shadowDue, roundOf(store.at+60), false)
	request := shadowRequest(execution.OperationNormal, store.at+60)
	slot := shadowSlot(t, store, request, shadowDue)
	slot.skip = true
	round := newSegmentShadowRound(request, slot)
	var at int64
	for _, state := range slot.plans {
		at = state.position + 60
	}
	writes := make([]execution.StateMutation, many)
	written := make(map[execution.StateKeyIdentity]shadowed, many)
	for index, series := range roundOf(at) {
		writes[index] = series.write
		written[series.write.Identity] = shadowed{view: series.view, class: series.class}
	}
	apply := &slotApply{shadow: round}
	if _, _, leaving := apply.decideLeaving(shadowDue, writes, written); len(leaving) != many {
		t.Fatalf("%d of %d steady members left: the case is not the one it is about", len(leaving), many)
	}
	if len(round.plans) != 0 {
		t.Fatalf("the decisions kept %d Plan rounds, want none: kept, a round is built once whatever the decision does", len(round.plans))
	}
	together := func(members int) float64 {
		return testing.AllocsPerRun(50, func() { apply.decideLeaving(shadowDue, writes[:members], written) })
	}
	alone := func(members int) float64 {
		return testing.AllocsPerRun(50, func() {
			for _, write := range writes[:members] {
				facts := written[write.Identity]
				round.decideLeave(nil, shadowDue, segment.FamilyReal, facts.view, write, facts.class, facts.eventsSent)
			}
		})
	}
	building := testing.AllocsPerRun(50, func() {
		round.peekPlanRound(shadowDue, segment.FamilyReal, writes[0].Identity, writes[0].RetentionPoints)
	})
	if building < 2 {
		t.Fatalf("building the Plan's round allocates %v times: the case is not the one it is about", building)
	}
	perTogether := (together(many) - together(1)) / (many - 1)
	perAlone := (alone(many) - alone(1)) / (many - 1)
	t.Logf("allocations a member: %.2f decided together, %.2f alone; the round's building %.0f", perTogether, perAlone, building)
	if perTogether > perAlone-building/2 {
		t.Fatalf("a member decided with its Plan's others allocates %.2f times, alone %.2f: the round, built in %.0f, is built again for each",
			perTogether, perAlone, building)
	}
}

// A member departing is renewed whole with the mark when the round before
// left members unwritten (its mark says so) and not otherwise; a departing
// member's key found gone is counted with the renewals' missing.
func TestADepartingMemberIsRenewedOnlyAfterARoundThatSkippedAndAGoneKeyIsCounted(t *testing.T) {
	member, other := shadowSeries(0), shadowSeries(1)
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	for _, skipped := range []bool{true, false} {
		store := newMemorySegments()
		// Both join, then are compared a round, on the grid.
		runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1320), steady(t, other, 1320)}, false)
		runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1380), steady(t, other, 1380)}, false)
		mark, err := segment.DecodeMark(store.marks[key])
		if err != nil {
			t.Fatal(err)
		}
		mark.Skipped = skipped
		if store.marks[key], err = segment.EncodeMark(mark); err != nil {
			t.Fatal(err)
		}
		store.departedGone = true
		request := shadowRequest(execution.OperationNormal, store.at+60)
		round := newSegmentShadowRound(request, shadowSlot(t, store, request, shadowDue))
		present := steady(t, other, mark.Position+60)
		round.observe(shadowDue, segment.FamilyReal, present.view, present.write, present.class, false)
		observer := &shadowObservations{}
		coordinator := &SlotExecutionCoordinator{ports: Ports{Now: time.Now, Observer: observer}}
		coordinator.foldSegmentShadow(context.Background(), request, round, []execution.DuePlan{shadowDue}, nil)
		var missing int64
		for _, observation := range observer.observed {
			if observation.SegmentShadow != nil {
				missing += observation.SegmentShadow.Renewals[observability.SegmentRenewalMissing]
			}
		}
		renewed := store.departed[key]
		if skipped && (len(renewed) != 1 || renewed[0] != member || missing != 1) {
			t.Errorf("after a round that skipped: renewed %v, missing %d; want the departing member, its gone key counted", renewed, missing)
		}
		if !skipped && (len(renewed) != 0 || missing != 0) {
			t.Errorf("after a round that wrote every State: renewed %v, missing %d; want none", renewed, missing)
		}
	}
}

// A piece's sampled round comes once a day when its interval divides a day,
// at the same time each day; about once a day, drifting, when it does not;
// every round when the interval is a day or more - a day, or 60 hours; and
// pieces are spread apart by their hash.
func TestAPiecesSampledRoundComesOnceADay(t *testing.T) {
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	hits := func(key segment.PlanKey, interval int64, days int64) []int64 {
		var at []int64
		for round := int64(1_700_000_000); round < 1_700_000_000+days*86400; round += interval {
			if sampleRound(key, round, interval) {
				at = append(at, round)
			}
		}
		return at
	}
	minute := hits(key, 60, 3)
	if len(minute) != 3 || minute[1]-minute[0] != 86400 || minute[2]-minute[1] != 86400 {
		t.Fatalf("a minute Plan's sampled rounds %v, want one a day, a day apart", minute)
	}
	if seven := hits(key, 420, 30); len(seven) < 29 || len(seven) > 31 {
		t.Fatalf("a seven-minute Plan has %d sampled rounds in thirty days, want about one a day", len(seven))
	}
	if daily := hits(key, 86400, 3); len(daily) != 3 {
		t.Fatalf("a daily Plan has %d sampled rounds in three days, want every round", len(daily))
	}
	// A Plan run every 60 hours, past a day: every round is its sampled
	// round, four in ten days.
	if longer := hits(key, 216000, 10); len(longer) != 4 {
		t.Fatalf("a 60-hour Plan has %d sampled rounds in ten days, want every round, four", len(longer))
	}
	other := key
	other.Shard = execution.ShardRef{Dimension: "bk_target_ip", Index: 1, Count: 2, MatcherDigest: string(shadowSeries(9))}
	if hits(other, 60, 1)[0] == minute[0] {
		t.Fatal("two pieces share their sampled round's time")
	}
}

// In the piece's sampled round no member is left: every one is written and
// compared. A member on a page gone half its lifetime unrenewed is written
// whole and taken off its page; one departing from such a page, in a round
// that skips, is renewed whole.
func TestTheSampledRoundAndAFallbackPageWriteTheirMembersWhole(t *testing.T) {
	member, departing := shadowSeries(0), shadowSeries(1)
	store := newMemorySegments()
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1320), steady(t, departing, 1320)}, false)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1380), steady(t, departing, 1380)}, false)
	request := shadowRequest(execution.OperationNormal, store.at+60)
	fresh := func() *segmentShadowRound {
		t.Helper()
		slot := shadowSlot(t, store, request, shadowDue)
		slot.skip = true
		return newSegmentShadowRound(request, slot)
	}
	series := steady(t, member, 1440)
	if !fresh().leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
		t.Fatal("the member is not left in an ordinary round")
	}
	control := fresh()
	control.planRound(shadowDue, segment.FamilyReal, series.write.Identity, series.write.RetentionPoints).sample = true
	if control.leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
		t.Error("a member is left in its piece's sampled round")
	}
	fallback := fresh()
	for _, state := range fallback.slot.plans {
		state.fallback = map[int]bool{0: true}
	}
	if fallback.leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
		t.Fatal("a member on a page gone half its lifetime unrenewed is left")
	}
	fallback.observe(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false)
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	coordinator := &SlotExecutionCoordinator{ports: Ports{Now: time.Now, Observer: &shadowObservations{}}}
	coordinator.foldSegmentShadow(context.Background(), request, fallback, []execution.DuePlan{shadowDue}, nil)
	if _, member := store.book(t, key.Plan, key.Generation).Entry(member); member {
		t.Error("the member written for its page's fallback stays on the page")
	}
	if renewed := store.departed[key]; len(renewed) != 1 || renewed[0] != departing {
		t.Errorf("departing from the fallback page renewed %v, want the departing member", renewed)
	}
}

// A Slot notes, as it reads its segments, the pages of each mark gone half
// their lifetime unrenewed by the worker's clock, and only those.
func TestASlotNotesThePagesGoneHalfTheirLifetimeUnrenewed(t *testing.T) {
	store := newMemorySegments()
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, shadowSeries(0), 1320)}, false)
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	due := shadowDue
	due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
	coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: store}, Now: time.Now}}
	for _, unrenewed := range []bool{false, true} {
		mark, err := segment.DecodeMark(store.marks[key])
		if err != nil {
			t.Fatal(err)
		}
		mark.Renewed[0] = segment.Renewal{At: time.Now().UnixMilli(), TTL: 3_600_000}
		if unrenewed {
			mark.Renewed[0].At -= 1_800_000
		}
		if store.marks[key], err = segment.EncodeMark(mark); err != nil {
			t.Fatal(err)
		}
		slot, _, _ := coordinator.loadSegments(context.Background(), shadowRequest(execution.OperationNormal, store.at+60),
			execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{due}})
		if slot == nil || len(slot.plans) != 1 {
			t.Fatal("no segments read")
		}
		for _, state := range slot.plans {
			if state.fallback[0] != unrenewed {
				t.Errorf("a page renewed %v ago is noted %v, want %v", map[bool]string{true: "half an hour", false: "now"}[unrenewed],
					state.fallback[0], unrenewed)
			}
		}
	}
}

// sampledRoundOf is the first evaluation time at or after from that is the
// Plan's piece's sampled round, at its interval.
func sampledRoundOf(t *testing.T, key segment.PlanKey, from, interval int64) int64 {
	t.Helper()
	for at := from; at < from+86400+interval; at += interval {
		if sampleRound(key, at, interval) {
			return at
		}
	}
	t.Fatal("no sampled round in a day")
	return 0
}

// A Plan's round writes what it did with its members' State writes and how
// many it left with its segments, for the object's reading: skip with the
// count left; sampled in its piece's sampled round, none left, its
// outcomes counted under round="sampled" and the round counted a sample;
// off when the Slot follows no word; operation for a retry.
func TestAPlansRoundSaysWhatItDidWithItsMembersWritesAndASampledRoundIsCounted(t *testing.T) {
	member := shadowSeries(0)
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	control := sampledRoundOf(t, key, 1_700_000_000/60*60, 60)
	cases := []struct {
		name      string
		operation execution.Operation
		follows   bool
		at        int64
		mode      string
		left      int
		round     string
	}{
		{"skip", execution.OperationNormal, true, control + 600, segment.SkipModeSkip, 1, observability.SegmentRoundNormal},
		{"sampled", execution.OperationNormal, true, control, segment.SkipModeSampled, 0, observability.SegmentRoundSampled},
		{"off", execution.OperationNormal, false, control + 600, segment.SkipModeOff, 0, observability.SegmentRoundNormal},
		{"operation", execution.OperationRetry, true, control + 600, segment.SkipModeOperation, 0, observability.SegmentRoundNormal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemorySegments()
			store.at = tc.at - 180
			runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, tc.at-120)}, false)
			runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, tc.at-60)}, false)
			request := shadowRequest(tc.operation, tc.at)
			slot := shadowSlot(t, store, request, shadowDue)
			slot.skip = tc.follows
			round := newSegmentShadowRound(request, slot)
			series := steady(t, member, tc.at)
			if !round.leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false) {
				round.observe(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false)
			}
			observer := &shadowObservations{}
			coordinator := &SlotExecutionCoordinator{ports: Ports{Now: time.Now, Observer: observer}}
			coordinator.foldSegmentShadow(context.Background(), request, round, []execution.DuePlan{shadowDue}, nil)
			write := store.written[key]
			if write.SkipMode != tc.mode || write.Skipped != tc.left {
				t.Fatalf("the round wrote skip mode %q with %d left, want %q with %d", write.SkipMode, write.Skipped, tc.mode, tc.left)
			}
			var samples, inRound, other int64
			for _, observation := range observer.observed {
				facts := observation.SegmentShadow
				if facts == nil {
					continue
				}
				samples += facts.Samples[observability.SegmentSampleKey{PeriodClass: observability.SegmentPeriodMinute}]
				for cell, count := range facts.Outcomes {
					if cell.Round == tc.round {
						inRound += count
					} else {
						other += count
					}
				}
			}
			if wantSamples := map[bool]int64{true: 1}[tc.round == observability.SegmentRoundSampled]; samples != wantSamples {
				t.Errorf("%d sampled rounds counted, want %d", samples, wantSamples)
			}
			if inRound != 1 || other != 0 {
				t.Errorf("outcomes %d in round %q and %d in the other, want the member's one in its round", inRound, tc.round, other)
			}
		})
	}
}

// A round that left a member fails its Slot when its segments' write is
// refused, and notes its Query Group: the next attempt leaves none, its
// round's mode write_failed, and a refused write then fails nothing. The
// note stays until the Query Group's Progress is committed or it is
// released; released, a round leaves the member again.
func TestARefusedSegmentWriteAfterARoundLeftAMemberIsNotedUntilTheQueryGroupGoes(t *testing.T) {
	member := shadowSeries(0)
	key := segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}
	store := newMemorySegments()
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1320)}, false)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1380)}, false)
	due := shadowDue
	due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
	request := shadowRequest(execution.OperationNormal, store.at+60)
	request.SegmentSkip = 7
	// The worker's clock where the rounds' renewals put it: the pages were
	// renewed at their rounds, none gone half its lifetime.
	clock := func() time.Time { return time.Unix(int64(request.Contract.Slot.EvaluationTime), 0) }
	coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: store}, Now: clock,
		Observer: &shadowObservations{}, InstalledControlEpoch: func() (uint64, bool) { return 7, true }}}
	ctx := context.Background()
	series := steady(t, member, 1440)
	attempt := func() (*segmentShadowRound, bool) {
		t.Helper()
		slot, _, _ := coordinator.loadSegments(ctx, request, execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{due}})
		if slot == nil {
			t.Fatal("no segments read")
		}
		round := newSegmentShadowRound(request, slot)
		left := round.leaves(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false)
		if !left {
			round.observe(shadowDue, segment.FamilyReal, series.view, series.write, series.class, false)
		}
		return round, left
	}
	store.refuse = segment.WriteStaleOwner
	round, left := attempt()
	failed, refused := coordinator.foldSegmentShadow(ctx, request, round, []execution.DuePlan{shadowDue}, nil)
	if !left || !failed {
		t.Fatalf("left %v, and a refused write after it did not fail the Slot", left)
	}
	// Refused by the fence, it is the owner's refusal (014 ⑩ F1).
	if !errors.Is(refused, ownership.ErrStaleFence) {
		t.Fatalf("a write the fence refused returned %v, want the owner's refusal", refused)
	}
	round, left = attempt()
	if left {
		t.Fatal("the attempt after a failed segment write left the member")
	}
	failed, refused = coordinator.foldSegmentShadow(ctx, request, round, []execution.DuePlan{shadowDue}, nil)
	if failed {
		t.Fatal("a round that left no member was failed by its refused write")
	}
	if !errors.Is(refused, ownership.ErrStaleFence) {
		t.Fatalf("a round that left no member, its write refused by the fence, returned %v", refused)
	}
	if mode := store.written[key].SkipMode; mode != segment.SkipModeWriteFailed {
		t.Fatalf("the attempt's round says %q, want write_failed", mode)
	}
	if !coordinator.segmentWriteFailures.failed(request.Contract.Slot.QueryGroup) {
		t.Fatal("a round that left none let the note go")
	}
	ForgetQueryGroup(coordinator, request.Contract.Slot.QueryGroup)
	if _, left := attempt(); !left {
		t.Fatal("after the Query Group was released, the round left no member")
	}
}
