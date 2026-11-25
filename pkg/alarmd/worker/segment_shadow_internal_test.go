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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

var shadowDue = execution.DuePlan{Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "9"},
	StateGeneration: "g", StateApplyEpoch: 1, ScheduleSpec: execution.ScheduleSpec{EvaluationIntervalSeconds: 60}}

// memorySegments is a segment.Store keeping the Plans' marks and pages in
// memory as the State store keeps them in Redis: a round reads its book from
// what the round before wrote, and nothing else.
type memorySegments struct {
	marks map[segment.PlanKey][]byte
	pages map[segment.PlanKey]map[int][]byte
	at    int64
	// ttl is the life a read says the Plan's keys are written with; an
	// hour unless set.
	ttl time.Duration
	// expiry is, by Plan and series, when its State key expires on the
	// wall clock wall reads, in milliseconds: written by writeState, renewed
	// by RenewSegments while it has not expired. Nil, renewals touch nothing.
	expiry map[segment.PlanKey]map[execution.SeriesIdentityDigest]int64
	wall   func() int64
	// failPage, when set, is the pages whose renewal fails: none of their
	// keys renewed, the page answered not renewed.
	failPage func(segment.PlanKey, int) bool
	renewals int
	// departed is the members each Plan's last write renewed as departed;
	// departedGone, when set, answers every one of them already gone.
	departed     map[segment.PlanKey][]execution.SeriesIdentityDigest
	departedGone bool
	// written is each Plan's last write; refuse, when set, answers every
	// write with it, nothing written.
	written map[segment.PlanKey]segment.PlanWrite
	refuse  string
	// readFails fails every read whole; unread answers its Plans failed.
	readFails bool
	unread    map[segment.PlanKey]bool
	// noLife answers every read with no lifetime: no write can be made.
	noLife bool
}

func newMemorySegments() *memorySegments {
	return &memorySegments{marks: map[segment.PlanKey][]byte{}, pages: map[segment.PlanKey]map[int][]byte{}}
}

// writeState writes series' State keys, at the wall clock, for the store's
// life.
func (store *memorySegments) writeState(key segment.PlanKey, series []execution.SeriesIdentityDigest) {
	if store.expiry == nil {
		store.expiry = map[segment.PlanKey]map[execution.SeriesIdentityDigest]int64{}
	}
	if store.expiry[key] == nil {
		store.expiry[key] = map[execution.SeriesIdentityDigest]int64{}
	}
	for _, digest := range series {
		store.expiry[key][digest] = store.wall() + store.life().Milliseconds()
	}
}

func (store *memorySegments) life() time.Duration {
	if store.noLife {
		return 0
	}
	if store.ttl > 0 {
		return store.ttl
	}
	return time.Hour
}

func (store *memorySegments) RenewSegments(_ context.Context, request segment.RenewRequest) (segment.RenewResult, error) {
	var result segment.RenewResult
	for _, plan := range request.Plans {
		renewed := segment.PlanRenewed{Renewed: make([]bool, len(plan.Pages))}
		for position, page := range plan.Pages {
			store.renewals++
			if store.failPage != nil && store.failPage(plan.Key, page.Page) {
				continue
			}
			renewed.Renewed[position] = true
			// One script a page here, at a run time of its own.
			renewed.Elapsed = append(renewed.Elapsed, time.Duration(len(page.Members)+1)*time.Microsecond)
			if store.expiry == nil {
				continue
			}
			for _, digest := range page.Members {
				if expires, ok := store.expiry[plan.Key][digest]; ok && expires > store.wall() {
					store.expiry[plan.Key][digest] = store.wall() + store.life().Milliseconds()
					continue
				}
				renewed.Missing++
			}
		}
		result.Plans = append(result.Plans, renewed)
	}
	return result, nil
}

func (store *memorySegments) ReadSegments(_ context.Context, request segment.ReadRequest) (segment.ReadResult, error) {
	var result segment.ReadResult
	if store.readFails {
		return result, errors.New("dial tcp: connection refused")
	}
	for _, plan := range request.Plans {
		if store.unread[plan.Key] {
			result.Plans = append(result.Plans, segment.Stored{Key: plan.Key, Source: segment.SourceRedis, Reason: segment.ReadFailed})
			continue
		}
		stored := segment.Stored{Key: plan.Key, Mark: store.marks[plan.Key], Source: segment.SourceRedis,
			Reason: segment.ReadCold, TTL: store.life()}
		// Bytes that are not a mark are answered as read, as the State
		// store answers them: the worker's decoding says they are not.
		if stored.Mark != nil {
			mark, err := segment.DecodeMark(stored.Mark)
			if err != nil {
				result.Plans = append(result.Plans, stored)
				continue
			}
			for page := 0; page < mark.Pages; page++ {
				stored.Pages = append(stored.Pages, store.pages[plan.Key][page])
			}
		}
		result.Plans = append(result.Plans, stored)
	}
	return result, nil
}

func (store *memorySegments) WriteSegments(_ context.Context, request segment.WriteRequest) (segment.WriteResult, error) {
	var result segment.WriteResult
	for _, plan := range request.Plans {
		if store.written == nil {
			store.written = map[segment.PlanKey]segment.PlanWrite{}
		}
		store.written[plan.Key] = plan
		if store.refuse != "" {
			result.Plans, result.DepartedGone = append(result.Plans, store.refuse), append(result.DepartedGone, 0)
			continue
		}
		if store.pages[plan.Key] == nil {
			store.pages[plan.Key] = map[int][]byte{}
		}
		for page, raw := range plan.Pages {
			store.pages[plan.Key][page] = raw
		}
		for page := plan.PageCount; page < plan.PriorCount; page++ {
			delete(store.pages[plan.Key], page)
		}
		store.marks[plan.Key] = plan.Mark
		result.Plans = append(result.Plans, segment.WriteApplied)
		if store.departed == nil {
			store.departed = map[segment.PlanKey][]execution.SeriesIdentityDigest{}
		}
		store.departed[plan.Key] = plan.Departed
		gone := 0
		if store.departedGone {
			gone = len(plan.Departed)
		}
		result.DepartedGone = append(result.DepartedGone, gone)
	}
	return result, nil
}

// book is a Plan's book as stored.
func (store *memorySegments) book(t *testing.T, plan execution.PlanIdentity, generation execution.StateGeneration) *segment.Book {
	t.Helper()
	key := segment.PlanKey{Plan: plan, Generation: generation}
	stored, _ := store.ReadSegments(context.Background(), segment.ReadRequest{Plans: []segment.ReadPlan{{Key: key}}})
	book, mark, _, err := stored.Plans[0].Book()
	if err != nil {
		t.Fatal(err)
	}
	// The mark names every page's size as stored: a cold read cuts its
	// MGETs by them.
	for page, raw := range stored.Plans[0].Pages {
		if mark.Sizes[page] != len(raw) {
			t.Fatalf("the mark says page %d is %d bytes, it is %d", page, mark.Sizes[page], len(raw))
		}
	}
	return book
}

// reading is the Plans stored and their open and closed members.
func (store *memorySegments) reading(t *testing.T) (plans, open, closed int) {
	t.Helper()
	for key := range store.marks {
		store.book(t, key.Plan, key.Generation).Each(func(entry segment.Entry) {
			if entry.Open() {
				open++
			} else {
				closed++
			}
		})
		plans++
	}
	return plans, open, closed
}

func shadowContract(at int64) execution.FrozenExecutionContractRef {
	return execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "q", EvaluationTime: execution.EvaluationTime(at)},
		SnapshotRevision: "snapshot-v1", QueryRevision: "query-v1", ScheduleRevision: "schedule-v1",
		ScheduleSegmentStart: 60, DuePlanSetDigest: "due-set-v1"}
}

func shadowRequest(operation execution.Operation, at int64) execution.SlotExecutionRequest {
	return execution.SlotExecutionRequest{Operation: operation, Contract: shadowContract(at)}
}

// shadowSlot is a Slot's segments read from store, the Plan due with a
// five-point window.
func shadowSlot(t *testing.T, store *memorySegments, request execution.SlotExecutionRequest, due execution.DuePlan) *segmentSlot {
	t.Helper()
	slot := loadSegmentSlot(context.Background(), store, request, []segmentDue{{due: due, retention: 5,
		lifetime: segment.Lifetime{Interval: time.Minute}}})
	if slot == nil {
		t.Fatal("no segments read")
	}
	return slot
}

// foldShadow folds a round and writes what it leaves to store.
func foldShadow(t *testing.T, store *memorySegments, round *segmentShadowRound, request execution.SlotExecutionRequest,
	due execution.DuePlan, withheld map[execution.PlanIdentity]struct{}) observability.SegmentShadowFacts {
	t.Helper()
	clock := func() time.Time { return time.Unix(int64(request.Contract.Slot.EvaluationTime), 0) }
	if store.wall != nil {
		clock = func() time.Time { return time.UnixMilli(store.wall()) }
	}
	facts, folded := foldSegmentShadowRound(round, []execution.DuePlan{due}, withheld, nil)
	writes, _ := settleSegmentShadow(context.Background(), store, request, folded, &facts, clock)
	if _, err := store.WriteSegments(context.Background(), segment.WriteRequest{Contract: request.Contract, Plans: writes}); err != nil {
		t.Fatal(err)
	}
	return facts
}

func shadowSeries(index int) execution.SeriesIdentityDigest {
	sum := sha256.Sum256([]byte(fmt.Sprint("shadow-series-", index)))
	return execution.SeriesIdentityDigest(hex.EncodeToString(sum[:]))
}

func shadowPoint(t *testing.T, series execution.SeriesIdentityDigest, at int64, fingerprint string) execution.StateHistoryPoint {
	t.Helper()
	recordID, err := contract.DeriveRecordIDV2(string(series), at)
	if err != nil {
		t.Fatal(err)
	}
	return execution.StateHistoryPoint{RecordID: recordID, SourceTime: at,
		Levels: []execution.StateLevelFact{{LevelID: 1, DetectFingerprint: fingerprint, Result: execution.LevelFactNormal}}}
}

// shadowView is a series' record as the store reads it back after a round
// at last: its points every 60 s, its Level FULL, processed up to last.
func shadowView(t *testing.T, series execution.SeriesIdentityDigest, last int64) execution.RuntimeStateView {
	t.Helper()
	view := execution.RuntimeStateView{Identity: execution.StateKeyIdentity{Plan: shadowDue.Identity, StateGeneration: "g", SeriesIdentityDigest: series},
		Status: execution.StateFoundReady, PersistedMutationDigest: execution.MutationDigest(fmt.Sprint("m-", last)), LastProcessedEventTime: last,
		Levels: []execution.RuntimeLevelStateView{{LevelID: 1, LevelStateCompatibility: "c", HistoryCompleteness: execution.HistoryFull,
			WarmupRequirementRef: "w", LastProcessedEventTime: last, LastRealPointTime: last}}}
	for at := last - 240; at <= last; at += 60 {
		view.History = append(view.History, shadowPoint(t, series, at, "f"))
	}
	return view
}

// shadowWrite is the round at at's write of a steady series: one normal point,
// its Level processed and last real at at.
func shadowWrite(t *testing.T, series execution.SeriesIdentityDigest, at int64) execution.StateMutation {
	t.Helper()
	return execution.StateMutation{Identity: execution.StateKeyIdentity{Plan: shadowDue.Identity, StateGeneration: "g", SeriesIdentityDigest: series},
		MutationDigest: execution.MutationDigest(fmt.Sprint("m-", at)), RetentionPoints: 5,
		Points: []execution.StateHistoryPoint{shadowPoint(t, series, at, "f")},
		Levels: []execution.RuntimeLevelStateMutation{{LevelID: 1, LevelStateCompatibility: "c", HistoryCompleteness: execution.HistoryFull,
			WarmupRequirementRef: "w", LastProcessedEventTime: at, LastRealPointTime: at}}}
}

// shadowRoundOf is one written series of a round: the view it was decided
// from, the write, its class and whether it sent events.
type shadowRoundOf struct {
	view       execution.RuntimeStateView
	write      execution.StateMutation
	class      execution.SeriesClass
	eventsSent bool
}

func steady(t *testing.T, series execution.SeriesIdentityDigest, at int64) shadowRoundOf {
	return shadowRoundOf{view: shadowView(t, series, at-60), write: shadowWrite(t, series, at), class: execution.SeriesClassStableNormal}
}

// runShadow is one completed Slot of the Query Group "q": the Plan's book
// read from store, every series written, folded and written back, the Slot
// at a round after its writes' latest point.
func runShadow(t *testing.T, store *memorySegments, written []shadowRoundOf, withheld bool) observability.SegmentShadowFacts {
	return runShadowFor(t, store, shadowDue, written, withheld)
}

func runShadowFor(t *testing.T, store *memorySegments, due execution.DuePlan, written []shadowRoundOf, withheld bool) observability.SegmentShadowFacts {
	t.Helper()
	return runShadowForAs(t, store, due, written, withheld, execution.OperationNormal)
}

// runShadowForAs is runShadowFor under the operation given.
func runShadowForAs(t *testing.T, store *memorySegments, due execution.DuePlan, written []shadowRoundOf, withheld bool,
	operation execution.Operation) observability.SegmentShadowFacts {
	t.Helper()
	at := store.at + 60
	for _, series := range written {
		at = max(at, series.write.Points[len(series.write.Points)-1].SourceTime+60)
	}
	store.at = at
	request := shadowRequest(operation, at)
	round := newSegmentShadowRound(request, shadowSlot(t, store, request, due))
	for _, series := range written {
		round.observe(due, segment.FamilyReal, series.view, series.write, series.class, series.eventsSent)
	}
	notLanded := map[execution.PlanIdentity]struct{}{}
	if withheld {
		notLanded[due.Identity] = struct{}{}
	}
	return foldShadow(t, store, round, request, due, notLanded)
}

func outcome(facts observability.SegmentShadowFacts, outcome observability.SegmentShadowOutcome) int64 {
	return facts.Outcomes[observability.SegmentShadowKey{Outcome: outcome, PeriodClass: observability.SegmentPeriodMinute, Round: observability.SegmentRoundNormal}]
}

func member(facts observability.SegmentShadowFacts, event observability.SegmentShadowEvent) int64 {
	return facts.Members[observability.SegmentShadowMemberKey{Event: event, PeriodClass: observability.SegmentPeriodMinute}]
}

func exceptions(facts observability.SegmentShadowFacts) int64 {
	var count int64
	for key, n := range facts.Outcomes {
		if observability.SegmentShadowException(key.Outcome) {
			count += n
		}
	}
	return count
}

func mismatches(facts observability.SegmentShadowFacts) map[observability.SegmentShadowOutcome]int64 {
	found := map[observability.SegmentShadowOutcome]int64{}
	for key, count := range facts.Outcomes {
		if key.Outcome != observability.SegmentShadowMatch && !observability.SegmentShadowException(key.Outcome) {
			found[key.Outcome] += count
		}
	}
	return found
}

// A series written and decided steady and normal joins its page at the mark,
// and every round after the read the skip would make of it - its stored
// points, then a normal point to the mark - is the round's write: a match,
// and no page rewritten while the membership holds.
func TestASteadySeriesJoinsAtTheMarkAndMatchesEveryRoundAfter(t *testing.T) {
	shadow := newMemorySegments()
	round := func(at int64) observability.SegmentShadowFacts {
		return runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), at), steady(t, shadowSeries(1), at), steady(t, shadowSeries(2), at)}, false)
	}
	// A series that decided otherwise is written and joins nothing.
	counting := steady(t, shadowSeries(9), 1200)
	counting.class = execution.SeriesClassCountingTrigger
	first := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200),
		steady(t, shadowSeries(2), 1200), counting}, false)
	if member(first, observability.SegmentShadowJoined) != 3 || outcome(first, observability.SegmentShadowMatch) != 0 {
		t.Fatalf("first round %+v, want the three steady joined and nothing compared", first)
	}
	if first.PageWrites[observability.SegmentPeriodMinute] != 1 {
		t.Fatalf("first round rewrote %d pages, want the one the joins are on", first.PageWrites[observability.SegmentPeriodMinute])
	}
	for at := int64(1260); at <= 1440; at += 60 {
		facts := round(at)
		if outcome(facts, observability.SegmentShadowMatch) != 3 || len(mismatches(facts)) != 0 || exceptions(facts) != 0 {
			t.Fatalf("round at %d: %+v, want three matches", at, facts)
		}
		if facts.PageWrites[observability.SegmentPeriodMinute] != 0 {
			t.Fatalf("round at %d rewrote pages with the membership unchanged", at)
		}
	}
	if plans, open, closed := shadow.reading(t); plans != 1 || open != 3 || closed != 0 {
		t.Fatalf("the books stored %d Plans, %d open, %d closed, want one Plan's three members", plans, open, closed)
	}
}

// What the skip would not leave unwritten is an exception and leaves its
// page: events sent, a Level not FULL, the write-reuse class not
// decision_stable, a point off the grid. A steady series rejoins the round
// after.
func TestWhatTheSkipWouldWriteWholeIsAnExceptionAndLeavesItsPage(t *testing.T) {
	shadow := newMemorySegments()
	all := func(at int64) []shadowRoundOf {
		written := make([]shadowRoundOf, 6)
		for index := range written {
			written[index] = steady(t, shadowSeries(index), at)
		}
		return written
	}
	runShadow(t, shadow, all(1200), false)
	if facts := runShadow(t, shadow, all(1260), false); outcome(facts, observability.SegmentShadowMatch) != 6 {
		t.Fatalf("second round %+v, want six matches", facts)
	}
	written := all(1320)
	written[0].eventsSent = true
	// Stored and written alike, so only the rule that every Level be FULL
	// tells it from a steady one.
	written[1].view.Levels[0].HistoryCompleteness = execution.HistoryGapped
	written[1].write.Levels[0].HistoryCompleteness = execution.HistoryGapped
	written[2].write.Levels[0].WarmupRequirementRef = "another" // the stored Levels changed: not decision_stable
	// Its last point at the mark, one between off the grid: only the grid
	// rule makes it an exception rather than a mismatch.
	written[3].write.Points = []execution.StateHistoryPoint{shadowPoint(t, shadowSeries(3), 1290, "f"), shadowPoint(t, shadowSeries(3), 1320, "f")}
	facts := runShadow(t, shadow, written, false)
	if exceptions(facts) != 4 || outcome(facts, observability.SegmentShadowMatch) != 2 || len(mismatches(facts)) != 0 {
		t.Fatalf("round with four exceptions: %+v, want four exceptions and two matches", facts)
	}
	for _, reason := range []observability.SegmentShadowOutcome{observability.SegmentShadowExceptionEvents,
		observability.SegmentShadowExceptionLevel, observability.SegmentShadowExceptionUnsteady, observability.SegmentShadowExceptionOffGrid} {
		if outcome(facts, reason) != 1 {
			t.Errorf("%s = %d, want the one member of it", reason, outcome(facts, reason))
		}
	}
	for index := 0; index < 4; index++ {
		if _, isMember := shadow.book(t, shadowDue.Identity, "g").Entry(shadowSeries(index)); isMember {
			t.Fatalf("exception %d is still a member", index)
		}
	}
	if facts := runShadow(t, shadow, all(1380), false); member(facts, observability.SegmentShadowJoined) != 4 || outcome(facts, observability.SegmentShadowMatch) != 2 {
		t.Fatalf("the round after %+v, want the four rejoined and two matches", facts)
	}
}

// The mark is the round's latest point, known once every series is in: a
// member the skip would have left whose last point falls short of it - a
// round catching up two points for one series and one for another - is an
// exception there, as the mark would read it normal where it has no point;
// the one at the mark is compared over both its points.
func TestAMemberShortOfTheRoundsMarkIsAnException(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	ahead := steady(t, shadowSeries(0), 1260)
	ahead.write.Points = []execution.StateHistoryPoint{shadowPoint(t, shadowSeries(0), 1260, "f"), shadowPoint(t, shadowSeries(0), 1320, "f")}
	ahead.write.Levels[0].LastProcessedEventTime, ahead.write.Levels[0].LastRealPointTime = 1320, 1320
	ahead.view = shadowView(t, shadowSeries(0), 1200)
	facts := runShadow(t, shadow, []shadowRoundOf{ahead, steady(t, shadowSeries(1), 1260)}, false)
	if outcome(facts, observability.SegmentShadowMatch) != 1 || outcome(facts, observability.SegmentShadowExceptionShortOfMark) != 1 || len(mismatches(facts)) != 0 {
		t.Fatalf("a round marked at 1320: %+v, want the one at it matched and the one short of it an exception", facts)
	}
	if _, isMember := shadow.book(t, shadowDue.Identity, "g").Entry(shadowSeries(1)); isMember {
		t.Fatal("the member short of the mark is still a member")
	}
}

// A write the read the skip would make does not reproduce is a mismatch, by
// each field it differs in: a skip would read the series otherwise than the
// round wrote it.
func TestAWriteTheMadeReadDoesNotReproduceIsAMismatchByField(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200), steady(t, shadowSeries(2), 1200)}, false)
	written := []shadowRoundOf{steady(t, shadowSeries(0), 1260), steady(t, shadowSeries(1), 1260), steady(t, shadowSeries(2), 1260)}
	written[0].write.Levels[0].LastRealPointTime = 1200 // the last real point did not move
	written[1].write.Points[0].Levels[0].DetectFingerprint = "f2"
	written[2].write.Points[0].RecordID = "legacy"
	facts := runShadow(t, shadow, written, false)
	want := map[observability.SegmentShadowOutcome]int64{observability.SegmentShadowMismatchLastReal: 1,
		observability.SegmentShadowMismatchFacts: 1, observability.SegmentShadowMismatchRecordIDs: 1}
	if got := mismatches(facts); fmt.Sprint(got) != fmt.Sprint(want) || outcome(facts, observability.SegmentShadowMatch) != 0 {
		t.Fatalf("mismatches %v (match %d), want %v", got, outcome(facts, observability.SegmentShadowMatch), want)
	}
}

// A member absent from a round closes at the mark before - its State's last
// point, where it stopped being read as normal - and its entry leaves once
// past its window and its key's life; a round with no series at all closes
// every member.
func TestAnAbsentMemberClosesAtTheMarkBeforeAndExpiresPastItsReach(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260)}, false); member(facts, observability.SegmentShadowDeparted) != 1 {
		t.Fatalf("a member absent: %+v, want it departed", facts)
	}
	book := shadow.book(t, shadowDue.Identity, "g")
	if entry, _ := book.Entry(shadowSeries(1)); entry.Closed != 1200 {
		t.Fatalf("closed at %d, want the mark before, 1200", entry.Closed)
	}
	if _, open, closed := shadow.reading(t); open != 1 || closed != 1 {
		t.Fatalf("the books stored %d open and %d closed, want one of each", open, closed)
	}
	if facts := runShadow(t, shadow, nil, false); member(facts, observability.SegmentShadowDeparted) != 1 {
		t.Fatalf("a round with no series: %+v, want the last open member departed", facts)
	}
	// 2 x 5 points x 60 s + 600 s past the later close, 1260.
	reachedAt := int64(1260 + 2*5*60 + segmentShadowHorizon + 60)
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(7), reachedAt)}, false); member(facts, observability.SegmentShadowExpired) != 2 {
		t.Fatalf("a mark past both members' reach: %+v, want both expired", facts)
	}
}

// A retry that finds some of a Plan's series already applied starts the
// Plan's shadow over, its members counted; a Plan whose output did not land
// and a round at or before the last folded leave the book as it was.
func TestARetryStartsOverAndAWithheldOrReplayedRoundChangesNothing(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260)}, true); !facts.Empty() {
		t.Fatalf("a withheld Plan counted %+v", facts)
	}
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false); !facts.Empty() {
		t.Fatalf("a replay of the round folded counted %+v", facts)
	}
	shadow.at += 60
	request := shadowRequest(execution.OperationRetry, shadow.at)
	round := newSegmentShadowRound(request, shadowSlot(t, shadow, request, shadowDue))
	round.retried(shadowDue)
	written := steady(t, shadowSeries(1), 1260)
	round.observe(shadowDue, segment.FamilyReal, written.view, written.write, written.class, false)
	facts := foldShadow(t, shadow, round, request, shadowDue, nil)
	if member(facts, observability.SegmentShadowReset) != 2 {
		t.Fatalf("a retry: %+v, want the Plan reset with its two members", facts)
	}
	// It is a round, under the mode it ran: one, comparing nothing, no gate.
	planRound := round.plans[shadowKey(shadowDue, shadowDue.StateGeneration, segment.FamilyReal)]
	mode := round.skipMode(planRound)
	if len(facts.Rounds) != 1 || facts.Rounds[observability.SegmentRoundKey{Mode: mode, PeriodClass: planRound.class}] != 1 ||
		len(facts.NotSkipped) != 0 || outcome(facts, observability.SegmentShadowMatch) != 0 {
		t.Fatalf("a retry counted rounds %v, gates %v and %d matches, want one %s round and nothing compared", facts.Rounds,
			facts.NotSkipped, outcome(facts, observability.SegmentShadowMatch), mode)
	}
	if _, open, closed := shadow.reading(t); open != 0 || closed != 0 {
		t.Fatalf("the reset stored %d open and %d closed, want an empty book", open, closed)
	}
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1320)}, false); member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("after the reset: %+v, want the series to join anew", facts)
	}
	for _, operation := range []execution.Operation{execution.OperationSupplement, execution.OperationProbe} {
		if newSegmentShadowRound(execution.SlotExecutionRequest{Operation: operation}, &segmentSlot{}) != nil {
			t.Fatalf("a %s has a shadow round", operation)
		}
	}
	if newSegmentShadowRound(execution.SlotExecutionRequest{Operation: execution.OperationNormal}, nil) != nil {
		t.Fatal("a Slot that read no segments has a shadow round")
	}
}

// A Plan's book is its generation's: an edit moves the Plan to a new
// generation, whose round reads a book of its own, empty, and the old one's
// members are not carried into it - the old book's keys are left to their
// life. A stored book that does not read starts the Plan over, its members
// that did read counted reset.
func TestANewGenerationReadsItsOwnBookAndAnUnreadableOneStartsOver(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	edited := shadowDue
	edited.StateGeneration = "g2"
	moved := func(round shadowRoundOf) shadowRoundOf {
		round.view.Identity.StateGeneration, round.write.Identity.StateGeneration = "g2", "g2"
		return round
	}
	facts := runShadowFor(t, shadow, edited, []shadowRoundOf{moved(steady(t, shadowSeries(0), 1260))}, false)
	if outcome(facts, observability.SegmentShadowMatch) != 0 || member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("the new generation's first round: %+v, want its series joined and nothing compared", facts)
	}
	if book := shadow.book(t, edited.Identity, "g2"); book.Len() != 1 {
		t.Fatalf("the new generation's book holds %d, want its one member", book.Len())
	}
	if book := shadow.book(t, shadowDue.Identity, "g"); book.Len() != 2 {
		t.Fatalf("the old generation's book holds %d, want it left as it was", book.Len())
	}
	key := segment.PlanKey{Plan: edited.Identity, Generation: "g2"}
	shadow.pages[key][0] = []byte("not a page")
	facts = runShadowFor(t, shadow, edited, []shadowRoundOf{moved(steady(t, shadowSeries(0), 1320))}, false)
	if member(facts, observability.SegmentShadowReset) != 0 || member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("a book that does not read: %+v, want the Plan started over and its series joined anew", facts)
	}
	delete(shadow.pages[key], 0)
	facts = runShadowFor(t, shadow, edited, []shadowRoundOf{moved(steady(t, shadowSeries(0), 1380))}, false)
	if member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("a book with its page gone: %+v, want the Plan started over", facts)
	}
	if book := shadow.book(t, edited.Identity, "g2"); book.Len() != 1 {
		t.Fatalf("after starting over the book holds %d", book.Len())
	}
}

// A Plan's first round writes every page of its book, a page no member
// joined among them: the mark names its pages, and a page it names that was
// never written would not read.
func TestAPlansFirstRoundWritesEveryPageItsMarkNames(t *testing.T) {
	shadow := newMemorySegments()
	counting := steady(t, shadowSeries(9), 1200)
	counting.class = execution.SeriesClassCountingTrigger
	facts := runShadow(t, shadow, []shadowRoundOf{counting}, false)
	if member(facts, observability.SegmentShadowJoined) != 0 || facts.PageWrites[observability.SegmentPeriodMinute] != 1 {
		t.Fatalf("a first round with no steady series: %+v, want nothing joined and its one page written", facts)
	}
	if book := shadow.book(t, shadowDue.Identity, "g"); book.Len() != 0 {
		t.Fatalf("the stored book holds %d", book.Len())
	}
	if facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260)}, false); member(facts, observability.SegmentShadowJoined) != 1 ||
		member(facts, observability.SegmentShadowReset) != 0 {
		t.Fatalf("the round after: %+v, want its steady series joined on the page as read", facts)
	}
}

// segmentStateStore is a State port whose store keeps segments in memory;
// nothing else of it is called.
type segmentStateStore struct {
	execution.StateStore
	*memorySegments
}

// shadowObservations keeps what the coordinator observed.
type shadowObservations struct {
	observed []observability.Observation
}

func (observations *shadowObservations) Observe(_ context.Context, observation observability.Observation) {
	observations.observed = append(observations.observed, observation)
}

// A Slot reserves the books it decoded as it reserves the State it holds,
// for its whole run; the pool refusing them runs the Slot without its
// shadow, counted book_refused - and only there: no capacity rejection is
// observed, as nothing the Slot needs was refused.
func TestASlotReservesItsBooksAndRunsWithoutTheShadowWhenThePoolRefusesThem(t *testing.T) {
	store := newMemorySegments()
	var written []shadowRoundOf
	for index := 0; index < 40; index++ {
		written = append(written, steady(t, shadowSeries(index), 1200))
	}
	runShadow(t, store, written, false)
	books := segment.BookBytes(40)
	for _, room := range []uint64{1 << 30, books - 1} {
		observations := &shadowObservations{}
		coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: store}, Observer: observations},
			budget: ProvisionalBudget{MaxSeries: 1000, MaxRetainedBytes: room}}
		request := shadowRequest(execution.OperationNormal, 1260)
		stream := &streamedExecution{coordinator: coordinator, request: request, began: true}
		held, _ := stream.holdSegments(context.Background(), shadowSlot(t, store, request, shadowDue), "")
		var skipped, rejected int
		for _, observation := range observations.observed {
			if observation.SegmentShadow != nil {
				skipped += int(observation.SegmentShadow.Skipped[observability.SegmentShadowSkippedBookRefused])
			}
			if observation.Component == observability.ComponentResource {
				rejected++
			}
		}
		if room > books {
			if held == nil || stream.retainedByPhase[retainPhaseState] != books || coordinator.reservations.retainedBytes != books || skipped != 0 {
				t.Fatalf("with room: held %v, State phase %d, pool %d, skipped %d; want the books' %d reserved as State",
					held != nil, stream.retainedByPhase[retainPhaseState], coordinator.reservations.retainedBytes, skipped, books)
			}
			continue
		}
		if held != nil || skipped != 1 || rejected != 0 || coordinator.reservations.retainedBytes != 0 {
			t.Fatalf("without room: held %v, skipped %d, capacity rejections %d, pool %d; want no shadow, one book_refused, none",
				held != nil, skipped, rejected, coordinator.reservations.retainedBytes)
		}
	}
}
