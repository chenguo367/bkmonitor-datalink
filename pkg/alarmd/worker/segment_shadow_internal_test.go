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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

var shadowDue = execution.DuePlan{Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "9"},
	StateGeneration: "g", ScheduleSpec: execution.ScheduleSpec{EvaluationIntervalSeconds: 60}}

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

// runShadow is one completed Slot of the Query Group "q": every series written,
// folded into the books, the Slot at a round after its writes' latest point.
func runShadow(shadow *segmentShadow, written []shadowRoundOf, withheld bool) observability.SegmentShadowFacts {
	return runShadowFor(shadow, shadowDue, written, withheld)
}

func runShadowFor(shadow *segmentShadow, due execution.DuePlan, written []shadowRoundOf, withheld bool) observability.SegmentShadowFacts {
	at := int64(0)
	for _, series := range written {
		at = max(at, series.write.Points[len(series.write.Points)-1].SourceTime+60)
	}
	round := newSegmentShadowRound(execution.SlotExecutionRequest{Operation: execution.OperationNormal,
		Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "q", EvaluationTime: execution.EvaluationTime(at)}}})
	for _, series := range written {
		round.observe(shadow, due, series.view, series.write, series.class, series.eventsSent)
	}
	notLanded := map[execution.PlanIdentity]struct{}{}
	if withheld {
		notLanded[due.Identity] = struct{}{}
	}
	return foldSegmentShadowRound(shadow, round, []execution.DuePlan{due}, notLanded)
}

func outcome(facts observability.SegmentShadowFacts, outcome observability.SegmentShadowOutcome) int64 {
	return facts.Outcomes[observability.SegmentShadowKey{Outcome: outcome, PeriodClass: observability.SegmentPeriodMinute}]
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
	shadow := &segmentShadow{}
	round := func(at int64) observability.SegmentShadowFacts {
		return runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), at), steady(t, shadowSeries(1), at), steady(t, shadowSeries(2), at)}, false)
	}
	// A series that decided otherwise is written and joins nothing.
	counting := steady(t, shadowSeries(9), 1200)
	counting.class = execution.SeriesClassCountingTrigger
	first := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200),
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
	if reading := shadow.reading(); reading != (SegmentShadowReading{Plans: 1, Open: 3,
		Bytes: 3*segmentShadowEntryBytes + segmentShadowPlanBytes}) {
		t.Fatalf("the books read %+v, want one Plan's three members", reading)
	}
}

// What the skip would not leave unwritten is an exception and leaves its
// page: events sent, a Level not FULL, the write-reuse class not
// decision_stable, a point off the grid. A steady series rejoins the round
// after.
func TestWhatTheSkipWouldWriteWholeIsAnExceptionAndLeavesItsPage(t *testing.T) {
	shadow := &segmentShadow{}
	all := func(at int64) []shadowRoundOf {
		written := make([]shadowRoundOf, 6)
		for index := range written {
			written[index] = steady(t, shadowSeries(index), at)
		}
		return written
	}
	runShadow(shadow, all(1200), false)
	if facts := runShadow(shadow, all(1260), false); outcome(facts, observability.SegmentShadowMatch) != 6 {
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
	facts := runShadow(shadow, written, false)
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
		if _, isMember := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g"}].book.Entry(shadowSeries(index)); isMember {
			t.Fatalf("exception %d is still a member", index)
		}
	}
	if facts := runShadow(shadow, all(1380), false); member(facts, observability.SegmentShadowJoined) != 4 || outcome(facts, observability.SegmentShadowMatch) != 2 {
		t.Fatalf("the round after %+v, want the four rejoined and two matches", facts)
	}
}

// The mark is the round's latest point, known once every series is in: a
// member the skip would have left whose last point falls short of it - a
// round catching up two points for one series and one for another - is an
// exception there, as the mark would read it normal where it has no point;
// the one at the mark is compared over both its points.
func TestAMemberShortOfTheRoundsMarkIsAnException(t *testing.T) {
	shadow := &segmentShadow{}
	runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	ahead := steady(t, shadowSeries(0), 1260)
	ahead.write.Points = []execution.StateHistoryPoint{shadowPoint(t, shadowSeries(0), 1260, "f"), shadowPoint(t, shadowSeries(0), 1320, "f")}
	ahead.write.Levels[0].LastProcessedEventTime, ahead.write.Levels[0].LastRealPointTime = 1320, 1320
	ahead.view = shadowView(t, shadowSeries(0), 1200)
	facts := runShadow(shadow, []shadowRoundOf{ahead, steady(t, shadowSeries(1), 1260)}, false)
	if outcome(facts, observability.SegmentShadowMatch) != 1 || outcome(facts, observability.SegmentShadowExceptionShortOfMark) != 1 || len(mismatches(facts)) != 0 {
		t.Fatalf("a round marked at 1320: %+v, want the one at it matched and the one short of it an exception", facts)
	}
	if _, isMember := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g"}].book.Entry(shadowSeries(1)); isMember {
		t.Fatal("the member short of the mark is still a member")
	}
}

// A write the read the skip would make does not reproduce is a mismatch, by
// each field it differs in: a skip would read the series otherwise than the
// round wrote it.
func TestAWriteTheMadeReadDoesNotReproduceIsAMismatchByField(t *testing.T) {
	shadow := &segmentShadow{}
	runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200), steady(t, shadowSeries(2), 1200)}, false)
	written := []shadowRoundOf{steady(t, shadowSeries(0), 1260), steady(t, shadowSeries(1), 1260), steady(t, shadowSeries(2), 1260)}
	written[0].write.Levels[0].LastRealPointTime = 1200 // the last real point did not move
	written[1].write.Points[0].Levels[0].DetectFingerprint = "f2"
	written[2].write.Points[0].RecordID = "legacy"
	facts := runShadow(shadow, written, false)
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
	shadow := &segmentShadow{}
	runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	if facts := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260)}, false); member(facts, observability.SegmentShadowDeparted) != 1 {
		t.Fatalf("a member absent: %+v, want it departed", facts)
	}
	book := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g"}].book
	if entry, _ := book.Entry(shadowSeries(1)); entry.Closed != 1200 {
		t.Fatalf("closed at %d, want the mark before, 1200", entry.Closed)
	}
	if reading := shadow.reading(); reading.Open != 1 || reading.Closed != 1 {
		t.Fatalf("the books read %+v, want one open member and one closed", reading)
	}
	if facts := runShadow(shadow, nil, false); member(facts, observability.SegmentShadowDeparted) != 1 {
		t.Fatalf("a round with no series: %+v, want the last open member departed", facts)
	}
	// 2 x 5 points x 60 s + 600 s past the later close, 1260.
	reachedAt := int64(1260 + 2*5*60 + segmentShadowHorizon + 60)
	if facts := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(7), reachedAt)}, false); member(facts, observability.SegmentShadowExpired) != 2 {
		t.Fatalf("a mark past both members' reach: %+v, want both expired", facts)
	}
}

// A retry that finds some of a Plan's series already applied starts the
// Plan's shadow over, its members counted; a Plan whose output did not land
// and a round at or before the last folded leave the book as it was.
func TestARetryStartsOverAndAWithheldOrReplayedRoundChangesNothing(t *testing.T) {
	shadow := &segmentShadow{}
	runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	if facts := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260)}, true); !facts.Empty() {
		t.Fatalf("a withheld Plan counted %+v", facts)
	}
	if facts := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false); !facts.Empty() {
		t.Fatalf("a replay of the round folded counted %+v", facts)
	}
	round := newSegmentShadowRound(execution.SlotExecutionRequest{Operation: execution.OperationRetry,
		Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "q"}}})
	round.retried(shadow, shadowDue)
	written := steady(t, shadowSeries(1), 1260)
	round.observe(shadow, shadowDue, written.view, written.write, written.class, false)
	if facts := foldSegmentShadowRound(shadow, round, []execution.DuePlan{shadowDue}, nil); member(facts, observability.SegmentShadowReset) != 2 {
		t.Fatalf("a retry: %+v, want the Plan reset with its two members", facts)
	}
	if facts := runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1320)}, false); member(facts, observability.SegmentShadowJoined) != 1 {
		t.Fatalf("after the reset: %+v, want the series to join anew", facts)
	}
	for _, operation := range []execution.Operation{execution.OperationSupplement, execution.OperationProbe} {
		if newSegmentShadowRound(execution.SlotExecutionRequest{Operation: operation}) != nil {
			t.Fatalf("a %s has a shadow round", operation)
		}
	}
	coordinator := &SlotExecutionCoordinator{}
	runShadow(&coordinator.segmentShadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200)}, false)
	if reading := SegmentShadowMemory(coordinator); reading.Open != 1 || reading.Plans != 1 {
		t.Fatalf("the books read %+v before the release, want the one member", reading)
	}
	ForgetQueryGroup(coordinator, "q")
	if _, held := coordinator.segmentShadow.byGroup["q"]; held || SegmentShadowMemory(coordinator) != (SegmentShadowReading{}) {
		t.Fatal("the Query Group's release kept its shadow")
	}
}

// A book no round will fold again goes: a Plan's older generation once a
// newer one of it is folded - an edit moves a Plan to a new generation, and
// its old book's members would stay open for good - and a Plan no longer
// folded for longer than its reach, by its own grid.
func TestABookNoRoundFoldsAgainIsDropped(t *testing.T) {
	shadow := &segmentShadow{}
	runShadow(shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	edited := shadowDue
	edited.StateGeneration = "g2"
	moved := func(round shadowRoundOf) shadowRoundOf {
		round.view.Identity.StateGeneration, round.write.Identity.StateGeneration = "g2", "g2"
		return round
	}
	runShadowFor(shadow, edited, []shadowRoundOf{moved(steady(t, shadowSeries(0), 1260))}, false)
	if _, kept := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g"}]; kept {
		t.Fatal("the old generation's book stayed after the new one was folded")
	}
	if reading := shadow.reading(); reading.Plans != 1 || reading.Open != 1 {
		t.Fatalf("books read %+v, want the new generation's one member", reading)
	}
	other := shadowDue
	other.Identity.StrategyID = "10"
	otherRound := func(at int64) shadowRoundOf {
		round := steady(t, shadowSeries(5), at)
		round.view.Identity.Plan, round.write.Identity.Plan = other.Identity, other.Identity
		return round
	}
	// The edited Plan no longer due: within its reach it stays; past it,
	// the next round of another Plan of the Query Group drops it.
	reach := int64(2*5*60 + segmentShadowHorizon)
	runShadowFor(shadow, other, []shadowRoundOf{otherRound(1260 + reach - 60)}, false)
	if _, kept := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g2"}]; !kept {
		t.Fatal("a book within its reach was dropped")
	}
	runShadowFor(shadow, other, []shadowRoundOf{otherRound(1260 + reach + 60)}, false)
	if _, kept := shadow.byGroup["q"][segmentShadowKey{plan: shadowDue.Identity, generation: "g2"}]; kept {
		t.Fatal("a book past its reach that no round folds stayed")
	}
}
