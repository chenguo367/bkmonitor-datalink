// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package absentalerts

import (
	"testing"
	"time"
)

var testNow = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func testBounds() Bounds {
	return Bounds{Grace: 10 * time.Minute, MaxSnapshotAge: 30 * time.Minute, MaxLinkHealthAge: 15 * time.Minute,
		MaxCloseStrategies: 4, MaxSnapshotShrinkRatio: 0.25, MinSnapshotForShrink: 10}
}

func key(id string) Key { return Key{TenantID: "system", StrategyID: id} }

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

// filled is a snapshot of the given size that lists none of the strategies
// the tests close.
func filled(count int) map[Key]struct{} {
	snapshot := make(map[Key]struct{}, count)
	for i := 0; i < count; i++ {
		snapshot[key("live-"+itoa(i))] = struct{}{}
	}
	return snapshot
}

func set(keys ...Key) map[Key]struct{} {
	result := make(map[Key]struct{}, len(keys))
	for _, k := range keys {
		result[k] = struct{}{}
	}
	return result
}

// ripe is the memory of a candidate this loop first found missing an hour
// ago under a different observation: everything a close needs except the
// round's own gates.
func ripe(keys ...Key) map[Key]Absence {
	absences := make(map[Key]Absence, len(keys))
	for _, k := range keys {
		absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
	}
	return absences
}

// roundFor is a healthy round: the link read and maintained a minute ago,
// a snapshot of 200 live strategies, and the roster listing the given
// strategies beside a few live ones.
func roundFor(roster map[Key]struct{}, absences map[Key]Absence) Round {
	for i := 0; i < 5; i++ {
		roster[key("live-"+itoa(i))] = struct{}{}
	}
	return Round{LinkRead: true, Roster: roster, RosterComplete: true, LinkLastSuccess: testNow.Add(-time.Minute),
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
}

// The whole point, stated once: a strategy the link holds an unrecovered
// alert for, and the snapshot no longer lists, gets its alerts closed - and
// it does not have to be one this process watched go.
func TestAStrategyTheLinkListsAndTheSnapshotDoesNotIsClosed(t *testing.T) {
	k := key("10")
	result := Compute(roundFor(set(k), ripe(k)), testBounds())
	if len(result.Close) != 1 || result.Close[0].Key != k || result.Counts.Closed != 1 {
		t.Fatalf("the strategy this capability exists for was not closed: %+v", result)
	}
	if result.Counts.Candidates != 1 || result.Counts.Roster != 6 {
		t.Fatalf("the live strategies on the roster are not candidates: %+v", result.Counts)
	}
	if result.Close[0].Identity != (Identity{}) {
		t.Fatalf("an identity nobody remembered was invented: %+v", result.Close[0])
	}
}

// When the catalog does remember the strategy, the close carries what it
// remembered.
func TestARememberedIdentityTravelsWithTheClose(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.Identities = map[Key]Identity{k: {BusinessID: 2, Revision: 7}}
	result := Compute(round, testBounds())
	if len(result.Close) != 1 || result.Close[0].Identity != (Identity{BusinessID: 2, Revision: 7}) {
		t.Fatalf("the remembered identity was lost: %+v", result.Close)
	}
}

// A strategy the snapshot lists is live, whatever the roster says.
func TestAStrategyTheSnapshotListsIsNotACandidate(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.SnapshotStrategies[k] = struct{}{}
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.Candidates != 0 {
		t.Fatalf("a strategy the source lists was closed: %+v", result)
	}
}

// The second condition is independent of the first: a strategy whose Plan
// the fleet is running is detecting, whatever the snapshot says.
func TestAStrategyStillRunningAPlanIsNotClosed(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.Published = set(k)
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.StillPublished != 1 {
		t.Fatalf("a strategy with a published Plan was closed: %+v", result)
	}
}

// Not read is not empty. The count is reported so that it is a reading.
func TestAStrategyTheLinkCouldNotReadIsReportedNotClosed(t *testing.T) {
	round := roundFor(set(), nil)
	round.RosterUnreadable = 3
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.RosterUnreadable != 3 {
		t.Fatalf("an unread set was hidden or closed: %+v", result)
	}
}

// No roster, no difference.
func TestAnUnreadRosterClosesNothing(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.LinkRead = false
	if result := Compute(round, testBounds()); result.Refusal != RefusalLinkUnavailable || len(result.Close) != 0 {
		t.Fatalf("a round without the roster decided something: %+v", result)
	}
}

// The link's own account of its maintenance gates the round: a latest
// discovery that failed, and one that never succeeded, both refuse it.
func TestALinkThatSaysItsMaintenanceFailedRefusesTheRound(t *testing.T) {
	k := key("10")
	for name, mutate := range map[string]func(*Round){
		"latest discovery failed": func(r *Round) { r.LinkError = "discovery_failed" },
		"never succeeded":         func(r *Round) { r.LinkLastSuccess = time.Time{} },
	} {
		round := roundFor(set(k), ripe(k))
		mutate(&round)
		if result := Compute(round, testBounds()); result.Refusal != RefusalLinkUnhealthy || len(result.Close) != 0 {
			t.Fatalf("%s: an unhealthy link was decided on: %+v", name, result)
		}
	}
}

// The health age bound, both sides of it: a discovery a minute inside the
// bound decides, one a minute past it does not.
func TestTheLinkHealthBoundIsABoundaryOnBothSides(t *testing.T) {
	k := key("10")
	bounds := testBounds()
	inside := roundFor(set(k), ripe(k))
	inside.LinkLastSuccess = testNow.Add(-bounds.MaxLinkHealthAge + time.Minute)
	if result := Compute(inside, bounds); result.Refusal != RefusalNone || result.Counts.Closed != 1 {
		t.Fatalf("a link inside its bound was refused: %+v", result)
	}
	past := roundFor(set(k), ripe(k))
	past.LinkLastSuccess = testNow.Add(-bounds.MaxLinkHealthAge - time.Minute)
	if result := Compute(past, bounds); result.Refusal != RefusalLinkUnhealthy {
		t.Fatalf("a link past its bound was decided on: %+v", result)
	}
}

// An incomplete walk is a smaller roster: it still decides on what it saw.
func TestAnIncompleteWalkStillClosesWhatItSaw(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.RosterComplete = false
	if result := Compute(round, testBounds()); result.Refusal != RefusalNone || result.Counts.Closed != 1 {
		t.Fatalf("an incomplete walk refused what it saw: %+v", result)
	}
}

// An unread snapshot is not an empty one. Deciding on it would close every
// strategy's alerts on a round that learned nothing.
func TestAnUnreadSnapshotClosesNothingAndKeepsItsDenominators(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.SnapshotUsable, round.SnapshotStrategies, round.SnapshotObservation = false, nil, ""
	result := Compute(round, testBounds())
	if result.Refusal != RefusalSnapshotUnusable || len(result.Close) != 0 {
		t.Fatalf("an unread snapshot decided something: %+v", result)
	}
	if result.Counts.Roster != 6 {
		t.Fatalf("the refusal dropped its denominators: %+v", result.Counts)
	}
}

func TestAnEmptySnapshotClosesNothing(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.SnapshotStrategies = map[Key]struct{}{}
	if result := Compute(round, testBounds()); result.Refusal != RefusalSnapshotEmpty || len(result.Close) != 0 {
		t.Fatalf("an empty snapshot decided something: %+v", result)
	}
}

// The snapshot age bound, both sides of it.
func TestTheSnapshotAgeBoundIsABoundaryOnBothSides(t *testing.T) {
	k := key("10")
	bounds := testBounds()
	inside := roundFor(set(k), ripe(k))
	inside.SnapshotAgeSeconds = int64((bounds.MaxSnapshotAge - time.Minute) / time.Second)
	if result := Compute(inside, bounds); result.Refusal != RefusalNone {
		t.Fatalf("a snapshot inside its bound was refused: %+v", result)
	}
	past := roundFor(set(k), ripe(k))
	past.SnapshotAgeSeconds = int64((bounds.MaxSnapshotAge + time.Minute) / time.Second)
	if result := Compute(past, bounds); result.Refusal != RefusalSnapshotStale || len(result.Close) != 0 {
		t.Fatalf("a stale snapshot decided something: %+v", result)
	}
}

// The gate that works is on the input. A snapshot that lost a large share of
// its strategies is refused, and a snapshot that lost a share just inside
// the ratio is not.
func TestTheShrinkGateIsABoundaryOnBothSides(t *testing.T) {
	k := key("10")
	past := roundFor(set(k), ripe(k))
	past.SnapshotStrategies = filled(74)
	past.PreviousSnapshotStrategies = 100
	result := Compute(past, testBounds())
	if result.Refusal != RefusalSnapshotShrunk || len(result.Close) != 0 {
		t.Fatalf("a snapshot that lost 26 percent of its strategies was decided on: %+v", result)
	}
	if result.Counts.PreviousSnapshotStrategies != 100 || result.Counts.SnapshotStrategies != 74 {
		t.Fatalf("the refusal has to carry both sizes: %+v", result.Counts)
	}
	inside := roundFor(set(k), ripe(k))
	inside.SnapshotStrategies = filled(76)
	inside.PreviousSnapshotStrategies = 100
	if result := Compute(inside, testBounds()); result.Refusal != RefusalNone {
		t.Fatalf("a snapshot that lost 24 percent was refused: %+v", result)
	}
}

// The same gate against the running catalog, for a leader whose first round
// has no previous snapshot.
func TestASnapshotSmallerThanTheRunningCatalogRefusesTheRound(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), ripe(k))
	round.SnapshotStrategies = filled(60)
	round.PreviousSnapshotStrategies = 0
	published := map[Key]struct{}{}
	for i := 0; i < 100; i++ {
		published[key("published-"+itoa(i))] = struct{}{}
	}
	round.Published = published
	if result := Compute(round, testBounds()); result.Refusal != RefusalSnapshotShrunk {
		t.Fatalf("a snapshot far smaller than the running catalog was decided on: %+v", result)
	}
}

// A deployment whose unrecovered alerts are mostly, or all, on deleted
// strategies is the case this capability exists for - and on its first run
// against a backlog, the difference can be a large share of everything.
// Nothing about that shape may be read as implausible: a hundred gone
// strategies beside a snapshot of two hundred is decided on, and the close
// bound works it off over rounds.
func TestABacklogLargerThanAnyRatioIsStillWorkedOff(t *testing.T) {
	roster := map[Key]struct{}{}
	gone := []Key{}
	for i := 0; i < 100; i++ {
		k := key("gone-" + itoa(i))
		roster[k] = struct{}{}
		gone = append(gone, k)
	}
	round := roundFor(roster, ripe(gone...))
	result := Compute(round, testBounds())
	if result.Refusal != RefusalNone || result.Counts.Closed != 4 || result.Counts.Deferred != 96 {
		t.Fatalf("the backlog this exists to clear was refused or not bounded: %+v", result)
	}
}

// The grace is this loop's own: a leader that has just been elected cannot
// close anything on its first round.
func TestACandidateIsNotClosedOnTheRoundItIsFirstSeen(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), map[Key]Absence{k: {Since: testNow, Observation: "observation-now"}})
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.WithinGrace != 1 {
		t.Fatalf("a candidate first seen this round was closed: %+v", result)
	}
}

// Confirmation is two observations of the source.
func TestACandidateSeenOnlyUnderOneObservationIsNotClosed(t *testing.T) {
	k := key("10")
	round := roundFor(set(k), map[Key]Absence{k: {Since: testNow.Add(-time.Hour), Observation: "observation-now"}})
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.Unconfirmed != 1 {
		t.Fatalf("one observation confirmed an absence: %+v", result)
	}
}

// The tracker, end to end over rounds: first seen, within grace, then closed
// once the grace has passed under a second observation.
func TestTheTrackerClosesOnlyAfterTheGraceAndASecondObservation(t *testing.T) {
	k := key("10")
	tracker := NewTracker(100)
	first := roundFor(set(k), nil)
	first.SnapshotObservation = "observation-one"
	if result := tracker.Round(first, testBounds()); result.Counts.WithinGrace != 1 || len(result.Close) != 0 {
		t.Fatalf("closed on first sight: %+v", result)
	}
	later := roundFor(set(k), nil)
	later.Now = testNow.Add(11 * time.Minute)
	later.SnapshotObservation = "observation-one"
	if result := tracker.Round(later, testBounds()); result.Counts.Unconfirmed != 1 {
		t.Fatalf("confirmed against the same observation: %+v", result)
	}
	later.SnapshotObservation = "observation-two"
	if result := tracker.Round(later, testBounds()); len(result.Close) != 1 {
		t.Fatalf("not closed after the grace and a second observation: %+v", result)
	}
}

// A refused round ages nothing: a run of refused rounds cannot mature a
// candidate into a close.
func TestARefusedRoundDoesNotStartOrAgeAClock(t *testing.T) {
	k := key("10")
	tracker := NewTracker(100)
	refused := roundFor(set(k), nil)
	refused.LinkError = "discovery_failed"
	tracker.Round(refused, testBounds())
	if tracker.Tracked() != 0 {
		t.Fatal("a refused round started a candidate's clock")
	}
}

// A strategy the link stops listing is forgotten, so it cannot come back
// later with an old clock.
func TestAStrategyTheLinkStopsListingIsForgotten(t *testing.T) {
	k := key("10")
	tracker := NewTracker(100)
	tracker.Round(roundFor(set(k), nil), testBounds())
	tracker.Round(roundFor(set(), nil), testBounds())
	if tracker.Tracked() != 0 {
		t.Fatal("a strategy the link no longer lists kept its clock")
	}
}

// Every candidate lands on exactly one outcome.
func TestEveryCandidateLandsOnExactlyOneOutcome(t *testing.T) {
	roster := map[Key]struct{}{}
	absences := map[Key]Absence{}
	published := map[Key]struct{}{}
	for _, shape := range []string{"closed", "published", "grace", "unconfirmed"} {
		k := key(shape)
		roster[k] = struct{}{}
		switch shape {
		case "closed":
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		case "published":
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
			published[k] = struct{}{}
		case "grace":
			absences[k] = Absence{Since: testNow, Observation: "observation-now"}
		case "unconfirmed":
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-now"}
		}
	}
	round := roundFor(roster, absences)
	round.Published = published
	counts := Compute(round, testBounds()).Counts
	filed := counts.Closed + counts.StillPublished + counts.WithinGrace + counts.Unconfirmed + counts.Deferred
	if filed != counts.Candidates || counts.Candidates != 4 {
		t.Fatalf("a candidate was not filed under any outcome: %+v", counts)
	}
	for _, count := range []int{counts.Closed, counts.StillPublished, counts.WithinGrace, counts.Unconfirmed} {
		if count != 1 {
			t.Fatalf("each shape should file once: %+v", counts)
		}
	}
}
