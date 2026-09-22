// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package absentalerts

import (
	"testing"
	"time"
)

var testNow = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func testBounds() Bounds {
	return Bounds{Grace: 10 * time.Minute, MaxSnapshotAge: 30 * time.Minute,
		MaxDifferenceRatio: 0.2, MinDifferenceForRatio: 5, MaxCloseStrategies: 4,
		MaxSnapshotShrinkRatio: 0.25, MinSnapshotForShrink: 10}
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

// filled is a snapshot of the given size that lists none of the departed
// strategies the tests use.
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

// closable is a strategy the catalog let go two hours ago, which this loop
// first found missing an hour ago under a different observation: everything
// a close needs except the round's own gates.
func closable(id string) (Key, map[Key]Departure, map[Key]Absence) {
	k := key(id)
	return k, map[Key]Departure{k: {Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}},
		map[Key]Absence{k: {Since: testNow.Add(-time.Hour), Observation: "observation-before"}}
}

func roundFor(k Key, departed map[Key]Departure, absences map[Key]Absence) Round {
	return Round{Departed: departed, WithOpenAlerts: set(k),
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
}

// The whole point, stated once: a strategy the catalog let go that still
// holds unrecovered alerts gets them closed.
func TestAStrategyTheCatalogLetGoHasItsAlertsClosed(t *testing.T) {
	k, departed, absences := closable("10")
	result := Compute(roundFor(k, departed, absences), testBounds())
	if len(result.Close) != 1 || result.Close[0].Key != k || result.Counts.Closed != 1 {
		t.Fatalf("the strategy this capability exists for was not closed: %+v", result)
	}
	if result.Close[0].Identity.BusinessID != 2 || result.Close[0].Identity.Revision != 7 {
		t.Fatalf("the close lost the identity only the catalog remembered: %+v", result.Close[0])
	}
}

// A departed strategy the source lists again is not deleted; it is a
// disagreement between the catalog and the source, and it is reported as
// one rather than skipped in silence.
func TestADepartedStrategyTheSnapshotListsAgainIsNotClosed(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.SnapshotStrategies[k] = struct{}{}
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.Returned != 1 || result.Counts.Candidates != 0 {
		t.Fatalf("a strategy the source lists again was closed or hidden: %+v", result)
	}
}

// The second condition is independent of the first: a strategy whose Plan
// the fleet is running is detecting, whatever any memory says. This is the
// shape that would otherwise take a strategy kept on its last good
// definition off the air.
func TestAStrategyStillRunningAPlanIsNotClosed(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.Published = set(k)
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.StillPublished != 1 {
		t.Fatalf("a strategy with a published Plan was closed: %+v", result)
	}
}

// Read and empty is a fact; not read is the absence of one. A strategy
// whose index could not be read must not become "it has no alerts", and
// must not become a close either.
func TestAStrategyWhoseIndexCouldNotBeReadIsNeitherClosedNorCleared(t *testing.T) {
	k, departed, absences := closable("10")
	round := Round{Departed: departed, WithOpenAlerts: map[Key]struct{}{}, Unreadable: set(k),
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.Candidates != 0 {
		t.Fatalf("an unread index produced a decision: %+v", result)
	}
	if result.Counts.UnreadableIndex != 1 {
		t.Fatalf("an unread index has to be a reading, not a silence: %+v", result.Counts)
	}
}

// An unread snapshot is not an empty one. Deciding on it would close every
// departed strategy's alerts on a round that learned nothing.
func TestAnUnreadSnapshotClosesNothingAndKeepsItsDenominators(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.SnapshotUsable, round.SnapshotStrategies, round.SnapshotObservation = false, nil, ""
	result := Compute(round, testBounds())
	if result.Refusal != RefusalSnapshotUnusable || len(result.Close) != 0 {
		t.Fatalf("an unread snapshot decided something: %+v", result)
	}
	if result.Counts.Departed != 1 || result.Counts.WithOpenAlerts != 1 {
		t.Fatalf("the refusal dropped its denominators, so zero closes cannot be told from a round that never ran: %+v", result.Counts)
	}
}

// An observed but empty snapshot is a source that lost its content. On a
// live deployment there are always strategies.
func TestAnEmptySnapshotClosesNothing(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.SnapshotStrategies = map[Key]struct{}{}
	if result := Compute(round, testBounds()); result.Refusal != RefusalSnapshotEmpty || len(result.Close) != 0 {
		t.Fatalf("an empty snapshot decided something: %+v", result)
	}
}

// A snapshot older than the bound has not seen what changed since.
func TestAStaleSnapshotClosesNothing(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.SnapshotAgeSeconds = int64((45 * time.Minute) / time.Second)
	if result := Compute(round, testBounds()); result.Refusal != RefusalSnapshotStale || len(result.Close) != 0 {
		t.Fatalf("a stale snapshot decided something: %+v", result)
	}
}

// The gate that works is on the input. A snapshot that lost a large share
// of its strategies is refused whatever the difference looks like - and the
// difference can look small, because it is the snapshot seen through "was
// let go by the catalog" and "still holds an alert".
func TestASnapshotThatLostAQuarterOfItsStrategiesRefusesTheRound(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
	round.SnapshotStrategies = filled(60)
	round.PreviousSnapshotStrategies = 100
	result := Compute(round, testBounds())
	if result.Refusal != RefusalSnapshotShrunk || len(result.Close) != 0 {
		t.Fatalf("a snapshot that lost 40 percent of its strategies was decided on: %+v", result)
	}
	if result.Counts.PreviousSnapshotStrategies != 100 || result.Counts.SnapshotStrategies != 60 {
		t.Fatalf("the refusal has to carry both sizes: %+v", result.Counts)
	}
}

// The same gate against the running catalog, for a leader whose first round
// has no previous snapshot but whose fleet is running Plans of strategies
// the snapshot no longer lists.
func TestASnapshotSmallerThanTheRunningCatalogRefusesTheRound(t *testing.T) {
	k, departed, absences := closable("10")
	round := roundFor(k, departed, absences)
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

// A deployment whose only unrecovered alerts are all on deleted strategies
// is the case this capability exists for, and nothing about its shape may
// be read as implausible.
func TestABacklogWhoseStrategiesAreAllGoneIsStillClosed(t *testing.T) {
	departed := map[Key]Departure{}
	absences := map[Key]Absence{}
	holding := map[Key]struct{}{}
	for i := 0; i < 3; i++ {
		k := key("gone-" + itoa(i))
		departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
		absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		holding[k] = struct{}{}
	}
	round := Round{Departed: departed, WithOpenAlerts: holding,
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
	result := Compute(round, testBounds())
	if result.Refusal != RefusalNone || result.Counts.Closed != 3 {
		t.Fatalf("the backlog this exists to clear was refused: %+v", result)
	}
}

// The grace is this loop's own, on top of the catalog's: a leader that has
// just been elected cannot close anything on its first round, whatever it
// inherited in the departure memory.
func TestACandidateIsNotClosedOnTheRoundItIsFirstSeen(t *testing.T) {
	k, departed, _ := closable("10")
	round := roundFor(k, departed, map[Key]Absence{k: {Since: testNow, Observation: "observation-now"}})
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.WithinGrace != 1 {
		t.Fatalf("a candidate first seen this round was closed: %+v", result)
	}
}

// Confirmation is two observations of the source. The reconciler reuses one
// observation across rounds, so waiting out the grace against a single
// observation confirms nothing.
func TestACandidateSeenOnlyUnderOneObservationIsNotClosed(t *testing.T) {
	k, departed, _ := closable("10")
	round := roundFor(k, departed, map[Key]Absence{k: {Since: testNow.Add(-time.Hour), Observation: "observation-now"}})
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.Unconfirmed != 1 {
		t.Fatalf("one observation confirmed an absence: %+v", result)
	}
}

// A close has to be addressed to a business. Nothing remembers this one, so
// the gap is named instead of guessed.
func TestACandidateWithNoRememberedBusinessIsNamedNotGuessed(t *testing.T) {
	k, _, absences := closable("10")
	round := roundFor(k, map[Key]Departure{k: {At: testNow.Add(-2 * time.Hour)}}, absences)
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.IdentityUnknown != 1 {
		t.Fatalf("a candidate with no remembered identity was closed anyway: %+v", result)
	}
}

// A legacy source publishes no snapshot revision and the close contract
// requires one. That is its own answer, not the same as an unknown business.
func TestACandidateWithoutASnapshotRevisionHasItsOwnAnswer(t *testing.T) {
	k, _, absences := closable("10")
	round := roundFor(k, map[Key]Departure{k: {Identity: Identity{BusinessID: 2}, At: testNow.Add(-2 * time.Hour)}}, absences)
	result := Compute(round, testBounds())
	if len(result.Close) != 0 || result.Counts.RevisionUnknown != 1 || result.Counts.IdentityUnknown != 0 {
		t.Fatalf("a missing revision was filed as a missing business: %+v", result)
	}
}

// A backlog is worked off over rounds, and what the bound left is reported
// as deferred rather than as an outcome: those strategies are still
// candidates.
func TestTheCloseBoundLeavesTheRestForALaterRound(t *testing.T) {
	departed := map[Key]Departure{}
	absences := map[Key]Absence{}
	holding := map[Key]struct{}{}
	for i := 0; i < 6; i++ {
		k := key("gone-" + itoa(i))
		departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
		absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		holding[k] = struct{}{}
	}
	round := Round{Departed: departed, WithOpenAlerts: holding,
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
	result := Compute(round, testBounds())
	if len(result.Close) != 4 || result.Counts.Deferred != 2 {
		t.Fatalf("the close bound was not applied or not reported: %+v", result)
	}
}

// Every candidate lands on exactly one outcome. Without this a candidate
// could fall through every branch and be reported nowhere, which reads as a
// healthy round.
func TestEveryCandidateLandsOnExactlyOneOutcome(t *testing.T) {
	departed := map[Key]Departure{}
	absences := map[Key]Absence{}
	holding := map[Key]struct{}{}
	published := map[Key]struct{}{}
	for _, shape := range []string{"closed", "published", "grace", "unconfirmed", "identity", "revision"} {
		k := key(shape)
		holding[k] = struct{}{}
		switch shape {
		case "closed":
			departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		case "published":
			departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
			published[k] = struct{}{}
		case "grace":
			departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow, Observation: "observation-now"}
		case "unconfirmed":
			departed[k] = Departure{Identity: Identity{BusinessID: 2, Revision: 7}, At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-now"}
		case "identity":
			departed[k] = Departure{At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		case "revision":
			departed[k] = Departure{Identity: Identity{BusinessID: 2}, At: testNow.Add(-2 * time.Hour)}
			absences[k] = Absence{Since: testNow.Add(-time.Hour), Observation: "observation-before"}
		}
	}
	round := Round{Departed: departed, WithOpenAlerts: holding, Published: published,
		SnapshotStrategies: filled(200), SnapshotUsable: true, SnapshotObservation: "observation-now",
		PreviousSnapshotStrategies: 200, FirstAbsent: absences, Now: testNow}
	counts := Compute(round, testBounds()).Counts
	filed := counts.Closed + counts.StillPublished + counts.WithinGrace + counts.Unconfirmed +
		counts.IdentityUnknown + counts.RevisionUnknown + counts.Deferred
	if filed != counts.Candidates || counts.Candidates != 6 {
		t.Fatalf("a candidate was not filed under any outcome: %+v", counts)
	}
	for _, count := range []int{counts.Closed, counts.StillPublished, counts.WithinGrace,
		counts.Unconfirmed, counts.IdentityUnknown, counts.RevisionUnknown} {
		if count != 1 {
			t.Fatalf("each shape should file once: %+v", counts)
		}
	}
}
