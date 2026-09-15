// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"fmt"
	"sort"
	"testing"
)

// withheldFixture is a catalog whose dispositions are the given ones, so a
// test can say what a round found without building Query Groups it does not
// look at.
func withheldFixture(dispositions ...ObjectDisposition) Catalog {
	return Catalog{Dispositions: dispositions}
}

// The first round after a restart names every object that is not ACCEPTED.
//
// Nothing has been said yet, so nothing is a repeat: a leader that has just
// taken over and reports only changes would report nothing at all, and an
// operator arriving after a failover would find no line for any of the
// strategies that are not running.
func TestAFullCompileNamesEveryObjectThatWasNotAccepted(t *testing.T) {
	composition := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionAccepted},
		ObjectDisposition{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		ObjectDisposition{SourceID: "3", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
		ObjectDisposition{SourceID: "4", Scope: "LEVEL", LevelID: 2, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		ObjectDisposition{SourceID: "5", Scope: "PLAN", Disposition: DispositionAccepted},
	))

	report := ChangedWithheld(composition.WithheldObjects, nil, WithheldLineBudget)

	if len(report.Lines) != 3 {
		t.Fatalf("lines = %d (%+v), want one per object that was not accepted", len(report.Lines), report.Lines)
	}
	if report.Dropped != 0 {
		t.Fatalf("Dropped = %d, want nothing cut on a round of three", report.Dropped)
	}
	named := map[string]ObjectDisposition{}
	for _, line := range report.Lines {
		named[line.SourceID] = line
	}
	for _, sourceID := range []string{"2", "3", "4"} {
		if _, present := named[sourceID]; !present {
			t.Fatalf("strategy %s has no line; lines = %+v", sourceID, report.Lines)
		}
	}
	// Each line says what happened to that object and why, not a generic
	// "withheld": the reason is what an operator acts on, and the two objects
	// below are held back for reasons that need different people.
	if line := named["2"]; line.Disposition != DispositionConfigRejected || line.Reason != "NO_DATA_CONFIG_INVALID" {
		t.Fatalf("strategy 2 = %+v, want the disposition and reason it was withheld under", line)
	}
	if line := named["4"]; line.Scope != "LEVEL" || line.LevelID != 2 {
		t.Fatalf("strategy 4 = %+v, want the level the disposition was attached to", line)
	}
	if line := named["1"]; line.SourceID != "" {
		t.Fatalf("an accepted strategy was named: %+v", line)
	}
}

// The round after it says nothing.
//
// This is the whole reason the report is a difference. A deployment with two
// hundred rejected strategies refreshes every few seconds; reporting the
// steady state would write two hundred identical lines each time, and the one
// strategy that started failing this morning would be in the middle of the two
// hundredth copy of a list nobody can read.
func TestARoundWhereNothingChangedNamesNothing(t *testing.T) {
	dispositions := []ObjectDisposition{
		{SourceID: "1", Scope: "PLAN", Disposition: DispositionAccepted},
		{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "3", Scope: "LEVEL", LevelID: 1, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
	}
	first := ComposeCatalog(withheldFixture(dispositions...))
	if len(ChangedWithheld(first.WithheldObjects, nil, WithheldLineBudget).Lines) != 2 {
		t.Fatal("the first round did not name the two withheld objects; the next assertion would pass for the wrong reason")
	}

	// The audit the first round published is what the second round compares
	// against, so the second round is given the same list under the same
	// identities.
	second := ComposeCatalog(withheldFixture(dispositions...))
	report := ChangedWithheld(second.WithheldObjects, dispositions, WithheldLineBudget)

	if len(report.Lines) != 0 {
		t.Fatalf("lines = %+v, want none: nothing changed between the two rounds", report.Lines)
	}
	if report.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0", report.Dropped)
	}
}

// One object changing is one line, and it is that object's.
//
// A report that named every withheld object whenever any of them changed would
// be as unreadable as reporting the steady state, and would also make the line
// count useless as a signal that something moved.
func TestOneChangedDispositionIsOneLine(t *testing.T) {
	previous := []ObjectDisposition{
		{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "2", Scope: "PLAN", Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		{SourceID: "3", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
	}
	// Strategy 2 was refused for the first time last round and its last good
	// Plan is retained now: the disposition moved, the object did not.
	current := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		ObjectDisposition{SourceID: "2", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "ALGORITHM_NOT_MIGRATED"},
		ObjectDisposition{SourceID: "3", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
	))

	report := ChangedWithheld(current.WithheldObjects, previous, WithheldLineBudget)

	if len(report.Lines) != 1 {
		t.Fatalf("lines = %+v, want exactly the one object whose disposition changed", report.Lines)
	}
	if line := report.Lines[0]; line.SourceID != "2" || line.Disposition != DispositionStaleConfig {
		t.Fatalf("line = %+v, want strategy 2 under its new disposition", line)
	}
}

// A reason that changes under an unchanged disposition is also a change.
//
// CONFIG_REJECTED for a bad no-data roster and CONFIG_REJECTED for an invalid
// trigger are two different things to go and fix, and a report keyed on the
// disposition alone would show the first one forever.
func TestAChangedReasonUnderTheSameDispositionIsAChange(t *testing.T) {
	previous := []ObjectDisposition{
		{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
	}
	current := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "TRIGGER_CONFIG_MISSING"},
	))

	report := ChangedWithheld(current.WithheldObjects, previous, WithheldLineBudget)

	if len(report.Lines) != 1 || report.Lines[0].Reason != "TRIGGER_CONFIG_MISSING" {
		t.Fatalf("lines = %+v, want the object reported again under its new reason", report.Lines)
	}
}

// The same strategy withheld at the plan and at a level is two records.
//
// They are two facts, not one changing its mind, and folding them together
// would make a level refusal hide a plan refusal on the same strategy - or
// report a change every round as the two took turns.
func TestPlanAndLevelRecordsForOneStrategyAreSeparate(t *testing.T) {
	previous := []ObjectDisposition{
		{SourceID: "1", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
		{SourceID: "1", Scope: "LEVEL", LevelID: 1, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		{SourceID: "1", Scope: "LEVEL", LevelID: 2, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
	}
	current := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
		ObjectDisposition{SourceID: "1", Scope: "LEVEL", LevelID: 1, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		ObjectDisposition{SourceID: "1", Scope: "LEVEL", LevelID: 2, Disposition: DispositionConfigRejected, Reason: "TRIGGER_CONFIG_MISSING"},
	))

	report := ChangedWithheld(current.WithheldObjects, previous, WithheldLineBudget)

	if len(report.Lines) != 1 {
		t.Fatalf("lines = %+v, want only the level whose disposition changed", report.Lines)
	}
	if line := report.Lines[0]; line.Scope != "LEVEL" || line.LevelID != 2 {
		t.Fatalf("line = %+v, want level 2 of strategy 1", line)
	}
}

// An object that stopped being withheld is not a line.
//
// The question these lines answer is "what is being held back and why". A
// strategy that is running is not being held back, and reporting it here would
// put a line that needs no action next to the ones that do. The counts are
// where a reader sees the total come down.
func TestAnObjectThatIsAcceptedAgainIsNotALine(t *testing.T) {
	previous := []ObjectDisposition{
		{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"},
	}
	current := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionAccepted},
		ObjectDisposition{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"},
	))

	report := ChangedWithheld(current.WithheldObjects, previous, WithheldLineBudget)

	if len(report.Lines) != 0 {
		t.Fatalf("lines = %+v, want none: one object was accepted again and the other did not change", report.Lines)
	}
	// And the counts did move, which is where that reader looks.
	if count := composedWithheldTotal(current); count != 1 {
		t.Fatalf("withheld total = %d, want the one object still held back", count)
	}
}

// The cap cuts, counts what it cut, and cuts the same tail every time.
//
// A capped report that does not say how much it dropped reads as a complete
// one, and a report that drops a different arbitrary subset each round lets a
// strategy hide by never being in the first N.
func TestTheLineBudgetCutsTheSameTailAndSaysHowMuchItCut(t *testing.T) {
	// Built in an order that is not the sorted one. A fixture already in
	// identity order cannot tell a report that sorted from one that kept
	// whatever order the round produced, and the round's order is not fixed:
	// it follows the order the source returned the strategies in.
	var dispositions []ObjectDisposition
	for index := 49; index >= 0; index-- {
		dispositions = append(dispositions, ObjectDisposition{
			SourceID: fmt.Sprintf("%03d", index), Scope: "PLAN",
			Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID",
		})
	}
	composition := ComposeCatalog(withheldFixture(dispositions...))
	if first := composition.WithheldObjects[0].SourceID; first != "049" {
		t.Fatalf("fixture starts at %s; this test needs an input order that differs from the sorted one", first)
	}

	report := ChangedWithheld(composition.WithheldObjects, nil, 10)

	if len(report.Lines) != 10 {
		t.Fatalf("lines = %d, want the budget", len(report.Lines))
	}
	if report.Dropped != 40 {
		t.Fatalf("Dropped = %d, want the 40 that did not fit", report.Dropped)
	}
	// The first ten by identity, not the first ten the map happened to yield.
	for index, line := range report.Lines {
		if want := fmt.Sprintf("%03d", index); line.SourceID != want {
			t.Fatalf("line %d = %s, want %s: the cap must take a determined tail", index, line.SourceID, want)
		}
	}
	// And a round that read the same strategies in a different order cuts the
	// same tail. Two calls over one slice would agree without any ordering at
	// all, and the failure this guards against is exactly that the order
	// changed between rounds: a strategy that is never in the first ten of
	// whatever order arrived is a strategy that is never named.
	shuffled := append([]ObjectDisposition(nil), composition.WithheldObjects...)
	sort.Slice(shuffled, func(left, right int) bool {
		return shuffled[left].SourceID > shuffled[right].SourceID
	})
	again := ChangedWithheld(shuffled, nil, 10)
	for index := range report.Lines {
		if again.Lines[index] != report.Lines[index] {
			t.Fatalf("line %d differs when the same round is read in another order: %+v vs %+v",
				index, again.Lines[index], report.Lines[index])
		}
	}
}

// The cut is determined inside one strategy too.
//
// A strategy whose levels are all refused produces one record per level and
// often a plan record besides, so the budget can fall in the middle of a
// single strategy. Ordering on the strategy alone leaves those records in
// whatever order they arrived, and the round that names levels 1 and 4 is
// followed by the round that names 2 and 3 - which reads as four levels
// changing state when nothing changed at all.
func TestTheCutIsDeterminedAmongOneStrategysOwnRecords(t *testing.T) {
	var dispositions []ObjectDisposition
	for level := uint32(8); level >= 1; level-- {
		dispositions = append(dispositions, ObjectDisposition{
			SourceID: "1", Scope: "LEVEL", LevelID: level,
			Disposition: DispositionConfigRejected, Reason: "TRIGGER_CONFIG_MISSING",
		})
	}
	dispositions = append(dispositions, ObjectDisposition{
		SourceID: "1", Scope: "PLAN",
		Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID",
	})
	forward := ComposeCatalog(withheldFixture(dispositions...))
	reversed := make([]ObjectDisposition, len(dispositions))
	for index, record := range dispositions {
		reversed[len(dispositions)-1-index] = record
	}
	backward := ComposeCatalog(withheldFixture(reversed...))

	first := ChangedWithheld(forward.WithheldObjects, nil, 3)
	second := ChangedWithheld(backward.WithheldObjects, nil, 3)

	if len(first.Lines) != 3 || first.Dropped != 6 {
		t.Fatalf("report = %+v, want three lines and six cut", first)
	}
	for index := range first.Lines {
		if first.Lines[index] != second.Lines[index] {
			t.Fatalf("line %d differs between two orders of one strategy's records: %+v vs %+v; "+
				"the cut inside a strategy must not follow the order the round read them in",
				index, first.Lines[index], second.Lines[index])
		}
	}
	// And it is the three lowest levels, in level order: the identity orders
	// by scope before level, and LEVEL sorts ahead of PLAN, so a strategy
	// whose records overflow the budget shows its levels and loses its plan
	// record to the cut. That is a consequence of the ordering rather than a
	// judgement about which record matters more -- what the budget owes the
	// reader is that the same records are named every round, and the count of
	// what it dropped.
	for index, line := range first.Lines {
		if line.Scope != "LEVEL" || line.LevelID != uint32(index+1) {
			t.Fatalf("line %d = %+v, want level %d", index, line, index+1)
		}
	}
}

// A budget of zero or less does not cut.
//
// This is not a lenient default: it is what says the cap belongs to the caller
// that sets one. The production caller passes WithheldLineBudget and a test
// that wants no cap must not have to guess a number bigger than its fixture.
func TestAnUnsetLineBudgetDoesNotCut(t *testing.T) {
	composition := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"},
		ObjectDisposition{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"},
	))

	report := ChangedWithheld(composition.WithheldObjects, nil, 0)

	if len(report.Lines) != 2 || report.Dropped != 0 {
		t.Fatalf("report = %+v, want both lines and nothing cut", report)
	}
}

// The lines and the counts are two outlets of one pass over one list.
//
// If they were two passes they could disagree, and the disagreement would be
// invisible: the page would say 41 strategies are withheld and the log would
// name 40, with nothing saying which of the two is right. This holds them to
// the same total on a catalog that exercises every fold the count does -- an
// unnamed disposition, a level record, an accepted object.
func TestTheLinesAndTheCountsComeFromOnePass(t *testing.T) {
	composition := ComposeCatalog(withheldFixture(
		ObjectDisposition{SourceID: "1", Scope: "PLAN", Disposition: DispositionAccepted},
		ObjectDisposition{SourceID: "2", Scope: "PLAN", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		ObjectDisposition{SourceID: "3", Scope: "LEVEL", LevelID: 1, Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		ObjectDisposition{SourceID: "4", Scope: "PLAN", Disposition: Disposition("A_DISPOSITION_NOBODY_LISTED"), Reason: "PLAN_INVALID"},
		ObjectDisposition{SourceID: "5", Scope: "PLAN", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
	))

	counted := composedWithheldTotal(composition)
	if len(composition.WithheldObjects) != counted {
		t.Fatalf("WithheldObjects = %d, Withheld counts = %d: the page and the log disagree about how many "+
			"objects were withheld, which is what one pass over one list exists to prevent",
			len(composition.WithheldObjects), counted)
	}
	// And the objects carry the same folded disposition the counts do, so a
	// reader who greps the log for the disposition a count moved under finds
	// the line.
	var folded int
	for _, object := range composition.WithheldObjects {
		if object.Disposition == DispositionOther {
			folded++
		}
	}
	if folded != 1 {
		t.Fatalf("objects folded to other = %d, want the one disposition this build does not name; "+
			"the counts fold it and the lines must fold it the same way", folded)
	}
	// A full first round names all of them, which is what makes the two totals
	// comparable in production rather than only here.
	if lines := ChangedWithheld(composition.WithheldObjects, nil, WithheldLineBudget).Lines; len(lines) != counted {
		t.Fatalf("first-round lines = %d, want the %d the counts report", len(lines), counted)
	}
}

func composedWithheldTotal(composition CatalogComposition) int {
	total := 0
	for _, count := range composition.Withheld {
		total += count
	}
	return total
}
