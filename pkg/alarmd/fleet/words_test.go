// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"testing"
	"time"
)

// Every check the fleet can file a row under has exactly one pair of words,
// both from the closed lists, and the table names no check the fleet does
// not file. A check added without a pair is red here, not folded at
// runtime; a pair pointing at a word outside the lists is red here, not
// rendered as its code.
func TestEveryCheckHasExactlyOnePairOfWords(t *testing.T) {
	states, actions := map[StateWord]bool{}, map[ActionWord]bool{}
	for _, word := range StateWords {
		states[word] = true
	}
	for _, word := range ActionWords {
		actions[word] = true
	}
	if len(StateWords) != 8 || len(ActionWords) != 6 || len(WatchReasons) != 4 {
		t.Fatalf("vocabulary = %d state / %d action / %d watch words, want 8 / 6 / 4: the count is the ruling", len(StateWords), len(ActionWords), len(WatchReasons))
	}
	for _, check := range Checks() {
		pair, paired := checkWords[check]
		if !paired {
			t.Errorf("%s has no pair of words: it would fold to %s / %s at runtime", check, unpairedWords.State, unpairedWords.Action)
			continue
		}
		if !states[pair.State] || !actions[pair.Action] {
			t.Errorf("%s pairs to %s / %s, not both from the closed lists", check, pair.State, pair.Action)
		}
	}
	listed := map[Check]bool{}
	for _, check := range Checks() {
		listed[check] = true
	}
	for check := range checkWords {
		if !listed[check] {
			t.Errorf("the table pairs %s, which the fleet does not file", check)
		}
	}
	words := ProductWords()
	for _, word := range StateWords {
		if words.State[word] == "" {
			t.Errorf("state word %s has no rendering", word)
		}
	}
	for _, word := range ActionWords {
		if words.Action[word] == "" {
			t.Errorf("action word %s has no rendering", word)
		}
	}
	for _, word := range WatchReasons {
		if words.Watch[word] == "" {
			t.Errorf("watch reason %s has no rendering", word)
		}
	}
	// The fold for a word the table does not know goes one way only: to
	// this side, to be looked at. Never towards nothing to do.
	unpaired := standingOf(Anomaly{Finding: Finding{Check: Check("NOT_A_CHECK")}}, now)
	if unpaired.State != StateDefect || unpaired.Action != ActionServiceFix || unpaired.RefinedBy != RuleUnpaired {
		t.Fatalf("an unpaired check folded to %+v, want DEFECT / SERVICE_FIX named as unpaired", unpaired)
	}
	if unpairedWords.Action == ActionNone || unpairedWords.Action == ActionWatch {
		t.Fatal("the unpaired fold reads as nothing to do")
	}
}

func shortWindows(worstValid, worstRequired uint32, verdicts ...WindowVerdict) *HistoryCoverage {
	coverage := &HistoryCoverage{Levels: uint32(len(verdicts)), Short: uint32(len(verdicts)), WorstValid: worstValid, WorstRequired: worstRequired, Guarded: 1}
	for i, verdict := range verdicts {
		window := WindowRow{Key: "s/1", Series: "s", Level: uint32(i + 1), Valid: worstValid, Required: worstRequired, Verdict: verdict}
		switch verdict {
		case VerdictDataAbsentWhenQueried:
			window.HolesBy.AnsweredWithoutSeries = worstRequired - worstValid
		case VerdictInputIncomplete:
			window.HolesBy.InputIncomplete = 1
		case VerdictPointsUnusable:
			window.HolesBy.Unusable = 1
		default:
			window.HolesBy.NotInMemory = 1
		}
		coverage.Windows = append(coverage.Windows, window)
	}
	return coverage
}

// The undecided window's words are decided by its holes when every short
// window is named: one minute this side did not see whole keeps it ours,
// unusable records without that are the strategy's, holes the data's when
// asked for are the data's. An unknown hole, or an unnamed short window,
// decides nothing and the check's own pair stands.
func TestTheUndecidedWindowsWordsAreDecidedByTheirHoles(t *testing.T) {
	row := func(coverage *HistoryCoverage) Anomaly {
		return Anomaly{Finding: Finding{Check: CheckWindowUndecided}, Coverage: coverage, ReasonLastAt: now}
	}
	for name, testCase := range map[string]struct {
		coverage *HistoryCoverage
		state    StateWord
		action   ActionWord
		rule     StandingRule
	}{
		"every hole the data's":     {shortWindows(6, 9, VerdictDataAbsentWhenQueried, VerdictDataAbsentWhenQueried), StateDataAbsent, ActionDataCheck, RuleWindowVerdict},
		"one minute not seen whole": {shortWindows(6, 9, VerdictDataAbsentWhenQueried, VerdictInputIncomplete), StateResultUntrusted, ActionServiceFix, RuleWindowVerdict},
		"unusable records":          {shortWindows(6, 9, VerdictPointsUnusable, VerdictDataAbsentWhenQueried), StateStrategyInvalid, ActionStrategyEdit, RuleWindowVerdict},
		"an unknown hole":           {shortWindows(6, 9, VerdictUnknown, VerdictDataAbsentWhenQueried), StateResultUntrusted, ActionServiceFix, ""},
		"no windows named":          {&HistoryCoverage{Levels: 2, Short: 2, WorstValid: 6, WorstRequired: 9, Guarded: 2}, StateResultUntrusted, ActionServiceFix, ""},
	} {
		standing := standingOf(row(testCase.coverage), now)
		if standing.State != testCase.state || standing.Action != testCase.action || standing.RefinedBy != testCase.rule {
			t.Errorf("%s: standing = %+v, want %s / %s by %q", name, standing, testCase.state, testCase.action, testCase.rule)
		}
	}
	// A short window unnamed beside a named one: the named one's verdict is
	// not the row's.
	partial := shortWindows(6, 9, VerdictDataAbsentWhenQueried)
	partial.Short, partial.Levels = 2, 2
	if standing := standingOf(row(partial), now); standing.RefinedBy == RuleWindowVerdict {
		t.Fatalf("one named window of two decided the row: %+v", standing)
	}
}

// A wait has a reason and the reason has a rule: the worst window gained
// points or its holes are sliding out, a guard's count moved within the
// stalled bound, nothing has been heard within the recent window, or the
// next round decides. A row that is moving is a wait even when its holes
// would hand it to the data owner; a row that has stopped moving is not.
func TestAWaitIsDecidedByWhatIsMoving(t *testing.T) {
	filling := Anomaly{Finding: Finding{Check: CheckWindowUndecided}, ReasonLastAt: now,
		Coverage: shortWindows(6, 9, VerdictDataAbsentWhenQueried)}
	filling.Coverage.PreviousKnown, filling.Coverage.PreviousWorstValid = true, 5
	if standing := standingOf(filling, now); standing.Action != ActionWatch || standing.Watch != WatchWindowFilling || standing.RefinedBy != RuleWatch {
		t.Fatalf("a window that gained a point = %+v, want WATCH / WINDOW_FILLING", standing)
	}
	sliding := Anomaly{Finding: Finding{Check: CheckWindowUndecided}, ReasonLastAt: now,
		Coverage: shortWindows(6, 9, VerdictDataAbsentWhenQueried), WindowFill: &WindowFill{Sliding: true}}
	if standing := standingOf(sliding, now); standing.Watch != WatchWindowFilling {
		t.Fatalf("a window whose holes are sliding out = %+v, want WINDOW_FILLING", standing)
	}
	guarded := Anomaly{Finding: Finding{Check: CheckWindowUndecided}, ReasonLastAt: now,
		Coverage: &HistoryCoverage{Levels: 1, Short: 1, WorstValid: 6, WorstRequired: 9, Guarded: 1},
		Guards:   []GapGuard{{Required: 9, Observed: 4, UnchangedRounds: 1}}}
	if standing := standingOf(guarded, now); standing.Action != ActionWatch || standing.Watch != WatchGuardMoving {
		t.Fatalf("a guard that moved last round = %+v, want WATCH / GUARD_MOVING", standing)
	}
	// The same guard flat for the stalled bound is not moving; with no
	// series bound the wait becomes the data owner's, else this side's.
	stalledGuard := guarded
	stalledGuard.Guards = []GapGuard{{Required: 9, Observed: 4, UnchangedRounds: StalledRounds}}
	stalledGuard.PlanSeries = []PlanSeriesMatched{{Matched: 0}}
	if standing := standingOf(stalledGuard, now); standing.Action != ActionDataCheck || standing.State != StateDataAbsent || standing.RefinedBy != RuleStalled {
		t.Fatalf("a stalled guard on a Plan bound to no series = %+v, want DATA_ABSENT / DATA_CHECK by STALLED", standing)
	}
	stalledGuard.PlanSeries = []PlanSeriesMatched{{Matched: 12}}
	if standing := standingOf(stalledGuard, now); standing.Action != ActionServiceFix || standing.RefinedBy != RuleStalled {
		t.Fatalf("a stalled guard on a Plan with series = %+v, want SERVICE_FIX by STALLED", standing)
	}
	// Stalled with the holes decided: the holes say whose.
	stalledWindow := Anomaly{Finding: Finding{Check: CheckSeriesDataMissing}, ReasonLastAt: now, Coverage: shortWindows(4, 9, VerdictDataAbsentWhenQueried)}
	stalledWindow.Coverage.UnchangedRounds = StalledRounds
	if standing := standingOf(stalledWindow, now); standing.Action != ActionDataCheck || standing.RefinedBy != RuleWindowVerdict {
		t.Fatalf("a stalled window whose holes are the data's = %+v, want DATA_CHECK by WINDOW_VERDICT", standing)
	}
	// Unheard within the recent window.
	silent := Anomaly{Finding: Finding{Check: CheckWindowUndecided}, ReasonLastAt: now.Add(-RecentSkipWindow - time.Minute),
		Coverage: &HistoryCoverage{Levels: 1, Short: 1, WorstValid: 6, WorstRequired: 9, Guarded: 1}}
	if standing := standingOf(silent, now); standing.Action != ActionWatch || standing.Watch != WatchUnconfirmed {
		t.Fatalf("an object unheard for the window = %+v, want WATCH / UNCONFIRMED", standing)
	}
	// The checks whose pair is a wait name the next round when nothing
	// else on the row says why.
	for _, check := range []Check{CheckConfigUnresolved, CheckObservationGap} {
		if standing := standingOf(Anomaly{Finding: Finding{Check: check}, ReasonLastAt: now}, now); standing.Action != ActionWatch || standing.Watch != WatchNextRound {
			t.Fatalf("%s = %+v, want WATCH / NEXT_ROUND", check, standing)
		}
	}
	// Under no check: detecting, nothing to do. A retained record older
	// than the window: recovered, nothing to do.
	if standing := standingOf(Anomaly{}, now); standing.State != StateDetecting || standing.Action != ActionNone {
		t.Fatalf("a row under no check = %+v", standing)
	}
	historical := Anomaly{Finding: Finding{Check: CheckDetectionAbandoned}, Kind: KindSkippedSpan, Loss: LossHistorical}
	if standing := standingOf(historical, now); standing.State != StateRecovered || standing.Action != ActionNone || standing.RefinedBy != RuleHistoricalLoss {
		t.Fatalf("a historical record = %+v, want RECOVERED / NONE", standing)
	}
	ongoing := historical
	ongoing.Loss = LossOngoing
	if standing := standingOf(ongoing, now); standing.State != StateNotDetecting || standing.Action != ActionServiceFix {
		t.Fatalf("an ongoing loss = %+v, want the check's own pair", standing)
	}
}
