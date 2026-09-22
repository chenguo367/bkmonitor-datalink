// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"testing"
)

// soundCoverage is a fact set every rule accepts, with one named window.
func soundCoverage() *HistoryCoverageFacts {
	return &HistoryCoverageFacts{Levels: 3, Short: 1, WorstValid: 6, WorstRequired: 9, Guarded: 1, Fresh: 1, ShortFresh: 1,
		Windows: []HistoryWindowFact{namedWindow()}}
}

// Every rule normalize refuses under has a name, and each fixture here breaks
// exactly one of them: the rejection names that rule, a rule about one window
// names the window's series, and the counts do not travel with the shell.
// The table is checked against the closed list both ways, so a rule added to
// one and not the other goes red.
func TestEveryCoverageRejectionHasItsOwnRule(t *testing.T) {
	t.Parallel()
	breakers := map[CoverageRejectionRule]func(*HistoryCoverageFacts){
		CoverageRejectLevelsZero: func(f *HistoryCoverageFacts) {
			f.Levels, f.Short, f.Guarded, f.Fresh, f.ShortFresh, f.Windows = 0, 0, 0, 0, 0, nil
		},
		CoverageRejectShortOverLevels:        func(f *HistoryCoverageFacts) { f.Short = f.Levels + 1 },
		CoverageRejectEmptyOverShort:         func(f *HistoryCoverageFacts) { f.Empty = f.Short + 1 },
		CoverageRejectGuardedOverLevels:      func(f *HistoryCoverageFacts) { f.Guarded = f.Levels + 1 },
		CoverageRejectFreshOverLevels:        func(f *HistoryCoverageFacts) { f.Fresh = f.Levels + 1 },
		CoverageRejectShortFreshOverShort:    func(f *HistoryCoverageFacts) { f.Fresh, f.ShortFresh = 3, 2 },
		CoverageRejectShortFreshOverFresh:    func(f *HistoryCoverageFacts) { f.Fresh, f.ShortFresh, f.Short = 0, 1, 1 },
		CoverageRejectUnusableOverLevels:     func(f *HistoryCoverageFacts) { f.Unusable, f.UnusableReason = f.Levels+1, "UNAVAILABLE" },
		CoverageRejectUnusableReasonUnpaired: func(f *HistoryCoverageFacts) { f.Unusable, f.UnusableReason = 1, "" },
		CoverageRejectEmptyWithValidPoints:   func(f *HistoryCoverageFacts) { f.Empty, f.WorstValid = 1, 6 },
		CoverageRejectWindowsOverShort:       func(f *HistoryCoverageFacts) { f.Short, f.ShortFresh = 0, 0 },
		CoverageRejectWindowsOverBound: func(f *HistoryCoverageFacts) {
			f.Levels, f.Short, f.Fresh, f.ShortFresh = 20, 20, 0, 0
			for len(f.Windows) <= MaxHistoryWindows {
				f.Windows = append(f.Windows, namedWindow())
			}
		},
		CoverageRejectWindowUnnamed:         func(f *HistoryCoverageFacts) { f.Windows[0].Series = "" },
		CoverageRejectWindowRequiredZero:    func(f *HistoryCoverageFacts) { f.Windows[0].Required = 0 },
		CoverageRejectWindowNotShort:        func(f *HistoryCoverageFacts) { f.Windows[0].Valid = f.Windows[0].Required },
		CoverageRejectWindowHoleArithmetic:  func(f *HistoryCoverageFacts) { f.Windows[0].MissingTotal++ },
		CoverageRejectWindowHoleListOverrun: func(f *HistoryCoverageFacts) { f.Windows[0].Missing = append(f.Windows[0].Missing, 60, 0, -60) },
		CoverageRejectWindowGuardReasonFree: func(f *HistoryCoverageFacts) { f.Windows[0].Guarded = false },
	}
	if len(breakers) != len(CoverageRejectionRules) {
		t.Fatalf("%d breakers for %d rules: every rule in the closed list needs a fixture", len(breakers), len(CoverageRejectionRules))
	}
	for _, rule := range CoverageRejectionRules {
		breaker, listed := breakers[rule]
		if !listed {
			t.Fatalf("rule %s has no fixture", rule)
		}
		facts := soundCoverage()
		breaker(facts)
		got, rejected := normalizeHistoryCoverageFacts(facts)
		if got != nil || rejected == nil {
			t.Fatalf("%s: facts survived as %+v, rejection %+v", rule, got, rejected)
		}
		if rejected.Rule != rule {
			t.Errorf("fixture for %s was refused under %s: the fixture trips another rule first, or the rule is misnamed", rule, rejected.Rule)
		}
		// A rule about one window names the window's series -- except the
		// rule that the window has none, which has nothing to name.
		if (isWindowRule(rule) && rule != CoverageRejectWindowUnnamed) != (rejected.Series != "") {
			t.Errorf("%s: series %q, want it named exactly for the rules about one named window", rule, rejected.Series)
		}
	}
	// The accepted set carries no rejection, and a nil set neither.
	if got, rejected := normalizeHistoryCoverageFacts(soundCoverage()); got == nil || rejected != nil {
		t.Fatalf("sound facts: %+v / %+v", got, rejected)
	}
	if got, rejected := normalizeHistoryCoverageFacts(nil); got != nil || rejected != nil {
		t.Fatalf("nil facts: %+v / %+v, want neither facts nor a rejection", got, rejected)
	}
}

func isWindowRule(rule CoverageRejectionRule) bool {
	switch rule {
	case CoverageRejectWindowUnnamed, CoverageRejectWindowRequiredZero, CoverageRejectWindowNotShort,
		CoverageRejectWindowHoleArithmetic, CoverageRejectWindowHoleListOverrun, CoverageRejectWindowGuardReasonFree:
		return true
	}
	return false
}

// The rejection rides the observation where the facts would have: normalize
// clears the facts and sets the rejection, and an accepted set sets neither.
func TestARefusedCoverageLeavesItsRuleOnTheObservation(t *testing.T) {
	t.Parallel()
	refused := NormalizeObservation(Observation{Component: ComponentScheduler, Stage: StageProgressCommitted, Result: ResultSuccess,
		HistoryCoverage: &HistoryCoverageFacts{Levels: 2, Short: 3}})
	if refused.HistoryCoverage != nil || refused.HistoryCoverageRejected == nil || refused.HistoryCoverageRejected.Rule != CoverageRejectShortOverLevels {
		t.Fatalf("refused observation carries coverage %+v rejection %+v", refused.HistoryCoverage, refused.HistoryCoverageRejected)
	}
	accepted := NormalizeObservation(Observation{Component: ComponentScheduler, Stage: StageProgressCommitted, Result: ResultSuccess,
		HistoryCoverage: soundCoverage()})
	if accepted.HistoryCoverage == nil || accepted.HistoryCoverageRejected != nil {
		t.Fatalf("accepted observation carries coverage %+v rejection %+v", accepted.HistoryCoverage, accepted.HistoryCoverageRejected)
	}
}
