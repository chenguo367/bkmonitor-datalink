// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "testing"

// The named windows are the worst few, worst first, however the records
// were ordered: the fleet reads Windows[0] as the window the worst pair
// belongs to, and a merge across records has to give the same list as one
// run over all of them. A window that is not short is not named.
func TestTheNamedWindowsAreTheWorstFewWorstFirst(t *testing.T) {
	var coverage HistoryCoverage
	// Twelve short windows with shortfalls 1..12 observed in a scrambled
	// order, plus a full one.
	for _, shortfall := range []uint32{5, 1, 12, 7, 3, 9, 2, 11, 4, 10, 6, 8} {
		coverage.ObserveWindow(WindowCoverage{Series: SeriesIdentityDigest("s"), LevelID: shortfall, Valid: 20 - shortfall, Required: 20})
	}
	coverage.ObserveWindow(WindowCoverage{Series: "full", LevelID: 1, Valid: 20, Required: 20})
	if len(coverage.Windows) != MaxCoverageWindows {
		t.Fatalf("named %d windows, want the bound of %d", len(coverage.Windows), MaxCoverageWindows)
	}
	for i, window := range coverage.Windows {
		if want := uint32(12 - i); window.Shortfall() != want {
			t.Fatalf("window %d has shortfall %d, want %d: the list is not worst first", i, window.Shortfall(), want)
		}
	}
	// A merge of two partial runs names the same list.
	var left, right HistoryCoverage
	for i, shortfall := range []uint32{5, 1, 12, 7, 3, 9, 2, 11, 4, 10, 6, 8} {
		target := &left
		if i%2 == 1 {
			target = &right
		}
		target.Levels++
		if int64(shortfall) > target.End {
			target.End = int64(shortfall)
		}
		target.ObserveWindow(WindowCoverage{Series: SeriesIdentityDigest("s"), LevelID: shortfall, Valid: 20 - shortfall, Required: 20, End: int64(shortfall)})
	}
	left.Merge(right)
	for i, window := range left.Windows {
		if window.LevelID != coverage.Windows[i].LevelID {
			t.Fatalf("merged window %d is Level %d, want %d", i, window.LevelID, coverage.Windows[i].LevelID)
		}
	}
	if left.End != 12 {
		t.Fatalf("merged end = %d, want the newest minute of either side", left.End)
	}
	// Ties by series then Level, so the same round names the same windows
	// whichever record came first.
	var a, b HistoryCoverage
	a.ObserveWindow(WindowCoverage{Series: "x", LevelID: 2, Valid: 1, Required: 3})
	a.ObserveWindow(WindowCoverage{Series: "x", LevelID: 1, Valid: 1, Required: 3})
	b.ObserveWindow(WindowCoverage{Series: "x", LevelID: 1, Valid: 1, Required: 3})
	b.ObserveWindow(WindowCoverage{Series: "x", LevelID: 2, Valid: 1, Required: 3})
	if a.Windows[0].LevelID != 1 || b.Windows[0].LevelID != 1 {
		t.Fatalf("ties ordered %d / %d, want Level 1 first on both", a.Windows[0].LevelID, b.Windows[0].LevelID)
	}
}

// A window is one series at one Level, and an object whose Plans cover the
// same series reaches the same window once per Plan. Named once: the list is
// the worst few windows, and a place spent on a copy is a genuinely
// different window the reader never sees.
//
// A six-Plan object on a live deployment produced seven names with four
// distinct windows among them -- three of them byte-identical repeats -- so
// three of the eight places were copies.
func TestAWindowReachedBySeveralPlansIsNamedOnce(t *testing.T) {
	// Two Plans over the same three series, merged the way a round merges
	// its Plans' coverage.
	var round HistoryCoverage
	for plan := 0; plan < 2; plan++ {
		var perPlan HistoryCoverage
		perPlan.Levels = 3
		for _, series := range []SeriesIdentityDigest{"a", "b", "c"} {
			perPlan.ObserveWindow(WindowCoverage{Series: series, LevelID: 1, Valid: 6, Required: 9, MissingTotal: 3})
		}
		round.Merge(perPlan)
	}
	if len(round.Windows) != 3 {
		t.Fatalf("named %d windows, want the three distinct ones: %+v", len(round.Windows), round.Windows)
	}
	seen := map[string]int{}
	for _, window := range round.Windows {
		seen[string(window.Series)+"/"+string(rune('0'+window.LevelID))]++
	}
	for key, count := range seen {
		if count != 1 {
			t.Errorf("window %s named %d times", key, count)
		}
	}
	// The same series at a different Level is a different window, and the
	// bound is still spent on distinct ones.
	round.ObserveWindow(WindowCoverage{Series: "a", LevelID: 2, Valid: 6, Required: 9, MissingTotal: 3})
	if len(round.Windows) != 4 {
		t.Fatalf("named %d windows, want the same series at another Level to be its own: %+v", len(round.Windows), round.Windows)
	}
	// A repeat that is worse replaces the one kept; a repeat that is not is
	// dropped. The list says the worst reading of each window, once.
	round.ObserveWindow(WindowCoverage{Series: "a", LevelID: 1, Valid: 1, Required: 9, MissingTotal: 8})
	round.ObserveWindow(WindowCoverage{Series: "a", LevelID: 1, Valid: 8, Required: 9, MissingTotal: 1})
	worst := uint32(0)
	named := 0
	for _, window := range round.Windows {
		if window.Series == "a" && window.LevelID == 1 {
			named, worst = named+1, window.Shortfall()
		}
	}
	if named != 1 || worst != 8 {
		t.Errorf("series a Level 1 named %d times with shortfall %d, want once with the worst reading (8)", named, worst)
	}
	if len(round.Windows) != 4 {
		t.Errorf("named %d windows after two repeats, want the four distinct ones", len(round.Windows))
	}
}
