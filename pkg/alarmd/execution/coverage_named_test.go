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
