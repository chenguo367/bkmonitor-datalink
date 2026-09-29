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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The short windows a round did not name are the data's only when every minute
// any of them is missing at was evaluated by a remembered round that answered
// its primary whole -- the reading a named hole gets as
// ROUND_ANSWERED_WITHOUT_SERIES. Every other shape of the union, and every
// other kind of round, says nothing about them.
func TestUnnamedShortWindowsAreTheDatasOnlyAtMinutesAnsweredWhole(t *testing.T) {
	rounds := []roundMark{
		{slot: 160, end: 100, kind: "COMPLETED_WITH_UNAVAILABLE", primary: primary("FULL", "DATA")},
		{slot: 220, end: 160, kind: "COMPLETED_WITH_UNAVAILABLE", primary: primary("FULL", "DATA")},
		{slot: 280, end: 220, kind: "COMPLETED_WITH_UNAVAILABLE", primary: primary("FULL", "EMPTY")},
		{slot: 340, end: 280, kind: "COMPLETED_WITH_PARTIAL_GAP", primary: primary("PARTIAL", "DATA")},
		{slot: 400, end: 340, kind: "COMPLETED_WITH_UNAVAILABLE"},
	}
	for name, tc := range map[string]struct {
		mutate func(*observability.HistoryCoverageFacts)
		want   bool
	}{
		"every unnamed minute answered whole":       {func(*observability.HistoryCoverageFacts) {}, true},
		"a minute answered empty":                   {func(f *observability.HistoryCoverageFacts) { f.MissingMinutes = []int64{100, 220} }, false},
		"a minute answered partly":                  {func(f *observability.HistoryCoverageFacts) { f.MissingMinutes = []int64{160, 280} }, false},
		"a minute whose answer went unrecorded":     {func(f *observability.HistoryCoverageFacts) { f.MissingMinutes = []int64{100, 340} }, false},
		"a minute no remembered round evaluated":    {func(f *observability.HistoryCoverageFacts) { f.MissingMinutes = []int64{100, 460} }, false},
		"the union was cut short":                   {func(f *observability.HistoryCoverageFacts) { f.MissingMinutesTruncated = true }, false},
		"an unnamed window holds an unusable point": {func(f *observability.HistoryCoverageFacts) { f.ShortUnusable = 1 }, false},
		"no union at all, as an older worker sends": {func(f *observability.HistoryCoverageFacts) { f.MissingMinutes = nil }, false},
		"every short window named":                  {func(f *observability.HistoryCoverageFacts) { f.Short = 8 }, false},
	} {
		facts := &observability.HistoryCoverageFacts{Levels: 20, Short: 14, Windows: make([]observability.HistoryWindowFact, 8),
			MissingMinutes: []int64{100, 160}, End: 400}
		tc.mutate(facts)
		if got := unlistedHolesAnswered(rounds, facts); got != tc.want {
			t.Errorf("%s: unlisted holes answered = %v, want %v", name, got, tc.want)
		}
	}
	if unlistedHolesAnswered(rounds, nil) {
		t.Error("a round with no coverage said its unnamed windows were the data's")
	}
}

// A Query Group five strategies share turned three hosts that miss whole
// minutes into fifteen short windows and named eight. Every named one was the
// data's; the row went to WINDOW_UNDECIDED -- this side's, to fix -- for want of
// the other seven. With their minutes read it is SERIES_SPARSE, and without
// them it stays where it was.
func TestAWindowLineWithUnnamedWindowsIsTheDatasOnlyWhenTheirMinutesAre(t *testing.T) {
	sparse := func(missing uint32) WindowRow {
		return WindowRow{Verdict: VerdictDataAbsentWhenQueried, MissingTotal: missing, HolesBy: WindowHoleCounts{AnsweredWithoutSeries: missing}}
	}
	named := []WindowRow{sparse(5), sparse(5), sparse(5), sparse(4), sparse(4), sparse(4), sparse(3), sparse(3)}
	guarded := func(unlisted bool, windows ...WindowRow) Anomaly {
		return Anomaly{Kind: KindDegradedRun, ReasonCode: "COMPLETED_WITH_UNAVAILABLE", Cause: "GAP_GUARD_WARMING", CauseReason: "GAP_GUARD_WARMING",
			Coverage: &HistoryCoverage{Levels: 33, Short: 15, WorstValid: 4, WorstRequired: 9, ShortRounds: 35, Guarded: 15,
				Windows: windows, UnlistedHolesAnswered: unlisted}}
	}
	gapped := func(unlisted bool, windows ...WindowRow) Anomaly {
		return Anomaly{Kind: KindDegradedRun, CauseReason: "HISTORY_GAPPED", Coverage: &HistoryCoverage{
			Levels: 33, Short: 15, WorstValid: 4, WorstRequired: 9, ShortRounds: 35, Windows: windows, UnlistedHolesAnswered: unlisted}}
	}
	incomplete := WindowRow{Verdict: VerdictInputIncomplete, MissingTotal: 2, HolesBy: WindowHoleCounts{AnsweredWithoutSeries: 1, InputIncomplete: 1}}
	for name, tc := range map[string]struct {
		row  Anomaly
		want Check
	}{
		"guarded, named sparse, the rest answered whole":  {guarded(true, named...), CheckSeriesSparse},
		"guarded, named sparse, the rest unread":          {guarded(false, named...), CheckWindowUndecided},
		"guarded, one named window incomplete":            {guarded(true, append(append([]WindowRow{}, named[:7]...), incomplete)...), CheckWindowUndecided},
		"gapped, named sparse, the rest answered whole":   {gapped(true, named...), CheckSeriesSparse},
		"gapped, named sparse, the rest unread":           {gapped(false, named...), CheckSeriesDataMissing},
		"guarded, nothing named, the rest answered whole": {guarded(true), CheckWindowUndecided},
	} {
		check, under, _ := checkOf(tc.row, ScheduleOnTime)
		if check != tc.want || !under {
			t.Errorf("%s: check = %s (under %v), want %s", name, check, under, tc.want)
		}
	}
}
