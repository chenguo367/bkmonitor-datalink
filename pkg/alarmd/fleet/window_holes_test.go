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
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func primary(completeness, dataState string) *observability.PrimaryInputFacts {
	return &observability.PrimaryInputFacts{Completeness: completeness, DataState: dataState}
}

// A round of the object: the Slot, what it completed as, what its primary
// answered, and the coverage it reported (nil for a round with no window).
func round(ctx context.Context, tracker *Tracker, slot int64, kind, cause, reason string, primary *observability.PrimaryInputFacts, coverage *observability.HistoryCoverageFacts) {
	tracker.Observe(ctx, observability.Observation{
		ProgressCompletionKind: kind, ProgressCompletionCause: cause, ProgressCompletionReason: reason,
		PrimaryInput: primary, HistoryCoverage: coverage,
		Trace: observability.TraceFields{StrategyID: "4101", BusinessID: "7", EvaluationTime: slot},
	})
}

// Every hole on a named window is read against the object's own rounds, and
// each is one of five things: the round of that minute answered whole and the
// series was not in it (the data's), answered whole with nothing at all (the
// data's, for the whole object), did not see the minute whole (this side's or
// its dependency's), holds a record the Level could not use, or is a minute
// no remembered round evaluated. The object here evaluates minute m at Slot
// m+60; two of its rounds carried no record, so their minute is inferred from
// that offset and the hole says so.
func TestEveryHoleIsReadAgainstTheRoundOfItsMinute(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-holes"})
	full := func(end int64) *observability.HistoryCoverageFacts {
		return &observability.HistoryCoverageFacts{Levels: 1, End: end}
	}
	// Minute 240: healthy, series c in it.
	round(ctx, tracker, 300, "FULL_COMPLETED", "", "", primary("FULL", "DATA"), full(240))
	// Minute 300: the primary did not answer; no window, minute inferred.
	round(ctx, tracker, 360, "COMPLETED_WITH_UNAVAILABLE", "PRIMARY_INPUT_UNAVAILABLE", "QUERY_TIMEOUT", primary("UNAVAILABLE", ""), nil)
	// Minute 360: answered whole, nothing in it; no window, minute inferred.
	round(ctx, tracker, 420, "FULL_EMPTY_COMPLETED", "", "", primary("FULL", "EMPTY"), nil)
	// Minute 420: healthy, series c in it.
	round(ctx, tracker, 480, "FULL_COMPLETED", "", "", primary("FULL", "DATA"), full(420))
	// Minute 480: healthy, another series in it, c not.
	round(ctx, tracker, 540, "FULL_COMPLETED", "", "", primary("FULL", "DATA"), full(480))
	// Minute 540: c is back, its six-position window has 240, 420, 540 and
	// lacks 300, 360, 480; 180 is before this process's memory.
	short := &observability.HistoryCoverageFacts{Levels: 1, Short: 1, WorstValid: 3, WorstRequired: 7, End: 540,
		Windows: []observability.HistoryWindowFact{{Series: "c", Level: 1, Valid: 3, Required: 7, End: 540,
			Missing: []int64{180, 300, 360, 480}, MissingTotal: 4}}}
	for i := 0; i < DefaultDegradedRounds; i++ {
		round(ctx, tracker, 600, "COMPLETED_WITH_UNAVAILABLE", "LEVEL_OUTCOME_UNKNOWN", "HISTORY_GAPPED", primary("FULL", "DATA"), short)
	}
	rows := anyColumn(tracker)
	if len(rows) != 1 || rows[0].Coverage == nil || len(rows[0].Coverage.Windows) != 1 {
		t.Fatalf("rows = %+v, want the one object with its one named window", rows)
	}
	coverage := rows[0].Coverage
	window := coverage.Windows[0]
	if window.Key != "c/1" || window.Series != "c" || window.Level != 1 || window.Valid != 3 || window.Required != 7 ||
		!window.End.Equal(time.Unix(540, 0)) {
		t.Fatalf("window = %+v, want c/1, 3 of 7, ending at 540", window)
	}
	want := []struct {
		at       int64
		cause    HoleCause
		kind     string
		inferred bool
	}{
		{180, HoleNotInMemory, "", false},
		{300, HoleInputIncomplete, "COMPLETED_WITH_UNAVAILABLE", true},
		{360, HoleAnsweredEmpty, "FULL_EMPTY_COMPLETED", true},
		{480, HoleAnsweredWithoutSeries, "FULL_COMPLETED", false},
	}
	if len(window.Holes) != len(want) {
		t.Fatalf("holes = %+v, want %d", window.Holes, len(want))
	}
	for i, hole := range window.Holes {
		if !hole.At.Equal(time.Unix(want[i].at, 0)) || hole.Cause != want[i].cause || hole.Round != want[i].kind || hole.Inferred != want[i].inferred {
			t.Fatalf("hole %d = %+v, want minute %d %s from %q (inferred %v)", i, hole, want[i].at, want[i].cause, want[i].kind, want[i].inferred)
		}
	}
	if window.Holes[1].Reason != "QUERY_TIMEOUT" {
		t.Fatalf("the incomplete round's hole carries reason %q, want the round's QUERY_TIMEOUT", window.Holes[1].Reason)
	}
	if by := window.HolesBy; by != (WindowHoleCounts{AnsweredWithoutSeries: 1, AnsweredEmpty: 1, InputIncomplete: 1, NotInMemory: 1}) {
		t.Fatalf("holes by cause = %+v", by)
	}
	if window.Verdict != VerdictInputIncomplete {
		t.Fatalf("verdict = %s, want INPUT_INCOMPLETE: one minute this side never saw whole is enough to keep the window ours", window.Verdict)
	}
	if coverage.RoundsRemembered != 5+DefaultDegradedRounds || coverage.RoundsKept != RecentRoundsKept {
		t.Fatalf("rounds remembered/kept = %d/%d, want %d/%d", coverage.RoundsRemembered, coverage.RoundsKept, 5+DefaultDegradedRounds, RecentRoundsKept)
	}
	if coverage.WorstWindow != "c/1" || coverage.WorstWindowChanged {
		t.Fatalf("worst window = %q (changed %v), want c/1 unchanged", coverage.WorstWindow, coverage.WorstWindowChanged)
	}
}

// The verdict is decided from the counts in one order, and the two
// readings a reader would hand to different owners are told apart: every
// hole the data's, or one of them ours. Unlisted holes and unusable
// records each take their own branch.
func TestTheWindowVerdictIsDecidedFromItsHoles(t *testing.T) {
	rounds := []roundMark{
		{slot: 300, end: 240, kind: "FULL_COMPLETED", primary: primary("FULL", "DATA")},
		{slot: 360, end: 300, kind: "FULL_COMPLETED", primary: primary("FULL", "DATA")},
	}
	window := func(missing []int64, missingTotal uint32, unusable []int64, unusableTotal uint32) observability.HistoryWindowFact {
		return observability.HistoryWindowFact{Series: "c", Level: 1, Valid: 9 - missingTotal - unusableTotal, Required: 9, End: 540,
			Missing: missing, MissingTotal: missingTotal, Unusable: unusable, UnusableTotal: unusableTotal}
	}
	for name, testCase := range map[string]struct {
		window  observability.HistoryWindowFact
		verdict WindowVerdict
	}{
		"all holes on answered rounds":   {window([]int64{240, 300}, 2, nil, 0), VerdictDataAbsentWhenQueried},
		"a hole before memory":           {window([]int64{120, 240}, 2, nil, 0), VerdictUnknown},
		"a hole beyond the listing":      {window([]int64{240}, 2, nil, 0), VerdictUnknown},
		"an unusable record":             {window([]int64{240}, 1, []int64{300}, 1), VerdictPointsUnusable},
		"unusable beside an unseen hole": {window([]int64{120}, 1, []int64{300}, 1), VerdictPointsUnusable},
	} {
		rows := windowRows(rounds, &observability.HistoryCoverageFacts{Levels: 1, Short: 1, Windows: []observability.HistoryWindowFact{testCase.window}})
		if len(rows) != 1 || rows[0].Verdict != testCase.verdict {
			t.Fatalf("%s: rows = %+v, want verdict %s", name, rows, testCase.verdict)
		}
	}
	// An incomplete round outranks everything else on the window.
	rounds = append(rounds, roundMark{slot: 420, end: 360, kind: "COMPLETED_WITH_UNAVAILABLE", primary: primary("PARTIAL", "DATA")})
	rows := windowRows(rounds, &observability.HistoryCoverageFacts{Levels: 1, Short: 1,
		Windows: []observability.HistoryWindowFact{window([]int64{120, 240, 360}, 3, []int64{300}, 1)}})
	if rows[0].Verdict != VerdictInputIncomplete || rows[0].HolesBy.InputIncomplete != 1 {
		t.Fatalf("rows = %+v, want INPUT_INCOMPLETE over unusable and unseen", rows)
	}
	// A completion with no primary fact at all -- a Slot skipped without a
	// query -- is a minute not seen whole.
	rounds = []roundMark{{slot: 300, end: 240, kind: "GAP_SKIPPED", reason: "GAP_SKIPPED"}}
	rows = windowRows(rounds, &observability.HistoryCoverageFacts{Levels: 1, Short: 1, Windows: []observability.HistoryWindowFact{window([]int64{240}, 1, nil, 0)}})
	if rows[0].Holes[0].Cause != HoleInputIncomplete || rows[0].Holes[0].Reason != "GAP_SKIPPED" {
		t.Fatalf("a skipped round's hole = %+v, want INPUT_INCOMPLETE carrying the skip reason", rows[0].Holes[0])
	}
	if windowRows(rounds, nil) != nil || windowRows(rounds, &observability.HistoryCoverageFacts{Levels: 1}) != nil {
		t.Fatal("rows were made for a round that named no window")
	}
}

// The round-over-round counters compare one window's pair with itself. When
// the worst pair moves to another series -- a round with one series, then
// two -- the comparison is between two windows and says nothing about
// either; the counters start over and the row says the window changed.
// On a live object 8 of 9, then 6, then 4 read as one window losing points
// until the windows were named.
func TestTheProgressCountersStartOverWhenTheWorstWindowChanges(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-moving"})
	short := func(slot int64, series string, valid uint32) {
		round(ctx, tracker, slot, "COMPLETED_WITH_UNAVAILABLE", "LEVEL_OUTCOME_UNKNOWN", "HISTORY_GAPPED", primary("FULL", "DATA"),
			&observability.HistoryCoverageFacts{Levels: 1, Short: 1, WorstValid: valid, WorstRequired: 9, End: slot - 60,
				Windows: []observability.HistoryWindowFact{{Series: series, Level: 1, Valid: valid, Required: 9, End: slot - 60,
					Missing: []int64{slot - 120}, MissingTotal: 9 - valid}}})
	}
	short(600, "a", 4)
	short(660, "a", 4)
	short(720, "a", 4)
	rows := anyColumn(tracker)
	if len(rows) != 1 || rows[0].Coverage.UnchangedRounds != 2 || rows[0].Coverage.NoProgressRounds != 2 || !rows[0].Coverage.PreviousKnown {
		t.Fatalf("after three flat rounds on one window: %+v", rows[0].Coverage)
	}
	// The worst pair moves to series b, at the same count.
	short(780, "b", 4)
	rows = anyColumn(tracker)
	coverage := rows[0].Coverage
	if !coverage.WorstWindowChanged || coverage.WorstWindow != "b/1" {
		t.Fatalf("the window moved and the row did not say so: %+v", coverage)
	}
	if coverage.UnchangedRounds != 0 || coverage.NoProgressRounds != 0 || coverage.PreviousKnown {
		t.Fatalf("counters after the window changed = flat %d / no-progress %d / previous known %v, want 0 / 0 / false: "+
			"the previous count was another window's", coverage.UnchangedRounds, coverage.NoProgressRounds, coverage.PreviousKnown)
	}
	// And they count again from there on the new window.
	short(840, "b", 4)
	coverage = anyColumn(tracker)[0].Coverage
	if coverage.WorstWindowChanged || coverage.UnchangedRounds != 1 || coverage.PreviousWorstValid != 4 || !coverage.PreviousKnown {
		t.Fatalf("the next round on the new window: %+v", coverage)
	}
}
