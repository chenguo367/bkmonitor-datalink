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
	"fmt"
	"strings"
	"testing"
	"time"
)

// The platform's list drops 22 strategies at the top of the hour and lists
// them again six minutes later; one round's dispositions say PENDING_REMOVAL,
// then REMOVED, then nothing. The account says what happened: when each
// went absent, that they came back, in which hour and how long they were
// gone -- and a strategy that never comes back within the window is a
// strategy removed, not a flap.
func TestTheSourceSetAccountTellsAFlapFromARemoval(t *testing.T) {
	start := time.Date(2026, 9, 21, 18, 59, 0, 0, time.UTC)
	ledger := NewSourceSetLedger(func() time.Time { return start })
	names := func(prefix string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, fmt.Sprintf("%s%02d", prefix, i))
		}
		return out
	}
	flapping := names("s", 22)
	stayed := []string{"keep-1", "keep-2"}
	gone := []string{"deleted-1"}
	all := append(append([]string{}, stayed...), flapping...)
	// 18:59: everything listed.
	ledger.NoteRound(SourceSetRound{At: start, Accepted: append(append([]string{}, all...), gone...)})
	// 19:01: the list lost 22 and the deleted one; grace.
	at := start.Add(2 * time.Minute)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: stayed, PendingRemoval: append(append([]string{}, flapping...), gone...)})
	facts := ledger.Facts(at)
	if facts.PendingRemoval != 23 || facts.Removed != 0 || len(facts.PendingRemovalSamples) != SourceSetSampleLimit ||
		!facts.PendingRemovalSamples[0].AbsentSince.Equal(at) {
		t.Fatalf("under grace: %+v", facts)
	}
	// 19:02: still under grace. The grace runs for minutes now, so the same
	// strategies are PENDING_REMOVAL round after round: absent since stays
	// the first round, and nothing is dropped twice.
	firstAbsent := at
	at = start.Add(3 * time.Minute)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: stayed, PendingRemoval: append(append([]string{}, flapping...), gone...)})
	facts = ledger.Facts(at)
	if facts.PendingRemoval != 23 || !facts.PendingRemovalSamples[0].AbsentSince.Equal(firstAbsent) || facts.Hours[0].Dropped != 23 {
		t.Fatalf("a second round under grace moved absent_since or dropped again: %+v / %+v", facts.PendingRemovalSamples[0], facts.Hours[0])
	}
	// 19:03: still absent, and the grace is over for them.
	at = start.Add(4 * time.Minute)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: stayed, Removed: append(append([]string{}, flapping...), gone...)})
	facts = ledger.Facts(at)
	if facts.PendingRemoval != 0 || facts.Removed != 23 {
		t.Fatalf("removed: %+v", facts)
	}
	// 19:07:45: the 22 are listed again; the deleted one is not.
	at = start.Add(8*time.Minute + 45*time.Second)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: all})
	facts = ledger.Facts(at)
	if facts.PendingRemoval != 0 || facts.Removed != 1 || facts.ReactivatedThisHour != 22 {
		t.Fatalf("after the return: %+v", facts)
	}
	if len(facts.Hours) != 1 {
		t.Fatalf("hours = %+v, want the one hour", facts.Hours)
	}
	hour := facts.Hours[0]
	if !hour.Hour.Equal(time.Date(2026, 9, 21, 19, 0, 0, 0, time.UTC)) || hour.Dropped != 23 || hour.Removed != 23 || hour.Reactivated != 22 ||
		hour.LongestAbsentSeconds != (6*time.Minute+45*time.Second).Seconds() || len(hour.Samples) != SourceSetSampleLimit {
		t.Fatalf("hour = %+v, want 23 dropped, 23 removed, 22 back after 6m45s, a bounded sample", hour)
	}
	// The same hour again next hour: a second hour on the account, and the
	// deleted strategy, absent past the return window, is forgotten rather
	// than reported as removed forever.
	at = start.Add(time.Hour + 2*time.Minute)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: stayed, PendingRemoval: flapping})
	at = start.Add(time.Hour + 8*time.Minute)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: all})
	facts = ledger.Facts(at)
	if len(facts.Hours) != 2 || facts.Hours[0].Hour.Hour() != 20 || facts.Hours[0].Reactivated != 22 || facts.Hours[1].Hour.Hour() != 19 {
		t.Fatalf("two hours, newest first: %+v", facts.Hours)
	}
	if facts.Removed != 1 {
		t.Fatalf("the deleted strategy is still within the return window: %+v", facts)
	}
	at = start.Add(7 * time.Hour)
	ledger.NoteRound(SourceSetRound{At: at, Accepted: all})
	if facts = ledger.Facts(at); facts.Removed != 0 || facts.PendingRemoval != 0 {
		t.Fatalf("past the return window the deleted strategy is forgotten: %+v", facts)
	}
	// Coming back after the window is not a flap.
	ledger.NoteRound(SourceSetRound{At: at.Add(time.Minute), Accepted: append(append([]string{}, all...), gone...)})
	if facts = ledger.Facts(at.Add(time.Minute)); facts.ReactivatedThisHour != 0 {
		t.Fatalf("a return after the window counted as a reactivation: %+v", facts)
	}
	if !facts.Since.Equal(start) {
		t.Fatalf("since = %v, want the ledger's start %v", facts.Since, start)
	}
}

// The account is a line: the hours of the last day in which strategies came
// back, newest first, each with its strategies, and the line's sentence
// says how often and what is under grace now. An account with no returns is
// no line, and a return older than a day is not on it.
func TestTheSourceSetLineFoldsByTheHourTheStrategiesCameBack(t *testing.T) {
	at := time.Date(2026, 9, 21, 19, 30, 0, 0, time.UTC)
	set := &SourceSetFacts{Since: at.Add(-30 * time.Hour), PendingRemoval: 3, ReactivatedThisHour: 22,
		PendingRemovalSamples: []AbsentSample{{StrategyID: "s01", AbsentSince: at.Add(-2 * time.Minute)}},
		Hours: []SourceSetHour{
			{Hour: at.Truncate(time.Hour), Dropped: 23, Removed: 23, Reactivated: 22, LongestAbsentSeconds: 405, Samples: []string{"s01", "s02"}},
			{Hour: at.Truncate(time.Hour).Add(-time.Hour), Dropped: 22, Removed: 22, Reactivated: 22, LongestAbsentSeconds: 390, Samples: []string{"s01"}},
			// Dropped and not yet back: not a fold of returns.
			{Hour: at.Truncate(time.Hour).Add(-2 * time.Hour), Dropped: 5},
			// Older than a day: history, off the line.
			{Hour: at.Truncate(time.Hour).Add(-26 * time.Hour), Dropped: 22, Reactivated: 22},
		}}
	view := &View{Source: sourceFactsWithSet(at, set), SourceReplica: "pod-leader"}
	reports := ReportChecks([][]Anomaly{nil, nil, nil, nil}, nil, view, at)
	var report *CheckReport
	for _, candidate := range reports {
		if candidate.Code == CheckSourceSetFlapping {
			report = &candidate
		}
	}
	if report == nil || report.Owner != OwnerPlatform || report.GroupBy != GroupByHour || report.Strategies != 44 || len(report.Groups) != 2 {
		t.Fatalf("report = %+v, want the platform's line over 44 strategy-returns in two hours", report)
	}
	if report.Groups[0].Key != "2026-09-21T19Z" || report.Groups[0].Strategies != 22 || len(report.Groups[0].Samples) != 2 ||
		report.Groups[1].Key != "2026-09-21T18Z" || report.Groups[0].Replicas[0] != "pod-leader" {
		t.Fatalf("groups = %+v, want newest hour first, each with its returns and samples", report.Groups)
	}
	if !strings.Contains(report.Line, "2 个小时") || !strings.Contains(report.Line, "44 条次") || !strings.Contains(report.Line, "3 条在宽限中") || !strings.Contains(report.Line, "本小时已回来 22 条") {
		t.Fatalf("line = %q", report.Line)
	}
	if !strings.Contains(report.Groups[0].Text, "22 条策略回到活动集") || !strings.Contains(report.Groups[0].Text, "7 分钟") {
		t.Fatalf("group text = %q", report.Groups[0].Text)
	}
	// No returns: no line.
	quiet := &View{Source: sourceFactsWithSet(at, &SourceSetFacts{Since: at, Hours: []SourceSetHour{{Hour: at.Truncate(time.Hour), Dropped: 2}}}), SourceReplica: "pod-leader"}
	for _, candidate := range ReportChecks([][]Anomaly{nil, nil, nil, nil}, nil, quiet, at) {
		if candidate.Code == CheckSourceSetFlapping {
			t.Fatalf("an account with no returns made a line: %+v", candidate)
		}
	}
}
