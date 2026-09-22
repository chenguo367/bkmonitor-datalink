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
	"strings"
	"testing"
	"time"
)

var (
	refA = StrategyRef{StrategyID: "4101", BusinessID: "7"}
	refB = StrategyRef{StrategyID: "4102", BusinessID: "7"}
	refC = StrategyRef{StrategyID: "4103", BusinessID: "7"}
)

// heldGuard is one held gap scope of a Plan, flat for longer than the
// stall bound, on a Plan that bound the given number of series.
func heldGuard(plan StrategyRef, matched int) GapGuard {
	return GapGuard{Plan: plan, Scope: "1", Status: "GAPPED", Required: 9, Observed: 0, Rounds: 20, UnchangedRounds: 19,
		SeriesMatched: matched, SeriesMatchedKnown: true}
}

// The evidence names one Plan only when every piece of it belongs to that
// Plan and the row lists it; anything else names none.
func TestTheEvidenceNamesOnePlanOrNone(t *testing.T) {
	three := []StrategyRef{refA, refB, refC}
	for name, testCase := range map[string]struct {
		row  Anomaly
		want StrategyRef
		one  bool
	}{
		"guards on one plan":            {Anomaly{Strategies: three, Guards: []GapGuard{heldGuard(refC, 0), heldGuard(refC, 0)}}, refC, true},
		"one plan bound no series":      {Anomaly{Strategies: three, PlanSeries: []PlanSeriesMatched{{Plan: refA, Matched: 8}, {Plan: refC, Matched: 0}}}, refC, true},
		"guards and series agree":       {Anomaly{Strategies: three, Guards: []GapGuard{heldGuard(refC, 0)}, PlanSeries: []PlanSeriesMatched{{Plan: refA, Matched: 8}, {Plan: refC}}}, refC, true},
		"guards on two plans":           {Anomaly{Strategies: three, Guards: []GapGuard{heldGuard(refB, 3), heldGuard(refC, 0)}}, StrategyRef{}, false},
		"two plans bound no series":     {Anomaly{Strategies: three, PlanSeries: []PlanSeriesMatched{{Plan: refB}, {Plan: refC}}}, StrategyRef{}, false},
		"guards and series disagree":    {Anomaly{Strategies: three, Guards: []GapGuard{heldGuard(refB, 3)}, PlanSeries: []PlanSeriesMatched{{Plan: refC}}}, StrategyRef{}, false},
		"no evidence":                   {Anomaly{Strategies: three, PlanSeries: []PlanSeriesMatched{{Plan: refA, Matched: 8}}}, StrategyRef{}, false},
		"named plan the row lacks":      {Anomaly{Strategies: []StrategyRef{refA, refB}, Guards: []GapGuard{heldGuard(refC, 0)}}, StrategyRef{}, false},
		"the only plan, trivially":      {Anomaly{Strategies: []StrategyRef{refA}, Guards: []GapGuard{heldGuard(refA, 0)}}, refA, true},
		"every series present, no hold": {Anomaly{Strategies: three}, StrategyRef{}, false},
	} {
		got, one := implicatedStrategy(testCase.row)
		if one != testCase.one || got != testCase.want {
			t.Errorf("%s: implicated = %+v/%v, want %+v/%v", name, got, one, testCase.want, testCase.one)
		}
	}
}

// One object running three Plans, stalled because one of them bound no
// series: the row folds onto that strategy alone, whose words are the
// data's; the two neighbours get no line from it. The same row under a
// check about the object's whole round folds onto all three. And the
// check's strategy group is the implicated Plan, not the smallest id.
func TestARowAboutOnePlanIsNotEveryStrategysLine(t *testing.T) {
	three := []StrategyRef{refA, refB, refC}
	about := Anomaly{QueryGroup: "qg-shared", Finding: Finding{Check: CheckSeriesDataMissing}, Since: now.Add(-time.Hour), SinceFrom: SinceBusinessState,
		ReasonLastAt: now, Strategies: three, Guards: []GapGuard{heldGuard(refC, 0), heldGuard(refC, 0)},
		PlanSeries: []PlanSeriesMatched{{Plan: refA, Matched: 8}, {Plan: refB, Matched: 823}, {Plan: refC, Matched: 0}},
		Coverage:   &HistoryCoverage{Levels: 30, Short: 4, WorstValid: 7, WorstRequired: 9, UnchangedRounds: 1}}
	lines := StrategyLines(&View{Anomalies: []Anomaly{about}}, now)
	if len(lines) != 1 || lines[0].StrategyID != refC.StrategyID {
		t.Fatalf("lines = %+v, want the one strategy the guards belong to", lines)
	}
	standing := lines[0].Standing
	if standing.State != StateDataAbsent || standing.Action != ActionDataCheck || standing.RefinedBy != RuleStalled ||
		standing.About == nil || *standing.About != refC {
		t.Fatalf("standing = %+v, want the data's words, stalled, about the Plan that bound no series", standing)
	}
	if !strings.HasPrefix(lines[0].Line, "策略 4103 · 1 个对象 · 数据没到 · 数据负责人查") {
		t.Fatalf("line reads %q", lines[0].Line)
	}
	if got := groupKeyOf(about, CheckSeriesDataMissing); got != refC.StrategyID {
		t.Fatalf("group key = %q, want the implicated Plan, not the smallest id", got)
	}
	// The whole object's round lost: every strategy's, guards or not.
	whole := about
	whole.Finding = Finding{Check: CheckDependencyDown}
	lines = StrategyLines(&View{Anomalies: []Anomaly{whole}}, now)
	if len(lines) != 3 {
		t.Fatalf("lines under a whole-round check = %+v, want all three strategies", lines)
	}
	for _, line := range lines {
		if line.Standing.About != nil {
			t.Fatalf("line %s carries About %+v under a whole-round check", line.StrategyID, line.Standing.About)
		}
	}
	// Evidence that names no single Plan keeps the row every strategy's,
	// with no About: the fold does not guess.
	split := about
	split.Guards = []GapGuard{heldGuard(refB, 3), heldGuard(refC, 0)}
	split.PlanSeries = nil
	lines = StrategyLines(&View{Anomalies: []Anomaly{split}}, now)
	if len(lines) != 3 || lines[0].Standing.About != nil {
		t.Fatalf("lines with guards on two Plans = %+v, want all three strategies and no About", lines)
	}
	if got := groupKeyOf(split, CheckSeriesDataMissing); got != refA.StrategyID {
		t.Fatalf("group key with no implicated Plan = %q, want the smallest id", got)
	}
	// The card for a neighbour says whose the words are.
	card := StrategyStanding{StrategyID: refB.StrategyID, Standing: StandingDetecting, Found: true,
		Plans: []StrategyPlanStanding{{StrategyPlanRef: StrategyPlanRef{QueryGroup: "qg-shared"}, Replica: "pod-a", Existence: "active",
			Rows: []Anomaly{withStanding(about)}}}}
	if line := strategyStandingLine(card); !strings.Contains(line, "在检测；同对象上策略 4103 的：数据没到·数据负责人查") {
		t.Fatalf("neighbour's card reads %q", line)
	}
	own := card
	own.StrategyID = refC.StrategyID
	if line := strategyStandingLine(own); !strings.Contains(line, "持有，数据没到·数据负责人查）") || strings.Contains(line, "同对象") {
		t.Fatalf("own card reads %q", line)
	}
}

// A historical row -- a loss the object has run past -- never decides a
// strategy's line over a current row, whatever the two checks' ranks; a
// strategy with only historical rows sorts after every current one.
func TestAPastLossDoesNotOutrankAPresentRow(t *testing.T) {
	historical := Anomaly{QueryGroup: "qg-one", Finding: Finding{Check: CheckDetectionAbandoned}, Loss: LossHistorical,
		Since: now.Add(-3 * time.Hour), SinceFrom: SinceProcessStart, LastHealthyAt: now.Add(-2 * time.Hour), Strategies: []StrategyRef{refA}}
	current := Anomaly{QueryGroup: "qg-one", Finding: Finding{Check: CheckWindowUndecided}, Since: now.Add(-time.Hour), SinceFrom: SinceBusinessState,
		ReasonLastAt: now, Strategies: []StrategyRef{refA}, CauseReason: "CONFIG_DRIFT",
		Coverage: &HistoryCoverage{Levels: 249, Short: 249, WorstValid: 14, WorstRequired: 1469, Guarded: 249}}
	onlyPast := Anomaly{QueryGroup: "qg-two", Finding: Finding{Check: CheckDetectionAbandoned}, Loss: LossHistorical,
		Since: now.Add(-3 * time.Hour), SinceFrom: SinceProcessStart, Strategies: []StrategyRef{refB}}
	if checkRank(CheckDetectionAbandoned) >= checkRank(CheckWindowUndecided) {
		t.Fatalf("the fixture needs the historical check to outrank the current one in the table; ranks %d vs %d",
			checkRank(CheckDetectionAbandoned), checkRank(CheckWindowUndecided))
	}
	lines := StrategyLines(&View{Anomalies: []Anomaly{historical, onlyPast, current}}, now)
	if len(lines) != 2 {
		t.Fatalf("lines = %+v", lines)
	}
	first := lines[0]
	if first.StrategyID != refA.StrategyID || first.Standing.Check != CheckWindowUndecided || first.Standing.State == StateRecovered {
		t.Fatalf("first line = %+v, want the current undecided window deciding, not the recovered loss", first)
	}
	if first.Objects != 1 || first.LastGoodAt == nil || !first.LastGoodAt.Equal(now.Add(-2*time.Hour)) {
		t.Fatalf("first line clocks/objects = %+v, want the historical row still counted for its object and its last good time", first)
	}
	if lines[1].StrategyID != refB.StrategyID || lines[1].Standing.RefinedBy != RuleHistoricalLoss {
		t.Fatalf("second line = %+v, want the strategy with only a past loss, after the current one", lines[1])
	}
}

// withStanding gives a row the standing walkObjectRows would.
func withStanding(row Anomaly) Anomaly {
	standing := standingOf(row)
	row.Standing = &standing
	return row
}
