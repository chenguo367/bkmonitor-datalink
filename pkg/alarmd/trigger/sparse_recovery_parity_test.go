// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package trigger

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// sparsePoint is one point of a series: its source time and whether its
// detection was anomalous.
type sparsePoint struct {
	time      int64
	anomalous bool
}

// evaluateAtV2 evaluates the record at points[index] the way the production
// path does: every point up to and including it is in the history, and the
// Slot it is read in is the point's own minute, frozen with hold. The series
// is new to this round unless an earlier point exists, as a series whose
// first record this is.
func evaluateAtV2(t *testing.T, plan *strategy.CompiledPlan, points []sparsePoint, index int, evaluationTime, hold int64) EvaluationResultV2 {
	return evaluateSeenAtV2(t, plan, points, index, evaluationTime, hold, index > 0)
}

func evaluateSeenAtV2(t *testing.T, plan *strategy.CompiledPlan, points []sparsePoint, index int, evaluationTime, hold int64, seen bool) EvaluationResultV2 {
	t.Helper()
	level := plan.Levels().At(0)
	history := pointHistory{step: 60, points: make(map[int64]bool, index+1)}
	for _, point := range points[:index+1] {
		history.points[point.time] = point.anomalous
	}
	result := DetectionNormal
	if points[index].anomalous {
		result = DetectionAnomalous
	}
	request := requestV2(t, plan, points[index].time, []DetectionFact{factV2(level, result)},
		[]LevelHistory{{LevelID: level.Definition().LevelID, View: history}}, activeFactsV2(t, plan, evaluationTime))
	request.EvaluationTime, request.ReadHoldMillis, request.SeriesSeenBefore = evaluationTime, hold, seen
	evaluated, err := EvaluateV2(request)
	if err != nil {
		t.Fatalf("EvaluateV2(%d) error = %v", points[index].time, err)
	}
	return evaluated
}

func recoveredV2(result EvaluationResultV2) bool {
	return result.RecordResult == contract.LevelResultRecovery
}

type pythonRecoveryCase struct {
	ID       string `json:"id"`
	Window   uint32 `json:"window"`
	Required uint32 `json:"required"`
	Recovery uint32 `json:"recovery"`
	OpenedAt *int64 `json:"opened_at"`
	Points   []struct {
		Time      int64 `json:"time"`
		Anomalous bool  `json:"anomalous"`
		Recovered bool  `json:"recovered"`
	} `json:"points"`
	// Lifecycle is the alert the first trigger opened, checked every minute
	// as check_recovery does: RECOVERED or CLOSED_NO_DATA at a minute, or
	// NEITHER by the end of the case.
	Lifecycle pythonLifecycle `json:"lifecycle"`
	// LifecycleRecoveryNoData is the same alert under a "recovery-nodata"
	// status setter, which closes it for no data (recover.py:315-321). Not
	// compared: alarmd ports no no-data close. Logged beside the outcome.
	LifecycleRecoveryNoData pythonLifecycle `json:"lifecycle_recovery_nodata"`
}

type pythonLifecycle struct {
	Outcome string `json:"outcome"`
	At      int64  `json:"at"`
}

const (
	pythonRecovered = "RECOVERED"
	pythonNeither   = "NEITHER"
)

// pythonRecoveryCases reads the oracle generate_sparse_recovery_python.py
// wrote by executing Python's own check_result_cache, from the pinned
// revision it names, on each case.
func pythonRecoveryCases(t *testing.T) map[string]pythonRecoveryCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/sparse_recovery_python.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Revision string               `json:"revision"`
		Cases    []pythonRecoveryCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Revision == "" || len(fixture.Cases) == 0 {
		t.Fatal("the Python oracle names no revision or holds no case")
	}
	cases := make(map[string]pythonRecoveryCase, len(fixture.Cases))
	for _, item := range fixture.Cases {
		cases[item.ID] = item
	}
	return cases
}

// pythonBase is the oracle's zero, minute 1000, so a logged time is seconds
// past it.
const pythonBase = int64(1000 * 60)

func (life pythonLifecycle) describe() string {
	if life.Outcome == pythonNeither {
		return life.Outcome
	}
	return life.Outcome + " at " + strconv.FormatInt(life.At-pythonBase, 10)
}

func (item pythonRecoveryCase) series() []sparsePoint {
	points := make([]sparsePoint, len(item.Points))
	for index, point := range item.Points {
		points[index] = sparsePoint{time: point.Time, anomalous: point.Anomalous}
	}
	return points
}

// compareWithPython evaluates every point of the case once its alert is open
// - Python checks recovery only for an open alert - and returns the times
// alarmd recovered at. At each point alarmd must recover exactly when
// Python's check_result_cache does at that point. Each point is evaluated
// twice, with the series' earlier state known and with only the retained
// positions to go on, and the two must agree.
//
// Then the case as a whole, against the alert Python's minute loop keeps
// under the default status setter: RECOVERED at a minute, alarmd recovers
// first at that minute; NEITHER, alarmd does not recover. The oracle has no
// CLOSED_NO_DATA under that setter, and alarmd ports no no-data close.
func compareWithPython(t *testing.T, item pythonRecoveryCase) (compared int, recovered []int64) {
	t.Helper()
	if item.OpenedAt == nil {
		return 0, nil
	}
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, item.Window, item.Required, item.Recovery, nil)})
	points := item.series()
	for index, point := range item.Points {
		if point.Time < *item.OpenedAt {
			continue
		}
		got := recoveredV2(evaluateSeenAtV2(t, plan, points, index, point.Time, 0, true))
		if got != point.Recovered {
			t.Fatalf("%s at %d: alarmd recovered = %t, Python recovered = %t", item.ID, point.Time, got, point.Recovered)
		}
		if fresh := recoveredV2(evaluateSeenAtV2(t, plan, points, index, point.Time, 0, false)); fresh != got {
			t.Fatalf("%s at %d: recovered = %t from the retained positions alone and %t with the state known",
				item.ID, point.Time, fresh, got)
		}
		compared++
		if got {
			recovered = append(recovered, point.Time)
		}
	}
	switch item.Lifecycle.Outcome {
	case pythonRecovered:
		if len(recovered) == 0 || recovered[0] != item.Lifecycle.At {
			t.Fatalf("%s: Python RECOVERED at %d, alarmd recovered at %v", item.ID, item.Lifecycle.At, recovered)
		}
	case pythonNeither:
		if len(recovered) > 0 {
			t.Fatalf("%s: Python neither recovered nor closed, alarmd recovered at %v", item.ID, recovered)
		}
	default:
		t.Fatalf("%s: oracle outcome %q", item.ID, item.Lifecycle.Outcome)
	}
	return compared, recovered
}

// The traced case: one trigger anomaly in five minutes, five
// recovery windows, an anomalous point at :42 and normal ones at :47 and :56.
// No window of five minutes ever holds five points, so before settled holes
// counted as non-anomalous every window was stepped over and the alert never
// closed. Python closes it at :56, the first point whose five windows all
// start after :42.
func TestTheTracedSparseCaseRecoversAtTheSameEvaluationAsPython(t *testing.T) {
	item := pythonRecoveryCases(t)["exact_42_47_56"]
	base := int64(1000 * 60)
	if len(item.Points) != 3 || item.Points[0].Time != base+42*60 || item.Points[2].Time != base+56*60 {
		t.Fatalf("oracle case = %+v, want points at :42, :47 and :56", item)
	}
	if item.Points[1].Recovered || !item.Points[2].Recovered {
		t.Fatalf("Python recovers %+v, want at :56 and not at :47", item.Points)
	}
	_, recovered := compareWithPython(t, item)
	if len(recovered) != 1 || recovered[0] != base+56*60 {
		t.Fatalf("alarmd recovered at %v, want only at :56", recovered)
	}
}

// The shape a real deployment reports: a point every five minutes under a
// five minute window, so exactly four holes between points and never a whole
// window. Python recovers at the first point whose windows all start after
// the anomaly, which is ten minutes after it; alarmd must do the same, at the
// same evaluation, for both anomalies of the series.
func TestASeriesReportingEveryFiveMinutesRecoversAsPythonDoes(t *testing.T) {
	item := pythonRecoveryCases(t)["every_five_minutes"]
	if item.Window != 5 || item.Required != 1 || item.Recovery != 5 {
		t.Fatalf("oracle case = %+v, want window 5, one anomaly, five recovery windows", item)
	}
	compared, recovered := compareWithPython(t, item)
	base := int64(1000 * 60)
	// Anomalies at minutes 0 and 15: the first closes at 10, the second at 25,
	// and every normal point after 25 still reads recovered.
	if compared != len(item.Points) || len(recovered) == 0 || recovered[0] != base+10*60 {
		t.Fatalf("compared %d of %d, alarmd recovered at %v, want first at minute 10", compared, len(item.Points), recovered)
	}
	afterSecond := []int64{}
	for _, at := range recovered {
		if at > base+15*60 {
			afterSecond = append(afterSecond, at)
		}
	}
	if len(afterSecond) == 0 || afterSecond[0] != base+25*60 {
		t.Fatalf("after the second anomaly alarmd recovered at %v, want first at minute 25", afterSecond)
	}
}

// Every case of the oracle: a point every k minutes for k = 1..9 under three
// trigger and recovery shapes, with the anomalies placed five ways. At every
// point after the alert opens, alarmd recovers exactly when Python's own
// check_result_cache does. k = 1 is the dense series, in the same sweep.
func TestSparseSeriesRecoverAtTheEvaluationsPythonDoes(t *testing.T) {
	cases := pythonRecoveryCases(t)
	total, recoveries, sparseRecoveries := 0, 0, 0
	ids := make([]string, 0, len(cases))
	for id := range cases {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		item := cases[id]
		compared, recovered := compareWithPython(t, item)
		first := "none"
		if len(recovered) > 0 {
			first = strconv.FormatInt(recovered[0]-pythonBase, 10)
		}
		t.Logf("%s | python %s | python with recovery-nodata %s | alarmd first RECOVERY %s",
			id, item.Lifecycle.describe(), item.LifecycleRecoveryNoData.describe(), first)
		total += compared
		recoveries += len(recovered)
		if len(recovered) > 0 && len(item.Points) > 1 && item.Points[1].Time-item.Points[0].Time >= int64(item.Window)*60 {
			sparseRecoveries++
		}
	}
	// The sweep must have something to say: points compared, and recoveries
	// on series sparser than their window, which are the ones this rule is for.
	if total < 1000 || recoveries == 0 || sparseRecoveries < 10 {
		t.Fatalf("compared %d points, %d recoveries, %d sparse cases recovered: the oracle no longer exercises the rule",
			total, recoveries, sparseRecoveries)
	}
}

// Inside the read hold a hole may still fill, so a window there keeps the
// skip rule, and once the hold has passed a late anomalous point that landed
// in it blocks the recovery it would have pre-empted.
//
// Window 5, one anomaly, five recovery windows, a point every five minutes,
// and a three minute hold: at :50 the windows ending :48 to :50 are inside the
// hold and stepped over, three settled ones are counted, and the fourth meets
// the anomaly at :40 - no recovery, where with no hold alarmd and Python both
// recover at :50. An anomalous point for :48 then arrives late. At :55 the
// newest settled window, ending :52, holds it and stops the walk. At :60 every
// window counted starts after :48 and the alert closes, as Python's does.
func TestARecoveryInsideTheReadHoldWaitsAndALateAnomalyBlocksIt(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	const hold = int64(3 * 60 * 1000)
	minute := func(m int64) int64 { return 6000 + m*60 }
	before := []sparsePoint{{minute(40), true}, {minute(45), false}, {minute(50), false}}

	if !recoveredV2(evaluateAtV2(t, plan, before, 2, minute(50), 0)) {
		t.Fatal("with no hold :50 does not recover; the fixture does not separate the two rules")
	}
	held := evaluateAtV2(t, plan, before, 2, minute(50), hold)
	if recoveredV2(held) {
		t.Fatalf("at :50 inside a three minute hold alarmd recovered: holes the hold may still fill were read as normal")
	}
	if recovery := held.LevelOutcomes[0].DecisionWindow; recovery != nil && recovery.Recovery.SkippedWindows != 3 {
		t.Fatalf("recovery evidence = %+v, want the three windows inside the hold stepped over", recovery.Recovery)
	}

	late := []sparsePoint{{minute(40), true}, {minute(45), false}, {minute(48), true}, {minute(50), false}, {minute(55), false}, {minute(60), false}}
	if recoveredV2(evaluateAtV2(t, plan, late, 4, minute(55), hold)) {
		t.Fatal("at :55 alarmd recovered over the late anomaly at :48 once it was settled")
	}
	if !recoveredV2(evaluateAtV2(t, plan, late, 5, minute(60), hold)) {
		t.Fatal("at :60 every counted window starts after :48 and alarmd still did not recover")
	}
}

// A dense series has no holes once its alert is open, so the settled rule and
// the skip rule read it the same: from the anomaly on, its recoveries fall on
// the same points whether every window is settled or every window is inside
// the hold, which is how the walk read it before. Window 5, one anomaly, five
// recovery windows, a point a minute, anomalies at minutes 3 and 4: the first
// point whose five windows all start after minute 4 is minute 13.
//
// Before its first anomaly the two rules differ for a series whose state is
// known, on windows reaching back past its first point: those minutes are
// settled and read as quiet, as Python's check would read them. No alert is
// open there, so no alert closes, and the comparison starts at the anomaly.
func TestADenseSeriesRecoversAtTheSamePointAsBefore(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	points := make([]sparsePoint, 20)
	for index := range points {
		points[index] = sparsePoint{time: 6000 + int64(index)*60, anomalous: index == 3 || index == 4}
	}
	first := -1
	for index := 3; index < len(points); index++ {
		settled := evaluateAtV2(t, plan, points, index, points[index].time, 0)
		unsettled := evaluateAtV2(t, plan, points, index, points[index].time, execution.MaxReadHoldMillis)
		if settled.RecordResult != unsettled.RecordResult || settled.Completion != unsettled.Completion {
			t.Fatalf("minute %d: settled reads %q/%s, inside the hold %q/%s: a dense series changed timing",
				index, settled.RecordResult, settled.Completion, unsettled.RecordResult, unsettled.Completion)
		}
		if index > 4 && first < 0 && recoveredV2(settled) {
			first = index
		}
	}
	if first != 13 {
		t.Fatalf("dense series first recovered at minute %d, want 13", first)
	}
}

// NORMAL still needs a FULL window. A sparse series never has one: with
// recovery off it reports its history short at every point, and with recovery
// on it reads RECOVERY or a short history, never NORMAL.
func TestASparseSeriesIsNeverNormal(t *testing.T) {
	points := make([]sparsePoint, 12)
	for index := range points {
		points[index] = sparsePoint{time: 6000 + int64(index)*7*60}
	}
	for _, level := range []contract.LevelIRV2{levelWithoutRecoveryV2(5, 1, 5, 1), levelV2(5, 1, 5, 2, 5, nil)} {
		plan := compilePlanV2(t, []contract.LevelIRV2{level})
		series := append([]sparsePoint(nil), points...)
		if plan.Levels().At(0).Recovery().Enabled {
			// One anomaly, under the threshold of two, so the Level never
			// reads ABNORMAL either.
			series[len(series)-1].anomalous = true
		}
		for index := range series {
			result := evaluateAtV2(t, plan, series, index, series[index].time, 0)
			if result.RecordResult == contract.LevelResultNormal {
				t.Fatalf("a sparse series read NORMAL at point %d: %+v", index, result.LevelOutcomes[0])
			}
		}
	}
}

// Python gives up when its whole range holds no check result
// (recover.py:429-431). A history with a point long before the range and
// nothing in it - only reachable here, the production path always holds the
// record's own point - must not recover from windows read out of nothing.
func TestARecoveryWalkOverNoPointIsNotARecovery(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	level := plan.Levels().At(0)
	const source = int64(60000)
	history := pointHistory{step: 60, points: map[int64]bool{source - 20*60: false}}
	request := requestV2(t, plan, source, []DetectionFact{factV2(level, DetectionNormal)},
		[]LevelHistory{{LevelID: 5, View: history}}, activeFactsV2(t, plan, source))
	result, err := EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredV2(result) {
		t.Fatal("alarmd recovered from a range that holds no point")
	}
	// One point at the oldest second Python reads is enough for it, and so
	// for alarmd.
	history.points[pythonRecoveryRangeStartV2(source, level.Trigger(), level.Recovery())] = false
	result, err = EvaluateV2(request)
	if err != nil {
		t.Fatal(err)
	}
	if !recoveredV2(result) {
		t.Fatal("a point at the start of Python's range did not let alarmd recover")
	}
}

func TestARequestWithAReadHoldOutOfRangeIsRefused(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	points := []sparsePoint{{time: 6000}}
	for _, hold := range []int64{-1, execution.MaxReadHoldMillis + 1} {
		request := requestV2(t, plan, 6000, []DetectionFact{factV2(plan.Levels().At(0), DetectionNormal)},
			[]LevelHistory{{LevelID: 5, View: pointHistory{step: 60, points: map[int64]bool{points[0].time: false}}}},
			activeFactsV2(t, plan, 6000))
		request.ReadHoldMillis = hold
		if _, err := EvaluateV2(request); err == nil {
			t.Fatalf("read hold %d was accepted", hold)
		}
	}
}

// A round of the Plan that was not read whole leaves a hole nobody read, and
// while its gap marker is open that round may be in any window the walk
// reads: none of them is settled. The traced case that recovers at :56 with
// the marker closed does not while it is open.
func TestAnOpenReadGapKeepsEveryWindowUnsettled(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	minute := func(m int64) int64 { return 6000 + m*60 }
	points := []sparsePoint{{minute(42), true}, {minute(47), false}, {minute(56), false}}
	evaluate := func(open bool) EvaluationResultV2 {
		level := plan.Levels().At(0)
		history := pointHistory{step: 60, points: map[int64]bool{}}
		for _, point := range points {
			history.points[point.time] = point.anomalous
		}
		request := requestV2(t, plan, minute(56), []DetectionFact{factV2(level, DetectionNormal)},
			[]LevelHistory{{LevelID: 5, View: history, ReadGapOpen: open}}, activeFactsV2(t, plan, minute(56)))
		request.SeriesSeenBefore = true
		result, err := EvaluateV2(request)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if !recoveredV2(evaluate(false)) {
		t.Fatal("with no read gap open :56 does not recover; the fixture does not separate the two")
	}
	if recoveredV2(evaluate(true)) {
		t.Fatal("with a read gap open alarmd recovered: a round nobody read was counted as a quiet minute")
	}
}

// Points every five minutes and five recovery windows, under a trigger
// window of two and of five: the two shapes production Python's recoveries
// were read from. The oracle recovers both ten minutes after the anomaly, the
// first point whose five windows all start after it, and alarmd recovers at
// that same point.
func TestEveryFiveMinutesUnderWindowsOfTwoAndFiveRecoversAtPythonsMinute(t *testing.T) {
	cases := pythonRecoveryCases(t)
	for _, id := range []string{"every_five_minutes_window_2", "every_five_minutes_window_5"} {
		item := cases[id]
		if item.Lifecycle.Outcome != pythonRecovered || item.OpenedAt == nil {
			t.Fatalf("%s: oracle = %+v, want a RECOVERED alert", id, item.Lifecycle)
		}
		_, recovered := compareWithPython(t, item)
		if len(recovered) == 0 || recovered[0]-*item.OpenedAt != 10*60 {
			t.Fatalf("%s: alarmd recovered at %v, opened at %d, want ten minutes after the anomaly as Python",
				id, recovered, *item.OpenedAt)
		}
	}
}

// The whole-range-empty branch, recover.py:429-431: an open alert whose
// recovery range, as Python bounds it, holds no present settled point is not
// recovered, and stays open - the no-data close is not ported. The alert
// opened at :00; at :20 the Slot is frozen with a two minute hold, so the
// record's own point is inside the hold, and Python's range :10 to :20 holds
// nothing else. Its pair is the same range with one present normal settled
// point, at :12, which recovers at :20 as Python's check does.
func TestAnOpenAlertWithNoSettledPointInItsRecoveryRangeStaysOpen(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	minute := func(m int64) int64 { return 6000 + m*60 }
	const hold = int64(2 * 60 * 1000)
	if start := pythonRecoveryRangeStartV2(minute(20), plan.Levels().At(0).Trigger(), plan.Levels().At(0).Recovery()); start != minute(10)+1 {
		t.Fatalf("Python's range starts at %d, want one second past :10", start)
	}
	empty := []sparsePoint{{minute(0), true}, {minute(20), false}}
	result := evaluateSeenAtV2(t, plan, empty, 1, minute(20), hold, true)
	if recoveredV2(result) {
		t.Fatal("alarmd recovered an alert whose recovery range holds no present settled point")
	}
	one := []sparsePoint{{minute(0), true}, {minute(12), false}, {minute(20), false}}
	if !recoveredV2(evaluateSeenAtV2(t, plan, one, 2, minute(20), hold, true)) {
		t.Fatal("with one present normal settled point in the range alarmd did not recover at :20")
	}
	// The same pair with no hold, so the record's own point is settled: it is
	// the one present point, and the alert recovers, as Python's does.
	if !recoveredV2(evaluateSeenAtV2(t, plan, empty, 1, minute(20), 0, true)) {
		t.Fatal("with the record's own point settled alarmd did not recover at :20")
	}
}

// A production alert's timing and flags: trigger window 2 needing 2, five
// recovery windows, anomalous points at :22, :23 and :32, a normal one at
// :33, nothing between. Python's check passes on the :32 point - the lone
// anomaly there is under the threshold of two, and every window is a miss -
// and alarmd, reading each point in the Slot a minute after it, recovers on
// that same point.
func TestAProductionShapedSeriesWithGapsRecoversAtPythonsMinute(t *testing.T) {
	item := pythonRecoveryCases(t)["production_w2_n2_r5"]
	if item.Lifecycle.Outcome != pythonRecovered || item.OpenedAt == nil {
		t.Fatalf("oracle = %+v, want a RECOVERED alert", item.Lifecycle)
	}
	compareWithPython(t, item)
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, item.Window, item.Required, item.Recovery, nil)})
	points := item.series()
	first := int64(-1)
	for index, point := range points {
		if point.time < *item.OpenedAt {
			continue
		}
		if recoveredV2(evaluateSeenAtV2(t, plan, points, index, point.time+60, 0, true)) {
			first = point.time
			break
		}
	}
	if first != item.Lifecycle.At {
		t.Fatalf("read a Slot after each point, alarmd first recovered on %d, Python at %d", first, item.Lifecycle.At)
	}
}

// The settled boundary's cost, pinned. A point every five minutes, window 5,
// one anomaly, five recovery windows; each point is read in the Slot a minute
// after it (a 60 s delay). Python recovers ten minutes after the anomaly. With
// a 96 s hold alarmd does too: one window is inside the hold and stepped over,
// and the five behind it still start after the anomaly at the next point.
// With a 600 s hold nine windows are inside it, and this Level retains nine
// positions, so no settled window is left to count: alarmd does not recover
// this series at all within the hour, where Python recovered at minute 10.
func TestTheReadHoldsCostOnASparseSeries(t *testing.T) {
	plan := compilePlanV2(t, []contract.LevelIRV2{levelV2(5, 1, 5, 1, 5, nil)})
	if retained := plan.Levels().At(0).StateRequirement().RetentionPoints; retained != 9 {
		t.Fatalf("the Level retains %d positions, want 9: the case reads off that bound", retained)
	}
	const delay = int64(60)
	points := []sparsePoint{}
	for m := int64(0); m <= 60; m += 5 {
		points = append(points, sparsePoint{time: pythonBase + m*60, anomalous: m == 0})
	}
	firstRecovery := func(holdSeconds int64) int64 {
		for index := 1; index < len(points); index++ {
			if recoveredV2(evaluateSeenAtV2(t, plan, points, index, points[index].time+delay, holdSeconds*1000, true)) {
				return (points[index].time - pythonBase) / 60
			}
		}
		return -1
	}
	const python = 10
	if got := firstRecovery(96); got != python {
		t.Fatalf("with a 96 s hold alarmd first recovered at minute %d, Python at %d", got, python)
	}
	if got := firstRecovery(600); got != -1 {
		t.Fatalf("with a 600 s hold alarmd first recovered at minute %d, want none within the hour (Python: %d)", got, python)
	}
}

// A gap marker clears after RequiredFullSlots whole Slots, the most history
// points any Level needs (W + R - 1), and the recovery walk reads positions on
// the trigger's step. The two count the same thing only if a Slot advances the
// window by one position, so a Plan detected more often than it aggregates -
// a 60 s detection over a 300 s aggregation - is the case to check.
//
// It does: the trigger's step must equal the evaluation interval, which is
// the Slot cadence (strategy compiler; controlplane planScheduleSpec holds the
// schedule to the same value). A 60 s interval over a 300 s aggregation runs
// its windows on 60 s positions, one per Slot, so W + R - 1 Slots are W + R - 1
// positions; a 300 s step under a 60 s interval is refused rather than run.
func TestAGapMarkerCountsSlotsInTheWalksOwnPositions(t *testing.T) {
	notTimeAligned := func(plan *contract.EvaluationPlanV2) {
		plan.StrategyIR.ExecutionSemantics.EvaluationInterval = 60
		plan.StrategyIR.ExecutionSemantics.AggregationInterval = 300
		plan.StrategyIR.ExecutionSemantics.QueryWindow = 300
	}
	level := levelV2(5, 1, 5, 1, 5, nil)
	plan := compilePlanV2WithOutput(t, []contract.LevelIRV2{level}, notTimeAligned)
	compiled := plan.Levels().At(0)
	if step := compiled.Trigger().StepSeconds; step != plan.EvaluationSemantics().EvaluationInterval {
		t.Fatalf("trigger step %d s under a %d s interval: a Slot would move the window by more or less than one position",
			step, plan.EvaluationSemantics().EvaluationInterval)
	}
	if points := compiled.RequiredDetectHistoryPoints(); points != 5+5-1 {
		t.Fatalf("required history %d positions, want W + R - 1 = 9 Slots of 60 s", points)
	}

	stepped := levelV2(5, 1, 5, 1, 5, nil)
	config := map[string]any{"window_size": 5, "required_anomalies": 1, "step_seconds": 300}
	stepped.TriggerPlan.Config = mustJSONV2(config)
	result := compileResultV2(t, []contract.LevelIRV2{stepped}, notTimeAligned)
	if _, ok := result.Plan(); ok && len(result.LevelTerminals()) == 0 {
		t.Fatal("a 300 s trigger step compiled under a 60 s evaluation interval: a marker counted in Slots would " +
			"clear while its unread bucket is still inside the walk")
	}
}
