// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The plan every case below reads: a 3-point trigger window needing 2
// anomalies, a value of 50 or more anomalous, and a recovery after 1 window
// that misses the trigger, so the Level retains 3 positions - the record's
// window, [480 540 600]. The record is at 600, its history one point a minute
// before it; each expectation is worked out from those numbers, not read off
// the evaluator.
const classAt = 600

func classRecord(value string) contract.CanonicalRecordV2 {
	return contract.CanonicalRecordV2{RecordID: strings.Repeat("f", 64), SourceTime: classAt, BusinessID: "2",
		DimensionIdentity: contract.DimensionIdentityV2{Digest: strings.Repeat("c", 64)},
		Values:            map[string]json.RawMessage{"value": json.RawMessage(value)}, Dimensions: map[string]json.RawMessage{}, ReceivedTime: classAt}
}

// classHistory is the four points before the record, oldest first: an x is
// anomalous, a dot normal.
func classHistory(t *testing.T, detect string, marks string) []execution.StateHistoryPoint {
	t.Helper()
	if len(marks) != 4 {
		t.Fatalf("history %q is not four points", marks)
	}
	history := make([]execution.StateHistoryPoint, 0, 4)
	for index, mark := range marks {
		result := execution.LevelFactNormal
		if mark == 'x' {
			result = execution.LevelFactAnomalous
		}
		history = append(history, execution.StateHistoryPoint{RecordID: strings.Repeat(string(rune('a'+index)), 64),
			SourceTime: classAt - int64(4-index)*60,
			Levels:     []execution.StateLevelFact{{LevelID: 5, DetectFingerprint: detect, Result: result}}})
	}
	return history
}

func classOf(t *testing.T, value, marks string) execution.SeriesClass {
	t.Helper()
	plan := compiledWindow(t, 3, 2)
	request := requestFixtureAt(t, plan, classAt, []contract.CanonicalRecordV2{classRecord(value)},
		classHistory(t, plan.Levels().At(0).Fingerprints().Detect, marks))
	result, err := newEvaluator(t).Evaluate(context.Background(), request)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Plans) != 1 || len(result.Plans[0].StateResults) != 1 {
		t.Fatalf("history %s value %s: %d state results, want the one series", marks, value, len(result.Plans[0].StateResults))
	}
	return result.Plans[0].StateResults[0].Class
}

// Each pair is one anomalous point apart and lands in neighbouring classes.
// The history marks 360 420 480 540; the retained positions, the only ones
// the trigger and the recovery walk read, are 480 540 600.
func TestASeriesIsClassedByWhatItsWindowsDecided(t *testing.T) {
	for _, c := range []struct {
		name, value, marks string
		want               execution.SeriesClass
	}{
		// Nothing anomalous anywhere: the walk's window misses and the
		// RECOVERY it decides restates a quiet series.
		{"quiet", "10", "....", execution.SeriesClassStableNormal},
		// One anomaly, at 540, in the record's window: 1 of the 2 it needs.
		{"one short of the trigger", "10", "...x", execution.SeriesClassCountingTrigger},
		// The record anomalous beside 540: 2 of 2, ABNORMAL.
		{"triggering", "80", "...x", execution.SeriesClassTransition},
		// 480 and 540 anomalous, the record normal: the record's own window
		// still meets the trigger, so the walk stops on it, open.
		{"still triggering, now normal", "10", "..xx", execution.SeriesClassCountingRecovery},
		// 420 and 480 anomalous, the round after [420 480 540] triggered:
		// the record's window holds 480 alone, a miss, and the RECOVERY is
		// decided with an anomaly still retained.
		{"recovered, an anomaly retained", "10", ".xx.", execution.SeriesClassCountingTrigger},
		// One anomaly fewer, 420 alone: it is past the retained positions,
		// and nothing the Level reads holds an anomaly.
		{"an anomaly past retention", "10", ".x..", execution.SeriesClassStableNormal},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := classOf(t, c.value, c.marks); got != c.want {
				t.Fatalf("history %s value %s classed %s, want %s", c.marks, c.value, got, c.want)
			}
		})
	}
}

// A series with no history decides nothing while its window fills: warming.
func TestAWarmingSeriesIsClassedWarming(t *testing.T) {
	plan := compiledWindow(t, 3, 2)
	request := requestFixtureAt(t, plan, classAt, []contract.CanonicalRecordV2{classRecord("10")}, nil)
	request.State.Items[0].Status = execution.StateMissingWarming
	request.State.Items[0].BlobRevision = 0
	request.State.Items[0].Levels = nil
	result, err := newEvaluator(t).Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Plans[0].StateResults[0].Class; got != execution.SeriesClassWarming {
		t.Fatalf("a series with one point classed %s, want warming", got)
	}
}

// A gapped history inside its hours decides nothing: gapped. The same series
// out of its hours is not evaluated at all: other.
func TestAGappedSeriesIsGappedAndOneOutOfItsHoursIsOther(t *testing.T) {
	for outOfHours, want := range map[bool]execution.SeriesClass{false: execution.SeriesClassGapped, true: execution.SeriesClassOther} {
		result, err := newEvaluator(t).Evaluate(context.Background(), gappedGuardRequest(t, outOfHours))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Plans[0].StateResults) != 1 {
			t.Fatalf("out of hours %v: %d state results", outOfHours, len(result.Plans[0].StateResults))
		}
		if got := result.Plans[0].StateResults[0].Class; got != want {
			t.Fatalf("out of hours %v classed %s, want %s", outOfHours, got, want)
		}
	}
}

// Two records of one series in one Slot are one write, and the write takes
// the higher class: an ABNORMAL first and a NORMAL after it is a transition,
// whichever came last. Two quiet records are stable_normal - the class is the
// highest of the records, not the first or the last.
func TestASeriesReadTwiceInOneSlotTakesTheHigherClass(t *testing.T) {
	plan := compiledWindow(t, 1, 1)
	record := func(id string, at int64, value string) contract.CanonicalRecordV2 {
		return contract.CanonicalRecordV2{RecordID: strings.Repeat(id, 64), SourceTime: at, BusinessID: "2",
			DimensionIdentity: contract.DimensionIdentityV2{Digest: strings.Repeat("c", 64)},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(value)}, Dimensions: map[string]json.RawMessage{}, ReceivedTime: at}
	}
	for _, c := range []struct {
		first, second string
		want          execution.SeriesClass
	}{
		{"80", "10", execution.SeriesClassTransition},
		{"10", "10", execution.SeriesClassStableNormal},
	} {
		t.Run(fmt.Sprintf("%s then %s", c.first, c.second), func(t *testing.T) {
			request := requestFixtureAt(t, plan, classAt, []contract.CanonicalRecordV2{record("d", 540, c.first), record("e", 600, c.second)}, nil)
			result, err := newEvaluator(t).Evaluate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			states := result.Plans[0].StateResults
			if len(states) != 1 || len(states[0].Mutation.AffectedRecords) != 2 {
				t.Fatalf("state results %+v, want one write for both records", states)
			}
			if states[0].Class != c.want {
				t.Fatalf("records %s, %s classed %s, want %s", c.first, c.second, states[0].Class, c.want)
			}
		})
	}
}

// A synthetic no-data series says so on its write; a real one does not.
func TestANoDataSeriesWriteIsMarkedNoData(t *testing.T) {
	plan := noDataCompiled(t, 1)
	result, err := newEvaluator(t).Evaluate(context.Background(), noDataRequestFixture(t, plan, 1))
	if err != nil {
		t.Fatal(err)
	}
	if states := result.Plans[0].StateResults; len(states) != 1 || !states[0].NoData {
		t.Fatalf("no-data series state results %+v, want one marked no-data", states)
	}
	real, err := newEvaluator(t).Evaluate(context.Background(), requestFixture(t, json.RawMessage(`10`), nil))
	if err != nil {
		t.Fatal(err)
	}
	if states := real.Plans[0].StateResults; len(states) != 1 || states[0].NoData {
		t.Fatalf("real series state results %+v, want one not marked no-data", states)
	}
}

// A recovery after 2 windows retains 4 positions, [420 480 540 600], one more
// than the trigger window [480 540 600]: an anomaly at 420 is past the
// trigger window and still in the positions the recovery walk read, so the
// series is not stable - its retained history holds an anomaly a write that
// left it alone would have to keep. One point later, 360, it is past both.
func TestAnAnomalyTheRecoveryWalkStillReadsKeepsASeriesOutOfStable(t *testing.T) {
	plan := compiledWindowEdited(t, 3, 2, nil, false, func(p *contract.EvaluationPlanV2) {
		p.StrategyIR.Levels[0].RecoveryPlan.Config = json.RawMessage(`{"enabled":true,"consecutive_windows":2}`)
	})
	if got := plan.Levels().At(0).StateRequirement().RetentionPoints; got != 4 {
		t.Fatalf("the Level retains %d positions, want the 4 this case is built on", got)
	}
	for marks, want := range map[string]execution.SeriesClass{
		".x..": execution.SeriesClassCountingTrigger,
		"x...": execution.SeriesClassStableNormal,
	} {
		request := requestFixtureAt(t, plan, classAt, []contract.CanonicalRecordV2{classRecord("10")},
			classHistory(t, plan.Levels().At(0).Fingerprints().Detect, marks))
		result, err := newEvaluator(t).Evaluate(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if got := result.Plans[0].StateResults[0].Class; got != want {
			t.Fatalf("history %s classed %s, want %s", marks, got, want)
		}
	}
}

// A recovery is decided before the completeness gate (decision-022), so a
// window with a hole and no anomaly decides RECOVERY on a history that is not
// whole: a 5-point window needing 3 anomalies, one position missing - 0
// anomalies and 1 hole is a miss. That history is not a steady series. The
// same window with the hole filled by a normal point is.
func TestARecoveryOnAHistoryWithAHoleIsNotStable(t *testing.T) {
	plan := compiledWindow(t, 5, 3)
	detect := plan.Levels().At(0).Fingerprints().Detect
	point := func(id string, at int64) execution.StateHistoryPoint {
		return execution.StateHistoryPoint{RecordID: strings.Repeat(id, 64), SourceTime: at,
			Levels: []execution.StateLevelFact{{LevelID: 5, DetectFingerprint: detect, Result: execution.LevelFactNormal}}}
	}
	for name, c := range map[string]struct {
		history []execution.StateHistoryPoint
		stable  bool
	}{
		"a hole at 480": {[]execution.StateHistoryPoint{point("a", 360), point("b", 420), point("d", 540)}, false},
		"no hole":       {[]execution.StateHistoryPoint{point("a", 360), point("b", 420), point("c", 480), point("d", 540)}, true},
	} {
		t.Run(name, func(t *testing.T) {
			request := requestFixtureAt(t, plan, classAt, []contract.CanonicalRecordV2{classRecord("10")}, c.history)
			result, err := newEvaluator(t).Evaluate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if got := result.Plans[0].LevelOutcomes[0].Outcome; got != execution.LevelOutcomeRecovery {
				t.Fatalf("outcome %s, want the RECOVERY the miss decides", got)
			}
			got := result.Plans[0].StateResults[0].Class
			switch {
			case c.stable && got != execution.SeriesClassStableNormal:
				t.Fatalf("a whole quiet window classed %s, want stable_normal", got)
			case !c.stable && got != execution.SeriesClassGapped && got != execution.SeriesClassWarming:
				t.Fatalf("a window with a hole classed %s, want gapped or warming", got)
			}
		})
	}
}
