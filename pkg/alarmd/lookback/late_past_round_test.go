// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// lateWindow ends one directed window of the fixture's series_late group:
// its Slot, the rung it was read at, the time_delay its query runs under,
// and what the window came to.
func lateWindow(f *fixture, evaluation int64, rung int, delaySeconds int64, outcome string, facts *execution.SupplementFacts) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	state := f.engine.groups["qg"]
	slot := &directedSlot{queryGroup: "qg", source: state.source, evaluation: execution.EvaluationTime(evaluation),
		step: state.step, rung: rung, queries: []*directedQuery{{spec: execution.PhysicalQuerySpec{
			PlanFacts: execution.QueryPlanFacts{QueryDelaySeconds: delaySeconds}}}}}
	f.engine.noteDirectedLocked(state, slot, outcome, "", facts)
}

func crossed(n int) *execution.SupplementFacts {
	return &execution.SupplementFacts{Candidates: n, CrossedT: n}
}

func mixed(admitted, crossedT int) *execution.SupplementFacts {
	return &execution.SupplementFacts{Candidates: admitted + crossedT, Admitted: admitted, Points: uint64(admitted), CrossedT: crossedT}
}

// Two supplemented windows in a row whose every late series had crossed its
// Slot report the Query Group, with a time_delay that reads those series:
// the one it runs under plus how long after the first read they were seen,
// aligned up to the step. One such window is not a report.
func TestAGroupWhoseLateSeriesCrossTheirSlotTwiceInARowIsReported(t *testing.T) {
	f, _ := directedFixture(t, SupplementOutcome{})
	lateWindow(f, 600, 1, 60, DirectedSupplemented, crossed(3))
	if readings := f.engine.LatePastRound(); len(readings) != 0 {
		t.Fatalf("one window reported: %+v", readings)
	}
	lateWindow(f, 660, 2, 60, DirectedSupplemented, crossed(2))
	readings := f.engine.LatePastRound()
	if len(readings) != 1 {
		t.Fatalf("readings %+v, want the group after two windows", readings)
	}
	reading := readings[0]
	// The deeper rung is the later sighting: 3.5 steps after the first read.
	want := int64(60) + int64(rungDelay(2, minute).Seconds())
	want = (want + 59) / 60 * 60
	if reading.CurrentDelaySeconds != 60 || reading.SuggestedDelaySeconds != want || reading.StepSeconds != 60 ||
		len(reading.Samples) != 2 || reading.Samples[1].CrossedSeries != 2 || reading.Samples[1].Rung != RungNames[2] {
		t.Fatalf("reading %+v, want current 60 and suggested %d from the samples", reading, want)
	}
	if residual := f.engine.ResidualMisses(); len(residual) != 0 {
		t.Fatalf("a window with nothing recovered counted as a residual miss: %+v", residual)
	}
}

// A window the supplement recovered anything in ends the report: the
// supplement is working, and a longer time_delay would slow the whole group
// for the few later still. Those few are the residual miss, counted with the
// window as evidence; a window with nothing late ends the report too.
func TestAWindowTheSupplementRecoveredPartOfIsAResidualMissAndEndsTheReport(t *testing.T) {
	f, _ := directedFixture(t, SupplementOutcome{})
	lateWindow(f, 600, 1, 60, DirectedSupplemented, crossed(3))
	lateWindow(f, 660, 1, 60, DirectedSupplemented, crossed(3))
	lateWindow(f, 720, 1, 60, DirectedSupplemented, mixed(4, 1))
	if readings := f.engine.LatePastRound(); len(readings) != 0 {
		t.Fatalf("still reported after a window the supplement recovered part of: %+v", readings)
	}
	lateWindow(f, 780, 1, 60, DirectedSupplemented, mixed(2, 3))
	residual := f.engine.ResidualMisses()
	if len(residual) != 1 || residual[0].Windows != 2 || residual[0].CrossedSeries != 4 || len(residual[0].Samples) != 2 ||
		residual[0].Samples[1].AdmittedSeries != 2 || residual[0].Samples[1].CrossedSeries != 3 {
		t.Fatalf("residual %+v, want two windows, four series not recovered, the latest window last", residual)
	}
	// Recovered everything: not a residual window.
	lateWindow(f, 840, 1, 60, DirectedSupplemented, mixed(5, 0))
	if residual := f.engine.ResidualMisses(); residual[0].Windows != 2 {
		t.Fatalf("a window recovered whole counted: %+v", residual)
	}
	lateWindow(f, 900, 1, 60, DirectedSupplemented, crossed(1))
	lateWindow(f, 960, 1, 60, DirectedNothingLate, nil)
	lateWindow(f, 1020, 1, 60, DirectedSupplemented, crossed(1))
	if readings := f.engine.LatePastRound(); len(readings) != 0 {
		t.Fatalf("a window with nothing late did not end the run: %+v", readings)
	}
}

// A window not read, or read and not supplemented, says nothing: it neither
// adds to a run nor ends one.
func TestAWindowNotSupplementedNeitherAddsToNorEndsTheRun(t *testing.T) {
	f, _ := directedFixture(t, SupplementOutcome{})
	lateWindow(f, 600, 1, 60, DirectedSupplemented, crossed(2))
	lateWindow(f, 660, 1, 60, DirectedUnobserved, nil)
	lateWindow(f, 720, 1, 60, DirectedFlightBusy, nil)
	if readings := f.engine.LatePastRound(); len(readings) != 0 {
		t.Fatalf("windows not supplemented added to the run: %+v", readings)
	}
	lateWindow(f, 780, 1, 60, DirectedSupplemented, crossed(2))
	if readings := f.engine.LatePastRound(); len(readings) != 1 {
		t.Fatalf("windows not supplemented ended the run: %+v", readings)
	}
}

// Both reports stand while the group is read directed, and go when its
// series are late no more.
func TestTheLateSeriesReportsGoWhenTheGroupIsNoLongerDirected(t *testing.T) {
	f, _ := directedFixture(t, SupplementOutcome{})
	lateWindow(f, 600, 1, 60, DirectedSupplemented, crossed(2))
	lateWindow(f, 660, 1, 60, DirectedSupplemented, crossed(2))
	lateWindow(f, 720, 1, 60, DirectedSupplemented, mixed(1, 1))
	lateWindow(f, 780, 1, 60, DirectedSupplemented, crossed(2))
	lateWindow(f, 840, 1, 60, DirectedSupplemented, crossed(2))
	if len(f.engine.LatePastRound()) != 1 || len(f.engine.ResidualMisses()) != 1 {
		t.Fatal("the fixture did not report both")
	}
	// lookback.get reads them beside read_early.
	if stats := f.engine.Stats(); len(stats.LatePastRound) != 1 || len(stats.ResidualMisses) != 1 ||
		stats.ResidualMisses[0].CrossedSeries != 1 {
		t.Fatalf("stats carry %+v and %+v", stats.LatePastRound, stats.ResidualMisses)
	}
	f.engine.mu.Lock()
	f.engine.groups["qg"].seriesLate = nil
	f.engine.mu.Unlock()
	if len(f.engine.LatePastRound()) != 0 || len(f.engine.ResidualMisses()) != 0 {
		t.Fatal("reported after the group stopped being read directed")
	}
}
