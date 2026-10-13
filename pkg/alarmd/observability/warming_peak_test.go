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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type warmingPeakFixture struct {
	clock time.Time
	peak  *WarmingPeak
	lines []Observation
}

func newWarmingPeakFixture(start time.Time) *warmingPeakFixture {
	fixture := &warmingPeakFixture{clock: start}
	fixture.peak = NewWarmingPeak(func() time.Time { return fixture.clock })
	fixture.peak.SetEmit(ObserverFunc(func(_ context.Context, observation Observation) {
		fixture.lines = append(fixture.lines, observation)
	}))
	return fixture
}

// apply is one Plan's state apply at offset from the fixture's start, with
// warming writes to series still warming and ready writes to series that
// are not.
func (fixture *warmingPeakFixture) apply(at time.Time, queryGroup, strategy string, warming, ready int64) {
	fixture.clock = at
	facts := &StateWriteReuseFacts{}
	for i := int64(0); i < warming; i++ {
		facts.Record(StateWriteReuseChanged, StateWriteReuseStoredWarming)
	}
	for i := int64(0); i < ready; i++ {
		facts.Record(StateWriteReuseDecisionStable, StateWriteReuseStoredReady)
	}
	ctx := ContextWithTraceFields(context.Background(), TraceFields{QueryGroupKey: queryGroup, StrategyID: strategy})
	fixture.peak.Observe(ctx, Observation{Component: ComponentState, Stage: StageMutationCompared,
		Result: ResultSuccess, StateWriteReuse: facts})
}

// An hour's line names the minute with the most writes to warming series and
// the groups that made them, beside the hour's total and its median minute,
// so a burst is told from the steady rate it stands on. Writes to series
// that are not warming are not counted.
func TestWarmingPeakNamesTheGroupsOfAnHoursBusiestMinute(t *testing.T) {
	hour := time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC)
	fixture := newWarmingPeakFixture(hour)
	fixture.apply(hour.Add(10*time.Second), "qg-a", "11", 100, 900)
	fixture.apply(hour.Add(70*time.Second), "qg-b", "22", 500, 0)
	fixture.apply(hour.Add(80*time.Second), "qg-c", "33", 200, 50)
	fixture.apply(hour.Add(90*time.Second), "qg-b", "22", 300, 0)
	fixture.apply(hour.Add(150*time.Second), "qg-a", "11", 50, 0)
	if len(fixture.lines) != 0 {
		t.Fatalf("lines %v before the hour ended, want none", fixture.lines)
	}

	fixture.apply(hour.Add(time.Hour+10*time.Second), "qg-a", "11", 0, 10)
	if len(fixture.lines) != 1 {
		t.Fatalf("lines %d after the hour ended, want one", len(fixture.lines))
	}
	line := fixture.lines[0]
	if line.Component != ComponentState || line.Stage != StageWarmingPeak || line.WarmingPeak == nil {
		t.Fatalf("line = %+v, want a %s line carrying the peak", line, StageWarmingPeak)
	}
	want := WarmingPeakFacts{
		HourStart: hour, Writes: 1150, MinutesWithWrites: 3, MedianMinuteWrites: 0,
		PeakMinute: hour.Add(time.Minute), PeakMinuteWrites: 1000,
		PeakGroups: []WarmingGroup{{QueryGroup: "qg-b", StrategyID: "22", Writes: 800}, {QueryGroup: "qg-c", StrategyID: "33", Writes: 200}},
	}
	if !reflect.DeepEqual(*line.WarmingPeak, want) {
		t.Fatalf("peak = %+v, want %+v", *line.WarmingPeak, want)
	}

	// The next hour starts empty: its first minute held only ready writes.
	fixture.apply(hour.Add(2*time.Hour+5*time.Second), "qg-a", "11", 0, 10)
	if len(fixture.lines) != 1 {
		t.Fatalf("an hour without warming writes wrote %v, want no line", fixture.lines[1:])
	}
}

// The median is over the hour's sixty minutes, the quiet ones included: a
// burst is read against the minute it is not.
func TestWarmingPeakMedianCountsTheHoursQuietMinutes(t *testing.T) {
	hour := time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC)
	fixture := newWarmingPeakFixture(hour)
	for minute := 0; minute < 40; minute++ {
		writes := int64(10)
		if minute >= 34 {
			writes = 5000
		}
		fixture.apply(hour.Add(time.Duration(minute)*time.Minute+time.Second), "qg-a", "11", writes, 0)
	}
	fixture.apply(hour.Add(time.Hour+time.Second), "qg-a", "11", 0, 1)
	if len(fixture.lines) != 1 {
		t.Fatalf("lines %d, want one", len(fixture.lines))
	}
	peak := fixture.lines[0].WarmingPeak
	if peak.MinutesWithWrites != 40 || peak.MedianMinuteWrites != 10 || peak.Writes != 34*10+6*5000 || peak.PeakMinuteWrites != 5000 ||
		!peak.PeakMinute.Equal(hour.Add(34*time.Minute)) {
		t.Fatalf("peak = %+v, want 40 minutes, median 10, the first 5000 minute the peak", peak)
	}
}

// A minute with more groups than the count holds keeps the count bounded and
// still names the heavy one; what an evicted group may have added to the one
// that took its place is said, not hidden.
func TestWarmingPeakStaysBoundedAndKeepsTheHeavyGroup(t *testing.T) {
	hour := time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC)
	fixture := newWarmingPeakFixture(hour)
	for i := 0; i < 3*warmingPeakGroups; i++ {
		fixture.apply(hour.Add(time.Second), fmt.Sprintf("qg-%03d", i), "1", 1, 0)
		if i%50 == 0 {
			fixture.apply(hour.Add(time.Second), "qg-heavy", "9", 400, 0)
		}
	}
	if held := fixture.peak.held(); held > warmingPeakGroups {
		t.Fatalf("held %d groups, want at most %d", held, warmingPeakGroups)
	}
	fixture.apply(hour.Add(time.Hour+time.Second), "qg-heavy", "9", 0, 1)
	peak := fixture.lines[0].WarmingPeak
	if peak.PeakMinuteWrites != int64(3*warmingPeakGroups)+16*400 || len(peak.PeakGroups) != warmingPeakTop {
		t.Fatalf("peak = %+v, want every write in the minute's total and %d groups named", peak, warmingPeakTop)
	}
	top := peak.PeakGroups[0]
	if top.QueryGroup != "qg-heavy" || top.Writes-top.Overcount > 16*400 || top.Writes < 16*400 {
		t.Fatalf("top = %+v, want qg-heavy with its 6400 writes, any overcount said", top)
	}
}

// The line goes out after the count's lock is released: an emit that comes
// back into the observer does not deadlock.
func TestWarmingPeakEmitsOutsideItsLock(t *testing.T) {
	hour := time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC)
	fixture := newWarmingPeakFixture(hour)
	fixture.peak.SetEmit(ObserverFunc(func(ctx context.Context, observation Observation) {
		fixture.lines = append(fixture.lines, observation)
		fixture.peak.Observe(ctx, Observation{StateWriteReuse: &StateWriteReuseFacts{}})
	}))
	fixture.apply(hour.Add(time.Second), "qg-a", "11", 5, 0)
	done := make(chan struct{})
	go func() {
		fixture.apply(hour.Add(time.Hour+time.Second), "qg-a", "11", 0, 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the hour's line deadlocked")
	}
	if len(fixture.lines) != 1 {
		t.Fatalf("lines %d, want one", len(fixture.lines))
	}
}

// The hour's line is written whatever the repeated-line budget has spent, and
// carries its numbers flat and the groups by name.
func TestWarmingPeakLineIsAlwaysWrittenWithItsGroups(t *testing.T) {
	var output bytes.Buffer
	observer := withheldObserver(t, &output)
	hour := time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		output.Reset()
		observer.Observe(context.Background(), Observation{Component: ComponentState, Stage: StageWarmingPeak, Result: ResultSuccess,
			WarmingPeak: &WarmingPeakFacts{HourStart: hour, Writes: 1150, MinutesWithWrites: 3, PeakMinute: hour.Add(time.Minute), PeakMinuteWrites: 1000,
				PeakGroups: []WarmingGroup{{QueryGroup: "qg-b", StrategyID: "22", Writes: 800}}}})
		var event map[string]any
		if err := json.Unmarshal(output.Bytes(), &event); err != nil {
			t.Fatalf("line %d: %v; log=%q", i, err, output.String())
		}
		groups, _ := event["warming_peak_groups"].([]any)
		if event["warming_writes"] != float64(1150) || event["warming_peak_minute_writes"] != float64(1000) || len(groups) != 1 ||
			groups[0].(map[string]any)["query_group"] != "qg-b" || event["warming_peak_minute"] != "2026-10-13T03:01:00Z" {
			t.Fatalf("line %d = %#v, want the hour's numbers and qg-b named", i, event)
		}
	}
}
