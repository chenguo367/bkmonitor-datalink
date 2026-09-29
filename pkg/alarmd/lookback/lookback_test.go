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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const (
	sourceTimeSeries = "bk_monitor/time_series"
	sourceLog        = "bk_log_search/log"
	minute           = time.Minute
)

func dataset(host string, points map[int64]string) *execution.Dataset {
	buckets := make([]int64, 0, len(points))
	for at := range points {
		buckets = append(buckets, at)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	records := make([]contract.CanonicalRecordV2, 0, len(points))
	for _, at := range buckets {
		records = append(records, contract.CanonicalRecordV2{RecordID: fmt.Sprintf("%s-%d", host, at), SourceTime: at,
			DimensionIdentity: contract.DimensionIdentityV2{Digest: "digest-" + host},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(points[at])},
			Dimensions:        map[string]json.RawMessage{"host": json.RawMessage(`"` + host + `"`)}})
	}
	return execution.NewDataset(records)
}

func summarize(datasets ...*execution.Dataset) readSummary {
	summary := newSummarizer("value")
	for _, one := range datasets {
		summary.add(one)
	}
	return summary.buckets
}

// The same data summed in another series order, split into other pages, or
// with a number rendered another way, is the same bits: an order the
// provider happens to deliver in must never read as late data.
func TestTheSummaryDoesNotDependOnOrderOrPagesOrRendering(t *testing.T) {
	h1 := map[int64]string{60: "1", 120: "2", 180: "3"}
	h2 := map[int64]string{60: "5", 120: "7"}
	whole := summarize(dataset("h1", h1), dataset("h2", h2))
	for name, other := range map[string]readSummary{
		"series reversed": summarize(dataset("h2", h2), dataset("h1", h1)),
		"pages split":     summarize(dataset("h1", map[int64]string{180: "3"}), dataset("h2", h2), dataset("h1", map[int64]string{60: "1", 120: "2"})),
		"number rendered": summarize(dataset("h1", map[int64]string{60: "1.0", 120: "2e0", 180: "3.00"}), dataset("h2", h2)),
	} {
		if len(other) != len(whole) {
			t.Fatalf("%s: %d buckets, want %d", name, len(other), len(whole))
		}
		for at, bucket := range whole {
			if other[at] != bucket {
				t.Fatalf("%s: bucket %d is %+v, want %+v", name, at, other[at], bucket)
			}
		}
	}
	if zero, negative := summarize(dataset("h1", map[int64]string{60: "0"})), summarize(dataset("h1", map[int64]string{60: "-0"})); zero[60] != negative[60] {
		t.Fatal("0 and -0 are two values")
	}
	if whole.bytes() != len(whole)*summaryEntryBytes {
		t.Fatalf("a summary of %d buckets holds %d bytes", len(whole), whole.bytes())
	}
}

// A bucket that changed is named by how: more points, fewer, as many from
// other series, or other values. A read compared with itself changed
// nothing.
func TestCompareNamesEveryChange(t *testing.T) {
	earlier := summarize(dataset("h1", map[int64]string{60: "1", 120: "2", 180: "3", 240: "4"}), dataset("h2", map[int64]string{300: "1"}))
	later := summarize(dataset("h1", map[int64]string{60: "1", 120: "9", 180: "3"}), dataset("h2", map[int64]string{60: "0"}),
		dataset("h3", map[int64]string{300: "1"}))
	got := compareSummaries(earlier, later)
	// 60: h1 and h2 where h1 was alone - added; 120: 2 -> 9 - values; 240:
	// gone - removed; 300: h2 -> h3 - series; 180: unchanged.
	want := map[string]int{ChangePointsAdded: 1, ChangeValuesChanged: 1, ChangePointsRemoved: 1, ChangeSeriesChanged: 1}
	if len(got) != len(want) {
		t.Fatalf("changes %v, want %v", got, want)
	}
	for class, n := range want {
		if got[class] != n {
			t.Fatalf("changes %v, want %v", got, want)
		}
	}
	if same := compareSummaries(earlier, earlier); len(same) != 0 {
		t.Fatalf("a read compared with itself changed: %v", same)
	}
	// Sums, not exclusive-ors: a series delivered twice in a bucket does not
	// cancel itself out, so two of h1 and two of h2 are other series.
	twice := func(host string) readSummary {
		return summarize(dataset(host, map[int64]string{60: "1"}), dataset(host, map[int64]string{60: "1"}))
	}
	if got := compareSummaries(twice("h1"), twice("h2")); got[ChangeSeriesChanged] != 1 || len(got) != 1 {
		t.Fatalf("h1 twice to h2 twice = %v, want the series changed", got)
	}
}

type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *clock) set(at time.Time) {
	c.mu.Lock()
	c.at = at
	c.mu.Unlock()
}

type answer func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error)

func full(series ...*execution.Dataset) answer {
	return func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		for _, one := range series {
			_ = sink.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{Dataset: one,
				Delivery: execution.SeriesDelivery{Bytes: 100}})
		}
		return execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil
	}
}

type fixture struct {
	t       *testing.T
	clock   *clock
	engine  *Engine
	answers chan answer
	mu      sync.Mutex
	refuse  string
	yield   chan struct{}
	owned   map[execution.QueryGroupIdentity]bool
	faults  []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, clock: &clock{at: time.Unix(1_700_000_100, 0)}, answers: make(chan answer, 16),
		owned: map[execution.QueryGroupIdentity]bool{"qg": true, "qg-b": true}}
	engine, err := New(Options{Now: f.clock.now,
		Refusals: []string{"waiters", "headroom", "lookback_limit"}, LimitRefusal: "lookback_limit", UnspreadFirstSamples: true,
		Recheck: func(ctx context.Context, _ execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("a recheck ran without a deadline")
			}
			select {
			case next := <-f.answers:
				return next(sink)
			case <-ctx.Done():
				return execution.ProviderCompletion{}, ctx.Err()
			}
		},
		Permit: func() (func(), <-chan struct{}, string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.refuse != "" {
				return nil, nil, f.refuse
			}
			return func() {}, f.yield, ""
		},
		Owns: func(queryGroup execution.QueryGroupIdentity) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.owned[queryGroup]
		},
		Owned: func() int {
			f.mu.Lock()
			defer f.mu.Unlock()
			n := 0
			for _, owned := range f.owned {
				if owned {
					n++
				}
			}
			return n
		},
		OnFault: func(reason string, _ execution.QueryGroupIdentity) {
			f.mu.Lock()
			f.faults = append(f.faults, reason)
			f.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.engine = engine
	return f
}

func (f *fixture) set(change func()) {
	f.mu.Lock()
	change()
	f.mu.Unlock()
}

// group is a copy of one Query Group's state.
func (f *fixture) group(queryGroup execution.QueryGroupIdentity) group {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	return *f.engine.groups[queryGroup]
}

// rung is the next rung of queryGroup's sample, or of the one waiting for
// its deep recheck when it has none, read under the engine's lock; false
// when it has neither.
func (f *fixture) rung(queryGroup execution.QueryGroupIdentity) (int, bool) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	state := f.engine.groups[queryGroup]
	for _, candidate := range [...]*sample{state.sample, state.probe} {
		if candidate != nil {
			return candidate.rung, true
		}
	}
	return 0, false
}

// query is a formal first read of one Query Group's Slot at slot, over the
// step before it.
func query(queryGroup string, slot int64, step time.Duration, semantics ...string) Query {
	return Query{Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{
		QueryGroup: execution.QueryGroupIdentity(queryGroup), EvaluationTime: execution.EvaluationTime(slot)}},
		Spec: execution.PhysicalQuerySpec{Digest: execution.PhysicalQueryDigest(queryGroup + "-query"),
			LogicalWindow: execution.QueryWindow{Start: slot - int64(step/time.Second), End: slot},
			PlanFacts: execution.QueryPlanFacts{SourceSemantics: semantics, StepMillis: step.Milliseconds(),
				Normalization: execution.DatasetNormalizationSpec{CanonicalValueField: "value"}}},
		Operation: execution.OperationNormal, AttemptNo: 1}
}

// point is h1 at the one bucket of the minute window before slot.
func point(slot int64, value string) *execution.Dataset {
	return dataset("h1", map[int64]string{slot - 60: value})
}

// capture takes q as its Query Group's sample, now, with data points.
func (f *fixture) capture(q Query, datasets ...*execution.Dataset) {
	f.t.Helper()
	read := f.engine.Begin(q)
	if read == nil || read.summary == nil {
		f.t.Fatalf("%s at %d was not taken as a sample", q.Contract.Slot.QueryGroup, q.Contract.Slot.EvaluationTime)
	}
	for _, one := range datasets {
		read.Series(one, 100)
	}
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
}

// recheck reads the sample's rung at its moment with the answer given and
// waits for the outcome to be counted.
func (f *fixture) recheck(source string, readAt time.Time, rung int, step time.Duration, next answer, outcome string) Stats {
	f.t.Helper()
	before := f.engine.Stats().Sources[source].Rechecks[RungNames[rung]][outcome]
	f.clock.set(readAt.Add(rungDelay(rung, step)))
	f.answers <- next
	f.engine.Step(context.Background())
	return f.waitFor(func(stats Stats) bool { return stats.Sources[source].Rechecks[RungNames[rung]][outcome] > before })
}

// sample reads one sample of queryGroup to its end, its deep recheck
// included, at a minute-step window ending now: first at its first read,
// and values(rung) at each rung.
func (f *fixture) sample(queryGroup, source string, step time.Duration, first string, values func(rung int) string) Stats {
	f.t.Helper()
	slot := f.clock.now().Unix()
	f.capture(query(queryGroup, slot, step, source), point(slot, first))
	readAt := f.clock.now()
	stats := f.engine.Stats()
	for {
		rung, pending := f.rung(execution.QueryGroupIdentity(queryGroup))
		if !pending {
			return stats
		}
		stats = f.recheck(source, readAt, rung, step, full(point(slot, values(rung))), RecheckCompared)
	}
}

// rest moves the clock to the moment queryGroup may be sampled again, or
// leaves it where it is when that has passed.
func (f *fixture) rest(queryGroup execution.QueryGroupIdentity) {
	if next := f.group(queryGroup).nextAt; next.After(f.clock.now()) {
		f.clock.set(next)
	}
}

func (f *fixture) waitFor(done func(Stats) bool) Stats {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats := f.engine.Stats()
		if done(stats) {
			return stats
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("condition not reached: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
}

var steady = map[int64]string{1_700_000_040: "3"}

// Every owned Query Group keeps one sample at a time: the first formal read
// of a Slot is taken, a second physical query of it and the reads while the
// sample is in flight are not, another Query Group is; nothing is sampled
// by chance. A retry or a recovery read is not a formal first read. Every
// formal first read is counted, with its bytes.
func TestEveryOwnedQueryGroupKeepsOneSampleAtATime(t *testing.T) {
	f := newFixture(t)
	first := query("qg", 1_700_000_100, minute, sourceLog)
	read := f.engine.Begin(first)
	if read == nil || read.summary == nil {
		t.Fatal("the first read of an owned Query Group was not taken")
	}
	if second := f.engine.Begin(first); second == nil || second.summary != nil {
		t.Fatal("a second physical query of the Slot was taken while the first was being read")
	}
	read.Series(dataset("h1", steady), 100)
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	next := f.engine.Begin(query("qg", 1_700_000_160, minute, sourceLog))
	if next.summary != nil {
		t.Fatal("the next Slot was taken while the sample was in flight")
	}
	next.Series(dataset("h1", steady), 50)
	next.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	retry := query("qg-b", 1_700_000_100, minute, sourceTimeSeries)
	retry.AttemptNo = 2
	recovery := query("qg-b", 1_700_000_100, minute, sourceTimeSeries)
	recovery.Operation = execution.OperationReplay
	if f.engine.Begin(retry) != nil || f.engine.Begin(recovery) != nil {
		t.Fatal("a retry or a recovery read was seen as a formal first read")
	}
	f.capture(query("qg-b", 1_700_000_100, minute, sourceTimeSeries), dataset("h1", steady))
	stats := f.engine.Stats()
	if stats.Pending != 2 || stats.PendingBytes != 2*summaryEntryBytes {
		t.Fatalf("pending %d bytes %d, want two one-bucket samples", stats.Pending, stats.PendingBytes)
	}
	if stats.Coverage != (Coverage{Owned: 2, Covered: 2, Ratio: 1}) {
		t.Fatalf("coverage %+v, want both owned Query Groups", stats.Coverage)
	}
	log := stats.Sources[sourceLog]
	if log.FirstReads != 3 || log.FirstReadBytes != 150 || stats.Sources[sourceTimeSeries].FirstReads != 1 {
		t.Fatalf("first reads log %d (%d bytes) time series %d", log.FirstReads, log.FirstReadBytes, stats.Sources[sourceTimeSeries].FirstReads)
	}
}

// A first read that was not complete is not a sample and leaves the Query
// Group free to be sampled at its next Slot.
func TestAnIncompleteFirstReadLeavesTheQueryGroupFree(t *testing.T) {
	f := newFixture(t)
	read := f.engine.Begin(query("qg", 1_700_000_100, minute, sourceLog))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil)
	if n := f.engine.Stats().Sources[sourceLog].Samples[OutcomeFirstReadIncomplete]; n != 1 {
		t.Fatalf("incomplete first reads %d, want 1", n)
	}
	f.capture(query("qg", 1_700_000_160, minute, sourceLog))
}

// A process spreads its Query Groups' first samples over an hour, by a hash
// of each: they all read for the first time within a period of its start,
// and would all be rechecked at once otherwise.
func TestFirstSamplesAreSpreadOverAnHour(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	engine, err := New(Options{Now: func() time.Time { return now },
		Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, nil
		},
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1000 }})
	if err != nil {
		t.Fatal(err)
	}
	earliest, latest, sampled := time.Duration(restCap), time.Duration(0), 0
	for index := 0; index < 1000; index++ {
		queryGroup := fmt.Sprintf("qg-%d", index)
		if read := engine.Begin(query(queryGroup, 1_700_000_100, minute, sourceLog)); read.summary != nil {
			sampled++
		}
		wait := engine.groups[execution.QueryGroupIdentity(queryGroup)].nextAt.Sub(now)
		earliest, latest = min(earliest, wait), max(latest, wait)
	}
	if sampled > 5 || earliest < 0 || latest >= restCap || latest-earliest < restCap*9/10 {
		t.Fatalf("%d of 1000 sampled at once; first samples from %v to %v, want spread over the hour", sampled, earliest, latest)
	}
}

// Rungs are counted in the Query Group's data steps, not its evaluation
// period: a Query Group evaluated once a day over minute data is read again
// 1.5 minutes after its read, not 1.5 days.
func TestRungsFollowTheDataStepNotTheEvaluationPeriod(t *testing.T) {
	f := newFixture(t)
	day := int64(24 * 60 * 60)
	f.capture(query("qg", 1_700_000_100-day, minute, sourceTimeSeries), dataset("h1", steady))
	readAt := f.clock.now()
	f.capture(query("qg-b", 1_700_000_100, 10*time.Second, sourceTimeSeries), dataset("h1", steady))
	compared := func(at time.Duration, answers int) uint64 {
		t.Helper()
		before := f.engine.Stats().Sources[sourceTimeSeries].Rechecks[RungNames[0]][RecheckCompared]
		f.clock.set(readAt.Add(at))
		for range answers {
			f.answers <- full(dataset("h1", steady))
		}
		f.engine.Step(context.Background())
		if answers > 0 {
			f.waitFor(func(stats Stats) bool {
				return stats.Sources[sourceTimeSeries].Rechecks[RungNames[0]][RecheckCompared] >= before+uint64(answers)
			})
		}
		return f.engine.Stats().Sources[sourceTimeSeries].Rechecks[RungNames[0]][RecheckCompared]
	}
	// Just before 15 seconds nothing is due; at 15 - 1.5 ten-second steps -
	// the ten-second Query Group is read, and the daily one over minute data
	// only at 90, 1.5 of its minute steps.
	if n := compared(14*time.Second, 0); n != 0 {
		t.Fatalf("a rung was read before its moment: %d", n)
	}
	if n := compared(15*time.Second, 1); n != 1 {
		t.Fatalf("compared at 15s: %d, want the ten-second Query Group only", n)
	}
	if n := compared(89*time.Second, 0); n != 1 {
		t.Fatalf("compared at 89s: %d, want the daily Query Group still waiting", n)
	}
	if n := compared(90*time.Second, 1); n != 2 {
		t.Fatalf("compared at 90s: %d, want both Query Groups", n)
	}
}

// A Query Group whose data is always complete at the first read reads one
// rung and rests longer after each clean sample, doubling up to an hour
// whatever its step: about one recheck an hour, at ten seconds, a minute or
// five minutes a step. Its first sample and every fourth one after it are
// probed; the rest runs from its one rung, not from its deep recheck.
func TestAPunctualQueryGroupIsRecheckedAboutHourlyWhateverItsStep(t *testing.T) {
	for step, want := range map[time.Duration][]float64{
		minute:           {3, 6, 12, 24, 48, 60, 60, 60},
		10 * time.Second: {3, 6, 12, 24, 48, 96, 192, 360, 360},
		5 * minute:       {3, 6, 12, 12},
	} {
		f := newFixture(t)
		rests := []float64{}
		for range want {
			readAt := f.clock.now()
			f.sample("qg", sourceTimeSeries, step, "3", func(int) string { return "3" })
			state := f.group("qg")
			if state.depth != 1 {
				t.Fatalf("step %v: depth %d", step, state.depth)
			}
			rests = append(rests, state.rest)
			finished := readAt.Add(rungDelay(0, step))
			rest := min(time.Duration(state.rest*float64(step)), restCap)
			if wantAt := finished.Add(time.Duration(float64(rest) * restSpread("qg"))); !state.nextAt.Equal(wantAt) {
				t.Fatalf("step %v: next sample at %v, want %v", step, state.nextAt, wantAt)
			}
			// Not before its rest has passed, and at once after.
			f.clock.set(state.nextAt.Add(-time.Second))
			if read := f.engine.Begin(query("qg", f.clock.now().Unix(), step, sourceTimeSeries)); read.summary != nil {
				t.Fatalf("step %v: sampled before its rest of %v passed", step, rest)
			}
			f.rest("qg")
		}
		for index := range want {
			if rests[index] != want[index] {
				t.Fatalf("step %v: rests %v, want %v", step, rests, want)
			}
		}
		if rest := time.Duration(rests[len(rests)-1] * float64(step)); rest < restCap {
			t.Fatalf("step %v: settled at a rest of %v, want an hour", step, rest)
		}
		source := f.engine.Stats().Sources[sourceTimeSeries]
		if source.Samples[OutcomeCompleted] != uint64(len(want)) || source.Probes[ProbeClean] != uint64((len(want)+probeEvery-1)/probeEvery) {
			t.Fatalf("step %v: completed %d, probes %v", step, source.Samples[OutcomeCompleted], source.Probes)
		}
	}
}

// A Query Group whose data still arrives at the last planned rung is
// followed a rung further within the same sample, the window is complete at
// the last rung that changed, the group reads one rung past it from then on,
// and rests only as long as that deepest rung.
func TestALateQueryGroupIsFollowedToWhereItsDataStops(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(point(slot, "4")), RecheckCompared)
	f.recheck(sourceLog, readAt, 1, minute, full(point(slot, "4"), dataset("h2", map[int64]string{slot - 60: "1"})), RecheckCompared)
	f.recheck(sourceLog, readAt, 2, minute, full(point(slot, "4"), dataset("h2", map[int64]string{slot - 60: "1"})), RecheckCompared)
	// The first sample is probed: nothing more at the deepest rung.
	stats := f.recheck(sourceLog, readAt, len(RungSteps)-1, minute, full(point(slot, "4"), dataset("h2", map[int64]string{slot - 60: "1"})), RecheckCompared)
	source := stats.Sources[sourceLog]
	if source.ChangedWindows[RungNames[0]] != 1 || source.ChangedWindows[RungNames[1]] != 1 || source.ChangedWindows[RungNames[2]] != 0 {
		t.Fatalf("changed windows %v", source.ChangedWindows)
	}
	if source.Changes[RungNames[0]][ChangeValuesChanged] != 1 || source.Changes[RungNames[1]][ChangePointsAdded] != 1 {
		t.Fatalf("changes %v", source.Changes)
	}
	state := f.group("qg")
	if source.Samples[OutcomeCompleted] != 1 || state.depth != 3 || state.rest != RungSteps[2] || source.DepthGroups["3"] != 1 {
		t.Fatalf("samples %v depth %d rest %v, want completed at depth 3 resting 7.5 steps", source.Samples, state.depth, state.rest)
	}
	// Complete at the second rung: 3.5 steps after the read, which was at
	// the window's end.
	lateness := time.Duration(RungSteps[1] * float64(minute))
	if len(stats.Latest) != 1 || stats.Latest[0].CompletionSeconds != int64(lateness/time.Second) ||
		source.MaxCompletionSeconds != int64(lateness/time.Second) || source.Completion[ageBucket(lateness)] != 1 {
		t.Fatalf("latest %+v max %d completion %v, want %v", stats.Latest, source.MaxCompletionSeconds, source.Completion, lateness)
	}
	if len(stats.Recent) != 2 || stats.Recent[1].Rung != RungNames[1] || source.RecheckBytes != 700 || source.Probes[ProbeClean] != 1 {
		t.Fatalf("recent %+v recheck bytes %d", stats.Recent, source.RecheckBytes)
	}
}

// Each Query Group learns how deep and how often it is rechecked from its
// own samples: a late group and a punctual one of the same source read to
// different depths, and the punctual one is not made to read deeper.
func TestEachQueryGroupLearnsItsOwnDepth(t *testing.T) {
	f := newFixture(t)
	f.sample("qg", sourceLog, minute, "1", func(int) string { return "2" }) // changes by the first rung
	f.sample("qg-b", sourceLog, minute, "1", func(int) string { return "1" })
	if late, punctual := f.group("qg"), f.group("qg-b"); late.depth != 2 || punctual.depth != 1 || punctual.rest != 3 {
		t.Fatalf("late depth %d, punctual depth %d rest %v; want 2, and 1 resting 3 steps", late.depth, punctual.depth, punctual.rest)
	}
	if groups := f.engine.Stats().Sources[sourceLog].DepthGroups; groups["1"] != 1 || groups["2"] != 1 {
		t.Fatalf("groups by depth %v", groups)
	}
}

// A Query Group reads fewer rungs only after cleanSamplesToShallow samples
// in a row needed fewer, and then at once as few as the most any of them
// needed, not a rung at a time: a group whose deep lateness has passed is
// back within that many samples. One sample that needed the rung again
// restarts the count.
func TestADeepQueryGroupShallowsOnlyAfterCleanSamplesInARow(t *testing.T) {
	f := newFixture(t)
	// sample's data changes at every rung up to lastChange, at none when -1.
	sample := func(lastChange int) int {
		t.Helper()
		f.sample("qg", sourceLog, minute, "3", func(rung int) string {
			if lastChange < 0 {
				return "3"
			}
			return fmt.Sprint(10 + min(rung, lastChange))
		})
		depth := f.group("qg").depth
		f.rest("qg")
		return depth
	}
	// clean reads n samples needing fewer rungs, the second of them changing
	// at the first rung when oneAtFirstRung, and returns the depth after each.
	clean := func(n int, oneAtFirstRung bool) []int {
		t.Helper()
		depths := []int{}
		for index := 0; index < n; index++ {
			if oneAtFirstRung && index == 1 {
				depths = append(depths, sample(0))
			} else {
				depths = append(depths, sample(-1))
			}
		}
		return depths
	}
	want := func(what string, got []int, depths ...int) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(depths) {
			t.Fatalf("%s: depths %v, want %v", what, got, depths)
		}
	}
	if depth := sample(1); depth != 3 {
		t.Fatalf("data changing up to the second rung left depth %d, want 3", depth)
	}
	// Seven in a row needing fewer, one of them two rungs, then one needing
	// the rung again: the count, and what the seven needed, start over.
	want("seven clean samples", clean(cleanSamplesToShallow-1, true), 3, 3, 3, 3, 3, 3, 3)
	if depth := sample(1); depth != 3 {
		t.Fatalf("a late sample did not hold the depth: %d", depth)
	}
	want("eight punctual samples after it", clean(cleanSamplesToShallow, false), 3, 3, 3, 3, 3, 3, 3, 1)
	// Eight in a row needing fewer, one of them two rungs: two rungs.
	sample(1)
	want("eight clean samples, one needing two rungs", clean(cleanSamplesToShallow, true), 3, 3, 3, 3, 3, 3, 3, 2)
	// What clean samples needed before the group deepened is not carried
	// past it: lateness to the fourth rung, then none, is one rung at once.
	sample(1)
	sample(0)
	if depth := sample(3); depth != 5 {
		t.Fatalf("data changing up to the fourth rung left depth %d, want 5", depth)
	}
	want("eight punctual samples after deep lateness", clean(cleanSamplesToShallow, false), 5, 5, 5, 5, 5, 5, 5, 1)
}

// A group that settles after a deep recheck forgets what its clean samples
// needed before: those were read at the depth the deep recheck found too
// shallow.
func TestASettlingQueryGroupForgetsWhatItsCleanSamplesNeededBefore(t *testing.T) {
	f := newFixture(t)
	// sample's data changes at every rung up to lastChange, and at the
	// deepest when late - read only by a deep recheck of a shallower group.
	sample := func(lastChange int, late bool) int {
		t.Helper()
		f.sample("qg", sourceLog, minute, "3", func(rung int) string {
			switch {
			case late && rung == len(RungSteps)-1:
				return "99"
			case lastChange < 0:
				return "3"
			}
			return fmt.Sprint(10 + min(rung, lastChange))
		})
		depth := f.group("qg").depth
		f.rest("qg")
		return depth
	}
	// Probed at its first sample and at its fifth.
	sample(1, false)
	sample(0, false)
	sample(-1, false)
	sample(-1, false)
	if depth := sample(-1, true); depth != len(RungSteps) || f.engine.Stats().Sources[sourceLog].Samples[OutcomeProbeChanged] != 1 {
		t.Fatalf("the fifth sample's deep recheck found data: depth %d, want every rung", depth)
	}
	if depth := sample(2, false); depth != 4 {
		t.Fatalf("settled at depth %d, want 4", depth)
	}
	depths := []int{}
	for range cleanSamplesToShallow {
		depths = append(depths, sample(-1, false))
	}
	if depths[len(depths)-1] != 1 {
		t.Fatalf("eight punctual samples after settling: depths %v, want one rung at the end", depths)
	}
}

// A Query Group late by the same rungs every time reads that deep, and
// rests like a punctual one once that holds: only the sample that deepened
// it brings it back sooner. The rechecks follow how deep the lateness goes,
// not how often it is seen again.
func TestAStablyLateQueryGroupRestsUpToAnHourAtItsDepth(t *testing.T) {
	f := newFixture(t)
	rests := []float64{}
	for sample := 0; sample < 7; sample++ {
		// Every sample: the data changes by the first rung and not after it.
		f.sample("qg", sourceLog, minute, "1", func(int) string { return fmt.Sprint(100 + sample) })
		state := f.group("qg")
		if state.depth != 2 {
			t.Fatalf("sample %d: depth %d, want 2 - one past the first rung", sample, state.depth)
		}
		rests = append(rests, state.rest)
		f.rest("qg")
	}
	want := []float64{3.5, 7, 14, 28, 56, 60, 60}
	for index := range want {
		if rests[index] != want[index] {
			t.Fatalf("rests %v, want %v", rests, want)
		}
	}
}

// Every rung not observed is named and never counted as a window that did
// not change: no permit through its window (yielded), a failed read, a
// partial one. A sample whose last rungs were not read is unobserved: no
// completion, no clean sample, the Query Group's depth and rest untouched -
// however many there are in a row. A refused permit is counted by its
// reason, an unnamed one as other, and one refused at the lookback's own
// share of the permits is a fault.
func TestAnUnobservedSampleChangesNothingItDidNotSee(t *testing.T) {
	f := newFixture(t)
	f.sample("qg", sourceLog, minute, "1", func(int) string { return "2" })
	f.rest("qg")
	before := f.group("qg")
	if before.depth != 2 {
		t.Fatalf("depth %d, want 2", before.depth)
	}
	f.set(func() { f.refuse = "waiters" })
	for sample := 0; sample < 2*cleanSamplesToShallow; sample++ {
		slot := f.clock.now().Unix()
		f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
		readAt := f.clock.now()
		for rung := 0; f.group("qg").sample != nil; rung++ {
			f.clock.set(readAt.Add(rungDelay(rung, minute)))
			f.engine.Step(context.Background())
			f.clock.set(readAt.Add(rungDelay(rung, minute) + rungWindow(rung, minute) + time.Second))
			f.engine.Step(context.Background())
		}
		f.rest("qg")
	}
	stats := f.engine.Stats()
	source := stats.Sources[sourceLog]
	after := f.group("qg")
	if after.depth != before.depth || after.rest != before.rest || after.clean != 0 {
		t.Fatalf("unobserved samples moved the group: depth %d rest %v clean %d, was depth %d rest %v",
			after.depth, after.rest, after.clean, before.depth, before.rest)
	}
	if source.Samples[OutcomeUnobserved] != 2*cleanSamplesToShallow || source.Samples[OutcomeCompleted] != 1 ||
		source.Completion["le_120s"] != 1 || source.UnobservedRatio < 0.9 {
		t.Fatalf("samples %v completion %v unobserved ratio %v", source.Samples, source.Completion, source.UnobservedRatio)
	}
	if stats.PermitRefusals["waiters"] == 0 || stats.Faults[FaultPermitLimit] != 0 {
		t.Fatalf("refusals %v faults %v", stats.PermitRefusals, stats.Faults)
	}
	// A failed read and a partial one leave their samples unobserved too.
	f.set(func() { f.refuse = "" })
	for _, next := range []answer{
		func(execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, errors.New("timeout")
		},
		func(execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil
		},
	} {
		slot := f.clock.now().Unix()
		f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
		readAt := f.clock.now()
		f.answers <- next
		f.answers <- next
		f.clock.set(readAt.Add(rungDelay(0, minute)))
		f.engine.Step(context.Background())
		f.waitFor(func(Stats) bool { rung, pending := f.rung("qg"); return !pending || rung >= 1 })
		f.clock.set(readAt.Add(rungDelay(1, minute)))
		f.engine.Step(context.Background())
		f.waitFor(func(Stats) bool { return f.group("qg").sample == nil })
		f.rest("qg")
	}
	source = f.engine.Stats().Sources[sourceLog]
	if source.Samples[OutcomeUnobserved] != 2*cleanSamplesToShallow+2 || source.Rechecks[RungNames[1]][RecheckFailed] != 1 ||
		source.Rechecks[RungNames[1]][RecheckPartial] != 1 {
		t.Fatalf("samples %v rechecks %v", source.Samples, source.Rechecks[RungNames[1]])
	}
	// An unnamed refusal is other; one at the lookback's own share, a fault.
	for _, refused := range []string{"unnamed", "lookback_limit"} {
		f.set(func() { f.refuse = refused })
		slot := f.clock.now().Unix()
		f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
		f.clock.set(f.clock.now().Add(rungDelay(0, minute)))
		f.engine.Step(context.Background())
		f.engine.Forget("qg")
	}
	stats = f.engine.Stats()
	if stats.PermitRefusals[RefusedOther] != 1 || stats.PermitRefusals["lookback_limit"] != 1 || stats.Faults[FaultPermitLimit] != 1 {
		t.Fatalf("refusals %v faults %v", stats.PermitRefusals, stats.Faults)
	}
}

// A sample whose last rung was not read still deepens its Query Group by
// the change it did read: its data arrived at least that late.
func TestAnUnobservedSampleStillDeepensByTheChangeItRead(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(point(slot, "2")), RecheckCompared)
	f.clock.set(readAt.Add(rungDelay(1, minute) + rungWindow(1, minute) + time.Second))
	f.engine.Step(context.Background())
	source := f.engine.Stats().Sources[sourceLog]
	if state := f.group("qg"); state.depth != 2 || state.sample != nil || state.probe != nil || source.Samples[OutcomeUnobserved] != 1 {
		t.Fatalf("depth %d, samples %v; want the unobserved sample to deepen its group to 2", state.depth, source.Samples)
	}
}

// A read asked to yield - a formal query is waiting for a permit - stops at
// once and gives its permit back, long before its own timeout. Its rung is
// not settled by it: counted as preempted, tried again inside its window,
// and then counted once, by what it came to.
func TestAReadAskedToYieldStopsAndItsRungIsTriedAgain(t *testing.T) {
	f := newFixture(t)
	yield := make(chan struct{})
	f.set(func() { f.yield = yield })
	f.capture(query("qg", 1_700_000_100, minute, sourceLog), dataset("h1", steady))
	readAt := f.clock.now()
	f.clock.set(readAt.Add(rungDelay(0, minute)))
	f.engine.Step(context.Background()) // no answer queued: the read blocks until it is asked to yield
	asked := time.Now()
	close(yield)
	f.waitFor(func(stats Stats) bool { return stats.Sources[sourceLog].Preempted[RungNames[0]] == 1 })
	if time.Since(asked) > RecheckTimeout/2 {
		t.Fatal("a read asked to yield kept running")
	}
	for _, outcome := range RecheckOutcomes {
		if n := f.engine.Stats().Sources[sourceLog].Rechecks[RungNames[0]][outcome]; n != 0 {
			t.Fatalf("the preempted read settled its rung as %s", outcome)
		}
	}
	f.set(func() { f.yield = nil })
	f.recheck(sourceLog, readAt, 0, minute, full(dataset("h1", steady)), RecheckCompared)
	stats := f.recheck(sourceLog, readAt, len(RungSteps)-1, minute, full(dataset("h1", steady)), RecheckCompared)
	if stats.Sources[sourceLog].Preempted[RungNames[0]] != 1 || stats.Sources[sourceLog].Samples[OutcomeCompleted] != 1 {
		t.Fatalf("preempted %v samples %v", stats.Sources[sourceLog].Preempted, stats.Sources[sourceLog].Samples)
	}
}

// A Query Group this process stops owning is forgotten with its sample,
// counted as owner_lost, and leaves the coverage; one that is owned but
// whose last measurement is older than twice a sample's cycle is not
// covered either.
func TestCoverageCountsOnlyOwnedQueryGroupsWithAFreshMeasurement(t *testing.T) {
	f := newFixture(t)
	f.capture(query("qg", 1_700_000_100, minute, sourceLog), dataset("h1", steady))
	readAt := f.clock.now()
	f.capture(query("qg-b", 1_700_000_100, minute, sourceLog), dataset("h1", steady))
	// Ownership moved on before the Runner set told the lookback: the group
	// is still in the table, and not covered, nor counted as owned.
	f.set(func() { f.owned["qg-b"] = false })
	if got := f.engine.Stats().Coverage; got != (Coverage{Owned: 1, Covered: 1, Ratio: 1}) {
		t.Fatalf("coverage with qg-b no longer owned: %+v", got)
	}
	f.engine.Forget("qg-b")
	stats := f.recheck(sourceLog, readAt, 0, minute, full(dataset("h1", steady)), RecheckCompared)
	if stats.Sources[sourceLog].Samples[OutcomeOwnerLost] != 1 || stats.Sources[sourceLog].Rechecks[RungNames[0]][RecheckOwnerLost] != 1 {
		t.Fatalf("samples %v", stats.Sources[sourceLog].Samples)
	}
	// Waiting for its deep recheck, the sample still covers its group, and
	// is pending with its one bucket.
	if _, waiting := f.rung("qg"); !waiting || f.group("qg").sample != nil || stats.Coverage != (Coverage{Owned: 1, Covered: 1, Ratio: 1}) ||
		stats.Pending != 1 || stats.PendingBytes != summaryEntryBytes {
		t.Fatalf("coverage %+v pending %d (%d bytes) while the deep recheck waits", stats.Coverage, stats.Pending, stats.PendingBytes)
	}
	f.recheck(sourceLog, readAt, len(RungSteps)-1, minute, full(dataset("h1", steady)), RecheckCompared)
	state := f.group("qg")
	cycle := min(time.Duration(state.rest*float64(minute)), restCap)*5/4 + rungDelay(state.depth-1, minute) + state.period
	f.clock.set(f.clock.now().Add(2*cycle + time.Second))
	if got := f.engine.Stats().Coverage; got.Covered != 0 || got.Owned != 1 {
		t.Fatalf("a stale measurement covered: %+v", got)
	}
}

// Every source is counted under a bounded label from the start - every data
// source a query can be compiled from, mixed and other - and a query is
// labelled as the catalog labels its Query Group: a plan with no semantics
// is plain time series, as the compiler writes it; a PromQL query reads the
// source its plan names; one source named twice is that source.
func TestSourcesAreBoundedAndCountedFromTheStart(t *testing.T) {
	f := newFixture(t)
	stats := f.engine.Stats()
	for _, source := range Sources {
		entry, present := stats.Sources[source]
		if !present || len(entry.Samples) != len(SampleOutcomes) || len(entry.Rechecks) != len(RungNames) ||
			len(entry.DepthGroups) != len(DepthLabels) || len(entry.Probes) != len(ProbeOutcomes) ||
			len(entry.EmptyFirstReads) != len(EmptyFirstReadOutcomes) || len(entry.EmptyFirstReadCompletion) != len(AgeBuckets) {
			t.Fatalf("source %s is not counted from the start: %+v", source, entry)
		}
	}
	if want := len(controlplane.SupportedSourceSemantics) + 2; len(stats.Sources) != want {
		t.Fatalf("sources %d, want %d", len(stats.Sources), want)
	}
	promql := query("qg", 1_700_000_100, minute, "prometheus/time_series")
	promql.Spec.PlanFacts.PromQL = &execution.PromQLQuery{}
	for want, facts := range map[string]execution.QueryPlanFacts{
		sourceTimeSeries:         {},
		SourceMixed:              {SourceSemantics: []string{sourceLog, sourceTimeSeries}},
		"prometheus/time_series": promql.Spec.PlanFacts,
		SourceOther:              {SourceSemantics: []string{"nobody/compiles_this"}},
		sourceLog:                {SourceSemantics: []string{sourceLog, sourceLog}},
	} {
		if got := sourceOf(facts); got != want {
			t.Fatalf("source of %+v = %s, want %s", facts.SourceSemantics, got, want)
		}
	}
	// Read through a first read: plain time series, as the compiler writes
	// it, is not other.
	f.capture(query("qg", 1_700_000_100, minute))
	stats = f.engine.Stats()
	if stats.Sources[sourceTimeSeries].FirstReads != 1 || stats.Sources[SourceOther].FirstReads != 0 {
		t.Fatalf("a plan with no semantics read as time series %d, other %d", stats.Sources[sourceTimeSeries].FirstReads,
			stats.Sources[SourceOther].FirstReads)
	}
}

// A read with more buckets than any window has is a defect: counted as a
// fault, reported, and not kept. Normal running never meets it.
func TestAReadPastEveryWindowIsAFault(t *testing.T) {
	f := newFixture(t)
	points := make(map[int64]string, maxBucketsPerSample+1)
	for at := int64(0); at <= maxBucketsPerSample; at++ {
		points[at] = "1"
	}
	read := f.engine.Begin(query("qg", 1_700_000_100, minute, sourceLog))
	read.Series(dataset("h1", points), 100)
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	stats := f.engine.Stats()
	if stats.Faults[FaultBucketsExceeded] != 1 || stats.Sources[sourceLog].Samples[OutcomeFault] != 1 || stats.Pending != 0 {
		t.Fatalf("faults %v samples %v pending %d", stats.Faults, stats.Sources[sourceLog].Samples, stats.Pending)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.faults) != 1 || f.faults[0] != FaultBucketsExceeded {
		t.Fatalf("faults reported %v", f.faults)
	}
}

// Query Groups rest as long on average, each spread by its own hash between
// three quarters of its rest and a quarter past it: groups read at one
// moment are not sampled, and rechecked, at one moment again.
func TestQueryGroupsRestSpreadAroundTheirRest(t *testing.T) {
	sum, low, high := 0.0, 2.0, 0.0
	const groups = 1000
	for index := 0; index < groups; index++ {
		spread := restSpread(execution.QueryGroupIdentity(fmt.Sprintf("qg-%d", index)))
		if spread < 0.75 || spread >= 1.25 {
			t.Fatalf("qg-%d rests %v of its rest", index, spread)
		}
		sum, low, high = sum+spread, min(low, spread), max(high, spread)
	}
	if mean := sum / groups; mean < 0.97 || mean > 1.03 || high-low < 0.45 {
		t.Fatalf("spreads mean %v from %v to %v, want about 1 over most of the range", mean, low, high)
	}
}

// facts are a valid structured query's, or a PromQL one's, so a tail read's
// digest can be derived again, as the query service checks it.
func facts(t testing.TB, step time.Duration, promql string, clause execution.QueryClause) execution.QueryPlanFacts {
	t.Helper()
	input := execution.QueryPlanFacts{Provider: execution.ProviderUQ, ProviderRouteRef: "uq-main", TenantID: "tenant", BusinessID: "2",
		SpaceScope: "bkcc__2", SourceSemantics: []string{sourceTimeSeries}, StepMillis: step.Milliseconds(),
		AlignmentMillis: step.Milliseconds(), Timezone: "UTC",
		Normalization: execution.DatasetNormalizationSpec{DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("a", 64),
			NormalizationDigest: strings.Repeat("b", 64), IdentityFields: []string{"host"}, SourceTimeField: "_time",
			ReceivedTimeField: "_received_time", DynamicDimensions: promql != ""},
			SourceTimeUnit: execution.TimeUnitMillisecond, CanonicalSourceTimeUnit: execution.TimeUnitSecond,
			SeriesIdentityMode: execution.SeriesIdentityUQGroupKeysValuesV1, GroupKeyRule: execution.GroupKeyStripTableSuffixV1,
			ValueSelectionMode: execution.ValueSelectionResultOrFirstReferenceV1, CanonicalValueField: "value",
			ReceivedTimeMode: execution.ReceivedTimeProviderReceivedAt, Version: "uq-threshold-normalization-v1"}}
	if promql != "" {
		input.PromQL = &execution.PromQLQuery{Expression: promql}
		input.Normalization.DatasetContract.IdentityFields = []string{}
	} else {
		clause.DataSource, clause.TableID, clause.FieldName, clause.ReferenceName = "bkmonitor", "system.cpu", "usage", "a"
		clause.Driver, clause.TimeField = "influxdb", "time"
		input.QueryList, input.MetricMerge = []execution.QueryClause{clause}, "a"
	}
	built, err := execution.BuildQueryPlanFacts(input)
	if err != nil {
		t.Fatal(err)
	}
	return built
}

// windowed is a query service over one series whose raw points, one a step,
// arrive late and are summed over a window: a bucket's value is the points
// less than window before it and not before the read's start that have
// arrived. Its first read and its rechecks both go through points.
type windowed struct {
	mu     sync.Mutex
	clock  *clock
	step   int64
	late   time.Duration
	window int64
	specs  []execution.PhysicalQuerySpec
}

func (w *windowed) points(spec execution.PhysicalQuerySpec) map[int64]string {
	now := w.clock.now().Unix()
	points := map[int64]string{}
	for at := spec.LogicalWindow.Start; at < spec.LogicalWindow.End; at += w.step {
		count := 0
		for raw := at - w.window; raw <= at; raw += w.step {
			if (w.window == 0 || raw > at-w.window) && raw >= spec.LogicalWindow.Start && raw+int64(w.late/time.Second) <= now {
				count++
			}
		}
		if count > 0 {
			points[at] = fmt.Sprint(count)
		}
	}
	return points
}

func (w *windowed) read(_ context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	w.mu.Lock()
	w.specs = append(w.specs, spec)
	w.mu.Unlock()
	_ = sink.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{Dataset: dataset("h1", w.points(spec)),
		Delivery: execution.SeriesDelivery{Bytes: 10}})
	return execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil
}

func (w *windowed) engine(t *testing.T) *Engine {
	t.Helper()
	engine, err := New(Options{Now: w.clock.now, Recheck: w.read, UnspreadFirstSamples: true,
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// run reads one sample of a window ending now to its end - the first read,
// then every rung at its moment - and moves the clock to its next sample.
func (w *windowed) run(t *testing.T, engine *Engine, span time.Duration, facts execution.QueryPlanFacts) {
	t.Helper()
	// Read at the window's end, on a whole second.
	end := w.clock.now().Add(time.Second - time.Nanosecond).Unix()
	w.clock.set(time.Unix(end, 0))
	spec := execution.PhysicalQuerySpec{Digest: "physical", PlanFacts: facts,
		LogicalWindow: execution.QueryWindow{Start: end - int64(span/time.Second), End: end}}
	spec.ProviderRange, spec.AcceptedRange = spec.LogicalWindow, spec.LogicalWindow
	read := engine.Begin(Query{Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "qg",
		EvaluationTime: execution.EvaluationTime(end)}}, Spec: spec, Operation: execution.OperationNormal, AttemptNo: 1})
	if read.summary == nil {
		t.Fatal("not sampled")
	}
	read.Series(dataset("h1", w.points(spec)), 10)
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	readAt := w.clock.now()
	step := time.Duration(w.step) * time.Second
	engine.mu.Lock()
	state := engine.groups["qg"]
	candidate := state.sample
	engine.mu.Unlock()
	// pending is the sample's next rung while it is its group's sample or
	// waits for its deep recheck.
	pending := func() (int, bool) {
		engine.mu.Lock()
		defer engine.mu.Unlock()
		if (state.sample == candidate || state.probe == candidate) && !candidate.running {
			return candidate.rung, true
		}
		return 0, candidate.running
	}
	for {
		rung, open := pending()
		if !open {
			break
		}
		w.clock.set(readAt.Add(rungDelay(rung, step)))
		engine.Step(context.Background())
		deadline := time.Now().Add(5 * time.Second)
		for {
			next, open := pending()
			if !open || next > rung {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("rung %d did not settle", rung)
			}
			time.Sleep(time.Millisecond)
		}
	}
	engine.mu.Lock()
	next := state.nextAt
	engine.mu.Unlock()
	if next.After(w.clock.now()) {
		w.clock.set(next)
	}
}

// A recheck reads and compares only the window's tail - the deepest rung
// and a step, however deep its Query Group reads - from the query's own
// lookback before that, under a digest derived again for that range, at a
// rung and at the deep recheck alike; the buckets outside the tail are not
// read as vanished.
func TestARecheckReadsAndComparesOnlyTheTail(t *testing.T) {
	w := &windowed{clock: &clock{at: time.Unix(1_700_006_000, 0)}, step: 60}
	engine := w.engine(t)
	w.run(t, engine, 24*time.Hour, facts(t, minute, "", execution.QueryClause{TimeAggregation: execution.QueryFunction{Method: "avg_over_time", Window: "60s"}}))
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.specs) != 2 {
		t.Fatalf("%d rechecks, want the one rung of a punctual Query Group and its deep recheck", len(w.specs))
	}
	end := int64(1_700_006_000)
	for _, tail := range w.specs {
		// ceil(63.5) + 1 = 65 steps; the lookback, 60s, before it.
		if tail.LogicalWindow.Start != end-65*60-60 || tail.ProviderRange.Start != tail.LogicalWindow.Start ||
			tail.AcceptedRange.Start != tail.LogicalWindow.Start || tail.LogicalWindow.End != end {
			t.Fatalf("recheck read %+v, want from %d", tail.LogicalWindow, end-66*60)
		}
		if digest, err := execution.DerivePhysicalQueryDigest(tail); err != nil || digest != tail.Digest || tail.Digest == "physical" {
			t.Fatalf("the tail read's digest %q, derived %q %v", tail.Digest, digest, err)
		}
	}
	source := engine.Stats().Sources[sourceTimeSeries]
	if source.Samples[OutcomeCompleted] != 1 || source.ChangedWindows[RungNames[0]] != 0 || source.Probes[ProbeClean] != 1 ||
		source.RecheckBytes != 20 || source.FirstReadBytes != 10 {
		t.Fatalf("samples %v changed %v probes %v bytes %d/%d: the buckets outside the tail must not read as vanished",
			source.Samples, source.ChangedWindows, source.Probes, source.RecheckBytes, source.FirstReadBytes)
	}
}

// A first read is kept to the window's tail, whatever the window's length
// and however deep its Query Group reads: a Query Group over a day of
// minutes holds 65 buckets, not a day's.
func TestAFirstReadIsKeptToItsTail(t *testing.T) {
	f := newFixture(t)
	day := map[int64]string{}
	end := f.clock.now().Unix()
	for at := end - 24*60*60; at < end; at += 60 {
		day[at] = "1"
	}
	q := query("qg", end, minute, sourceTimeSeries)
	q.Spec.LogicalWindow.Start = end - 24*60*60
	f.capture(q, dataset("h1", day))
	if tailSteps != 65 {
		t.Fatalf("a tail of %d steps, want ceil(63.5) + 1", tailSteps)
	}
	if stats := f.engine.Stats(); stats.PendingBytes != 65*summaryEntryBytes {
		t.Fatalf("a day's first read holds %d bytes, want 65 buckets'", stats.PendingBytes)
	}
}

// A tail read starts the query's own lookback early, so its first compared
// bucket is computed from the same points the first read computed it from:
// over a window longer than the tail, a rate over five minutes and a
// function window of four minutes over a minute's aggregation read no
// change where nothing arrived. A query whose lookback cannot be read is
// read from a step early, and counted.
func TestATailReadStartsEarlyByTheQuerysLookback(t *testing.T) {
	for name, query := range map[string]execution.QueryPlanFacts{
		"promql rate": facts(t, minute, "sum(rate(cpu_usage[5m]))", execution.QueryClause{}),
		"function window": facts(t, minute, "", execution.QueryClause{TimeAggregation: execution.QueryFunction{Method: "sum_over_time", Window: "60s"},
			Functions: []execution.QueryFunction{{Method: "moving_avg", Window: "4m"}}}),
	} {
		w := &windowed{clock: &clock{at: time.Unix(1_700_006_000, 0)}, step: 60, window: 300}
		engine := w.engine(t)
		for range 3 {
			w.run(t, engine, 2*time.Hour, query)
		}
		source := engine.Stats().Sources[sourceTimeSeries]
		if source.Samples[OutcomeCompleted] != 3 || source.ChangedWindows[RungNames[0]] != 0 || source.UnknownLookback != 0 {
			t.Fatalf("%s: samples %v changed %v unknown %d, want no change read where nothing arrived",
				name, source.Samples, source.ChangedWindows, source.UnknownLookback)
		}
	}
	w := &windowed{clock: &clock{at: time.Unix(1_700_006_000, 0)}, step: 60}
	engine := w.engine(t)
	w.run(t, engine, 2*time.Hour, facts(t, minute, "sum(rate(cpu_usage[$__interval]))", execution.QueryClause{}))
	if source := engine.Stats().Sources[sourceTimeSeries]; source.UnknownLookback != 1 {
		t.Fatalf("unknown lookback %d, want the unreadable range counted", source.UnknownLookback)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if from := w.specs[0].LogicalWindow.Start; from != 1_700_006_000-65*60-60 {
		t.Fatalf("an unknown lookback read from %d, want a step before the tail", from)
	}
}

// Data arriving at a constant delay is followed to where it stops, however
// late up to the deepest rung and however short the window: within three
// samples the Query Group reads deep enough to see its last point arrive,
// and the window is complete no earlier than the delay less a step and no
// later than the first rung at or past the delay. Data later than every rung
// its group reads over a window shorter than the delay - empty at each of
// them - is found by the deep recheck: that sample is probe_changed, and the
// next reads every rung and settles. A window empty at its first read whose
// data arrived later is counted as such.
func TestAConstantLatenessIsFollowedToWhereItStops(t *testing.T) {
	for _, window := range []time.Duration{time.Hour, 5 * minute} {
		for _, steps := range []float64{1.8, 3, 3.75, 5, 6, 10, 20, 40} {
			late := time.Duration(steps * float64(minute))
			w := &windowed{clock: &clock{at: time.Unix(1_700_006_000, 0)}, step: 60, late: late}
			engine := w.engine(t)
			query := facts(t, minute, "", execution.QueryClause{TimeAggregation: execution.QueryFunction{Method: "sum_over_time", Window: "60s"}})
			for range 3 {
				w.run(t, engine, window, query)
			}
			source := engine.Stats().Sources[sourceTimeSeries]
			engine.mu.Lock()
			state := *engine.groups["qg"]
			engine.mu.Unlock()
			name := fmt.Sprintf("window %v late %v", window, late)
			t.Logf("%s: depth %d, complete at %v, samples %v, empty first reads %v", name, state.depth, state.completion,
				source.Samples, source.EmptyFirstReads)
			reaches := time.Duration(RungSteps[len(RungSteps)-1] * float64(minute))
			for _, rung := range RungSteps {
				if at := time.Duration(rung * float64(minute)); at >= late {
					reaches = at
					break
				}
			}
			if !state.measured || state.completion < late-minute || state.completion > reaches {
				t.Fatalf("%s: measured %v at %v, want from the delay less a step to %v", name, state.measured, state.completion, reaches)
			}
			if time.Duration(RungSteps[state.depth-1]*float64(minute)) < state.completion {
				t.Fatalf("%s: depth %d reads to %v steps only", name, state.depth, RungSteps[state.depth-1])
			}
			if source.Samples[OutcomeCompleted] == 0 || source.Samples[OutcomeUnobserved] != 0 {
				t.Fatalf("%s: samples %v", name, source.Samples)
			}
			// Over five minutes, from ten steps late the window is still
			// empty at the first rung: only the deep recheck sees its data.
			if short := window == 5*minute && steps >= 10; short != (source.Samples[OutcomeProbeChanged] == 1) {
				t.Fatalf("%s: probe_changed %d", name, source.Samples[OutcomeProbeChanged])
			}
			// Over five minutes, from six steps late the first read is empty.
			arrived := uint64(0)
			for _, n := range source.EmptyFirstReadCompletion {
				arrived += n
			}
			if empty := window == 5*minute && steps >= 6; empty != (source.EmptyFirstReads[EmptyArrived] > 0) ||
				arrived != source.EmptyFirstReads[EmptyArrived] || source.EmptyFirstReads[EmptyStayedEmpty] != 0 {
				t.Fatalf("%s: empty first reads %v, completion %v", name, source.EmptyFirstReads, source.EmptyFirstReadCompletion)
			}
		}
	}
}

// A deep recheck that finds data later than every rung its Query Group
// reads makes its sample probe_changed - no completion, no clean sample -
// and the group reads every rung from its next sample on; the next completed
// sample sets its depth at once from where its data stopped, not a rung at a
// time. Waiting for its deep recheck, a sample does not hold the next one
// back.
func TestADeepRecheckThatFindsLateDataSettlesTheDepthAtOnce(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(point(slot, "1")), RecheckCompared)
	if state := f.group("qg"); state.sample != nil || state.probe == nil || state.depth != 1 {
		t.Fatalf("after its one rung: sample %v, deep recheck %v, depth %d", state.sample != nil, state.probe != nil, state.depth)
	}
	// The next sample is taken, and read, while the deep recheck waits.
	f.rest("qg")
	next := f.clock.now().Unix()
	f.capture(query("qg", next, minute, sourceLog), point(next, "1"))
	f.recheck(sourceLog, f.clock.now(), 0, minute, full(point(next, "1")), RecheckCompared)
	stats := f.recheck(sourceLog, readAt, len(RungSteps)-1, minute, full(point(slot, "9")), RecheckCompared)
	source := stats.Sources[sourceLog]
	state := f.group("qg")
	if source.Samples[OutcomeProbeChanged] != 1 || source.Samples[OutcomeCompleted] != 1 || source.Probes[ProbeChanged] != 1 ||
		state.depth != len(RungSteps) || !state.settle || state.clean != 0 || state.rest != RungSteps[len(RungSteps)-1] {
		t.Fatalf("samples %v probes %v depth %d settle %v clean %d rest %v", source.Samples, source.Probes, state.depth,
			state.settle, state.clean, state.rest)
	}
	completions := uint64(0)
	for _, n := range source.Completion {
		completions += n
	}
	if completions != 1 || source.ProbeChangedRatio != 0.5 || source.UnobservedRatio != 0 {
		t.Fatalf("completions %v, probe_changed ratio %v, unobserved ratio %v; want the completed sample's only, 0.5, 0",
			source.Completion, source.ProbeChangedRatio, source.UnobservedRatio)
	}
	// Read to every rung, its data changing up to the second: three rungs
	// from here, at once.
	f.rest("qg")
	f.sample("qg", sourceLog, minute, "1", func(rung int) string { return fmt.Sprint(10 + min(rung, 1)) })
	if state := f.group("qg"); state.depth != 3 || state.settle {
		t.Fatalf("settled at depth %d, settle %v; want 3", state.depth, state.settle)
	}
}

// A Query Group forgotten while a sample of it waits for its deep recheck
// counts that sample owner_lost, at the deepest rung, and leaves nothing
// pending.
func TestAForgottenQueryGroupDropsTheSampleWaitingForItsDeepRecheck(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	f.recheck(sourceLog, f.clock.now(), 0, minute, full(point(slot, "1")), RecheckCompared)
	f.engine.Forget("qg")
	stats := f.engine.Stats()
	source := stats.Sources[sourceLog]
	if source.Samples[OutcomeOwnerLost] != 1 || source.Rechecks[RungNames[len(RungNames)-1]][RecheckOwnerLost] != 1 || stats.Pending != 0 {
		t.Fatalf("samples %v rechecks %v pending %d", source.Samples, source.Rechecks[RungNames[len(RungNames)-1]], stats.Pending)
	}
}

// A deep recheck not read leaves its sample complete as its rungs read it,
// and the next sample is probed instead.
func TestADeepRecheckNotReadIsTriedAtTheNextSample(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(point(slot, "1")), RecheckCompared)
	deepest := len(RungSteps) - 1
	f.clock.set(readAt.Add(rungDelay(deepest, minute) + rungWindow(deepest, minute) + time.Second))
	f.engine.Step(context.Background())
	source := f.engine.Stats().Sources[sourceLog]
	if source.Probes[ProbeUnobserved] != 1 || source.Samples[OutcomeCompleted] != 1 || source.Rechecks[RungNames[deepest]][RecheckYielded] != 1 {
		t.Fatalf("probes %v samples %v", source.Probes, source.Samples)
	}
	f.rest("qg")
	next := f.clock.now().Unix()
	f.capture(query("qg", next, minute, sourceLog), point(next, "1"))
	f.engine.mu.Lock()
	probed := f.engine.groups["qg"].sample.probe
	f.engine.mu.Unlock()
	if !probed {
		t.Fatal("the sample after a deep recheck not read is not probed")
	}
}

// A completed sample whose first read was complete and held no point is
// counted by whether its data arrived at a later rung - with when it was
// complete - or stayed empty.
func TestAnEmptyFirstReadIsCountedByWhetherItsDataArrived(t *testing.T) {
	f := newFixture(t)
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog))
	readAt := f.clock.now()
	for rung, pending := f.rung("qg"); pending; rung, pending = f.rung("qg") {
		f.recheck(sourceLog, readAt, rung, minute, full(point(slot, "1")), RecheckCompared)
	}
	f.rest("qg")
	slot = f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog))
	readAt = f.clock.now()
	for rung, pending := f.rung("qg"); pending; rung, pending = f.rung("qg") {
		f.recheck(sourceLog, readAt, rung, minute, full(), RecheckCompared)
	}
	source := f.engine.Stats().Sources[sourceLog]
	if source.Samples[OutcomeCompleted] != 2 || source.EmptyFirstReads[EmptyArrived] != 1 || source.EmptyFirstReads[EmptyStayedEmpty] != 1 ||
		source.EmptyFirstReadCompletion["le_120s"] != 1 {
		t.Fatalf("samples %v empty first reads %v completion %v", source.Samples, source.EmptyFirstReads, source.EmptyFirstReadCompletion)
	}
}

// Punctual Query Groups, their formal reads every step, settle at about a
// sample an hour each and a deep recheck at one in four - 1 to 1.25
// rechecks an hour, a fifth of them deep - at a ten-second, a minute or a
// five-minute step: a sample waiting for its deep recheck, hours away at a
// long step, does not hold the next one back. A longer step is a little
// less: a group rests from its first rung, 1.5 steps after its read, and
// its next sample waits for its next first read.
func TestPunctualQueryGroupsSettleAtAboutOnePointTwoRechecksAnHourWhateverTheStep(t *testing.T) {
	for _, step := range []time.Duration{10 * time.Second, minute, 5 * minute} {
		perHour, deep := punctualRecheckRate(t, step)
		t.Logf("step %v: %.2f rechecks a Query Group an hour, %.0f%% deep", step, perHour, 100*deep)
		if perHour < 0.95 || perHour > 1.35 || deep < 0.15 || deep > 0.25 {
			t.Fatalf("step %v: %.2f rechecks a Query Group an hour, %.2f of them deep; want 1 to 1.25, a fifth deep", step, perHour, deep)
		}
	}
}

// punctualRecheckRate runs Query Groups whose data is complete at the first
// read, reading every step, and returns their rechecks per group an hour
// once settled, and the share of those that were deep.
func punctualRecheckRate(t *testing.T, step time.Duration) (float64, float64) {
	t.Helper()
	c := &clock{at: time.Unix(1_700_006_400, 0)}
	const groups, warmup, measured = 32, 4 * time.Hour, 12 * time.Hour
	engine, err := New(Options{Now: c.now, UnspreadFirstSamples: true,
		Recheck: func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			_ = sink.ConsumeProviderSeries(ctx, execution.ProviderSeriesBatch{Dataset: point(spec.LogicalWindow.End, "1")})
			return execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil
		},
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return groups }})
	if err != nil {
		t.Fatal(err)
	}
	settled := func() bool {
		engine.mu.Lock()
		defer engine.mu.Unlock()
		for _, state := range engine.groups {
			for _, candidate := range [...]*sample{state.sample, state.probe} {
				if candidate != nil && candidate.running {
					return false
				}
			}
		}
		return true
	}
	count := func() (rechecks, probes uint64) {
		source := engine.Stats().Sources[sourceTimeSeries]
		for _, outcomes := range source.Rechecks {
			rechecks += outcomes[RecheckCompared]
		}
		return rechecks, source.Probes[ProbeClean]
	}
	start := c.now()
	var rechecksBefore, probesBefore uint64
	for tick := time.Duration(0); tick < warmup+measured; tick += step {
		if tick == warmup {
			rechecksBefore, probesBefore = count()
		}
		c.set(start.Add(tick))
		slot := c.now().Unix()
		for index := 0; index < groups; index++ {
			if read := engine.Begin(query(fmt.Sprintf("qg-%d", index), slot, step, sourceTimeSeries)); read.summary != nil {
				read.Series(point(slot, "1"), 100)
				read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
			}
		}
		engine.Step(context.Background())
		deadline := time.Now().Add(5 * time.Second)
		for !settled() {
			if time.Now().After(deadline) {
				t.Fatalf("step %v at %v: rechecks did not settle", step, tick)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	rechecksAfter, probesAfter := count()
	rechecks, probes := rechecksAfter-rechecksBefore, probesAfter-probesBefore
	return float64(rechecks) / groups / measured.Hours(), float64(probes) / float64(rechecks)
}

// A rung compared covers the rungs not read before it: it is compared with
// the last read kept, so a window whose first rung found no permit and whose
// second found nothing new is complete, not unobserved.
func TestAComparedRungCoversTheRungsNotReadBeforeIt(t *testing.T) {
	f := newFixture(t)
	f.sample("qg", sourceLog, minute, "1", func(int) string { return "2" }) // depth 2 from here
	f.rest("qg")
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "5"))
	readAt := f.clock.now()
	f.clock.set(readAt.Add(rungDelay(0, minute) + rungWindow(0, minute) + time.Second))
	f.engine.Step(context.Background()) // the first rung passes without a permit
	stats := f.recheck(sourceLog, readAt, 1, minute, full(point(slot, "5")), RecheckCompared)
	source := stats.Sources[sourceLog]
	if source.Rechecks[RungNames[0]][RecheckYielded] != 1 || source.Samples[OutcomeCompleted] != 2 || source.Samples[OutcomeUnobserved] != 0 {
		t.Fatalf("rechecks %v samples %v, want the second sample complete", source.Rechecks[RungNames[0]], source.Samples)
	}
}

// A query's lookback is its longest window, function window and offset, or
// its PromQL ranges, subquery ranges and offsets, in the durations the query
// service writes; one it cannot read is unknown.
func TestQueryLookbackReadsWindowsRangesAndOffsets(t *testing.T) {
	structured := execution.QueryPlanFacts{QueryList: []execution.QueryClause{
		{TimeAggregation: execution.QueryFunction{Window: "60s"}, Functions: []execution.QueryFunction{{Window: "4m"}, {Window: "2m"}}, Offset: "1h"},
		{TimeAggregation: execution.QueryFunction{Window: "30m"}},
	}}
	for name, want := range map[string]struct {
		facts    execution.QueryPlanFacts
		lookback time.Duration
		known    bool
	}{
		// 60s, the longest function window 4m and the offset 1h; the second
		// clause's 30m is shorter.
		"structured": {structured, 65 * time.Minute, true},
		"range and offset": {execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: "sum(rate(x[5m] offset 1h))"}},
			65 * time.Minute, true},
		"subquery": {execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: "max_over_time(rate(x[1m])[30m:1m])"}},
			30 * time.Minute, true},
		"negative offset": {execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: "x offset -1d"}}, 24 * time.Hour, true},
		"no range":        {execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: "sum(x)"}}, 0, true},
		"variable range":  {execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: "rate(x[$__interval])"}}, 0, false},
		"bad window": {execution.QueryPlanFacts{QueryList: []execution.QueryClause{{TimeAggregation: execution.QueryFunction{Window: "1 minute"}}}},
			0, false},
	} {
		lookback, known := queryLookback(want.facts)
		if lookback != want.lookback || known != want.known {
			t.Fatalf("%s: %v %v, want %v %v", name, lookback, known, want.lookback, want.known)
		}
	}
	for text, want := range map[string]time.Duration{"": 0, "5ms": 5 * time.Millisecond, "90s": 90 * time.Second, "1w": 7 * 24 * time.Hour,
		"1h30m": 90 * time.Minute} {
		if got, ok := parseDuration(text); !ok || got != want {
			t.Fatalf("parseDuration(%q) = %v %v, want %v", text, got, ok, want)
		}
	}
}
