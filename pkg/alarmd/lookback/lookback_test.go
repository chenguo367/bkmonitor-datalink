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
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
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
			_ = sink.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{Dataset: one})
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
	engine, err := New(Options{Now: f.clock.now, Sources: []string{sourceTimeSeries, sourceLog}, Refusals: []string{"waiters", "headroom"},
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

// capture takes q as its Query Group's sample, now, with data points.
func (f *fixture) capture(q Query, datasets ...*execution.Dataset) {
	f.t.Helper()
	read := f.engine.Begin(q)
	if read == nil {
		f.t.Fatalf("%s at %d was not taken as a sample", q.Contract.Slot.QueryGroup, q.Contract.Slot.EvaluationTime)
	}
	for _, one := range datasets {
		read.Series(one)
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
// by chance. A retry or a recovery read is never a sample.
func TestEveryOwnedQueryGroupKeepsOneSampleAtATime(t *testing.T) {
	f := newFixture(t)
	first := query("qg", 1_700_000_100, minute, sourceLog)
	read := f.engine.Begin(first)
	if read == nil {
		t.Fatal("the first read of an owned Query Group was not taken")
	}
	if f.engine.Begin(first) != nil {
		t.Fatal("a second physical query of the Slot was taken while the first was being read")
	}
	read.Series(dataset("h1", steady))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if f.engine.Begin(query("qg", 1_700_000_160, minute, sourceLog)) != nil {
		t.Fatal("the next Slot was taken while the sample was in flight")
	}
	retry := query("qg-b", 1_700_000_100, minute, sourceTimeSeries)
	retry.AttemptNo = 2
	recovery := query("qg-b", 1_700_000_100, minute, sourceTimeSeries)
	recovery.Operation = execution.OperationReplay
	if f.engine.Begin(retry) != nil || f.engine.Begin(recovery) != nil {
		t.Fatal("a retry or a recovery read was taken as a sample")
	}
	f.capture(query("qg-b", 1_700_000_100, minute, sourceTimeSeries), dataset("h1", steady))
	stats := f.engine.Stats()
	if stats.Pending != 2 || stats.PendingBytes != 2*summaryEntryBytes {
		t.Fatalf("pending %d bytes %d, want two one-bucket samples", stats.Pending, stats.PendingBytes)
	}
	if stats.Coverage != (Coverage{Owned: 2, Covered: 2, Ratio: 1}) {
		t.Fatalf("coverage %+v, want both owned Query Groups", stats.Coverage)
	}
	if stats.Sources[sourceLog].FirstReads != 3 || stats.Sources[sourceTimeSeries].FirstReads != 1 {
		t.Fatalf("first reads log %d time series %d", stats.Sources[sourceLog].FirstReads, stats.Sources[sourceTimeSeries].FirstReads)
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

// A source whose data is always complete at the first read reads one rung
// and rests longer after each clean sample, doubling up to 64 steps: about
// one recheck an hour per Query Group at a minute step.
func TestAPunctualSourceStaysShallowAndRestsUpToSixtyFourSteps(t *testing.T) {
	f := newFixture(t)
	rests := []float64{}
	for sample := 0; sample < 8; sample++ {
		slot := f.clock.now().Unix()
		f.capture(query("qg", slot, minute, sourceTimeSeries), dataset("h1", steady))
		readAt := f.clock.now()
		stats := f.recheck(sourceTimeSeries, readAt, 0, minute, full(dataset("h1", steady)), RecheckCompared)
		source := stats.Sources[sourceTimeSeries]
		if source.Depth != 1 || source.Samples[OutcomeCompleted] != uint64(sample+1) {
			t.Fatalf("sample %d: depth %d completed %d", sample, source.Depth, source.Samples[OutcomeCompleted])
		}
		rests = append(rests, source.RestSteps)
		finished := f.clock.now()
		rest := time.Duration(source.RestSteps * restSpread("qg") * float64(minute))
		// Not before its rest has passed, and at once after.
		f.clock.set(finished.Add(rest - time.Second))
		if f.engine.Begin(query("qg", slot+60, minute, sourceTimeSeries)) != nil {
			t.Fatalf("sample %d: a Query Group was sampled before its rest of %v passed", sample, rest)
		}
		f.clock.set(finished.Add(rest))
	}
	want := []float64{3, 6, 12, 24, 48, 64, 64, 64}
	for index := range want {
		if rests[index] != want[index] {
			t.Fatalf("rests %v, want %v", rests, want)
		}
	}
	stats := f.engine.Stats()
	if stats.Sources[sourceTimeSeries].Completion["le_60s"] != 8 || stats.Sources[sourceTimeSeries].ChangedWindows[RungNames[0]] != 0 {
		t.Fatalf("completion %v changed %v", stats.Sources[sourceTimeSeries].Completion, stats.Sources[sourceTimeSeries].ChangedWindows)
	}
}

// A source whose data still arrives at the last planned rung is followed a
// rung further within the same sample, the window is complete at the last
// rung that changed, the source reads one rung past it from then on, and
// its Query Groups rest only as long as that deepest rung.
func TestALateSourceIsFollowedToWhereItsDataStops(t *testing.T) {
	f := newFixture(t)
	f.capture(query("qg", 1_700_000_100, minute, sourceLog), dataset("h1", map[int64]string{1_700_000_040: "1"}))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(dataset("h1", map[int64]string{1_700_000_040: "4"})), RecheckCompared)
	f.recheck(sourceLog, readAt, 1, minute, full(dataset("h1", map[int64]string{1_700_000_040: "4"}), dataset("h2", map[int64]string{1_700_000_040: "1"})), RecheckCompared)
	stats := f.recheck(sourceLog, readAt, 2, minute, full(dataset("h1", map[int64]string{1_700_000_040: "4"}), dataset("h2", map[int64]string{1_700_000_040: "1"})), RecheckCompared)
	source := stats.Sources[sourceLog]
	if source.ChangedWindows[RungNames[0]] != 1 || source.ChangedWindows[RungNames[1]] != 1 || source.ChangedWindows[RungNames[2]] != 0 {
		t.Fatalf("changed windows %v", source.ChangedWindows)
	}
	if source.Changes[RungNames[0]][ChangeValuesChanged] != 1 || source.Changes[RungNames[1]][ChangePointsAdded] != 1 {
		t.Fatalf("changes %v", source.Changes)
	}
	if source.Samples[OutcomeCompleted] != 1 || source.Depth != 3 || source.RestSteps != RungSteps[2] {
		t.Fatalf("completed %d depth %d rest %v, want depth 3 resting 7.5 steps", source.Samples[OutcomeCompleted], source.Depth, source.RestSteps)
	}
	// Complete at the second rung: 3.5 steps after the read, which was 60s
	// past the window's end.
	lateness := time.Duration(RungSteps[1]*float64(minute)) + readAt.Sub(time.Unix(1_700_000_100, 0))
	if len(stats.Latest) != 1 || stats.Latest[0].CompletionSeconds != int64(lateness/time.Second) ||
		source.MaxCompletionSeconds != int64(lateness/time.Second) || source.Completion[ageBucket(lateness)] != 1 {
		t.Fatalf("latest %+v max %d completion %v, want %v", stats.Latest, source.MaxCompletionSeconds, source.Completion, lateness)
	}
	if len(stats.Recent) != 2 || stats.Recent[1].Rung != RungNames[1] {
		t.Fatalf("recent %+v", stats.Recent)
	}
}

// A source reads one rung less only after cleanSamplesToShallow samples in
// a row needed less; one sample that needed the rung again restarts the
// count.
func TestADeepSourceShallowsOnlyAfterCleanSamplesInARow(t *testing.T) {
	f := newFixture(t)
	// sample reads one sample to its end, its data changing at every rung up
	// to lastChange and at none after it.
	sample := func(lastChange int) SourceStats {
		t.Helper()
		completed := f.engine.Stats().Sources[sourceLog].Samples[OutcomeCompleted]
		f.capture(query("qg", f.clock.now().Unix(), minute, sourceLog), dataset("h1", steady))
		readAt := f.clock.now()
		stats := f.engine.Stats()
		for rung := 0; stats.Sources[sourceLog].Samples[OutcomeCompleted] == completed; rung++ {
			value := fmt.Sprint(10 + min(rung, lastChange))
			if lastChange < 0 {
				value = "3"
			}
			stats = f.recheck(sourceLog, readAt, rung, minute, full(dataset("h1", map[int64]string{1_700_000_040: value})), RecheckCompared)
		}
		source := stats.Sources[sourceLog]
		f.clock.set(f.clock.now().Add(time.Duration(source.RestSteps * restSpread("qg") * float64(minute))))
		return source
	}
	if got := sample(1); got.Depth != 3 {
		t.Fatalf("data changing up to the second rung left depth %d, want 3", got.Depth)
	}
	for clean := 1; clean < cleanSamplesToShallow; clean++ {
		if got := sample(-1); got.Depth != 3 {
			t.Fatalf("after %d clean samples depth %d, want 3", clean, got.Depth)
		}
	}
	if got := sample(1); got.Depth != 3 {
		t.Fatalf("a late sample did not hold the depth: %d", got.Depth)
	}
	for clean := 1; clean < cleanSamplesToShallow; clean++ {
		sample(-1)
	}
	if got := sample(-1); got.Depth != 2 {
		t.Fatalf("after %d clean samples in a row depth %d, want 2", cleanSamplesToShallow, got.Depth)
	}
}

// Every rung not observed is named and never counted as a window that did
// not change: no permit through its window (yielded), a failed read, a
// partial one. A refused permit is counted by its reason, an unnamed one
// as other.
func TestEveryUnobservedRungIsNamed(t *testing.T) {
	f := newFixture(t)
	f.capture(query("qg", 1_700_000_100, minute, sourceLog), dataset("h1", steady))
	readAt := f.clock.now()
	f.set(func() { f.refuse = "waiters" })
	f.clock.set(readAt.Add(rungDelay(0, minute)))
	f.engine.Step(context.Background())
	f.set(func() { f.refuse = "unnamed" })
	f.engine.Step(context.Background())
	f.clock.set(readAt.Add(rungDelay(0, minute) + rungWindow(0, minute) + time.Second))
	f.engine.Step(context.Background())
	stats := f.engine.Stats()
	if stats.Sources[sourceLog].Rechecks[RungNames[0]][RecheckYielded] != 1 {
		t.Fatalf("rechecks %v, want the first rung yielded", stats.Sources[sourceLog].Rechecks[RungNames[0]])
	}
	if stats.PermitRefusals["waiters"] != 1 || stats.PermitRefusals[RefusedOther] != 1 || stats.PermitRefusals["headroom"] != 0 {
		t.Fatalf("refusals %v", stats.PermitRefusals)
	}
	f.set(func() { f.refuse = "" })
	// Yielding the only planned rung finished the sample, as observed at the first read.
	if stats.Sources[sourceLog].Samples[OutcomeCompleted] != 1 {
		t.Fatalf("samples %v", stats.Sources[sourceLog].Samples)
	}
	f.clock.set(f.clock.now().Add(time.Duration(stats.Sources[sourceLog].RestSteps * restSpread("qg") * float64(minute))))
	f.capture(query("qg", 1_700_010_100, minute, sourceLog), dataset("h1", steady))
	readAt = f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, func(execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		return execution.ProviderCompletion{}, errors.New("timeout")
	}, RecheckFailed)
	f.clock.set(f.clock.now().Add(time.Hour))
	f.capture(query("qg", 1_700_020_100, minute, sourceLog), dataset("h1", steady))
	readAt = f.clock.now()
	stats = f.recheck(sourceLog, readAt, 0, minute, func(execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		return execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil
	}, RecheckPartial)
	if stats.Sources[sourceLog].ChangedWindows[RungNames[0]] != 0 || stats.Sources[sourceLog].Rechecks[RungNames[0]][RecheckCompared] != 0 {
		t.Fatalf("an unobserved rung counted as compared: %v", stats.Sources[sourceLog].Rechecks[RungNames[0]])
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
	stats := f.recheck(sourceLog, readAt, 0, minute, full(dataset("h1", steady)), RecheckCompared)
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
	if stats.Coverage != (Coverage{Owned: 1, Covered: 1, Ratio: 1}) {
		t.Fatalf("coverage %+v", stats.Coverage)
	}
	source := stats.Sources[sourceLog]
	cycle := time.Duration((source.RestSteps + RungSteps[source.Depth-1]) * float64(minute))
	f.clock.set(f.clock.now().Add(2*cycle + time.Second))
	if got := f.engine.Stats().Coverage; got.Covered != 0 || got.Owned != 1 {
		t.Fatalf("a stale measurement covered: %+v", got)
	}
}

// Every source is counted under a bounded label from the start: the named
// ones, and mixed, promql and other for the rest.
func TestSourcesAreBoundedAndCountedFromTheStart(t *testing.T) {
	f := newFixture(t)
	stats := f.engine.Stats()
	for _, source := range []string{sourceTimeSeries, sourceLog, SourceMixed, SourcePromQL, SourceOther} {
		entry, present := stats.Sources[source]
		if !present || len(entry.Samples) != len(SampleOutcomes) || len(entry.Rechecks) != len(RungNames) || entry.Depth != 1 {
			t.Fatalf("source %s is not counted from the start: %+v", source, entry)
		}
	}
	if len(stats.Sources) != 5 {
		t.Fatalf("sources %d, want 5", len(stats.Sources))
	}
	promql := query("qg", 1_700_000_100, minute)
	promql.Spec.PlanFacts.PromQL = &execution.PromQLQuery{}
	for want, facts := range map[string]execution.QueryPlanFacts{
		SourceMixed:  {SourceSemantics: []string{sourceLog, sourceTimeSeries}},
		SourcePromQL: promql.Spec.PlanFacts,
		SourceOther:  {SourceSemantics: []string{"custom/event"}},
		sourceLog:    {SourceSemantics: []string{sourceLog}},
	} {
		if got := sourceOf(facts, f.engine.named); got != want {
			t.Fatalf("source of %+v = %s, want %s", facts.SourceSemantics, got, want)
		}
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
	read.Series(dataset("h1", points))
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

// Query Groups of one source rest as long on average, each spread by its own
// hash between three quarters of the rest and a quarter past it: groups read
// at one moment are not sampled, and rechecked, at one moment again.
func TestQueryGroupsRestSpreadAroundTheirSourcesRest(t *testing.T) {
	sum, low, high := 0.0, 2.0, 0.0
	const groups = 1000
	for index := 0; index < groups; index++ {
		spread := restSpread(execution.QueryGroupIdentity(fmt.Sprintf("qg-%d", index)))
		if spread < 0.75 || spread >= 1.25 {
			t.Fatalf("qg-%d rests %v of its source's rest", index, spread)
		}
		sum, low, high = sum+spread, min(low, spread), max(high, spread)
	}
	if mean := sum / groups; mean < 0.97 || mean > 1.03 || high-low < 0.45 {
		t.Fatalf("spreads mean %v from %v to %v, want about 1 over most of the range", mean, low, high)
	}
}

// A source late by the same rungs every time reads that deep, and rests like
// a punctual one once that holds: only the sample that deepened it brings
// its Query Groups back sooner. The rechecks follow how deep the lateness
// goes, not how often it is seen again.
func TestAStablyLateSourceRestsUpToSixtyFourStepsAtItsDepth(t *testing.T) {
	f := newFixture(t)
	rests := []float64{}
	for sample := 0; sample < 7; sample++ {
		f.capture(query("qg", f.clock.now().Unix(), minute, sourceLog), dataset("h1", steady))
		readAt := f.clock.now()
		completed := f.engine.Stats().Sources[sourceLog].Samples[OutcomeCompleted]
		stats := f.engine.Stats()
		// Every sample: the data changes by the first rung and not after it.
		for rung := 0; stats.Sources[sourceLog].Samples[OutcomeCompleted] == completed; rung++ {
			stats = f.recheck(sourceLog, readAt, rung, minute, full(dataset("h1", map[int64]string{1_700_000_040: fmt.Sprint(100 + sample)})), RecheckCompared)
		}
		source := stats.Sources[sourceLog]
		if source.Depth != 2 {
			t.Fatalf("sample %d: depth %d, want 2 - one past the first rung", sample, source.Depth)
		}
		rests = append(rests, source.RestSteps)
		f.clock.set(f.clock.now().Add(time.Duration(source.RestSteps * restSpread("qg") * float64(minute))))
	}
	want := []float64{3.5, 7, 14, 28, 56, 64, 64}
	for index := range want {
		if rests[index] != want[index] {
			t.Fatalf("rests %v, want %v", rests, want)
		}
	}
}
