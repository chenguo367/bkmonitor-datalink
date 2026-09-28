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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// thresholdPlan compiles a real one-Level static threshold Plan, the way the
// formal detector gets one: value >= threshold is abnormal.
func thresholdPlan(t testing.TB, threshold string) *strategy.CompiledPlan {
	t.Helper()
	config, _ := json.Marshal(map[string]any{"value_field": "value", "data_unit": "", "threshold_unit_prefix": "",
		"precision": map[string]any{"decimal_places": 6, "rounding": "HALF_EVEN"},
		"groups":    []any{map[string]any{"conditions": []any{map[string]any{"operator": "GTE", "threshold_decimal": threshold}}}}})
	level := contract.LevelIRV2{Definition: contract.LevelDefinitionV2{LevelID: 1, Priority: 1}, Connector: contract.LevelConnectorAND,
		DetectPlan:   contract.DetectPlanV2{Algorithms: []contract.AlgorithmIRV2{{Type: strategy.DetectorKindThreshold, Version: 1, Config: config}}},
		TriggerPlan:  contract.TypedPlanV1{Type: strategy.TriggerPlanTypeNOfM, Version: 1, Config: json.RawMessage(`{"window_size":1,"required_anomalies":1,"step_seconds":60}`)},
		RecoveryPlan: contract.TypedPlanV1{Type: strategy.RecoveryPlanTypeContinuousTriggerMiss, Version: 1, Config: json.RawMessage(`{"enabled":true,"consecutive_windows":1}`)}}
	ref := contract.StrategyRefV2{TenantID: "default", StrategyID: "1001", Revision: "r1"}
	projection := contract.InputProjectionV2{ValueFields: []string{"value"}, DimensionFields: []string{"host"}, BusinessIdentityField: "bk_biz_id",
		MultiValueAlignment: "SINGLE_VALUE", DataUnit: "", MissingValuePolicy: contract.MissingValuePolicyRequired}
	plan := contract.EvaluationPlanV2{PlanID: "1001", StrategyRef: ref, InputProjection: projection, StrategyIR: contract.StrategyIRV2{
		Schema: contract.Schema{Name: contract.StrategyIRSchemaV2, Major: 2, Minor: 0}, RequiredFeatures: []string{}, StrategyRef: ref,
		ExecutionSemantics: contract.ExecutionSemanticsV2{EvaluationScope: contract.EvaluationScopeSeries, QueryWindow: 300, AggregationInterval: 60,
			EvaluationInterval: 60, LatenessTolerance: 120},
		InputProjection: projection, Levels: []contract.LevelIRV2{level}}}
	compiler, err := strategy.NewCompiler(strategy.NewDefaultAlgorithmCompilerRegistry(), strategy.Limits{
		MaxPlanBytes: 1 << 20, MaxLevelsPerPlan: 32, MaxAlgorithmsPerLevel: 32, MaxGroupsPerAlgorithm: 64,
		MaxConditionsPerAlgorithm: 256, MaxASTNodesPerLevel: 4096, MaxRequiredHistoryPoints: 4096,
		MaxTriggerWindowSize: 4096, MaxRecoveryConsecutiveWindows: 4096, MaxTriggerComputeCost: 1 << 20,
		MaxCompiledPlanBytes: 1 << 20, MaxCacheEntries: 128, MaxCacheBytes: 16 << 20, NegativeCacheTTL: time.Minute, BudgetRevision: "test"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiler.Compile(context.Background(), strategy.CompileRequest{Plan: plan,
		DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64), NormalizationDigest: strings.Repeat("2", 64),
			IdentityFields: []string{"host"}, SourceTimeField: "time", ReceivedTimeField: "received_time"},
		StateSemantics: strategy.StateSemantics{StateSchemaVersion: "s", CodecSemanticsVersion: "c", IdentitySchemaDigest: strings.Repeat("3", 64),
			SourceTimeSemanticsVersion: "t", HistoryCellSemanticsVersion: "h"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled, ok := result.Plan()
	if !ok {
		t.Fatalf("threshold plan did not compile: %#v", result.PlanTerminal())
	}
	return compiled
}

var planA = execution.PlanIdentity{StrategyID: "1001"}

func series(points map[int64]string) *seriesRead {
	return read(1, "", points)
}

func read(admitted uint64, dims string, points map[int64]string) *seriesRead {
	kept := &seriesRead{admitted: admitted, dims: dims}
	for _, bucket := range sortedBuckets(points) {
		kept.buckets = append(kept.buckets, bucket)
		kept.text = append(kept.text, points[bucket]...)
		kept.ends = append(kept.ends, uint32(len(kept.text)))
	}
	return kept
}

func sortedBuckets(points map[int64]string) []int64 {
	buckets := make([]int64, 0, len(points))
	for bucket := range points {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	return buckets
}

// Every class of difference, and the verdict under the original threshold:
// the unit is (series, bucket), a missing record is not a 0, and only a
// Plan that admitted the series at the first read judges it.
func TestCompareClassesEveryDifferenceAndJudgesUnderTheFrozenThreshold(t *testing.T) {
	plans := []planCheck{planCheckOf(planA, thresholdPlan(t, "5"), "value")}
	if !plans[0].comparable {
		t.Fatal("a static threshold Plan is not comparable")
	}
	first := readSet{
		"h1": series(map[int64]string{60: "0", 120: "2", 180: "4", 240: "6"}),
		"h2": series(map[int64]string{60: "5"}),
		"h4": read(0, "", map[int64]string{60: "1"}),
	}
	recheck := readSet{
		"h1": read(0, `host="h1"`, map[int64]string{60: "1", 120: "7", 180: "4", 300: "9"}),
		"h3": read(0, "", map[int64]string{60: "3"}),
		"h4": read(0, "", map[int64]string{60: "8"}),
	}
	got := compare(first, recheck, plans)
	want := map[string]int{DiffZeroToNonzero: 1, DiffIncreased: 2, DiffUnchanged: 1, DiffVanishedPoint: 1, DiffNewPoint: 1,
		DiffVanishedSeries: 1, DiffNewSeries: 1}
	for class, n := range want {
		if got.differences[class] != n {
			t.Fatalf("differences %v, want %v", got.differences, want)
		}
	}
	if got.buckets != 8 || got.newSeries != 1 || got.vanishedSeries != 1 || !got.differsInWindow {
		t.Fatalf("buckets %d new %d vanished %d differs %t", got.buckets, got.newSeries, got.vanishedSeries, got.differsInWindow)
	}
	// h1: 0->1 normal->normal, 2->7 normal->abnormal, 4->4 unchanged, absent->9 abnormal.
	// h4 was not admitted by the Plan: 1->8 is data only. h3 is new: no judgment.
	wantJudge := map[string]int{JudgeUnchanged: 2, JudgeNormalToAbnormal: 1, JudgeAbsentToAbnormal: 1}
	for class, n := range wantJudge {
		if got.judgments[class] != n {
			t.Fatalf("judgments %v, want %v", got.judgments, wantJudge)
		}
	}
	if got.judgments[JudgeAbnormalToNormal] != 0 || len(got.examples) == 0 || got.examples[0].Dimensions == "" && got.examples[0].Class != DiffVanishedSeries {
		t.Fatalf("judgments %v examples %+v", got.judgments, got.examples)
	}
	if unchanged := compare(first, first, plans); unchanged.differsInWindow || unchanged.differences[DiffUnchanged] != 6 {
		t.Fatalf("a read compared with itself differs: %+v", unchanged.differences)
	}
	if got := compare(readSet{"h1": series(map[int64]string{60: "6"})}, readSet{"h1": series(map[int64]string{60: "2"})}, plans); got.judgments[JudgeAbnormalToNormal] != 1 || got.differences[DiffDecreased] != 1 {
		t.Fatalf("6 -> 2 under >= 5: %v %v", got.differences, got.judgments)
	}
	notComparable := []planCheck{{identity: planA}}
	if got := compare(first, recheck, notComparable); len(got.judgments) != 0 || got.differences[DiffIncreased] != 2 {
		t.Fatalf("a Plan the lookback cannot decide was judged: %v", got.judgments)
	}
}

type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func logSpec(digest string) execution.PhysicalQuerySpec {
	return execution.PhysicalQuerySpec{Digest: execution.PhysicalQueryDigest(digest),
		LogicalWindow: execution.QueryWindow{Start: 1_700_000_000, End: 1_700_000_060},
		PlanFacts: execution.QueryPlanFacts{SourceSemantics: []string{SourceLogSearch},
			QueryList:     []execution.QueryClause{{DataSource: "bklog", TimeAggregation: execution.QueryFunction{Method: "count_over_time"}}},
			Normalization: execution.DatasetNormalizationSpec{CanonicalValueField: "value"}}}
}

// sampledQuery finds a Slot the fixed sampling picks for the spec.
func sampledQuery(t testing.TB, spec execution.PhysicalQuerySpec, plans []execution.DuePlan) Query {
	t.Helper()
	for at := execution.EvaluationTime(60); at < 60*100000; at += 60 {
		if sampled(spec.Digest, at) {
			return Query{Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "qg", EvaluationTime: at}},
				Spec: spec, Operation: execution.OperationNormal, AttemptNo: 1, Plans: plans}
		}
	}
	t.Fatal("no sampled Slot")
	return Query{}
}

func dataset(host string, points map[int64]string) *execution.Dataset {
	records := make([]contract.CanonicalRecordV2, 0, len(points))
	for _, at := range sortedBuckets(points) {
		value := points[at]
		records = append(records, contract.CanonicalRecordV2{RecordID: fmt.Sprintf("%s-%d", host, at), SourceTime: at,
			DimensionIdentity: contract.DimensionIdentityV2{Digest: "digest-" + host},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(value)},
			Dimensions:        map[string]json.RawMessage{"host": json.RawMessage(`"` + host + `"`)}})
	}
	return execution.NewDataset(records)
}

type fixture struct {
	clock   *clock
	engine  *Engine
	answers chan func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error)
	refuse  string
	owned   bool
	mu      sync.Mutex
}

func newFixture(t *testing.T, memory int) *fixture {
	t.Helper()
	f := &fixture{clock: &clock{at: time.Unix(1_700_000_100, 0)}, owned: true,
		answers: make(chan func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error), 8)}
	engine, err := New(Options{Now: f.clock.now, MemoryBytes: memory,
		Recheck: func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("a recheck ran without a deadline")
			}
			answer := <-f.answers
			return answer(sink)
		},
		Permit: func() (func(), string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.refuse != "" {
				return nil, f.refuse
			}
			return func() {}, ""
		},
		Owns: func(execution.QueryGroupIdentity) bool { f.mu.Lock(); defer f.mu.Unlock(); return f.owned }})
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

func full(series ...*execution.Dataset) func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	return func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		for _, one := range series {
			_ = sink.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{Dataset: one})
		}
		return execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil
	}
}

func (f *fixture) waitRechecks(t *testing.T, source, tier, outcome string, n uint64) Stats {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats := f.engine.Stats()
		if stats.Rechecks[source][tier][outcome] >= n {
			return stats
		}
		if time.Now().After(deadline) {
			t.Fatalf("rechecks %v, waiting for %s/%s/%s >= %d", stats.Rechecks[source], source, tier, outcome, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A sampled first read of 0 that is 1 when read again: counted as a zero
// becoming non-zero and, under the original >= 1, a normal becoming
// abnormal; rechecked at each tier; released when the last tier is done.
// Nothing here can write a State: the engine has no port to one.
func TestASampledReadIsRecheckedAtEveryTierAndReleased(t *testing.T) {
	f := newFixture(t, 1<<20)
	query := sampledQuery(t, logSpec("q-zero"), []execution.DuePlan{{Identity: planA, CompiledPlan: thresholdPlan(t, "1")}})
	read := f.engine.Begin(query)
	if read == nil {
		t.Fatal("a sampled log query was not kept")
	}
	read.Series(dataset("h1", map[int64]string{60: "0"}), nil)
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if stats := f.engine.Stats(); stats.Pending != 1 || stats.PendingBytes == 0 || stats.Samples[SourceLogSearch][OutcomeCaptured] != 1 {
		t.Fatalf("after capture %+v", stats.Samples)
	}
	f.engine.Step(context.Background())
	if f.engine.Stats().Rechecks[SourceLogSearch]["t90"][RecheckCompared] != 0 {
		t.Fatal("rechecked before its tier")
	}
	for index, offset := range []time.Duration{90 * time.Second, 120 * time.Second, 300 * time.Second} {
		f.clock.advance(offset)
		f.answers <- full(dataset("h1", map[int64]string{60: "1"}))
		f.engine.Step(context.Background())
		f.waitRechecks(t, SourceLogSearch, TierNames[index], RecheckCompared, 1)
	}
	stats := f.engine.Stats()
	if stats.Differences[SourceLogSearch]["t90"][DiffZeroToNonzero] != 1 || stats.Judgments[SourceLogSearch]["t90"][JudgeNormalToAbnormal] != 1 ||
		stats.ComparedBuckets[SourceLogSearch]["t510"] != 1 || stats.ComparedWindows[SourceLogSearch]["t210"]["yes"] != 1 {
		t.Fatalf("differences %v judgments %v buckets %v windows %v", stats.Differences[SourceLogSearch], stats.Judgments[SourceLogSearch],
			stats.ComparedBuckets[SourceLogSearch], stats.ComparedWindows[SourceLogSearch])
	}
	if stats.Pending != 0 || stats.PendingBytes != 0 || stats.Samples[SourceLogSearch][OutcomeCompleted] != 1 {
		t.Fatalf("after the last tier pending %d bytes %d samples %v", stats.Pending, stats.PendingBytes, stats.Samples[SourceLogSearch])
	}
	if len(stats.Recent) != 3 || stats.Recent[0].ReadAgeSeconds != 130 || stats.Recent[0].Examples[0].Class != DiffZeroToNonzero {
		t.Fatalf("recent %+v", stats.Recent)
	}
	if stats.ByAge[SourceLogSearch]["le_240s"]["yes"] != 1 || stats.ByAge[SourceLogSearch]["le_360s"]["yes"] != 1 || stats.ByAge[SourceLogSearch]["le_600s"]["yes"] != 1 {
		t.Fatalf("delay profile %v", stats.ByAge[SourceLogSearch])
	}
}

// A first read with no series at all is a sample: the empty dimension set
// is exactly what later arrivals are measured against.
func TestAnEmptyFirstReadIsKeptAndItsNewSeriesAreDataOnly(t *testing.T) {
	f := newFixture(t, 1<<20)
	read := f.engine.Begin(sampledQuery(t, logSpec("q-empty"), []execution.DuePlan{{Identity: planA, CompiledPlan: thresholdPlan(t, "1")}}))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	f.clock.advance(90 * time.Second)
	f.answers <- full(dataset("h9", map[int64]string{60: "3"}))
	f.engine.Step(context.Background())
	stats := f.waitRechecks(t, SourceLogSearch, "t90", RecheckCompared, 1)
	if stats.Series[SourceLogSearch]["t90"]["new"] != 1 || stats.Differences[SourceLogSearch]["t90"][DiffNewSeries] != 1 {
		t.Fatalf("new series %v differences %v", stats.Series[SourceLogSearch]["t90"], stats.Differences[SourceLogSearch]["t90"])
	}
	for class, n := range stats.Judgments[SourceLogSearch]["t90"] {
		if n != 0 {
			t.Fatalf("a series with no admission was judged: %s=%d", class, n)
		}
	}
}

// Every way a window is not observed is named and kept out of the
// denominators: not sampled, an incomplete first read, a first read past
// its bounds, no memory, a lost owner, no permit past the tier's window,
// and a recheck that failed, was partial or ran past its bounds.
func TestEveryUnobservedWindowIsNamedAndNeverCountedAsStable(t *testing.T) {
	f := newFixture(t, 1<<20)
	spec := logSpec("q-names")
	for _, query := range []Query{
		{Spec: spec, Operation: execution.OperationRetry, AttemptNo: 1},
		{Spec: spec, Operation: execution.OperationNormal, AttemptNo: 2},
		{Spec: execution.PhysicalQuerySpec{Digest: "q", PlanFacts: execution.QueryPlanFacts{SourceSemantics: []string{"bk_monitor/time_series"},
			QueryList: []execution.QueryClause{{}}}}, Operation: execution.OperationNormal, AttemptNo: 1},
	} {
		if f.engine.Begin(query) != nil {
			t.Fatalf("sampled %+v", query.Operation)
		}
	}
	query := sampledQuery(t, spec, nil)
	f.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil)
	f.engine.Begin(query).Complete(execution.ProviderCompletion{}, errors.New("query failed"))
	big := f.engine.Begin(query)
	points := map[int64]string{}
	for at := int64(60); at <= int64(MaxPointsPerSample+1)*60; at += 60 {
		points[at] = "1"
	}
	big.Series(dataset("h1", points), nil)
	big.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	stats := f.engine.Stats()
	if stats.Samples[SourceLogSearch][OutcomeFirstReadIncomplete] != 2 || stats.Samples[SourceLogSearch][OutcomeUncovered] != 1 || stats.Pending != 0 {
		t.Fatalf("samples %v pending %d", stats.Samples[SourceLogSearch], stats.Pending)
	}

	tiny := newFixture(t, 10)
	tiny.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if tiny.engine.Stats().Samples[SourceLogSearch][OutcomeMemoryFull] != 1 {
		t.Fatal("a sample past the memory share was kept")
	}

	f.set(func() { f.owned = false })
	f.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	f.set(func() { f.owned = true })
	if f.engine.Stats().Samples[SourceLogSearch][OutcomeOwnerLost] != 1 {
		t.Fatal("a read completed after its owner left was kept")
	}

	// No permit through a tier's whole window: yielded, and the next tier.
	f.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	f.set(func() { f.refuse = "waiters" })
	f.clock.advance(90 * time.Second)
	f.engine.Step(context.Background())
	f.clock.advance(TierWindow + time.Second)
	f.engine.Step(context.Background())
	if n := f.engine.Stats().Rechecks[SourceLogSearch]["t90"][RecheckYielded]; n != 1 {
		t.Fatalf("yielded %d, want 1", n)
	}
	f.set(func() { f.refuse = "" })
	// t210 fails, then later samples are partial and truncated.
	f.clock.advance(60 * time.Second)
	f.answers <- func(execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		return execution.ProviderCompletion{}, errors.New("timeout")
	}
	f.engine.Step(context.Background())
	f.waitRechecks(t, SourceLogSearch, "t210", RecheckFailed, 1)
	f.clock.advance(300 * time.Second)
	f.answers <- func(sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		return execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil
	}
	f.engine.Step(context.Background())
	stats = f.waitRechecks(t, SourceLogSearch, "t510", RecheckPartial, 1)
	for _, tier := range TierNames {
		if stats.ComparedBuckets[SourceLogSearch][tier] != 0 || stats.ComparedWindows[SourceLogSearch][tier]["no"] != 0 {
			t.Fatalf("an unobserved window entered the denominators at %s: %v", tier, stats.ComparedWindows[SourceLogSearch])
		}
	}

	// A recheck past the bounds is truncated, not compared.
	g := newFixture(t, 1<<20)
	g.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	g.clock.advance(90 * time.Second)
	g.answers <- full(dataset("h1", points))
	g.engine.Step(context.Background())
	g.waitRechecks(t, SourceLogSearch, "t90", RecheckTruncated, 1)

	// An owner lost while waiting drops the sample.
	h := newFixture(t, 1<<20)
	h.engine.Begin(query).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	h.engine.Forget("qg")
	if stats := h.engine.Stats(); stats.Pending != 0 || stats.PendingBytes != 0 || stats.Rechecks[SourceLogSearch]["t90"][RecheckOwnerLost] != 1 {
		t.Fatalf("after Forget pending %d bytes %d", stats.Pending, stats.PendingBytes)
	}
}

// Every counter cell exists before anything happened: a zero is a count.
func TestEveryCounterCellExistsFromTheStart(t *testing.T) {
	stats := newFixture(t, 1).engine.Stats()
	for _, source := range Sources {
		if len(stats.Samples[source]) != len(SampleOutcomes) || len(stats.ByAge[source]) != len(AgeBuckets) {
			t.Fatalf("%s samples %v ages %v", source, stats.Samples[source], stats.ByAge[source])
		}
		for _, tier := range TierNames {
			if len(stats.Rechecks[source][tier]) != len(RecheckOutcomes) || len(stats.Differences[source][tier]) != len(Differences) ||
				len(stats.Judgments[source][tier]) != len(Judgments) {
				t.Fatalf("%s/%s cells missing", source, tier)
			}
		}
	}
	if _, measured := SourceOf(execution.QueryPlanFacts{SourceSemantics: []string{SourceCollectorLog},
		QueryList: []execution.QueryClause{{FieldName: "event.count", TimeAggregation: execution.QueryFunction{Method: "sum_over_time"}}}}); !measured {
		t.Fatal("the collector's log event count is not measured")
	}
}

// Not sampled costs nothing; sampled at the bounds is timed.
func BenchmarkFirstReadCapture(b *testing.B) {
	engine, _ := New(Options{Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
		return execution.ProviderCompletion{}, nil
	}, Permit: func() (func(), string) { return nil, "x" }, Owns: func(execution.QueryGroupIdentity) bool { return true }, MemoryBytes: 1 << 30})
	spec := logSpec("q-bench")
	b.Run("not_sampled", func(b *testing.B) {
		query := Query{Spec: spec, Operation: execution.OperationRetry, AttemptNo: 1}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if engine.Begin(query) != nil {
				b.Fatal("sampled")
			}
		}
	})
	b.Run("sampled_at_bounds", func(b *testing.B) {
		query := sampledQuery(b, spec, nil)
		sets := make([]*execution.Dataset, MaxSeriesPerSample)
		for i := range sets {
			points := map[int64]string{}
			for p := 0; p < MaxPointsPerSample/MaxSeriesPerSample; p++ {
				points[int64(60*(p+1))] = "12"
			}
			sets[i] = dataset(fmt.Sprintf("h%d", i), points)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			read := engine.Begin(query)
			for _, one := range sets {
				read.Series(one, nil)
			}
		}
	})
}

// A series delivered out of order, or with a bucket twice, is kept in
// bucket order with the later record of a bucket, as a map would have.
func TestAKeptSeriesIsInBucketOrderWithTheLaterRecordOfABucket(t *testing.T) {
	kept := &seriesRead{}
	kept.keep(dataset("h1", map[int64]string{120: "2", 180: "3"}), "value")
	kept.keep(dataset("h1", map[int64]string{60: "1", 120: "7"}), "value")
	kept.seal()
	var got []string
	for index, bucket := range kept.buckets {
		got = append(got, fmt.Sprintf("%d=%s", bucket, kept.value(index)))
	}
	if strings.Join(got, ",") != "60=1,120=7,180=3" {
		t.Fatalf("kept %v", got)
	}
}
