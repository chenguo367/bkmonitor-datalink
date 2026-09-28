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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The first version's constants. They are the design's, not an operator's:
// only whether the lookback runs is configured.
const (
	// SampleOneIn: one physical query in this many is sampled, by a digest of
	// the query and the Slot, so every replica samples the same ones.
	SampleOneIn = 100
	// MaxSeriesPerSample and MaxPointsPerSample bound one kept first read;
	// past either the sample is uncovered rather than cut and compared.
	MaxSeriesPerSample = 2000
	MaxPointsPerSample = 20000
	// RecheckTimeout bounds one recheck, below any formal query's budget, so
	// a formal query that a lookback permit holds up waits a bounded time.
	RecheckTimeout = 5 * time.Second
	// TierWindow is how long past its moment a tier may still find a permit
	// before it counts as yielded.
	TierWindow = time.Minute
	// maxRecent bounds the differing rechecks kept whole for a reader.
	maxRecent = 32
	// maxPendingSamples bounds the samples waiting, whatever their size.
	maxPendingSamples = 1024
)

// Tiers are the recheck moments after the first read. Offset by half a
// period from the first reads of the next Slots, which for Plans of one
// period all fall on one phase: a recheck at a whole minute after a first
// read lands on the next Slot's first read and competes with it.
var Tiers = []time.Duration{90 * time.Second, 210 * time.Second, 510 * time.Second}

// TierNames label the tiers in counters.
var TierNames = []string{"t90", "t210", "t510"}

// Sample outcomes, closed: what became of a sampled first read.
const (
	OutcomeCaptured            = "captured"
	OutcomeUncovered           = "uncovered"
	OutcomeFirstReadIncomplete = "first_read_incomplete"
	OutcomeMemoryFull          = "memory_full"
	OutcomeOwnerLost           = "owner_lost"
	OutcomeCompleted           = "completed"
)

// SampleOutcomes is every sample outcome.
var SampleOutcomes = []string{OutcomeCaptured, OutcomeUncovered, OutcomeFirstReadIncomplete, OutcomeMemoryFull, OutcomeOwnerLost, OutcomeCompleted}

// Recheck outcomes, closed: what one tier of one sample came to. Only
// "compared" enters the comparison's denominators; every other outcome is a
// window that was not observed, never a window that did not change.
const (
	RecheckCompared  = "compared"
	RecheckYielded   = "yielded"
	RecheckFailed    = "recheck_failed"
	RecheckTruncated = "truncated"
	RecheckPartial   = "partial"
	RecheckOwnerLost = "owner_lost"
)

// RecheckOutcomes is every recheck outcome.
var RecheckOutcomes = []string{RecheckCompared, RecheckYielded, RecheckFailed, RecheckTruncated, RecheckPartial, RecheckOwnerLost}

// AgeBuckets label how long after the window's end a recheck actually read,
// the delay profile's axis: a yielded or late recheck reads later than its
// tier, and the profile is by the age read, not by the tier planned.
var AgeBuckets = []string{"le_120s", "le_240s", "le_360s", "le_600s", "gt_600s"}

func ageBucket(age time.Duration) string {
	switch {
	case age <= 120*time.Second:
		return AgeBuckets[0]
	case age <= 240*time.Second:
		return AgeBuckets[1]
	case age <= 360*time.Second:
		return AgeBuckets[2]
	case age <= 600*time.Second:
		return AgeBuckets[3]
	default:
		return AgeBuckets[4]
	}
}

// Recheck reads a frozen physical query again; see uq.Client.Recheck.
type Recheck func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error)

// Permit grants a lookback query permit now, or refuses with the reason.
type Permit func() (release func(), refused string)

// Options wire an Engine. MemoryBytes is the lookback's explicit share of
// the diagnostics memory; zero refuses every sample, as memory_full.
// SampleOneIn is SampleOneIn when zero; only a test sets it.
type Options struct {
	Now         func() time.Time
	Recheck     Recheck
	Permit      Permit
	Owns        func(execution.QueryGroupIdentity) bool
	MemoryBytes int
	SampleOneIn uint64
}

// Query is what the access layer knows about a physical query before it is
// sent: enough to decide whether it is sampled. Plans is every due Plan of
// the Slot and Requirements the query's own; the Plans the query feeds are
// picked out only once it is sampled, so a query that is not costs nothing.
type Query struct {
	Contract     execution.FrozenExecutionContractRef
	Spec         execution.PhysicalQuerySpec
	Operation    execution.Operation
	AttemptNo    uint32
	Plans        []execution.DuePlan
	Requirements []execution.DataRequirement
}

// Engine is the lookback of one process.
type Engine struct {
	options Options

	mu      sync.Mutex
	pending []*sample
	bytes   int
	counts  counters
	recent  []Recent
	nextID  uint64
}

type sample struct {
	id         uint64
	source     string
	queryGroup execution.QueryGroupIdentity
	evaluation execution.EvaluationTime
	spec       execution.PhysicalQuerySpec
	plans      []planCheck
	windowEnd  time.Time
	readAt     time.Time
	first      readSet
	bytes      int
	tier       int
	running    bool
	// dropped is set when the sample leaves pending, so a Step that took it
	// as due before a Forget removed it does not start its recheck, and no
	// sample is settled twice.
	dropped bool
}

// Recent is one recheck that found a difference, kept whole for a reader.
type Recent struct {
	Source         string                       `json:"source"`
	QueryGroup     execution.QueryGroupIdentity `json:"query_group"`
	EvaluationTime execution.EvaluationTime     `json:"evaluation_time"`
	Tier           string                       `json:"tier"`
	ReadAgeSeconds int64                        `json:"read_age_seconds"`
	Buckets        int                          `json:"buckets"`
	Differences    map[string]int               `json:"differences"`
	Judgments      map[string]int               `json:"judgments,omitempty"`
	Examples       []DiffExample                `json:"examples"`
	At             time.Time                    `json:"at"`
}

// New builds an Engine; nil when it cannot run.
func New(options Options) (*Engine, error) {
	if options.Recheck == nil || options.Permit == nil || options.Owns == nil {
		return nil, errors.New("alarmd lookback: recheck, permit and ownership are required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.SampleOneIn == 0 {
		options.SampleOneIn = SampleOneIn
	}
	return &Engine{options: options, counts: newCounters()}, nil
}

// Begin decides whether a physical query is sampled, before it is sent. A
// query not sampled costs nothing more; a sampled one returns the Read its
// series and completion are handed to.
func (engine *Engine) Begin(query Query) *Read {
	if engine == nil || query.Operation != execution.OperationNormal || query.AttemptNo != 1 {
		return nil
	}
	source, measured := SourceOf(query.Spec.PlanFacts)
	if !measured || !sampled(query.Spec.Digest, query.Contract.Slot.EvaluationTime, engine.options.SampleOneIn) {
		return nil
	}
	plans := make([]planCheck, 0, len(query.Plans))
	for _, due := range query.Plans {
		if feeds(query.Requirements, due.Identity) {
			plans = append(plans, planCheckOf(due.Identity, due.CompiledPlan, query.Spec.PlanFacts.Normalization.CanonicalValueField))
		}
	}
	return &Read{engine: engine, source: source, query: query, plans: plans, first: readSet{}, readAt: engine.options.Now()}
}

func sampled(digest execution.PhysicalQueryDigest, at execution.EvaluationTime, oneIn uint64) bool {
	sum := sha256.New()
	sum.Write([]byte(digest))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(at))
	sum.Write(buf[:])
	return binary.BigEndian.Uint64(sum.Sum(nil)[:8])%oneIn == 0
}

// feeds says whether a Plan consumes one of the query's requirements. With
// no requirements given every Plan does.
func feeds(requirements []execution.DataRequirement, plan execution.PlanIdentity) bool {
	if len(requirements) == 0 {
		return true
	}
	for _, requirement := range requirements {
		for _, consumer := range requirement.Consumers {
			if consumer.Consumer.Plan == plan {
				return true
			}
		}
	}
	return false
}

// Read is one sampled first read being kept. The access layer calls Series
// for every series the provider delivered, before any target filtering --
// the layer a recheck reads at -- and Complete once.
type Read struct {
	engine    *Engine
	source    string
	query     Query
	plans     []planCheck
	first     readSet
	points    int
	uncovered bool
	readAt    time.Time
}

// Series keeps one delivered series: every record's bucket and value, and
// which of the sample's Plans admitted it. admitted nil means no target
// filter ran and every Plan admitted it.
func (read *Read) Series(dataset *execution.Dataset, admitted map[execution.PlanIdentity]bool) {
	if read == nil || read.uncovered || dataset == nil || dataset.Len() == 0 {
		return
	}
	if len(read.first) >= MaxSeriesPerSample || read.points+dataset.Len() > MaxPointsPerSample {
		read.uncovered = true
		read.first = nil
		return
	}
	mask := uint64(0)
	for index, plan := range read.plans {
		if index >= 64 {
			break
		}
		if admitted == nil || admitted[plan.identity] {
			mask |= 1 << uint(index)
		}
	}
	first, _ := dataset.Record(0)
	digest := first.DimensionIdentityDigest()
	series := read.first[digest]
	if series == nil {
		series = &seriesRead{admitted: mask, buckets: make([]int64, 0, dataset.Len()), ends: make([]uint32, 0, dataset.Len())}
		read.first[digest] = series
	}
	series.keep(dataset, read.query.Spec.PlanFacts.Normalization.CanonicalValueField)
	read.points += dataset.Len()
}

// Complete hands the kept read to the engine, never waiting: a read the
// engine has no room for is counted and dropped. A first read that is not
// complete is dropped here, before any recheck -- there is nothing a later
// read could be compared against.
func (read *Read) Complete(completion execution.ProviderCompletion, err error) {
	if read == nil {
		return
	}
	engine := read.engine
	switch {
	case err != nil || completion.Completeness != execution.CompletenessFull:
		engine.count(read.source, OutcomeFirstReadIncomplete)
		return
	case read.uncovered:
		engine.count(read.source, OutcomeUncovered)
		return
	}
	if !engine.options.Owns(read.query.Contract.Slot.QueryGroup) {
		engine.count(read.source, OutcomeOwnerLost)
		return
	}
	size := sampleBytes(read.first)
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.bytes+size > engine.options.MemoryBytes || len(engine.pending) >= maxPendingSamples {
		engine.counts.samples[key2(read.source, OutcomeMemoryFull)]++
		return
	}
	engine.nextID++
	window := read.query.Spec.LogicalWindow
	engine.pending = append(engine.pending, &sample{id: engine.nextID, source: read.source,
		queryGroup: read.query.Contract.Slot.QueryGroup, evaluation: read.query.Contract.Slot.EvaluationTime,
		spec: read.query.Spec, plans: read.plans, windowEnd: time.Unix(window.End, 0), readAt: read.readAt,
		first: read.first, bytes: size})
	engine.bytes += size
	engine.counts.samples[key2(read.source, OutcomeCaptured)]++
}

// sampleBytes is what a kept first read is charged: a fixed part, and per
// series its entry and what its slices hold. Sealing orders each series, so
// a later comparison can walk the two reads side by side.
func sampleBytes(first readSet) int {
	size := 1024
	for digest, series := range first {
		series.seal()
		size += 128 + len(digest) + series.bytes()
	}
	return size
}

// Forget drops every sample of a Query Group this process no longer owns.
func (engine *Engine) Forget(queryGroup execution.QueryGroupIdentity) {
	if engine == nil {
		return
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	kept := engine.pending[:0]
	for _, candidate := range engine.pending {
		if candidate.queryGroup != queryGroup || candidate.running {
			kept = append(kept, candidate)
			continue
		}
		engine.dropLocked(candidate, OutcomeOwnerLost)
		engine.counts.rechecks[key3(candidate.source, TierNames[candidate.tier], RecheckOwnerLost)]++
	}
	engine.pending = kept
}

func (engine *Engine) dropLocked(candidate *sample, outcome string) {
	if candidate.dropped {
		return
	}
	candidate.dropped = true
	engine.bytes -= candidate.bytes
	engine.counts.samples[key2(candidate.source, outcome)]++
}

// Run rechecks due samples until ctx ends. One goroutine: a recheck runs on
// its own goroutine only once a permit is held, so the permits bound them.
func (engine *Engine) Run(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			engine.Step(ctx)
		}
	}
}

// Step is one pass over the due samples.
func (engine *Engine) Step(ctx context.Context) {
	now := engine.options.Now()
	engine.mu.Lock()
	due := make([]*sample, 0)
	kept := engine.pending[:0]
	for _, candidate := range engine.pending {
		if candidate.running {
			kept = append(kept, candidate)
			continue
		}
		at := candidate.readAt.Add(Tiers[candidate.tier])
		switch {
		case now.Before(at):
			kept = append(kept, candidate)
		case now.After(at.Add(TierWindow)):
			// Past its window without a permit: the tier is not observed.
			engine.counts.rechecks[key3(candidate.source, TierNames[candidate.tier], RecheckYielded)]++
			if engine.advanceLocked(candidate) {
				kept = append(kept, candidate)
			}
		default:
			due = append(due, candidate)
			kept = append(kept, candidate)
		}
	}
	engine.pending = kept
	engine.mu.Unlock()
	sort.Slice(due, func(i, j int) bool { return due[i].id < due[j].id })
	for _, candidate := range due {
		if !engine.options.Owns(candidate.queryGroup) {
			engine.Forget(candidate.queryGroup)
			continue
		}
		release, refused := engine.options.Permit()
		if refused != "" {
			// No room now; the tier keeps its window and is tried again.
			break
		}
		engine.mu.Lock()
		if candidate.dropped {
			// Forgotten between the ownership check and here: the Query
			// Group left, and its sample was settled as owner_lost.
			engine.mu.Unlock()
			release()
			continue
		}
		candidate.running = true
		engine.mu.Unlock()
		go engine.recheck(ctx, candidate, release)
	}
}

// advanceLocked moves a sample to its next tier, and false when it has none:
// the sample is done and its memory returned.
func (engine *Engine) advanceLocked(candidate *sample) bool {
	candidate.tier++
	if candidate.tier < len(Tiers) {
		return true
	}
	engine.dropLocked(candidate, OutcomeCompleted)
	return false
}

func (engine *Engine) recheck(ctx context.Context, candidate *sample, release func()) {
	readCtx, cancel := context.WithTimeout(ctx, RecheckTimeout)
	sink := &recheckSink{series: readSet{}, valueField: candidate.spec.PlanFacts.Normalization.CanonicalValueField}
	started := engine.options.Now()
	completion, err := engine.options.Recheck(readCtx, candidate.spec, sink)
	cancel()
	release()
	tier := TierNames[candidate.tier]
	outcome := RecheckCompared
	switch {
	case err != nil || completion.Completeness == execution.CompletenessUnavailable:
		outcome = RecheckFailed
	case sink.truncated:
		outcome = RecheckTruncated
	case completion.Completeness != execution.CompletenessFull:
		outcome = RecheckPartial
	}
	var result comparison
	if outcome == RecheckCompared {
		for _, series := range sink.series {
			series.seal()
		}
		result = compare(candidate.first, sink.series, candidate.plans)
	}
	age := started.Sub(candidate.windowEnd)
	engine.mu.Lock()
	defer engine.mu.Unlock()
	candidate.running = false
	if candidate.dropped {
		return
	}
	engine.counts.rechecks[key3(candidate.source, tier, outcome)]++
	if outcome == RecheckCompared {
		engine.counts.record(candidate.source, tier, ageBucket(age), result)
		if result.differsInWindow {
			engine.remember(Recent{Source: candidate.source, QueryGroup: candidate.queryGroup, EvaluationTime: candidate.evaluation,
				Tier: tier, ReadAgeSeconds: int64(age / time.Second), Buckets: result.buckets, Differences: result.differences,
				Judgments: result.judgments, Examples: result.examples, At: started})
		}
	}
	if !engine.advanceLocked(candidate) {
		kept := engine.pending[:0]
		for _, other := range engine.pending {
			if other != candidate {
				kept = append(kept, other)
			}
		}
		engine.pending = kept
	}
}

func (engine *Engine) remember(recent Recent) {
	engine.recent = append(engine.recent, recent)
	if len(engine.recent) > maxRecent {
		engine.recent = append([]Recent(nil), engine.recent[len(engine.recent)-maxRecent:]...)
	}
}

func (engine *Engine) count(source, outcome string) {
	engine.mu.Lock()
	engine.counts.samples[key2(source, outcome)]++
	engine.mu.Unlock()
}

// recheckSink keeps a recheck's series under the same bounds as a first
// read, with each series' dimensions rendered for examples.
type recheckSink struct {
	series     readSet
	points     int
	truncated  bool
	valueField string
}

func (sink *recheckSink) ConsumeProviderSeries(_ context.Context, batch execution.ProviderSeriesBatch) error {
	if sink.truncated || batch.Dataset == nil || batch.Dataset.Len() == 0 {
		return nil
	}
	if len(sink.series) >= MaxSeriesPerSample || sink.points+batch.Dataset.Len() > MaxPointsPerSample {
		sink.truncated = true
		return nil
	}
	first, _ := batch.Dataset.Record(0)
	digest := first.DimensionIdentityDigest()
	series := sink.series[digest]
	if series == nil {
		series = &seriesRead{dims: renderDimensions(first.Dimensions())}
		sink.series[digest] = series
	}
	series.keep(batch.Dataset, sink.valueField)
	sink.points += batch.Dataset.Len()
	return nil
}

// renderDimensions is a bounded, ordered rendering of a series' dimensions.
func renderDimensions(dimensions map[string]json.RawMessage) string {
	names := make([]string, 0, len(dimensions))
	for name := range dimensions {
		names = append(names, name)
	}
	sort.Strings(names)
	var out strings.Builder
	for _, name := range names {
		if out.Len() > 0 {
			out.WriteString(",")
		}
		out.WriteString(name + "=" + string(dimensions[name]))
		if out.Len() > 256 {
			return out.String()[:256] + "..."
		}
	}
	return out.String()
}
