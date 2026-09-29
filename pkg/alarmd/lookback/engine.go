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
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// RungSteps are the recheck moments after a first read, in the Query Group's
// data steps: rung k at (2^(k+1) - 0.5) steps. Lateness is the data path's
// delay, so the unit is the step the data is bucketed at, not how often the
// strategy is evaluated: a daily strategy over minute data is rechecked
// minutes after its read, not days. Half a step off the whole steps keeps a
// recheck off the next Slot's first read when a Slot runs every step, and
// the doubling reaches an hour of lateness at a minute step in six reads.
// Past 64 steps nothing is rechecked.
var RungSteps = []float64{1.5, 3.5, 7.5, 15.5, 31.5, 63.5}

// RungNames label the rungs by their moment in steps.
var RungNames = []string{"x1.5", "x3.5", "x7.5", "x15.5", "x31.5", "x63.5"}

func rungDelay(rung int, step time.Duration) time.Duration {
	return time.Duration(RungSteps[rung] * float64(step))
}

// rungWindow is how long past its moment a rung may still find a permit:
// half the gap to the next rung, 2^rung steps, so a rung that waits never
// runs into the next one.
func rungWindow(rung int, step time.Duration) time.Duration {
	return time.Duration(1<<rung) * step
}

const (
	// RecheckTimeout bounds one recheck, below any formal query's budget.
	RecheckTimeout = 5 * time.Second
	// maxRestSteps is the longest a source whose data is always complete at
	// the first read rests between two samples of one Query Group: 64
	// steps, where the rungs also stop - about one recheck an hour per
	// Query Group at a minute step.
	maxRestSteps = 64
	// cleanSamplesToShallow is how many samples of a source in a row must
	// show nothing changing at its two deepest rungs before it rechecks one
	// rung less. At a true late rate of one sample in four there, eight
	// clean ones in a row happen one time in ten; a single late sample
	// restores the rung at once.
	cleanSamplesToShallow = 8
	// maxRecent bounds the changed rechecks kept whole, maxLatest the Query
	// Groups listed by their latest completion.
	maxRecent = 32
	maxLatest = 32
)

// Sample outcomes, closed: what became of a first read taken as a sample.
const (
	OutcomeCaptured            = "captured"
	OutcomeFirstReadIncomplete = "first_read_incomplete"
	OutcomeOwnerLost           = "owner_lost"
	OutcomeCompleted           = "completed"
	OutcomeFault               = "fault"
)

// SampleOutcomes is every sample outcome.
var SampleOutcomes = []string{OutcomeCaptured, OutcomeFirstReadIncomplete, OutcomeOwnerLost, OutcomeCompleted, OutcomeFault}

// Recheck outcomes, closed: what one rung of one sample came to. Only
// "compared" is a window observed; every other outcome is a window not
// observed, never a window that did not change.
const (
	RecheckCompared  = "compared"
	RecheckYielded   = "yielded"
	RecheckFailed    = "recheck_failed"
	RecheckPartial   = "partial"
	RecheckOwnerLost = "owner_lost"
)

// RecheckOutcomes is every recheck outcome.
var RecheckOutcomes = []string{RecheckCompared, RecheckYielded, RecheckFailed, RecheckPartial, RecheckOwnerLost}

// Faults, closed. Normal running never meets one: each is a defect to fix,
// logged through Options.OnFault as well as counted.
const (
	// FaultBucketsExceeded is a read with more buckets than any window has.
	FaultBucketsExceeded = "buckets_exceeded"
)

// Faults is every fault.
var Faults = []string{FaultBucketsExceeded}

// RefusedOther counts a permit refusal whose reason Options.Refusals does
// not name.
const RefusedOther = "other"

// AgeBuckets label when a window's data was complete, as its age past the
// window's end: the delay profile.
var AgeBuckets = []string{"le_60s", "le_120s", "le_300s", "le_600s", "le_1800s", "le_3600s", "gt_3600s"}

func ageBucket(age time.Duration) string {
	for index, bound := range []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour} {
		if age <= bound {
			return AgeBuckets[index]
		}
	}
	return AgeBuckets[len(AgeBuckets)-1]
}

// Recheck reads a frozen physical query again; see uq.Client.Recheck.
type Recheck func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error)

// Permit grants a lookback query permit now, or refuses with the reason. A
// granted permit's yield is closed when a formal query has to wait for one:
// the read holding it stops and releases it.
type Permit func() (release func(), yield <-chan struct{}, refused string)

// Options wire an Engine. Owned is how many Query Groups this process owns,
// the denominator of the coverage. Sources are the data sources counted by
// name; any other is counted as SourceOther. Refusals is every reason Permit
// refuses with, counted from the start; any other as RefusedOther. OnFault,
// when set, is told of every fault.
type Options struct {
	Now      func() time.Time
	Recheck  Recheck
	Permit   Permit
	Refusals []string
	Owns     func(execution.QueryGroupIdentity) bool
	Owned    func() int
	Sources  []string
	OnFault  func(reason string, queryGroup execution.QueryGroupIdentity)
}

// Query is what the access layer knows about a physical query before it is
// sent: enough to decide whether it is taken as its Query Group's sample.
type Query struct {
	Contract  execution.FrozenExecutionContractRef
	Spec      execution.PhysicalQuerySpec
	Operation execution.Operation
	AttemptNo uint32
}

// Engine is the lookback of one process.
type Engine struct {
	options Options
	named   map[string]bool
	labels  []string

	mu      sync.Mutex
	groups  map[execution.QueryGroupIdentity]*group
	sources map[string]*sourceState
	counts  counters
	recent  []Recent
	nextID  uint64
}

// group is one Query Group: its sample in flight, if any, when the next may
// start, and its last measurement.
type group struct {
	source    string
	step      time.Duration
	capturing bool
	sample    *sample
	nextAt    time.Time
	// period is the observed time between two of its Slots, lastSlot the
	// latest one seen; a Query Group is sampled at its first reads, so how
	// fresh its measurement can be depends on how often it reads.
	period     time.Duration
	lastSlot   execution.EvaluationTime
	measured   bool
	measuredAt time.Time
	completion time.Duration
}

// sourceState is how one source is rechecked: how many rungs, how long a
// Query Group rests between samples, in steps, and the clean samples in a
// row counted towards one rung less.
type sourceState struct {
	depth         int
	rest          float64
	clean         int
	maxCompletion time.Duration
}

type sample struct {
	id         uint64
	source     string
	queryGroup execution.QueryGroupIdentity
	evaluation execution.EvaluationTime
	spec       execution.PhysicalQuerySpec
	step       time.Duration
	windowEnd  time.Time
	readAt     time.Time
	// last is the latest read: each rung is compared with the read before it.
	last readSummary
	// rung is the next rung, planned how many are read: the source's depth at
	// capture, one more each time the last planned rung still changed.
	rung    int
	planned int
	// lastChange is the last rung that changed, -1 for none, and
	// lastChangeAge how long after the window's end it read.
	lastChange    int
	lastChangeAge time.Duration
	running       bool
	dropped       bool
}

// Recent is one recheck that found a change, kept whole for a reader.
type Recent struct {
	Source         string                       `json:"source"`
	QueryGroup     execution.QueryGroupIdentity `json:"query_group"`
	EvaluationTime execution.EvaluationTime     `json:"evaluation_time"`
	Rung           string                       `json:"rung"`
	ReadAgeSeconds int64                        `json:"read_age_seconds"`
	Changes        map[string]int               `json:"changes"`
	At             time.Time                    `json:"at"`
}

// New builds an Engine.
func New(options Options) (*Engine, error) {
	if options.Recheck == nil || options.Permit == nil || options.Owns == nil || options.Owned == nil {
		return nil, errors.New("alarmd lookback: recheck, permit, ownership and the owned count are required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	engine := &Engine{options: options, named: map[string]bool{}, groups: map[execution.QueryGroupIdentity]*group{},
		sources: map[string]*sourceState{}}
	for _, source := range options.Sources {
		if !engine.named[source] {
			engine.named[source] = true
			engine.labels = append(engine.labels, source)
		}
	}
	for _, source := range []string{SourceMixed, SourcePromQL, SourceOther} {
		if !engine.named[source] {
			engine.labels = append(engine.labels, source)
		}
	}
	for _, source := range engine.labels {
		engine.sources[source] = &sourceState{depth: 1, rest: RungSteps[0]}
	}
	engine.counts = newCounters(engine.labels, options.Refusals)
	return engine, nil
}

// Begin decides whether a physical query is taken as its Query Group's
// sample, before it is sent: the group's first formal read after its last
// sample finished and rested. Any other query costs a counter.
func (engine *Engine) Begin(query Query) *Read {
	if engine == nil || query.Operation != execution.OperationNormal || query.AttemptNo != 1 {
		return nil
	}
	facts := query.Spec.PlanFacts
	source := sourceOf(facts, engine.named)
	step := time.Duration(facts.StepMillis) * time.Millisecond
	slot := query.Contract.Slot
	now := engine.options.Now()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.counts.firstReads[source]++
	if step <= 0 {
		return nil
	}
	state := engine.groups[slot.QueryGroup]
	if state == nil {
		state = &group{}
		engine.groups[slot.QueryGroup] = state
	}
	if slot.EvaluationTime > state.lastSlot {
		if state.lastSlot > 0 {
			state.period = time.Duration(slot.EvaluationTime-state.lastSlot) * time.Second
		}
		state.lastSlot = slot.EvaluationTime
	}
	state.source, state.step = source, step
	if state.capturing || state.sample != nil || now.Before(state.nextAt) {
		return nil
	}
	state.capturing = true
	return &Read{engine: engine, source: source, query: query, step: step, readAt: now,
		summary: newSummarizer(facts.Normalization.CanonicalValueField)}
}

// Read is one first read being taken as a sample. The access layer calls
// Series for every series the provider delivered, before any target
// filtering - the layer a recheck reads at - and Complete once.
type Read struct {
	engine  *Engine
	source  string
	query   Query
	step    time.Duration
	readAt  time.Time
	summary *summarizer
}

// Series adds one delivered series to the summary.
func (read *Read) Series(dataset *execution.Dataset) {
	if read == nil {
		return
	}
	read.summary.add(dataset)
}

// Complete hands the summary to the engine, never waiting. A first read
// that is not complete is dropped: there is nothing a later read could be
// compared against.
func (read *Read) Complete(completion execution.ProviderCompletion, err error) {
	if read == nil {
		return
	}
	engine := read.engine
	queryGroup := read.query.Contract.Slot.QueryGroup
	owned := engine.options.Owns(queryGroup)
	engine.mu.Lock()
	defer engine.mu.Unlock()
	state := engine.groups[queryGroup]
	if state == nil || !state.capturing {
		engine.counts.samples[key2(read.source, OutcomeOwnerLost)]++
		return
	}
	state.capturing = false
	switch {
	case err != nil || completion.Completeness != execution.CompletenessFull:
		engine.counts.samples[key2(read.source, OutcomeFirstReadIncomplete)]++
		return
	case read.summary.faulted:
		engine.faultLocked(FaultBucketsExceeded, read.source, queryGroup)
		return
	case !owned:
		engine.counts.samples[key2(read.source, OutcomeOwnerLost)]++
		return
	}
	engine.nextID++
	state.sample = &sample{id: engine.nextID, source: read.source, queryGroup: queryGroup,
		evaluation: read.query.Contract.Slot.EvaluationTime, spec: read.query.Spec, step: read.step,
		windowEnd: time.Unix(read.query.Spec.LogicalWindow.End, 0), readAt: read.readAt,
		last: read.summary.buckets, planned: engine.sources[read.source].depth, lastChange: -1}
	engine.counts.samples[key2(read.source, OutcomeCaptured)]++
}

func (engine *Engine) faultLocked(reason, source string, queryGroup execution.QueryGroupIdentity) {
	engine.counts.samples[key2(source, OutcomeFault)]++
	engine.counts.faults[reason]++
	if engine.options.OnFault != nil {
		engine.options.OnFault(reason, queryGroup)
	}
}

// Forget drops a Query Group this process no longer owns, with its sample.
func (engine *Engine) Forget(queryGroup execution.QueryGroupIdentity) {
	if engine == nil {
		return
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	state := engine.groups[queryGroup]
	if state == nil {
		return
	}
	if candidate := state.sample; candidate != nil && !candidate.dropped {
		candidate.dropped = true
		engine.counts.samples[key2(candidate.source, OutcomeOwnerLost)]++
		engine.counts.rechecks[key3(candidate.source, RungNames[candidate.rung], RecheckOwnerLost)]++
	}
	delete(engine.groups, queryGroup)
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

// Step is one pass over the samples: a rung whose moment has come is read
// again if a permit is free, and one past its window without a permit is
// counted as yielded and the next rung planned.
func (engine *Engine) Step(ctx context.Context) {
	now := engine.options.Now()
	engine.mu.Lock()
	due := make([]*sample, 0)
	for _, state := range engine.groups {
		candidate := state.sample
		if candidate == nil || candidate.running {
			continue
		}
		at := candidate.readAt.Add(rungDelay(candidate.rung, candidate.step))
		switch {
		case now.Before(at):
		case now.After(at.Add(rungWindow(candidate.rung, candidate.step))):
			engine.counts.rechecks[key3(candidate.source, RungNames[candidate.rung], RecheckYielded)]++
			engine.advanceLocked(state, candidate, now)
		default:
			due = append(due, candidate)
		}
	}
	engine.mu.Unlock()
	sort.Slice(due, func(i, j int) bool { return due[i].id < due[j].id })
	for _, candidate := range due {
		if !engine.options.Owns(candidate.queryGroup) {
			engine.Forget(candidate.queryGroup)
			continue
		}
		release, yield, refused := engine.options.Permit()
		if refused != "" {
			// No room now; the rung keeps its window and is tried again.
			engine.mu.Lock()
			if _, named := engine.counts.refusals[refused]; !named {
				refused = RefusedOther
			}
			engine.counts.refusals[refused]++
			engine.mu.Unlock()
			break
		}
		engine.mu.Lock()
		if candidate.dropped {
			engine.mu.Unlock()
			release()
			continue
		}
		candidate.running = true
		engine.mu.Unlock()
		go engine.recheck(ctx, candidate, release, yield)
	}
}

// advanceLocked moves a sample to its next rung, and finishes it after its
// last planned one.
func (engine *Engine) advanceLocked(state *group, candidate *sample, now time.Time) {
	candidate.rung++
	if candidate.rung < candidate.planned {
		return
	}
	engine.finishLocked(state, candidate, now)
}

// finishLocked records a finished sample and what it teaches its source.
//
// The window was complete at the last rung that changed, or at the first
// read if none did. The source then needs one rung past its last change, a
// guard showing nothing more arrived: more at once when a sample needed
// more, one fewer only after cleanSamplesToShallow samples in a row needed
// fewer. A Query Group rests before its next sample: only the new deepest
// rung's length when its source just deepened - lateness it had not shown,
// to be learned quickly - and otherwise twice its last rest, up to
// maxRestSteps, however late the data is, as long as it is late as before.
// So a source is rechecked as deep as its lateness goes and, once that
// holds, about as rarely as a punctual one. Each Query Group's rest is
// spread around its source's (restSpread).
func (engine *Engine) finishLocked(state *group, candidate *sample, now time.Time) {
	state.sample = nil
	source := engine.sources[candidate.source]
	completion := candidate.readAt.Sub(candidate.windowEnd)
	if candidate.lastChange >= 0 {
		completion = candidate.lastChangeAge
	}
	completion = max(completion, 0)
	state.measured, state.measuredAt, state.completion = true, now, completion
	engine.counts.samples[key2(candidate.source, OutcomeCompleted)]++
	engine.counts.completion[key2(candidate.source, ageBucket(completion))]++
	source.maxCompletion = max(source.maxCompletion, completion)

	need := min(max(candidate.lastChange+2, 1), len(RungSteps))
	switch {
	case need > source.depth:
		source.depth, source.clean = need, 0
		source.rest = RungSteps[source.depth-1]
	case need < source.depth:
		source.clean++
		if source.clean >= cleanSamplesToShallow {
			source.depth, source.clean = source.depth-1, 0
		}
		source.rest = min(source.rest*2, maxRestSteps)
	default:
		source.clean = 0
		source.rest = min(source.rest*2, maxRestSteps)
	}
	state.nextAt = now.Add(time.Duration(source.rest * restSpread(candidate.queryGroup) * float64(state.step)))
}

// restSpread is how much of its source's rest a Query Group rests: from
// three quarters to a quarter past, by a hash of the Query Group. Every
// group of a source rests as long on average, and groups whose Slots read at
// one moment - which all Query Groups of one period do - are not all
// sampled, and so all rechecked, at one moment again.
func restSpread(queryGroup execution.QueryGroupIdentity) float64 {
	return 0.75 + 0.5*float64(mix(hashString(string(queryGroup)))%1024)/1024
}

func (engine *Engine) recheck(ctx context.Context, candidate *sample, release func(), yield <-chan struct{}) {
	readCtx, cancel := context.WithTimeout(ctx, RecheckTimeout)
	if yield != nil {
		go func() {
			select {
			case <-yield:
				cancel()
			case <-readCtx.Done():
			}
		}()
	}
	sink := &recheckSink{summary: newSummarizer(candidate.spec.PlanFacts.Normalization.CanonicalValueField)}
	started := engine.options.Now()
	completion, err := engine.options.Recheck(readCtx, candidate.spec, sink)
	cancel()
	release()
	rung := RungNames[candidate.rung]
	if (err != nil || completion.Completeness != execution.CompletenessFull) && closed(yield) {
		// Stopped for a formal query, not a read that failed: the rung keeps
		// its window and is tried again, counted once by what it comes to.
		engine.mu.Lock()
		candidate.running = false
		if !candidate.dropped {
			engine.counts.preempted[key2(candidate.source, rung)]++
		}
		engine.mu.Unlock()
		return
	}
	outcome := RecheckCompared
	switch {
	case err != nil || completion.Completeness == execution.CompletenessUnavailable:
		outcome = RecheckFailed
	case completion.Completeness != execution.CompletenessFull:
		outcome = RecheckPartial
	case sink.summary.faulted:
		outcome = RecheckFailed
	}
	var changes map[string]int
	if outcome == RecheckCompared {
		changes = compareSummaries(candidate.last, sink.summary.buckets)
	}
	age := started.Sub(candidate.windowEnd)
	engine.mu.Lock()
	defer engine.mu.Unlock()
	candidate.running = false
	if candidate.dropped {
		return
	}
	if sink.summary.faulted {
		engine.faultLocked(FaultBucketsExceeded, candidate.source, candidate.queryGroup)
	}
	engine.counts.rechecks[key3(candidate.source, rung, outcome)]++
	if len(changes) > 0 {
		engine.counts.changed[key2(candidate.source, rung)]++
		for class, n := range changes {
			engine.counts.changes[key3(candidate.source, rung, class)] += uint64(n)
		}
		candidate.last = sink.summary.buckets
		candidate.lastChange, candidate.lastChangeAge = candidate.rung, age
		if candidate.rung == candidate.planned-1 && candidate.planned < len(RungSteps) {
			// Still arriving at the last planned rung: follow it one further.
			candidate.planned++
		}
		engine.remember(Recent{Source: candidate.source, QueryGroup: candidate.queryGroup, EvaluationTime: candidate.evaluation,
			Rung: rung, ReadAgeSeconds: int64(age / time.Second), Changes: changes, At: started})
	}
	if state := engine.groups[candidate.queryGroup]; state != nil && state.sample == candidate {
		engine.advanceLocked(state, candidate, engine.options.Now())
	}
}

// closed reports whether a yield has been closed; a nil one never is.
func closed(yield <-chan struct{}) bool {
	if yield == nil {
		return false
	}
	select {
	case <-yield:
		return true
	default:
		return false
	}
}

func (engine *Engine) remember(recent Recent) {
	engine.recent = append(engine.recent, recent)
	if len(engine.recent) > maxRecent {
		engine.recent = append([]Recent(nil), engine.recent[len(engine.recent)-maxRecent:]...)
	}
}

// recheckSink sums a recheck the way a first read was summed.
type recheckSink struct {
	summary *summarizer
}

func (sink *recheckSink) ConsumeProviderSeries(_ context.Context, batch execution.ProviderSeriesBatch) error {
	sink.summary.add(batch.Dataset)
	return nil
}
