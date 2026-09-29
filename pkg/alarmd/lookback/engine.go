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
	"sync/atomic"
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
	// restCap is the longest a Query Group rests between two samples,
	// whatever its step: how late a source's data is changes over hours -
	// a pipeline's load, a deployment upstream - so an hour is as rarely as
	// a Query Group whose data is complete at the first read is looked at.
	// It is also the span a process spreads its Query Groups' first samples
	// over. The rungs still reach 63.5 steps.
	restCap = time.Hour
	// cleanSamplesToShallow is how many samples of a Query Group in a row
	// must show nothing changing at its two deepest rungs before it
	// rechecks only as deep as the most any of them needed - at once, so a
	// group whose lateness has passed is back within as many samples,
	// however deep it went. At a true late rate of one sample in four there,
	// eight clean ones in a row happen one time in ten; a single late sample
	// restores the rung at once.
	cleanSamplesToShallow = 8
	// probeEvery: one sample in so many of a Query Group, and its second,
	// is read once more at the deepest rung after the rungs its group reads
	// - the deep recheck - so data later than those rungs is seen however
	// late it is up to the deepest one, and however short the window. A
	// punctual group rechecks a quarter more for it. Not its first: the
	// first samples of every group come due within the hour after a start,
	// and the second, a rest later, no longer come together.
	probeEvery = 4
	// maxRecent bounds the changed rechecks kept whole, maxLatest the Query
	// Groups listed by their latest completion.
	maxRecent = 32
	maxLatest = 32
)

// Sample outcomes, closed: what became of a first read taken as a sample.
// completed is a window whose completion was observed; unobserved one whose
// last rungs were not read (yielded, failed, partial) with no rung compared
// after them, and probe_changed one whose deep recheck found data arriving
// after the rungs its group read: complete somewhere between its last rung
// and the deepest, not measured. Neither of those two is a window that did
// not change: they enter no completion, and count as no clean sample.
const (
	OutcomeCaptured            = "captured"
	OutcomeFirstReadIncomplete = "first_read_incomplete"
	OutcomeOwnerLost           = "owner_lost"
	OutcomeCompleted           = "completed"
	OutcomeUnobserved          = "unobserved"
	OutcomeProbeChanged        = "probe_changed"
	OutcomeFault               = "fault"
)

// SampleOutcomes is every sample outcome.
var SampleOutcomes = []string{OutcomeCaptured, OutcomeFirstReadIncomplete, OutcomeOwnerLost, OutcomeCompleted,
	OutcomeUnobserved, OutcomeProbeChanged, OutcomeFault}

// Deep recheck outcomes, closed: clean found nothing after the rungs its
// group read, changed found data arriving there, unobserved was not read -
// and is tried again at the next sample.
const (
	ProbeClean      = "clean"
	ProbeChanged    = "changed"
	ProbeUnobserved = "unobserved"
)

// ProbeOutcomes is every deep recheck outcome.
var ProbeOutcomes = []string{ProbeClean, ProbeChanged, ProbeUnobserved}

// Empty first reads, closed: a completed sample whose first read was
// complete and held no point either had its data arrive at a later rung or
// stayed empty.
const (
	EmptyArrived     = "arrived"
	EmptyStayedEmpty = "stayed_empty"
)

// EmptyFirstReadOutcomes is every empty first read outcome.
var EmptyFirstReadOutcomes = []string{EmptyArrived, EmptyStayedEmpty}

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
	// FaultYieldOverdue is a recheck that still held its permit
	// RecheckTimeout after a waiting formal query asked it to yield. The
	// yield cancels the read, and the read's own deadline is RecheckTimeout
	// from its start, so only a read that honours neither holds on that
	// long: a formal query waits for no lookback read longer than one takes
	// to stop, and this counts the reads that did not stop.
	FaultYieldOverdue = "yield_overdue"
)

// Faults is every fault.
var Faults = []string{FaultBucketsExceeded, FaultYieldOverdue}

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
// the denominator of the coverage. Refusals is every reason Permit
// refuses with, counted from the start, any other as RefusedOther. OnFault,
// when set, is told of every fault. UnspreadFirstSamples takes each Query
// Group's first sample at its first read instead of spreading them over
// restCap, and ProbeFirstSamples probes that first sample instead of the
// second; only a test sets either, to pin a mechanism on a group's first
// sample.
//
// Memory is the process's memory line: asked for bytes before a read's
// series table grows, it answers whether they may be taken. A refusal stops
// that read's series sums and nothing else. Nothing is given back: what a
// table took is in the live heap from the next collection on, and a sample
// that ends leaves it there no longer. Nil admits everything.
type Options struct {
	Now                  func() time.Time
	Recheck              Recheck
	Permit               Permit
	Refusals             []string
	Owns                 func(execution.QueryGroupIdentity) bool
	Owned                func() int
	OnFault              func(reason string, queryGroup execution.QueryGroupIdentity)
	Memory               func(bytes uint64) bool
	UnspreadFirstSamples bool
	ProbeFirstSamples    bool
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
	// firstReadBytes is every formal first read's delivered bytes by
	// source, counted on the query's own goroutine: the map is built once
	// and only its counters change.
	firstReadBytes map[string]*atomic.Uint64

	mu     sync.Mutex
	groups map[execution.QueryGroupIdentity]*group
	counts counters
	recent []Recent
	nextID uint64
}

// group is one Query Group: how it is rechecked, which it learns from its
// own samples, its sample in flight, if any, when the next may start, and
// its last measurement. A source's label only sums its groups' readings; a
// late group of a source does not make a punctual one of it read deeper.
type group struct {
	source string
	step   time.Duration
	// depth is how many rungs its sample reads, rest how long it rests
	// between two samples, in its steps, clean its samples in a row that
	// needed less than depth and cleanNeed the most any of them needed.
	// settle is set when a deep recheck found
	// data later than depth: the group reads every rung, and its next
	// completed sample sets depth from where its data stopped, shallower
	// at once if that is shallower.
	depth     int
	rest      float64
	clean     int
	cleanNeed int
	settle    bool
	// sinceProbe is its samples finished since its last deep recheck;
	// probe the sample, if any, whose rungs were read and which waits for
	// its deep recheck. It does not hold the next sample back.
	sinceProbe int
	probe      *sample
	// readEarly is its run of samples whose window was read early, and
	// seriesLate the rung its late series were last seen at, if any.
	readEarly  *readEarlyState
	seriesLate *seriesLateState
	capturing  bool
	sample     *sample
	nextAt     time.Time
	// period is the observed time between two of its Slots, lastSlot the
	// latest one seen; a Query Group is sampled at its first reads, so how
	// fresh its measurement can be depends on how often it reads.
	period     time.Duration
	lastSlot   execution.EvaluationTime
	measured   bool
	measuredAt time.Time
	completion time.Duration
	// incompleteFirstReads is how many of its first reads were not whole,
	// and completeFirstRead whether one ever was: a group whose every first
	// read so far was incomplete has never been measurable, which is its own
	// finding and not a gap in the coverage (Coverage.NeverCompleteFirstRead).
	incompleteFirstReads uint64
	completeFirstRead    bool
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
	// lookback is how far before its first compared bucket a recheck reads,
	// so that bucket is computed from the data the first read had.
	lookback time.Duration
	// last is the latest read of the kept tail, from keptFrom on: each rung
	// is compared with the read before it.
	last     readSummary
	keptFrom int64
	// rung is the next rung, planned how many are read: the group's depth at
	// capture, one more each time the last planned rung still changed.
	rung    int
	planned int
	// lastChange is the last rung that changed, -1 for none, and
	// lastChangeAge how long after the window's end it read. unread is
	// true while the latest rung was not read - no rung compared since; a
	// rung compared is compared with the last read kept, so it covers the
	// ones not read before it.
	lastChange    int
	lastChangeAge time.Duration
	unread        bool
	running       bool
	dropped       bool
	// yieldAt is when a waiting formal query asked the read in flight to
	// yield, zero while none has; overdue is set once the read held its
	// permit past RecheckTimeout after that and was counted as a fault.
	// returned is set the moment the read came back, before it gives its
	// permit back: a yield after that did not stop it.
	yieldAt  time.Time
	overdue  bool
	returned bool
	// probe is set on a sample to be read once more at the deepest rung
	// after its planned ones; probing once its planned rungs are read and
	// its completion taken, and probeChanged when that read changed.
	// settles is set on a sample taken while its group settles.
	probe        bool
	probing      bool
	probeChanged bool
	settles      bool
	completion   time.Duration
	// emptyFirstRead is a first read complete with no point in it.
	emptyFirstRead bool
	// first is the first read's series over the kept tail, which every rung
	// is classified against - nil when the memory line refused it, and once
	// the sample waits for its deep recheck, which classifies nothing;
	// delaySeconds the query's effective time_delay.
	first        map[uint64]seriesSummary
	delaySeconds int64
	// existingChanged is set once a rung read a series of the first read
	// with other points or values, or without it; early is that rung.
	// seriesAdded is set once a rung read a series the first read did not
	// have, at rung seriesAddedRung. seriesUnknown is set once a rung
	// changed and its series could not be compared, one side's table having
	// been refused.
	existingChanged bool
	early           *ReadEarlySample
	seriesAdded     bool
	seriesAddedRung int
	seriesUnknown   bool
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
	engine := &Engine{options: options, groups: map[execution.QueryGroupIdentity]*group{},
		firstReadBytes: map[string]*atomic.Uint64{}}
	for _, source := range Sources {
		engine.firstReadBytes[source] = &atomic.Uint64{}
	}
	engine.counts = newCounters(Sources, options.Refusals)
	return engine, nil
}

// Begin sees every formal first read before it is sent. It counts it, and
// takes it as its Query Group's sample when the group has none in flight
// and has rested since its last; the Read it returns counts the bytes the
// read delivers either way, and keeps a summary only of a sample.
func (engine *Engine) Begin(query Query) *Read {
	if engine == nil || query.Operation != execution.OperationNormal || query.AttemptNo != 1 {
		return nil
	}
	facts := query.Spec.PlanFacts
	source := sourceOf(facts)
	read := &Read{engine: engine, source: source, bytes: engine.firstReadBytes[source]}
	step := time.Duration(facts.StepMillis) * time.Millisecond
	slot := query.Contract.Slot
	now := engine.options.Now()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.counts.firstReads[source]++
	if step <= 0 {
		return read
	}
	state := engine.groups[slot.QueryGroup]
	if state == nil {
		// A group not seen before is probed at its first sample.
		state = &group{depth: 1, rest: RungSteps[0], sinceProbe: probeEvery - 2}
		if engine.options.ProbeFirstSamples {
			state.sinceProbe = probeEvery - 1
		}
		if !engine.options.UnspreadFirstSamples {
			// A process's Query Groups all read for the first time within a
			// period of its start: spread their first samples over an hour,
			// by the same hash as their rests, or their rechecks would all
			// come due at once.
			state.nextAt = now.Add(time.Duration(spreadFraction(slot.QueryGroup) * float64(restCap)))
		}
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
		return read
	}
	state.capturing = true
	read.query, read.step, read.readAt, read.depth = query, step, now, state.depth
	read.keptFrom = tailFrom(query.Spec.LogicalWindow, step, tailSteps)
	read.summary = newSummarizer(facts.Normalization.CanonicalValueField).trackSeries(read.keptFrom, engine.options.Memory)
	return read
}

// Read is one formal first read. The access layer calls Series for every
// series the provider delivered, before any target filtering - the layer a
// recheck reads at - and Complete once.
type Read struct {
	engine *Engine
	source string
	bytes  *atomic.Uint64
	// The rest is set only on a Query Group's sample.
	query    Query
	step     time.Duration
	readAt   time.Time
	depth    int
	keptFrom int64
	summary  *summarizer
}

// Series counts one delivered series' bytes, and on a sample adds it to the
// summary.
func (read *Read) Series(dataset *execution.Dataset, bytes uint64) {
	if read == nil {
		return
	}
	read.bytes.Add(bytes)
	if read.summary != nil {
		read.summary.add(dataset)
	}
}

// Complete hands a sample's summary to the engine, never waiting, kept to
// the window's tail. A first read that is not complete is dropped: there is
// nothing a later read could be compared against.
func (read *Read) Complete(completion execution.ProviderCompletion, err error) {
	if read == nil || read.summary == nil {
		return
	}
	engine := read.engine
	queryGroup := read.query.Contract.Slot.QueryGroup
	owned := engine.options.Owns(queryGroup)
	lookback, lookbackKnown := queryLookback(read.query.Spec.PlanFacts)
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
		state.incompleteFirstReads++
		return
	}
	state.completeFirstRead = true
	switch {
	case read.summary.faulted:
		engine.faultLocked(FaultBucketsExceeded, read.source, queryGroup)
		return
	case !owned:
		engine.counts.samples[key2(read.source, OutcomeOwnerLost)]++
		return
	}
	if !lookbackKnown {
		// Read from a step early: the tail's first bucket may still differ
		// where a window reaches further back than that.
		engine.counts.unknownLookback[read.source]++
		lookback = read.step
	}
	keptFrom := read.keptFrom
	engine.nextID++
	state.sample = &sample{id: engine.nextID, source: read.source, queryGroup: queryGroup,
		evaluation: read.query.Contract.Slot.EvaluationTime, spec: read.query.Spec, step: read.step,
		windowEnd: time.Unix(read.query.Spec.LogicalWindow.End, 0), readAt: read.readAt, lookback: lookback,
		last: trimSummary(read.summary.buckets, keptFrom), keptFrom: keptFrom, planned: read.depth, lastChange: -1,
		probe: state.sinceProbe >= probeEvery-1, settles: state.settle, emptyFirstRead: len(read.summary.buckets) == 0,
		first: read.summary.series, delaySeconds: read.query.Spec.PlanFacts.QueryDelaySeconds}
	engine.counts.samples[key2(read.source, OutcomeCaptured)]++
}

func (engine *Engine) faultLocked(reason, source string, queryGroup execution.QueryGroupIdentity) {
	if reason == FaultBucketsExceeded {
		engine.counts.samples[key2(source, OutcomeFault)]++
	}
	engine.counts.faults[reason]++
	if engine.options.OnFault != nil {
		engine.options.OnFault(reason, queryGroup)
	}
}

// Forget drops a Query Group this process no longer owns, with its samples.
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
	for _, candidate := range [...]*sample{state.sample, state.probe} {
		if candidate != nil && !candidate.dropped {
			candidate.dropped = true
			engine.counts.samples[key2(candidate.source, OutcomeOwnerLost)]++
			engine.counts.rechecks[key3(candidate.source, RungNames[candidate.rung], RecheckOwnerLost)]++
		}
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
// counted as yielded, the window as not read there, and the next rung
// planned.
func (engine *Engine) Step(ctx context.Context) {
	now := engine.options.Now()
	engine.mu.Lock()
	due := make([]*sample, 0)
	for _, state := range engine.groups {
		for _, candidate := range [...]*sample{state.sample, state.probe} {
			if candidate != nil && candidate.running {
				// A read asked to yield that still holds its permit past its
				// own deadline has stopped honouring either.
				if !candidate.yieldAt.IsZero() && !candidate.overdue && now.Sub(candidate.yieldAt) > RecheckTimeout {
					candidate.overdue = true
					engine.faultLocked(FaultYieldOverdue, candidate.source, candidate.queryGroup)
				}
				continue
			}
			if candidate == nil {
				continue
			}
			at := candidate.readAt.Add(rungDelay(candidate.rung, candidate.step))
			switch {
			case now.Before(at):
			case now.After(at.Add(rungWindow(candidate.rung, candidate.step))):
				engine.counts.rechecks[key3(candidate.source, RungNames[candidate.rung], RecheckYielded)]++
				candidate.unread = true
				engine.advanceLocked(state, candidate, now)
			default:
				due = append(due, candidate)
			}
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
// last planned one - or, when it was probing, after its deep recheck.
func (engine *Engine) advanceLocked(state *group, candidate *sample, now time.Time) {
	candidate.rung++
	if candidate.rung < candidate.planned {
		return
	}
	if candidate.probing {
		engine.probedLocked(state, candidate, now)
		return
	}
	engine.finishLocked(state, candidate, now)
}

// finishLocked takes a sample whose planned rungs were read, what it teaches
// its Query Group, and when the group's next sample may start.
//
// A sample every rung of which was read after its last change is complete:
// the window was complete at that change, or at the first read if none. The
// group then needs one rung past its last change, a guard showing nothing
// more arrived: more at once when a sample needed more, fewer only after
// cleanSamplesToShallow samples in a row needed fewer, and then the most any
// of them needed - or at once, to what it needed, when the group settles
// after a deep recheck. It rests only its
// new deepest rung's length when it just deepened - lateness it had not
// shown, to be learned quickly - and otherwise twice its last rest, up to
// restCap, however late its data is, as long as it is late as before: a
// group is rechecked as deep as its lateness goes and, once that holds,
// about as rarely as a punctual one.
//
// A sample whose last rungs were not read is not complete: what it read is
// a lower bound. It still deepens a group whose change it did read, and
// changes nothing else.
//
// A complete sample taken to be probed waits for its deep recheck before it
// is counted; its group rests from now, and its next sample does not wait.
func (engine *Engine) finishLocked(state *group, candidate *sample, now time.Time) {
	state.sample = nil
	need := min(max(candidate.lastChange+2, 1), len(RungSteps))
	deepened := need > state.depth
	if deepened {
		state.depth, state.clean, state.cleanNeed, state.rest = need, 0, 0, RungSteps[need-1]
	}
	outcome := OutcomeCompleted
	if candidate.unread {
		outcome = OutcomeUnobserved
	}
	if outcome == OutcomeCompleted {
		candidate.completion = candidate.readAt.Sub(candidate.windowEnd)
		if candidate.lastChange >= 0 {
			candidate.completion = candidate.lastChangeAge
		}
		candidate.completion = max(candidate.completion, 0)
		if candidate.settles {
			state.settle = false
		}
		if !deepened {
			switch {
			case candidate.settles:
				state.depth, state.clean, state.cleanNeed = need, 0, 0
			case need < state.depth:
				state.clean, state.cleanNeed = state.clean+1, max(state.cleanNeed, need)
				if state.clean >= cleanSamplesToShallow {
					state.depth, state.clean, state.cleanNeed = state.cleanNeed, 0, 0
				}
			default:
				state.clean, state.cleanNeed = 0, 0
			}
			state.rest = min(state.rest*2, float64(restCap)/float64(state.step))
		}
	}
	state.nextAt = now.Add(time.Duration(min(state.rest*float64(state.step), float64(restCap)) * restSpread(candidate.queryGroup)))
	if outcome == OutcomeCompleted && candidate.probe && candidate.planned < len(RungSteps) && state.probe == nil {
		// Its rungs have classed it already, and a clean deep recheck cannot
		// add to that - a series that changes changes its buckets - so the
		// first read's series are not kept through the wait.
		candidate.probing, candidate.rung, candidate.planned = true, len(RungSteps)-1, len(RungSteps)
		candidate.first = nil
		state.probe, state.sinceProbe = candidate, 0
		return
	}
	state.sinceProbe++
	engine.recordLocked(state, candidate, outcome, now)
}

// probedLocked takes a sample's deep recheck. Clean, the sample is complete
// as its rungs read it. Changed, its data arrived after them: the sample is
// counted as probe_changed, with no completion, and its group reads every
// rung from its next sample on and settles from what that one reads. Not
// read, the sample is complete as its rungs read it, and the next sample is
// probed instead.
func (engine *Engine) probedLocked(state *group, candidate *sample, now time.Time) {
	state.probe = nil
	switch {
	case candidate.unread:
		engine.counts.probes[key2(candidate.source, ProbeUnobserved)]++
		state.sinceProbe = max(state.sinceProbe, probeEvery-1)
		engine.recordLocked(state, candidate, OutcomeCompleted, now)
	case candidate.probeChanged:
		engine.counts.probes[key2(candidate.source, ProbeChanged)]++
		deepest := len(RungSteps)
		state.depth, state.clean, state.rest, state.settle = deepest, 0, RungSteps[deepest-1], true
		engine.recordLocked(state, candidate, OutcomeProbeChanged, now)
	default:
		engine.counts.probes[key2(candidate.source, ProbeClean)]++
		engine.recordLocked(state, candidate, OutcomeCompleted, now)
	}
}

// recordLocked counts a finished sample by its outcome, and a completed one
// by its completion, as its group's latest measurement.
func (engine *Engine) recordLocked(state *group, candidate *sample, outcome string, now time.Time) {
	engine.counts.samples[key2(candidate.source, outcome)]++
	if outcome != OutcomeCompleted {
		return
	}
	completion := candidate.completion
	state.measured, state.measuredAt, state.completion = true, now, completion
	engine.noteClassLocked(state, candidate, now)
	engine.counts.completion[key2(candidate.source, ageBucket(completion))]++
	engine.counts.maxCompletion[candidate.source] = max(engine.counts.maxCompletion[candidate.source], completion)
	if candidate.emptyFirstRead {
		if candidate.lastChange >= 0 {
			engine.counts.emptyFirstReads[key2(candidate.source, EmptyArrived)]++
			engine.counts.emptyCompletion[key2(candidate.source, ageBucket(completion))]++
		} else {
			engine.counts.emptyFirstReads[key2(candidate.source, EmptyStayedEmpty)]++
		}
	}
}

// spreadFraction places a Query Group in [0, 1) by a hash of its identity.
func spreadFraction(queryGroup execution.QueryGroupIdentity) float64 {
	return float64(mix(hashString(string(queryGroup)))%1024) / 1024
}

// restSpread is how much of its rest a Query Group rests: from three
// quarters to a quarter past, by the same hash. Every group rests as long
// on average, and groups whose Slots read at one moment - which all Query
// Groups of one period do - are not all sampled, and so all rechecked, at
// one moment again.
func restSpread(queryGroup execution.QueryGroupIdentity) float64 {
	return 0.75 + 0.5*spreadFraction(queryGroup)
}

// recheckFrom is where a rung of a sample reads from: the query's own
// lookback before the kept tail, so the tail's first bucket is computed from
// the data the first read had - never before the window, where the first
// read did not read either.
func recheckFrom(candidate *sample) int64 {
	return max(candidate.spec.LogicalWindow.Start, candidate.keptFrom-int64(candidate.lookback/time.Second))
}

func (engine *Engine) recheck(ctx context.Context, candidate *sample, release func(), yield <-chan struct{}) {
	readCtx, cancel := context.WithTimeout(ctx, RecheckTimeout)
	// The watcher waits for the read to come back, not for the read's
	// context: a read that honours neither the yield nor its deadline is
	// still holding its permit after its context ended, and a yield that
	// comes then is the one yield_overdue is for.
	watched, back := make(chan struct{}), make(chan struct{})
	if yield != nil {
		go func() {
			defer close(watched)
			select {
			case <-yield:
				engine.mu.Lock()
				if !candidate.returned {
					candidate.yieldAt = engine.options.Now()
				}
				engine.mu.Unlock()
				cancel()
			case <-back:
			}
		}()
	} else {
		close(watched)
	}
	sink := &recheckSink{summary: newSummarizer(candidate.spec.PlanFacts.Normalization.CanonicalValueField)}
	if candidate.first != nil {
		sink.summary.trackSeries(candidate.keptFrom, engine.options.Memory)
	}
	started := engine.options.Now()
	completion, err := engine.options.Recheck(readCtx, tailSpec(candidate.spec, recheckFrom(candidate)), sink)
	engine.mu.Lock()
	candidate.returned = true
	engine.mu.Unlock()
	release()
	released := engine.options.Now()
	// Only after the permit is back: a yield that came while it was being
	// given back finds the watcher still waiting, and returned, not the
	// order two ready channels happen to be picked in, is what leaves it
	// untimed.
	close(back)
	cancel()
	engine.releasedAfterYield(candidate, released, watched)
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
	var read readSummary
	var series seriesChange
	var buckets []int64
	seriesUnknown := false
	if outcome == RecheckCompared {
		read = trimSummary(sink.summary.buckets, candidate.keptFrom)
		changes = compareSummaries(candidate.last, read)
		switch {
		case candidate.first != nil && sink.summary.series != nil:
			series = compareSeries(candidate.first, sink.summary.series)
		case len(changes) > 0 && !candidate.probing:
			seriesUnknown = true
		}
		if len(changes) > 0 {
			buckets = changedBuckets(candidate.last, read, maxEvidenceBuckets)
		}
	}
	age := started.Sub(candidate.windowEnd)
	engine.mu.Lock()
	defer engine.mu.Unlock()
	candidate.running = false
	if candidate.dropped {
		return
	}
	engine.counts.recheckBytes[candidate.source] += sink.bytes
	if sink.summary.faulted {
		engine.faultLocked(FaultBucketsExceeded, candidate.source, candidate.queryGroup)
	}
	candidate.seriesUnknown = candidate.seriesUnknown || seriesUnknown
	engine.counts.rechecks[key3(candidate.source, rung, outcome)]++
	// A rung compared covers any rung before it that was not read: it is
	// compared with the last read kept, not with the rung it follows.
	candidate.unread = outcome != RecheckCompared
	if series.existingChanged > 0 && !candidate.existingChanged {
		candidate.existingChanged = true
		candidate.early = &ReadEarlySample{Rung: rung, ChangedAgeSeconds: int64(age / time.Second), Buckets: buckets}
	}
	if candidate.emptyFirstRead && len(changes) > 0 && candidate.early == nil {
		// Nothing was there to change: the rung the data first came at.
		candidate.early = &ReadEarlySample{Rung: rung, ChangedAgeSeconds: int64(age / time.Second), Buckets: buckets}
	}
	if series.added > 0 && !candidate.seriesAdded {
		candidate.seriesAdded, candidate.seriesAddedRung = true, candidate.rung
	}
	if len(changes) > 0 {
		engine.counts.changed[key2(candidate.source, rung)]++
		for class, n := range changes {
			engine.counts.changes[key3(candidate.source, rung, class)] += uint64(n)
		}
		candidate.last = read
		if candidate.probing {
			candidate.probeChanged = true
		} else {
			candidate.lastChange, candidate.lastChangeAge = candidate.rung, age
			if candidate.rung == candidate.planned-1 && candidate.planned < len(RungSteps) {
				// Still arriving at the last planned rung: follow it one further.
				candidate.planned++
			}
		}
		engine.remember(Recent{Source: candidate.source, QueryGroup: candidate.queryGroup, EvaluationTime: candidate.evaluation,
			Rung: rung, ReadAgeSeconds: int64(age / time.Second), Changes: changes, At: started})
	}
	if state := engine.groups[candidate.queryGroup]; state != nil && (state.sample == candidate || state.probe == candidate) {
		engine.advanceLocked(state, candidate, engine.options.Now())
	}
}

// releasedAfterYield counts how long a read asked to yield took to give its
// permit back, once its watcher is done. A yield is noted only while the
// read has not come back (sample.returned), so one that came after it did
// not stop it and is not counted. The read is clear of its yield for its
// next rung.
func (engine *Engine) releasedAfterYield(candidate *sample, released time.Time, watched <-chan struct{}) {
	<-watched
	engine.mu.Lock()
	defer engine.mu.Unlock()
	yieldAt, overdue := candidate.yieldAt, candidate.overdue
	candidate.yieldAt, candidate.overdue, candidate.returned = time.Time{}, false, false
	if yieldAt.IsZero() {
		return
	}
	took := released.Sub(yieldAt)
	engine.counts.yieldReleases[candidate.source]++
	engine.counts.yieldReleaseSeconds[candidate.source] += took.Seconds()
	engine.counts.yieldReleaseMax[candidate.source] = max(engine.counts.yieldReleaseMax[candidate.source], took)
	if took > RecheckTimeout && !overdue {
		engine.faultLocked(FaultYieldOverdue, candidate.source, candidate.queryGroup)
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

// recheckSink sums a recheck the way a first read was summed, and counts
// the bytes it delivered as a first read's are counted.
type recheckSink struct {
	summary *summarizer
	bytes   uint64
}

func (sink *recheckSink) ConsumeProviderSeries(_ context.Context, batch execution.ProviderSeriesBatch) error {
	sink.bytes += batch.Delivery.Bytes
	sink.summary.add(batch.Dataset)
	return nil
}
