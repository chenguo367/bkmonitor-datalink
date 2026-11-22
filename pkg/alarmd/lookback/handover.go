// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"context"
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Query Group's lookback handed to its next owner (014 B4 H1, H2). What a
// process knows of a Query Group's late series lives in its memory: its
// Slots still on their reread ladder, each due to be read again at its
// rung's delay and its late series supplemented, and its series_late state.
// Handed over with the Query Group - moved away, declined, stopping - its
// next owner reads those Slots again as the previous one would have, and
// starts series_late where it was rather than waiting for the control
// plane's plan.
//
// A Slot handed over carries its times and its rung, not its first read's
// series: on a large Query Group those are megabytes, and its next owner
// does without them. It reads the Slot's window again whole and supplements
// every series of it; the supplement decides nothing a second time for a
// series whose State or whose stable segment reached the Slot (supplement
// CrossedT, CoveredT), which is every one its first read held, so what it
// decides is what a reread against that first read would have found late.

// HandoverSlot is one Slot on the reread ladder as its owner hands it over:
// its evaluation time, the rung its reread is due at, its data step, when
// its first read was read and the read hold its contract was frozen with -
// which its reread is frozen with again - and the source it reads.
type HandoverSlot struct {
	Evaluation     execution.EvaluationTime `json:"evaluation"`
	Rung           int                      `json:"rung"`
	StepMillis     int64                    `json:"step_ms"`
	ReadAtMillis   int64                    `json:"read_at_ms"`
	ReadHoldMillis int64                    `json:"read_hold_ms"`
	Source         string                   `json:"source,omitempty"`
}

// HandoverSeriesLate is a Query Group's series_late state as its owner hands
// it over: the rung its late series were last seen at, since when it has
// been read directed, how many late series its own samples saw, and whether
// a plan started it.
type HandoverSeriesLate struct {
	Rung        int    `json:"rung"`
	SinceMillis int64  `json:"since_ms"`
	Seen        uint64 `json:"seen,omitempty"`
	Seeded      bool   `json:"seeded,omitempty"`
}

// Handover is what of a Query Group's lookback its owner hands over.
type Handover struct {
	SeriesLate *HandoverSeriesLate `json:"series_late,omitempty"`
	Slots      []HandoverSlot      `json:"slots,omitempty"`
}

// What became of a Slot handed over, closed: restored, back on its ladder
// here; expired, past its rung's window when it came; not_series_late, the
// Query Group read directed neither by the previous owner's word nor by its
// own samples here, which no Slot is read directed under; present, a Slot
// this process already holds; no_slice_shape, a restored Slot let go past
// its ladder's deepest rung's window with no read here yet to say which
// slices the Query Group is read in - it is read in those, never whole. A
// Slot let go so was counted restored when it came.
const (
	HandoverSlotRestored      = "restored"
	HandoverSlotExpired       = "expired"
	HandoverSlotNotSeriesLate = "not_series_late"
	HandoverSlotPresent       = "present"
	HandoverSlotNoSliceShape  = "no_slice_shape"
)

// HandoverSlotResults is every result.
var HandoverSlotResults = []string{HandoverSlotRestored, HandoverSlotExpired, HandoverSlotNotSeriesLate, HandoverSlotPresent,
	HandoverSlotNoSliceShape}

// What became of a series_late state handed over, closed: restored; kept_local,
// a sample of the Query Group here already decides it.
const (
	HandoverLateRestored  = "restored"
	HandoverLateKeptLocal = "kept_local"
)

// HandoverLateResults is every result.
var HandoverLateResults = []string{HandoverLateRestored, HandoverLateKeptLocal}

// How a restored Slot's supplement told which series were decided already,
// closed: segment, a member its stable segment covered, its State not read;
// persisted_version, one whose State had reached the Slot; read, one whose
// State was read and had not, which the supplement went on to decide.
const (
	RestoredDedupSegment          = "segment"
	RestoredDedupPersistedVersion = "persisted_version"
	RestoredDedupRead             = "read"
)

// RestoredDedupSources is every source.
var RestoredDedupSources = []string{RestoredDedupSegment, RestoredDedupPersistedVersion, RestoredDedupRead}

// UnobservedRestoredUnplanned is a restored Slot whose queries could not be
// planned again (Options.Respec): it is read nowhere, as a Slot whose first
// read did not complete is not. UnobservedNoSliceShape is one let go at its
// deepest rung with no slices to read it in (HandoverSlotNoSliceShape).
const (
	UnobservedRestoredUnplanned = "restored_unplanned"
	UnobservedNoSliceShape      = "no_slice_shape"
)

// RestoredPlan is how a Slot handed over is read again here: its physical
// queries, planned again with its read hold; the slices this process reads
// the Query Group in now, the zero one for a Query Group read whole - read
// one after another, as the previous owner read its first read's - Shaped
// false while no read here has said which; and Covered, which says a series
// the read returns was decided at the Slot already, its Plans' stable
// segments covering its last point there, so the read keeps it not. Nil
// keeps every series.
type RestoredPlan struct {
	Specs   []execution.PhysicalQuerySpec
	Slices  []execution.InputSlice
	Shaped  bool
	Covered func(series execution.SeriesIdentityDigest, last int64) bool
}

// Respec plans a Slot of a Query Group again, its contract frozen with the
// read hold given: how a Slot handed over without its first read is read
// again (RestoredPlan).
type Respec func(ctx context.Context, queryGroup execution.QueryGroupIdentity, evaluation execution.EvaluationTime,
	readHoldMillis int64) (RestoredPlan, error)

// ForgetHandingOver is Forget for a Query Group this process hands to its
// next owner, with what of its lookback that owner reads on from: its Slots
// still on their reread ladder and its series_late state; nil with neither.
func (engine *Engine) ForgetHandingOver(queryGroup execution.QueryGroupIdentity, reason string) *Handover {
	if engine == nil {
		return nil
	}
	engine.mu.Lock()
	handover := engine.handoverLocked(queryGroup)
	engine.mu.Unlock()
	engine.Forget(queryGroup, reason)
	return handover
}

func (engine *Engine) handoverLocked(queryGroup execution.QueryGroupIdentity) *Handover {
	state := engine.groups[queryGroup]
	if state == nil {
		return nil
	}
	handover := &Handover{}
	for evaluation, slot := range state.directed {
		if slot.dropped {
			continue
		}
		handover.Slots = append(handover.Slots, HandoverSlot{Evaluation: evaluation, Rung: slot.rung, StepMillis: slot.step.Milliseconds(),
			ReadAtMillis: slot.readAt.UnixMilli(), ReadHoldMillis: slot.readHold, Source: slot.source})
	}
	sort.Slice(handover.Slots, func(i, j int) bool { return handover.Slots[i].Evaluation < handover.Slots[j].Evaluation })
	if late := state.seriesLate; late != nil {
		handover.SeriesLate = &HandoverSeriesLate{Rung: late.rung, SinceMillis: late.since.UnixMilli(), Seen: late.seen, Seeded: late.seeded}
	}
	if handover.SeriesLate == nil && len(handover.Slots) == 0 {
		return nil
	}
	return handover
}

// Restore takes what the previous owner of a Query Group handed over, as
// this process takes the Query Group over and before its first Slot here.
// Its series_late state is taken as a plan's is (seedLocked), the evidence
// here first: a Query Group a sample of which completed here keeps its own;
// one read directed only on a plan takes the previous owner's. Its Slots
// still within their rung's window go back on their ladder, read again at
// their rung's delay from their first read, as the previous owner would
// have; each is counted by what became of it.
func (engine *Engine) Restore(queryGroup execution.QueryGroupIdentity, handover *Handover) {
	if engine == nil || handover == nil {
		return
	}
	now := engine.options.Now()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	state := engine.groups[queryGroup]
	if state == nil {
		state = &group{depth: 1, rest: RungSteps[0], sinceProbe: probeEvery - 2}
		state.reading.since = now
		if engine.options.ProbeFirstSamples {
			state.sinceProbe = probeEvery - 1
		}
		if !engine.options.UnspreadFirstSamples {
			state.nextAt = now.Add(time.Duration(spreadFraction(queryGroup) * float64(restCap)))
		}
		engine.groups[queryGroup] = state
	}
	if late := handover.SeriesLate; late != nil && late.Rung >= 0 && late.Rung < len(RungSteps) {
		switch current := state.seriesLate; {
		case state.sampled:
			engine.counts.handoverLate[HandoverLateKeptLocal]++
		case current == nil || current.seeded && current.seen == 0:
			state.seriesLate = &seriesLateState{rung: late.Rung, since: time.UnixMilli(late.SinceMillis), seen: late.Seen, seeded: late.Seeded}
			engine.counts.handoverLate[HandoverLateRestored]++
		default:
			engine.counts.handoverLate[HandoverLateKeptLocal]++
		}
	}
	for _, handed := range handover.Slots {
		step := time.Duration(handed.StepMillis) * time.Millisecond
		readAt := time.UnixMilli(handed.ReadAtMillis)
		switch {
		case handed.Rung < 0 || handed.Rung >= len(RungSteps) || step <= 0 ||
			!now.Before(readAt.Add(rungDelay(handed.Rung, step)+rungWindow(handed.Rung, step))):
			engine.counts.handoverSlots[HandoverSlotExpired]++
			continue
		case state.seriesLate == nil:
			engine.counts.handoverSlots[HandoverSlotNotSeriesLate]++
			continue
		case state.directed[handed.Evaluation] != nil:
			engine.counts.handoverSlots[HandoverSlotPresent]++
			continue
		}
		if state.directed == nil {
			state.directed = map[execution.EvaluationTime]*directedSlot{}
		}
		if state.supplement == nil {
			state.supplement = newSupplementTally(now)
		}
		source := handed.Source
		if source == "" {
			source = state.source
		}
		engine.nextID++
		state.directed[handed.Evaluation] = &directedSlot{id: engine.nextID, source: source, queryGroup: queryGroup,
			evaluation: handed.Evaluation, step: step, readAt: readAt, rung: handed.Rung, period: state.seriesLate,
			readHold: handed.ReadHoldMillis, restored: true}
		engine.counts.handoverSlots[HandoverSlotRestored]++
	}
}

// respec plans a restored Slot's queries again, outside the lock, and puts
// them on the Slot as its first reads, one a slice, of no series: the
// reread then keeps every series of its slices its Plans' segments do not
// cover (keptSink). The reason the Slot is not read, "" when it is; deferred
// when no read here has said which slices the Query Group is read in yet -
// asked before it is planned, which reads its segments: the Slot waits for
// it where it is on its ladder, and every later Slot of its Query Group
// with it (dueDirectedLocked), so none is supplemented before it.
func (engine *Engine) respec(ctx context.Context, slot *directedSlot) (reason string, deferred bool) {
	if engine.options.Respec == nil {
		return UnobservedRestoredUnplanned, false
	}
	if known := engine.options.ShapeKnown; known != nil && !known(slot.queryGroup) {
		engine.mu.Lock()
		slot.awaitingShape = true
		engine.mu.Unlock()
		return "", true
	}
	plan, err := engine.options.Respec(ctx, slot.queryGroup, slot.evaluation, slot.readHold)
	switch {
	case err != nil || len(plan.Specs) == 0:
		return UnobservedRestoredUnplanned, false
	case len(plan.Specs) != 1:
		return UnobservedMultiQuery, false
	case plan.Specs[0].PlanFacts.NamedOutputs != nil:
		return UnobservedNamedOutputs, false
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if !plan.Shaped || len(plan.Slices) == 0 {
		slot.awaitingShape = true
		return "", true
	}
	slot.awaitingShape = false
	slot.queries = make([]*directedQuery, 0, len(plan.Slices))
	for _, slice := range plan.Slices {
		slot.queries = append(slot.queries, &directedQuery{spec: plan.Specs[0], slice: slice, first: &seriesSet{}, complete: true,
			finished: true, slot: slot, covered: plan.Covered})
	}
	return "", false
}

// noteRestoredLocked counts how a restored Slot's supplement told the series
// decided already from those it decided.
func (engine *Engine) noteRestoredLocked(facts execution.SupplementFacts) {
	engine.counts.restoredDedup[RestoredDedupSegment] += uint64(facts.CoveredT)
	engine.counts.restoredDedup[RestoredDedupPersistedVersion] += uint64(max(facts.CrossedT-facts.CoveredT, 0))
	engine.counts.restoredDedup[RestoredDedupRead] += uint64(facts.Unreached)
}
