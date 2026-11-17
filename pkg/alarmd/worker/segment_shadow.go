// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"reflect"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The shadow of the Plans' stable segments (014 B3, first stage). Every
// State is written as before; beside it the coordinator keeps in process,
// a book a Plan, the member pages and the mark a round would leave were the
// steady members not written (3.3), and compares for every open member it
// would not have written the read the skip would make (segment.Made) with
// what the round wrote (state_segment_shadow_total). It writes nothing and
// reads nothing more than the round does.

// segmentShadowHorizon is how far past a closed member's close its entry is
// kept, beyond its record's retention twice over: the shadow's stand-in for
// a key's life (its window, the lateness its Levels allow and the replay
// margin), never shorter than it.
const segmentShadowHorizon = 600

type segmentShadowKey struct {
	plan       execution.PlanIdentity
	generation execution.StateGeneration
}

// segmentShadowPlan is one Plan's book and the position of the last round
// folded into it, W, zero before the first; and the Plan's grid as its last
// written round had it, for a round that writes none of its series.
type segmentShadowPlan struct {
	book      *segment.Book
	position  int64
	interval  int64
	retention uint32
	// open and closed are the book's entries as its last fold left them,
	// read under the shadow's lock by the memory gauge: the book itself is
	// the fold's.
	open, closed int
}

// segmentShadow is the coordinator's books, by Query Group, forgotten with
// the Query Group's release (ForgetQueryGroup).
type segmentShadow struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]map[segmentShadowKey]*segmentShadowPlan
}

func (shadow *segmentShadow) plan(group execution.QueryGroupIdentity, key segmentShadowKey) *segmentShadowPlan {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	if shadow.byGroup == nil {
		shadow.byGroup = map[execution.QueryGroupIdentity]map[segmentShadowKey]*segmentShadowPlan{}
	}
	plans := shadow.byGroup[group]
	if plans == nil {
		plans = map[segmentShadowKey]*segmentShadowPlan{}
		shadow.byGroup[group] = plans
	}
	state := plans[key]
	if state == nil {
		book, _ := segment.NewBook(nil)
		state = &segmentShadowPlan{book: book}
		plans[key] = state
	}
	return state
}

// existing is a Plan's book when it has one.
func (shadow *segmentShadow) existing(group execution.QueryGroupIdentity, key segmentShadowKey) *segmentShadowPlan {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	return shadow.byGroup[group][key]
}

func (shadow *segmentShadow) reset(group execution.QueryGroupIdentity, key segmentShadowKey) {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	delete(shadow.byGroup[group], key)
}

func (shadow *segmentShadow) forget(group execution.QueryGroupIdentity) {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	delete(shadow.byGroup, group)
}

// settled records a Plan's entries once a fold is done with its book.
func (shadow *segmentShadow) settled(state *segmentShadowPlan) {
	open := 0
	state.book.Each(func(entry segment.Entry) {
		if entry.Open() {
			open++
		}
	})
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	state.open, state.closed = open, state.book.Len()-open
}

// sweep drops a Query Group's books that no round will fold again: a Plan's
// older generation once a newer one of it was folded - an edit moves a Plan
// to a new generation and its old book's members would stay open for good -
// and a book not folded for longer than its reach, by the Slot's evaluation
// time and the Plan's own grid (a Plan of a longer interval is not due every
// round): a Plan deleted, stopped or moved out of the Query Group.
func (shadow *segmentShadow) sweep(group execution.QueryGroupIdentity, folded map[segmentShadowKey]bool, at int64) {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	plans := shadow.byGroup[group]
	newest := map[execution.PlanIdentity]execution.StateGeneration{}
	for key := range folded {
		newest[key.plan] = key.generation
	}
	for key, state := range plans {
		if generation, moved := newest[key.plan]; moved && generation != key.generation {
			delete(plans, key)
			continue
		}
		if folded[key] {
			continue
		}
		reach := 2*int64(state.retention)*state.interval + segmentShadowHorizon
		if state.position == 0 || at-state.position > reach {
			delete(plans, key)
		}
	}
}

// Estimated bytes of the shadow's books: a member's entry - its 64-character
// digest as the map's key and in the entry, the entry's two times and the
// map's share of a bucket - and a Plan's book and its key. An estimate, of
// the order the live heap is read against.
const (
	segmentShadowEntryBytes = 160
	segmentShadowPlanBytes  = 256
)

// SegmentShadowReading is the shadow's books, as the
// worker_segment_shadow_memory gauge reports them: the Plans with a book,
// their open and closed entries and the bytes they are estimated to hold.
type SegmentShadowReading struct {
	Plans, Open, Closed, Bytes uint64
}

// SegmentShadowMemory reads the coordinator's shadow books, at the scrape.
func SegmentShadowMemory(coordinator *SlotExecutionCoordinator) SegmentShadowReading {
	return coordinator.segmentShadow.reading()
}

func (shadow *segmentShadow) reading() SegmentShadowReading {
	shadow.mu.Lock()
	defer shadow.mu.Unlock()
	var reading SegmentShadowReading
	for _, plans := range shadow.byGroup {
		for _, state := range plans {
			reading.Plans++
			reading.Open += uint64(state.open)
			reading.Closed += uint64(state.closed)
		}
	}
	reading.Bytes = (reading.Open+reading.Closed)*segmentShadowEntryBytes + reading.Plans*segmentShadowPlanBytes
	return reading
}

// segmentShadowRound is one Slot attempt's shadow, folded into the books
// once the attempt's Progress is written (foldSegmentShadow): an attempt that
// fails leaves the books as they were, and its retry finds a round some of
// whose series are already applied - their membership this round unknown -
// and starts those Plans over.
type segmentShadowRound struct {
	group execution.QueryGroupIdentity
	// at is the Slot's evaluation time, what a book's reach is measured
	// from when no round folds it.
	at    int64
	plans map[segmentShadowKey]*segmentShadowPlanRound
}

type segmentShadowPlanRound struct {
	plan      execution.PlanIdentity
	state     *segmentShadowPlan
	interval  int64
	retention uint32
	class     string
	// present is each series the round wrote, by the source time of its
	// last point; exceptions the open members it would have written whole;
	// compared the ones it would have left, by their last point, since the
	// mark W is the round's latest and known only at the fold; joins the
	// series decided steady and normal, by their last point too.
	present    map[execution.SeriesIdentityDigest]int64
	exceptions map[observability.SegmentShadowOutcome][]execution.SeriesIdentityDigest
	compared   map[int64]*segmentShadowTally
	joins      map[int64][]execution.SeriesIdentityDigest
	retried    bool
}

type segmentShadowTally struct {
	members  []execution.SeriesIdentityDigest
	outcomes map[observability.SegmentShadowOutcome]int64
}

func newSegmentShadowRound(request execution.SlotExecutionRequest) *segmentShadowRound {
	// A supplement completes no Slot - a round's late points, not a round -
	// and a probe writes none.
	if request.Operation == execution.OperationSupplement || request.Operation == execution.OperationProbe {
		return nil
	}
	return &segmentShadowRound{group: request.Contract.Slot.QueryGroup, at: int64(request.Contract.Slot.EvaluationTime),
		plans: map[segmentShadowKey]*segmentShadowPlanRound{}}
}

// planInterval is a Plan's evaluation interval in seconds, its schedule's
// and otherwise its compiled semantics'.
func planInterval(due execution.DuePlan) int64 {
	if due.ScheduleSpec.EvaluationIntervalSeconds > 0 {
		return due.ScheduleSpec.EvaluationIntervalSeconds
	}
	if due.CompiledPlan == nil {
		return 0
	}
	return int64(due.CompiledPlan.EvaluationSemantics().EvaluationInterval)
}

func (round *segmentShadowRound) planRound(shadow *segmentShadow, due execution.DuePlan,
	identity execution.StateKeyIdentity, retention uint32) *segmentShadowPlanRound {
	key := segmentShadowKey{plan: due.Identity, generation: identity.StateGeneration}
	if planRound := round.plans[key]; planRound != nil {
		return planRound
	}
	interval := planInterval(due)
	if interval <= 0 || retention == 0 {
		return nil
	}
	planRound := &segmentShadowPlanRound{plan: due.Identity, state: shadow.plan(round.group, key),
		interval: interval, retention: retention, class: observability.SegmentPeriodClass(interval),
		present: map[execution.SeriesIdentityDigest]int64{}, compared: map[int64]*segmentShadowTally{},
		exceptions: map[observability.SegmentShadowOutcome][]execution.SeriesIdentityDigest{},
		joins:      map[int64][]execution.SeriesIdentityDigest{}}
	round.plans[key] = planRound
	return planRound
}

// retried is a Plan the attempt found series of applied by one before it,
// resumed before evaluation (an equal version, stream_session's resume): what
// the round did with them is not in this attempt, so the Plan's shadow starts
// over. Every series an equal version reaches is resumed there - real and
// no-data alike come through evaluateCompletedSeriesBatch - so the write's
// preflight, reading the same view, never finds one applied.
func (round *segmentShadowRound) retried(shadow *segmentShadow, due execution.DuePlan) {
	if round == nil {
		return
	}
	key := segmentShadowKey{plan: due.Identity, generation: due.StateGeneration}
	planRound := round.plans[key]
	if planRound == nil {
		planRound = &segmentShadowPlanRound{plan: due.Identity, state: shadow.plan(round.group, key),
			class: observability.SegmentPeriodClass(planInterval(due))}
		round.plans[key] = planRound
	}
	planRound.retried = true
}

// observe is one series the round wrote, against the view it was decided
// from.
func (round *segmentShadowRound) observe(shadow *segmentShadow, due execution.DuePlan,
	view execution.RuntimeStateView, mutation execution.StateMutation, class execution.SeriesClass, eventsSent bool) {
	if round == nil || len(mutation.Points) == 0 {
		return
	}
	planRound := round.planRound(shadow, due, mutation.Identity, mutation.RetentionPoints)
	if planRound == nil || planRound.retried {
		return
	}
	digest := mutation.Identity.SeriesIdentityDigest
	last := mutation.Points[len(mutation.Points)-1].SourceTime
	planRound.present[digest] = last
	entry, member := planRound.state.book.Entry(digest)
	switch {
	case member && entry.Open():
		if exception := planRound.wouldLeave(view, mutation, class, eventsSent); exception != "" {
			planRound.exceptions[exception] = append(planRound.exceptions[exception], digest)
			return
		}
		tally := planRound.compared[last]
		if tally == nil {
			tally = &segmentShadowTally{outcomes: map[observability.SegmentShadowOutcome]int64{}}
			planRound.compared[last] = tally
		}
		tally.members = append(tally.members, digest)
		for _, outcome := range compareMadeRead(view, mutation, segment.Coverage{Entry: entry, Position: last,
			Interval: planRound.interval, Retention: planRound.retention}) {
			tally.outcomes[outcome]++
		}
	case member:
		// Back after it was closed: written whole, and off its page.
		planRound.exceptions[observability.SegmentShadowExceptionReturned] =
			append(planRound.exceptions[observability.SegmentShadowExceptionReturned], digest)
	case class == execution.SeriesClassStableNormal && !eventsSent:
		// Steady and normal and quiet, as a member must be to be left.
		planRound.joins[last] = append(planRound.joins[last], digest)
	}
}

// wouldLeave is the exception that keeps the skip from leaving an open
// member present this round unwritten (3.3 step 2), the first in order:
// events sent; not decided steady and normal with its stored Levels and
// guard unchanged (the write-reuse class decision_stable, the decision
// write_reuse counts); a Level not FULL or with a gap reason; its stored
// points not up to the mark before or the round's not on the grid after
// it. None, the skip would leave it.
func (planRound *segmentShadowPlanRound) wouldLeave(view execution.RuntimeStateView, mutation execution.StateMutation,
	class execution.SeriesClass, eventsSent bool) observability.SegmentShadowOutcome {
	if eventsSent {
		return observability.SegmentShadowExceptionEvents
	}
	if execution.ClassifyStateWriteReuse(view, mutation, class) != execution.StateWriteReuseDecisionStable {
		return observability.SegmentShadowExceptionUnsteady
	}
	for _, level := range mutation.Levels {
		if level.HistoryCompleteness != execution.HistoryFull || level.GapReasonCode != "" {
			return observability.SegmentShadowExceptionLevel
		}
	}
	if planRound.state.position == 0 || view.LastProcessedEventTime != planRound.state.position {
		return observability.SegmentShadowExceptionOffGrid
	}
	for index, point := range mutation.Points {
		if point.SourceTime != planRound.state.position+int64(index+1)*planRound.interval {
			return observability.SegmentShadowExceptionOffGrid
		}
	}
	return ""
}

// compareMadeRead is every field the read the skip would make for a member
// differs in from what the round wrote: the points made after the stored
// ones against the round's, and the Levels and guard as the made read holds
// them against the written. None is a match.
func compareMadeRead(view execution.RuntimeStateView, mutation execution.StateMutation, coverage segment.Coverage) []observability.SegmentShadowOutcome {
	made, end, extension := segment.Made(view, coverage)
	if extension != segment.Extended {
		return []observability.SegmentShadowOutcome{observability.SegmentShadowMismatchUnmade}
	}
	var outcomes []observability.SegmentShadowOutcome
	differs := func(outcome observability.SegmentShadowOutcome, differ bool) {
		if differ {
			outcomes = append(outcomes, outcome)
		}
	}
	times, records, facts := len(made) != len(mutation.Points), len(made) != len(mutation.Points), len(made) != len(mutation.Points)
	for index := 0; index < min(len(made), len(mutation.Points)); index++ {
		times = times || made[index].SourceTime != mutation.Points[index].SourceTime
		records = records || made[index].RecordID != mutation.Points[index].RecordID
		facts = facts || !reflect.DeepEqual(made[index].Levels, mutation.Points[index].Levels)
	}
	differs(observability.SegmentShadowMismatchPointTimes, times)
	differs(observability.SegmentShadowMismatchRecordIDs, records)
	differs(observability.SegmentShadowMismatchFacts, facts)
	levels := len(view.Levels) != len(mutation.Levels)
	var completeness, gap, compatibility, warmup, processed, lastReal bool
	for index := 0; !levels && index < len(view.Levels); index++ {
		stored, written := view.Levels[index], mutation.Levels[index]
		// The gap reason's origin is not compared, as the decision and the
		// digest leave it out (decisionStateChanged): it changes nothing a
		// later round decides on.
		levels = levels || stored.LevelID != written.LevelID
		completeness = completeness || stored.HistoryCompleteness != written.HistoryCompleteness
		gap = gap || stored.GapReasonCode != written.GapReasonCode
		compatibility = compatibility || stored.LevelStateCompatibility != written.LevelStateCompatibility
		warmup = warmup || stored.WarmupRequirementRef != written.WarmupRequirementRef
		processed = processed || written.LastProcessedEventTime != end
		// A Level that keeps no last real point (it ends on no data only
		// if it does) writes zero, and the made read leaves it zero.
		expectedReal := int64(0)
		if stored.LastRealPointTime != 0 {
			expectedReal = end
		}
		lastReal = lastReal || written.LastRealPointTime != expectedReal
	}
	differs(observability.SegmentShadowMismatchLevels, levels)
	differs(observability.SegmentShadowMismatchCompleteness, completeness)
	differs(observability.SegmentShadowMismatchGapReason, gap)
	differs(observability.SegmentShadowMismatchCompatibility, compatibility)
	differs(observability.SegmentShadowMismatchWarmupRef, warmup)
	differs(observability.SegmentShadowMismatchLastProcessed, processed)
	differs(observability.SegmentShadowMismatchLastReal, lastReal)
	differs(observability.SegmentShadowMismatchGuard, !reflect.DeepEqual(view.SeriesGuard, mutation.SeriesGuard))
	if len(outcomes) == 0 {
		return []observability.SegmentShadowOutcome{observability.SegmentShadowMatch}
	}
	return outcomes
}

// foldSegmentShadow folds a completed attempt's shadow into the books, a
// Plan at a time (3.3): the mark is the latest point the round wrote; the
// members compared at it are counted, those whose last point fell short of
// it are exceptions, as the skip would have found them off the grid;
// exceptions leave their page; the series decided steady and normal at the
// mark join it; the open members absent from the round close at the mark
// before; the closed ones past their reach expire. A Plan whose output did
// not all land, or that the attempt found already applied in part, is not
// what the round decided: the first is left as it was, the second starts
// over.
func (coordinator *SlotExecutionCoordinator) foldSegmentShadow(ctx context.Context, request execution.SlotExecutionRequest,
	round *segmentShadowRound, due []execution.DuePlan, withheld map[execution.PlanIdentity]struct{}) {
	facts := foldSegmentShadowRound(&coordinator.segmentShadow, round, due, withheld)
	if facts.Empty() {
		return
	}
	coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.Operation(request.Operation),
		ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
	})
}

// foldSegmentShadowRound is foldSegmentShadow on the books, and what it
// counted.
func foldSegmentShadowRound(shadow *segmentShadow, round *segmentShadowRound, due []execution.DuePlan,
	withheld map[execution.PlanIdentity]struct{}) observability.SegmentShadowFacts {
	var facts observability.SegmentShadowFacts
	if round == nil {
		return facts
	}
	folded := map[segmentShadowKey]bool{}
	defer func() { shadow.sweep(round.group, folded, round.at) }()
	// A due Plan with a book and no series written this round is a round
	// all of whose members were absent: they close.
	for _, plan := range due {
		key := segmentShadowKey{plan: plan.Identity, generation: plan.StateGeneration}
		if _, folded := round.plans[key]; folded {
			continue
		}
		if state := shadow.existing(round.group, key); state != nil && state.interval > 0 {
			round.plans[key] = &segmentShadowPlanRound{plan: plan.Identity, state: state, interval: state.interval,
				retention: state.retention, class: observability.SegmentPeriodClass(state.interval),
				present: map[execution.SeriesIdentityDigest]int64{}}
		}
	}
	for key, planRound := range round.plans {
		if _, notLanded := withheld[planRound.plan]; notLanded {
			continue
		}
		state, class := planRound.state, planRound.class
		folded[key] = true
		if planRound.retried {
			facts.Member(observability.SegmentShadowReset, class, int64(state.book.Len()))
			shadow.reset(round.group, key)
			continue
		}
		mark := state.position
		for _, last := range planRound.present {
			mark = max(mark, last)
		}
		if mark <= state.position && len(planRound.present) > 0 {
			// A round at or before the one folded last: a replay, not news.
			continue
		}
		book := state.book
		for last, tally := range planRound.compared {
			if last != mark {
				facts.Outcome(observability.SegmentShadowExceptionShortOfMark, class, int64(len(tally.members)))
				for _, digest := range tally.members {
					_ = book.Remove(digest)
				}
				continue
			}
			for outcome, count := range tally.outcomes {
				facts.Outcome(outcome, class, count)
			}
		}
		for exception, digests := range planRound.exceptions {
			facts.Outcome(exception, class, int64(len(digests)))
			for _, digest := range digests {
				_ = book.Remove(digest)
			}
		}
		var departing []execution.SeriesIdentityDigest
		book.Each(func(entry segment.Entry) {
			if _, present := planRound.present[entry.Digest]; entry.Open() && !present {
				departing = append(departing, entry.Digest)
			}
		})
		for _, digest := range departing {
			if book.Close(digest, state.position) == nil {
				facts.Member(observability.SegmentShadowDeparted, class, 1)
			}
		}
		for _, digest := range planRound.joins[mark] {
			if book.Join(digest, mark) == nil {
				facts.Member(observability.SegmentShadowJoined, class, 1)
			}
		}
		reach := 2*int64(planRound.retention)*planRound.interval + segmentShadowHorizon
		if expired, err := book.Expire(mark - reach); err == nil {
			facts.Member(observability.SegmentShadowExpired, class, int64(expired))
		}
		dirty := book.Dirty()
		pages := book.Pages()
		var bytes int64
		for _, page := range dirty {
			bytes += int64(segment.PageBytes(pages[page]))
		}
		facts.Pages(class, int64(len(dirty)), bytes)
		book.Settle()
		state.position, state.interval, state.retention = mark, planRound.interval, planRound.retention
		shadow.settled(state)
	}
	return facts
}
