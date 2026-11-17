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
	"fmt"
	"reflect"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The shadow of the Plans' stable segments (014 B3, second stage). Every
// State is written as before; beside it each Plan's book - its member pages
// and its mark - is kept in the store beside its State: read once a Slot
// before its State (loadSegments), changed by the round as the skip would
// change it (3.3), and written once before its Progress (foldSegmentShadow).
// For every open member the skip would not have written, the read it would
// make (segment.Made) is compared with what the round wrote
// (state_segment_shadow_total).
//
// RETIRE(0.7): comparing every round goes once stage 3's skip has run on
// every deployment for 14 days with state_segment_shadow_total
// mismatch_* at 0 in each period class (le_5m, le_1h, le_1d, gt_1d), each
// class compared at least one round. What stays is a sampled comparison:
// one round a Plan's piece every 24 h, staggered by hash(Plan, piece), the
// round that writes the Plan whole anyway and compares on that write - no
// read of its own, so the sample is a self-healing write too - its rate
// fixed in code, the counter kept.

// segmentShadowHorizon is how far past a closed member's close its entry is
// kept, beyond its record's retention twice over: the shadow's stand-in for
// a key's life (its window, the lateness its Levels allow and the replay
// margin), never shorter than it.
const segmentShadowHorizon = 600

// shadowKey is a due Plan's segments under generation: the Plan, its piece
// of a split strategy and the generation, as its State store keys them.
func shadowKey(due execution.DuePlan, generation execution.StateGeneration) segment.PlanKey {
	return segment.PlanKey{Plan: due.Identity, Shard: due.Shard, Generation: generation}
}

// segmentDue is a due Plan whose segments a Slot reads: what its keys live
// by, and the points its window keeps.
type segmentDue struct {
	due       execution.DuePlan
	lifetime  segment.Lifetime
	retention uint32
}

// segmentSlot is one Slot's Plans' segments, as read before its State.
type segmentSlot struct {
	store segment.Store
	plans map[segment.PlanKey]*segmentShadowPlan
}

// segmentShadowPlan is one Plan's book as the Slot read it: its members, the
// position of the round its mark was written by (W, zero before the first),
// the mark and the pages it named. fresh is a Plan with no stored book that
// read - none yet, or one that did not read (lost, its members) - whose
// round writes every page.
type segmentShadowPlan struct {
	key       segment.PlanKey
	lifetime  segment.Lifetime
	ttl       time.Duration
	book      *segment.Book
	position  int64
	mark      segment.Mark
	hadMark   bool
	prior     int
	fresh     bool
	lost      int64
	interval  int64
	retention uint32
}

// segmentDues is the due Plans a Slot reads the segments of: a Plan on a
// grid, whose window keeps points.
func (coordinator *SlotExecutionCoordinator) segmentDues(header execution.InternalExecutionHeader) []segmentDue {
	var dues []segmentDue
	for _, due := range header.DuePlans {
		interval := planInterval(due)
		if interval <= 0 {
			continue
		}
		requirements, err := execution.DeriveStateRetentionRequirement(due.CompiledPlan)
		if err != nil {
			continue
		}
		var retention uint32
		for _, requirement := range requirements {
			retention = max(retention, requirement.RetentionPoints)
		}
		if retention == 0 {
			continue
		}
		dues = append(dues, segmentDue{due: due, retention: retention, lifetime: segment.Lifetime{Retention: requirements,
			Horizon: coordinator.stateHorizon(due), Interval: time.Duration(interval) * time.Second}})
	}
	return dues
}

// loadSegments reads the Slot's due Plans' segments before any of its State
// is read; nil for an operation that completes no round (a supplement's late
// points, a probe) or a store that keeps none, and nil with the reason when
// the read failed - counted by the store for its Plans - and the Slot runs
// without its shadow.
func (coordinator *SlotExecutionCoordinator) loadSegments(ctx context.Context, request execution.SlotExecutionRequest,
	header execution.InternalExecutionHeader) (*segmentSlot, string) {
	if request.Operation == execution.OperationSupplement || request.Operation == execution.OperationProbe {
		return nil, ""
	}
	store, ok := coordinator.ports.State.(segment.Store)
	if !ok {
		return nil, ""
	}
	slot := loadSegmentSlot(ctx, store, request, coordinator.segmentDues(header))
	if slot == nil {
		return nil, observability.SegmentShadowSkippedReadFailed
	}
	return slot, ""
}

// bookBytes is what the Slot's decoded books hold, at most (BookBytes).
func (slot *segmentSlot) bookBytes() uint64 {
	var bytes uint64
	for _, state := range slot.plans {
		bytes += segment.BookBytes(state.book.Len())
	}
	return bytes
}

// skipSegmentShadow counts the Slot run without its shadow, by why.
func (coordinator *SlotExecutionCoordinator) skipSegmentShadow(ctx context.Context, operation execution.Operation, reason string) {
	var facts observability.SegmentShadowFacts
	facts.Skip(reason)
	coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.Operation(operation),
		ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
	})
}

func loadSegmentSlot(ctx context.Context, store segment.Store, request execution.SlotExecutionRequest, dues []segmentDue) *segmentSlot {
	slot := &segmentSlot{store: store, plans: map[segment.PlanKey]*segmentShadowPlan{}}
	if len(dues) == 0 {
		return slot
	}
	read := segment.ReadRequest{Contract: request.Contract, OwnerFence: request.OwnerFence}
	for _, due := range dues {
		read.Plans = append(read.Plans, segment.ReadPlan{Key: shadowKey(due.due, due.due.StateGeneration), Lifetime: due.lifetime})
	}
	result, err := store.ReadSegments(ctx, read)
	if err != nil || len(result.Plans) != len(dues) {
		return nil
	}
	for index, stored := range result.Plans {
		due := dues[index]
		state := &segmentShadowPlan{key: read.Plans[index].Key, lifetime: due.lifetime, ttl: stored.TTL,
			interval: planInterval(due.due), retention: due.retention}
		book, mark, had, err := stored.Book()
		switch {
		case err != nil:
			// What was lost is not known: the Plan starts over, its pages
			// past one removed by the count its mark named, when it read.
			book, _ = segment.NewBook(nil)
			state.fresh, state.lost = true, int64(stored.Members())
			if had {
				state.prior = mark.Pages
			}
		case !had:
			state.fresh = true
		default:
			state.position, state.mark, state.hadMark, state.prior = mark.Position, mark, true, mark.Pages
		}
		state.book = book
		slot.plans[read.Plans[index].Key] = state
	}
	return slot
}

// segmentsOf is the segments the Slot running under ctx read, nil for none.
func segmentsOf(ctx context.Context) *segmentSlot {
	if stream := retainedOwnerFrom(ctx); stream != nil {
		return stream.segments
	}
	return nil
}

// segmentShadowRound is one Slot attempt's shadow, folded into the books and
// written before the attempt's Progress (foldSegmentShadow): an attempt that
// fails before it leaves the books as they were, and a retry - finding a
// round some of whose series are already applied, their membership this
// round unknown - starts those Plans over.
type segmentShadowRound struct {
	slot  *segmentSlot
	plans map[segment.PlanKey]*segmentShadowPlanRound
}

type segmentShadowPlanRound struct {
	due       execution.DuePlan
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

func newSegmentShadowRound(request execution.SlotExecutionRequest, slot *segmentSlot) *segmentShadowRound {
	// A supplement completes no Slot - a round's late points, not a round -
	// and a probe writes none.
	if slot == nil || request.Operation == execution.OperationSupplement || request.Operation == execution.OperationProbe {
		return nil
	}
	return &segmentShadowRound{slot: slot, plans: map[segment.PlanKey]*segmentShadowPlanRound{}}
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

func (round *segmentShadowRound) planRound(due execution.DuePlan, identity execution.StateKeyIdentity, retention uint32) *segmentShadowPlanRound {
	key := shadowKey(due, identity.StateGeneration)
	if planRound := round.plans[key]; planRound != nil {
		return planRound
	}
	state := round.slot.plans[key]
	interval := planInterval(due)
	if state == nil || interval <= 0 || retention == 0 {
		return nil
	}
	planRound := &segmentShadowPlanRound{due: due, state: state, interval: interval, retention: retention,
		class: observability.SegmentPeriodClass(interval), present: map[execution.SeriesIdentityDigest]int64{},
		compared: map[int64]*segmentShadowTally{}, exceptions: map[observability.SegmentShadowOutcome][]execution.SeriesIdentityDigest{},
		joins: map[int64][]execution.SeriesIdentityDigest{}}
	round.plans[key] = planRound
	return planRound
}

// retried is a Plan the attempt found series of applied by one before it,
// resumed before evaluation (an equal version, stream_session's resume): what
// the round did with them is not in this attempt, so the Plan's shadow starts
// over. Every series an equal version reaches is resumed there - real and
// no-data alike come through evaluateCompletedSeriesBatch - so the write's
// preflight, reading the same view, never finds one applied.
func (round *segmentShadowRound) retried(due execution.DuePlan) {
	if round == nil {
		return
	}
	key := shadowKey(due, due.StateGeneration)
	planRound := round.plans[key]
	if planRound == nil {
		state := round.slot.plans[key]
		if state == nil {
			return
		}
		planRound = &segmentShadowPlanRound{due: due, state: state, interval: state.interval, retention: state.retention,
			class: observability.SegmentPeriodClass(state.interval)}
		round.plans[key] = planRound
	}
	planRound.retried = true
}

// observe is one series the round wrote, against the view it was decided
// from.
func (round *segmentShadowRound) observe(due execution.DuePlan, view execution.RuntimeStateView, mutation execution.StateMutation,
	class execution.SeriesClass, eventsSent bool) {
	if round == nil || len(mutation.Points) == 0 {
		return
	}
	planRound := round.planRound(due, mutation.Identity, mutation.RetentionPoints)
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

// foldSegmentShadow folds an attempt's shadow into its Plans' books, renews
// their pages due and writes them, before the attempt's Progress (3.3,
// 3.5): the mark is the latest point the round wrote; the members compared
// at it are counted, those whose last point fell short of it are
// exceptions, as the skip would have found them off the grid; exceptions
// leave their page; the series decided steady and normal at the mark join
// it; the open members absent from the round close at the mark before; the
// closed ones past their reach expire; the pages due are renewed; the pages
// changed and the mark are written. A Plan whose output did not all land,
// or that the attempt found already applied in part, is not what the round
// decided: the first is left as it was, the second starts over. A write or
// renewal that fails is counted and lets the Plan's cache go: the Slot goes
// on, its State written whole.
func (coordinator *SlotExecutionCoordinator) foldSegmentShadow(ctx context.Context, request execution.SlotExecutionRequest,
	round *segmentShadowRound, due []execution.DuePlan, withheld map[execution.PlanIdentity]struct{}) {
	if round == nil {
		return
	}
	facts, folded := foldSegmentShadowRound(round, due, withheld, coordinator.segmentJoinAdmission(ctx))
	writes := settleSegmentShadow(ctx, round.slot.store, request, folded, &facts, coordinator.now)
	if len(writes) > 0 {
		_, _ = round.slot.store.WriteSegments(ctx, segment.WriteRequest{Contract: request.Contract,
			Fence: execution.StateApplyFence{Fence: request.OwnerFence, ContentScope: request.ContentScope}, Plans: writes})
	}
	if facts.Empty() {
		return
	}
	coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.Operation(request.Operation),
		ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
	})
}

// segmentJoinAdmission reserves the members a round joins as the books its
// Slot read were reserved (readSegments): quietly, the Slot going on whatever
// the pool says. Nil, every join admitted, for a Slot that holds nothing.
func (coordinator *SlotExecutionCoordinator) segmentJoinAdmission(ctx context.Context) func(int) bool {
	stream := retainedOwnerFrom(ctx)
	if stream == nil {
		return nil
	}
	return func(joins int) bool {
		bytes := uint64(joins) * segment.BookMemberBytes
		if err := coordinator.acquireProvisional(0, bytes, stream, "segment_book"); err != nil {
			return false
		}
		stream.retainBytes(retainPhaseState, bytes)
		return true
	}
}

// segmentFolded is one Plan's book as its round left it, to be renewed and
// written: the position of its mark; and, once settled, its pages laid out,
// the ones changed, their renewal table and the ones due.
type segmentFolded struct {
	state    *segmentShadowPlan
	due      execution.DuePlan
	position int64
	pages    [][]segment.Entry
	dirty    []int
	renewed  []segment.Renewal
	renew    []int
}

// foldSegmentShadowRound is foldSegmentShadow on the books: what it counted,
// and the books it changed. admit, when set, says whether a Plan's joins
// are taken in.
func foldSegmentShadowRound(round *segmentShadowRound, due []execution.DuePlan, withheld map[execution.PlanIdentity]struct{},
	admit func(joins int) bool) (observability.SegmentShadowFacts, []*segmentFolded) {
	var facts observability.SegmentShadowFacts
	if round == nil {
		return facts, nil
	}
	// A due Plan with a book and no series written this round is a round
	// all of whose members were absent: they close.
	for _, plan := range due {
		key := shadowKey(plan, plan.StateGeneration)
		if _, folded := round.plans[key]; folded {
			continue
		}
		if state := round.slot.plans[key]; state != nil && (state.hadMark || state.lost > 0) {
			round.plans[key] = &segmentShadowPlanRound{due: plan, state: state, interval: state.interval,
				retention: state.retention, class: observability.SegmentPeriodClass(state.interval),
				present: map[execution.SeriesIdentityDigest]int64{}}
		}
	}
	var folded []*segmentFolded
	for _, planRound := range round.plans {
		if _, notLanded := withheld[planRound.due.Identity]; notLanded {
			continue
		}
		state, class := planRound.state, planRound.class
		facts.Member(observability.SegmentShadowReset, class, state.lost)
		state.lost = 0
		if planRound.retried {
			facts.Member(observability.SegmentShadowReset, class, int64(state.book.Len()))
			state.book, _ = segment.NewBook(nil)
			state.position, state.fresh = 0, true
			folded = append(folded, &segmentFolded{state: state, due: planRound.due})
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
		// The members it joins are reserved as the books read were; the pool
		// refusing them, they join a later round, written whole as today.
		joins := planRound.joins[mark]
		if len(joins) > 0 && admit != nil && !admit(len(joins)) {
			facts.Skip(observability.SegmentShadowSkippedBookRefused)
			joins = nil
		}
		for _, digest := range joins {
			if book.Join(digest, mark) == nil {
				facts.Member(observability.SegmentShadowJoined, class, 1)
			}
		}
		reach := 2*int64(planRound.retention)*planRound.interval + segmentShadowHorizon
		if expired, err := book.Expire(mark - reach); err == nil {
			facts.Member(observability.SegmentShadowExpired, class, int64(expired))
		}
		folded = append(folded, &segmentFolded{state: state, due: planRound.due, position: mark})
		state.position = mark
	}
	return facts, folded
}

// settleSegmentShadow lays the folded books out over their pages, renews
// the pages due (3.5) and returns the writes that leave them. A page is due
// once a quarter of the shorter of the lifetime its last renewal gave and
// this round's has passed by the worker's wall clock (rule 1); its open
// members' State keys are renewed, and its renewal time moves on to the
// wall clock read before the first of them was sent only if every segment
// carrying them answered (rule 3) - otherwise it stays, and the page is
// renewed whole next round. A page unrenewed for half its lifetime is
// counted fallback_write: in the second stage every State is written, so
// nothing more is done.
func settleSegmentShadow(ctx context.Context, store segment.Store, request execution.SlotExecutionRequest, folded []*segmentFolded,
	facts *observability.SegmentShadowFacts, clock func() time.Time) []segment.PlanWrite {
	now := clock().UnixMilli()
	var renewals []segment.PlanRenewal
	var renewing []*segmentFolded
	for _, fold := range folded {
		state := fold.state
		fold.dirty = state.book.Dirty()
		fold.pages = state.book.Pages()
		if state.fresh {
			fold.dirty = fold.dirty[:0]
			for page := range fold.pages {
				fold.dirty = append(fold.dirty, page)
			}
		}
		ttl := state.ttl.Milliseconds()
		if ttl <= 0 {
			continue
		}
		fold.renewed = segment.Renewals(state.mark, state.hadMark && !state.fresh, len(fold.pages), segmentPlanName(state.key), now, ttl)
		var pages []segment.PageRenewal
		for page, renewal := range fold.renewed {
			if renewal.Fallback(now) {
				facts.Renewal(observability.SegmentRenewalFallbackWrite, 1)
			}
			if !renewal.Due(now, ttl) {
				continue
			}
			due := segment.PageRenewal{Page: page}
			for _, entry := range fold.pages[page] {
				if entry.Open() {
					due.Members = append(due.Members, entry.Digest)
				}
			}
			fold.renew = append(fold.renew, page)
			pages = append(pages, due)
		}
		if len(pages) > 0 {
			renewals = append(renewals, segment.PlanRenewal{Key: state.key, Lifetime: state.lifetime, Pages: pages})
			renewing = append(renewing, fold)
		}
	}
	if len(renewals) > 0 {
		at := clock().UnixMilli()
		result, err := store.RenewSegments(ctx, segment.RenewRequest{Contract: request.Contract,
			Fence: execution.StateApplyFence{Fence: request.OwnerFence, ContentScope: request.ContentScope}, Plans: renewals})
		for index, fold := range renewing {
			var plan segment.PlanRenewed
			if err == nil && index < len(result.Plans) {
				plan = result.Plans[index]
			}
			for position, page := range fold.renew {
				if position < len(plan.Renewed) && plan.Renewed[position] {
					fold.renewed[page] = segment.Renewed(at, fold.state.ttl.Milliseconds())
					facts.Renewal(observability.SegmentRenewalRenewed, 1)
					continue
				}
				facts.Renewal(observability.SegmentRenewalFailed, 1)
			}
			facts.Renewal(observability.SegmentRenewalMissing, int64(plan.Missing))
			facts.Renewal(observability.SegmentRenewalOverBudget, int64(plan.OverBudget))
		}
	}
	var writes []segment.PlanWrite
	for _, fold := range folded {
		if write, ok := segmentPlanWrite(fold, request.Contract, facts); ok {
			writes = append(writes, write)
		}
	}
	return writes
}

// segmentPlanName is what a Plan's new pages' renewal times are staggered
// by: a piece of a split strategy's with its matcher digest, a Plan not
// split's as it always was.
func segmentPlanName(key segment.PlanKey) string {
	name := fmt.Sprintf("%s/%s/%s/%s", key.Plan.TenantID, key.Plan.BusinessID, key.Plan.StrategyID, key.Generation)
	if !key.Shard.IsZero() {
		name += "/" + key.Shard.MatcherDigest
	}
	return name
}

// segmentPlanWrite is the write that leaves a Plan's book as its round
// changed and renewed it: the pages it changed - every page of a fresh book
// - and the mark at its position with the pages' renewal table and sizes.
// The pages written and their bytes are counted into facts. False writes
// nothing: a book that does not encode, or a Plan with no life to give its
// keys.
func segmentPlanWrite(fold *segmentFolded, contract execution.FrozenExecutionContractRef, facts *observability.SegmentShadowFacts) (segment.PlanWrite, bool) {
	state, pages := fold.state, fold.pages
	if state.ttl <= 0 || len(fold.renewed) != len(pages) {
		return segment.PlanWrite{}, false
	}
	encoded := make(map[int][]byte, len(fold.dirty))
	var bytes int64
	// A page not written keeps its size: the count did not change, or every
	// page would be written.
	sizes := make([]int, len(pages))
	if state.hadMark && !state.fresh && len(state.mark.Sizes) == len(pages) {
		copy(sizes, state.mark.Sizes)
	}
	for _, page := range fold.dirty {
		raw, err := segment.EncodePage(pages[page])
		if err != nil {
			return segment.PlanWrite{}, false
		}
		encoded[page] = raw
		sizes[page] = len(raw)
		bytes += int64(len(raw))
	}
	version, err := execution.BuildApplyVersion(contract, fold.due.StateApplyEpoch)
	if err != nil {
		return segment.PlanWrite{}, false
	}
	written := segment.Mark{Version: version, Position: fold.position, Pages: len(pages), Renewed: fold.renewed, Sizes: sizes}
	mark, err := segment.EncodeMark(written)
	if err != nil {
		return segment.PlanWrite{}, false
	}
	facts.Pages(observability.SegmentPeriodClass(state.interval), int64(len(fold.dirty)), bytes)
	state.book.Settle()
	write := segment.PlanWrite{Key: state.key, Lifetime: state.lifetime, Pages: encoded, Mark: mark, PageCount: len(pages),
		PriorCount: state.prior}
	// The book is as written: the next round of this Slot's attempt, were
	// there one, would start from it.
	state.mark, state.hadMark, state.fresh, state.prior = written, true, false, len(pages)
	return write, true
}
