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
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Slot is applied window by window once its results no longer fit one
// (decision-020 batch 8, 2.1-2.4). Series are evaluated in the order they
// always were - Plan, then series digest, in the State preflight's batches -
// and before a series' result is merged, a window it would overfill is
// applied first: its Plans' events written and acknowledged, then their
// State, under a Sequence of the window's own State keys. A window is cut
// between series only, so every binding of one (Plan, series) - a Plan of
// several inputs included - is in the window its series is in, and each
// State key belongs to exactly one window.
//
// The gap markers go in a head and a tail. The head, one Sequence of every
// due Plan's gap key, runs before the first window's events and writes each
// Plan's open statement, derived from every binding the completion handed
// the Plan (P2) rather than from the series evaluated so far: a series in
// the fifth window whose input was incomplete must have opened the marker
// before the first window's events went out. The tail, another Sequence of
// the same keys, runs after the last window and writes the clears, the
// no-data memory and ends, the activation recheck and Progress.
//
// A Slot that fits one window never gets here: it is applied whole, inside
// the one Sequence of every key it always ran under (finalizeSummarized),
// which is a head, one window and a tail at once with nothing to nest.
//
// Head, windows and tail are each their own Sequence, never one inside
// another, so a Slot holds at most one at any time. Nested, they deadlock in
// two places. The key gate grants a reservation only at the head of every
// one of its keys' queues (OrderedKeyGate.canGrantLocked): a reservation
// holding a gap key and a State key of this Slot - a supplement takes the
// Slot's whole scope, supplement.go - registered between the head and a
// window waits behind the head on the gap key, the window waits behind it on
// the State key, and the head waits for the window it encloses. And the
// admission semaphore would hold two of its places per Slot
// (KeyedSideEffectSequencer.Sequence), so N Slots each holding one and
// asking for the second wait on each other at full capacity.
//
// Between the head and the tail the gap keys are free. A supplement or a
// replay of the same Plan that runs there finds the marker the head opened
// and no clear: the shape a Slot that crashed between its open statement and
// its clear has always left, which is what an open marker says - events may
// be out, State may not all have landed - and which every reader already
// handles as a gap. Nothing new is stored for it.
//
// A supplement is left on the one-window path: it writes no marker, memory
// or Progress, so there is no head or tail to split it around, and its
// series are the scope a Slot left undecided.

// slotWindows is what a Slot keeps while it is applied window by window.
//
// Results stay Slot-wide in stream.evaluated for now, with cursors saying how
// far the windows applied have taken them; the window being filled is
// everything past the cursors. Letting a window's results, views and inputs
// go once it is applied (batch 8, the commit after this one) turns the
// cursors into truncation: windowParts and the cursor updates in applyWindow
// are the places that change.
type slotWindows struct {
	// prepared is every series the completion handed the evaluation, for the
	// head's open statements (headOpenStatements). It is the same slice the
	// evaluation walks, not a copy.
	prepared []preparedSeries
	// started is set once the head has run: from then on the Slot is applied
	// window by window, and its tail runs after the evaluation in place of
	// the one Sequence a Slot that fits a window runs.
	started bool
	// single is set when the head found the Slot cannot be cut - its
	// activations could not be read, or a Plan's first-seen judgement waits
	// on every one of its series (firstSeenUndecided) - and nothing was
	// written: no window is cut from then on, and the Slot is applied whole,
	// as it was before windows.
	single bool
	apply  *slotApply
	// headOpened is the Plans the head opened a statement for. Their tail
	// writes no clear: any open statement of the Slot excludes a clear
	// (mergeGapGuardStatements), and one statement per Plan per Slot is what
	// the marker takes.
	headOpened map[execution.PlanIdentity]struct{}
	// applied is, by Plan, how many of its State results in stream.evaluated
	// a window has taken; views is the same for stream.state.Items and
	// stream.stateItems, which grow together, one entry per evaluated series.
	applied map[execution.PlanIdentity]int
	views   int
	// bytes is what the output and state phases have retained since the last
	// window was applied: the window's own share of the Slot.
	bytes uint64
	// blocked is the Plans whose output was not written in a window -
	// deferred, its ACK unknown, or refused whole (planOutputWithheld). None
	// of their later windows is written and their tail writes nothing, as a
	// Slot applied whole skips the rest of such a Plan: a clear over series
	// whose decisions were never sent would report the Plan's history whole
	// while those series sit one Slot behind. A refusal stays TERMINAL and
	// Progress moves past it; the others leave the Slot to be retried.
	//
	// A batch the sink wrote but for some events (planOutputPartial) does
	// not block the Plan: those series keep their State and every other
	// series moves, in this window and the later ones, which is what the
	// same batch does to a Slot applied whole.
	blocked map[execution.PlanIdentity]struct{}
	// replaced is the Plans a Slot over its budget answers UNAVAILABLE for
	// (completeWindowsBeyondSlotBudget): their results past the cursors are
	// not written, and their tail writes the budget's open statement.
	replaced map[execution.PlanIdentity]struct{}
	// evaluating is the Plan whose result the evaluation is producing, and
	// passed the Plans whose series it has moved past.
	evaluating    execution.PlanIdentity
	evaluatingSet bool
	passed        map[execution.PlanIdentity]struct{}
	// resumed is, by Plan, the series this attempt found already written at
	// this Slot and did not evaluate again (resumedSeriesResult).
	resumed map[execution.PlanIdentity]int
	landing planLanding
	decided map[execution.PlanIdentity]struct{}
	count   int
}

// beginEvaluating notes the Plan the next result is for. The Plan before it
// has had every series of the phase evaluated; the Plan itself is in
// progress again, which a no-data Plan is when its synthetic series come
// after every Plan's own.
func (stream *streamedExecution) beginEvaluating(plan execution.PlanIdentity) {
	windows := &stream.windows
	if windows.evaluatingSet && windows.evaluating == plan {
		return
	}
	if windows.passed == nil {
		windows.passed = make(map[execution.PlanIdentity]struct{})
	}
	if windows.evaluatingSet {
		windows.passed[windows.evaluating] = struct{}{}
	}
	delete(windows.passed, plan)
	windows.evaluating, windows.evaluatingSet = plan, true
}

// noteResumed counts a series this attempt resumed rather than evaluated.
func (stream *streamedExecution) noteResumed(plan execution.PlanIdentity) {
	if stream.windows.resumed == nil {
		stream.windows.resumed = make(map[execution.PlanIdentity]int)
	}
	stream.windows.resumed[plan]++
}

// windowReleasable is what applying the window would give back of what the
// Slot holds. Nothing yet: every window's results, views and inputs stay
// until the Slot ends, so a window applied for its bytes would leave the
// Slot exactly as full and be refused by its share all the same - having
// written its events first. Once a window's results are let go after it is
// applied, this is windows.bytes.
func (stream *streamedExecution) windowReleasable() uint64 {
	return 0
}

// windowBound is the most the window being filled may hold: the Slot's
// share of the pool, less what the Slot holds outside the window - the
// inputs kept to the completion, the gap facts, the no-data memory, and the
// windows applied before it until they are let go.
func (stream *streamedExecution) windowBound() uint64 {
	share := stream.coordinator.qgShareBytes()
	if share == 0 {
		return math.MaxUint64
	}
	outside := stream.retainedTotal() - min(stream.windowReleasable(), stream.retainedTotal())
	if outside >= share {
		return 0
	}
	return share - outside
}

// windowBeforeMerge applies the window being filled when the evaluated
// series next would overfill it: past the per-Slot State mutation or event
// cap, or past the window's bytes bound. The series then starts the next
// window. A window holding no State and no event is not applied for it: a
// series that alone does not fit is refused by name, as it always was.
func (stream *streamedExecution) windowBeforeMerge(ctx context.Context, next execution.EvaluationResult, retained uint64) error {
	windows := &stream.windows
	if stream.supplement != nil || windows.single || stream.effects.states+stream.effects.events == 0 {
		return nil
	}
	delta := addedEffectCounts(stream.evaluated, next)
	budget := stream.coordinator.slotBudget()
	full := stream.effects.states+delta.states > budget.MaxStateMutations ||
		stream.effects.events+delta.events > budget.MaxEvents
	if !full && stream.windowReleasable() > 0 {
		full = retained+newEffectBytes(stream.evaluated, next) > stream.windowBound()
	}
	if !full {
		return nil
	}
	if !windows.started {
		started, err := stream.coordinator.applyWindowHead(ctx, stream)
		if err != nil {
			return err
		}
		if !started {
			windows.single = true
			return nil
		}
	}
	return stream.coordinator.applyWindow(ctx, stream, false)
}

// headOpenStatements is the open statement each due Plan writes this Slot,
// derived from every binding the completion handed it, by the rules each
// result is built by: a series whose PRIMARY did not complete proposes from
// its PRIMARY bindings (mergePrimaryIncompleteSeries), every other series
// from the inputs the guard reads (evaluateLoadedSeries), and a Plan without
// series from its completion-only set (noSeriesPlanResult). The fold is one
// per scope over all of them, which is what the batches' statements union
// to (unionGapOpenStatements).
//
// Every series counts, the ones this attempt will find already written or
// superseded included: which of them those are is only known from their
// State, and the statement has to be out before the first window's events.
func (stream *streamedExecution) headOpenStatements() (map[execution.PlanIdentity]execution.PlanGapMutation, error) {
	incomplete := make(map[execution.PlanIdentity][]execution.NamedInputBinding)
	for _, prepared := range stream.windows.prepared {
		plan := prepared.due.Identity
		if primary := primaryIncompleteBindings(prepared.inputs); len(primary) != 0 {
			incomplete[plan] = append(incomplete[plan], primary...)
			continue
		}
		for _, input := range prepared.inputs {
			for _, binding := range input.Inputs {
				if execution.InputIncompleteForGuard(binding) {
					incomplete[plan] = append(incomplete[plan], binding)
				}
			}
		}
	}
	for _, due := range stream.header.DuePlans {
		if len(stream.planSeries[due.Identity]) != 0 {
			continue
		}
		bindings := stream.noSeriesBindings(due)
		if _, found := firstNonFullPrimary(bindings); found {
			incomplete[due.Identity] = bindings
		}
	}
	opens := make(map[execution.PlanIdentity]execution.PlanGapMutation, len(incomplete))
	for _, due := range stream.header.DuePlans {
		bindings := incomplete[due.Identity]
		if len(bindings) == 0 {
			continue
		}
		mutation, err := stream.completionGapMutationFor(due, bindings)
		if err != nil {
			return nil, err
		}
		opens[due.Identity] = mutation
	}
	return opens, nil
}

// applyWindowHead runs the head: the activations read, and every Plan's open
// statement written, before the first window's events. It returns false,
// having written nothing, when the Slot cannot be cut.
func (coordinator *SlotExecutionCoordinator) applyWindowHead(ctx context.Context, stream *streamedExecution) (bool, error) {
	opens, err := stream.headOpenStatements()
	if err != nil {
		return false, err
	}
	started := false
	err = coordinator.ports.Sequencer.Sequence(ctx, sequencingScope(stream.header, nil, stream.gapItems), func(sequenceCtx context.Context) error {
		// No unseen Plans are handed over: a Plan that could be first-seen
		// stops the head below, and for every other forced Plan the answer
		// does not depend on them.
		apply, read := coordinator.beginSlotApply(sequenceCtx, stream.request, stream.header, stream.gaps, nil)
		if !read || apply.firstSeenUndecided {
			return nil
		}
		apply.keys = make(slotStateKeys)
		headOpened := make(map[execution.PlanIdentity]struct{}, len(opens))
		for _, due := range sortedDuePlans(stream.header.DuePlans) {
			if apply.changed(due) {
				apply.withheldNoData[due.Identity] = struct{}{}
				continue
			}
			open, found := opens[due.Identity]
			if !found {
				continue
			}
			headOpened[due.Identity] = struct{}{}
			if err := coordinator.admit(sequenceCtx, stream.request, due); err != nil {
				return err
			}
			statement := execution.PlanEvaluationResult{Plan: due.Identity, GuardBeforeEvents: []execution.PlanGapMutation{open}}
			if _, err := apply.planGapsBefore(sequenceCtx, due, &statement); err != nil {
				return err
			}
		}
		stream.windows.apply, stream.windows.headOpened, stream.windows.started = apply, headOpened, true
		started = true
		return nil
	})
	return started, err
}

// windowPart is one Plan's part of a window.
type windowPart struct {
	due     execution.DuePlan
	results []execution.StateEvaluation
}

// windowParts is the window being filled: by Plan, the State results past
// the Plan's cursor, and every Plan with a view in the window even when it
// has no result there - its frozen series are renewed from that window.
func (stream *streamedExecution) windowParts(views []execution.RuntimeStateView) ([]windowPart, error) {
	windows := &stream.windows
	parts := make(map[execution.PlanIdentity]*windowPart)
	partFor := func(plan execution.PlanIdentity) (*windowPart, error) {
		if part, found := parts[plan]; found {
			return part, nil
		}
		due, found := duePlan(stream.header.DuePlans, plan)
		if !found {
			return nil, errors.New("alarmd worker: evaluated plan is not due")
		}
		part := &windowPart{due: due}
		parts[plan] = part
		return part, nil
	}
	for index := range stream.evaluated.Plans {
		plan := &stream.evaluated.Plans[index]
		from := windows.applied[plan.Plan]
		if from >= len(plan.StateResults) {
			continue
		}
		part, err := partFor(plan.Plan)
		if err != nil {
			return nil, err
		}
		part.results = plan.StateResults[from:]
	}
	for _, view := range views {
		if _, err := partFor(view.Identity.Plan); err != nil {
			return nil, err
		}
	}
	ordered := make([]windowPart, 0, len(parts))
	for _, part := range parts {
		ordered = append(ordered, *part)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return lessPlanIdentity(ordered[left].due.Identity, ordered[right].due.Identity)
	})
	return ordered, nil
}

// applyWindow writes the window being filled, as its own Sequence of the
// window's State keys: Plan by Plan, admission, the events written and
// acknowledged, then the State. Frozen series are renewed from this window's
// own views and results (P1): a key this window read and does not write.
// Reading them off views kept from earlier windows against this window's
// results would renew a key an earlier window wrote, and the Slot would
// write it twice.
func (coordinator *SlotExecutionCoordinator) applyWindow(ctx context.Context, stream *streamedExecution, final bool) error {
	windows := &stream.windows
	apply := windows.apply
	views := stream.state.Items[windows.views:]
	items := stream.stateItems[windows.views:]
	parts, err := stream.windowParts(views)
	if err != nil {
		return err
	}
	if len(parts) > 0 {
		windowState := execution.StatePreflightResult{Items: views}
		stateIndex := indexStatePreflight(windowState)
		windowCtx := withPlanLanding(withNoDataAbsenceStarts(ctx, stream.noDataAbsenceStarts), &windows.landing)
		err = coordinator.ports.Sequencer.Sequence(windowCtx, sequencingScope(stream.header, items, nil), func(sequenceCtx context.Context) error {
			for _, part := range parts {
				plan := part.due.Identity
				if apply.changed(part.due) {
					continue
				}
				if _, blocked := windows.blocked[plan]; blocked {
					continue
				}
				if _, replaced := windows.replaced[plan]; replaced {
					continue
				}
				output, err := apply.applyPlanState(sequenceCtx, part.due, part.results, windowState, stateIndex)
				if err != nil {
					return err
				}
				if output == planOutputWithheld {
					if windows.blocked == nil {
						windows.blocked = make(map[execution.PlanIdentity]struct{})
					}
					windows.blocked[plan] = struct{}{}
				}
			}
			return nil
		})
	}
	for index := range stream.evaluated.Plans {
		plan := &stream.evaluated.Plans[index]
		if windows.applied == nil {
			windows.applied = make(map[execution.PlanIdentity]int)
		}
		windows.applied[plan.Plan] = len(plan.StateResults)
	}
	windows.views, windows.bytes = len(stream.state.Items), 0
	// The window's State and event counts go back to the pool once it is
	// applied: the per-Slot caps bound one window, not the Slot. The gap
	// count stays, because the gap statements are the head's and the tail's.
	stream.coordinator.releaseEffects(effectCounts{states: stream.effects.states, events: stream.effects.events})
	stream.effectsSpent.states += stream.effects.states
	stream.effectsSpent.events += stream.effects.events
	stream.effects.states, stream.effects.events = 0, 0
	windows.count++
	stream.decideLanding(ctx, final)
	return err
}

// finishWindowedSlot applies the last window and runs the tail.
func (coordinator *SlotExecutionCoordinator) finishWindowedSlot(ctx context.Context, stream *streamedExecution) (execution.SlotExecutionResult, error) {
	if err := coordinator.applyWindow(ctx, stream, true); err != nil {
		return execution.SlotExecutionResult{}, err
	}
	ctx = withUnseenPlans(withNoDataEnds(withNoDataAbsenceStarts(ctx, stream.noDataAbsenceStarts), stream.noDataEndRound()), stream.unseenPlans())
	var result execution.SlotExecutionResult
	err := coordinator.ports.Sequencer.Sequence(ctx, sequencingScope(stream.header, nil, stream.gapItems), func(sequenceCtx context.Context) error {
		var tailErr error
		result, tailErr = stream.windows.apply.windowedTail(sequenceCtx, stream)
		return tailErr
	})
	return result, err
}

// windowedTail writes what follows the last window: each Plan's clear, or
// the open statement of a Plan the head opened none for and the budget
// answered UNAVAILABLE, then everything the one-window path writes after its
// Plans.
func (apply *slotApply) windowedTail(ctx context.Context, stream *streamedExecution) (execution.SlotExecutionResult, error) {
	windows := &stream.windows
	for _, planResult := range sortedPlanResults(stream.evaluated) {
		due, ok := duePlan(apply.header.DuePlans, planResult.Plan)
		if !ok {
			return execution.SlotExecutionResult{}, errors.New("alarmd worker: evaluated plan is not due")
		}
		if apply.changed(due) {
			continue
		}
		if _, blocked := windows.blocked[planResult.Plan]; blocked {
			continue
		}
		if _, opened := windows.headOpened[planResult.Plan]; !opened {
			if err := apply.tailStatements(ctx, due, planResult); err != nil {
				return execution.SlotExecutionResult{}, err
			}
		}
		apply.noteRetryPending(planResult)
	}
	census := stream.seriesCensus
	census.Written += apply.written
	return apply.complete(ctx, stream.bindings, &stream.summary, stream.noDataMutations,
		stream.evaluated.Result, stream.evaluated.ReasonCode, stream.queryEvidence, census, stream.targetSummaries())
}

// tailStatements writes a Plan's statement in the tail: its clear, as merged
// across the windows by agreedGapClearStatement - a window that proposed no
// clear does not veto one, two different ones are refused by name - or, for
// a Plan the budget answered UNAVAILABLE, the open statement that came with
// it. Nothing of such a Plan was written past what its earlier windows
// carried, so an open written here precedes no event it guards that the
// head could have preceded.
func (apply *slotApply) tailStatements(ctx context.Context, due execution.DuePlan, planResult execution.PlanEvaluationResult) error {
	before, after := planResult.GuardBeforeEvents, planResult.GuardAfterState
	if len(before) != 0 {
		after = nil
	}
	if len(before)+len(after) == 0 {
		return nil
	}
	gapRetention, err := generationRetentionOf(due)
	if err != nil {
		return err
	}
	before = uncommittedGapMutations(apply.loadedGaps, before)
	after = uncommittedGapMutations(apply.loadedGaps, after)
	apply.coordinator.observeDuplicatedGapStatements(ctx, apply.request,
		execution.PlanEvaluationResult{Plan: planResult.Plan, GuardBeforeEvents: before, GuardAfterState: after})
	if len(before)+len(after) == 0 {
		return nil
	}
	if err := apply.coordinator.admit(ctx, apply.request, due); err != nil {
		return err
	}
	if err := apply.coordinator.applyGap(ctx, apply.request.Operation, apply.request.Contract, before, gapRetention, GapSiteBeforeEvents); err != nil {
		return err
	}
	return apply.planGapAfter(ctx, after, gapRetention)
}

// completeWindowsBeyondSlotBudget answers a Slot over a per-Slot cap once
// windows of it are written. Those windows stay written: their events are
// out and their State landed, and voiding them would only leave the store
// saying one thing and the completion another. What it does:
//
//   - A Plan whose every series the evaluation got past keeps its results;
//     the ones no window took yet go out in the last window.
//   - Every other Plan - the one the refused series belongs to, and every
//     Plan the evaluation had not reached - is answered as a Slot over its
//     budget answers every Plan: UNAVAILABLE, with a Plan-wide budget gap,
//     written in the tail unless the head already opened its marker. None of
//     its results past what earlier windows took is written. Its windows
//     already written stay, and its marker records that the rest of it was
//     not decided.
//   - The no-data memory is applied for no Plan, as on the one-window path:
//     a no-data Plan whose synthetic series were cut short would otherwise
//     remember an absence it never reported.
//
// The released counts are the window's, so the replacement gaps are counted
// against an empty window; the retained bytes stay until the stream releases
// everything.
func (stream *streamedExecution) completeWindowsBeyondSlotBudget(ctx context.Context) error {
	stream.coordinator.releaseEffects(stream.effects)
	stream.effectsSpent.states += stream.effects.states
	stream.effectsSpent.events += stream.effects.events
	stream.effectsSpent.gaps += stream.effects.gaps
	stream.effects = effectCounts{}
	stream.closeNoDataBeyondSlotBudget(ctx)
	windows := &stream.windows
	reason := execution.ReasonCode(contract.ReasonExecutionBudgetExhausted)
	for _, due := range stream.header.DuePlans {
		if _, passed := windows.passed[due.Identity]; passed {
			continue
		}
		if windows.replaced == nil {
			windows.replaced = make(map[execution.PlanIdentity]struct{})
		}
		windows.replaced[due.Identity] = struct{}{}
		mutation, err := stream.gapMutationForReasons(due, map[execution.GapScope]execution.ReasonCode{{}: reason})
		if err != nil {
			return err
		}
		result := execution.EvaluationResult{Contract: stream.header.Contract, Result: observability.ResultDegraded,
			ReasonCode: reason, Plans: []execution.PlanEvaluationResult{{Plan: due.Identity,
				Disposition: execution.PlanUnavailable, ReasonCode: reason,
				GuardBeforeEvents: []execution.PlanGapMutation{mutation}}}}
		if err := stream.mergeProvisional(ctx, result, 0); err != nil {
			return err
		}
	}
	return nil
}

// decideLanding records, for every Plan whose windows are all applied, whether
// this attempt got it whole into the store (appliedPlanRecorder). Its keys
// are counted across every window, and the series this attempt resumed count
// as landed - they are in the store at this Slot - so a retry that resumes
// the windows an earlier attempt wrote and writes the rest records the Plan
// as applied. A Plan is decided once its last window is applied: before then
// the keys of its later windows have not been sent, and counting what landed
// so far would vouch for them. A no-data Plan's synthetic series come after
// every Plan's own, so it is decided with the last window.
func (stream *streamedExecution) decideLanding(ctx context.Context, final bool) {
	windows := &stream.windows
	recorder := appliedPlansFrom(ctx)
	for _, due := range stream.header.DuePlans {
		plan := due.Identity
		if _, decided := windows.decided[plan]; decided {
			continue
		}
		_, passed := windows.passed[plan]
		noData := due.CompiledPlan != nil && due.CompiledPlan.NoData() != nil
		if !final && (!passed || noData) {
			continue
		}
		if windows.decided == nil {
			windows.decided = make(map[execution.PlanIdentity]struct{})
		}
		windows.decided[plan] = struct{}{}
		resumed := windows.resumed[plan]
		landed, expected := windows.landing.landed[plan]+resumed, windows.landing.expected[plan]+resumed
		if expected == 0 {
			continue
		}
		_, blocked := windows.blocked[plan]
		_, replaced := windows.replaced[plan]
		if blocked || replaced || landed != expected {
			recorder.recordIncomplete(plan)
			continue
		}
		recorder.recordApplied(plan)
	}
}

// planLanding is a windowed Slot's count, by Plan, of the keys its applies
// sent and of those the store wrote, kept across windows: applyState decides
// a Plan per call, which for a Plan written in several windows would decide
// it off its first one.
type planLanding struct {
	landed, expected map[execution.PlanIdentity]int
}

func (landing *planLanding) add(plan execution.PlanIdentity, landed, expected int) {
	if landing.landed == nil {
		landing.landed = make(map[execution.PlanIdentity]int)
		landing.expected = make(map[execution.PlanIdentity]int)
	}
	landing.landed[plan] += landed
	landing.expected[plan] += expected
}

type planLandingKey struct{}

func withPlanLanding(ctx context.Context, landing *planLanding) context.Context {
	return context.WithValue(ctx, planLandingKey{}, landing)
}

func planLandingFrom(ctx context.Context) *planLanding {
	landing, _ := ctx.Value(planLandingKey{}).(*planLanding)
	return landing
}

// StateKeyRepeatedInSlotError is a Slot asking the store to write or renew
// one State key a second time.
//
// A Slot writes each of its State keys at most once: the ApplyVersion carries
// the epoch, the evaluation time and the Slot digest and nothing finer, so a
// second write of one key in one Slot is refused by the store as another
// statement at the same version, or as a revision conflict. Windows are cut
// between series and every series is one key, so this can only be a window
// cut through a series or a view kept into the next window; it is named here,
// before anything of the repeat is sent, rather than left to read as the
// store's conflict.
type StateKeyRepeatedInSlotError struct {
	Identity execution.StateKeyIdentity
	// First and Again are the requests the key was in: apply or renewal.
	First, Again string
}

func (err *StateKeyRepeatedInSlotError) Error() string {
	return fmt.Sprintf("alarmd worker: State key %s/%s/%s asked for in a %s after a %s of the same Slot",
		err.Identity.Plan.StrategyID, err.Identity.StateGeneration, err.Identity.SeriesIdentityDigest, err.Again, err.First)
}

// slotStateKeys is every State key a windowed Slot has sent to an apply or a
// renewal, and which. One digest-sized entry per series, held only for the
// Slot; the commit that lets windows go accounts for it with the rest of
// what a Slot holds outside its window.
type slotStateKeys map[execution.StateKeyIdentity]string

const (
	slotKeyApply   = "apply"
	slotKeyRenewal = "renewal"
)

// claim takes one Plan's part of a window, its frozen series and its
// mutations, refusing any key the Slot asked for before or that the part
// names twice.
func (keys slotStateKeys) claim(frozen []execution.FrozenSeriesState, mutations []execution.StateMutation) error {
	take := func(identity execution.StateKeyIdentity, site string) error {
		if first, taken := keys[identity]; taken {
			return &StateKeyRepeatedInSlotError{Identity: identity, First: first, Again: site}
		}
		keys[identity] = site
		return nil
	}
	for _, series := range frozen {
		if err := take(series.Identity, slotKeyRenewal); err != nil {
			return err
		}
	}
	for _, mutation := range mutations {
		if err := take(mutation.Identity, slotKeyApply); err != nil {
			return err
		}
	}
	return nil
}

func sortedDuePlans(plans []execution.DuePlan) []execution.DuePlan {
	sorted := append([]execution.DuePlan(nil), plans...)
	sort.Slice(sorted, func(left, right int) bool { return lessPlanIdentity(sorted[left].Identity, sorted[right].Identity) })
	return sorted
}
