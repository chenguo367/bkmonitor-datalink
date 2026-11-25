package worker

import (
	"context"
	"errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

type effectCounts struct{ states, events, gaps uint64 }

// minus is the State and event counts of counts that are not other's: a
// window's own, the windows in flight taken off what the Slot holds. The gap
// count is the head's and the tail's, never a window's, and is not in it.
func (counts effectCounts) minus(other effectCounts) effectCounts {
	return effectCounts{states: counts.states - min(other.states, counts.states), events: counts.events - min(other.events, counts.events)}
}

// plus is counts and other together.
func (counts effectCounts) plus(other effectCounts) effectCounts {
	return effectCounts{states: counts.states + other.states, events: counts.events + other.events, gaps: counts.gaps + other.gaps}
}

// admitEffects judges what the Slot holds in hand with delta added against
// its per-Slot caps and, when they hold it, adds delta, in one step: two
// merges into one Slot are never both judged on counts holding neither. It
// returns the counts judged, delta included either way.
func (stream *streamedExecution) admitEffects(delta effectCounts, budget ProvisionalBudget) (effectCounts, error) {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	count := stream.effects.plus(delta)
	if err := checkEffectCounts(count, budget); err != nil {
		return count, err
	}
	stream.effects = count
	return count, nil
}

// addEffects adds what the pool took for the Slot beyond the caps' judgement.
func (stream *streamedExecution) addEffects(delta effectCounts) {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	stream.effects = stream.effects.plus(delta)
}

// giveBackEffects takes off what a window that landed, or a reservation the
// pool refused, held of the Slot's counts.
func (stream *streamedExecution) giveBackEffects(held effectCounts) {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	stream.effects = effectCounts{states: stream.effects.states - min(held.states, stream.effects.states),
		events: stream.effects.events - min(held.events, stream.effects.events), gaps: stream.effects.gaps - min(held.gaps, stream.effects.gaps)}
}

// heldEffects is what the Slot holds of its counts now.
func (stream *streamedExecution) heldEffects() effectCounts {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	return stream.effects
}

// takeEffects is everything the Slot holds of its counts, now none.
func (stream *streamedExecution) takeEffects() effectCounts {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	held := stream.effects
	stream.effects = effectCounts{}
	return held
}

func checkEffectCounts(count effectCounts, budget ProvisionalBudget) error {
	for _, check := range []struct {
		count, limit uint64
		kind         observability.CapacityBudget
	}{
		{count.states, budget.MaxStateMutations, observability.CapacityBudgetStateMutations},
		{count.events, budget.MaxEvents, observability.CapacityBudgetEvents},
		{count.gaps, budget.MaxGapMutations, observability.CapacityBudgetGapMutations},
	} {
		if check.count > check.limit {
			return &provisionalBudgetExceededError{budget: check.kind}
		}
	}
	return nil
}

func countEffects(result execution.EvaluationResult) (count effectCounts) {
	for _, plan := range result.Plans {
		count.states += uint64(len(plan.StateResults))
		count.gaps += uint64(len(plan.GuardBeforeEvents) + len(plan.GuardAfterState))
		for _, state := range plan.StateResults {
			count.events += uint64(len(state.Events))
		}
	}
	return count
}

func newGapCount(current, next []execution.PlanGapMutation) uint64 {
	var count uint64
	for index, candidate := range next {
		if !containsGap(current, candidate) && !containsGap(next[:index], candidate) {
			count++
		}
	}
	return count
}

func containsGap(current []execution.PlanGapMutation, candidate execution.PlanGapMutation) bool {
	for _, existing := range current {
		if existing.Identity == candidate.Identity && existing.MutationDigest == candidate.MutationDigest {
			return true
		}
	}
	return false
}

// Counts only additions; existing State/Event collections are never revisited.
func addedEffectCounts(current, next execution.EvaluationResult) effectCounts {
	var count effectCounts
	for _, plan := range next.Plans {
		count.states += uint64(len(plan.StateResults))
		for _, state := range plan.StateResults {
			count.events += uint64(len(state.Events))
		}
		var previous *execution.PlanEvaluationResult
		for index := range current.Plans {
			if current.Plans[index].Plan == plan.Plan {
				previous = &current.Plans[index]
				break
			}
		}
		if previous == nil {
			count.gaps += uint64(len(plan.GuardBeforeEvents) + len(plan.GuardAfterState))
		} else {
			count.gaps += newGapCount(previous.GuardBeforeEvents, plan.GuardBeforeEvents) + newGapCount(previous.GuardAfterState, plan.GuardAfterState)
		}
	}
	return count
}

// mergeBytes is what merging one result reserves, by what it is for: the
// loaded State the caller measured (retained) and the per-key structures
// held beside it for the Slot (held), both under the state phase; the
// result's own effects and what the summary keeps of it (summarized), both
// under the output phase. retained and effects are the window's and go back
// with it; held and summarized stay to the Slot's end.
type mergeBytes struct {
	retained, held, effects, summarized uint64
}

func (bytes mergeBytes) total() uint64 {
	return bytes.retained + bytes.held + bytes.effects + bytes.summarized
}

// measureMerge is what merging next into the Slot's results would reserve.
// It reads the results and the summary and changes neither, so the window
// check before a merge and the merge reserve the same number.
func (stream *streamedExecution) measureMerge(next execution.EvaluationResult, retained, held uint64) mergeBytes {
	return mergeBytes{retained: retained, held: held, effects: newEffectBytes(stream.evaluated, next),
		summarized: stream.summary.addedBytes(next, stream.noDataEnds)}
}

func (stream *streamedExecution) mergeProvisional(ctx context.Context, next execution.EvaluationResult, retained uint64) error {
	return stream.mergeMeasured(ctx, next, stream.measureMerge(next, retained, 0))
}

func (stream *streamedExecution) mergeMeasured(ctx context.Context, next execution.EvaluationResult, measured mergeBytes) error {
	if stream.evaluated.Contract != (execution.FrozenExecutionContractRef{}) && stream.evaluated.Contract != next.Contract {
		return errors.New("alarmd worker: series evaluations changed frozen contract")
	}
	if err := stream.takeLandings(ctx); err != nil {
		return err
	}
	delta := addedEffectCounts(stream.evaluated, next)
	// The Slot's own output is checked against the per-Slot caps before the
	// shared reservation: a Slot above them can never be applied by this
	// process, so that rejection is deterministic and carries the Slot's own
	// facts instead of the shared usage. Admitted, the counts are the
	// Slot's at once (admitEffects), and given back if the pool refuses.
	slotBudget := stream.coordinator.slotBudget()
	count, err := stream.admitEffects(delta, slotBudget)
	if err != nil && stream.inflightEffects() != (effectCounts{}) {
		// The counts are the windows in hand: one written, one filled. With
		// a window still being written, the cap is judged again once it has
		// landed and given its counts back.
		if drainErr := stream.drainApply(ctx); drainErr != nil {
			return drainErr
		}
		count, err = stream.admitEffects(delta, slotBudget)
	}
	if err != nil {
		var exceeded *provisionalBudgetExceededError
		if errors.As(err, &exceeded) {
			err = stream.slotBudgetRejection(exceeded.budget, count, delta, slotBudget)
			stream.coordinator.observeCapacityRejection(ctx, stream.request.Operation, exceeded.budget, err)
		}
		return err
	}
	// Split where it was summed: the loaded State the caller measured under
	// its own phase, the effects this call adds under the output's. One
	// reservation, the same total, two answers instead of one.
	charges := retainedCharges{retainPhaseState: measured.retained + measured.held,
		retainPhaseOutput: measured.effects + measured.summarized}
	if err := stream.coordinator.acquireEffects(delta, stream, stream.reservationPhase("normal_output"), charges); err != nil {
		stream.giveBackEffects(delta)
		var exceeded *provisionalBudgetExceededError
		if errors.As(err, &exceeded) {
			stream.coordinator.observeCapacityRejection(ctx, stream.request.Operation, exceeded.budget, err)
		}
		return err
	}
	// Counted as reserved: what the pool holds of it the stream gives back,
	// whether or not the append below takes the batch.
	stream.retainedCharged(retainPhaseState, charges[retainPhaseState])
	stream.retainedCharged(retainPhaseOutput, charges[retainPhaseOutput])
	// The contract check and shared reservation cover everything this append
	// can refuse except one: two batches disagreeing about a Plan's gap
	// marker, which is a shape neither batch's own result contains.
	// Loaded Gap facts remain independently accounted in stream.gapFacts.
	// At its place among the Slot's batches: the slice its series were read
	// in, then its turn (execution.MergeOrder).
	order := stream.summary.nextOrder(stream.sliceNumber)
	if err := appendProvisionalAt(&stream.evaluated, next, order, &stream.summary.results); err != nil {
		return err
	}
	// After the append and only when it took the batch, so the summary
	// never holds a batch the results do not: every result the Slot keeps -
	// evaluated, resumed, superseded, no-data, a Plan without series, a
	// budget replacement - arrives here.
	stream.summary.addAt(next, stream.noDataEnds, order)
	stream.keysHeld += measured.held
	stream.summaryHeld += measured.summarized
	// The window's own share: what applying it gives back (applyWindow).
	stream.windows.stateBytes += measured.retained
	stream.windows.bytes += measured.retained + measured.effects
	return nil
}

func newEffectBytes(current, next execution.EvaluationResult) uint64 {
	var retained uint64
	for _, plan := range next.Plans {
		var previous *execution.PlanEvaluationResult
		for index := range current.Plans {
			if current.Plans[index].Plan == plan.Plan {
				previous = &current.Plans[index]
				break
			}
		}
		if previous == nil {
			// Split the same way the incremental branch below is, so the first
			// series of a Slot is charged for its history on the same terms as
			// every series after it.
			withoutState := plan
			withoutState.StateResults = nil
			retained += 2 * (retainedObjectBytes(withoutState) + retainedStateResultBytes(plan.StateResults))
			continue
		}
		if len(plan.LevelOutcomes) != 0 {
			retained += 2 * retainedObjectBytes(plan.LevelOutcomes)
		}
		if len(plan.StateResults) != 0 {
			retained += 2 * retainedStateResultBytes(plan.StateResults)
		}
		for _, pair := range []struct{ previous, next []execution.PlanGapMutation }{{previous.GuardBeforeEvents, plan.GuardBeforeEvents}, {previous.GuardAfterState, plan.GuardAfterState}} {
			for index, candidate := range pair.next {
				if !containsGap(pair.previous, candidate) && !containsGap(pair.next[:index], candidate) {
					retained += 2 * retainedObjectBytes(candidate)
				}
			}
		}
	}
	return retained
}

// retainedCharges is what one reservation charges each phase of a stream.
type retainedCharges [retainPhaseCount]uint64

// total is the bytes the reservation holds of the pool.
func (charges retainedCharges) total() uint64 {
	var total uint64
	for _, bytes := range charges {
		total += bytes
	}
	return total
}

// acquireEffects reserves the side effects of delta and the bytes charges
// holds for stream, the bytes judged against its share and the pool and
// charged to its phases in the same step (chargeRetained).
func (coordinator *SlotExecutionCoordinator) acquireEffects(delta effectCounts, stream *streamedExecution, phase string,
	charges retainedCharges) error {
	retained := charges.total()
	reservation := &coordinator.reservations
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	remaining := ProvisionalBudget{MaxStateMutations: coordinator.budget.MaxStateMutations - reservation.states,
		MaxEvents: coordinator.budget.MaxEvents - reservation.events, MaxGapMutations: coordinator.budget.MaxGapMutations - reservation.gaps}
	if err := checkEffectCounts(delta, remaining); err != nil {
		var exceeded *provisionalBudgetExceededError
		if errors.As(err, &exceeded) {
			var used, requested, limit uint64
			switch exceeded.budget {
			case observability.CapacityBudgetStateMutations:
				used, requested, limit = reservation.states, delta.states, coordinator.budget.MaxStateMutations
			case observability.CapacityBudgetEvents:
				used, requested, limit = reservation.events, delta.events, coordinator.budget.MaxEvents
			case observability.CapacityBudgetGapMutations:
				used, requested, limit = reservation.gaps, delta.gaps, coordinator.budget.MaxGapMutations
			}
			return budgetRejection(exceeded.budget, phase, used, requested, limit, stream.ownBudget(exceeded.budget), stream.ownBudgetUsage(coordinator.budget))
		}
		return err
	}
	// The share is checked before the pool, and against this execution's own
	// total rather than against what is left.
	//
	// Order matters: with the pool nearly full, an object over its share is
	// refused by the pool first and reported as somebody else's doing, which is
	// the attribution this whole distinction exists to fix. Own total rather
	// than remaining, because a share measured against what happens to be free
	// means a different thing every round - the same object would fit or not by
	// the luck of who else is running, and nobody could act on the answer.
	//
	// A refusal the stream's read ahead may stand behind is judged again once
	// it has yielded (yieldReadAhead), outside the lock its giving back takes.
	for {
		err := coordinator.judgeRetainedLocked(stream, retained, phase)
		if err == nil {
			break
		}
		reservation.mu.Unlock()
		yielded := stream.yieldReadAhead(phase)
		reservation.mu.Lock()
		if !yielded {
			return err
		}
	}
	reservation.states += delta.states
	reservation.events += delta.events
	reservation.gaps += delta.gaps
	reservation.retainedBytes += retained
	if stream != nil {
		for charge, bytes := range charges {
			stream.chargeRetained(retainPhase(charge), bytes)
		}
	}
	return nil
}

func (coordinator *SlotExecutionCoordinator) releaseEffects(count effectCounts) {
	reservation := &coordinator.reservations
	reservation.mu.Lock()
	reservation.states -= count.states
	reservation.events -= count.events
	reservation.gaps -= count.gaps
	reservation.mu.Unlock()
}
