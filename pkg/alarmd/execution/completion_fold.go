// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "slices"

// MergeOrder is where one batch of a Slot's results stands among the Slot's
// batches: the number of the slice its series were read in, then its place
// among the batches merged. A Slot read one slice at a time merges its
// batches in this order; one whose slices are consumed side by side merges
// them as they come, and every fold that keeps the first of several alike -
// a Plan's place, a reason of equal rank, an UNKNOWN outcome - keeps the one
// first in this order, so its fold is the one a Slot read one slice at a time
// makes.
type MergeOrder struct {
	Slice int
	Seq   uint64
}

// Before says whether order stands before other.
func (order MergeOrder) Before(other MergeOrder) bool {
	if order.Slice != other.Slice {
		return order.Slice < other.Slice
	}
	return order.Seq < other.Seq
}

// CompletionAttributionFold is what the completion derivation reads of a
// Slot's evaluation, kept as the results arrive rather than as the results.
//
// The derivation used to walk every Level outcome of every series the Slot
// held, which is why the Slot had to hold them all until it completed. A
// Slot that applies its series window by window and lets each window go
// cannot do that: by the time it completes, most of its outcomes are gone.
// What the derivation actually reads of them is small, and this is it - per
// Plan, the disposition and reason the Slot's results fold to, whether any
// Level was decided TERMINAL, and the few UNKNOWN outcomes that can still be
// the one it names.
//
// A few is enough, and exactly so: the derivation replaces its finding only
// with one ranked strictly higher, so among outcomes it would rank alike the
// first is the only one that can ever be reported. How it ranks an UNKNOWN
// outcome depends on nothing but the outcome's Plan, whether it is a guard's
// tail, and the Slot's inputs (SlotInputWholeness.UnknownIsGuardTail). The
// inputs are the same for every outcome of the Slot, so keeping the first
// outcome of each (Plan, guard tail) pair, in the order they arrived, and
// replaying those in that order finds what replaying every outcome would.
// Whether the Plan's inputs were whole is asked only when the Slot completes,
// because that is when its inputs are all known.
type CompletionAttributionFold struct {
	plans []completionPlanFold
	// next is the place Add gives a batch it is handed without one.
	next uint64
}

// completionPlanFold is one Plan's part of the fold. The field names are the
// Plan result's own, so the derivation reads a Plan the way it read the
// result.
type completionPlanFold struct {
	Plan        PlanIdentity
	Disposition PlanDisposition
	ReasonCode  ReasonCode
	// terminal is a Level of the Plan decided TERMINAL.
	terminal bool
	// unknown is the first UNKNOWN outcome of each (outcome Plan, guard
	// tail) pair, in merge order (MergeOrder): two in practice, one per
	// guard tail value, since a Plan's outcomes name the Plan. unknownOrder
	// is each one's batch's order; first is the Plan's first batch's, and
	// disposed the batch's whose disposition it holds.
	unknown         []LevelOutcome
	unknownOrder    []MergeOrder
	first, disposed MergeOrder
}

// Add folds one batch of a Slot's results in. A Plan already in the fold
// takes the batch's disposition and reason only when the batch's outranks
// it, which is the rule the worker merges a Slot's results by, so the fold
// of a Slot's batches is the fold of the merged result.
func (fold *CompletionAttributionFold) Add(result EvaluationResult) {
	fold.next++
	fold.add(result, true, MergeOrder{Seq: fold.next})
}

// AddAt folds one batch in at its place among the Slot's batches: where the
// fold keeps the first of several alike, the first in that order, whatever
// order the batches come in.
func (fold *CompletionAttributionFold) AddAt(result EvaluationResult, order MergeOrder) {
	fold.add(result, true, order)
}

// FoldCompletionAttribution is the fold of one result taken whole, each Plan
// result in it as it stands. A result that names a Plan twice is folded as
// two Plans, which is how the derivation has always read such a result; a
// Slot's merged result never names one twice.
func FoldCompletionAttribution(result EvaluationResult) CompletionAttributionFold {
	var fold CompletionAttributionFold
	fold.add(result, false, MergeOrder{})
	return fold
}

func (fold *CompletionAttributionFold) add(result EvaluationResult, merge bool, order MergeOrder) {
	for _, next := range result.Plans {
		index := -1
		if merge {
			for current := range fold.plans {
				if fold.plans[current].Plan == next.Plan {
					index = current
					break
				}
			}
		}
		if index < 0 {
			// In order of the Plans' first batches: last, for a batch
			// merged after every one before it.
			index = len(fold.plans)
			for index > 0 && order.Before(fold.plans[index-1].first) {
				index--
			}
			fold.plans = slices.Insert(fold.plans, index, completionPlanFold{Plan: next.Plan, Disposition: next.Disposition,
				ReasonCode: next.ReasonCode, first: order, disposed: order})
		} else {
			if order.Before(fold.plans[index].first) {
				// Met first in this batch: to its place among the Plans' first.
				moved := fold.plans[index]
				moved.first = order
				fold.plans = slices.Delete(fold.plans, index, index+1)
				for index > 0 && order.Before(fold.plans[index-1].first) {
					index--
				}
				fold.plans = slices.Insert(fold.plans, index, moved)
			}
			if current := &fold.plans[index]; PlanDispositionOutranks(next.Disposition, current.Disposition) ||
				!PlanDispositionOutranks(current.Disposition, next.Disposition) && order.Before(current.disposed) {
				current.Disposition, current.ReasonCode, current.disposed = next.Disposition, next.ReasonCode, order
			}
		}
		plan := &fold.plans[index]
		for _, outcome := range next.LevelOutcomes {
			switch outcome.Outcome {
			case LevelOutcomeTerminal:
				plan.terminal = true
			case LevelOutcomeUnknown:
				plan.keepUnknown(outcome, order)
			}
		}
	}
}

// keepUnknown keeps outcome when it is the first of its (Plan, guard tail)
// pair in merge order, in its place among the outcomes kept.
func (plan *completionPlanFold) keepUnknown(outcome LevelOutcome, order MergeOrder) {
	for index, kept := range plan.unknown {
		if kept.Plan == outcome.Plan && kept.GuardTail == outcome.GuardTail {
			if !order.Before(plan.unknownOrder[index]) {
				return
			}
			plan.unknown = slices.Delete(plan.unknown, index, index+1)
			plan.unknownOrder = slices.Delete(plan.unknownOrder, index, index+1)
			break
		}
	}
	at := len(plan.unknown)
	for at > 0 && order.Before(plan.unknownOrder[at-1]) {
		at--
	}
	plan.unknown = slices.Insert(plan.unknown, at, outcome)
	plan.unknownOrder = slices.Insert(plan.unknownOrder, at, order)
}

// Plans is how many Plans the fold holds.
func (fold CompletionAttributionFold) Plans() int {
	return len(fold.plans)
}

// Dispositions is each Plan's folded disposition, in the order the Plans
// first arrived.
func (fold CompletionAttributionFold) Dispositions() []PlanDisposition {
	dispositions := make([]PlanDisposition, 0, len(fold.plans))
	for _, plan := range fold.plans {
		dispositions = append(dispositions, plan.Disposition)
	}
	return dispositions
}

// DeriveFoldedCompletionAttribution is DeriveCompletionAttribution over a
// fold of the Slot's results instead of the results.
func DeriveFoldedCompletionAttribution(input InternalExecution, fold CompletionAttributionFold) (CompletionKind, CompletionAttribution, error) {
	return deriveCompletionAttribution(input, fold)
}

// PlanDispositionOutranks says whether a Plan's result in one batch of a Slot
// replaces the disposition the Plan already has from another: the worse
// outcome wins, so a Plan that could not be decided in one series is not
// reported decided because another series of it was.
func PlanDispositionOutranks(next, current PlanDisposition) bool {
	return planDispositionRank(next) > planDispositionRank(current)
}

func planDispositionRank(disposition PlanDisposition) int {
	switch disposition {
	case PlanRetryPending:
		return 5
	case PlanTerminal:
		return 4
	case PlanUnavailable, PlanReadinessGap:
		return 3
	case PlanDecidedDegraded:
		return 2
	case PlanDecided:
		return 1
	default:
		return 0
	}
}
