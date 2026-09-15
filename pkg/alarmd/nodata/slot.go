// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// SlotInput is one Slot's no-data evidence for one Plan, as the worker holds it
// before any of it has been turned into a decision.
//
// The series are their dimensions and nothing else. Absence is decided from
// which groups reported, never from what they reported: a group that sent a
// value this item would call anomalous is present, and a group that sent
// nothing is absent, and no value anywhere changes either.
type SlotInput struct {
	Plan           *contract.EvaluationPlanV2
	EvaluationTime int64
	PeriodSeconds  int64
	Completeness   execution.Completeness
	// Series is one entry per series the Slot saw, holding that series'
	// dimensions with values already as text.
	Series []map[string]string
	// KnownHosts is the set of "address|cloud" keys the CMDB index confirmed
	// for this business, for a target roster to be intersected with.
	KnownHosts map[string]struct{}
	// OutOfBusiness names the groups whose host resolved to another business.
	OutOfBusiness map[string]struct{}
	Memory        map[string]GroupMemory
	// RosterVersion names the derivation the roster came from, carried into the
	// facts so a reader can tell one round's expected set from another's.
	RosterVersion string
}

// SlotOutcome says what happened to one no-data Plan in one Slot. Every Plan
// that detects no-data and did not error lands on exactly one of the three
// named outcomes every Slot, and that is the point: a round in which absence
// was not judged has to say so by name.
//
// It cannot be folded into the verdicts. A round skipped for budget looks
// exactly like a round whose query was not complete if both arrive as
// UNAVAILABLE - and they are not the same thing at all. A history roster only
// grows, so a Plan whose synthetic series do not fit the Slot's remaining
// mutation budget does not fit next round either: that is a permanent stop
// wearing the shape of a transient one, and the only thing that tells them
// apart is a name.
type SlotOutcome string

const (
	// OutcomeNone is the zero outcome: no no-data round happened for this Plan.
	// Either the Plan does not detect no-data at all, or the seam refused it and
	// returned an error. It is not one of the three buckets - a caller reading
	// it as a bucket would be counting refusals as rounds - and a caller must
	// check the error before reading the outcome.
	//
	// The refusing case is meant to be unreachable for a compiled Plan: the
	// catalog withholds a Plan whose roster ClassifyRoster refuses, and the seam
	// calls that same function rather than deriving the class a second time, so
	// the two locks cannot disagree. The seam still refuses rather than guess,
	// because a lock that trusts the other lock is one lock.
	OutcomeNone SlotOutcome = ""
	// OutcomeEvaluated means absence was judged this Slot.
	OutcomeEvaluated SlotOutcome = "EVALUATED"
	// OutcomeSkippedQueryNotFull means the Slot did not see the whole period,
	// so absence is not evidence and nothing was judged or remembered.
	OutcomeSkippedQueryNotFull SlotOutcome = "SKIPPED_QUERY_NOT_FULL"
	// OutcomeSkippedSlotBudget means the Slot could not carry this Plan's
	// no-data work. The evaluation never produces it - the budget is the
	// worker's fact, not this package's - and it is named here so that both
	// skips are read from one list.
	OutcomeSkippedSlotBudget SlotOutcome = "SKIPPED_SLOT_BUDGET"
)

// SlotOutcomes is every outcome a Plan that detects no-data can land on, for a
// partition to pre-create and for a reader to bound the family by.
var SlotOutcomes = []SlotOutcome{OutcomeEvaluated, OutcomeSkippedQueryNotFull, OutcomeSkippedSlotBudget}

// EvaluateSlot turns one Slot's evidence into the no-data decision for it.
//
// It is the seam the worker calls: everything above it is state and wiring,
// everything below it is the projection, the roster and the absence rules. It
// reads no clock, no store and no CMDB index, so a Slot that is retried reaches
// the same decision from the same evidence - which is the property the whole
// evaluation is built on, and the one that stops being true the moment any of
// this is decided inside the worker instead.
//
// A Plan that does not detect no-data returns the zero result and no error. The
// caller does not have to ask twice, and a Plan that gains the section later
// starts being evaluated without the caller changing.
//
// The outcome says whether absence was judged. A Slot that did not see the
// whole period still produces a result - every expected group is UNAVAILABLE
// and the memory comes back untouched - and the outcome is what distinguishes
// that from a round that judged and found nothing absent.
func EvaluateSlot(input SlotInput) (AbsenceResult, SlotOutcome, error) {
	if input.Plan == nil || input.Plan.NoData == nil {
		return AbsenceResult{}, OutcomeNone, nil
	}
	config := input.Plan.NoData
	tally := ProjectSeries(input.Series, config.AggDimension)
	roster, err := BuildRoster(RosterRequest{
		AggDimension: config.AggDimension,
		Scope:        input.Plan.TargetScope,
		KnownHosts:   input.KnownHosts,
		Memory:       input.Memory,
	})
	if err != nil {
		return AbsenceResult{}, OutcomeNone, err
	}
	roster.Version = input.RosterVersion
	outcome := OutcomeEvaluated
	if input.Completeness != execution.CompletenessFull {
		outcome = OutcomeSkippedQueryNotFull
	}
	return Evaluate(AbsenceInput{
		EvaluationTime: input.EvaluationTime,
		PeriodSeconds:  input.PeriodSeconds,
		Completeness:   input.Completeness,
		Present:        tally.Groups,
		Dropped:        tally.Dropped,
		Roster:         roster,
		Memory:         input.Memory,
		OutOfBusiness:  input.OutOfBusiness,
	}), outcome, nil
}
