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
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// SlotCounter is told how each Slot attempt's BeginSlot went, and why an
// attempt that began its Slot returned without writing it down: the two
// numbers slot_begin_total and slot_not_committed_total read, so that the
// attempts a Query Group spends on a Slot can be told from the Slots it
// completes. The words are execution's (SlotBeginResults,
// SlotNotCommittedReasons); the metric recorder is one.
type SlotCounter interface {
	RecordSlotBegin(result string)
	RecordSlotNotCommitted(reason string)
}

// countSlotBegin counts one BeginSlot by its status.
func (coordinator *SlotExecutionCoordinator) countSlotBegin(status execution.ProgressCommitStatus) {
	if counter := coordinator.ports.SlotCounts; counter != nil {
		counter.RecordSlotBegin(execution.SlotBeginResult(status))
	}
}

// countSlotBeginDeferred counts an attempt that put its BeginSlot off and
// made none (execution.SlotBeginDeferred).
func (coordinator *SlotExecutionCoordinator) countSlotBeginDeferred() {
	if counter := coordinator.ports.SlotCounts; counter != nil {
		counter.RecordSlotBegin(execution.SlotBeginDeferred)
	}
}

// countNotCommitted counts an attempt that began its Slot, once, as it
// returns: nothing when its Progress commit went through, and otherwise the
// reason it returned on (notCommittedReason). mode is the finalization the
// attempt resolved, empty when it returned before resolving one; result and
// err are what the attempt returned.
func (coordinator *SlotExecutionCoordinator) countNotCommitted(applied *appliedPlanRecorder, mode execution.FinalizationMode,
	result execution.SlotExecutionResult, err error) {
	counter := coordinator.ports.SlotCounts
	if counter == nil || applied.wasCommitted() {
		return
	}
	counter.RecordSlotNotCommitted(notCommittedReason(mode, result, err))
}

// notCommittedReason is the word of execution.SlotNotCommittedReasons an
// attempt that did not write its Slot down returned on: with an error, by
// what the error is and then by the step it failed in (SlotStepError); with
// none, by its finalization and its result.
func notCommittedReason(mode execution.FinalizationMode, result execution.SlotExecutionResult, err error) string {
	if err != nil {
		var exceeded *provisionalBudgetExceededError
		_, refused := ownership.RefusalReason(err)
		_, shared := execution.ShareRefusalOf(err)
		_, sliced := InputSliceRefusal(err)
		_, stateConflict := StateConflictReason(err)
		_, gapRefused := GapApplyReason(err)
		_, guardConflict := GapGuardConflictReason(err)
		_, guardDisagree := GapGuardDisagreeReason(err)
		// A cancellation or a deadline is read before the finalization: the
		// process stopping or the Query Group leaving while the frozen Plan
		// is read is a cancellation, not a finalization that did not
		// resolve - a rollout would otherwise read as a burst of those.
		switch {
		case isReadinessDeferred(err):
			return execution.SlotNotCommittedQueryNotReady
		case refused:
			return execution.SlotNotCommittedOwnerRefused
		case queryAdmissionFailed(err):
			return execution.SlotNotCommittedQueryPermitFailed
		case errors.Is(err, context.Canceled):
			return execution.SlotNotCommittedCancelled
		case errors.Is(err, context.DeadlineExceeded):
			return execution.SlotNotCommittedDeadlineExceeded
		case segmentsUnread(err):
			return execution.SlotNotCommittedSegmentReadFailed
		case mode == "":
			return execution.SlotNotCommittedFinalizationFailed
		case errors.Is(err, execution.ErrExecutionDeclined):
			return execution.SlotNotCommittedDeclined
		case PoolHeldByOthers(err) || shared || sliced || errors.As(err, &exceeded):
			return execution.SlotNotCommittedCapacityRefused
		case stateConflict || gapRefused || guardConflict || guardDisagree:
			return execution.SlotNotCommittedStoreConflict
		}
		if step := innermostSlotStep(err); step != "" {
			return step
		}
		return execution.SlotNotCommittedOther
	}
	switch {
	case mode == execution.FinalizationSnapshotRetry:
		return execution.SlotNotCommittedSnapshotRetry
	case result.Result == "":
		return execution.SlotNotCommittedNoResult
	}
	switch string(result.ReasonCode) {
	case contract.ReasonActivationReadFailed:
		return execution.SlotNotCommittedActivationReadFailed
	case contract.ReasonSegmentWriteFailed:
		return execution.SlotNotCommittedSegmentWriteFailed
	case contract.ReasonConfigDrift, contract.ReasonPlanReactivated, contract.ReasonPlanNotActive:
		return execution.SlotNotCommittedActivationChanged
	case contract.ReasonOutputACKUnknown, contract.ReasonOutputLeaseExpiring:
		return execution.SlotNotCommittedOutputRetryPending
	}
	switch result.Result {
	case observability.ResultRetrying:
		return execution.SlotNotCommittedRetryPending
	case observability.ResultSuccess:
		return execution.SlotNotCommittedOther
	}
	return execution.SlotNotCommittedIncomplete
}

// queryAdmissionFailed is whether the error is the query admission's, a
// physical query permit not had, at whatever depth it was wrapped: a permit
// wait that reached the query's deadline wraps the deadline, and is the
// permit's before it is a deadline.
func queryAdmissionFailed(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if failure, ok := err.(interface{ QueryFailure() (string, string) }); ok {
			if category, _ := failure.QueryFailure(); category == observability.QueryFailureCategoryAdmission {
				return true
			}
		}
	}
	return false
}

// SlotStepError is an attempt's error with the step of Execute it failed in,
// a word of execution.SlotNotCommittedReasons; its text is the error's own.
// The runtime reads the innermost step a Slot's error names (SlotStepOf).
type SlotStepError struct {
	Step string
	Err  error
}

func (e *SlotStepError) Error() string { return e.Err.Error() }
func (e *SlotStepError) Unwrap() error { return e.Err }

// inSlotStep is err failed in step; nil stays nil.
func inSlotStep(step string, err error) error {
	if err == nil {
		return nil
	}
	return &SlotStepError{Step: step, Err: err}
}

// SlotStepOf is the step of Execute an attempt's error failed in, the word
// slot_not_committed_total counts it under when nothing else names it
// (execution.SlotNotCommittedReasons); empty for an error no step named.
func SlotStepOf(err error) string {
	return innermostSlotStep(err)
}

// innermostSlotStep is the step the error failed in, the innermost one named:
// a Progress commit inside the frozen Slot's apply names the commit.
func innermostSlotStep(err error) string {
	step := ""
	for err != nil {
		if named, ok := err.(*SlotStepError); ok {
			step = named.Step
		}
		err = errors.Unwrap(err)
	}
	return step
}

// seriesNotEvaluatedLineEvery is how often a Query Group's
// series_not_evaluated line is written at most: the first at once, then one
// in this long, carrying the Slots and series held back since. The counter
// counts every one; the line is to name the Query Group and its Plans, and
// a loss on every round - a defect, or a count that misreads a whole class
// of series - would otherwise write a line a Slot for as long as it lasts.
// At most the Query Groups' count of lines every ten minutes.
const seriesNotEvaluatedLineEvery = 10 * time.Minute

// seriesNotEvaluatedLines is, by Query Group, when its last
// series_not_evaluated line was written and what has been held back since.
// A Query Group's goes with its release (ForgetQueryGroup).
type seriesNotEvaluatedLines struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]*seriesNotEvaluatedLine
}

type seriesNotEvaluatedLine struct {
	at            time.Time
	slots, series uint64
}

// take says whether the Query Group's Slot that lost series writes its line
// at now, and with it what was held back since the last; a Slot that does
// not is held back itself.
func (lines *seriesNotEvaluatedLines) take(queryGroup execution.QueryGroupIdentity, now time.Time, lost uint64) (slots, series uint64, write bool) {
	lines.mu.Lock()
	defer lines.mu.Unlock()
	line := lines.byGroup[queryGroup]
	if line != nil && now.Sub(line.at) < seriesNotEvaluatedLineEvery {
		line.slots++
		line.series += lost
		return 0, 0, false
	}
	if line != nil {
		slots, series = line.slots, line.series
	}
	if lines.byGroup == nil {
		lines.byGroup = make(map[execution.QueryGroupIdentity]*seriesNotEvaluatedLine)
	}
	lines.byGroup[queryGroup] = &seriesNotEvaluatedLine{at: now}
	return slots, series, true
}

// ForgetQueryGroup lets go of what the coordinator keeps of a Query Group
// this replica released: when its last series_not_evaluated line was
// written. Whoever owns it next writes its first at once. A function rather
// than a method, as SliceMemory is: Execute is the coordinator's one method.
func ForgetQueryGroup(coordinator *SlotExecutionCoordinator, queryGroup execution.QueryGroupIdentity) {
	coordinator.notEvaluatedLines.mu.Lock()
	delete(coordinator.notEvaluatedLines.byGroup, queryGroup)
	coordinator.notEvaluatedLines.mu.Unlock()
	coordinator.segmentWriteFailures.forget(queryGroup)
	coordinator.segmentDegradations.forget(queryGroup)
	coordinator.segmentFallbackLines.forget(queryGroup)
}

// reportSeriesNotEvaluated names, for a completed Slot that bound series it
// never took up (lost, by family), which Plans lost them: one WARN line,
// never sampled, at most one a Query Group in seriesNotEvaluatedLineEvery
// (observability.ObserveSeriesNotEvaluated). A Slot that took up every
// series writes nothing.
func (coordinator *SlotExecutionCoordinator) reportSeriesNotEvaluated(ctx context.Context, request execution.SlotExecutionRequest,
	stream *streamedExecution, lost [observability.SeriesNotEvaluatedFamilyCount]uint64) {
	if lost == ([observability.SeriesNotEvaluatedFamilyCount]uint64{}) {
		return
	}
	plans := stream.seriesNotEvaluatedPlans()
	missing := uint64(0)
	for _, plan := range plans {
		missing += plan.Missing
	}
	slots, series, write := coordinator.notEvaluatedLines.take(request.Contract.Slot.QueryGroup, coordinator.now(), missing)
	if !write {
		return
	}
	observability.ObserveSeriesNotEvaluated(ctx, coordinator.ports.Observer, observability.TraceFields{
		QueryGroupKey: string(request.Contract.Slot.QueryGroup), EvaluationTime: int64(request.Contract.Slot.EvaluationTime),
	}, observability.Operation(request.Operation), observability.SeriesNotEvaluatedFacts{Plans: plans,
		SuppressedSlots: slots, SuppressedSeries: series})
}
