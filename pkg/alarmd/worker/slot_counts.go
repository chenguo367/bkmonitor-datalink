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
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
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

// countNotCommitted counts an attempt that began its Slot, once, as it
// returns: nothing when its Progress commit went through, and otherwise the
// reason it returned on. mode is the finalization the attempt resolved, empty
// when it returned before resolving one; err is what the attempt returned.
func (coordinator *SlotExecutionCoordinator) countNotCommitted(applied *appliedPlanRecorder, mode execution.FinalizationMode, err error) {
	counter := coordinator.ports.SlotCounts
	if counter == nil || applied.wasCommitted() {
		return
	}
	reason := execution.SlotNotCommittedOther
	switch {
	case err != nil && isReadinessDeferred(err):
		reason = execution.SlotNotCommittedQueryNotReady
	case err == nil && mode == execution.FinalizationSnapshotRetry:
		reason = execution.SlotNotCommittedSnapshotRetry
	case err == nil && mode == execution.FinalizationExactSetBlocked:
		reason = execution.SlotNotCommittedExactSetBlocked
	}
	counter.RecordSlotNotCommitted(reason)
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
	defer coordinator.notEvaluatedLines.mu.Unlock()
	delete(coordinator.notEvaluatedLines.byGroup, queryGroup)
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
