// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
)

// What a Slot costs the Progress record, and what its attempts come to: the
// reads of the record a Query Group's owner makes, by whether the key or the
// owner's record answered them (B2-3), and how each attempt's BeginSlot went
// and why one that began did not write its Slot down (B7), and which fields
// of an unfinished Slot's projection a later attempt of it changed. Every
// cell of every family exists from the start, so a zero is a reading.
type slotProgressMetrics struct {
	reads, begins, notCommitted, superseded, conflicts, appliedWhole *prometheus.CounterVec
}

func newSlotProgressMetrics() slotProgressMetrics {
	metrics := slotProgressMetrics{
		reads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "progress_reads_total",
			Help: "Reads of a Query Group's Progress record by its owner, by caller - next (the slot source choosing " +
				"the round's Slot), begin, commit, skip (a pruned cursor's skip), range (an expired range's begin or " +
				"commit) - and by source: redis (the key was read) or cache (the record as the owner last read or " +
				"wrote it under the same fence answered). A Slot whose record nothing else wrote reads the key at " +
				"most once every ten rounds or five minutes; a redis read beside a begin or commit is a cold start, " +
				"a new fence, or a write that met a record changed behind the owner and read it again. The Leader's, " +
				"the CLI's and a diagnosis's reads are not counted here.",
		}, []string{"caller", "source"}),
		begins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "slot_begin_total",
			Help: "Slot attempts' BeginSlot calls that returned a status, by it: committed (the Slot is begun), " +
				"conflict (the record is not at this Slot), stale_owner and content_moved (the fence refused the " +
				"write), retryable_io (the store did not answer); deferred, an attempt that put its BeginSlot off " +
				"and sent its commit in its place - an empty round - while one that put it off and then wrote, or " +
				"left without writing its Slot down, is counted by the BeginSlot it made then, and an empty round " +
				"whose commit the record refused by that status, as a BeginSlot at its start; the results sum to " +
				"the attempts. A " +
				"BeginSlot that failed outright is counted by its slot_completed line only.",
		}, []string{"result"}),
		notCommitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "slot_not_committed_total",
			Help: "Slot attempts that began or deferred their Slot (slot_begin_total{result=\"committed\"} and " +
				"{result=\"deferred\"}) and returned without " +
				"writing it down, once each, by why: QUERY_NOT_READY, SNAPSHOT_RETRY; a result by its reason or kind " +
				"(activation_read_failed, activation_changed, output_retry_pending, segment_write_failed - a round " +
				"that left members' State unwritten whose stable segments' write did not land, retried whole - " +
				"retry_pending, incomplete, " +
				"no_result: a panic); an error by what it is (finalization_failed, owner_refused - its fence stale, its " +
				"content moved, or its Query Group no longer desired for it, which a write's own script reports as " +
				"a stale fence, so its reason reads OWNERSHIP_STALE_FENCE where a check before it read " +
				"OWNERSHIP_NOT_DESIRED - query_permit_failed, cancelled, " +
				"deadline_exceeded, declined, capacity_refused, store_conflict, segment_read_failed - a Slot with a " +
				"segment skip word whose segments did not read, its retry reading them as stored) or else the step it failed in " +
				"(query_, complete_, apply_, output_, progress_commit_, query_free_failed); other, none of these.",
		}, []string{"reason"}),
		superseded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "progress_projection_superseded_total",
			Help: "Fields of an unfinished Slot's projection that a later attempt's fenced BeginSlot replaced, " +
				"counted once the write applied, a field each: contract, deadline (the earliest query deadline), " +
				"keep_until, due_plan_targets. A supersede that changed two fields counts in both. A write refused " +
				"(stale_owner, content_moved) or of unknown outcome replaced nothing and counts none, though its " +
				"progress_committed line, degraded CONFIG_DRIFT, naming the fields, is written as every supersede's " +
				"is. A BeginSlot made again on a conflict counts once, from the pass its write ended on.",
		}, []string{"field"}),
		conflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "progress_conflicts_total",
			Help: "Progress writes the record refused as a conflict, once a call, by operation - begin (a Slot's " +
				"BeginSlot), commit, range (an expired range's begin or commit) - and by the fact that refused it: " +
				"RANGE_IN_FLIGHT (an expired range is being written, or another range than this one), CURSOR_MOVED " +
				"(the record's cursor is not at the Slot or range - a commit's progress_committed line says the Slot " +
				"its completed facts derive as next and the cursor it stores), PROGRESS_MISSING (no record for a Slot " +
				"or range that began), CAS_CONFLICT (the record changed under the write), IDENTITY_MISMATCH (the " +
				"record names another Query Group), SEGMENT_OLDER (the Slot asked through an older Segment than its " +
				"unfinished projection's), SLOT_IN_FLIGHT (an unfinished Slot where a range would begin), " +
				"RANGE_MISSING (a range committed that was never begun), GRID_MOVED (the timeline's successor of a " +
				"range's last Slot is no longer its proof's); UNNAMED, a conflict its path returned without a word, and OTHER, " +
				"a word this build does not know - neither should be anything but zero. A conflict met on the owner's " +
				"record and written again counts once, from the pass the write ended on.",
		}, []string{"operation", "reason"}),
		appliedWhole: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "progress_query_free_applied_total",
			Help: "Query-free completions (GAP_SKIPPED, SNAPSHOT_UNAVAILABLE) committed for a Slot an earlier " +
				"attempt had applied whole - every Plan evaluated, its events sent and its State written - and " +
				"ended before writing its Slot down, by kind, once the commit applied. Such a completion records " +
				"no gap. It is how often an attempt ends between its apply and its commit.",
		}, []string{"kind"}),
	}
	for _, caller := range progress.ProgressReadCallers {
		for _, source := range progress.ProgressReadSources {
			metrics.reads.WithLabelValues(caller, source)
		}
	}
	for _, result := range execution.SlotBeginResults {
		metrics.begins.WithLabelValues(result)
	}
	for _, reason := range execution.SlotNotCommittedReasons {
		metrics.notCommitted.WithLabelValues(reason)
	}
	for _, field := range progress.SupersededProjectionFields {
		metrics.superseded.WithLabelValues(field)
	}
	for _, operation := range progress.ProgressConflictOperations {
		for _, reason := range progressConflictReasons() {
			metrics.conflicts.WithLabelValues(operation, reason)
		}
	}
	for _, kind := range progress.QueryFreeAppliedKinds {
		metrics.appliedWhole.WithLabelValues(kind)
	}
	return metrics
}

func (m slotProgressMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.reads, m.begins, m.notCommitted, m.superseded, m.conflicts, m.appliedWhole}
}

// RecordProgressRead counts one read of a Progress record by its owner; a
// caller or source outside the closed lists is dropped.
func (r *Recorder) RecordProgressRead(caller, source string) {
	if r == nil || !knownLabel(progress.ProgressReadCallers, caller) || !knownLabel(progress.ProgressReadSources, source) {
		return
	}
	r.phaseTwo.slotProgress.reads.WithLabelValues(caller, source).Inc()
}

// RecordSlotBegin counts one BeginSlot by its result; a result outside
// execution.SlotBeginResults is dropped.
func (r *Recorder) RecordSlotBegin(result string) {
	if r == nil || !knownLabel(execution.SlotBeginResults, result) {
		return
	}
	r.phaseTwo.slotProgress.begins.WithLabelValues(result).Inc()
}

// RecordSlotNotCommitted counts one attempt that began its Slot and did not
// write it down; a reason outside execution.SlotNotCommittedReasons is counted
// under other.
func (r *Recorder) RecordSlotNotCommitted(reason string) {
	if r == nil {
		return
	}
	if !knownLabel(execution.SlotNotCommittedReasons, reason) {
		reason = execution.SlotNotCommittedOther
	}
	r.phaseTwo.slotProgress.notCommitted.WithLabelValues(reason).Inc()
}

// RecordProgressProjectionSuperseded counts one field of an unfinished Slot's
// projection a later attempt of it changed; a field outside
// progress.SupersededProjectionFields is dropped.
func (r *Recorder) RecordProgressProjectionSuperseded(field string) {
	if r == nil || !knownLabel(progress.SupersededProjectionFields, field) {
		return
	}
	r.phaseTwo.slotProgress.superseded.WithLabelValues(field).Inc()
}

// RecordProgressConflict counts one Progress write the record refused as a
// conflict, by operation and the fact that refused it. A reason outside
// execution.ProgressConflicts is counted under OTHER, not dropped: a word a
// path learns later is still a conflict counted. An operation outside
// progress.ProgressConflictOperations is dropped.
func (r *Recorder) RecordProgressConflict(operation, reason string) {
	if r == nil || !knownLabel(progress.ProgressConflictOperations, operation) {
		return
	}
	if !knownLabel(execution.ProgressConflicts, reason) {
		reason = observability.CursorRefusalOther
	}
	r.phaseTwo.slotProgress.conflicts.WithLabelValues(operation, reason).Inc()
}

// progressConflictReasons is the conflicts family's reason cells: every
// word, and OTHER.
func progressConflictReasons() []string {
	return append(append([]string(nil), execution.ProgressConflicts...), observability.CursorRefusalOther)
}

// RecordProgressQueryFreeApplied counts one query-free completion committed
// for a Slot an earlier attempt had applied whole, by its kind; a kind
// outside progress.QueryFreeAppliedKinds is dropped.
func (r *Recorder) RecordProgressQueryFreeApplied(kind string) {
	if r == nil || !knownLabel(progress.QueryFreeAppliedKinds, kind) {
		return
	}
	r.phaseTwo.slotProgress.appliedWhole.WithLabelValues(kind).Inc()
}
