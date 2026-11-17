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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
)

// What a Slot costs the Progress record, and what its attempts come to: the
// reads of the record a Query Group's owner makes, by whether the key or the
// owner's record answered them (B2-3), and how each attempt's BeginSlot went
// and why one that began did not write its Slot down (B7), and which fields
// of an unfinished Slot's projection a later attempt of it changed. Every
// cell of every family exists from the start, so a zero is a reading.
type slotProgressMetrics struct {
	reads, begins, notCommitted, superseded *prometheus.CounterVec
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
				"write), retryable_io (the store did not answer). A BeginSlot that failed outright is counted by " +
				"its slot_completed line only.",
		}, []string{"result"}),
		notCommitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "slot_not_committed_total",
			Help: "Slot attempts that began their Slot (slot_begin_total{result=\"committed\"}) and returned without " +
				"writing it down, once each, by why: QUERY_NOT_READY (a readiness deferral, the data not in yet), " +
				"SNAPSHOT_RETRY and EXACT_SET_BLOCKED (the finalization asked for the attempt again), other " +
				"(every other return: a failed query, State write or Progress commit among them). Begun less " +
				"these is the attempts that committed.",
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
	return metrics
}

func (m slotProgressMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.reads, m.begins, m.notCommitted, m.superseded}
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
