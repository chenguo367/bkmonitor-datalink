// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// SlotBeginResults is every word a Slot attempt's BeginSlot is counted under
// (slot_begin_total): its status, in lower case, and SlotBeginDeferred.
var SlotBeginResults = []string{
	SlotBeginResult(ProgressCommitted), SlotBeginResult(ProgressConflict), SlotBeginResult(ProgressStaleOwner),
	SlotBeginResult(ProgressRetryableIO), SlotBeginResult(ProgressContentMoved), SlotBeginDeferred,
}

// SlotBeginDeferred is an attempt that put its BeginSlot off and sent its
// commit in its place, so never began its Slot (014 ⑩ F5): an empty round.
// One that put it off and then wrote, or left without writing its Slot
// down, is counted by the BeginSlot it made then, and an empty round whose
// commit the record refused by that status, as a BeginSlot at its start:
// the words still sum to the attempts.
const SlotBeginDeferred = "deferred"

// SlotBeginResult is the word a BeginSlot that returned status is counted
// under.
func SlotBeginResult(status ProgressCommitStatus) string {
	return strings.ToLower(string(status))
}

// Why a Slot attempt that began its Slot returned without writing it down
// (slot_not_committed_total), closed. Returned with a result and no error:
// its data was not in yet (QUERY_NOT_READY, a readiness deferral); its
// finalization asked for the attempt again (SNAPSHOT_RETRY; nothing in
// production resolves EXACT_SET_BLOCKED, which as a retrying result would
// read retry_pending); activation_read_failed, its Plans' activation could
// not be read; activation_changed, an activation moved under it (CONFIG_DRIFT,
// PLAN_REACTIVATED, PLAN_NOT_ACTIVE) and the next attempt runs the new one;
// output_retry_pending, its events' output is retried (OUTPUT_ACK_UNKNOWN,
// OUTPUT_LEASE_EXPIRING); retry_pending, another retrying result, a State or
// gap read that may succeed next time; incomplete, a result neither
// completed nor retrying (a probe's degraded round); no_result, no result at
// all, which only a panic leaves. Returned with an error: finalization_failed,
// its finalization did not resolve; owner_refused, its owner fence lost or
// its content moved; query_permit_failed, a physical query permit not had
// (the query admission's words: its wait reached the query's deadline,
// QUERY_PERMIT_DEADLINE, its queue full, recovery's permits off, its wait
// cancelled); cancelled, its context cancelled (the process stopping,
// the Query Group leaving); deadline_exceeded; declined, the watchdog
// declined it as hung; capacity_refused, the retained pool, a share or a
// slice refused it; store_conflict, a State or gap write refused by what it
// found; and by the step it failed in otherwise - query_failed,
// complete_failed (completing the Slot: evaluation, State preflight),
// apply_failed (admission, State and gap apply), output_failed (its events'
// output), progress_commit_failed, query_free_failed; segment_write_failed,
// a round that left members' State unwritten whose stable segments' write
// did not land (SEGMENT_WRITE_FAILED, retried whole); segment_read_failed,
// a Slot dispatched with a segment skip word whose segments did not read
// (SEGMENT_READ_FAILED, its retry reading them as stored). other is what
// none of these names.
const (
	SlotNotCommittedQueryNotReady        = contract.ReasonQueryNotReady
	SlotNotCommittedSnapshotRetry        = string(FinalizationSnapshotRetry)
	SlotNotCommittedActivationReadFailed = "activation_read_failed"
	SlotNotCommittedActivationChanged    = "activation_changed"
	SlotNotCommittedOutputRetryPending   = "output_retry_pending"
	SlotNotCommittedRetryPending         = "retry_pending"
	SlotNotCommittedIncomplete           = "incomplete"
	SlotNotCommittedNoResult             = "no_result"
	SlotNotCommittedFinalizationFailed   = "finalization_failed"
	SlotNotCommittedOwnerRefused         = "owner_refused"
	SlotNotCommittedQueryPermitFailed    = "query_permit_failed"
	SlotNotCommittedCancelled            = "cancelled"
	SlotNotCommittedDeadlineExceeded     = "deadline_exceeded"
	SlotNotCommittedDeclined             = "declined"
	SlotNotCommittedCapacityRefused      = "capacity_refused"
	SlotNotCommittedStoreConflict        = "store_conflict"
	SlotNotCommittedQueryFailed          = "query_failed"
	SlotNotCommittedCompleteFailed       = "complete_failed"
	SlotNotCommittedApplyFailed          = "apply_failed"
	SlotNotCommittedOutputFailed         = "output_failed"
	SlotNotCommittedProgressCommitFailed = "progress_commit_failed"
	SlotNotCommittedQueryFreeFailed      = "query_free_failed"
	SlotNotCommittedSegmentWriteFailed   = "segment_write_failed"
	SlotNotCommittedSegmentReadFailed    = "segment_read_failed"
	SlotNotCommittedOther                = "other"
)

// SlotNotCommittedReasons lists them.
var SlotNotCommittedReasons = []string{SlotNotCommittedQueryNotReady, SlotNotCommittedSnapshotRetry,
	SlotNotCommittedActivationReadFailed, SlotNotCommittedActivationChanged,
	SlotNotCommittedOutputRetryPending, SlotNotCommittedRetryPending, SlotNotCommittedIncomplete,
	SlotNotCommittedNoResult, SlotNotCommittedFinalizationFailed, SlotNotCommittedOwnerRefused,
	SlotNotCommittedQueryPermitFailed, SlotNotCommittedCancelled, SlotNotCommittedDeadlineExceeded, SlotNotCommittedDeclined,
	SlotNotCommittedCapacityRefused, SlotNotCommittedStoreConflict, SlotNotCommittedQueryFailed,
	SlotNotCommittedCompleteFailed, SlotNotCommittedApplyFailed, SlotNotCommittedOutputFailed,
	SlotNotCommittedProgressCommitFailed, SlotNotCommittedQueryFreeFailed, SlotNotCommittedSegmentWriteFailed,
	SlotNotCommittedSegmentReadFailed, SlotNotCommittedOther}
