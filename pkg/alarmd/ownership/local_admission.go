// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package ownership

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// CapabilityViewAssignment is a worker that is sent, with each Query Group
// of its view, the Assignment record's own fields - generation, control
// epoch, placement reason, assignment time and the segment skip words - and
// admits its Runner's attempts from the record it rebuilds of them (015 R1).
// A worker without it is sent its view as before: it recomputes its view's
// digest from the fields it decodes, and fields it cannot decode would make
// every view it is sent not match.
const CapabilityViewAssignment = "view-assignment.v1"

// LocalOwnership is what the view a worker installed says of a Query Group,
// for ValidateLocally: whether it names it, whether it carries the record's
// own fields, the record it rebuilt of them, and whether the Leader was
// heard from recently enough for the view to stand (HeardWithin).
type LocalOwnership struct {
	Named       bool
	Complete    bool
	Record      AssignmentRecord
	HeardWithin bool
}

// Why a local admission was not made; the attempt checks its fence with the
// store, as it did before (alarmd_ownership_local_check_total).
const (
	LocalCheckPass              = "local_pass"
	LocalCheckOwnLease          = "own_lease"
	LocalCheckNotInView         = "not_in_view"
	LocalCheckViewFieldsMissing = "view_fields_missing"
	LocalCheckNotDesired        = "not_desired"
	LocalCheckNeverConfirmed    = "never_confirmed"
	LocalCheckGenerationChanged = "generation_changed"
	LocalCheckPendingContent    = "pending_content"
	LocalCheckUnconfirmedRecord = "unconfirmed_record"
	LocalCheckLeaseRefused      = "lease_refused"
	// LocalCheckOther is a check counted under an outcome and reason this
	// build does not know: a word added without its cell.
	LocalCheckOther        = "other"
	LocalCheckViewSilent   = "view_silent"
	LocalCheckFenceRefused = "fence_refused"
)

// Outcomes of a Runner's ownership check, alarmd_ownership_local_check_total:
// admitted locally, checked with the store, or refused by the lease itself.
const (
	OwnershipCheckLocalPass     = "local_pass"
	OwnershipCheckFallbackRedis = "fallback_redis"
	OwnershipCheckLocalRefused  = "local_refused"
)

// OwnershipCheckCells is every (outcome, reason) a Runner's check is counted
// under: the pass, each reason an attempt checks its fence with the store,
// and the lease refusing itself.
func OwnershipCheckCells() [][2]string {
	cells := [][2]string{{OwnershipCheckLocalPass, LocalCheckPass}}
	for _, reason := range LocalCheckReasons {
		if reason != LocalCheckPass && reason != LocalCheckLeaseRefused {
			cells = append(cells, [2]string{OwnershipCheckFallbackRedis, reason})
		}
	}
	return append(cells, [2]string{OwnershipCheckLocalRefused, LocalCheckLeaseRefused}, [2]string{LocalCheckOther, LocalCheckOther})
}

// LocalCheckReasons lists every word a Runner's ownership check is counted
// under, the pass first.
var LocalCheckReasons = []string{LocalCheckPass, LocalCheckOwnLease, LocalCheckNotInView, LocalCheckViewFieldsMissing,
	LocalCheckNotDesired, LocalCheckNeverConfirmed, LocalCheckGenerationChanged, LocalCheckPendingContent,
	LocalCheckUnconfirmedRecord, LocalCheckViewSilent, LocalCheckFenceRefused, LocalCheckLeaseRefused}

// ValidateLocally admits an attempt on this lease from view, without the
// store, when everything a fence check would tell the attempt is already
// known: the lease admits work at at (admittedLease, the local half of every
// validation), it is held under a worker session, the view names the Query
// Group with the record's own fields, its desired worker is this one, its
// generation is the one the last fence check that confirmed the lease read
// (a move away and back reads as a new generation), no content change is
// pending, the content scope and timeline revision are the ones the lease
// was last confirmed with (a change that took effect since is confirmed with
// the store first: only a fence check moves the lease's own), the Leader was
// heard from recently, and no write's fence refused the lease since that
// check.
//
// It answers the attempt's admission and nothing else. It does not move the
// lease's horizon, content or timeline: those move only on a fence check
// that reads the record, so a holder whose view is behind a move never
// extends its own lease past the move's grace (015 R1, section 2.1).
//
// reason is LocalCheckPass with the fence and the rebuilt record, or why the
// attempt is to check its fence with the store; err is a local refusal of
// the lease itself, as ValidateCurrentWithAssignment would give it before
// asking the store, with reason LocalCheckLeaseRefused.
func (session *Session) ValidateLocally(at time.Time, view LocalOwnership) (execution.OwnerFence, AssignmentRecord, string, error) {
	lease, err := session.admittedLease(at)
	if err != nil {
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckLeaseRefused, err
	}
	session.mu.RLock()
	under, generation, refused := session.under, session.confirmedGeneration, !session.fenceRefusedAt.IsZero()
	session.mu.RUnlock()
	record := view.Record
	switch {
	case under == nil:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckOwnLease, nil
	case !view.Named || record.QueryGroup != lease.Fence.QueryGroup:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckNotInView, nil
	case !view.Complete || record.Validate() != nil:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckViewFieldsMissing, nil
	case record.DesiredWorkerID != lease.Fence.OwnerID:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckNotDesired, nil
	case generation == 0:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckNeverConfirmed, nil
	case record.AssignmentGeneration != generation:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckGenerationChanged, nil
	case record.PendingContentScope != "" || !record.EffectiveAt.IsZero() || lease.ContentChangePending():
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckPendingContent, nil
	case record.ContentScope != lease.ContentScope || record.TimelineRecordRevision != lease.TimelineRecordRevision:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckUnconfirmedRecord, nil
	case !view.HeardWithin:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckViewSilent, nil
	case refused:
		return execution.OwnerFence{}, AssignmentRecord{}, LocalCheckFenceRefused, nil
	}
	return lease.Fence, record, LocalCheckPass, nil
}

// NoteFenceRefused records that a write's own fence refused this lease at
// at - a BeginSlot answered STALE_OWNER or CONTENT_MOVED: the store says what
// the view did not, and attempts check their fence with the store until a
// check that began after at confirms the lease; its answer ends admission
// the way it always has.
func (session *Session) NoteFenceRefused(at time.Time) {
	if session == nil {
		return
	}
	session.mu.Lock()
	if at.After(session.fenceRefusedAt) {
		session.fenceRefusedAt = at
	}
	session.mu.Unlock()
}
