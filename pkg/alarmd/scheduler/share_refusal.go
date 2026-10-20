// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// ShareRefusalsToFinalize is how many identical deterministic share refusals
// of one Slot the Runner takes before it finalizes the Slot without a query.
//
// Two, not one. The refusal is deterministic by construction - the share is
// half the replica's retained pool, fixed by configuration, and is compared
// with the Slot's own total, never with what other Query Groups hold - so the
// first refusal would already be safe to act on. The second costs one more
// read of the window and buys two things: an answer that was oversized once
// for a reason of its own (a provider that answered a batch twice, a share
// changed by a resize between the two attempts, which also changes the limit
// and so does not count as identical) does not lose its Slot, and the Slot is
// still given up within one retry backoff instead of after the replay age -
// ten minutes and a dozen whole-window reads, which is what it cost before.
const ShareRefusalsToFinalize = 2

// shareRefusalState is what the Runner holds of the Slot it is retrying after
// deterministic share refusals: in memory beside the Slot's recovery attempt,
// keyed by the same frozen contract, and never persisted. A Runner that loses
// it - the Query Group moves, the process restarts - counts again from zero,
// which costs at most one more read before the same decision.
type shareRefusalState struct {
	contract execution.FrozenExecutionContractRef
	last     execution.ShareRefusal
	count    uint32
}

// noteShareRefusal records one Execute error of slot. It reports whether the
// Slot is now to be finalized as RESOURCE_SHARE_EXCEEDED: the error is a
// deterministic share refusal and the same refusal (same phase, same share)
// has now been seen ShareRefusalsToFinalize times in a row for this Slot.
//
// A refusal on the pool, or any other error, is not counted and leaves the
// count as it stands: those are retried as before, and a timeout between two
// share refusals says nothing about whether the Slot fits.
func (runner *Runner) noteShareRefusal(slot FrozenSlot, err error) bool {
	refusal, deterministic := execution.ShareRefusalOf(err)
	if !deterministic {
		return false
	}
	state := runner.shareRefusal
	if state == nil || state.contract != slot.Contract || !state.last.Same(refusal) {
		state = &shareRefusalState{contract: slot.Contract}
		runner.shareRefusal = state
	}
	state.last = refusal
	state.count++
	return state.count >= ShareRefusalsToFinalize
}

// shareExceeded reports whether slot is the Slot this Runner decided to
// finalize for its share.
func (runner *Runner) shareExceeded(slot FrozenSlot) bool {
	state := runner.shareRefusal
	return state != nil && state.contract == slot.Contract && state.count >= ShareRefusalsToFinalize
}

// forgetShareRefusal drops the count once its Slot completed, or the Runner
// moved on to another Slot.
func (runner *Runner) forgetShareRefusal(slot FrozenSlot) {
	if state := runner.shareRefusal; state != nil && state.contract == slot.Contract {
		runner.shareRefusal = nil
	}
}

// shareSkipFacts is the refusal a share-exceeded skip was decided on, for the
// Slot's slot_skipped line.
func (runner *Runner) shareSkipFacts(slot FrozenSlot) *observability.SlotSkipShareFacts {
	if !runner.shareExceeded(slot) {
		return nil
	}
	last := runner.shareRefusal.last
	return &observability.SlotSkipShareFacts{Phase: last.Phase, OwnUsedBytes: last.OwnUsedBytes,
		RequestedBytes: last.RequestedBytes, LimitBytes: last.LimitBytes, Refusals: runner.shareRefusal.count}
}
