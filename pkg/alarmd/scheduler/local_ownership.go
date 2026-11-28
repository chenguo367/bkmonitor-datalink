// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// LocalOwnership answers a Runner's attempt from what the Worker already
// holds - its lease and the view it installed - when that says everything a
// fence check would (ownership.Session.ValidateLocally): the fence and the
// record rebuilt from the view with LocalCheckPass, or the reason the attempt
// checks its fence with the store, as it always did. NoteFenceRefused is told
// when a write's own fence refused the lease the view admitted, so the next
// attempt asks the store (015 R1).
type LocalOwnership interface {
	ValidateLocally(time.Time) (execution.OwnerFence, ownership.AssignmentRecord, string, error)
	NoteFenceRefused(time.Time)
}

// WithLocalOwnership lets the Runner admit an attempt from local, counting
// each check with count (nil counts nothing). A Runner without it checks its
// fence with the store before every attempt.
func (runner *Runner) WithLocalOwnership(local LocalOwnership, count func(outcome, reason string)) *Runner {
	if runner != nil {
		runner.local, runner.countLocal = local, count
	}
	return runner
}

// validateOwnership is the attempt's ownership check: locally when the
// Runner has a local answer and it passes, otherwise with the store.
func (runner *Runner) validateOwnership(ctx context.Context) (execution.OwnerFence, ownership.AssignmentRecord, error) {
	at := runner.now()
	if runner.local == nil {
		return runner.session.ValidateCurrentWithAssignment(ctx, at)
	}
	fence, record, reason, err := runner.local.ValidateLocally(at)
	switch {
	case err != nil:
		// The lease refused itself before any store was asked, as the store
		// path would have: that answer stands.
		runner.countOwnershipCheck(ownership.OwnershipCheckLocalRefused, reason)
		return execution.OwnerFence{}, ownership.AssignmentRecord{}, err
	case reason == ownership.LocalCheckPass:
		runner.countOwnershipCheck(ownership.OwnershipCheckLocalPass, reason)
		return fence, record, nil
	}
	runner.countOwnershipCheck(ownership.OwnershipCheckFallbackRedis, reason)
	return runner.session.ValidateCurrentWithAssignment(ctx, at)
}

func (runner *Runner) countOwnershipCheck(outcome, reason string) {
	if runner.countLocal != nil {
		runner.countLocal(outcome, reason)
	}
}

// fenceRefused is whether err carries a fence's refusal of the lease: a
// stale owner, a Query Group no longer desired here, or content moved past.
func fenceRefused(err error) bool {
	return errors.Is(err, ownership.ErrStaleFence) || errors.Is(err, ownership.ErrNotDesired) ||
		errors.Is(err, ownership.ErrContentScopeMoved)
}

// noteFenceRefused passes a write's fence refusal to the local answer.
func (runner *Runner) noteFenceRefused() {
	if runner.local != nil {
		runner.local.NoteFenceRefused(runner.now())
	}
}
