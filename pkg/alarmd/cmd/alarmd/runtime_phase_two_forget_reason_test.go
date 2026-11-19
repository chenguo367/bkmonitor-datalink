// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// Every way a Query Group leaves the Runner set tells the lookback why, each
// in its own word, so alarmd_lookback_groups_forgotten_total says which of
// them took a group's state away. The bundle needs Redis, a control plane and
// a lease to stand up each way, so the callers are read off the source: what
// this catches is a caller passing another's word, or a new caller added
// without one of its own.
func TestEveryRunnerRemovalTellsTheLookbackItsOwnReason(t *testing.T) {
	raw, err := os.ReadFile("runtime_phase_two.go")
	if err != nil {
		t.Fatalf("read the bundle: %v", err)
	}
	source := string(raw)
	body := func(signature string) string {
		start := strings.Index(source, signature)
		if start < 0 {
			t.Fatalf("%s moved; this guard has to move with it", signature)
		}
		end := strings.Index(source[start:], "\n}\n")
		if end < 0 {
			t.Fatalf("could not find the end of %s", signature)
		}
		return source[start : start+end]
	}
	for _, c := range []struct{ function, call string }{
		{"func (bundle *phaseTwoWorkerBundle) Shutdown(", "removeRunnerLocked(queryGroup, lookback.ForgottenShutdown)"},
		{"func (bundle *phaseTwoWorkerBundle) applyAssignment(", "removeRunnerLocked(queryGroup, lookback.ForgottenMoved)"},
		{"func (bundle *phaseTwoWorkerBundle) declineHungExecutions(", "removeRunnerLocked(queryGroup, lookback.ForgottenDeclinedHung)"},
		{"func (bundle *phaseTwoWorkerBundle) stopLostQueryGroup(", "detachQueryGroup(queryGroup, lifecycle, lostForgetReason(err))"},
		{"func (bundle *phaseTwoWorkerBundle) detachLostQueryGroup(", "detachQueryGroup(queryGroup, lifecycle, lostForgetReason(err))"},
		{"func (bundle *phaseTwoWorkerBundle) detachQueryGroup(", "removeRunnerLocked(queryGroup, reason)"},
		{"func (bundle *phaseTwoWorkerBundle) removeRunnerLocked(", "Lookback.Forget(queryGroup, reason)"},
	} {
		if !strings.Contains(body(c.function), c.call) {
			t.Errorf("%s does not call %s", c.function, c.call)
		}
	}
	// Those are all of them: four removals and one detach, each named above.
	if got := strings.Count(source, "removeRunnerLocked(") - 1; got != 4 {
		t.Errorf("%d calls of removeRunnerLocked, want the 4 named here", got)
	}
	if got := strings.Count(source, "detachQueryGroup(") - 1; got != 2 {
		t.Errorf("%d calls of detachQueryGroup, want the 2 named here", got)
	}
}

// A Query Group let go of outside a planned stop is forgotten as
// content_switch when its lease ran out at a content switch, however wrapped,
// and as lease_lost otherwise: lostResult's judgement, in the lookback's
// words.
func TestALostQueryGroupIsForgottenByHowItsLeaseEnded(t *testing.T) {
	switched := &ownership.LeaseEndedAtContentSwitch{Err: ownership.ErrStaleFence}
	for _, c := range []struct {
		err  error
		want string
	}{
		{switched, lookback.ForgottenContentSwitch},
		{fmt.Errorf("renew: %w", switched), lookback.ForgottenContentSwitch},
		{errors.Join(switched, errors.New("release failed")), lookback.ForgottenContentSwitch},
		{ownership.ErrStaleFence, lookback.ForgottenLeaseLost},
		{errors.New("connection reset"), lookback.ForgottenLeaseLost},
		{nil, lookback.ForgottenLeaseLost},
	} {
		if got := lostForgetReason(c.err); got != c.want {
			t.Errorf("lostForgetReason(%v) = %s, want %s", c.err, got, c.want)
		}
	}
}
