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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// A fenced write of the slot source's own - the read hold's freeze - that
// the fence refused is the owner's refusal, as the Runner's own check would
// have had it: returned as ownership's error, with no source backoff. It
// was read as a source retry, the Query Group backed off and counted a
// source failure (014 ⑩ F1).
func TestASlotSourcesFencedWriteRefusedIsTheOwnersRefusalNotASourceRetry(t *testing.T) {
	for _, refusal := range []error{ownership.ErrStaleFence, ownership.ErrContentScopeMoved} {
		t.Run(refusal.Error(), func(t *testing.T) {
			clock := newMutableClock(time.Unix(200, 0))
			fence := execution.OwnerFence{QueryGroup: "query-group-1", OwnerID: "worker-1", OwnerEpoch: 1, LeaseToken: "token-1"}
			source := &fakeSlotSource{err: &SourceRetryError{Err: refusal}}
			flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), clock.Now)
			if err != nil {
				t.Fatal(err)
			}
			runner, err := NewRunner("query-group-1", &fakeSession{fence: fence}, source, &blockingExecutor{}, flights, clock.Now)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = runner.RunOne(context.Background())
			if !errors.Is(err, refusal) {
				t.Fatalf("RunOne returned %v, want the fence's refusal", err)
			}
			if ready := runner.NextReadyAt(); !ready.IsZero() {
				t.Fatalf("the owner's refusal set a source backoff, ready again at %s", ready)
			}
		})
	}
}
