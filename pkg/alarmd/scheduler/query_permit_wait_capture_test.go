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
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A query's wait for its permit is the attempt's own as well as the
// process's: the wait a permit granted after another's release took is
// added to the budget capture on its attempt's context, as query_permit_wait
// times it, and an attempt whose permits were free adds no more than they
// took.
func TestAPermitsWaitIsAddedToItsAttemptsBudgetCapture(t *testing.T) {
	clock := newMutableClock(time.Unix(300, 0))
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	freeCtx, free := observability.ContextWithBudgetCapture(context.Background())
	deadline := clock.Now().Add(time.Minute)
	var held []*QueryPermit
	for i := 0; i < testRecoveryLimits().ProcessQueryPermits; i++ {
		permit, err := flights.AcquireQueryPermit(freeCtx, execution.SlotIdentity{QueryGroup: execution.QueryGroupIdentity(rune('a' + i)),
			EvaluationTime: 60}, execution.OperationNormal, deadline)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, permit)
	}
	lateCtx, late := observability.ContextWithBudgetCapture(context.Background())
	granted := make(chan *QueryPermit, 1)
	go func() {
		permit, err := flights.AcquireQueryPermit(lateCtx, execution.SlotIdentity{QueryGroup: "late", EvaluationTime: 60},
			execution.OperationNormal, deadline)
		if err != nil {
			t.Error(err)
		}
		granted <- permit
	}()
	testwait.Until(t, "a caller queued for a permit", func() bool { return flights.queryPermitSnapshot().NormalWaiting != 0 })
	const held50ms = 50 * time.Millisecond
	time.Sleep(held50ms) // fixed-wait: the permit is held a known time, the least the queued query waits
	held[0].Release()
	permit := <-granted
	if permit == nil {
		t.Fatal("fixture: the late permit was not granted")
	}
	permit.Release()
	for _, permit := range held[1:] {
		permit.Release()
	}
	total, longest := late.PermitWaits()
	if total < held50ms || longest != total {
		t.Fatalf("the late attempt's permit waits are %s summed, %s longest; want its one wait, at least %s", total, longest, held50ms)
	}
	if total, _ := free.PermitWaits(); total >= held50ms {
		t.Fatalf("the attempt whose permits were free waited %s for them", total)
	}
	bare, err := flights.AcquireQueryPermit(context.Background(), execution.SlotIdentity{QueryGroup: "none", EvaluationTime: 60},
		execution.OperationNormal, deadline)
	if err != nil {
		t.Fatalf("a permit asked without a budget capture: %v", err)
	}
	bare.Release()
}
