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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// requestExecutor completes every Slot and keeps the request it was handed.
type requestExecutor struct {
	requests []execution.SlotExecutionRequest
}

func (executor *requestExecutor) Execute(_ context.Context, request execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
	executor.requests = append(executor.requests, request)
	return execution.SlotExecutionResult{Completed: true, CompletionKind: execution.CompletionFull, Result: observability.ResultSuccess}, nil
}

// The worker is told a replay's distance and its age past its deadline, as
// the scheduler judged them (014 next9 2): a Slot diverted short of time -
// distance one, age zero - reaches it a replay short of time, one past its
// deadline a replay that is not, and a normal round carries neither.
func TestTheWorkerIsToldAReplaysDistanceAndAge(t *testing.T) {
	const queryGroup = execution.QueryGroupIdentity("query-group-1")
	for _, c := range []struct {
		name      string
		operation execution.Operation
		recovery  SlotRecoveryFacts
		distance  uint32
		age       int64
		short     bool
	}{
		{"diverted short of time", execution.OperationReplay, SlotRecoveryFacts{Disposition: ReplayEligible, Distance: 1, RecheckAtUnixMilli: 101_000},
			1, 0, true},
		{"past its deadline", execution.OperationReplay, SlotRecoveryFacts{Disposition: ReplayEligible, Distance: 2, Age: 7 * time.Second,
			RecheckAtUnixMilli: 701_000}, 2, 7_000, false},
		{"a normal round", execution.OperationNormal, SlotRecoveryFacts{}, 0, 0, false},
	} {
		slot := frozenSlot(queryGroup)
		slot.Dispatch.Operation, slot.Recovery = c.operation, c.recovery
		now := time.UnixMilli(100_500)
		executor := &requestExecutor{}
		runner, err := NewRunner(queryGroup, &fakeSession{fence: slot.Dispatch.OwnerFence},
			&fakeSlotSource{slot: slot, facts: SlotDueFacts{IntervalSeconds: 60}}, executor, NewFlightCoordinator(), func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if _, attempted, err := runner.RunOne(context.Background()); err != nil || !attempted || len(executor.requests) != 1 {
			t.Fatalf("%s: RunOne() = attempted %t, %v, %d requests", c.name, attempted, err, len(executor.requests))
		}
		request := executor.requests[0]
		if request.ReplayDistance != c.distance || request.ReplayAgeMillis != c.age || request.ReplayShortOfTime() != c.short {
			t.Fatalf("%s: the worker was told distance %d, age %d ms, short of time %t; want %d, %d, %t", c.name, request.ReplayDistance,
				request.ReplayAgeMillis, request.ReplayShortOfTime(), c.distance, c.age, c.short)
		}
	}
}
