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

// tailExecutor completes every Slot as the worker does: its query returns
// at the runner's clock, and the Slot tail after it.
type tailExecutor struct {
	clock *time.Time
	tail  time.Duration
}

func (executor *tailExecutor) Execute(ctx context.Context, _ execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
	execution.MarkQueryReturned(ctx, *executor.clock)
	*executor.clock = executor.clock.Add(executor.tail)
	return execution.SlotExecutionResult{Completed: true, CompletionKind: execution.CompletionFull, Result: observability.ResultSuccess}, nil
}

// A replay's post-query tail does not move the live deadline its group's next
// normal round's query runs under (014 next8): the replay rewrites the group
// whole, its tail past any normal round's, and kept it would cut the next
// normal round's read earlier, toward another divert. A normal round's tail
// moves it, as before.
func TestAReplaysTailDoesNotMoveTheNextNormalRoundsLiveDeadline(t *testing.T) {
	const queryGroup = execution.QueryGroupIdentity("query-group-1")
	consumer := execution.DataRequirementConsumer{ConsumerDeadlineUnixMilli: 1_700_000_200_000, DownstreamExecutionReserveMilliSec: 5_000}
	for _, c := range []struct {
		operation execution.Operation
		moves     bool
	}{{execution.OperationReplay, false}, {execution.OperationNormal, true}} {
		slot := frozenSlot(queryGroup)
		slot.Dispatch.Operation = c.operation
		now := time.UnixMilli(slot.EarliestQueryDeadlineUnixMilli - 30_000)
		executor := &tailExecutor{clock: &now, tail: 40 * time.Second}
		live := &execution.LiveReserve{}
		// The pooled tail is published, so a group's own tails move its
		// deadline once they are longer.
		live.Set(2 * time.Second)
		flights := NewFlightCoordinator()
		flights.SetLiveReserve(live)
		runner, err := NewRunner(queryGroup, &fakeSession{fence: slot.Dispatch.OwnerFence},
			&fakeSlotSource{slot: slot, facts: SlotDueFacts{IntervalSeconds: 60}}, executor, flights, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		before, _ := live.QueryDeadlineUnixMilli(consumer, queryGroup, 0)
		if _, attempted, err := runner.RunOne(context.Background()); err != nil || !attempted {
			t.Fatalf("%s: RunOne() = attempted %t, %v", c.operation, attempted, err)
		}
		after, _ := live.QueryDeadlineUnixMilli(consumer, queryGroup, 0)
		if moved := after != before; moved != c.moves {
			t.Fatalf("%s: the next normal round's live deadline %d -> %d (group tail %v), want it moved %t", c.operation, before,
				after, live.GroupTail(queryGroup), c.moves)
		}
		if c.moves && after != consumer.ConsumerDeadlineUnixMilli-(40*time.Second).Milliseconds() {
			t.Fatalf("a normal round's 40 s tail puts the next live deadline at %d, want 40 s before the consumer's", after)
		}
	}
}
