// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slotContextEnds runs a Slot read in slices over the slices its last read
// left - consumed side by side by a process of four cores (k 2: two consumers
// from its second slice, a read width of four), or by one stream (k 1) - its
// context cancelled as the nth of its events happens: an observation said, or
// a read asked of the provider, from whichever goroutine. n 0 cancels nothing.
// It returns the events the Slot had, whether Execute returned within the
// bound and with what, and the fixture.
func slotContextEnds(t *testing.T, k int, n int64) (int64, bool, execution.SlotExecutionResult, error, *slicedFixture) {
	t.Helper()
	if k == 1 {
		worker.SetSliceConsumersForTest(t, 1)
	} else {
		worker.SetSliceConsumersByCoresForTest(t, 4)
	}
	request := windowedRequest()
	fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
	fixture.queryPermits, fixture.counts = 16, &slotCounts{}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
		Events: fixture.ports}, 200)
	worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
		[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var events atomic.Int64
	happened := func() {
		if events.Add(1) == n {
			cancel()
		}
	}
	fixture.observe = func(observability.Observation) { happened() }
	fixture.source.beforeRead = happened
	type outcome struct {
		result execution.SlotExecutionResult
		err    error
	}
	ended := make(chan outcome, 1)
	go func() {
		result, err := fixture.coordinator.Execute(ctx, request)
		ended <- outcome{result, err}
	}()
	bound, stop := context.WithTimeout(context.Background(), 10*time.Second) // fixed-wait: a Slot whose context ended returns as soon as what it waits on sees it; ten seconds is that bound with room for -race
	defer stop()
	select {
	case got := <-ended:
		return events.Load(), true, got.result, got.err, fixture
	case <-bound.Done():
		return events.Load(), false, execution.SlotExecutionResult{}, nil, fixture
	}
}

// A Slot whose context ends - its lease lost, its Query Group moved to
// another worker, the process stopped - returns, wherever it is when it
// ends: consumed side by side, cancelled as each of its events happens in
// turn, from the first to the last a Slot left to run has, Execute returns
// within the bound, everything the Slot held of the process's pool given
// back, none of its consumers, reads ahead or apply goroutines left behind.
// One stream (k 1), the control, alike. A wait on the C1 path that nothing
// but its peer ends is a Slot that never returns once that peer has seen the
// context end and gone (015 C1 section 7). Which of two goroutines ready at
// once goes first is the scheduler's, so each event is cancelled at in three
// rounds side by side, in one for the control. The read ahead of a job a
// consumer freed by a split never took - a window these cases reach too
// seldom to tell - is held by
// TestAReadAheadInAJobNoConsumerTookIsStoppedAsTheConsumersStop.
func TestASlotWhoseContextEndsReturnsWhereverItIsConsumedSideBySideAsOneStreamDoes(t *testing.T) {
	for _, shape := range []struct {
		k      int
		rounds int
	}{{1, 1}, {2, 3}} {
		k := shape.k
		total, returned, result, err, _ := slotContextEnds(t, k, 0)
		if !returned || err != nil || !result.Completed {
			t.Fatalf("fixture: k %d, a Slot whose context does not end returned %v with %+v, %v", k, returned, result, err)
		}
		if total < 10 {
			t.Fatalf("fixture: k %d, a Slot of %d events, want a run of them", k, total)
		}
		for run := 0; run < shape.rounds*int(total); run++ {
			n := int64(run/shape.rounds) + 1
			_, returned, _, _, fixture := slotContextEnds(t, k, n)
			if !returned {
				t.Fatalf("k %d, its context ended at its event %d of %d (round %d): Execute did not return", k, n, total,
					run%shape.rounds+1)
			}
			at := fmt.Sprintf("k %d, ended at its event %d of %d", k, n, total)
			testwait.Until(t, at+": the Slot's pool given back", func() bool {
				return worker.PoolHeldForTest(fixture.coordinator) == [6]uint64{}
			})
			testwait.Until(t, at+": no consumer, read ahead or apply goroutine of the Slot left", func() bool {
				return !goroutinesIn("(*sliceConsumer).run") && !goroutinesIn("startSliceReadAhead") && worker.ApplyGoroutinesForTest() == 0
			})
		}
	}
}
