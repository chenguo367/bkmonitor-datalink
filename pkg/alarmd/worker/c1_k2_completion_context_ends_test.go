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
	"bytes"
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// goroutinesInAll is how many goroutines of the process are in every one of
// frames.
func goroutinesInAll(frames ...string) int {
	stacks := make([]byte, 1<<22)
	count := 0
	for _, goroutine := range bytes.Split(stacks[:runtime.Stack(stacks, true)], []byte("\n\n")) {
		in := true
		for _, frame := range frames {
			in = in && bytes.Contains(goroutine, []byte(frame))
		}
		if in {
			count++
		}
	}
	return count
}

// workerStacks is the stacks of the goroutines in this package's code, each
// cut to its first frames: what a Slot that did not end waits on.
func workerStacks() string {
	stacks := make([]byte, 1<<22)
	var out []string
	for _, goroutine := range strings.Split(string(stacks[:runtime.Stack(stacks, true)]), "\n\n") {
		if !strings.Contains(goroutine, "alarmd/worker.") {
			continue
		}
		lines := strings.Split(goroutine, "\n")
		out = append(out, strings.Join(lines[:min(len(lines), 13)], "\n"))
	}
	return strings.Join(out, "\n\n")
}

// cancellingState is the fixture's State, the Slot's context cancelled as a
// completion first reads it once when holds, and every read on an ended
// context refused with it, as a store's read in Redis ends with its context.
type cancellingState struct {
	*persistedRetryState
	when   func() bool
	cancel context.CancelFunc
	once   sync.Once
}

func (state *cancellingState) LoadRuntime(ctx context.Context, request execution.StatePreflightRequest) (execution.StatePreflightResult, error) {
	if state.when() {
		state.once.Do(state.cancel)
	}
	if err := ctx.Err(); err != nil {
		return execution.StatePreflightResult{}, err
	}
	return state.persistedRetryState.LoadRuntime(ctx, request)
}

// A Slot whose context ends - its lease lost, its Query Group moved, the
// process stopping - while it completes a slice whose read came back whole
// ends as its completion failing, not its query: the step that failed is
// ours (a State read of ours timing out brought 163 Slots down in a day,
// the query fine), and a read judged once is not judged again as a read that
// failed - which, the context ended by a deadline, would also be remembered
// as a slice out of time. The context ends as the completion reads State,
// which ends with it. One stream, the context ended in its first
// completion: the completion failed, at stream_complete. Two consumers, the
// context ended as a consumer's completion first reads State: it has waited
// for every slice below its own to be in its completion (waitBelow), so the
// lowest slice any consumer has is one whose read was judged - the same. Its
// completion failing and the context ending are both before the dispatch at
// once, and either may go first: ten rounds.
func TestASlotWhoseContextEndsAsASliceCompletesEndsAsItsCompletionAsOneStreamDoes(t *testing.T) {
	request := windowedRequest()
	setup := func(cancel context.CancelFunc, when func() bool) *slicedFixture {
		fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
		fixture.queryPermits, fixture.counts = 16, &slotCounts{}
		var state execution.StateStore = fixture.state
		if cancel != nil {
			state = &cancellingState{persistedRetryState: fixture.state, when: when, cancel: cancel}
		}
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		return fixture
	}
	for _, k := range []int{1, 2} {
		rounds := 1
		if k == 2 {
			rounds = 10
		}
		for round := 0; round < rounds; round++ {
			ctx, cancel := context.WithCancel(context.Background())
			when := func() bool { return true }
			if k == 1 {
				worker.SetSliceConsumersForTest(t, 1)
			} else {
				worker.SetSliceConsumersByCoresForTest(t, 4)
				when = func() bool { return goroutinesInAll("(*sliceConsumer).run", "(*streamedExecution).complete") > 0 }
			}
			fixture := setup(cancel, when)
			ended := make(chan error, 1)
			go func() {
				_, err := fixture.coordinator.Execute(ctx, request)
				ended <- err
			}()
			bound, stop := context.WithTimeout(context.Background(), 20*time.Second) // fixed-wait: a Slot whose context ended returns as soon as what it waits on sees it; twenty seconds is that bound with room for -race
			var err error
			select {
			case err = <-ended:
			case <-bound.Done():
				stacks := workerStacks()
				stop()
				cancel()
				t.Fatalf("k %d round %d: the Slot did not end; its goroutines:\n%s", k, round, stacks)
			}
			stop()
			cancel()
			if err == nil || !strings.Contains(err.Error(), "complete Slot") {
				t.Fatalf("k %d round %d: a Slot whose context ended as it completed a slice ended with %v, want its completion's failure",
					k, round, err)
			}
			// A context ended is counted as cancelled whatever step it ended in.
			if want := []string{execution.SlotNotCommittedCancelled}; !reflect.DeepEqual(fixture.counts.notCommitted, want) {
				t.Fatalf("k %d round %d: the Slot was counted not committed as %v (%v), want %v", k, round, fixture.counts.notCommitted, err,
					want)
			}
			var stages []string
			completing, reading := false, false
			slicedObservationsMu.Lock()
			observations := append([]observability.Observation(nil), *fixture.observations...)
			slicedObservationsMu.Unlock()
			for _, observation := range observations {
				if observation.QueryFailure != nil {
					stages = append(stages, observation.QueryFailure.Stage)
					completing = completing || observation.QueryFailure.Stage == "stream_complete"
					reading = reading || observation.QueryFailure.Stage == "execute"
				}
			}
			if !completing || reading {
				t.Fatalf("k %d round %d: the Slot's query failures were said at %v (%v), want at stream_complete and not execute", k, round,
					stages, err)
			}
		}
	}
}
