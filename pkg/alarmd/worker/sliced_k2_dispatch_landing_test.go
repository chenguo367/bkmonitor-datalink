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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot whose slices two consumers read side by side by the production
// wiring - k by the process's cores, from its second slice - reads its first
// slice on its own stream, the dispatch's, and hands the rest out. That
// slice's windows land as they are written, and what they held - the slice's
// whole input - is given back: a consumer counts none of it beside its own
// slice. Counted, it narrowed every consumer's limit for the whole Slot (the
// 1.0 next release gate's k = 2 cell, where the first slice was 30% of its
// Query Group's input): slices that fit were let go, and the splits they
// left were remembered round after round.
//
// 1,200 series of 2 KiB in a share of 6 MiB: a slice may hold 768 KiB with
// nothing beside it at k = 2. The first Slot is read whole and let go; from
// the second on, the Slot starts in slices and its first is the dispatch's.
// Each starts as one whose Query Group's last Slot was consumed on one stream
// - as after a restart on fewer cores - so the dispatch reads its first slice
// to one stream's limit, and the slices the consumers read, counted for one,
// are let go past theirs: what each consumer counted beside its slice is on
// the let_go lines.
func TestAConsumerCountsNoneOfTheInputTheDispatchsOwnSliceHeld(t *testing.T) {
	worker.SetSliceConsumersByCoresForTest(t, 4)
	worker.SetSizedSplitSeriesForTest(t, 300)
	before := slicedMaxSeries
	slicedMaxSeries = 4000
	t.Cleanup(func() { slicedMaxSeries = before })
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 0, 1200)
	for index := 0; index < 1200; index++ {
		series = append(series, slicedSeries{
			host: fmt.Sprintf("h%04d%c%c", index, alphabet[(index*13)%len(alphabet)], alphabet[(index*17+5)%len(alphabet)]), padding: 2 << 10,
		})
	}
	fixture := newSlicedFixture(t, series, sliceableFacts(), 2000)
	// Sixteen permits read four slices at once: the two consumers' and two
	// ahead.
	fixture.queryPermits, fixture.pool = 16, 8<<20
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
		Events: fixture.ports}, 2000)
	request := windowedRequest()
	group := request.Contract.Slot.QueryGroup
	slices := make([]uint64, 0, 6)
	judged := 0
	for round := 1; round <= 6; round++ {
		if round > 1 {
			worker.RememberSliceConsumersForTest(fixture.coordinator, group, 1)
		}
		lines, calls := len(fixture.letGoLines()), len(fixture.source.calls)
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed {
			t.Fatalf("round %d: result=%+v err=%v", round, result, err)
		}
		if round > 1 && result.Usage.InputSliceConsumers != 2 {
			t.Fatalf("round %d consumed its slices with %d streams, want 2 side by side", round, result.Usage.InputSliceConsumers)
		}
		// The dispatch's own slice: the round's first read the Slot made
		// itself, completed before its consumers began.
		own := 0
		for _, call := range fixture.source.calls[calls:] {
			if !call.readAhead {
				own = call.padding
				break
			}
		}
		// By the round's last slice let go the dispatch's own windows have
		// long landed: of its input a consumer counts a sliver - what the
		// dispatch holds beside them - and none of its own slice's. Its
		// windows in flight, a consumer counts them with it, never less than
		// it holds; they are what it gives back as they land, while the
		// consumers read.
		var last *observability.InputSliceLetGoFacts
		for _, line := range fixture.letGoLines()[lines:] {
			if line.Consumers == 2 {
				judged++
				last = &line
			}
		}
		if last != nil && last.Dispatch[0]*2*10 >= uint64(own) {
			t.Fatalf("round %d: the last slice a consumer let go counted %d bytes of the dispatch's input, its own slice "+
				"%d bytes of padding (line %+v): want none of that slice's, its windows given back as they landed",
				round, last.Dispatch[0]*2, own, *last)
		}
		// And of all it holds, its windows' State and output given back with
		// their input, what it keeps for the Slot - the keys and Plans it
		// merged first - is well under its slice.
		if last != nil && last.DispatchBytes()*2*2 >= uint64(own) {
			t.Fatalf("round %d: the last slice a consumer let go counted %d bytes of the dispatch in all, its own slice "+
				"%d bytes of padding (line %+v): want what it keeps for the Slot alone, under half its slice",
				round, last.DispatchBytes()*2, own, *last)
		}
		slices = append(slices, result.Usage.InputSlices)
	}
	if judged == 0 {
		t.Fatal("no consumer let a slice go: nothing a consumer counted of the dispatch was read")
	}
	// Settled in the slices the rounds before left, the Slot reads as many of
	// them round after round: none let go for the dispatch's slice and split
	// again.
	for round := 5; round <= 6; round++ {
		if slices[round-1] != slices[3] {
			t.Fatalf("slices by round %v: round %d read %d, the fourth %d", slices, round, slices[round-1], slices[3])
		}
	}
}

// forgetState empties the State the fixture's store keeps: the same Slot
// executed again then finds none of its series applied and writes each,
// window by window - a write held or failed is a window's.
func forgetState(fixture *slicedFixture) {
	fixture.state.mu.Lock()
	defer fixture.state.mu.Unlock()
	fixture.state.saved = map[execution.StateKeyIdentity]execution.StateMutation{}
}

// heldApplyState holds the first apply after it is armed until the Slot's
// consumers begin or the case lets it go (release), the test's deadline at
// the latest; fail, set, is what it then fails its write with.
type heldApplyState struct {
	*persistedRetryState
	armed atomic.Bool
	gate  atomic.Pointer[applyGate]
	// fail, set, is what the held apply fails its write with once let go.
	fail error
}

type applyGate struct {
	holding, began, release          chan struct{}
	holdOnce, beganOnce, releaseOnce sync.Once
	expired                          <-chan time.Time
}

func newApplyGate(t *testing.T) *applyGate {
	return &applyGate{holding: make(chan struct{}), began: make(chan struct{}), release: make(chan struct{}),
		expired: testwait.Expired(t)}
}

// letGo lets the held apply write.
func (gate *applyGate) letGo() { gate.releaseOnce.Do(func() { close(gate.release) }) }

func (s *heldApplyState) ApplyRuntime(ctx context.Context, req execution.StateApplyRequest) (execution.StateApplyResult, error) {
	if gate := s.gate.Load(); gate != nil && s.armed.CompareAndSwap(true, false) {
		gate.holdOnce.Do(func() { close(gate.holding) })
		select {
		case <-gate.began:
		case <-gate.release:
		case <-gate.expired:
		}
		if s.fail != nil {
			return execution.StateApplyResult{}, s.fail
		}
	}
	return s.persistedRetryState.ApplyRuntime(ctx, req)
}

// consumersBegin notes the consumers beginning, and lets the held apply
// write.
func (s *heldApplyState) consumersBegin() {
	if gate := s.gate.Load(); gate != nil {
		gate.beganOnce.Do(func() { close(gate.began) })
	}
}

// The window of the slice the dispatch reads itself lands before its
// consumers begin (015 C1, the k = 2 share, 3.1): in flight it is still the
// dispatch's, counted 1/k by each consumer, and a consumer's first slice
// judged beside it was let go and split for room the window gives back once
// written. Its write is held here, and let go only once the dispatch is seen
// waiting for it (drainApply): consumers that begin while it is held fail
// the case.
func TestTheDispatchsOwnSliceWindowLandsBeforeItsConsumersBegin(t *testing.T) {
	worker.SetSliceConsumersByCoresForTest(t, 4)
	worker.SetSizedSplitSeriesForTest(t, 300)
	before := slicedMaxSeries
	slicedMaxSeries = 4000
	t.Cleanup(func() { slicedMaxSeries = before })
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 0, 1200)
	for index := 0; index < 1200; index++ {
		series = append(series, slicedSeries{
			host: fmt.Sprintf("h%04d%c%c", index, alphabet[(index*13)%len(alphabet)], alphabet[(index*17+5)%len(alphabet)]), padding: 2 << 10,
		})
	}
	// And one without the host: the slice of the series without it is the
	// tree's first, the one the dispatch reads itself - its window has a
	// State to write as the consumers begin.
	series = append(series, slicedSeries{padding: 2 << 10})
	fixture := newSlicedFixture(t, series, sliceableFacts(), 2000)
	state := &heldApplyState{persistedRetryState: fixture.state}
	worker.SetConsumersBeganForTest(t, state.consumersBegin)
	fixture.queryPermits, fixture.pool = 16, 8<<20
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: state, GapGuard: fixture.ports,
		Events: fixture.ports}, 2000)
	request := windowedRequest()
	// The first Slots are read whole and split, and refine their slices until
	// the one the dispatch reads first is within a consumer's limit.
	for round := 1; round <= 3; round++ {
		if result, err := fixture.coordinator.Execute(context.Background(), request); err != nil || !result.Completed {
			t.Fatalf("warming Slot %d: result=%+v err=%v", round, result, err)
		}
	}
	for round := 4; round <= 5; round++ {
		forgetState(fixture)
		var own atomic.Bool
		state.armed.Store(false)
		gate := newApplyGate(t)
		state.gate.Store(gate)
		fixture.source.beforeReadAs = func(_ context.Context, readAhead bool) {
			if !readAhead && own.CompareAndSwap(false, true) {
				// The Slot's own first read, the dispatch's: its windows'
				// first write is held.
				state.armed.Store(true)
			}
		}
		type ending struct {
			result execution.SlotExecutionResult
			err    error
		}
		ended := make(chan ending, 1)
		go func() {
			result, err := fixture.coordinator.Execute(context.Background(), request)
			ended <- ending{result, err}
		}()
		testwait.Until(t, "the dispatch's window's write held", func() bool { return isClosed(gate.holding) })
		testwait.Until(t, "the dispatch waiting for its window, or its consumers beginning", func() bool {
			return isClosed(gate.began) || goroutinesInAll("(*streamedExecution).drainApply") > 0
		})
		if isClosed(gate.began) {
			gate.letGo()
			<-ended
			t.Fatalf("round %d: the consumers began with the dispatch's window in flight", round)
		}
		gate.letGo()
		var got ending
		select {
		case got = <-ended:
		case <-testwait.Expired(t):
			t.Fatalf("round %d: the Slot did not end once the dispatch's window was let go", round)
		}
		if got.err != nil || !got.result.Completed || got.result.Usage.InputSliceConsumers != 2 {
			t.Fatalf("round %d: result=%+v err=%v, want it completed side by side", round, got.result, got.err)
		}
		if !isClosed(gate.began) {
			t.Fatalf("round %d: the consumers never began", round)
		}
	}
}

// isClosed is whether done is closed.
func isClosed(done chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// A window of the dispatch's own slice that fails its write fails the Slot
// before its consumers begin (015 C1, the k = 2 share, 3.1), as a failed
// write of the dispatch's fails it while they read: no consumer reads a slice
// of a Slot that cannot complete.
func TestADispatchWindowFailingItsWriteEndsTheSlotBeforeItsConsumersBegin(t *testing.T) {
	worker.SetSliceConsumersByCoresForTest(t, 4)
	worker.SetSizedSplitSeriesForTest(t, 300)
	before := slicedMaxSeries
	slicedMaxSeries = 4000
	t.Cleanup(func() { slicedMaxSeries = before })
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 0, 1200)
	for index := 0; index < 1200; index++ {
		series = append(series, slicedSeries{
			host: fmt.Sprintf("h%04d%c%c", index, alphabet[(index*13)%len(alphabet)], alphabet[(index*17+5)%len(alphabet)]), padding: 2 << 10,
		})
	}
	// And one without the host: the slice of the series without it is the
	// tree's first, the one the dispatch reads itself - its window has a
	// State to write as the consumers begin.
	series = append(series, slicedSeries{padding: 2 << 10})
	fixture := newSlicedFixture(t, series, sliceableFacts(), 2000)
	state := &heldApplyState{persistedRetryState: fixture.state}
	worker.SetConsumersBeganForTest(t, state.consumersBegin)
	fixture.queryPermits, fixture.pool = 16, 8<<20
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: state, GapGuard: fixture.ports,
		Events: fixture.ports}, 2000)
	request := windowedRequest()
	var slices uint64
	for round := 1; round <= 3; round++ {
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed {
			t.Fatalf("warming Slot %d: result=%+v err=%v", round, result, err)
		}
		slices = result.Usage.InputSlices
	}
	failed := errors.New("injected: the dispatch's window failed its write")
	state.fail = failed
	forgetState(fixture)
	gate := newApplyGate(t)
	gate.letGo()
	state.gate.Store(gate)
	var own atomic.Bool
	fixture.source.beforeReadAs = func(_ context.Context, readAhead bool) {
		if !readAhead && own.CompareAndSwap(false, true) {
			// The Slot's own first read, the dispatch's: its windows' first
			// write fails.
			state.armed.Store(true)
		}
	}
	_, err := fixture.coordinator.Execute(context.Background(), request)
	if !isClosed(gate.holding) {
		t.Fatal("fixture: no write of the dispatch's window was failed")
	}
	if !errors.Is(err, failed) || !strings.Contains(err.Error(), "complete Slot") {
		t.Fatalf("the Slot ended with %v, want its completion failing on its dispatch's window's failed write", err)
	}
	if isClosed(gate.began) {
		t.Fatalf("the consumers of the Slot's %d slices began though its dispatch's window had failed its write", slices)
	}
}
