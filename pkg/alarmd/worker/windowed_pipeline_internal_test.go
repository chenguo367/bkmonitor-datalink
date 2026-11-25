// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// An apply-side hold the share refuses asks the Slot's goroutine to let its
// reads ahead go and waits for the answer; the Slot's goroutine answers it
// while it waits to hand the next task over, the apply goroutine busy with
// the one asking (014 next9 4, C11): neither waits on the other for good.
func TestAnApplySideHoldRefusedWhileTheSlotWaitsToHandOverIsAnswered(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream := fixture.stream
	ctx := context.Background()
	asked := make(chan error, 1)
	first := applyTask{run: func(applyCtx context.Context) error {
		// More than the whole share: refused, and the reads ahead asked to go.
		err := stream.coordinator.holdRetained(retainedOwnerFrom(applyCtx), stream.coordinator.qgShareBytes()+1, "frames")
		asked <- err
		return nil
	}}
	if err := stream.handOver(ctx, first); err != nil {
		t.Fatal(err)
	}
	handed := make(chan error, 1)
	go func() { handed <- stream.handOver(ctx, applyTask{run: func(context.Context) error { return nil }}) }()
	select {
	case err := <-handed:
		if err != nil {
			t.Fatal(err)
		}
	case <-testwait.Expired(t):
		t.Fatal("the Slot's goroutine and the apply goroutine waited on each other")
	}
	var exceeded *provisionalBudgetExceededError
	if err := <-asked; !errors.As(err, &exceeded) {
		t.Fatalf("the apply side's hold past the share = %v, want it refused", err)
	}
	if err := stream.drainApply(ctx); err != nil {
		t.Fatal(err)
	}
}

// While a window is written, what the apply side holds - with what the
// windows in flight give back as they land - stays within the half of the
// share they were cut in: a frame past it is refused (and only not kept),
// and the other half, the window being filled, merges as before. Outside a
// window's write - the tail - the apply side is judged by the share alone.
func TestTheApplySideHoldsWithinItsHalfOfTheShareWhileAWindowIsWritten(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	share := stream.coordinator.qgShareBytes()
	pipe := stream.applyPipeline(context.Background())
	judge := func(bytes uint64) error {
		stream.coordinator.reservations.mu.Lock()
		defer stream.coordinator.reservations.mu.Unlock()
		return pipe.judgeHeldLocked(bytes, "frames")
	}
	stream.inflightHeld.Store(share / 2)
	pipe.writingWindow.Store(true)
	if err := judge(1); err == nil {
		t.Fatal("a frame past the half of the share a window in flight was cut in was judged to fit")
	}
	pipe.writingWindow.Store(false)
	if err := judge(1); err != nil {
		t.Fatalf("outside a window's write, a hold within the share: %v", err)
	}
	stream.inflightHeld.Store(0)
	// The Slot's side merges up to its own half beside it.
	if err := stream.reserveRetainedInput(context.Background(), share/2); err != nil {
		t.Fatalf("the Slot's side reserving its half: %v", err)
	}
}

// A window's Plans are decided as it lands by what the Slot had passed as it
// was cut (014 next9 4, C2): a Plan the evaluation moves past while the next
// window is filled is not decided by the window cut before - its series in
// later windows have not been sent - and is by the window cut after.
func TestAPlanPassedWhileTheNextWindowIsFilledIsDecidedByTheWindowCutAfter(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	ctx := context.Background()
	plan := fixture.due.Identity
	fixture.written("series-1")
	fixture.startWindows(t)
	cut, err := stream.takeWindow(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	stream.windows.passed = map[execution.PlanIdentity]struct{}{plan: {}}
	stream.decideLanding(ctx, cut)
	if _, decided := stream.windows.decided[plan]; decided {
		t.Fatal("the window cut before the Plan was passed decided it")
	}
	after, err := stream.takeWindow(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	stream.decideLanding(ctx, after)
	if _, decided := stream.windows.decided[plan]; !decided {
		t.Fatal("the window cut after the Plan was passed did not decide it")
	}
}

// What the apply side holds of the Slot's share - a segment round's joins,
// taken in the tail (C16) - is the Slot's once the apply goroutine is joined,
// and goes back with everything else the Slot holds.
func TestWhatTheApplySideHoldsIsTheSlotsOnceJoinedAndGoesBackWithIt(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	coordinator := stream.coordinator
	before := RetainedReserved(coordinator)
	pipe := stream.applyPipeline(context.Background())
	if err := coordinator.holdRetained(pipe, 100, "segment_book"); err != nil {
		t.Fatal(err)
	}
	if stream.applyHeld() != 100 || stream.retainedTotal() != 100 || RetainedReserved(coordinator) != before+100 {
		t.Fatalf("the apply side holds %d, the Slot %d, the pool %d; want the 100 joined in all three", stream.applyHeld(),
			stream.retainedTotal(), RetainedReserved(coordinator)-before)
	}
	stream.joinApply(context.Background())
	if stream.applyHeld() != 0 || stream.retainedByPhase[retainPhaseState] != 100 {
		t.Fatalf("joined: the apply side holds %d, the Slot's state %d; want the 100 the Slot's", stream.applyHeld(),
			stream.retainedByPhase[retainPhaseState])
	}
	stream.releaseProvisional()
	if got := RetainedReserved(coordinator); got != before {
		t.Fatalf("the pool holds %d after the Slot, want %d", got, before)
	}
}

// heldWindowState holds the first State apply until released, saying when it
// is held.
type heldWindowState struct {
	execution.StateStore
	first    sync.Once
	entered  chan struct{}
	released chan struct{}
}

func (state *heldWindowState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	state.first.Do(func() {
		close(state.entered)
		<-state.released
	})
	return state.StateStore.ApplyRuntime(ctx, request)
}

// A Slot past its per-Slot cap once windows of it are written is answered -
// the Plans it had not got past replaced, their no-data withheld - only once
// the window it handed is written and landed (014 next9 4, C3): what the
// replacement writes is what the apply side reads, and the window cut before
// the cap was passed is written as it was.
func TestASlotPastItsCapIsAnsweredOnceTheWindowInFlightHasLanded(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream := fixture.stream
	ctx := context.Background()
	held := &heldWindowState{StateStore: stream.coordinator.ports.State, entered: make(chan struct{}), released: make(chan struct{})}
	stream.coordinator.ports.State = held
	fixture.written("series-1")
	fixture.startWindows(t)
	task, err := stream.takeWindow(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.handOver(ctx, applyTask{window: task}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-held.entered:
	case <-testwait.Expired(t):
		t.Fatal("the window handed was never written")
	}
	go func() {
		time.Sleep(20 * time.Millisecond) // fixed-wait: the window still written as the cap is passed, the subject of the case
		close(held.released)
	}()
	if err := stream.completeWindowsBeyondSlotBudget(ctx); err != nil {
		t.Fatal(err)
	}
	if inflight := stream.windows.pipe.inflight; inflight != 0 {
		t.Fatalf("the Slot was answered with %d windows still in flight", inflight)
	}
	if _, replaced := stream.windows.replaced[fixture.due.Identity]; !replaced {
		t.Fatal("the Plan the evaluation had not got past was not replaced")
	}
}

// sampledState takes 20 ms over each State apply and reads, as it starts,
// what the pool holds.
type sampledState struct {
	execution.StateStore
	coordinator *SlotExecutionCoordinator
	most        atomic.Uint64
}

func (state *sampledState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	if held := RetainedReserved(state.coordinator); held > state.most.Load() {
		state.most.Store(held)
	}
	time.Sleep(20 * time.Millisecond) // fixed-wait: a window written while the next is filled, the subject of the case
	return state.StateStore.ApplyRuntime(ctx, request)
}

// A Slot whose views alone are more than its share, its windows written while
// the next is filled, holds two windows at once - the one in flight and the
// one filled - and never more than its share between them (014 next9 4): each
// window is cut at half of what the share leaves it, so the two fit. Each
// State key is written once.
func TestTwoWindowsInHandHoldNoMoreThanTheShare(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream, coordinator := fixture.stream, fixture.stream.coordinator
	share := coordinator.qgShareBytes()
	sampled := &sampledState{StateStore: coordinator.ports.State, coordinator: coordinator}
	coordinator.ports.State = sampled
	const series = 10
	const inputBytes, viewBytes = uint64(30_000), uint64(100_000)
	if series*(inputBytes+viewBytes) <= share {
		t.Fatalf("the Slot holds %d at most, under its share %d: nothing to cut", series*(inputBytes+viewBytes), share)
	}
	names := make([]string, series)
	for index := range names {
		names[index] = fmt.Sprintf("series-%02d", index)
		fixture.streamInput(t, names[index], inputBytes)
	}
	twoInHand := false
	for index, name := range names {
		fixture.mergeSeries(t, index, name, viewBytes)
		if held := RetainedReserved(coordinator); held > share {
			t.Fatalf("after %s the pool holds %d, over the share %d", name, held, share)
		}
		pipe := stream.windows.pipe
		twoInHand = twoInHand || (pipe != nil && pipe.inflight > 0 && len(stream.stateItems) > 0)
	}
	if err := coordinator.applyWindow(context.Background(), stream, true); err != nil {
		t.Fatal(err)
	}
	if !twoInHand {
		t.Fatal("no window was filled while another was written")
	}
	if most := sampled.most.Load(); most > share {
		t.Fatalf("as a window was written the pool held %d, over the share %d", most, share)
	}
	written := map[execution.StateKeyIdentity]int{}
	for _, key := range fixture.ports.stateApplied {
		written[key]++
	}
	if len(written) != series {
		t.Fatalf("State written for %d keys, want %d", len(written), series)
	}
	for key, times := range written {
		if times != 1 {
			t.Fatalf("key %s written %d times", key.SeriesIdentityDigest, times)
		}
	}
}

// With four tenths of the share held outside its results, a Slot whose
// windows are written while the next is filled never holds more than its
// share, and the bound of the window being filled is the same before the
// window in flight lands as after (014 next9 4, 3.5): what that window gives
// back was taken off what is outside as it was cut, so its landing moves
// nothing.
func TestTheBoundOfTheWindowFilledDoesNotMoveAsTheWindowBeforeLands(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream, coordinator := fixture.stream, fixture.stream.coordinator
	ctx := context.Background()
	share := coordinator.qgShareBytes()
	outside := share * 2 / 5
	if err := coordinator.holdRetained(stream, outside, "frames"); err != nil {
		t.Fatal(err)
	}
	sampled := &sampledState{StateStore: coordinator.ports.State, coordinator: coordinator}
	coordinator.ports.State = sampled
	const series = 12
	inputBytes, viewBytes := uint64(1_000), share/40
	names := make([]string, series)
	for index := range names {
		names[index] = fmt.Sprintf("series-%02d", index)
		fixture.streamInput(t, names[index], inputBytes)
	}
	landed := 0
	for index, name := range names {
		fixture.mergeSeries(t, index, name, viewBytes)
		if held := RetainedReserved(coordinator); held > share {
			t.Fatalf("after %s the pool holds %d, over the share %d", name, held, share)
		}
		if pipe := stream.windows.pipe; pipe == nil || pipe.inflight == 0 {
			continue
		}
		before := stream.windowBound()
		if err := stream.drainApply(ctx); err != nil {
			t.Fatal(err)
		}
		if after := stream.windowBound(); after != before {
			t.Fatalf("after %s the bound of the window filled was %d with the window before in flight and %d once it landed",
				name, before, after)
		}
		landed++
	}
	if err := coordinator.applyWindow(ctx, stream, true); err != nil {
		t.Fatal(err)
	}
	if landed == 0 {
		t.Fatal("no window was in flight as the next was filled")
	}
	if most := sampled.most.Load(); most > share {
		t.Fatalf("as a window was written the pool held %d, over the share %d", most, share)
	}
	coordinator.letGoRetained(stream, outside)
}

// A task the apply goroutine is handed after one that failed is not run: it
// comes back with the failure, so nothing of a window after the one that
// failed is written whenever the Slot's goroutine happened to hand it - a
// landing that came back just after the handover took the landings there
// leaves the select free to hand the next task over (014 next9 4).
func TestATaskHandedAfterOneThatFailedIsNotRun(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	pipe := stream.applyPipeline(context.Background())
	failure := errors.New("the first task fails")
	ran := false
	pipe.seq++
	pipe.tasks <- applyTask{seq: pipe.seq, run: func(context.Context) error { return failure }}
	pipe.seq++
	pipe.tasks <- applyTask{seq: pipe.seq, run: func(context.Context) error {
		ran = true
		return nil
	}}
	pipe.inflight += 2
	if err := stream.drainApply(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("drain = %v, want the first task's failure", err)
	}
	if ran {
		t.Fatal("a task handed after one that failed was run")
	}
}

// A frame the apply side's half of the share refuses while a window is
// written is refused without asking the Slot to let its reads ahead go (014
// next9 4): they are outside that half, and letting them go makes it no
// room. The read ahead of a Slot near its share stays, no yield is counted,
// and the frame is refused - only not kept.
func TestAFrameTheApplySidesHalfRefusesLetsNoReadAheadGo(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream := fixture.stream
	coordinator := stream.coordinator
	share := coordinator.qgShareBytes()
	pipe := stream.applyPipeline(context.Background())
	done := make(chan struct{})
	close(done)
	held := share / 8
	ahead := &sliceReadAhead{coordinator: coordinator, stream: stream, counter: &stream.aheadBytes, leaf: inputslice.Leaf{ID: 2},
		limit: 1 << 20, cancel: func() {}, done: done, held: held}
	ahead.changed = sync.NewCond(&ahead.mu)
	stream.aheads = []*sliceReadAhead{ahead}
	stream.aheadBytes.Store(held)
	coordinator.reservations.mu.Lock()
	coordinator.reservations.retainedBytes += held
	coordinator.reservations.mu.Unlock()
	// The window written gives half the share back as it lands.
	stream.inflightHeld.Store(share / 2)
	pipe.writingWindow.Store(true)
	refused := make(chan error, 1)
	go func() { refused <- coordinator.holdRetained(pipe, 1, "frames") }()
serve:
	for {
		select {
		case request := <-pipe.yields:
			stream.serveYield(request)
		case err := <-refused:
			if err == nil {
				t.Fatal("a frame past the apply side's half of the share was kept")
			}
			break serve
		case <-testwait.Expired(t):
			t.Fatal("the frame's hold never returned")
		}
	}
	if len(stream.aheads) != 1 || stream.aheads[0] != ahead || stream.readAheadEnds[readAheadYielded] != 0 ||
		stream.readAheadYieldedBytes != 0 {
		t.Fatalf("reads ahead %d, yielded %d of %d bytes: want the read ahead kept", len(stream.aheads),
			stream.readAheadEnds[readAheadYielded], stream.readAheadYieldedBytes)
	}
	pipe.writingWindow.Store(false)
	stream.inflightHeld.Store(0)
	stream.aheads = nil
	coordinator.reservations.mu.Lock()
	coordinator.reservations.retainedBytes -= held
	coordinator.reservations.mu.Unlock()
}

// A window that leaves keys unwritten - here its Plan held back after the
// first window - gives back their witnesses as it lands, as the window bound
// took them off what is outside as it was cut: the bound of the window being
// filled does not move as it lands, as it does not for one that wrote every
// key (014 next9, ④ §7 item 4; 3.5's estimate of the witnesses, before,
// shrank it by a witness a key unwritten, kept to the Slot's end). The share
// holds: the pool never holds more than it after any merge or landing.
func TestAWindowLeavingKeysUnwrittenKeepsTheBoundAsItLandsAndTheShareHolds(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	stream, coordinator := fixture.stream, fixture.stream.coordinator
	ctx := context.Background()
	share := coordinator.qgShareBytes()
	// Four tenths of the share held outside the results, so the windows are
	// cut by their bytes.
	outside := share * 2 / 5
	if err := coordinator.holdRetained(stream, outside, "frames"); err != nil {
		t.Fatal(err)
	}
	const series = 12
	inputBytes, viewBytes := uint64(1_000), share/40
	names := make([]string, series)
	for index := range names {
		names[index] = fmt.Sprintf("series-%02d", index)
		fixture.streamInput(t, names[index], inputBytes)
	}
	withheld := 0
	for index, name := range names {
		fixture.mergeSeries(t, index, name, viewBytes)
		if held := RetainedReserved(coordinator); held > share {
			t.Fatalf("after %s the pool holds %d, over the share %d", name, held, share)
		}
		if pipe := stream.windows.pipe; pipe == nil || pipe.inflight == 0 {
			continue
		}
		before := stream.windowBound()
		if err := stream.drainApply(ctx); err != nil {
			t.Fatal(err)
		}
		after := stream.windowBound()
		if stream.windows.blocked == nil {
			// The first window written whole, its Plan is held back from the
			// next on: set between two windows, the apply side idle.
			if after != before {
				t.Fatalf("a window that wrote every key moved the bound from %d to %d as it landed", before, after)
			}
			stream.windows.blocked = map[execution.PlanIdentity]struct{}{fixture.due.Identity: {}}
			continue
		}
		if after != before {
			t.Fatalf("after %s the bound of the window filled was %d with the window before in flight and %d once it landed: "+
				"want it unmoved, the keys it left unwritten giving their witnesses back", name, before, after)
		}
		withheld++
		if held := RetainedReserved(coordinator); held > share {
			t.Fatalf("once the window before %s landed the pool holds %d, over the share %d", name, held, share)
		}
	}
	if err := coordinator.applyWindow(ctx, stream, true); err != nil {
		t.Fatal(err)
	}
	coordinator.letGoRetained(stream, outside)
	if withheld == 0 {
		t.Fatal("fixture: no window of the held-back Plan landed while the next was filled")
	}
	if written := len(fixture.ports.stateApplied); written == 0 || written >= series {
		t.Fatalf("State written for %d of %d keys, want the first window's alone", written, series)
	}
}
