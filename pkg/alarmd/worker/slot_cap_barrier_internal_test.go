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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// A merge of one of two consumers past the Slot's cap on State mutations -
// the other consumer's window, handed and still being written, holding the
// cap - is judged again behind the Slot's barrier, and admitted once that
// window is written and has given its counts back (015 C1 section 6, C1).
// The other consumer stops for the barrier at its next safe point, landing
// its window first; or, reading its next slice outside a completion, it is
// counted as stopped, and its window is written ahead of the fence the
// barrier hands. Judged on the counts in hand the merge is past the cap: the
// barrier is asked for once, with the window still held.
func TestAMergePastTheSlotsCapIsAdmittedBehindTheBarrierOnceTheOtherConsumersWindowIsWritten(t *testing.T) {
	for _, other := range []struct {
		name string
		// stop is what the other consumer does once the merge holds the
		// barrier, on its own goroutine.
		stop func(ctx context.Context, stream *streamedExecution) error
		// fenced: the barrier's fence is handed before the window is let go.
		fenced bool
	}{
		{name: "at its next safe point", stop: func(ctx context.Context, stream *streamedExecution) error {
			return stream.safePoint(ctx)
		}},
		{name: "reading its next slice", fenced: true, stop: func(_ context.Context, stream *streamedExecution) error {
			stream.leaveCompletion()
			return nil
		}},
	} {
		t.Run(other.name, func(t *testing.T) {
			fixture := newWindowedFixture(t)
			applySerially.Store(false)
			dispatch := fixture.stream
			co := dispatch.coordinator
			co.budget.MaxStateMutations = 1
			held := &heldWindowState{StateStore: co.ports.State, entered: make(chan struct{}), released: make(chan struct{})}
			co.ports.State = held
			// The head is the dispatch's here; a consumer's first cut finds the
			// windows started (startWindows).
			fixture.startWindows(t)
			streams := sideBySideStreams(dispatch, 2, nil, nil)
			first, second := streams[0], streams[1]
			ctx, cancel := context.WithCancel(testwait.Context(t))
			release := sync.OnceFunc(func() { close(held.released) })
			var running sync.WaitGroup
			// Before the fixture joins the apply goroutine: every wait here ends.
			t.Cleanup(func() {
				cancel()
				release()
				running.Wait()
			})
			for _, stream := range streams {
				if err := stream.enterCompletion(ctx); err != nil {
					t.Fatal(err)
				}
			}

			// The first consumer's window: one series, the cap, handed and held
			// as it is written.
			if err := fixture.mergeSeriesOn(ctx, first, 0, "series-1", 100); err != nil {
				t.Fatal(err)
			}
			if err := first.cutWindow(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-held.entered:
			case <-testwait.Expired(t):
				t.Fatal("the first consumer's window was never written")
			}
			if inflight := first.inflightEffects(); inflight.states != 1 {
				t.Fatalf("the first consumer's window in flight holds %d State mutations, want the cap's 1", inflight.states)
			}

			var asked atomic.Int32
			note := func() { asked.Add(1) }
			barrierAskedForTest.Store(&note)
			t.Cleanup(func() { barrierAskedForTest.Store(nil) })
			insideAtHold := holdsAlone(t)
			barriersBefore := barriersOf(dispatch)
			merged := make(chan error, 1)
			running.Add(1)
			go func() {
				defer running.Done()
				merged <- fixture.mergeSeriesOn(ctx, second, 0, "series-2", 100)
			}()
			testwait.Until(t, "the second consumer's merge past the cap holding the barrier", func() bool {
				second.barrier.mu.Lock()
				defer second.barrier.mu.Unlock()
				return second.barrier.holder == second
			})
			if got := asked.Load(); got != 1 {
				t.Fatalf("the merge past the cap asked for the barrier %d times, want once", got)
			}
			select {
			case err := <-merged:
				t.Fatalf("the merge past the cap returned (%v) with the first consumer still in its completion", err)
			default:
			}

			stopped := make(chan error, 1)
			running.Add(1)
			go func() {
				defer running.Done()
				stopped <- other.stop(ctx, first)
			}()
			if other.fenced {
				pipe := dispatch.windows.pipe
				testwait.Until(t, "the barrier's fence handed behind the window", func() bool {
					return pipe.lane(second.producer) != nil || len(merged) != 0
				})
				select {
				case err := <-merged:
					t.Fatalf("the merge past the cap returned (%v) before the window in flight was written", err)
				default:
				}
			}
			release()
			select {
			case err := <-merged:
				if err != nil {
					t.Fatalf("the merge past the cap behind the barrier = %v, want it admitted once the window was written", err)
				}
			case <-testwait.Expired(t):
				t.Fatal("the merge behind the barrier never returned")
			}
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-testwait.Expired(t):
				t.Fatal("the first consumer never went on")
			}

			if got := barriersOf(dispatch) - barriersBefore; got != 1 {
				t.Fatalf("the Slot held %d barriers for the merge, want 1", got)
			}
			if got := asked.Load(); got != 1 {
				t.Fatalf("the barrier was asked for %d times in all, want once", got)
			}
			if most := insideAtHold.Load(); most != 1 {
				t.Fatalf("a consumer had the barrier with %d consumers in a completion and not stopped, want itself alone", most)
			}
			if held, spent := dispatch.heldEffects(), spentEffectsOf(dispatch); held.states != 1 || spent.states != 1 {
				t.Fatalf("the Slot holds %d State mutations and spent %d, want the second consumer's 1 in hand and the first's window's 1 spent",
					held.states, spent.states)
			}
			if own := first.ownHeldEffects(); own.states != 0 {
				t.Fatalf("the first consumer holds %d State mutations once its window was written, want none", own.states)
			}
			if own := second.ownHeldEffects(); own.states != 1 {
				t.Fatalf("the second consumer holds %d State mutations, want the 1 it merged", own.states)
			}
		})
	}
}

// A merge still past the Slot's cap behind the barrier - the other
// consumer's window in hand, not yet cut, holding the cap - has the Slot's
// results replaced behind a barrier of its own (015 C1 section 6, C3): the
// Plan answered UNAVAILABLE, and every consumer's counts let go with the
// Slot's, so the window the other consumer goes on to cut gives back none
// the pool was given back already, and writes nothing of the Plan. Left
// there, that consumer's part would be given back a second time as its
// window is written, and the pool's count would wrap. Whether or not the
// Slot's windows had started, as a replacement of a Slot read whole or of
// one applied window by window.
func TestAMergeStillPastTheCapBehindTheBarrierReplacesTheResultsOfEveryConsumer(t *testing.T) {
	for _, windows := range []struct {
		name    string
		started bool
	}{{"no window yet", false}, {"windows started", true}} {
		t.Run(windows.name, func(t *testing.T) {
			run := replacePastTheCap(t, windows.started, 10)
			fixture, first := run.fixture, run.first
			dispatch, co := fixture.stream, fixture.stream.coordinator
			if !run.replaced || run.replaceErr != nil {
				t.Fatalf("the Slot's results replaced=%v err=%v, want them replaced", run.replaced, run.replaceErr)
			}
			if run.barriers != 2 {
				t.Fatalf("the Slot held %d barriers, want 2: the merge judged again, the results replaced", run.barriers)
			}
			if own := first.ownHeldEffects(); own != (effectCounts{}) {
				t.Fatalf("the first consumer still holds %+v of the Slot's counts once its results were replaced, want none", own)
			}
			if windows.started {
				if _, gone := dispatch.windows.replaced[fixture.due.Identity]; !gone {
					t.Fatal("the Plan in progress is not among the ones replaced")
				}
			}

			// The first consumer goes on: its window, cut and written.
			if err := first.cutWindow(run.ctx); err != nil {
				t.Fatal(err)
			}
			if err := first.drainApply(run.ctx); err != nil {
				t.Fatal(err)
			}
			co.reservations.mu.Lock()
			states, events := co.reservations.states, co.reservations.events
			co.reservations.mu.Unlock()
			if states != 0 || events != 0 {
				t.Fatalf("the pool holds %d State mutations and %d events once the replaced Slot's windows are written, want none", states, events)
			}
			if written := slices.ContainsFunc(fixture.ports.trace, func(call string) bool {
				return strings.HasPrefix(call, "state:series-1") || strings.HasPrefix(call, "event:event-series-1")
			}); written {
				t.Fatalf("the replaced Plan's series was written by the window cut after: %v", fixture.ports.trace)
			}
		})
	}
}

// The replacement's own merge past a cap - its gap more than the Slot may
// write - is judged behind the barrier the replacement holds, which the
// merge asks for again: held already, it is held once, and let go when the
// replacement is done, which fails on the cap as it does with one stream.
// Taken again, it would be let go by the merge, the other consumers going on
// while the Slot's results are still replaced.
func TestAReplacementPastTheCapIsJudgedBehindTheBarrierItHolds(t *testing.T) {
	run := replacePastTheCap(t, false, 0)
	var exceeded *provisionalBudgetExceededError
	if !run.replaced || !errors.As(run.replaceErr, &exceeded) || !exceeded.slot {
		t.Fatalf("the replacement past the gap cap replaced=%v err=%v, want it judged past the Slot's cap", run.replaced, run.replaceErr)
	}
	if run.barriers != 2 {
		t.Fatalf("the Slot held %d barriers, want 2: the merge judged again, the results replaced", run.barriers)
	}
	barrier := &run.second.barrier
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.holder != nil {
		t.Fatal("the barrier is still held once the replacement returned")
	}
}

// capReplacement is a Slot of two consumers past its cap of one State
// mutation (replacePastTheCap): what the second consumer's merge and its
// replacement came back with, and the barriers the Slot held for them.
type capReplacement struct {
	fixture              *windowedFixture
	first, second        *streamedExecution
	ctx                  context.Context
	replaced             bool
	mergeErr, replaceErr error
	barriers             uint64
}

// replacePastTheCap runs a Slot of two consumers past its cap of one State
// mutation, its gap cap gapCap, its windows started or not: the first
// consumer's window in hand holding the cap, the second's merge judged
// again behind the barrier and, past the cap still, answered by the
// replacement as its completion does (replaceBeyondSlotBudget), the first
// stopping at its safe points meanwhile.
func replacePastTheCap(t *testing.T, started bool, gapCap uint64) *capReplacement {
	t.Helper()
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	dispatch := fixture.stream
	co := dispatch.coordinator
	co.budget.MaxStateMutations, co.budget.MaxGapMutations = 1, gapCap
	if started {
		fixture.startWindows(t)
	}
	streams := sideBySideStreams(dispatch, 2, nil, nil)
	run := &capReplacement{fixture: fixture, first: streams[0], second: streams[1]}
	ctx, cancel := context.WithCancel(testwait.Context(t))
	run.ctx = ctx
	var running sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		running.Wait()
	})
	for _, stream := range streams {
		if err := stream.enterCompletion(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The first consumer's window in hand: one series, the cap.
	if err := fixture.mergeSeriesOn(ctx, run.first, 0, "series-1", 100); err != nil {
		t.Fatal(err)
	}
	before := barriersOf(dispatch)
	insideAtHold := holdsAlone(t)
	done := make(chan struct{})
	stopped := make(chan error, 1)
	running.Add(1)
	go func() {
		defer running.Done()
		stopped <- stopsAtSafePoints(ctx, run.first, done)
	}()
	judged := make(chan struct{})
	running.Add(1)
	go func() {
		defer running.Done()
		defer close(judged)
		run.mergeErr = fixture.mergeSeriesOn(ctx, run.second, 0, "series-2", 100)
		run.replaced, run.replaceErr = run.second.replaceBeyondSlotBudget(ctx, run.mergeErr)
	}()
	select {
	case <-judged:
	case <-testwait.Expired(t):
		t.Fatal("the merge past the cap and its replacement never returned")
	}
	close(done)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	var exceeded *provisionalBudgetExceededError
	if !errors.As(run.mergeErr, &exceeded) || !exceeded.slot {
		t.Fatalf("the merge judged behind the barrier with the other consumer's window in hand = %v, want it past the Slot's cap", run.mergeErr)
	}
	run.barriers = barriersOf(dispatch) - before
	if most := insideAtHold.Load(); most != 1 {
		t.Fatalf("a consumer had the barrier with %d consumers in a completion and not stopped, want itself alone", most)
	}
	return run
}

// holdsAlone has every barrier the case's consumers hold say how many were in
// a completion and not stopped as it was had (barrierHeldForTest), and
// returns the most it said: one when the holder had it alone each time.
func holdsAlone(t *testing.T) *atomic.Int32 {
	t.Helper()
	var most atomic.Int32
	note := func(inside int) {
		for {
			seen := most.Load()
			if int32(inside) <= seen || most.CompareAndSwap(seen, int32(inside)) {
				return
			}
		}
	}
	barrierHeldForTest.Store(&note)
	t.Cleanup(func() { barrierHeldForTest.Store(nil) })
	return &most
}

// Before the Slot finishes, every consumer's windows are written and landed
// (015 C1 section 6, the fourth): the finish merges on the dispatch against
// the Slot's counts, which a consumer's window still being written holds its
// part of. While it is written the dispatch's merge is past the cap; once the
// consumers have landed it is admitted, as one stream's is once its own
// windows have.
func TestTheDispatchFinishesOnceEveryConsumersWindowsHaveLanded(t *testing.T) {
	fixture := newWindowedFixture(t)
	applySerially.Store(false)
	dispatch := fixture.stream
	co := dispatch.coordinator
	co.budget.MaxStateMutations = 1
	held := &heldWindowState{StateStore: co.ports.State, entered: make(chan struct{}), released: make(chan struct{})}
	co.ports.State = held
	fixture.startWindows(t)
	first := sideBySideStreams(dispatch, 2, nil, nil)[0]
	ctx := testwait.Context(t)
	release := sync.OnceFunc(func() { close(held.released) })
	t.Cleanup(release)
	if err := first.enterCompletion(ctx); err != nil {
		t.Fatal(err)
	}
	// The consumer's last slice: its window handed and held as it is written,
	// the consumer out of its completion and ended.
	if err := fixture.mergeSeriesOn(ctx, first, 0, "series-1", 100); err != nil {
		t.Fatal(err)
	}
	if err := first.cutWindow(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-held.entered:
	case <-testwait.Expired(t):
		t.Fatal("the consumer's window was never written")
	}
	first.leaveCompletion()

	finishMerge := func() error { return fixture.mergeSeriesOn(ctx, dispatch, 0, "series-2", 100) }
	var exceeded *provisionalBudgetExceededError
	if err := finishMerge(); !errors.As(err, &exceeded) || !exceeded.slot {
		t.Fatalf("the dispatch's merge with the consumer's window still written = %v, want it past the Slot's cap", err)
	}
	release()
	if err := dispatch.landConsumers(ctx); err != nil {
		t.Fatal(err)
	}
	if inflight := first.inflightEffects(); inflight != (effectCounts{}) {
		t.Fatalf("the consumer's windows hold %+v in flight once the consumers have landed, want none", inflight)
	}
	if err := finishMerge(); err != nil {
		t.Fatalf("the dispatch's merge once the consumers have landed = %v, want it admitted", err)
	}
}

// stopsAtSafePoints has stream stop at a safe point each time another
// consumer holds the Slot's barrier, as a consumer between two merges does,
// until done.
func stopsAtSafePoints(ctx context.Context, stream *streamedExecution, done <-chan struct{}) error {
	barrier := &stream.barrier
	for {
		barrier.mu.Lock()
		for barrier.holder == nil || barrier.holder == stream {
			if barrier.changed == nil {
				barrier.changed = make(chan struct{})
			}
			changed := barrier.changed
			barrier.mu.Unlock()
			select {
			case <-changed:
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
			barrier.mu.Lock()
		}
		barrier.mu.Unlock()
		if err := stream.safePoint(ctx); err != nil {
			return err
		}
	}
}

// barriersOf is how many barriers the Slot has held.
func barriersOf(stream *streamedExecution) uint64 {
	stream.sharedMu.Lock()
	defer stream.sharedMu.Unlock()
	return stream.barriers
}

// spentEffectsOf is the counts the Slot's windows have written.
func spentEffectsOf(stream *streamedExecution) effectCounts {
	stream.effectsMu.Lock()
	defer stream.effectsMu.Unlock()
	return stream.effectsSpent
}
