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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The window pipeline (014 next9 4). A Slot applied window by window writes
// each window on an apply goroutine of its own, the Slot's goroutine filling
// the next window meanwhile: a window cut is handed over (handOver) and the
// Slot goes on evaluating; the apply goroutine writes the windows one at a
// time, in the order they were handed, and hands each back as it lands. The
// Slot's goroutine takes the landings at its safe points - before each merge,
// before a cut, at a slice's end, before the tail (takeLandings) - and gives
// back what each window held there (landWindow).
//
// One task at a time: the apply goroutine runs a window's writes, a slice's
// head or the tail, each its own Sequence, so the Slot holds at most one
// Sequence at any time, as before. A handover waits until the apply goroutine
// takes the task: at most one window being written and one being filled,
// which is what the window bound - half the share each (windowBound) - is for.
//
// What each side touches (n8-window-pipeline 3, 3.1):
//
//   - The Slot's goroutine: the evaluation and its results, the prepared
//     series and their inputs, retention and the read ahead, the present sets
//     and the no-data staging, first-seen, the effect counts, its clock.
//   - The apply goroutine: the slotApply - its activations, census, keys and
//     segment shadow - and the gap statements, blocked, landing and decided;
//     what it holds of its own (frames, first writes, segment joins) and its
//     clock, as the Slot's retained owner on its context (applyPipe).
//   - Across them, only three ways: a handover takes the window's own things
//     with it, and snapshots of what it reads of the Slot as it was cut
//     (takeWindow); a landing hands back what the window wrote and the
//     observations its applies made; and a drain waits until the apply
//     goroutine is idle, for the rare paths that touch both sides: a Slot
//     over its per-Slot caps, a slice's head with first-seen candidates, and
//     the tail.
//
// A landing's error stops the Slot at its next safe point; no window is
// handed after it. Every return of Execute joins the apply goroutine first
// (joinApply), and a panic on it is raised again on the Slot's goroutine.

// applyPipe is a Slot's apply goroutine and what passes between it and the
// Slot's goroutine.
type applyPipe struct {
	stream *streamedExecution
	// ctx is the apply goroutine's: the Slot's completion context, with this
	// pipe as the retained owner in place of the stream.
	ctx      context.Context
	tasks    chan applyTask
	landings chan applyResult
	yields   chan yieldRequest
	done     chan struct{}

	// The Slot's goroutine's alone.
	inflight int
	// inflightEffects is the State and event counts of the windows handed
	// and not yet landed.
	inflightEffects effectCounts
	// inflightRelease is what those windows give back when they land, by
	// phase as they were cut (windowTask.release), and at retainPhaseWitness
	// the witnesses among their state.
	inflightRelease [retainPhaseCount + 1]uint64
	seq             uint64
	failed          error
	panicked        *sideBySidePanic
	closed          bool

	// writingWindow is set while the apply goroutine writes a window
	// (judgeHeldLocked).
	writingWindow atomic.Bool
	// halfRefused is whether the last hold judgeHeldLocked refused was
	// refused by the half of the share a window written is held to: no read
	// ahead is in that half, so yieldHeld asks none to go for it.
	halfRefused atomic.Bool

	// The apply goroutine's, read by the Slot's once it has joined it.
	busy time.Duration

	// peak is the Slot's total, both sides, at the highest an apply-side
	// hold took it to, and peakByPhase its phases then, folded into the
	// Slot's at the join. What the apply side holds is the stream's
	// applyHeldBytes.
	peakMu      sync.Mutex
	peak        uint64
	peakByPhase [retainPhaseCount]uint64
}

// applyTask is one task of the apply goroutine: a window, or a slice's head
// or the tail. producer and seq order it: tasks of one producer run in the
// order it handed them. There is one producer, the Slot's goroutine.
type applyTask struct {
	producer, seq uint64
	window        *windowTask
	run           func(ctx context.Context) error
}

// applyResult is a task done: its window's landing, the error it stopped on,
// what it said, held for the Slot's goroutine to say (deferredObservations),
// and a panic it raised.
type applyResult struct {
	task         applyTask
	landing      windowLanding
	err          error
	observations []deferredObservation
	panicked     *sideBySidePanic
}

// yieldRequest asks the Slot's goroutine to let its reads ahead go for a
// reservation of the apply side's (yieldReadAhead): their state is the
// Slot's goroutine's.
type yieldRequest struct {
	phase string
	reply chan bool
}

// applySerially has every task handed wait until it has landed: the windows
// written one after another, the Slot's goroutine waiting on each, as before
// the pipeline. Set only by the cases that compare the two; production never
// sets it.
var applySerially atomic.Bool

// applyGoroutines is how many Slots' apply goroutines are running now.
var applyGoroutines atomic.Int64

// windowLandedForTest, set only by cases, is told what each window gave back
// by phase as it landed, on the Slot's goroutine.
var windowLandedForTest atomic.Pointer[func(given [retainPhaseCount]uint64)]

// applyPipeline is the Slot's apply goroutine, started at the first
// handover on the Slot's completion context.
func (stream *streamedExecution) applyPipeline(ctx context.Context) *applyPipe {
	if pipe := stream.windows.pipe; pipe != nil {
		return pipe
	}
	base := stream.applyCtx
	if base == nil {
		base = ctx
	}
	pipe := &applyPipe{stream: stream, tasks: make(chan applyTask), landings: make(chan applyResult, 4),
		yields: make(chan yieldRequest), done: make(chan struct{})}
	pipe.ctx = context.WithValue(base, retainedOwnerKey{}, retainedOwner(pipe))
	stream.windows.pipe = pipe
	applyGoroutines.Add(1)
	go pipe.run()
	return pipe
}

func (pipe *applyPipe) run() {
	defer close(pipe.done)
	defer applyGoroutines.Add(-1)
	var last [1]uint64
	// stopped is the first failure a task came back with. A task handed
	// after it is not run and comes back with it: nothing of a window after
	// the one that failed is written, whenever the Slot's goroutine handed
	// it - a landing that comes back between the handover taking the
	// landings there and its select leaves the select free to hand the next
	// task over.
	var stopped error
	for task := range pipe.tasks {
		if task.seq <= last[task.producer] {
			panic("alarmd worker: an apply task ran out of its producer's order")
		}
		last[task.producer] = task.seq
		if stopped != nil {
			pipe.landings <- applyResult{task: task, err: stopped}
			continue
		}
		result := pipe.execute(task)
		switch {
		case result.panicked != nil:
			stopped = result.panicked
		case result.err != nil:
			stopped = result.err
		}
		pipe.landings <- result
	}
}

// execute runs one task on the apply goroutine.
func (pipe *applyPipe) execute(task applyTask) (result applyResult) {
	result.task = task
	started := time.Now()
	observations := &deferredObservations{}
	defer func() {
		pipe.busy += time.Since(started)
		if recovered := recover(); recovered != nil {
			result.panicked = &sideBySidePanic{value: recovered, stack: debug.Stack()}
		}
	}()
	ctx := withDeferredObservations(pipe.ctx, observations)
	if task.window != nil {
		pipe.writingWindow.Store(true)
		defer pipe.writingWindow.Store(false)
		result.landing = pipe.stream.coordinator.writeWindow(ctx, pipe.stream, task.window)
		result.err = result.landing.err
	} else {
		result.err = task.run(ctx)
	}
	observations.mu.Lock()
	result.observations = observations.said
	observations.mu.Unlock()
	return result
}

// handOver hands a task to the apply goroutine once it has taken every
// landing back that is there, and waits until the apply goroutine takes it:
// it is idle then, the task before landed or landing. It refuses once a
// landing came back with an error.
func (stream *streamedExecution) handOver(ctx context.Context, task applyTask) error {
	pipe := stream.applyPipeline(ctx)
	if err := stream.takeLandings(ctx); err != nil {
		return err
	}
	pipe.seq++
	task.seq = pipe.seq
	leave := stream.enter(slotPhaseApplyWait)
	defer leave()
	for {
		select {
		case pipe.tasks <- task:
			pipe.inflight++
			if window := task.window; window != nil {
				pipe.inflightEffects.states += window.effects.states
				pipe.inflightEffects.events += window.effects.events
				for phase, bytes := range window.release {
					pipe.inflightRelease[phase] += bytes
					pipe.stream.inflightHeld.Add(bytes)
				}
				pipe.inflightRelease[retainPhaseWitness] += window.release[retainPhaseState] - min(window.stateBytes, window.release[retainPhaseState])
			}
			if applySerially.Load() {
				return stream.drainApply(ctx)
			}
			return nil
		case request := <-pipe.yields:
			stream.serveYield(request)
		case result := <-pipe.landings:
			stream.landResult(ctx, result)
			if pipe.failed != nil {
				return pipe.failed
			}
		}
	}
}

// takeLandings takes back every landing that is there, not waiting for one,
// and returns the first error a landing came back with.
func (stream *streamedExecution) takeLandings(ctx context.Context) error {
	pipe := stream.windows.pipe
	if pipe == nil {
		return nil
	}
	for {
		select {
		case result := <-pipe.landings:
			stream.landResult(ctx, result)
		case request := <-pipe.yields:
			stream.serveYield(request)
		default:
			return pipe.failed
		}
	}
}

// drainApply waits until every task handed has landed, and returns the first
// error a landing came back with: the apply goroutine is idle then, and the
// Slot's goroutine may touch what is the apply side's.
func (stream *streamedExecution) drainApply(ctx context.Context) error {
	pipe := stream.windows.pipe
	if pipe == nil {
		return nil
	}
	if pipe.inflight > 0 {
		leave := stream.enter(slotPhaseApplyWait)
		for pipe.inflight > 0 {
			select {
			case result := <-pipe.landings:
				stream.landResult(ctx, result)
			case request := <-pipe.yields:
				stream.serveYield(request)
			}
		}
		leave()
	}
	return pipe.failed
}

// landResult takes one task's result back on the Slot's goroutine: the
// observations its applies made are said, and a window gives back what it
// held (landWindow), whatever its error.
func (stream *streamedExecution) landResult(ctx context.Context, result applyResult) {
	pipe := stream.windows.pipe
	pipe.inflight--
	defer stream.enter(slotPhaseLand)()
	stream.coordinator.sayDeferred(result.observations)
	err := result.err
	if result.panicked != nil {
		if pipe.panicked == nil {
			pipe.panicked = result.panicked
		}
		err = result.panicked
	}
	if window := result.task.window; window != nil {
		pipe.inflightEffects.states -= window.effects.states
		pipe.inflightEffects.events -= window.effects.events
		for phase, bytes := range window.release {
			pipe.inflightRelease[phase] -= bytes
			pipe.stream.inflightHeld.Add(^(bytes - 1))
		}
		pipe.inflightRelease[retainPhaseWitness] -= window.release[retainPhaseState] - min(window.stateBytes, window.release[retainPhaseState])
		landing := result.landing
		landing.task, landing.err = window, err
		err = stream.landWindow(ctx, landing)
	}
	if err != nil && pipe.failed == nil {
		pipe.failed = err
	}
}

// serveYield lets the Slot's reads ahead go for the apply side, on the Slot's
// goroutine.
func (stream *streamedExecution) serveYield(request yieldRequest) {
	request.reply <- stream.yieldReadAhead(request.phase)
}

// joinApply ends the Slot's apply goroutine: no task is handed after it, the
// one running returns, every landing is taken back - what each window held is
// given back - and what the apply side holds and its peak are folded into the
// Slot's. Every return of Execute runs it before anything else of the Slot is
// given back (registered after cancelCompletion), and the paths that read
// the Slot's usage and timing run it first. A panic on the apply goroutine is
// raised again here.
func (stream *streamedExecution) joinApply(ctx context.Context) {
	pipe := stream.windows.pipe
	if pipe == nil || pipe.closed {
		return
	}
	pipe.closed = true
	close(pipe.tasks)
	for ended := false; !ended; {
		select {
		case result := <-pipe.landings:
			stream.landResult(ctx, result)
		case request := <-pipe.yields:
			stream.serveYield(request)
		case <-pipe.done:
			ended = true
		}
	}
	for taken := false; !taken; {
		select {
		case result := <-pipe.landings:
			stream.landResult(ctx, result)
		default:
			taken = true
		}
	}
	if held := pipe.stream.applyHeldBytes.Swap(0); held > 0 {
		stream.retainedByPhase[retainPhaseState] += held
		stream.retainedNow[retainPhaseState].Add(held)
	}
	if pipe.peak > stream.peakRetained {
		stream.peakRetained, stream.peakByPhase = pipe.peak, pipe.peakByPhase
	}
	if pipe.panicked != nil {
		panic(pipe.panicked)
	}
}

// applyBusy is how long the apply goroutine ran tasks, once joined.
func (stream *streamedExecution) applyBusy() time.Duration {
	if pipe := stream.windows.pipe; pipe != nil && pipe.closed {
		return pipe.busy
	}
	return 0
}

// applyHeld is what the apply side holds of the Slot's share now.
func (stream *streamedExecution) applyHeld() uint64 {
	return stream.applyHeldBytes.Load()
}

// retainedOwner is what holds the bytes kept further down a Slot's context,
// and whose clock their time is charged to: the Slot's stream, or, on its
// apply goroutine, its apply pipe.
type retainedOwner interface {
	enter(phase slotPhase) (leave func())
	slotSegments() *segmentSlot
	// heldTotal is what the owner's Slot holds now, both sides.
	heldTotal() uint64
	// judgeHeldLocked judges bytes more held, under reservations.mu: against
	// the Slot's share, then the pool (judgeRetainedLocked).
	judgeHeldLocked(bytes uint64, phase string) error
	// yieldHeld lets the Slot's reads ahead go for a refused hold, reporting
	// whether that may let it through (yieldReadAhead).
	yieldHeld(phase string) bool
	// keepHeldLocked counts, under reservations.mu, bytes the pool just took
	// for the owner where the other side of its Slot judges by; keepHeld
	// counts them after the lock. letGoHeld gives them back to the pool and
	// from the owner's count.
	keepHeldLocked(bytes uint64)
	keepHeld(bytes uint64)
	letGoHeld(bytes uint64)
}

func (stream *streamedExecution) slotSegments() *segmentSlot {
	if stream == nil {
		return nil
	}
	return stream.segments
}

func (stream *streamedExecution) heldTotal() uint64 { return stream.retainedTotal() }

func (stream *streamedExecution) judgeHeldLocked(bytes uint64, phase string) error {
	return stream.coordinator.judgeRetainedLocked(stream, bytes, phase)
}

func (stream *streamedExecution) yieldHeld(phase string) bool { return stream.yieldReadAhead(phase) }

// keepHeldLocked counts nothing under the lock: the Slot's side counts what it
// holds after it (keepHeld, retainBytes), as its own reservations do, so the
// apply side, judging the two sides' sum, may miss the Slot's latest hold -
// by one reservation at most, the pool exact.
func (stream *streamedExecution) keepHeldLocked(uint64) {}

func (stream *streamedExecution) keepHeld(bytes uint64) { stream.retainBytes(retainPhaseState, bytes) }

func (stream *streamedExecution) letGoHeld(bytes uint64) { stream.unretain(retainPhaseState, bytes) }

// The apply pipe's own clock is the task it runs (applyPipe.busy): the
// phases inside a task are the apply's, not the Slot's partition.
func (pipe *applyPipe) enter(slotPhase) (leave func()) { return func() {} }

func (pipe *applyPipe) slotSegments() *segmentSlot { return pipe.stream.segments }

// heldTotal reads the Slot's total from the atomic copies of its phases, the
// apply side's goroutine never reading the Slot's own fields.
func (pipe *applyPipe) heldTotal() uint64 {
	stream := pipe.stream
	total := stream.aheadBytes.Load() + pipe.stream.applyHeldBytes.Load()
	for phase := range stream.retainedNow {
		total += stream.retainedNow[phase].Load()
	}
	return total
}

// judgeHeldLocked is judgeRetainedLocked for the apply side: the same share
// and pool, the Slot's total read from heldTotal. While it writes a window,
// what it holds is also kept to the half of the share that window was cut in:
// with what the window gives back as it lands, no more than half the share -
// the other half is the window being filled, whose merges the share would
// otherwise refuse for frames the write could do without (a frame refused is
// only encoded again by the write). The refusal carries no usage of the
// Slot's: that is the Slot's goroutine's to read.
func (pipe *applyPipe) judgeHeldLocked(bytes uint64, phase string) error {
	stream := pipe.stream
	coordinator := stream.coordinator
	total := pipe.heldTotal()
	share := coordinator.qgShareBytes()
	var err error
	half := false
	if share > 0 && bytes > 0 && total+bytes > share {
		err = shareRejection(phase, total, bytes, share, nil)
	} else if inHalf := pipe.stream.applyHeldBytes.Load() + pipe.stream.inflightHeld.Load() + bytes; share > 0 && bytes > 0 && pipe.writingWindow.Load() && inHalf > share/2 {
		err, half = shareRejection(phase, inHalf-bytes, bytes, share/2, nil), true
	} else if !coordinator.fitsLocked(bytes) {
		err = poolHeldByOthersRejection(phase, total, coordinator.reservations.retainedBytes, bytes,
			coordinator.budget.MaxRetainedBytes, nil)
	}
	pipe.halfRefused.Store(half)
	if err != nil {
		stream.aheadAtRefusal.Store(stream.aheadBytes.Load())
	}
	return err
}

// yieldHeld asks the Slot's goroutine to let its reads ahead go, and waits
// for it: it answers at its next safe point, or while it waits on the apply
// goroutine - a handover, a drain, the join, which serves the apply side
// until the apply goroutine has ended. A hold the half of the share a window
// written is held to refused asks nothing: the reads ahead are outside that
// half, and letting them go would make it no room.
func (pipe *applyPipe) yieldHeld(phase string) bool {
	if pipe.halfRefused.Load() {
		return false
	}
	reply := make(chan bool, 1)
	pipe.yields <- yieldRequest{phase: phase, reply: reply}
	return <-reply
}

// keepHeldLocked counts the apply side's hold where the Slot's side reads it
// (applyHeld), under the lock its reservations are judged under: the Slot's
// side never misses it.
func (pipe *applyPipe) keepHeldLocked(bytes uint64) { pipe.stream.applyHeldBytes.Add(bytes) }

func (pipe *applyPipe) keepHeld(bytes uint64) {
	total := pipe.heldTotal()
	pipe.peakMu.Lock()
	defer pipe.peakMu.Unlock()
	if total <= pipe.peak {
		return
	}
	pipe.peak = total
	stream := pipe.stream
	for phase := range stream.retainedNow {
		pipe.peakByPhase[phase] = stream.retainedNow[phase].Load()
	}
	pipe.peakByPhase[retainPhaseState] += pipe.stream.applyHeldBytes.Load()
}

func (pipe *applyPipe) letGoHeld(bytes uint64) {
	bytes = min(bytes, pipe.stream.applyHeldBytes.Load())
	if bytes == 0 {
		return
	}
	pipe.stream.applyHeldBytes.Add(^(bytes - 1))
	pipe.stream.coordinator.releaseProvisional(0, bytes)
}

// enterOwnerPhase charges the time until the returned leave to phase on the
// clock of the retained owner under ctx, if any.
func enterOwnerPhase(ctx context.Context, phase slotPhase) (leave func()) {
	if owner := retainedOwnerFrom(ctx); owner != nil {
		return owner.enter(phase)
	}
	return func() {}
}

type deferredObservationsKey struct{}

// deferredObservations is what a task on the apply goroutine said, in the
// order it said it, each with the context it was said under, held for the
// Slot's goroutine to say as it takes the task's landing.
//
// A line a series - a window's mutations compared, one each - is held as its
// duration alone: lines said one after another under one context that differ
// in nothing else are one entry and their durations (addPlain), a window of
// half a million series some 8 bytes a series rather than an Observation
// each, so what a window in flight holds of its lines is within what its
// series are charged for (loadedKeyHeldBytes). Each is said as it would have
// been, field by field, in the order it was made.
type deferredObservations struct {
	mu   sync.Mutex
	said []deferredObservation
}

type deferredObservation struct {
	ctx context.Context
	// observation is a line as it was said; nil for a run of plain lines,
	// plain and one duration each.
	observation *observability.Observation
	plain       plainObservation
	durations   []time.Duration
}

// plainObservation is a line coordinator.observe makes without an error: its
// fields but its duration, the defaults emitObservation gives filled in.
type plainObservation struct {
	component observability.Component
	stage     observability.Stage
	operation observability.Operation
	result    observability.Result
	reason    observability.ReasonCode
}

// deferredDurationHeldBytes is what a plain line held takes at most: its
// duration, twice over for the room an append leaves.
const deferredDurationHeldBytes = 2 * uint64(unsafe.Sizeof(time.Duration(0)))

// addPlain holds a plain line said under ctx, as its duration when the line
// held last is the same line under the same context.
func (deferred *deferredObservations) addPlain(ctx context.Context, plain plainObservation, took time.Duration) {
	deferred.mu.Lock()
	defer deferred.mu.Unlock()
	if last := len(deferred.said) - 1; last >= 0 {
		if entry := &deferred.said[last]; entry.observation == nil && entry.plain == plain && sameContext(entry.ctx, ctx) {
			entry.durations = append(entry.durations, took)
			return
		}
	}
	deferred.said = append(deferred.said, deferredObservation{ctx: ctx, plain: plain, durations: []time.Duration{took}})
}

// sameContext is whether a and b are the one context; a context of a type
// that cannot be compared is not.
func sameContext(a, b context.Context) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a == b
}

// withDeferredObservations has what is said under ctx held in observations
// rather than said: the observer is then only ever called from the Slot's
// goroutine, as before the window pipeline - a Query Group's cost account,
// which drops a line it cannot take its lock for at once, is never touched
// from two goroutines (observability cost summary); the hourly log sampling
// and the reason transitions see the Slot's lines in one order. Each is said
// as the Slot takes the landing back: its log line and the cost window it
// falls in are of then, its durations of when it was made.
func withDeferredObservations(ctx context.Context, observations *deferredObservations) context.Context {
	return context.WithValue(ctx, deferredObservationsKey{}, observations)
}

func deferredObservationsFrom(ctx context.Context) *deferredObservations {
	observations, _ := ctx.Value(deferredObservationsKey{}).(*deferredObservations)
	return observations
}

// deferringObserver is the coordinator's observer: it holds what is said
// under a context of a Slot's apply goroutine (withDeferredObservations) and
// says the rest.
type deferringObserver struct{ next observability.Observer }

func (observer deferringObserver) Observe(ctx context.Context, observation observability.Observation) {
	if deferred := deferredObservationsFrom(ctx); deferred != nil {
		deferred.mu.Lock()
		deferred.said = append(deferred.said, deferredObservation{ctx: ctx, observation: &observation})
		deferred.mu.Unlock()
		return
	}
	observer.next.Observe(ctx, observation)
}

// sayDeferred says what a task held, on the Slot's goroutine, each line as it
// was made; an observer panic never fails the Slot (emitObservation).
func (coordinator *SlotExecutionCoordinator) sayDeferred(said []deferredObservation) {
	observer, ok := coordinator.ports.Observer.(deferringObserver)
	if !ok {
		return
	}
	for _, entry := range said {
		if entry.observation != nil {
			sayRecovered(observer.next, entry.ctx, *entry.observation)
			continue
		}
		for _, took := range entry.durations {
			sayRecovered(observer.next, entry.ctx, observability.Observation{
				Component: entry.plain.component, Stage: entry.plain.stage, Result: entry.plain.result,
				Operation: entry.plain.operation, ReasonCode: entry.plain.reason, Duration: took, DurationKnown: true,
			})
		}
	}
}

func sayRecovered(observer observability.Observer, ctx context.Context, observation observability.Observation) {
	defer func() { _ = recover() }()
	observer.Observe(ctx, observation)
}
