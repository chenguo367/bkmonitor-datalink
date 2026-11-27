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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// sliceConsumersForTest, set only by cases and the alarmd_measure build
// (SetSliceConsumersForMeasurement), is how many streams consume the
// slices of a Slot read in slices side by side: k (015 C1). Unset, or one,
// the Slot's own stream reads and consumes them one at a time.
var sliceConsumersForTest atomic.Int32

// slicesBySubstreamForTest, set only by cases and the alarmd_measure build,
// is told how many slices each of the k consumers consumed, and how many
// barriers they held, as a Slot's consumers end.
var slicesBySubstreamForTest atomic.Pointer[func(consumed []int, barriers uint64)]

// sliceConsumerProcsForTest, set only by cases, is the cores a case asks
// sliceConsumersByCores for (sliceConsumers).
var sliceConsumerProcsForTest atomic.Int32

// sliceConsumers is k for this Slot: one, its own stream reading and
// consuming its slices one at a time, however many cores the process has.
// Two side by side (015 C1) each hold a share that what a consumer keeps to
// the Slot's end narrows, so slices small enough to read are let go and the
// splits they leave are remembered round after round (the 1.0 next release
// gate): until that is mended, k is more than one only where a case or the
// measurement build sets it (sliceConsumersForTest), or a case asks for a
// process's cores (sliceConsumerProcsForTest) - sliceConsumersByCores of
// them within the Slot's read width, k at most w - 1, a read of the width
// left to read ahead (015 C1 section 9) - and, for a Slot read in more than
// one slice, only once it has begun, its header saying the width: a Slot
// read in slices from its start reads its first on its own stream.
func (stream *streamedExecution) sliceConsumers() int {
	if !stream.readInSlices() {
		return 1
	}
	if k := int(sliceConsumersForTest.Load()); k > 0 {
		return k
	}
	procs := int(sliceConsumerProcsForTest.Load())
	if !stream.began || procs == 0 {
		return 1
	}
	width := stream.coordinator.sliceReadWidth(judgedRequests(stream.header))
	return max(1, min(sliceConsumersByCores(procs), width-1))
}

// sliceConsumersByCores is k by the process's cores, as the cells chose it
// (015 C1 section 12 item 2): two from four cores - at 500k
// series k = 2 took 21% off one Slot at four cores and 23% at eight, past
// the fifth the criterion asks, and six between them was not measured - and
// one below four, not measured either.
func sliceConsumersByCores(procs int) int {
	if procs >= 4 {
		return 2
	}
	return 1
}

// sliceConsumer is one of the k streams consuming the Slot's slices: it
// reads a slice the dispatch hands it, waits for what the dispatch makes of
// the read, and completes the slice on its own goroutine.
type sliceConsumer struct {
	stream *streamedExecution
	jobs   chan sliceJob
	// go hands the dispatch's verdict on the read back, with the
	// completion's context to complete the slice in.
	verdicts chan sliceGo
	// leaf is the slice it consumes and busy whether it has one; the
	// dispatch's.
	leaf inputslice.Leaf
	busy bool
	// handed is when the dispatch handed it the slice: its read's start, for
	// a read the Slot's context ended (consumeSideBySide). judged says the
	// read was judged to be consumed: it is completing the slice.
	handed time.Time
	judged bool
	// consumed is how many slices it consumed, and completion what its
	// slice's read came back with; the dispatch's.
	consumed   int
	completion execution.QueryExecutionCompletion
}

// sliceJob is a slice handed to a consumer, with its read ahead when the
// dispatch read it ahead (takeReadAhead).
type sliceJob struct {
	leaf  inputslice.Leaf
	ahead *sliceReadAhead
}

// sliceGo is the dispatch's verdict on a consumer's read.
type sliceGo struct {
	verdict       sliceVerdict
	completionCtx context.Context
}

// sliceEvent is a consumer telling the dispatch it read its slice (read,
// with what came back and when) or consumed it (err).
type sliceEvent struct {
	consumer             *sliceConsumer
	read                 bool
	completion           execution.QueryExecutionCompletion
	err                  error
	started, ended, done time.Time
}

// sideBySideStreams makes the Slot dispatch streams one consumed by k
// streams side by side: each its own producer of the apply pipe, one to k,
// and one of the Slot's accounts after the dispatch's, asking the dispatch
// on yields to let its reads ahead go until done.
func sideBySideStreams(dispatch *streamedExecution, k int, yields chan<- yieldRequest, done <-chan struct{}) []*streamedExecution {
	dispatch.consumers = k
	dispatch.accounts = []*streamedExecution{dispatch}
	streams := make([]*streamedExecution, k)
	for index := range streams {
		streams[index] = &streamedExecution{slotStream: dispatch.slotStream, dispatch: dispatch, producer: uint64(index + 1),
			yieldTo: yields, yieldDone: done}
		dispatch.accounts = append(dispatch.accounts, streams[index])
	}
	return streams
}

// consumeSideBySide reads and consumes the Slot's slices with k consumers,
// the dispatch - the Slot's own stream - handing each the next slice pending
// as it is free, judging each read (judgeSliceRead) and landing each slice
// consumed, until every slice has landed. It returns as the one loop ends:
// ended says the Slot ended there, with result and err, rather than going
// on to finish.
//
// What k consumers change of the Slot each changes its own way: every stream
// its own retained bytes, clock, slice and window (015 C1 step 3.1 and 3.2),
// the Slot's results under the merge lock, its windows by producer. A step
// that takes every stream of the Slot - a merge past its caps, its results
// replaced past its budget - is taken behind the Slot's barrier
// (slotBarrier).
func (coordinator *SlotExecutionCoordinator) consumeSideBySide(ctx context.Context, run *sliceRun, dispatch *streamedExecution,
	k int,
) (result execution.SlotExecutionResult, err error, ended bool) {
	yields := make(chan yieldRequest)
	done := make(chan struct{})
	events := make(chan sliceEvent)
	consumers := make([]*sliceConsumer, k)
	for index, stream := range sideBySideStreams(dispatch, k, yields, done) {
		consumers[index] = &sliceConsumer{stream: stream, jobs: make(chan sliceJob, 1), verdicts: make(chan sliceGo, 1)}
	}
	consumerCtx, cancel := context.WithCancel(ctx)
	// The context the consumers complete their slices under: the Slot's
	// completion context, ended as the consumers are (stop). A consumer in
	// its completion waits on what only another consumer ends - the slices
	// numbered below its own (waitBelow, waitHeadsBelow), the Slot's barrier
	// (enterCompletion, safePoint, holdBarrier), the Slot's State load another
	// consumer began (loadSlot), the State reads it shares (startPreflightRead)
	// - and a slice below that failed is neither completed nor let go: on the
	// Slot's completion context alone the consumer above would wait until
	// that ended, which bounds nothing but the lease of a Slot with no short
	// deadline. Made as the first slice is handed to be completed, once the
	// Slot's completion context is (judgeSliceRead).
	var completions context.Context
	completionsCtx := func() context.Context {
		if completions == nil {
			var cancelCompletions context.CancelFunc
			completions, cancelCompletions = context.WithCancel(run.completionCtx)
			context.AfterFunc(consumerCtx, cancelCompletions)
		}
		return completions
	}
	var wait sync.WaitGroup
	for _, consumer := range consumers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			consumer.run(withRetainedOwner(consumerCtx, consumer.stream), coordinator, run.queryRequest, events)
		}()
	}
	// Every consumer ends before the Slot goes on, on every return: a
	// consumer waiting on the dispatch is let go (done, cancel), one in its
	// completion has it end (completionsCtx), and every one's jobs closed.
	// What a consumer handed the apply side is written as it is for one
	// stream: the apply goroutine writes it under the Slot's own context and
	// the Slot joins it (joinApply), a hand-over or drain waiting only on it.
	stop := func() { stopConsumers(consumers, done, cancel, &wait, events, yields) }
	// The slices the dispatch read before - a first read let go and split -
	// were let go: none above them waits for them.
	for number := range dispatch.slices.dispatched {
		dispatch.ledger.letGo(number)
	}
	inFlight := map[int]bool{}
	// The slice numbered last of those consumed, what its read came back with
	// and whether its own series took the Slot past its budget: the Slot
	// finishes on it, as the one stream does on the slice it read last
	// (finishSideBySide).
	lastNumber, lastCompletion, lastReplaced := -1, execution.QueryExecutionCompletion{}, false
	for {
		// Hand the next slices pending to the consumers free, in the order
		// the tree has them.
		for _, consumer := range consumers {
			if consumer.busy {
				continue
			}
			leaf, found := inputslice.Leaf{}, false
			for _, pending := range dispatch.slices.pending() {
				if !inFlight[pending.ID] {
					leaf, found = pending, true
					break
				}
			}
			if !found {
				break
			}
			inFlight[leaf.ID] = true
			consumer.leaf, consumer.busy, consumer.handed, consumer.judged = leaf, true, time.Now(), false
			consumer.stream.beginSlice(leaf, false)
			ahead := dispatch.takeReadAhead(leaf)
			if ahead == nil {
				// Counted by the reads in flight with it: the consumers'
				// and the reads ahead.
				dispatch.noteReadBegan(busyConsumers(consumers))
			}
			consumer.jobs <- sliceJob{leaf: leaf, ahead: ahead}
		}
		busy := false
		for _, consumer := range consumers {
			busy = busy || consumer.busy
		}
		if !busy {
			break
		}
		execution.MarkStage(ctx, execution.SlotStageQuery)
		select {
		case <-ctx.Done():
			// The Slot's context ended - its lease lost, its Query Group moved,
			// the process stopping. A consumer that sees it end returns from
			// whatever it waits on without a word to the dispatch (run), so the
			// dispatch does not wait for one: the Slot ends as one stream does
			// when the read in hand fails on that context, the read in hand
			// being the lowest slice a consumer has.
			consumer := lowestBusyConsumer(consumers)
			if consumer.judged {
				// Its read was judged and it is completing the slice: the Slot
				// ends as one stream does when its completion fails on that
				// context - a step of ours, not the query, which was fine.
				coordinator.observeQueryFailure(run.completionCtx, run.request.Operation, run.started, "stream_complete", ctx.Err())
				stop()
				return execution.SlotExecutionResult{}, inSlotStep(execution.SlotNotCommittedCompleteFailed,
					fmt.Errorf("alarmd worker: complete Slot: %w", ctx.Err())), true
			}
			_, failed, failure := coordinator.judgeSliceRead(ctx, run, consumer.stream, consumer.leaf,
				execution.QueryExecutionCompletion{}, ctx.Err(), consumer.handed, time.Now())
			stop()
			if failure == nil {
				failure = inSlotStep(execution.SlotNotCommittedQueryFailed, fmt.Errorf("alarmd worker: query: %w", ctx.Err()))
			}
			return failed, failure, true
		case request := <-yields:
			request.reply <- dispatch.yieldReadAhead(request.phase)
		case event := <-events:
			consumer := event.consumer
			if event.read {
				consumer.completion = event.completion
				first := !run.read
				verdict, failed, failure := coordinator.judgeSliceRead(ctx, run, consumer.stream, consumer.leaf, event.completion, event.err,
					event.started, event.ended)
				if first && run.read {
					// The input phase is the dispatch's: from the Slot's
					// start to its first read back. Its clock starts as the
					// consumers end (finishSideBySide).
					dispatch.spend(slotPhaseInput, dispatch.beganAt)
				}
				if verdict == sliceConsume {
					dispatch.readAheadSideBySide(ctx, coordinator, run, inFlight, k)
				}
				switch verdict {
				case sliceAgain:
					// Let go and split, or its dimensions forgotten: its
					// number has no slice, and no slice above it waits for it.
					dispatch.ledger.letGo(consumer.stream.sliceNumber)
					delete(inFlight, consumer.leaf.ID)
					consumer.busy = false
					if !dispatch.readInSlices() {
						// Its dimensions forgotten - found at the Slot's first
						// Begin, before any slice completed: the Slot is read
						// whole from here, on its own stream, as one stream
						// reads it. The reads of the other consumers, of slices
						// it no longer has, go with nothing of them kept, none
						// judged: judged, one begun once the dimensions were
						// forgotten would be consumed beside the whole read.
						// Reached only where k is set from the Slot's first
						// slice, by a case or the measurement build: by the
						// process's cores, consumers begin once the Slot has
						// begun, and its dimensions are found gone at its
						// first Begin.
						stop()
						dispatch.letGoConsumers(consumers)
						return execution.SlotExecutionResult{}, nil, false
					}
				case sliceFailed:
					consumer.verdicts <- sliceGo{verdict: sliceFailed}
					stop()
					return failed, failure, true
				}
				consumer.judged = verdict == sliceConsume
				consumer.verdicts <- sliceGo{verdict: verdict, completionCtx: completionsCtx()}
				continue
			}
			if event.err != nil {
				coordinator.observeQueryFailure(run.completionCtx, run.request.Operation, run.started, "stream_complete", event.err)
				stop()
				return execution.SlotExecutionResult{}, inSlotStep(execution.SlotNotCommittedCompleteFailed,
					fmt.Errorf("alarmd worker: complete Slot: %w", event.err)), true
			}
			dispatch.slices.landSliceRead(&consumer.stream.slice, consumer.leaf)
			delete(inFlight, consumer.leaf.ID)
			consumer.busy = false
			consumer.consumed++
			if number := consumer.stream.sliceNumber; number > lastNumber {
				lastNumber, lastCompletion, lastReplaced = number, consumer.completion, consumer.stream.slice.replaced
			}
		}
	}
	stop()
	// Before the Slot finishes (015 C1 section 6, the fourth): the finish
	// merges on the dispatch, judged against the Slot's counts, and a
	// consumer's window still in flight holds its part of them - it would
	// refuse a merge one stream admits once its own windows have landed, and,
	// given back by the replacement that refusal leads to, be given back again
	// as it is written.
	if err := dispatch.landConsumers(run.completionCtx); err != nil {
		coordinator.observeQueryFailure(run.completionCtx, run.request.Operation, run.started, "stream_complete", err)
		return execution.SlotExecutionResult{}, inSlotStep(execution.SlotNotCommittedCompleteFailed,
			fmt.Errorf("alarmd worker: complete Slot: %w", err)), true
	}
	for _, consumer := range consumers {
		dispatch.seams.fold(consumer.stream.seams)
	}
	if note := slicesBySubstreamForTest.Load(); note != nil {
		consumed := make([]int, len(consumers))
		for index, consumer := range consumers {
			consumed[index] = consumer.consumed
		}
		(*note)(consumed, dispatch.barriers)
	}
	dispatch.finishSideBySide(lastCompletion, lastReplaced)
	return execution.SlotExecutionResult{}, nil, false
}

// addSeries adds to the series the Slot holds of the pool - less, by a
// negative count, no further than none.
func (stream *streamedExecution) addSeries(series int64) {
	stream.sharedMu.Lock()
	defer stream.sharedMu.Unlock()
	if series < 0 {
		stream.series -= min(stream.series, uint64(-series))
		return
	}
	stream.series += uint64(series)
}

// startWindows starts the Slot's windowed apply, once for every stream of it,
// at its first window (applyWindowHead), and says whether it is written in
// windows: a Slot whose head did not start them is applied in one window
// (windows.single), its head tried again at the next cut, as before.
func (stream *streamedExecution) startWindows(ctx context.Context) (bool, error) {
	stream.windowHeadMu.Lock()
	defer stream.windowHeadMu.Unlock()
	windows := &stream.windows
	if windows.started {
		return true, nil
	}
	started, err := stream.coordinator.applyWindowHead(ctx, stream)
	if err != nil {
		return false, err
	}
	if !started {
		windows.single = true
	}
	return started, nil
}

// windowsStarted says whether the Slot's windowed apply has started.
func (stream *streamedExecution) windowsStarted() bool {
	stream.windowHeadMu.Lock()
	defer stream.windowHeadMu.Unlock()
	return stream.windows.started
}

// addShared runs change, one of the Slot's accumulators its streams add to side
// by side - its census, queries' facts and evidence, the series it took up -
// under sharedMu.
func (stream *streamedExecution) addShared(change func()) {
	stream.sharedMu.Lock()
	defer stream.sharedMu.Unlock()
	change()
}

// seriesNow is the series the Slot holds of the pool.
func (stream *streamedExecution) seriesNow() uint64 {
	stream.sharedMu.Lock()
	defer stream.sharedMu.Unlock()
	return stream.series
}

// run is a consumer's goroutine: each slice handed, read, judged by the
// dispatch, and completed on this stream.
func (consumer *sliceConsumer) run(ctx context.Context, coordinator *SlotExecutionCoordinator, template execution.QueryExecutionRequest,
	events chan<- sliceEvent,
) {
	for job := range consumer.jobs {
		request := template
		request.Slice, request.FollowingRead = job.leaf.Slice, true
		started := time.Now()
		completion, err, replayed := execution.QueryExecutionCompletion{}, error(nil), false
		if job.ahead != nil {
			completion, err, replayed = consumer.stream.replayAhead(ctx, job.ahead)
		}
		if !replayed {
			completion, err = coordinator.ports.Query.Execute(ctx, request, consumer.stream)
		}
		select {
		case events <- sliceEvent{consumer: consumer, read: true, completion: completion, err: err, started: started, ended: time.Now()}:
		case <-ctx.Done():
			return
		}
		var verdict sliceGo
		select {
		case verdict = <-consumer.verdicts:
		case <-ctx.Done():
			return
		}
		if verdict.verdict != sliceConsume {
			continue
		}
		err = consumer.stream.complete(withRetainedOwner(verdict.completionCtx, consumer.stream), completion)
		select {
		case events <- sliceEvent{consumer: consumer, err: err, done: time.Now()}:
		case <-ctx.Done():
			return
		}
	}
}

// stopConsumers ends the consumers of a Slot consumed side by side, on every
// way consumeSideBySide returns: done closed (the yields a consumer's stream
// asks end unserved), their context cancelled (and with it the context they
// complete under), their jobs closed, and their events and yields served
// until every one has ended (wait) - a consumer is never left waiting on the
// dispatch - then the read ahead of every job they left untaken stopped.
func stopConsumers(consumers []*sliceConsumer, done chan struct{}, cancel context.CancelFunc, wait *sync.WaitGroup,
	events <-chan sliceEvent, yields <-chan yieldRequest,
) {
	close(done)
	cancel()
	for _, consumer := range consumers {
		close(consumer.jobs)
	}
	finished := make(chan struct{})
	go func() { wait.Wait(); close(finished) }()
	for alive := true; alive; {
		select {
		case <-finished:
			alive = false
		case <-events:
		case request := <-yields:
			request.reply <- false
		}
	}
	stopUntakenReadsAhead(consumers)
}

// stopUntakenReadsAhead stops the read ahead of every job the consumers -
// every one ended, their jobs closed - left untaken. A job a consumer ended
// before taking - handed as its slice was let go and split, while it waited
// for that verdict - holds a read ahead taken from the dispatch
// (takeReadAhead): no one else stops it, and it would wait for room, or read
// on, holding what it kept.
func stopUntakenReadsAhead(consumers []*sliceConsumer) {
	for _, consumer := range consumers {
		for job := range consumer.jobs {
			if job.ahead != nil {
				job.ahead.stop()
			}
		}
	}
}

// lowestBusyConsumer is the consumer with the lowest numbered slice of those
// that have one; the dispatch calls it only while one has.
func lowestBusyConsumer(consumers []*sliceConsumer) *sliceConsumer {
	var lowest *sliceConsumer
	for _, consumer := range consumers {
		if consumer.busy && (lowest == nil || consumer.stream.sliceNumber < lowest.stream.sliceNumber) {
			lowest = consumer
		}
	}
	return lowest
}

// busyConsumers is how many of the consumers have a slice.
func busyConsumers(consumers []*sliceConsumer) int {
	busy := 0
	for _, consumer := range consumers {
		if consumer.busy {
			busy++
		}
	}
	return busy
}

// readAheadSideBySide reads ahead the slices pending that no consumer has,
// as many as the read width leaves beside the k consumers' reads (015 C1
// 4.3): the k slices consumed and w-k read ahead, w reads in flight as with
// one stream. A consumer that takes a slice read ahead replays it
// (takeReadAhead).
func (stream *streamedExecution) readAheadSideBySide(ctx context.Context, coordinator *SlotExecutionCoordinator, run *sliceRun,
	inFlight map[int]bool, k int,
) {
	width := coordinator.sliceReadWidth(judgedRequests(stream.header))
	if width <= k || sliceReadAheadOff.Load() {
		return
	}
	// startSliceReadAheads reads ahead after the slice it is given first.
	next := []inputslice.Leaf{{}}
	for _, leaf := range stream.slices.pending() {
		if !inFlight[leaf.ID] {
			next = append(next, leaf)
		}
	}
	if len(next) > 1 {
		stream.startSliceReadAheads(ctx, run.queryRequest, next, width-k+1)
	}
}

// letGoConsumers lets go of the slices the consumers - ended - were reading
// as the Slot stopped being read in slices, each consumer's streamed input
// given back (abandonSlice) and its number left with no slice, and has the
// Slot read on its own stream from here.
func (stream *streamedExecution) letGoConsumers(consumers []*sliceConsumer) {
	for _, consumer := range consumers {
		if consumer.busy {
			consumer.stream.abandonSlice()
			stream.ledger.letGo(consumer.stream.sliceNumber)
			consumer.busy = false
		}
		stream.seams.fold(consumer.stream.seams)
	}
	stream.consumers = 0
}

// landConsumers waits until the windows every consumer handed are written,
// and lands each on its own stream: on the dispatch, once the consumers have
// ended.
func (stream *streamedExecution) landConsumers(ctx context.Context) error {
	for _, each := range stream.accounts {
		if each == stream {
			continue
		}
		if err := each.drainApply(ctx); err != nil {
			return err
		}
	}
	return nil
}

// finishSideBySide leaves the dispatch where the one stream is once its last
// slice was consumed: what is read so far is every slice, its clock runs,
// and the Slot finishes (finishSlices) on the completion of the slice
// numbered last, its bindings and physical queries one read's, as the one
// stream finishes on the slice it read last - evaluating nothing more when
// that slice's own series took the Slot past its budget (replaced).
func (stream *streamedExecution) finishSideBySide(last execution.QueryExecutionCompletion, replaced bool) {
	series, bindings := stream.ledger.through(stream.ledger.last())
	counts := make(map[execution.PlanIdentity]*int, len(stream.header.DuePlans))
	for _, due := range stream.header.DuePlans {
		matched := series[due.Identity]
		counts[due.Identity] = &matched
	}
	stream.planSeriesCounts, stream.bindings = counts, bindings
	stream.clockPhase, stream.clockSince = slotPhaseOther, time.Now()
	keyed := make(map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding, len(last.CompletionBindings))
	for _, binding := range last.CompletionBindings {
		keyed[struct {
			consumer    execution.ConsumerRef
			requirement execution.RequirementID
		}{consumer: binding.Consumer, requirement: binding.RequirementID}] = binding
	}
	stream.slices.finish = sliceFinish{set: true, completion: last, bindings: keyed, replaced: replaced}
}
