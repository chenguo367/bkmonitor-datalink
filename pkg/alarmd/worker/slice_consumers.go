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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
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

// consumersBeganForTest, set only by cases, is told as a Slot's consumers
// are about to begin, its dispatch's own slice read.
var consumersBeganForTest atomic.Pointer[func()]

// sliceReadTakenForTest, set only by cases, is told the slice of each read
// the dispatch takes back from its consumers, before it judges the read.
var sliceReadTakenForTest atomic.Pointer[func(slice execution.InputSlice)]

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
	if stream.slices.chosen > 0 {
		return stream.slices.chosen
	}
	width := stream.coordinator.sliceReadWidth(judgedRequests(stream.header))
	return max(1, min(sliceConsumersByCores(procs), width-1))
}

// sliceFloorDivisor is L_min, the least a slice of a Slot consumed
// side by side is to be able to hold, as a part of the share: S/64 (015 C1,
// the k = 2 share, K1), 12 MiB of a 768 MiB share. Below it a slice's
// fixed cost - its query, its Begin - takes too much of what it reads for
// another consumer to pay.
const sliceFloorDivisor = 64

// sliceRoomHeldForTest, set only by cases, is held beside what the dispatch
// holds as a Slot's k is chosen by its room (consumersForRoom).
var sliceRoomHeldForTest atomic.Uint64

// consumersForRoom is k for a Slot by the room its share leaves once the
// dispatch's own windows have landed (015 C1, the k = 2 share, 3.2): the
// most of cores consumers whose slices may each still hold L_min, one when
// none may. What the Slot holds that slicing does not change - its output
// and State - is counted in full, 1/k by each consumer as before; what is
// chosen is how many it is split between. A consumer holds a slice and a
// window of at most L each, a read ahead a slice, w - k of them: so
// L(k) = (S/2 - D)/(k + w), D what the dispatch holds, the reads ahead
// counted at their most rather than as they are, none begun yet. A k above
// the one the Query Group's last Slot consumed with needs a quarter more,
// so a room near L_min does not split and join its shape round by round; a
// Query Group with no k remembered takes the most whose slices hold L_min.
// L(k) chooses k and bounds nothing: the consumers' slice limits are
// sliceLimit's, read as they read.
func (stream *streamedExecution) consumersForRoom(cores int) consumersChoice {
	share := stream.coordinator.qgShareBytes()
	if share == 0 || cores <= 1 {
		return consumersChoice{k: cores, from: observability.InputSliceConsumersFromCores}
	}
	width := uint64(stream.coordinator.sliceReadWidth(judgedRequests(stream.header)))
	room := boundLess(share/2, stream.ownTotalNow()+sliceRoomHeldForTest.Load())
	floor := share / sliceFloorDivisor
	last := stream.coordinator.slicedInput.lastInput(stream.request.Contract.Slot.QueryGroup).consumers
	need := func(k int) uint64 {
		if last > 0 && k > last {
			return floor + floor/4
		}
		return floor
	}
	// For a k the room took down: the cores' k's limit and the bar it fell
	// short of - the floor, or a quarter more to rise past the k remembered.
	short := consumersChoice{from: observability.InputSliceConsumersFromRoom, limit: room / (uint64(cores) + width),
		floor: need(cores)}
	for k := cores; k > 1; k-- {
		if room/(uint64(k)+width) >= need(k) {
			if k == cores {
				return consumersChoice{k: k, from: observability.InputSliceConsumersFromCores}
			}
			short.k = k
			return short
		}
	}
	short.k = 1
	return short
}

// consumersChoice is the k a Slot was given and where it came from
// (observability.InputSliceConsumersFromSources): for a k the room took
// down, the slice limit the room left the cores' k and the bar it fell short
// of - the floor, or a quarter more to rise past the k remembered.
type consumersChoice struct {
	k            int
	from         string
	limit, floor uint64
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
	// The k the Slot consumed with, for its usage row and its Query Group's
	// next Slot (rememberSlotInput): written before any consumer starts.
	dispatch.slices.consumers = k
	// The dispatch's own windows - of the slice it read before its consumers
	// began - have landed: the Slot waited for them before choosing its k
	// (015 C1, the k = 2 share, 3.1), and the dispatch hands none while its
	// consumers read. So nothing of its own is taken as they read; a window of
	// its handed after all is drained, and its failed write found, as the Slot
	// finishes (landConsumers). Whoever has the dispatch hand windows while
	// its consumers read is to take their landings here again.
	dispatch.stopClockForConsumers()
	if note := consumersBeganForTest.Load(); note != nil {
		(*note)()
	}
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
			failed, failure := coordinator.endSideBySideOnContext(ctx, run, consumers, stop)
			return failed, failure, true
		case request := <-yields:
			if ctx.Err() != nil {
				failed, failure := coordinator.endSideBySideOnContext(ctx, run, consumers, stop)
				return failed, failure, true
			}
			request.reply <- dispatch.yieldReadAhead(request.phase)
		case event := <-events:
			if ctx.Err() != nil {
				// Whatever came back with the context ended - a slice above
				// read, the lowest's completion failed on it - the Slot ends
				// by the lowest slice a consumer has, as it does when the
				// context's end is what the dispatch takes first.
				failed, failure := coordinator.endSideBySideOnContext(ctx, run, consumers, stop)
				return failed, failure, true
			}
			consumer := event.consumer
			if event.read {
				if note := sliceReadTakenForTest.Load(); note != nil {
					(*note)(consumer.leaf.Slice)
				}
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
					if err := ctx.Err(); err != nil && errors.Is(failure, err) && lowestBusyConsumer(consumers) != consumer {
						// The context ended as this read was judged, after the
						// dispatch took it, and failed it on the completion
						// deadline: a slice below it busy, the Slot ends by
						// that slice, as it does when the end came first. This
						// read the lowest, its judgement is one stream's.
						failed, failure := coordinator.endSideBySideOnContext(ctx, run, consumers, stop)
						return failed, failure, true
					}
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

// stopClockForConsumers stops the dispatch's clock while its consumers read:
// what it spent since its first slice completed - waiting for its own
// windows to land among it (drainApply, apply_wait) - is charged now, and
// nothing is charged to it while they read - a Slot's phases are its
// streams' together, and the wall clock of the consumers' reads is theirs.
// It starts again as the consumers end (finishSideBySide).
func (dispatch *streamedExecution) stopClockForConsumers() {
	if dispatch.clockSince.IsZero() {
		return
	}
	dispatch.chargeClock()
	dispatch.clockSince = time.Time{}
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

// endSideBySideOnContext ends a Slot whose context ended as its slices were
// consumed side by side - its lease lost, its Query Group moved, the process
// stopping - and stops its consumers. A consumer that sees the context end
// returns from whatever it waits on without a word to the dispatch (run), so
// the dispatch does not wait for one: the Slot ends as one stream does when
// the read in hand fails on that context, the read in hand being the lowest
// slice a consumer has. Whatever the dispatch takes first once the context
// ended - the end itself, a slice above read, the lowest's completion failed
// on it - or a read above it judged as the context ends, the Slot ends by
// that slice.
func (coordinator *SlotExecutionCoordinator) endSideBySideOnContext(ctx context.Context, run *sliceRun,
	consumers []*sliceConsumer, stop func(),
) (execution.SlotExecutionResult, error) {
	consumer := lowestBusyConsumer(consumers)
	if consumer.judged {
		// Its read was judged and it is completing the slice: the Slot ends
		// as one stream does when its completion fails on that context - a
		// step of ours, not the query, which was fine.
		coordinator.observeQueryFailure(run.completionCtx, run.request.Operation, run.started, "stream_complete", ctx.Err())
		stop()
		return execution.SlotExecutionResult{}, inSlotStep(execution.SlotNotCommittedCompleteFailed,
			fmt.Errorf("alarmd worker: complete Slot: %w", ctx.Err()))
	}
	_, failed, failure := coordinator.judgeSliceRead(ctx, run, consumer.stream, consumer.leaf,
		execution.QueryExecutionCompletion{}, ctx.Err(), consumer.handed, time.Now())
	stop()
	if failure == nil {
		failure = inSlotStep(execution.SlotNotCommittedQueryFailed, fmt.Errorf("alarmd worker: query: %w", ctx.Err()))
	}
	return failed, failure
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

// landConsumers waits until the windows every consumer and the dispatch
// handed are written, and lands each on its own stream: on the dispatch,
// once the consumers have ended.
func (stream *streamedExecution) landConsumers(ctx context.Context) error {
	// The dispatch's own lane too (015 C1, the k = 2 share, 3.3): a window of
	// its in flight as the Slot finishes would be counted in the merge that
	// finishes it, as a consumer's would - refused where one stream's is
	// admitted, and given back again after the replacement. A guard: the
	// dispatch's windows landed before its consumers began, and it hands none
	// as they read, so its lane is empty here today.
	for _, each := range stream.accounts {
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
