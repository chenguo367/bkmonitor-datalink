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
	"reflect"
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

// heldState holds the Slot's first State apply - its first window's - until
// the Slot's evaluation has reached a series of a later window, or the case's
// deadline, and says once it has returned: the first window cannot land
// before the Slot evaluates that series, wherever the apply goroutine is when
// it does.
type heldState struct {
	*persistedRetryState
	first    sync.Once
	returned atomic.Bool
	released chan struct{}
	release  sync.Once
	expired  <-chan time.Time
}

func (state *heldState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	held := false
	state.first.Do(func() {
		held = true
		select {
		case <-state.released:
		case <-state.expired:
		}
	})
	result, err := state.persistedRetryState.ApplyRuntime(ctx, request)
	if held {
		state.returned.Store(true)
	}
	return result, err
}

// watchingEvaluator is the recording evaluator, which releases the held
// first window once it is asked for a series of a later one, and says whether
// that window's State apply had returned by then: not returned, the first
// window was still being written while the Slot evaluated on.
type watchingEvaluator struct {
	*recordingPorts
	state      *heldState
	later      string
	overlapped bool
	mu         sync.Mutex
}

func (evaluator *watchingEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	if evaluator.state != nil && string(request.State.Items[0].Identity.SeriesIdentityDigest) == evaluator.later {
		if !evaluator.state.returned.Load() {
			evaluator.mu.Lock()
			evaluator.overlapped = true
			evaluator.mu.Unlock()
		}
		evaluator.state.release.Do(func() { close(evaluator.state.released) })
	}
	return evaluator.recordingPorts.Evaluate(ctx, request)
}

// pipelineRun is one Execute of the windowed Slot - a cap of four State
// writes, so windows of two, a b | c d | e f - and what it wrote.
type pipelineRun struct {
	result  execution.SlotExecutionResult
	batches [][]string
	saved   map[execution.StateKeyIdentity]execution.MutationDigest
	writes  []string
	applied []observability.Observation
	guards  int
	// released is what each window gave back as it landed, of its state,
	// output and input; sequences the most Sequences that ran at once; observedAtOnce
	// whether the observer was ever called from two goroutines at once.
	released       [][3]uint64
	sequences      int64
	observedAtOnce bool
}

func runWindowedSlot(t *testing.T, held bool) (pipelineRun, *watchingEvaluator) {
	t.Helper()
	return runWindowedSlotCapped(t, held, 4)
}

// runWindowedSlotCapped is runWindowedSlot under a State cap of its own.
func runWindowedSlotCapped(t *testing.T, held bool, stateCap uint64) (pipelineRun, *watchingEvaluator) {
	t.Helper()
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	persisted := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	var state execution.StateStore = persisted
	evaluator := &watchingEvaluator{recordingPorts: ports, later: strings.Repeat("e", 64)}
	if held {
		gate := &heldState{persistedRetryState: persisted, released: make(chan struct{}), expired: testwait.Expired(t)}
		evaluator.state, state = gate, gate
	}
	var observedMu sync.Mutex
	var applied []observability.Observation
	var released [][3]uint64
	worker.SetWindowLandedForTest(t, func(state, output, input uint64) {
		released = append(released, [3]uint64{state, output, input})
	})
	var observing atomic.Int64
	var observedAtOnce atomic.Bool
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: evaluator, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
		Hosts: worker.SharedHostBusiness, Events: ports, State: state, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			if observing.Add(1) > 1 {
				observedAtOnce.Store(true)
			}
			defer observing.Add(-1)
			if observation.Stage == observability.StageStateApplied {
				observedMu.Lock()
				observation.Duration, observation.StateApplyChunk = 0, nil
				applied = append(applied, observation)
				observedMu.Unlock()
			}
		})},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: stateCap, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v", result, err)
	}
	run := pipelineRun{result: result, batches: sortedBatches(ports.eventBatches, 0), applied: applied,
		saved: map[execution.StateKeyIdentity]execution.MutationDigest{}, guards: len(ports.gapMutations),
		released: released, sequences: ports.sequencesMost.Load(), observedAtOnce: observedAtOnce.Load()}
	for key, mutation := range persisted.saved {
		run.saved[key] = mutation.MutationDigest
	}
	for _, stage := range *ports.trace {
		switch stage {
		case "event_ack", "state_apply", "gap", "progress_commit":
			run.writes = append(run.writes, stage)
		}
	}
	return run, evaluator
}

// A Slot cut into windows writes each window on its apply goroutine while it
// fills the next (014 next9 4): its first window's State apply is held until
// the Slot is evaluating a series of its third window, which it reaches only
// by going on while the first is written. And what it writes is what the same
// Slot writes with each window written before the next is filled: the same
// windows, cut at half the State cap, the same event batches and State, the
// writes in the same order, the same usage - and its window accounting never
// took off more than it held.
func TestAWindowIsWrittenWhileTheNextIsFilledAndTheSlotWritesWhatItWritesSerially(t *testing.T) {
	pipelined, evaluator := runWindowedSlot(t, true)
	evaluator.mu.Lock()
	overlapped := evaluator.overlapped
	evaluator.mu.Unlock()
	if !overlapped {
		t.Fatal("the Slot did not evaluate a later window's series while its first window was written")
	}
	worker.SetApplySeriallyForTest(t)
	serial, _ := runWindowedSlot(t, false)

	series := windowedSeries("a", "b", "c", "d", "e", "f")
	if want := [][]string{{series[0], series[1]}, {series[2], series[3]}, {series[4], series[5]}}; !reflect.DeepEqual(pipelined.batches, want) {
		t.Fatalf("pipelined event batches = %v, want windows of two", pipelined.batches)
	}
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"event batches", pipelined.batches, serial.batches},
		{"State written", pipelined.saved, serial.saved},
		{"write order", pipelined.writes, serial.writes},
		{"applied chunks", pipelined.applied, serial.applied},
		{"what each window gave back as it landed", pipelined.released, serial.released},
		{"State mutations", pipelined.result.Usage.StateMutations, serial.result.Usage.StateMutations},
		{"events", pipelined.result.Usage.Events, serial.result.Usage.Events},
		{"windows", pipelined.result.Usage.Windows, serial.result.Usage.Windows},
		{"ceiling windows", pipelined.result.Usage.WindowCeilingWindows, serial.result.Usage.WindowCeilingWindows},
		{"completed", pipelined.result.Completed, serial.result.Completed},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s: pipelined %v, serial %v", c.name, c.got, c.want)
		}
	}
	if pipelined.sequences != 1 || pipelined.observedAtOnce {
		t.Fatalf("%d Sequences ran at once, the observer called from two goroutines at once %t; want one, and never",
			pipelined.sequences, pipelined.observedAtOnce)
	}
	for _, run := range []pipelineRun{pipelined, serial} {
		if run.result.Usage.WindowOutsideClamped+run.result.Usage.SliceOthersClamped != 0 {
			t.Errorf("window accounting clamped: window_outside %d, slice_others %d", run.result.Usage.WindowOutsideClamped,
				run.result.Usage.SliceOthersClamped)
		}
	}
	if len(pipelined.saved) != len(series) {
		t.Fatalf("State written for %d series, want %d", len(pipelined.saved), len(series))
	}
}

// hookedState calls hook with the State apply's number, from one, as each
// begins, on the goroutine that writes it.
type hookedState struct {
	*persistedRetryState
	calls atomic.Int64
	hook  func(call int64)
}

func (state *hookedState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	state.hook(state.calls.Add(1))
	return state.persistedRetryState.ApplyRuntime(ctx, request)
}

// hookedPipelineSlot is the windowed Slot of runWindowedSlot, its State apply
// hooked and its clock now.
func hookedPipelineSlot(t *testing.T, now func() time.Time, hook func(int64)) (*recordingPorts, *worker.SlotExecutionCoordinator) {
	t.Helper()
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	state := &hookedState{persistedRetryState: &persistedRetryState{recordingPorts: ports,
		saved: map[execution.StateKeyIdentity]execution.StateMutation{}}, hook: hook}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: ports, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
		Hosts: worker.SharedHostBusiness, Events: ports, State: state, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {}), Now: now},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 4, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	return ports, coordinator
}

// A window written on the Slot's apply goroutine is inside its commit for
// the whole of its write, wherever the Slot's goroutine is (014 next9 4, C9):
// the watchdog, judging the Slot far past its deadline from inside the first
// window's State apply, does not claim it, and the window lands. Its deadline
// moves as it lands - by the clock then, not when it was cut - to a window's
// bound past that: the second window's State apply finds it there.
func TestAWindowIsInsideItsCommitOnItsApplyGoroutineAndMovesTheDeadlineAsItLands(t *testing.T) {
	start := time.Now().Truncate(time.Second)
	var clock atomic.Int64
	clock.Store(start.UnixMilli())
	now := func() time.Time { return time.UnixMilli(clock.Load()) }
	ctx, marker := markedContext(start.Add(time.Minute))
	landedAt := start.Add(7 * time.Second)
	claimed := false
	var second time.Time
	_, coordinator := hookedPipelineSlot(t, now, func(call int64) {
		switch call {
		case 1:
			_, _, claimed = marker.Decline(start.Add(24*time.Hour), watchdogGrace)
			// The first window lands now.
			clock.Store(landedAt.UnixMilli())
		case 2:
			second = marker.Deadline()
		}
	})
	result, err := coordinator.Execute(ctx, windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v", result, err)
	}
	if claimed {
		t.Fatal("the watchdog claimed the Slot from inside its first window's State apply")
	}
	if want := landedAt.Add(windowBound); !second.Equal(want) {
		t.Fatalf("the deadline as the second window is written = %v, want the first window's landing %v and a window's bound past it, %v",
			second, landedAt, want)
	}
}

// What a Slot cut into windows says of its clock (014 next9 4, C10): its own
// goroutine's phases with the time it waited on its apply goroutine and took
// its windows back partition its time; the apply goroutine's busy time runs
// beside them, the part of it the Slot did not wait for its overlap. A Slot
// that fits one window writes it on its own goroutine and has none of the
// three.
func TestASlotCutIntoWindowsPartitionsItsClockWithItsWaitsAndLandings(t *testing.T) {
	run, _ := runWindowedSlot(t, false)
	timing := run.result.Timing
	partition := timing.Input + timing.Preflight + timing.Evaluate + timing.RecordIDs + timing.Cut + timing.Merge +
		timing.Series + timing.NoDataRead + timing.NoDataWrite + timing.ApplyWait + timing.Land + timing.Unattributed
	// Each phase rounds down to a millisecond on its own.
	if partition > timing.Slot || timing.Slot-partition > 14 {
		t.Fatalf("the phases with apply_wait and land come to %d ms of the Slot's %d: %+v", partition, timing.Slot, timing)
	}
	if overlap := max(int64(timing.Apply)-int64(timing.ApplyWait), 0); int64(timing.ApplyOverlap) < overlap-1 ||
		int64(timing.ApplyOverlap) > overlap+1 {
		t.Fatalf("apply_overlap %d, want apply %d less apply_wait %d", timing.ApplyOverlap, timing.Apply, timing.ApplyWait)
	}

	_, _, coordinator, _ := windowedSlot(t, nil)
	whole, err := coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !whole.Completed || whole.Usage.Windows != 0 {
		t.Fatalf("the one-series Slot: result=%+v err=%v, want it applied whole", whole, err)
	}
	if whole.Timing.ApplyWait != 0 || whole.Timing.Land != 0 || whole.Timing.ApplyOverlap != 0 {
		t.Fatalf("a Slot applied whole says apply_wait %d, land %d, apply_overlap %d; want none", whole.Timing.ApplyWait,
			whole.Timing.Land, whole.Timing.ApplyOverlap)
	}
}

// failingLaterEvaluator is the recording evaluator, failing the evaluation of
// one series: one of a later window, evaluated while the first is written.
type failingLaterEvaluator struct {
	*recordingPorts
	series string
}

func (evaluator *failingLaterEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	if string(request.State.Items[0].Identity.SeriesIdentityDigest) == evaluator.series {
		return execution.EvaluationResult{}, errors.New("the series' evaluation fails")
	}
	return evaluator.recordingPorts.Evaluate(ctx, request)
}

// Every return of a Slot cut into windows ends its apply goroutine before the
// Slot gives anything back (014 next9 4, 3.2): an attempt whose second
// window's State write fails, and one whose evaluation fails while its first
// window is written, each return with no apply goroutine left, nothing of the
// pool or the per-Slot counts still held, and their census reported once,
// what the windows wrote counted written.
func TestEveryReturnOfASlotCutIntoWindowsEndsItsApplyGoroutineAndGivesEverythingBack(t *testing.T) {
	for _, c := range []struct {
		name string
		wire func(*recordingPorts) execution.Evaluator
	}{
		{"a window's State write failed", func(ports *recordingPorts) execution.Evaluator {
			ports.failStateApplyAt = 2
			return ports
		}},
		{"an evaluation failed after the first window was handed", func(ports *recordingPorts) execution.Evaluator {
			return &failingLaterEvaluator{recordingPorts: ports, series: strings.Repeat("e", 64)}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			trace := make([]string, 0)
			var mu sync.Mutex
			observations := make([]observability.Observation, 0)
			ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
			evaluator := c.wire(ports)
			// The first window's State write takes 100 ms: an attempt that
			// fails on the Slot's goroutine meanwhile returns with it in flight.
			state := &hookedState{persistedRetryState: &persistedRetryState{recordingPorts: ports,
				saved: map[execution.StateKeyIdentity]execution.StateMutation{}}, hook: func(call int64) {
				if call == 1 {
					time.Sleep(100 * time.Millisecond) // fixed-wait: a window in flight as the attempt fails, the subject of the case
				}
			}}
			coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
				Query: ports, Sequencer: ports, Evaluator: evaluator, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
				Hosts: worker.SharedHostBusiness, Events: ports, ExecutionEvidence: newMemoryEvidenceStore(), Progress: ports,
				State: state,
				Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					mu.Lock()
					defer mu.Unlock()
					observations = append(observations, observability.NormalizeObservation(observation))
				})},
				worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 4, MaxEvents: 100, MaxGapMutations: 10})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Execute(context.Background(), windowedRequest()); err == nil {
				t.Fatal("the attempt did not fail")
			}
			if live := worker.ApplyGoroutinesForTest(); live != 0 {
				t.Fatalf("%d apply goroutines still running after the attempt returned", live)
			}
			if held := worker.RetainedReserved(coordinator); held != 0 {
				t.Fatalf("%d bytes of the pool still held after the attempt returned", held)
			}
			if states, events, gaps := worker.EffectsReservedForTest(coordinator); states+events+gaps != 0 {
				t.Fatalf("the per-Slot counts still hold %d State, %d events and %d gaps", states, events, gaps)
			}
			mu.Lock()
			defer mu.Unlock()
			// Settled once the windows in flight landed: what they wrote is
			// counted written.
			if census := censusOf(t, observations); census.reports != 1 || census.written != int64(len(state.saved)) {
				t.Fatalf("the attempt's census was reported %d times with %d written, want once with the %d keys written",
					census.reports, census.written, len(state.saved))
			}
		})
	}
}

// slowLaterEvaluator is the recording evaluator, taking a while over one
// series of a later window: the Slot's goroutine is busy there, and takes the
// first window's landing back only after it.
type slowLaterEvaluator struct {
	*recordingPorts
	series string
	took   time.Duration
}

func (evaluator *slowLaterEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	if string(request.State.Items[0].Identity.SeriesIdentityDigest) == evaluator.series {
		time.Sleep(evaluator.took) // fixed-wait: the Slot busy over one series, the subject of the case
	}
	return evaluator.recordingPorts.Evaluate(ctx, request)
}

// What the apply goroutine says is said by the Slot's goroutine as it takes
// the landing back, with what was measured where it happened (014 next9 4):
// a State apply of 40 ms is said as 40 ms however late the Slot takes its
// window back - here after a series of the next window that takes 300 ms.
func TestWhatTheApplyGoroutineSaysIsMeasuredWhereItHappened(t *testing.T) {
	const apply, busy = 40 * time.Millisecond, 300 * time.Millisecond
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
	state := &hookedState{persistedRetryState: &persistedRetryState{recordingPorts: ports,
		saved: map[execution.StateKeyIdentity]execution.StateMutation{}}, hook: func(int64) {
		time.Sleep(apply) // fixed-wait: a State apply that takes its time, the subject of the case
	}}
	var mu sync.Mutex
	var applied []time.Duration
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
		Query: ports, Sequencer: ports, Evaluator: &slowLaterEvaluator{recordingPorts: ports, series: strings.Repeat("e", 64), took: busy},
		Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness, Events: ports, State: state,
		Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			if observation.Stage == observability.StageStateApplied {
				mu.Lock()
				applied = append(applied, observation.Duration)
				mu.Unlock()
			}
		})},
		worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 4, MaxEvents: 100, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) == 0 {
		t.Fatal("no State apply was said")
	}
	for _, took := range applied {
		if took < apply || took >= busy {
			t.Fatalf("State applies said as %v, want each its own %v or a little more, never the %v the Slot was busy", applied, apply, busy)
		}
	}
	if result.Timing.Apply < uint64(apply.Milliseconds())*3 {
		t.Fatalf("apply %d ms, want the three windows' State applies of %v each and more", result.Timing.Apply, apply)
	}
}

// Under a State cap of one a window holds one series, half the cap rounded
// up to one, and the next series' merge finds the cap held by the window in
// flight and its own: it waits for that window to land and merges (014 next9
// 4, C1). The Slot writes what it writes with each window landed before the
// next is filled - a window a series, every series written, no more
// statements than that - and uses what it uses.
func TestAMergePastTheCapWithAWindowInFlightMergesOnceItHasLanded(t *testing.T) {
	pipelined, _ := runWindowedSlotCapped(t, false, 1)
	worker.SetApplySeriallyForTest(t)
	serial, _ := runWindowedSlotCapped(t, false, 1)
	series := windowedSeries("a", "b", "c", "d", "e", "f")
	if len(pipelined.batches) != len(series) || len(pipelined.saved) != len(series) || pipelined.guards != serial.guards {
		t.Fatalf("pipelined: %d event batches, %d series written, %d statements; want a window a series, all written, the serial %d statements",
			len(pipelined.batches), len(pipelined.saved), pipelined.guards, serial.guards)
	}
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"event batches", pipelined.batches, serial.batches},
		{"State written", pipelined.saved, serial.saved},
		{"write order", pipelined.writes, serial.writes},
		{"State mutations", pipelined.result.Usage.StateMutations, serial.result.Usage.StateMutations},
		{"events", pipelined.result.Usage.Events, serial.result.Usage.Events},
		{"windows", pipelined.result.Usage.Windows, serial.result.Usage.Windows},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s: pipelined %v, serial %v", c.name, c.got, c.want)
		}
	}
}

// firstWindowRefused is the recording ports' Sequencer, refusing the first
// Sequence over State keys - the first window's - 50 ms in, before its commit.
type firstWindowRefused struct {
	*recordingPorts
	refused atomic.Bool
}

func (sequencer *firstWindowRefused) Sequence(ctx context.Context, scope execution.SequencingScope, run func(context.Context) error) error {
	if len(scope.StateKeys) > 0 && sequencer.refused.CompareAndSwap(false, true) {
		time.Sleep(50 * time.Millisecond) // fixed-wait: the next window handed while this one is refused, the subject of the case
		return errors.New("the first window's Sequence refused")
	}
	return sequencer.recordingPorts.Sequence(ctx, scope, run)
}

// A window whose write fails ends the Slot with its error, and no window
// after it is written (014 next9 4): the next window, filled and handed while
// the failing one is written, is not run - its events not sent, its State not
// written - whether the window failed inside its commit (its State write) or
// before it (its Sequence refused). A last window that fails is followed by
// no tail: the Slot's progress is not written.
func TestAWindowWhoseWriteFailedEndsTheSlotWithItsErrorAndNoLaterWindowIsWritten(t *testing.T) {
	for _, c := range []struct {
		name   string
		wire   func(*recordingPorts, *worker.Ports)
		failed string
		states int
		events int
	}{
		{"its State write failed", func(ports *recordingPorts, _ *worker.Ports) {
			ports.failStateApplyAt = 1
		}, "injected state_apply", 1, 1},
		{"its Sequence was refused", func(ports *recordingPorts, wired *worker.Ports) {
			wired.Sequencer = &firstWindowRefused{recordingPorts: ports}
		}, "the first window's Sequence refused", 0, 0},
		{"the last window's State write failed", func(ports *recordingPorts, _ *worker.Ports) {
			ports.failStateApplyAt = 3
		}, "injected state_apply", 3, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			trace := make([]string, 0)
			ports := &recordingPorts{trace: &trace, ready: true, extraSeries: windowedSeries("a", "b", "d", "e", "f")}
			state := &hookedState{persistedRetryState: &persistedRetryState{recordingPorts: ports,
				saved: map[execution.StateKeyIdentity]execution.StateMutation{}}, hook: func(call int64) {
				if call == 1 {
					time.Sleep(50 * time.Millisecond) // fixed-wait: the next window handed while this one is written, the subject of the case
				}
			}}
			wired := worker.Ports{OpenAlerts: ports, Finalization: ports, Activation: ports,
				Query: ports, Sequencer: ports, Evaluator: ports, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore,
				Hosts: worker.SharedHostBusiness, Events: ports, State: state, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
				Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {})}
			c.wire(ports, &wired)
			coordinator, err := worker.NewSlotExecutionCoordinator(wired,
				worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 4, MaxEvents: 100, MaxGapMutations: 10})
			if err != nil {
				t.Fatal(err)
			}
			_, err = coordinator.Execute(context.Background(), windowedRequest())
			if err == nil || !strings.Contains(err.Error(), c.failed) {
				t.Fatalf("Execute() err=%v, want the first window's failure %q", err, c.failed)
			}
			if ports.stateApplyCalls != c.states || len(ports.eventBatches) != c.events || !isZeroProgressCommit(ports.lastProgress) {
				t.Fatalf("%d State writes, %d event batches and progress %+v; want %d and %d, up to the failing window, and no progress",
					ports.stateApplyCalls, len(ports.eventBatches), ports.lastProgress, c.states, c.events)
			}
		})
	}
}
