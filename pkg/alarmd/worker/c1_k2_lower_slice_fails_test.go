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
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// goroutinesIn reports whether a goroutine of the process is in frame.
func goroutinesIn(frame string) bool {
	stacks := make([]byte, 1<<22)
	return bytes.Contains(stacks[:runtime.Stack(stacks, true)], []byte(frame))
}

// consumerFails runs a Slot of a process of four cores - two consumers from
// its second slice, a read width of four, so the slices after its first are
// read ahead as it is read - over the slices its last read left, failed by
// fail, given the slices' digests by number. fail may hold a read until
// release, which closes once waiting, said by what, holds. It returns how the
// Slot ended, under a context the Slot is not to wait out, and the exits it
// was counted under.
func consumerFails(t *testing.T, what string, waiting func() bool,
	fail func(source *slicedSource, numbered []string, release <-chan struct{}),
) (error, error, []string, *slicedFixture) {
	t.Helper()
	request := windowedRequest()
	setup := func() *slicedFixture {
		fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
		fixture.queryPermits, fixture.counts = 16, &slotCounts{}
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		return fixture
	}
	// One stream lands the slices in the order they are numbered.
	learn := setup()
	if _, err := learn.coordinator.Execute(context.Background(), request); err != nil {
		t.Fatalf("one stream: %v", err)
	}
	var numbered []string
	for _, call := range learn.source.landed() {
		numbered = append(numbered, call.slice.Digest())
	}
	if len(numbered) < 4 {
		t.Fatalf("fixture: one stream landed %d slices, want several", len(numbered))
	}

	worker.SetSliceConsumersByCoresForTest(t, 4)
	fixture := setup()
	release := make(chan struct{})
	// Released on every way out, the case's failure too: a read held on
	// release would otherwise hold its Slot's goroutines into the cases after.
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	fail(fixture.source, numbered, release)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // fixed-wait: the Slot's context, which it is not to wait out: in production a lease's, with no end of its own
	defer cancel()
	ended := make(chan error, 1)
	go func() {
		_, err := fixture.coordinator.Execute(ctx, request)
		ended <- err
	}()
	if waiting != nil {
		testwait.Until(t, what, waiting)
	}
	releaseOnce()
	var err error
	select {
	case err = <-ended:
	case <-testwait.Expired(t):
		t.Fatal("the Slot did not end")
	}
	testwait.Until(t, "every consumer of the failed Slot ended", func() bool {
		return !goroutinesIn("(*sliceConsumer).run")
	})
	return err, ctx.Err(), fixture.counts.notCommitted, fixture
}

// lowerSliceFails is consumerFails with the slice numbered 1, the first a
// consumer is handed, held at its read until the consumer of the slice
// numbered 2 waits in its completion for the slices below its own (waitBelow),
// then failed by fail.
func lowerSliceFails(t *testing.T, fail func(source *slicedSource, lowest string, release <-chan struct{})) (error, error, []string) {
	t.Helper()
	err, ctxErr, exits, _ := consumerFails(t, "a goroutine in (*sliceLedger).waitBelow", func() bool {
		return goroutinesIn("(*sliceLedger).waitBelow")
	}, func(source *slicedSource, numbered []string,
		release <-chan struct{}) {
		fail(source, numbered[1], release)
	})
	return err, ctxErr, exits
}

// A Slot whose slice numbered 1 fails its read, while the consumer of the
// slice numbered 2 has read its own and waits in its completion for the
// slices below it, ends as the read fails (015 C1 section 7): the consumer
// above is stopped with the rest, not left waiting for a slice that will
// never complete until the Slot's context ends - the lease of a Slot with no
// short deadline. It is counted as the query that failed.
func TestASlotWhoseLowerSlicesReadFailsEndsWithoutWaitingOutItsContext(t *testing.T) {
	failed := errors.New("injected: the slice's query failed")
	err, ctxErr, exits := lowerSliceFails(t, func(source *slicedSource, lowest string, release <-chan struct{}) {
		source.failSlice = func(slice execution.InputSlice) error {
			if slice.Digest() != lowest {
				return nil
			}
			<-release
			return failed
		}
	})
	if !errors.Is(err, failed) {
		t.Fatalf("the Slot ended with %v, want the slice's query failure", err)
	}
	if ctxErr != nil {
		t.Fatalf("the Slot ended once its context did (%v): a consumer waited for the failed slice", ctxErr)
	}
	if want := []string{execution.SlotNotCommittedQueryFailed}; !reflect.DeepEqual(exits, want) {
		t.Fatalf("the Slot was counted not committed as %v, want %v", exits, want)
	}
}

// A Slot whose slice numbered 1 fails as it is completed - its answer
// breaks the completion contract - while the consumer of the slice numbered
// 2 waits in its completion for the slices below it, ends as that slice's
// completion fails, the consumer above stopped with the rest. It is counted
// as the completion that failed.
func TestASlotWhoseLowerSliceFailsItsCompletionEndsWithoutWaitingOutItsContext(t *testing.T) {
	err, ctxErr, exits := lowerSliceFails(t, func(source *slicedSource, lowest string, release <-chan struct{}) {
		source.failSlice = func(slice execution.InputSlice) error {
			if slice.Digest() == lowest {
				<-release
			}
			return nil
		}
		source.duplicateBinding = func(slice execution.InputSlice) bool { return slice.Digest() == lowest }
	})
	if err == nil {
		t.Fatal("a Slot whose slice's completion failed completed")
	}
	if ctxErr != nil {
		t.Fatalf("the Slot ended once its context did (%v): a consumer waited for the failed slice", ctxErr)
	}
	if want := []string{execution.SlotNotCommittedCompleteFailed}; !reflect.DeepEqual(exits, want) {
		t.Fatalf("the Slot was counted not committed as %v (%v), want %v", exits, err, want)
	}
}

// A Slot whose slice numbered 2 fails its read while the consumer handed the
// slice numbered 1 replays its read ahead - read as the Slot's first slice
// was, still reading - ends as the read fails: the consumer waiting for the
// rest of that read is stopped with the rest, and the read let go, rather
// than waiting for it to end with the Slot's context, the one it was read
// under. Whether the slice numbered 1 is read ahead, and replayed before the
// failure ends the Slot, is the reads' as they fall: the wait itself is held
// by TestAReplayOfAReadAheadStillReadingReturnsAsItsContextEnds.
func TestASlotWhoseSliceFailsWhileAConsumerReplaysAReadAheadEndsWithoutWaitingOutItsContext(t *testing.T) {
	failed := errors.New("injected: the slice's query failed")
	var lowest string
	err, ctxErr, exits, fixture := consumerFails(t, "", nil, func(source *slicedSource, numbered []string, _ <-chan struct{}) {
		lowest = numbered[1]
		source.hold = func(ctx context.Context, slice execution.InputSlice) {
			if slice.Digest() == lowest {
				<-ctx.Done()
			}
		}
		source.failSlice = func(slice execution.InputSlice) error {
			if slice.Digest() == numbered[2] {
				return failed
			}
			return nil
		}
	})
	if !errors.Is(err, failed) {
		t.Fatalf("the Slot ended with %v, want the slice's query failure", err)
	}
	if ctxErr != nil {
		t.Fatalf("the Slot ended once its context did (%v): a consumer waited for the read ahead it replays", ctxErr)
	}
	if want := []string{execution.SlotNotCommittedQueryFailed}; !reflect.DeepEqual(exits, want) {
		t.Fatalf("the Slot was counted not committed as %v, want %v", exits, want)
	}
	readAhead := false
	for _, call := range fixture.source.calls {
		readAhead = readAhead || call.slice.Digest() == lowest && call.readAhead
	}
	if !readAhead {
		t.Fatal("fixture: the slice numbered 1 was not read ahead, no consumer replayed a read still reading")
	}
}
