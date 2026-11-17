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
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// waitCounts is a ReadAheadWaitCounter keeping what it was told.
type waitCounts struct {
	mu      sync.Mutex
	waits   map[[2]string]int
	waiting []time.Duration
}

func (counts *waitCounts) RecordReadAheadWait(outcome, duration string, _ time.Duration) {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	if counts.waits == nil {
		counts.waits = map[[2]string]int{}
	}
	counts.waits[[2]string{outcome, duration}]++
}

func (counts *waitCounts) RecordReadAheadWaiting(waited time.Duration) {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	counts.waiting = append(counts.waiting, waited)
}

func (counts *waitCounts) outcomes() map[string]int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	byOutcome := map[string]int{}
	for cell, count := range counts.waits {
		byOutcome[cell[0]] += count
	}
	return byOutcome
}

// waitingReadAhead is a read ahead of the stream waiting for room on its own
// goroutine, as a read with an answer past its limit does; done closes as its
// keep returns.
func waitingReadAhead(t *testing.T, stream *streamedExecution, ctx context.Context) *sliceReadAhead {
	t.Helper()
	done := make(chan struct{})
	ahead := &sliceReadAhead{coordinator: stream.coordinator, stream: stream, counter: &stream.aheadBytes, limit: 1,
		cancel: func() {}, done: done}
	ahead.changed = sync.NewCond(&ahead.mu)
	go func() {
		defer close(done)
		ahead.mu.Lock()
		_, _ = ahead.keep(ctx, 64)
		ahead.mu.Unlock()
	}()
	testwait.Until(t, "the read ahead waits for room", func() bool {
		stream.readAheadWaitingMu.Lock()
		defer stream.readAheadWaitingMu.Unlock()
		return stream.readAheadWaitingNow > 0
	})
	stream.aheads = append(stream.aheads, ahead)
	return ahead
}

// A wait is counted as it ends, however the read ends - let go while the
// Slot runs (another leaf), let go as the Slot ends, its context ended - and
// the Slot time with one waiting as the last of them ends: none of it waits
// for a usage the Slot computes before it lets its reads ahead go, or has
// none of when it fails.
func TestEveryReadAheadWaitIsCountedAsItEndsHoweverTheReadEnds(t *testing.T) {
	for _, end := range []struct {
		name string
		let  func(stream *streamedExecution, cancel context.CancelFunc, ahead *sliceReadAhead)
		want string
	}{
		{"let go while the Slot runs", func(stream *streamedExecution, _ context.CancelFunc, _ *sliceReadAhead) {
			stream.letGoReadAheads(readAheadOtherLeaf)
		}, "stopped"},
		{"let go as the Slot ends", func(stream *streamedExecution, _ context.CancelFunc, _ *sliceReadAhead) {
			stream.letGoReadAhead()
		}, "stopped"},
		{"the runner cancels, the Slot's end lets it go", func(stream *streamedExecution, cancel context.CancelFunc, _ *sliceReadAhead) {
			cancel() // a canceled context wakes no wait
			stream.letGoReadAhead()
		}, "stopped"},
		{"its context ended, woken by room changing", func(_ *streamedExecution, cancel context.CancelFunc, ahead *sliceReadAhead) {
			cancel()
			ahead.mu.Lock()
			ahead.changed.Broadcast()
			ahead.mu.Unlock()
			<-ahead.done
		}, "canceled"},
	} {
		t.Run(end.name, func(t *testing.T) {
			stream := newWindowedFixture(t).stream
			counts := &waitCounts{}
			stream.coordinator.ports.ReadAheadWaits = counts
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ahead := waitingReadAhead(t, stream, ctx)
			end.let(stream, cancel, ahead)
			if got := counts.outcomes(); len(got) != 1 || got[end.want] != 1 {
				t.Fatalf("waits counted %v, want one %s", got, end.want)
			}
			if len(counts.waiting) != 1 || counts.waiting[0] <= 0 {
				t.Fatalf("Slot time with one waiting told %v, want once, above zero", counts.waiting)
			}
		})
	}
}

// The waits of reads ahead side by side overlap: the Slot time with at least
// one waiting is told once, as the last of them ends, from the first to begin
// while none waited.
func TestTheSlotTimeWithAReadAheadWaitingCountsOverlapsOnce(t *testing.T) {
	stream := &streamedExecution{}
	start := time.Unix(1_800_000_000, 0)
	at := func(millis int) time.Time { return start.Add(time.Duration(millis) * time.Millisecond) }
	stream.readAheadWaitBegan(at(0))
	stream.readAheadWaitBegan(at(10))
	if _, last := stream.readAheadWaitEnded(at(30)); last {
		t.Fatal("told at the first of two to end")
	}
	if waited, last := stream.readAheadWaitEnded(at(40)); !last || waited != 40*time.Millisecond {
		t.Fatalf("told %v (last %v), want 40 ms once the last ends: the overlap once", waited, last)
	}
	stream.readAheadWaitBegan(at(100))
	if waited, last := stream.readAheadWaitEnded(at(105)); !last || waited != 5*time.Millisecond {
		t.Fatalf("told %v, want 5 ms from the next wait's own start", waited)
	}
	if _, last := stream.readAheadWaitEnded(at(200)); last {
		t.Fatal("an end with none waiting told time")
	}
}
