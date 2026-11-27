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
	"sync"
	"testing"
	"time"
)

// A consumer replaying a read ahead still reading - read under the Slot's
// context, which a stop of the consumers does not end - returns as its own
// context ends, the read let go (stopped: its read ended, what it kept given
// back), rather than waiting for the read to end with the Slot's context
// (015 C1 section 7). The read here is held as a provider's slow read is,
// until its context ends. Which reads a Slot reads ahead, lets go or reads
// again is the read width's and the share's, so the cases of the whole Slot
// reach this wait as the reads fall; this one goes to it.
func TestAReplayOfAReadAheadStillReadingReturnsAsItsContextEnds(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	ahead := &sliceReadAhead{coordinator: stream.coordinator, stream: stream, counter: &stream.aheadBytes, cancel: cancelRead,
		done: make(chan struct{})}
	ahead.changed = sync.NewCond(&ahead.mu)
	go func() {
		defer close(ahead.done)
		<-readCtx.Done()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err, _ := stream.replayAhead(ctx, ahead)
		returned <- err
	}()
	cancel()
	bound, stop := context.WithTimeout(context.Background(), 10*time.Second) // fixed-wait: a replay whose context ended returns at once; ten seconds is that bound with room for -race, the read itself never ending before it
	defer stop()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the replay returned %v, want its context's end", err)
		}
	case <-bound.Done():
		t.Fatal("the replay waited for the read ahead past its own context")
	}
	select {
	case <-ahead.done:
	default:
		t.Fatal("the read ahead the replay left still reads")
	}
	ahead.mu.Lock()
	defer ahead.mu.Unlock()
	if !ahead.stopped {
		t.Fatal("the read ahead the replay left was not stopped")
	}
}
