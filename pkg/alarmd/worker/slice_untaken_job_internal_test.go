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
)

// A read ahead the dispatch handed in a job no consumer took - the consumer
// freed by its slice let go and split, ended at that verdict as the Slot's
// consumers stopped - is stopped with them, by the stop every way
// consumeSideBySide returns runs (stopConsumers): its read, held as a
// provider's slow read is until its context ends, ends, and the read ahead is
// stopped, what it kept given back. Nothing else holds it once the dispatch
// took it (takeReadAhead). The window is a consumer choosing its context's end
// over a verdict already sent, which the cases of the whole Slot reach too
// seldom to tell.
func TestAReadAheadInAJobNoConsumerTookIsStoppedAsTheConsumersStop(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	readCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ahead := &sliceReadAhead{coordinator: stream.coordinator, stream: stream, counter: &stream.aheadBytes, cancel: cancel,
		done: make(chan struct{})}
	ahead.changed = sync.NewCond(&ahead.mu)
	go func() {
		defer close(ahead.done)
		<-readCtx.Done()
	}()
	// The consumer has ended: nothing in wait, no event or yield to come.
	consumer := &sliceConsumer{jobs: make(chan sliceJob, 1)}
	consumer.jobs <- sliceJob{ahead: ahead}
	var wait sync.WaitGroup
	stopConsumers([]*sliceConsumer{consumer}, make(chan struct{}), func() {}, &wait, make(chan sliceEvent), make(chan yieldRequest))
	select {
	case <-ahead.done:
	default:
		t.Fatal("the read ahead of the job no consumer took still reads")
	}
	ahead.mu.Lock()
	defer ahead.mu.Unlock()
	if !ahead.stopped {
		t.Fatal("the read ahead of the job no consumer took was not stopped")
	}
}
