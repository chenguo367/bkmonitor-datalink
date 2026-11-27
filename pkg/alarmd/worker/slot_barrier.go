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
	"sync/atomic"
)

// barrierAskedForTest, set only by cases, is told each time a consumer asks
// for the Slot's barrier, before it waits for the others.
var barrierAskedForTest atomic.Pointer[func()]

// barrierHeldForTest, set only by cases, is told how many consumers are in a
// completion and not stopped - the holder among them - the moment a consumer
// has the barrier, before its step: one, the holder alone.
var barrierHeldForTest atomic.Pointer[func(inside int)]

// slotBarrier is a Slot's barrier for the k streams consuming its slices
// (015 C1 section 6): a step that takes every stream of the Slot at once - a
// merge past the per-Slot caps with windows in flight, its results replaced
// past its budget - is taken by the consumer that reached it once every
// other consumer has stopped and the apply side is idle.
//
// A consumer stops at its safe points (safePoint): entering a slice's
// completion, before a merge, before a cut, at the slice's end. Stopping, it
// lands its own windows first, so every lane of the apply pipe is empty
// once all have stopped; the window it fills stays in its hand, and what the
// Slot holds does not grow. A consumer outside a completion - reading,
// waiting on the dispatch or for the slices below its own (sliceLedger) -
// is counted as stopped: it touches nothing of the Slot's until it next
// enters one, where it stops.
type slotBarrier struct {
	mu sync.Mutex
	// holder is the consumer taking the barrier, nil with none; inside the
	// consumers in a completion and not stopped.
	holder *streamedExecution
	inside map[*streamedExecution]bool
	// changed is closed, and replaced, on every change.
	changed chan struct{}
}

// changedLocked wakes every wait. Under mu.
func (barrier *slotBarrier) changedLocked() {
	if barrier.changed != nil {
		close(barrier.changed)
		barrier.changed = nil
	}
}

// waitLocked waits, mu released meanwhile, until something changes or ctx
// ends. Under mu, held again on return.
func (barrier *slotBarrier) waitLocked(ctx context.Context) error {
	if barrier.changed == nil {
		barrier.changed = make(chan struct{})
	}
	changed := barrier.changed
	barrier.mu.Unlock()
	defer barrier.mu.Lock()
	select {
	case <-changed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sideBySide says whether this stream is one of k consumers, which barriers
// hold; with one stream no barrier is needed and every call here is none.
func (stream *streamedExecution) sideBySide() bool { return stream.dispatch != nil }

// enterCompletion has this consumer enter a slice's completion, waiting out
// a barrier another holds.
func (stream *streamedExecution) enterCompletion(ctx context.Context) error {
	if !stream.sideBySide() {
		return nil
	}
	barrier := &stream.barrier
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	for barrier.holder != nil && barrier.holder != stream {
		if err := barrier.waitLocked(ctx); err != nil {
			return err
		}
	}
	if barrier.inside == nil {
		barrier.inside = make(map[*streamedExecution]bool)
	}
	barrier.inside[stream] = true
	barrier.changedLocked()
	return nil
}

// leaveCompletion has this consumer leave a slice's completion.
func (stream *streamedExecution) leaveCompletion() {
	if !stream.sideBySide() {
		return
	}
	barrier := &stream.barrier
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	delete(barrier.inside, stream)
	barrier.changedLocked()
}

// safePoint stops this consumer while another holds the barrier: its own
// windows landed, it waits for the barrier's release and goes on.
func (stream *streamedExecution) safePoint(ctx context.Context) error {
	if !stream.sideBySide() {
		return nil
	}
	barrier := &stream.barrier
	barrier.mu.Lock()
	held := barrier.holder != nil && barrier.holder != stream
	barrier.mu.Unlock()
	if !held {
		return nil
	}
	if err := stream.drainApply(ctx); err != nil {
		return err
	}
	return stream.stopped(ctx, func() error {
		barrier.mu.Lock()
		defer barrier.mu.Unlock()
		for barrier.holder != nil && barrier.holder != stream {
			if err := barrier.waitLocked(ctx); err != nil {
				return err
			}
		}
		return nil
	})
}

// stopped runs wait - a wait that touches nothing of the Slot's - with this
// consumer counted as stopped, and enters its completion again after.
func (stream *streamedExecution) stopped(ctx context.Context, wait func() error) error {
	if !stream.sideBySide() {
		return wait()
	}
	stream.leaveCompletion()
	err := wait()
	if enterErr := stream.enterCompletion(ctx); err == nil {
		err = enterErr
	}
	return err
}

// holdBarrier takes the Slot's barrier: once every other consumer has
// stopped and this one's windows have landed, the apply side is idle and
// the step that takes every stream of the Slot runs; release lets them go
// on. A barrier another holds is waited out first, stopped. Asked again by
// the consumer holding it - a merge past a cap behind the barrier its
// replacement holds - it is held already, and released by the outer hold.
func (stream *streamedExecution) holdBarrier(ctx context.Context) (release func(), err error) {
	if !stream.sideBySide() {
		return func() {}, nil
	}
	barrier := &stream.barrier
	barrier.mu.Lock()
	again := barrier.holder == stream
	barrier.mu.Unlock()
	if again {
		return func() {}, nil
	}
	if note := barrierAskedForTest.Load(); note != nil {
		(*note)()
	}
	barrier.mu.Lock()
	for barrier.holder != nil && barrier.holder != stream {
		// Stopped while another holds it.
		delete(barrier.inside, stream)
		barrier.changedLocked()
		if err := barrier.waitLocked(ctx); err != nil {
			barrier.mu.Unlock()
			return nil, err
		}
	}
	barrier.holder = stream
	if barrier.inside == nil {
		barrier.inside = make(map[*streamedExecution]bool)
	}
	barrier.inside[stream] = true
	barrier.changedLocked()
	for others := true; others; {
		others = false
		for consumer := range barrier.inside {
			if consumer != stream {
				others = true
			}
		}
		if others {
			if err := barrier.waitLocked(ctx); err != nil {
				barrier.holder = nil
				barrier.changedLocked()
				barrier.mu.Unlock()
				return nil, err
			}
		}
	}
	if note := barrierHeldForTest.Load(); note != nil {
		(*note)(len(barrier.inside))
	}
	barrier.mu.Unlock()
	release = func() {
		barrier.mu.Lock()
		defer barrier.mu.Unlock()
		barrier.holder = nil
		barrier.changedLocked()
	}
	// A consumer outside its completion may still have windows of its slice
	// before being written: a fence handed after every task handed so far is
	// written after them, and once it lands the apply side is idle.
	if err := stream.handOver(ctx, applyTask{run: func(context.Context) error { return nil }}); err != nil {
		release()
		return nil, err
	}
	if err := stream.drainApply(ctx); err != nil {
		release()
		return nil, err
	}
	stream.addShared(func() { stream.barriers++ })
	return release, nil
}
