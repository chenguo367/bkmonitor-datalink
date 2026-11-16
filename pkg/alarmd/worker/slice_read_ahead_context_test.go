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
	"testing"
)

// Every read ahead of a Slot has its context cancelled by the Slot's end,
// the context the Slot ran under still live: a read's context is a child of
// the Slot's, which is its runner's for as long as the runner runs (the
// scheduler gives a Slot no context of its own to cancel), and a child never
// cancelled stays among the runner's children with all the Slot's context
// carries. A read ahead replayed - handed to the Slot as its slice's read -
// is the one no letting go cancels.
func TestEveryReadAheadsContextIsCancelledByTheSlotsEnd(t *testing.T) {
	fixture, request := shapedFixture(t)
	runner, stop := context.WithCancel(context.Background())
	defer stop()
	fixture.source.mu.Lock()
	first := len(fixture.source.calls)
	fixture.source.mu.Unlock()
	result, err := fixture.coordinator.Execute(runner, request)
	if err != nil || !result.Completed || result.Usage.InputSliceShapeStarted != 1 || result.Usage.InputSlicesReadAhead == 0 {
		t.Fatalf("result=%+v err=%v, want it completed in the shape, a slice read ahead and replayed", result, err)
	}
	fixture.source.mu.Lock()
	defer fixture.source.mu.Unlock()
	aheads := 0
	for _, call := range fixture.source.calls[first:] {
		if !call.readAhead {
			continue
		}
		aheads++
		if call.ctx.Err() == nil {
			t.Fatalf("the read ahead of slice %s ended with its context live, kept among the runner's children", call.slice.Digest())
		}
	}
	if aheads == 0 || runner.Err() != nil {
		t.Fatalf("%d reads ahead, runner ended %v: want reads ahead under a live runner", aheads, runner.Err())
	}
}

// A read ahead replayed - handed to the Slot as its slice's read - keeps its
// context for as long as the Slot goes on, as a read the Slot made itself
// does: what it handed on is the Slot's, and the Slot goes on with it after
// the read has ended - it reads the slices after it. Its context ends with
// the Slot. Each read asked looks at the reads ahead completed before it.
func TestAReplayedReadsContextLivesUntilTheSlotEnds(t *testing.T) {
	fixture, request := shapedFixture(t)
	var ended []string
	fixture.source.beforeRead = func() {
		fixture.source.mu.Lock()
		defer fixture.source.mu.Unlock()
		for _, call := range fixture.source.calls {
			if call.readAhead && call.completed && call.ctx.Err() != nil {
				ended = append(ended, call.slice.Digest())
			}
		}
	}
	runner, stop := context.WithCancel(context.Background())
	defer stop()
	fixture.source.mu.Lock()
	fixture.source.calls = nil
	fixture.source.mu.Unlock()
	result, err := fixture.coordinator.Execute(runner, request)
	fixture.source.beforeRead = nil
	if err != nil || !result.Completed || result.Usage.InputSlicesReadAhead < 2 {
		t.Fatalf("result=%+v err=%v, want it completed, slices read ahead and replayed before others read", result, err)
	}
	if let := result.Usage.InputSlicesReadAheadOtherLeaf + result.Usage.InputSlicesReadAheadYielded + result.Usage.InputSlicesReadAheadFailed; let != 0 {
		t.Fatalf("%d reads ahead let go, want every one replayed", let)
	}
	if len(ended) != 0 {
		t.Fatalf("slices were read after replayed reads %v had their contexts ended, want them live until the Slot ends", ended)
	}
	if lost := result.Usage.SeriesNotEvaluated; lost != ([len(lost)]uint64{}) {
		t.Fatalf("series bound and not taken up %v, want every series the slices answered taken up", lost)
	}
}
