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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slicedRun is what one Execute of a sliced fixture read and wrote.
type slicedRun struct {
	reads     []string
	saved     map[execution.StateKeyIdentity]execution.MutationDigest
	events    int
	slices    uint64
	discarded uint64
	clamped   uint64
	completed bool
}

func readSliced(t *testing.T, fixture *slicedFixture) slicedRun {
	t.Helper()
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot completed in slices", result, err)
	}
	run := slicedRun{saved: map[execution.StateKeyIdentity]execution.MutationDigest{}, events: fixture.ports.eventCount,
		slices: result.Usage.InputSlices, discarded: result.Usage.InputSlicesDiscarded, completed: result.Completed,
		clamped: result.Usage.WindowOutsideClamped + result.Usage.SliceOthersClamped}
	for _, call := range fixture.source.calls {
		run.reads = append(run.reads, call.slice.Digest())
	}
	for key, mutation := range fixture.state.saved {
		run.saved[key] = mutation.MutationDigest
	}
	return run
}

// startedSlicedState counts the State applies started.
type startedSlicedState struct {
	*persistedRetryState
	started atomic.Int64
}

func (state *startedSlicedState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	state.started.Add(1)
	return state.persistedRetryState.ApplyRuntime(ctx, request)
}

// A Slot read in slices one at a time reads its next slice while the last
// window of the slice before is written (014 next9 4, C5): a State apply
// starts while a slice's read does, which it does only if the slice's end
// does not wait for its last window to land. The slice's limit counts the
// Slot's side alone, so the slices read, how they are cut and let go, and
// what is written are what the Slot reads and writes with each window landed
// before the next is filled; and its window accounting never took off more
// than it held.
func TestTheNextSliceIsReadWhileTheLastWindowOfTheSliceBeforeIsWritten(t *testing.T) {
	pipelined := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	// One slice at a time: no read ahead.
	pipelined.queryPermits = 2
	state := &startedSlicedState{persistedRetryState: pipelined.state}
	pipelined.coordinator = pipelined.build(t, worker.Ports{Activation: pipelined.ports, State: state, GapGuard: pipelined.ports,
		Events: pipelined.ports}, 200)
	// Each read starts 100 ms late: a State apply started then is a window
	// written while the next slice is read.
	var overlapped atomic.Bool
	pipelined.source.beforeRead = func() {
		before := state.started.Load()
		time.Sleep(100 * time.Millisecond) // fixed-wait: a read that takes its time, the subject of the case
		if state.started.Load() > before {
			overlapped.Store(true)
		}
	}
	run := readSliced(t, pipelined)
	if !overlapped.Load() {
		t.Fatal("no slice was read while a window of the slice before was written")
	}

	worker.SetApplySeriallyForTest(t)
	fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	fixture.queryPermits = 2
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
		Events: fixture.ports}, 200)
	serial := readSliced(t, fixture)
	if run.slices < 2 {
		t.Fatalf("fixture: %d slices read, want the Slot read in slices", run.slices)
	}
	if !reflect.DeepEqual(run, serial) || run.clamped != 0 {
		t.Fatalf("pipelined %+v, serial %+v; want the same slices read and let go and the same written, nothing clamped", run, serial)
	}
}

// A series alone past the slice limit of the Slot's half of its share and
// within the limit of a Slot with no window in flight is taken in once the
// windows in flight have landed (014 next9 4): the Slot completes as it does
// with each window landed before the next is filled, nothing refused.
func TestASeriesPastTheSlotsHalfIsTakenInOnceTheWindowsInFlightHaveLanded(t *testing.T) {
	series := append(slicedHosts(4, 8<<10), slicedSeries{host: "host-huge", padding: 500 << 10})
	pipelined := readSliced(t, newSlicedFixture(t, series, sliceableFacts(), 200))
	worker.SetApplySeriallyForTest(t)
	serial := readSliced(t, newSlicedFixture(t, series, sliceableFacts(), 200))
	if !reflect.DeepEqual(pipelined, serial) || len(pipelined.saved) != len(series) {
		t.Fatalf("pipelined %+v, serial %+v; want every series written as serially", pipelined, serial)
	}
}
