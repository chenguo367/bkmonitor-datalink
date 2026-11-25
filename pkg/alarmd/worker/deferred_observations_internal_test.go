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
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// capturedObservations keeps what it was told and under which context; one
// call can be made to panic.
type capturedObservations struct {
	observations []observability.Observation
	contexts     []context.Context
	panicAt      int
}

func (captured *capturedObservations) Observe(ctx context.Context, observation observability.Observation) {
	if captured.panicAt > 0 && len(captured.observations)+1 == captured.panicAt {
		captured.panicAt = 0
		panic("the observer fails once")
	}
	captured.observations = append(captured.observations, observation)
	captured.contexts = append(captured.contexts, ctx)
}

// saidLines says lines of a window's mutations compared, a line a series, one
// an hour older than the next, with a counted line among them, on
// coordinator under ctx.
func saidLines(coordinator *SlotExecutionCoordinator, ctx context.Context, series int) {
	now := time.Now()
	for index := range series {
		if index == series/2 {
			coordinator.observeWithCounts(ctx, observability.ComponentState, observability.StageStateApplied, execution.OperationNormal,
				now, "", "", observability.Counts{Keys: 7}, nil)
		}
		coordinator.observe(ctx, observability.ComponentState, observability.StageMutationCompared, execution.OperationNormal,
			now.Add(-time.Duration(index)*time.Hour), observability.ResultSuccess, observability.ReasonNone, nil)
	}
}

// What a window says a line a series of is held, until it lands, in no more
// than a few bytes a series - 16, its duration twice over, which its loaded
// key is charged for (loadedKeyHeldBytes) - and none of the
// window's lines is said other than it would have been (014 next9 4): said
// as the Slot takes the landing back, each line is, field by field and under
// its own context, the line the coordinator says straight away, in the order
// made. Before, each was held as a whole Observation, some 162 MB of a
// window's at the peak of a 100000x30 Slot.
func TestALineASeriesIsHeldAsItsDurationAndSaidAsItWouldHaveBeen(t *testing.T) {
	const series = 20_000
	direct := &capturedObservations{}
	saidLines(&SlotExecutionCoordinator{ports: Ports{Observer: direct}}, context.Background(), series)

	held := &capturedObservations{}
	coordinator := &SlotExecutionCoordinator{ports: Ports{Observer: deferringObserver{next: held}}}
	deferred := &deferredObservations{}
	ctx := withDeferredObservations(context.Background(), deferred)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	saidLines(coordinator, ctx, series)
	runtime.GC()
	runtime.ReadMemStats(&after)
	grown := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if grown > series*16+1<<20 {
		t.Fatalf("%d lines held take %d bytes of the heap, want at most %d: a few bytes a series", series+1, grown, series*16+1<<20)
	}
	// And within what each series' loaded key is charged for beside its
	// witness and its entry, so a window in flight holds no more than it is
	// charged for.
	entry := (uint64(unsafe.Sizeof(execution.StateKeyIdentity{})+unsafe.Sizeof("")) + 1) * 8 / 7
	if charged := loadedKeyHeldBytes - stateWitnessHeldBytes - entry; uint64(max(grown, 0))/series > charged {
		t.Fatalf("a held line takes %d bytes, its key is charged %d for it", uint64(max(grown, 0))/series, charged)
	}
	if len(held.observations) != 0 {
		t.Fatal("a line was said on the apply goroutine")
	}
	coordinator.sayDeferred(deferred.said)
	if len(held.observations) != len(direct.observations) {
		t.Fatalf("%d lines said as the window landed, want the %d said straight away", len(held.observations), len(direct.observations))
	}
	for index, want := range direct.observations {
		got := held.observations[index]
		if held.contexts[index] != ctx {
			t.Fatalf("line %d said under another context", index)
		}
		// The durations are each of when the line was made: an hour apart.
		if got.Duration.Round(time.Hour) != want.Duration.Round(time.Hour) {
			t.Fatalf("line %d took %v, want %v", index, got.Duration, want.Duration)
		}
		got.Duration, want.Duration = 0, 0
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("line %d said as %+v, want %+v", index, got, want)
		}
	}
	runtime.KeepAlive(deferred)
}

// A line the observer panics on, said as the window lands, fails nothing:
// the lines after it are said (emitObservation's rule).
func TestALineTheObserverPanicsOnAsTheWindowLandsFailsNothing(t *testing.T) {
	held := &capturedObservations{panicAt: 2}
	coordinator := &SlotExecutionCoordinator{ports: Ports{Observer: deferringObserver{next: held}}}
	deferred := &deferredObservations{}
	saidLines(coordinator, withDeferredObservations(context.Background(), deferred), 4)
	coordinator.sayDeferred(deferred.said)
	if len(held.observations) != 4 {
		t.Fatalf("%d of the 5 lines said around the one the observer panicked on, want 4", len(held.observations))
	}
}
