// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package memoryline

import (
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHeap is the runtime's reading, set by the test.
type fakeHeap struct{ heap }

func (h *fakeHeap) read() heap { return h.heap }

// Observation grows while the live heap, what detection may still take and
// what observation was granted since the last collection fit the soft limit:
// up to the last byte, not one past it. Each refusal is counted under the
// consumer refused and nothing else is.
func TestObservationGrowsUpToTheLineAndNoFurther(t *testing.T) {
	h := &fakeHeap{heap{limit: 1000, live: 600, cycles: 1}}
	line := newLine(h.read)
	unused := uint64(100)
	line.Reserve(func() uint64 { return unused })

	if !line.Admit(ConsumerCostSummary, 200) {
		t.Fatal("200 refused with 300 of room")
	}
	if !line.Admit(ConsumerLookback, 100) {
		t.Fatal("the last 100 refused: 600 live + 100 detection + 200 granted + 100 is the limit exactly")
	}
	if line.Admit(ConsumerLookback, 1) {
		t.Fatal("one byte past the line admitted")
	}
	reading := line.Read()
	if reading.RefusedTotal[ConsumerLookback] != 1 || reading.RefusedTotal[ConsumerCostSummary] != 0 ||
		reading.AdmittedBytes[ConsumerCostSummary] != 200 || reading.AdmittedBytes[ConsumerLookback] != 100 {
		t.Fatalf("reading = %+v, want one lookback refusal and the two grants by consumer", reading)
	}
	if reading.HeadroomBytes != 300 || reading.GrantedBytes != 300 || reading.ReservedBytes != 100 || reading.LiveBytes != 600 {
		t.Fatalf("reading = %+v, want headroom 1000-600-100 = 300 with 300 granted", reading)
	}

	// Detection taking what it had room for gives observation nothing: the
	// room was never observation's, and until the next collection the live
	// heap does not show that detection took it.
	unused = 0
	if line.Admit(ConsumerSeriesSampler, 1) {
		t.Fatal("admitted into room detection had and filled")
	}

	// The next collection holds what the grants took: the grants are
	// forgotten, and the live heap is the reading.
	h.heap = heap{limit: 1000, live: 950, cycles: 2}
	if !line.Admit(ConsumerSeriesSampler, 50) || line.Admit(ConsumerSeriesSampler, 1) {
		t.Fatal("after a collection: want exactly the 50 the live heap leaves")
	}
	// Past the line: every consumer is refused, whichever asks.
	h.heap = heap{limit: 1000, live: 1200, cycles: 3}
	for _, consumer := range Consumers {
		if line.Admit(consumer, 1) {
			t.Fatalf("%s admitted past the line", consumer)
		}
	}
	if reading := line.Read(); reading.HeadroomBytes != -200 {
		t.Fatalf("headroom past the line = %d, want -200", reading.HeadroomBytes)
	}
}

// What detection gives back after a collection is still in the live heap
// that collection measured: the line reads detection's room as the
// collection left it, not again as room. A budget reserved after the
// collection is room from the next reading on.
func TestWhatDetectionGivesBackIsNotItsRoomTwice(t *testing.T) {
	// Detection held 300 of its 400 when the collection measured 700 live.
	h := &fakeHeap{heap{limit: 1000, live: 700, cycles: 1}}
	line := newLine(h.read)
	unused := uint64(100)
	line.Reserve(func() uint64 { return unused })
	if reading := line.Read(); reading.HeadroomBytes != 200 {
		t.Fatalf("headroom = %d, want 1000-700-100", reading.HeadroomBytes)
	}
	// Its Slots finish: all 400 are room again, and the 300 they held are
	// still in the 700 until the next collection.
	unused = 400
	if reading := line.Read(); reading.HeadroomBytes != 200 || reading.ReservedBytes != 100 {
		t.Fatalf("after detection gave back = %+v, want headroom 200: the 300 are live, not room as well", reading)
	}
	if !line.Admit(ConsumerCostProjection, 200) {
		t.Fatal("observation refused the 200 the line has")
	}
	// The next collection finds the 300 gone and all 400 room.
	h.heap = heap{limit: 1000, live: 600, cycles: 2}
	if reading := line.Read(); reading.HeadroomBytes != 0 || reading.ReservedBytes != 400 {
		t.Fatalf("after the next collection = %+v, want 1000-600-400", reading)
	}
	// A budget reserved since is room at once.
	line.Reserve(func() uint64 { return 50 })
	if reading := line.Read(); reading.ReservedBytes != 450 {
		t.Fatalf("reserved = %d, want the new budget's 50 beside the 400", reading.ReservedBytes)
	}
}

// Without a soft limit the runtime reports the largest one; the line then
// never refuses. A nil line admits everything and reads as empty.
func TestAProcessWithoutASoftLimitNeverRefuses(t *testing.T) {
	line := newLine((&fakeHeap{heap{limit: math.MaxInt64, live: 1 << 40, cycles: 1}}).read)
	if !line.Admit(ConsumerLookback, 1<<40) || !line.Read().LimitUnlimited {
		t.Fatal("no soft limit: refused, or not read as unlimited")
	}
	var none *Line
	if !none.Admit(ConsumerLookback, math.MaxUint64) || none.Read().HeadroomBytes != 0 || len(none.Read().RefusedTotal) != len(Consumers) {
		t.Fatal("a nil line refused, or read as other than empty")
	}
}

// The line over the running process reads the runtime: a limit and a live
// heap, and a grant within them is admitted.
func TestTheLineReadsTheRunningProcess(t *testing.T) {
	runtime.GC()
	line := New()
	reading := line.Read()
	if reading.LimitBytes == 0 || reading.LiveBytes == 0 {
		t.Fatalf("reading = %+v, want the runtime's limit and live heap", reading)
	}
	if !line.Admit(ConsumerCostProjection, 1) {
		t.Fatalf("one byte refused by a test process: %+v", reading)
	}
}

// The line over the running process takes detection's reserve as a
// collection ends. Detection that fills after the collection and before
// observation next asks is in neither the live heap nor what the budgets can
// still take by then; read at that next ask, the line would have counted the
// fill nowhere and given its room to observation.
func TestTheReserveIsTakenAsTheCollectionEnds(t *testing.T) {
	line := New()
	var unused atomic.Uint64
	unused.Store(1 << 30)
	line.Reserve(func() uint64 { return unused.Load() })
	before := line.read().cycles
	runtime.GC()
	deadline := time.Now().Add(10 * time.Second)
	for {
		line.mu.Lock()
		cycle := line.cycle
		line.mu.Unlock()
		if cycle > before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot after the collection: the line is at cycle %d, the collection made it past %d", cycle, before)
		}
		time.Sleep(time.Millisecond)
	}
	// Detection fills 900 MiB of its budget before observation asks.
	unused.Store(1<<30 - 900<<20)
	if reading := line.Read(); reading.ReservedBytes != 1<<30 {
		t.Fatalf("reserved = %d, want the %d the budgets could take as the collection ended", reading.ReservedBytes, uint64(1<<30))
	}
}

// A closed line is told of no collection after the one that ends its watch:
// a bundle that closed let its line go instead of keeping it, and every
// budget it reserves, for the life of the process.
func TestAClosedLineIsToldOfNoMoreCollections(t *testing.T) {
	line := New()
	line.Close()
	// The collection whose finalizer sees the line closed.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	line.mu.Lock()
	cycle := line.cycle
	line.mu.Unlock()
	for range 3 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	line.mu.Lock()
	defer line.mu.Unlock()
	if line.cycle != cycle {
		t.Fatalf("a closed line moved from cycle %d to %d with no reading of its own", cycle, line.cycle)
	}
}
