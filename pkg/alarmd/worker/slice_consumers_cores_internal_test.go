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
	"runtime"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// k by the process's cores (015 C1 section 9): two from four
// cores, one below - within the Slot's read width, a read of it left to
// read ahead (k at most w - 1), and one until the Slot has begun and its
// header says the width, or for a Slot not read in slices.
func TestAProcessOfFourCoresOrMoreConsumesASlotsSlicesTwoAtATimeWithinItsReadWidth(t *testing.T) {
	for procs, want := range map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 6: 2, 8: 2, 16: 2} {
		if got := sliceConsumersByCores(procs); got != want {
			t.Errorf("a process of %d cores takes k = %d, want %d", procs, got, want)
		}
	}
	tree, err := inputslice.New([]string{"host"}, 4, "host")
	if err != nil {
		t.Fatal(err)
	}
	stream := func(permits int, began bool, slices *sliceReading) *streamedExecution {
		coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{QueryPermits: permits}}
		return &streamedExecution{slotStream: &slotStream{coordinator: coordinator, began: began, slices: slices}}
	}
	before, beforeProcs := sliceConsumersForTest.Swap(0), sliceConsumerProcsForTest.Swap(8)
	t.Cleanup(func() {
		sliceConsumersForTest.Store(before)
		sliceConsumerProcsForTest.Store(beforeProcs)
	})
	for _, slot := range []struct {
		name    string
		permits int
		began   bool
		slices  *sliceReading
		want    int
	}{
		{"four wide", 16, true, &sliceReading{tree: tree}, 2},
		{"three wide", 6, true, &sliceReading{tree: tree}, 2},
		{"two wide", 4, true, &sliceReading{tree: tree}, 1},
		{"no permits, two wide", 0, true, &sliceReading{tree: tree}, 1},
		{"not begun", 16, false, &sliceReading{tree: tree}, 1},
		{"read whole", 16, true, &sliceReading{}, 1},
		{"not sliced", 16, true, nil, 1},
	} {
		if got := stream(slot.permits, slot.began, slot.slices).sliceConsumers(); got != slot.want {
			t.Errorf("%s at eight cores: k = %d, want %d", slot.name, got, slot.want)
		}
	}
	sliceConsumerProcsForTest.Store(2)
	if got := stream(16, true, &sliceReading{tree: tree}).sliceConsumers(); got != 1 {
		t.Errorf("four wide at two cores: k = %d, want 1", got)
	}
	sliceConsumersForTest.Store(3)
	if got := stream(4, false, &sliceReading{tree: tree}).sliceConsumers(); got != 3 {
		t.Errorf("k set by a case is %d, want the 3 it set, whatever the width", got)
	}
}

// A process of eight cores consumes a Slot's slices on the Slot's own stream,
// one at a time, as one of one core does: k = 2 is taken only where a case
// asks for it.
func TestAProcessOfEightCoresConsumesASlotsSlicesOneAtATime(t *testing.T) {
	tree, err := inputslice.New([]string{"host"}, 4, "host")
	if err != nil {
		t.Fatal(err)
	}
	before, beforeProcs := sliceConsumersForTest.Swap(0), sliceConsumerProcsForTest.Swap(0)
	cores := runtime.GOMAXPROCS(8)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(cores)
		sliceConsumersForTest.Store(before)
		sliceConsumerProcsForTest.Store(beforeProcs)
	})
	coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{QueryPermits: 16}}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, began: true, slices: &sliceReading{tree: tree}}}
	if got := stream.sliceConsumers(); got != 1 {
		t.Fatalf("a Slot four wide, begun, at eight cores: k = %d, want 1", got)
	}
	sliceConsumerProcsForTest.Store(8)
	if got := stream.sliceConsumers(); got != 2 {
		t.Fatalf("the same Slot where a case asks for eight cores: k = %d, want 2", got)
	}
}
