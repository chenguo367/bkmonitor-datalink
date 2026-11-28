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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// The slice the Slot's own stream reads before its consumers begin is held
// to the limit a consumer has, by the k this Slot consumes with - known from
// its header at Begin, before a series of the slice arrives: read to one
// stream's limit, it was the largest slice of every Slot and read before any
// other began. It reads no memory of the Query Group: held so after a
// restart too, and a Slot one stream consumes - its process's cores fewer
// than the Slot before had - keeps one stream's limit, whatever that Slot
// consumed with.
func TestTheSliceTheDispatchReadsBeforeItsConsumersBeginIsHeldToTheirLimit(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxRetainedBytes: 64 << 20, QueryPermits: 16}}
	share := coordinator.qgShareBytes()
	before, beforeProcs := sliceConsumersForTest.Swap(0), sliceConsumerProcsForTest.Load()
	t.Cleanup(func() {
		sliceConsumersForTest.Store(before)
		sliceConsumerProcsForTest.Store(beforeProcs)
	})
	tree, err := inputslice.New([]string{"host"}, 4, "host")
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct {
		name  string
		procs int32
		want  uint64
	}{
		{"two consumers by eight cores", 8, share / 2 / 2 / 2},
		{"one stream by two cores", 2, share / 2 / 2},
	} {
		sliceConsumerProcsForTest.Store(cell.procs)
		dispatch := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, began: true,
			slices: &sliceReading{tree: tree}}}
		if got := dispatch.sliceLimit(); got != cell.want {
			t.Fatalf("%s: the Slot's own stream may hold %d of a slice, want %d", cell.name, got, cell.want)
		}
	}
}
