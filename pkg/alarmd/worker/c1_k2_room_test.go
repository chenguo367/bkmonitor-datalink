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
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot whose room, once its dispatch's own windows landed, holds no two
// consumers' slices to the floor is read by its dispatch alone, and says so:
// its k came from the room, with the limit the room left two and the floor
// it fell short of (015 C1, the k = 2 share, 3.2). Its Query Group remembers
// the one stream, so the next Slot takes two again only with a quarter more
// room than the floor asks for two: one at a tenth more, two at half more.
// What the dispatch holds is set beside what it holds here
// (SetSliceRoomHeldForTest), the room read four wide - 16 permits, one query
// - L(2) the room over 6.
func TestASlotWhoseRoomHoldsNoTwoConsumersSlicesIsReadByItsDispatchAloneAndSaysSo(t *testing.T) {
	worker.SetSliceConsumersByCoresForTest(t, 4)
	worker.SetSizedSplitSeriesForTest(t, 300)
	before := slicedMaxSeries
	slicedMaxSeries = 4000
	t.Cleanup(func() { slicedMaxSeries = before })
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 0, 1200)
	for index := 0; index < 1200; index++ {
		series = append(series, slicedSeries{
			host: fmt.Sprintf("h%04d%c%c", index, alphabet[(index*13)%len(alphabet)], alphabet[(index*17+5)%len(alphabet)]), padding: 2 << 10,
		})
	}
	series = append(series, slicedSeries{padding: 2 << 10})
	fixture := newSlicedFixture(t, series, sliceableFacts(), 2000)
	fixture.queryPermits, fixture.pool = 16, 8<<20
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
		Events: fixture.ports}, 2000)
	request := windowedRequest()
	share, floor := worker.SliceShareForTest(fixture.coordinator)
	// The first Slots are read whole and split, and refine their slices with
	// room to spare: two consumers, from the cores.
	for round := 1; round <= 4; round++ {
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed {
			t.Fatalf("warming Slot %d: result=%+v err=%v", round, result, err)
		}
		if round == 4 && (result.Usage.InputSliceConsumers != 2 || result.Usage.InputSliceConsumersFrom != observability.InputSliceConsumersFromCores) {
			t.Fatalf("fixture: warmed with %d consumers from %q, want 2 from the cores", result.Usage.InputSliceConsumers,
				result.Usage.InputSliceConsumersFrom)
		}
	}
	for _, cell := range []struct {
		name string
		// limitTenths is L(2), the room over 6, in tenths of the floor;
		// rising, that the bar two fell short of is the floor and a quarter
		// more, to rise past the one remembered, rather than the floor.
		limitTenths int
		rising      bool
		consumers   uint64
		from        string
	}{
		{"half the floor for two", 5, false, 1, observability.InputSliceConsumersFromRoom},
		{"a tenth past the floor, one remembered", 11, true, 1, observability.InputSliceConsumersFromRoom},
		{"half past the floor, one remembered", 15, false, 2, observability.InputSliceConsumersFromCores},
	} {
		room := 6 * floor * uint64(cell.limitTenths) / 10
		worker.SetSliceRoomHeldForTest(t, share/2-room)
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed {
			t.Fatalf("%s: result=%+v err=%v", cell.name, result, err)
		}
		usage := result.Usage
		if usage.InputSliceConsumers != cell.consumers || usage.InputSliceConsumersFrom != cell.from {
			t.Fatalf("%s: consumed with %d streams, k from %q, want %d from %q", cell.name, usage.InputSliceConsumers,
				usage.InputSliceConsumersFrom, cell.consumers, cell.from)
		}
		bar := floor
		if cell.rising {
			bar = floor + floor/4
		}
		if cell.from == observability.InputSliceConsumersFromRoom &&
			(usage.InputSliceFloorBytes != bar || usage.InputSliceRoomLimitBytes == 0 || usage.InputSliceRoomLimitBytes >= bar) {
			t.Fatalf("%s: said the room left two %d bytes a slice against a bar of %d, want short of %d", cell.name,
				usage.InputSliceRoomLimitBytes, usage.InputSliceFloorBytes, bar)
		}
	}
}
