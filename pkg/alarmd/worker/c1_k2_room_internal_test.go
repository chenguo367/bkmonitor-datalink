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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Slot's k is the most of its cores' consumers whose slices may each hold
// L_min = S/64 in the room its share leaves beside what the dispatch holds
// once its own windows landed: L(k) = (S/2 - D)/(k + w), the reads ahead
// counted at their most, w - k of them - none have begun as k is chosen. A k
// above the one the Query Group last consumed with needs a quarter more; one
// with no k remembered takes the most whose slices hold L_min (015 C1, the
// k = 2 share, 3.2). Read four wide (16 permits, one query), two cores'
// worth, L(2) is the room over 6, L(1) over 5.
func TestASlotsKIsTheMostConsumersWhoseSlicesTheRoomLeftHoldsTheFloor(t *testing.T) {
	request := execution.SlotExecutionRequest{Contract: execution.FrozenExecutionContractRef{
		Slot: execution.SlotIdentity{QueryGroup: "query-group", EvaluationTime: 1_788_000_000}}}
	const pool = 1 << 30
	for _, cell := range []struct {
		name string
		// room is S/2 - D as a multiple of L_min, in tenths; last the k the
		// Query Group's last Slot consumed with, zero for none.
		roomTenths, last, want int
	}{
		{"room for two at six times the floor, none remembered", 60, 0, 2},
		{"room for two well past the floor, one remembered", 80, 1, 2},
		{"just short of six times the floor", 59, 0, 1},
		{"short of the floor for two, two remembered", 59, 2, 1},
		{"past the floor for two, short of a quarter more, two remembered", 66, 2, 2},
		{"past the floor for two, short of a quarter more, one remembered", 66, 1, 1},
		{"a quarter more than the floor for two, one remembered", 75, 1, 2},
		{"no room at all", 0, 2, 1},
	} {
		coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{MaxRetainedBytes: pool, QueryPermits: 16}}
		share := coordinator.qgShareBytes()
		floor := share / sliceFloorDivisor
		room := floor * uint64(cell.roomTenths) / 10
		if cell.last > 0 {
			coordinator.slicedInput.remember(request.Contract.Slot.QueryGroup, slicedReading{consumers: cell.last})
		}
		stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, request: request}}
		if width := coordinator.sliceReadWidth(judgedRequests(stream.header)); width != 4 {
			t.Fatalf("fixture: read %d wide, want 4", width)
		}
		// What the dispatch holds, by phase: output and State.
		held := share/2 - room
		stream.ownNow[retainPhaseOutput].Store(held / 2)
		stream.ownNow[retainPhaseState].Store(held - held/2)
		got := stream.consumersForRoom(2)
		if got.k != cell.want {
			t.Fatalf("%s: k = %d, want %d (room %d of share %d, floor %d)", cell.name, got.k, cell.want, room, share, floor)
		}
		// Said where it came from: the cores' k the room allowed, or fewer
		// with the limit the room left two and the floor it fell short of.
		wantFrom, wantLimit, wantFloor := observability.InputSliceConsumersFromCores, uint64(0), uint64(0)
		if cell.want < 2 {
			// The bar two fell short of: the floor, or a quarter more to
			// rise past the one remembered.
			wantFrom, wantLimit, wantFloor = observability.InputSliceConsumersFromRoom, room/6, floor
			if cell.last == 1 {
				wantFloor = floor + floor/4
			}
		}
		if got.from != wantFrom || got.limit != wantLimit || got.floor != wantFloor {
			t.Fatalf("%s: from %q limit %d floor %d, want %q %d %d", cell.name, got.from, got.limit, got.floor, wantFrom,
				wantLimit, wantFloor)
		}
	}
	// A Slot of no share is held to none: its cores' k.
	coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{QueryPermits: 16}}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, request: request}}
	if got := stream.consumersForRoom(2); got.k != 2 || got.from != observability.InputSliceConsumersFromCores {
		t.Fatalf("a Slot of no share took k = %d from %q, want its cores' 2", got.k, got.from)
	}
}
