// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// Within one round a Query Group is moved once at most for its bytes, and
// the count correction that follows never moves one the byte moves took off
// a worker back onto it: a byte move leaves a worker past its line, and the
// correction lands a Query Group only where it fits (fits) or, every worker
// over, never one still landing (relieves, Arrived). So no round moves a
// Query Group away and back - its record moved, its owner the record's own -
// and a round's view tells a move by the owner it ends on differing from
// the record it read (015 R1: the record's own fields are not sent for it).
// Searched over fleets of two and three workers, both judgements, the round
// in the order the Leader runs it.
func TestNoRoundMovesAQueryGroupAwayAndBack(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	live := now.Add(time.Hour)
	router := NewRouter(nil)
	random := rand.New(rand.NewSource(20261126))
	pools := []uint64{200, 500, 1000, 2000}
	peakSizes := []uint64{20, 50, 100, 300, 600}
	both, onward := 0, 0
	for trial := 0; trial < 6000; trial++ {
		ids := []string{"a", "b", "c"}[:2+random.Intn(2)]
		readings := ByteReadings{Pool: map[string]uint64{}, Peak: map[execution.QueryGroupIdentity]uint64{}}
		workers := make([]ownership.WorkerRegistration, 0, len(ids))
		for _, id := range ids {
			pool := pools[random.Intn(len(pools))]
			readings.Pool[id] = pool
			workers = append(workers, byteWorker(id, pool, live))
		}
		owners := map[execution.QueryGroupIdentity]string{}
		sums := map[string]uint64{}
		for index := 0; index < 2+random.Intn(12); index++ {
			queryGroup := execution.QueryGroupIdentity(fmt.Sprintf("qg-%02d", index))
			owner := ids[random.Intn(len(ids))]
			if random.Intn(3) > 0 {
				owner = ids[0]
			}
			peak := peakSizes[random.Intn(len(peakSizes))]
			owners[queryGroup], readings.Peak[queryGroup] = owner, peak
			sums[owner] += peak
		}
		if random.Intn(2) == 0 {
			readings.Occupancy = map[string]uint64{}
			for _, id := range ids {
				readings.Occupancy[id] = sums[id] + uint64(random.Intn(3))*100
			}
		}
		describe := func() string {
			return fmt.Sprintf("trial %d: pools %v, peaks %v, occupancy %v, owners before %v", trial, readings.Pool, readings.Peak, readings.Occupancy, owners)
		}
		bytePlan := router.PlanByteMoves(owners, workers, readings, now)
		movedFrom := map[execution.QueryGroupIdentity]string{}
		moves := make([]RebalanceMove, 0, len(bytePlan.Moves))
		for _, move := range bytePlan.Moves {
			if _, twice := movedFrom[move.QueryGroup]; twice {
				t.Fatalf("%s: the byte moves moved %s twice: %+v", describe(), move.QueryGroup, bytePlan.Moves)
			}
			movedFrom[move.QueryGroup] = move.From
			moves = append(moves, RebalanceMove{QueryGroup: move.QueryGroup, From: move.From, To: move.To})
		}
		after := make(map[execution.QueryGroupIdentity]string, len(owners))
		for queryGroup, owner := range owners {
			after[queryGroup] = owner
		}
		for _, move := range moves {
			after[move.QueryGroup] = move.To
		}
		count := router.PlanRebalanceWithin(after, workers, readings.Moved(moves), now, -1)
		for _, move := range count.Moves {
			if from, moved := movedFrom[move.QueryGroup]; moved {
				if move.To == from {
					t.Fatalf("%s: %s moved for its bytes from %s and back by the count correction: bytes %+v, count %+v",
						describe(), move.QueryGroup, from, bytePlan.Moves, count.Moves)
				}
				onward++
			}
		}
		if len(moves) > 0 && len(count.Moves) > 0 {
			both++
		}
	}
	// The search reached rounds that move for both reasons, or it shows
	// nothing.
	if both == 0 {
		t.Fatal("no trial moved for its bytes and its counts in one round; the search proves nothing")
	}
	t.Logf("%d rounds moved for both, %d count moves took a Query Group the byte moves had moved on to a third worker", both, onward)
}
