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
	"math"
	"sync/atomic"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// keptOutRig is a threshold rig of steady hosts, the skip word set so that
// its rounds leave their members, whose State cache takes in no more than
// room: the retained pool's room as the coordinator lends it, set by the
// test. Its rounds run from just after a sampled round, which leaves no
// member, so that none falls among them.
type keptOutRig struct {
	*shadowRig
	room atomic.Int64
	at   execution.EvaluationTime
}

const keptOutRoomOpen = int64(math.MaxInt64)

func newKeptOutRig(t *testing.T, hosts int) *keptOutRig {
	t.Helper()
	return newKeptOutRigWithin(t, hosts, 16<<20)
}

// newKeptOutRigWithin is the rig with a State cache opened with budget, its
// floor.
func newKeptOutRigWithin(t *testing.T, hosts int, budget int64) *keptOutRig {
	t.Helper()
	rig := &keptOutRig{shadowRig: newThresholdShadowRig(t, hosts, budget)}
	rig.skipWord.Store(7)
	rig.room.Store(keptOutRoomOpen)
	rig.store.StateCacheBorrower().BorrowRetainedWithin(func() uint64 { return uint64(rig.room.Load()) })
	rig.at = sampledAfter(t, frozenContract().Slot.EvaluationTime)
	return rig
}

// step runs the next round with the cache's room at room, and reads the
// cache after it.
func (rig *keptOutRig) step(room int64) state.StateCacheStats {
	rig.t.Helper()
	rig.room.Store(room)
	rig.at += 60
	if completed, err := rig.run(rig.at, nil, nil, false); !completed {
		rig.t.Fatalf("Slot at %d: completed=%t err=%v", rig.at, completed, err)
	}
	return rig.store.StateCacheStats()
}

// warm runs the rounds that take every host in as an open member its rounds
// leave unwritten.
func (rig *keptOutRig) warm(hosts int) {
	rig.t.Helper()
	for range 6 {
		rig.step(keptOutRoomOpen)
	}
	if open, _ := rig.storedMembers(rig.at); open != hosts {
		rig.t.Fatalf("the book holds %d open members, want all %d hosts", open, hosts)
	}
}

// readBackSince is what the rounds since before kept of the values they read
// back and declined as the group's kept out.
func readBackSince(before, after state.StateCacheStats) (kept, declined uint64) {
	return after.Readback[state.StateCacheReadbackKept] - before.Readback[state.StateCacheReadbackKept],
		after.Readback[state.StateCacheReadbackDeclinedRefused] - before.Readback[state.StateCacheReadbackDeclinedRefused]
}

// A Query Group the State cache keeps out is priced each round by the State
// values it reads back, and comes back once its round fits (014 next8):
// through Execute on the real State store, the Plan's book of hundreds of
// members a mark and a page in Redis, each Slot reading them before its
// State and its preflight reading under the coverage they give. The
// segments, read first in every round the group is out, add what they cost
// and price none of the values read back after them - a page is tens of KB,
// which their mean would charge each member - so a round that fits brings
// the group back, and one a little short does not.
func TestAKeptOutGroupIsPricedByItsValuesReadBackAndComesBackOnceItsRoundFits(t *testing.T) {
	const hosts = 300
	// The round's first eight read back are measured (014 next8), as the
	// design has it, not as the constant says.
	const measured = 8
	rig := newKeptOutRig(t, hosts)
	rig.warm(hosts)
	// Put out, then a round kept out whole: it measures its first eight
	// values read back and declines the rest.
	stats := rig.step(1)
	if stats.Bytes != 0 || stats.Departures[state.StateCacheDepartYielded] == 0 {
		t.Fatalf("the round with no room left the cache %d bytes, departures %v; want the group put out", stats.Bytes, stats.Departures)
	}
	before := stats
	stats = rig.step(1)
	if kept, declined := readBackSince(before, stats); kept != measured || declined != hosts-measured {
		t.Fatalf("the round kept out kept %d and declined %d of the values read back, want %d and %d", kept, declined,
			measured, hosts-measured)
	}
	// Its round as it holds it once back: every member compact beside the
	// Plan's segments.
	stats = rig.step(keptOutRoomOpen)
	if stats.Entries[state.StateCacheEntryCompact] != hosts {
		t.Fatalf("with the room open the group holds %v, want all %d members compact", stats.Entries, hosts)
	}
	held, segments := stats.Bytes, stats.SegmentBytes
	if segments <= 0 || held <= segments {
		t.Fatalf("the group holds %d bytes, %d of them segments; want both its segments and its members", held, segments)
	}
	// A round kept out counts its segments as read and again as written:
	// the room that fits it is what it holds and its segments over, a
	// twentieth to spare; a twentieth short of what it holds fits none.
	fits, short := held+segments+held/20, held-held/20
	t.Logf("held %d bytes, %d of them segments; room that fits %d, short %d", held, segments, fits, short)
	left := stats.Departures[state.StateCacheDepartYielded]
	if stats = rig.step(1); stats.Bytes != 0 || stats.Departures[state.StateCacheDepartYielded] <= left {
		t.Fatalf("the second round with no room left the cache %d bytes, departures %v; want the group put out again",
			stats.Bytes, stats.Departures)
	}
	rig.step(1)
	if stats = rig.step(short); stats.Entries[state.StateCacheEntryCompact] != 0 {
		t.Fatalf("with room a twentieth short of its round the group holds %v, want it kept out", stats.Entries)
	}
	before = stats
	stats = rig.step(fits)
	if stats.Entries[state.StateCacheEntryCompact] != hosts {
		t.Fatalf("with room for its round the group holds %v, read back %v; want all %d members back", stats.Entries,
			stats.Readback, hosts)
	}
	if kept, declined := readBackSince(before, stats); kept != hosts || declined != 0 {
		t.Fatalf("the round that came back kept %d and declined %d of the values read back, want all %d kept", kept, declined, hosts)
	}
	// And it does not swing: no departure after, every member served.
	hits, departures := stats.Lookups[state.StateCacheHit], stats.Departures
	stats = rig.step(fits)
	if stats.Departures[state.StateCacheDepartYielded] != departures[state.StateCacheDepartYielded] ||
		stats.Departures[state.StateCacheDepartOverBudget] != 0 {
		t.Fatalf("departures %v once back, %v before; want none after the test put it out", stats.Departures, departures)
	}
	if served := stats.Lookups[state.StateCacheHit] - hits; served < hosts {
		t.Fatalf("the round after it came back served %d members from the cache, want all %d", served, hosts)
	}
}
