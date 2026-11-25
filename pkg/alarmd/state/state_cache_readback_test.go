// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The values read back for the State cache (014 next7 3.1, 3.2), from the
// store's own entry points: what a Slot's preflight reads of the members its
// round leaves unwritten is kept, and served on the rounds after it while
// their pages' renewals keep their keys.

var third = slot{at: 180, epoch: 1, segmentStart: 60, fence: fenceOf(3)}

// readbackCover is the series of indexes as open members of their Plan's
// segment, within it (their own point at its end), their round leaving
// them unwritten or not, their page renewed at renewedAt for an hour.
func readbackCover(renewedAt time.Time, leaves bool, indexes ...int) coverByIdentity {
	cover := coverByIdentity{}
	for _, index := range indexes {
		identity := seriesIdentity(index)
		cover[identity] = segment.Coverage{Entry: segment.Entry{Digest: identity.SeriesIdentityDigest, Start: 60}, Position: 60,
			Interval: 60, Retention: 5, Leaves: leaves, Renewal: segment.Renewal{At: renewedAt.UnixMilli(), TTL: time.Hour.Milliseconds()}}
	}
	return cover
}

func readbackItems(s slot, series int) []execution.StatePreflightItem {
	items := make([]execution.StatePreflightItem, series)
	for index := range items {
		items[index] = execution.StatePreflightItem{Identity: seriesIdentity(index), ApplyVersion: s.version(), Sharded: s.sharded}
	}
	return items
}

// loadRequest runs one preflight and says how many MGETs it took.
func (rig *cacheRig) loadRequest(ctx context.Context, request execution.StatePreflightRequest) ([]execution.RuntimeStateView, int) {
	rig.t.Helper()
	before := rig.backend.mgetCalls
	loaded, err := rig.store.LoadRuntime(ctx, request)
	if err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	return loaded.Items, rig.backend.mgetCalls - before
}

// readBack is the State entries kept of values read back, compact or not.
func readBack(stats StateCacheStats) int64 {
	return stats.Entries[StateCacheEntryReadback] + stats.Entries[StateCacheEntryCompact]
}

// written is a backend holding four series written by the first Slot, by a
// store without a cache.
func written(t *testing.T) *cacheRig {
	t.Helper()
	writer := newCacheRig(t, 0)
	writer.round(first, 4, 0)
	return writer
}

// A member its round leaves unwritten is read back once and served from the
// cache on the rounds after, decoded as the store's bytes are; a series no
// member is read back every round. The first read counts each member's miss
// as nothing kept, and each record by what became of it.
func TestAMemberItsRoundLeavesUnwrittenIsServedFromWhatItsRoundReadBack(t *testing.T) {
	writer := written(t)
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, 0, 1, 2))
	_, reads := reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 4),
		OwnerFence: second.fence})
	stats := reader.store.StateCacheStats()
	if reads == 0 || stats.Readback[StateCacheReadbackKept] != 3 || stats.Readback[StateCacheReadbackDeclinedNotMember] != 1 ||
		readBack(stats) != 3 || stats.Entries[StateCacheEntryWritten] != 0 {
		t.Fatalf("the first read: %d MGETs, read back %v, entries %v; want the three members kept and the fourth not a member",
			reads, stats.Readback, stats.Entries)
	}
	if !reflect.DeepEqual(stats.Fallbacks, map[string]uint64{StateCacheFallbackAbsent: 3, StateCacheFallbackYielded: 0,
		StateCacheFallbackOverBudget: 0, StateCacheFallbackExpired: 0, StateCacheFallbackActivation: 0}) {
		t.Fatalf("fallbacks %v, want the three members read back for nothing kept", stats.Fallbacks)
	}
	reader.now = reader.now.Add(time.Minute)
	served, reads := reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: third.contract(), Items: readbackItems(third, 4),
		OwnerFence: third.fence})
	if reads != 1 || reader.lookups(StateCacheHit) != 3 {
		t.Fatalf("the next round: %d MGETs, %d hits; want the members served and the fourth alone read", reads, reader.lookups(StateCacheHit))
	}
	stored := segmentRig(t, writer.backend, 0, nil).viewsUnder(ctx, third, readbackItems(third, 4))
	if !reflect.DeepEqual(served, stored) {
		t.Fatalf("served %+v,\nthe store reads %+v", served, stored)
	}
	if got := reader.store.StateCacheStats().Fallbacks[StateCacheFallbackAbsent]; got != 3 {
		t.Fatalf("fallbacks absent = %d after a round served, want still 3", got)
	}
}

// A value read back is kept only for an open member its round leaves
// unwritten, under the Slot's own fence, in its own preflight, of its own
// generation, while its page's renewal keeps its key past the replay age:
// each condition that does not hold keeps nothing, so the next round reads
// the series back, and is counted by what failed - or not at all for a read
// no Slot's write follows from, or one under no fence.
func TestAValueReadBackIsKeptOnlyForAMemberItsRoundLeavesUnwritten(t *testing.T) {
	writer := written(t)
	now := time.Unix(1_700_000_000, 0)
	closed := readbackCover(now, true, 0)
	entry := closed[seriesIdentity(0)]
	entry.Entry.Closed = 60
	closed[seriesIdentity(0)] = entry
	carried := seriesIdentity(0)
	carried.StateGeneration = "next-generation"
	for name, test := range map[string]struct {
		cover   coverByIdentity
		request func() execution.StatePreflightRequest
		counted map[string]uint64
	}{
		"its round writes it whole": {cover: readbackCover(now, false, 0),
			counted: map[string]uint64{StateCacheReadbackDeclinedOperation: 1}},
		"its page's renewal leaves less than the replay age": {cover: readbackCover(now.Add(-time.Hour), true, 0),
			counted: map[string]uint64{StateCacheReadbackDeclinedNoRenewal: 1}},
		"a closed member": {cover: closed, counted: map[string]uint64{StateCacheReadbackDeclinedNotMember: 1}},
		"a split strategy's Plan": {cover: readbackCover(now, true, 0), request: func() execution.StatePreflightRequest {
			items := readbackItems(second, 1)
			items[0].Sharded = true
			return execution.StatePreflightRequest{Contract: second.contract(), Items: items, OwnerFence: second.fence}
		}, counted: map[string]uint64{StateCacheReadbackDeclinedNotMember: 1}},
		"no fence": {cover: readbackCover(now, true, 0), request: func() execution.StatePreflightRequest {
			return execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 1)}
		}},
		"a read no write follows": {cover: readbackCover(now, true, 0), request: func() execution.StatePreflightRequest {
			return execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 1), OwnerFence: second.fence,
				Unwitnessed: true}
		}},
		"the generation carried from": {cover: readbackCover(now, true, 0), request: func() execution.StatePreflightRequest {
			return execution.StatePreflightRequest{Contract: second.contract(), OwnerFence: second.fence, Items: []execution.StatePreflightItem{
				{Identity: carried, ApplyVersion: second.version(), CarryFrom: seriesIdentity(0).StateGeneration}}}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			reader := segmentRig(t, writer.backend, 1<<20, nil)
			ctx := segment.WithSource(context.Background(), test.cover)
			request := execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 1), OwnerFence: second.fence}
			if test.request != nil {
				request = test.request()
			}
			views, _ := reader.loadRequest(ctx, request)
			if name == "the generation carried from" && views[0].Carried == nil {
				t.Fatalf("the carried generation read nothing: %+v", views[0])
			}
			stats := reader.store.StateCacheStats()
			for _, result := range StateCacheReadbackResults {
				if stats.Readback[result] != test.counted[result] {
					t.Fatalf("read back %v, want %v", stats.Readback, test.counted)
				}
			}
			if readBack(stats) != 0 {
				t.Fatalf("%d values kept", readBack(stats))
			}
			reader.now = reader.now.Add(time.Minute)
			next := request
			next.Contract, next.Items = third.contract(), append([]execution.StatePreflightItem(nil), request.Items...)
			for index := range next.Items {
				next.Items[index].ApplyVersion = third.version()
			}
			if _, reads := reader.loadRequest(ctx, next); reads == 0 || reader.lookups(StateCacheHit) != 0 {
				t.Fatalf("the next round: %d MGETs and %d hits, want it read back", reads, reader.lookups(StateCacheHit))
			}
		})
	}
}

// A page renewal moves the State entries of its members on: renewed under
// the Slot's fence and every key found, they live as long as the keys now
// do, past the life their read gave them; a renewal that found a key of the
// chunk gone lets every entry of it go, read back on the next round; one
// the fence refused changes nothing.
func TestAPageRenewalMovesItsMembersEntriesOnAndOneFindingAKeyGoneLetsThemGo(t *testing.T) {
	rig := newSegmentRedis(t)
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	writer := rig.storeAt(t, 0, clock)
	ctx := context.Background()
	mutations := make([]execution.StateMutation, 3)
	items := readbackItems(first, 3)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), first.version(), 0, "")
	}
	if _, err := writer.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: first.contract(), Items: items, OwnerFence: first.fence}); err != nil {
		t.Fatal(err)
	}
	result, err := writer.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: first.contract(), Retention: testRetention(),
		Items: mutations}, testApplyFence())
	if err != nil {
		t.Fatal(err)
	}
	requireAllStatus(t, result, execution.StateApplied)

	reader := rig.storeAt(t, 1<<20, clock)
	cover := coverByIdentity{}
	digests := make([]execution.SeriesIdentityDigest, 3)
	for index := range digests {
		identity := seriesIdentity(index)
		digests[index] = identity.SeriesIdentityDigest
		// Renewed a minute ago for two: its life, less the replay age, ends
		// a minute from now.
		cover[identity] = segment.Coverage{Entry: segment.Entry{Digest: identity.SeriesIdentityDigest, Start: 60}, Position: 60,
			Interval: 60, Retention: 5, Leaves: true, Renewal: segment.Renewal{At: now.Add(-time.Minute).UnixMilli(), TTL: 2 * time.Minute.Milliseconds()}}
	}
	covered := segment.WithSource(ctx, cover)
	load := func(s slot) StateCacheStats {
		t.Helper()
		if _, err := reader.LoadRuntime(covered, execution.StatePreflightRequest{Contract: s.contract(), Items: readbackItems(s, 3),
			OwnerFence: s.fence}); err != nil {
			t.Fatal(err)
		}
		return reader.StateCacheStats()
	}
	if stats := load(second); stats.Readback[StateCacheReadbackKept] != 3 {
		t.Fatalf("read back %v, want the three members kept", stats.Readback)
	}
	plan := segment.PlanKey{Plan: seriesIdentity(0).Plan, Generation: seriesIdentity(0).StateGeneration}
	renewal := segment.PlanRenewal{Key: plan, Lifetime: segmentLifetime(), Pages: []segment.PageRenewal{{Page: 0, Members: digests}}}
	renewSegments(t, reader, testApplyFence(), renewal)
	if got := reader.StateCacheStats().MemberRenewals; got[StateCacheMemberRenewed] != 3 || got[StateCacheMemberDroppedMissing] != 0 {
		t.Fatalf("member renewals %v, want the three entries renewed", got)
	}
	// Past the life the read gave them - a minute less a second - within the
	// renewal's, and touched within two of their Plan's intervals, as a
	// round every minute touches them.
	now = now.Add(90 * time.Second)
	if stats := load(third); stats.Lookups[StateCacheHit] != 3 {
		t.Fatalf("lookups %v past the read's life, want the three renewed entries served", stats.Lookups)
	}

	stale := testApplyFence()
	stale.Fence.OwnerEpoch = 99
	renewSegments(t, reader, stale, renewal)
	if got := reader.StateCacheStats(); got.MemberRenewals[StateCacheMemberRenewed] != 3 || readBack(got) != 3 {
		t.Fatalf("a refused renewal: renewals %v, entries %v, want nothing changed", got.MemberRenewals, got.Entries)
	}

	key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	rig.clients[0].Del(ctx, key)
	renewSegments(t, reader, testApplyFence(), renewal)
	if got := reader.StateCacheStats(); got.MemberRenewals[StateCacheMemberDroppedMissing] != 3 || readBack(got) != 0 ||
		got.StateDepartures[StateCacheDepartRenewalMissing] != 3 {
		t.Fatalf("a renewal finding a key gone: renewals %v, entries %v, departures %v; want the chunk's three let go",
			got.MemberRenewals, got.Entries, got.StateDepartures)
	}
	now = now.Add(time.Minute)
	fourth := slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	if stats := load(fourth); stats.Lookups[StateCacheHit] != 3 || stats.Fallbacks[StateCacheFallbackAbsent] != 6 {
		t.Fatalf("after the chunk was let go: lookups %v, fallbacks %v; want no more hits and the members read back for nothing kept",
			stats.Lookups, stats.Fallbacks)
	}
}

// The cache's budget follows what the Query Groups it holds read a round
// (014 next9 1): the State entries it holds, each at the cache's mean charge
// and never less than a compact member's estimate, a quarter more, beside
// what the segments hold; never below the budget the store was opened with,
// and that while it holds nothing and keeps nothing out. Passed the soft
// limit, the cache gives back what it is passed by, its Query Group whole,
// read back on the next round. The group's values come in as a group kept
// out takes them in: its first round read back, larger than the floor, is
// kept out, and the resize after sizes the budget for what it read.
func TestTheCachesBudgetFollowsWhatItsGroupsReadAndItGivesBackPastTheSoftLimit(t *testing.T) {
	const floor, series = 4096, 20
	// A compact member's estimate, as the design has it (014 n8), not as the
	// constant says.
	const compactCharge = 528
	writer := newCacheRig(t, 0)
	writer.round(first, series, 0)
	rig := segmentRig(t, writer.backend, floor, nil)
	rig.store.SizeStateCache(0)
	if budget := rig.store.StateCacheStats().BudgetBytes; budget != floor {
		t.Fatalf("budget %d holding nothing and keeping nothing out, want the floor %d", budget, floor)
	}
	readBackRound(rig, second, series)
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != 0 {
		t.Fatalf("fixture: a round of %d values read back held in a cache of %d bytes (%v entries), want it kept out", series,
			floor, stats.Entries)
	}
	rig.store.SizeStateCache(0)
	rig.now = rig.now.Add(time.Minute)
	readBackRound(rig, third, series)
	stats := rig.store.StateCacheStats()
	if readBack(stats) != series || stats.QueryGroups != 1 {
		t.Fatalf("fixture: %v entries in %d groups", stats.Entries, stats.QueryGroups)
	}
	charge := max((stats.Bytes-stats.SegmentBytes)/series, compactCharge)
	want := series*charge*5/4 + stats.SegmentBytes
	if want <= floor {
		t.Fatalf("fixture: %d entries of %d bytes are sized at %d, not past the floor %d", series, charge, want, floor)
	}
	rig.store.SizeStateCache(0)
	if budget := rig.store.StateCacheStats().BudgetBytes; budget != want {
		t.Fatalf("budget %d for %d entries of %d bytes each, want %d", budget, series, charge, want)
	}
	rig.store.SizeStateCache(1)
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != 0 || stats.StateDepartures[StateCacheDepartYielded] != series {
		t.Fatalf("passed the soft limit by a byte: %d groups, departures %v; want the group given back whole", stats.QueryGroups,
			stats.StateDepartures)
	}
	rig.now = rig.now.Add(time.Minute)
	if reads, _ := rig.round(slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, series, 1); reads == 0 ||
		rig.lookups(StateCacheMissYielded) != series {
		t.Fatalf("reads %d, miss_yielded %d; want the group given back read from the store", reads, rig.lookups(StateCacheMissYielded))
	}
}

// The budget follows what the rounds read, not what the cache holds (014
// next9 1): entries a round read back that no later round reads stay in the
// cache until they leave, but once the round before has read them no more
// they do not raise the budget. The first round of them all, larger than the
// floor, is kept out, and the resize after sizes the budget for it.
func TestTheBudgetIsWhatTheRoundsReadNotWhatTheCacheHolds(t *testing.T) {
	const floor, read, unread = 4096, 20, 200
	// A compact member's estimate, as the design has it (014 n8).
	const compactCharge = 528
	writer := newCacheRig(t, 0)
	writer.round(first, read+unread, 0)
	rig := segmentRig(t, writer.backend, floor, nil)
	readBackRound(rig, second, read+unread)
	rig.store.SizeStateCache(0)
	readBackRound(rig, third, read+unread)
	readBackRound(rig, slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, read)
	readBackRound(rig, slot{at: 300, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, read)
	stats := rig.store.StateCacheStats()
	if readBack(stats) != read+unread {
		t.Fatalf("fixture: the cache holds %v, want all %d entries read back", stats.Entries, read+unread)
	}
	charge := max((stats.Bytes-stats.SegmentBytes)/(read+unread), compactCharge)
	rig.store.SizeStateCache(0)
	if budget, want := rig.store.StateCacheStats().BudgetBytes, max(floor, read*charge*5/4+stats.SegmentBytes); budget != want {
		t.Fatalf("budget %d with %d entries held and %d read a round, want %d: the %d read at %d bytes each", budget, read+unread,
			read, want, read, charge)
	}
}

// readBackRound reads the first series series back at s as a Slot of a
// frozen Plan does, each an open member of its Plan's segment its round
// leaves unwritten.
func readBackRound(rig *cacheRig, s slot, series int) {
	rig.t.Helper()
	indexes := make([]int, series)
	for index := range indexes {
		indexes[index] = index
	}
	ctx := segment.WithSource(context.Background(), readbackCover(rig.now, true, indexes...))
	rig.loadRequest(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: readbackItems(s, series), OwnerFence: s.fence})
}

// A Query Group put out by a round of writes kept as their bytes - a replayed
// round, a sampled round of the day, its members written whole - comes back
// once a round of its values read back, compact, fits: each refused round is
// priced by what it measures itself, not by the round before.
func TestAGroupPutOutByARoundOfBytesComesBackOnceItsCompactRoundFits(t *testing.T) {
	const members = 12
	writer := newCacheRig(t, 0)
	writer.round(first, members, 0)
	indexes := make([]int, members)
	for index := range indexes {
		indexes[index] = index
	}
	one := compactChargeOf(t, writer)
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	var room atomic.Int64
	bytesCost := entryCost(cacheKey("big", 0), make([]byte, 1000))
	room.Store(bytesCost + bytesCost/2)
	reader.store.StateCacheBorrower().BorrowRetainedWithin(func() uint64 { return uint64(room.Load()) })
	// The round of writes as bytes: the group is put out at its second.
	group := second.contract().Slot.QueryGroup
	for index := range members {
		key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(index))
		if err != nil {
			t.Fatal(err)
		}
		reader.store.cache.applied(stateCacheEntry{key: key, group: group, identity: seriesIdentity(index), raw: make([]byte, 1000),
			fence: second.fence, epoch: 1, segmentStart: 60, slot: second.at, usableUntil: reader.now.Add(time.Hour)}, nil)
	}
	if left := reader.store.StateCacheStats().Departures[StateCacheDepartYielded]; left != 1 {
		t.Fatalf("the round of bytes left %d departures, want the group put out once", left)
	}
	room.Store(members*one + one/2)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, indexes...))
	for _, evaluation := range []execution.EvaluationTime{180, 240} {
		reader.now = reader.now.Add(time.Minute)
		s := slot{at: evaluation, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
		reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: readbackItems(s, members), OwnerFence: s.fence})
	}
	if held := reader.store.StateCacheStats().Entries[StateCacheEntryCompact]; held != members {
		t.Fatalf("after a round of bytes and a round read back compact the group holds %d members, want all %d back", held, members)
	}
}

// Each refused round measures its first stateCacheRefusalMeasured values read
// back and charges the rest at their mean, and only theirs: a first value
// that does not compress, as costly as bytes, does not price the round alone,
// and a round before it, priced otherwise, does not either. The round's
// segments - read before its State, a page tens of KB - and its writes add
// what they cost and price nothing.
func TestARefusedRoundIsChargedAtTheMeanOfItsFirstMeasuredValues(t *testing.T) {
	// The round's first eight (014 next8), as the design has it, not as
	// the constant says.
	const measured = 8
	cache := bareCache(1)
	const group = execution.QueryGroupIdentity("big")
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.refused[group] = &stateCacheRefusal{bound: StateCacheDepartYielded}
	cache.refused[group].begin(60, 1_000_000, false, true)
	const mark, page, write = 300, 40_000, 1500
	const costly, cheap = 1600, 520
	capacity := int64(100)
	if cache.admitLocked(group, 120, mark, true, false, capacity) {
		t.Fatal("the next round's mark was taken in")
	}
	cache.admitLocked(group, 120, page, true, false, capacity)
	if cache.refusedReadbackLocked(group, 120) {
		t.Fatal("the round's first value read back, after its segments, was declined before it was measured")
	}
	cache.admitLocked(group, 120, costly, false, true, capacity)
	for range measured - 1 {
		if cache.refusedReadbackLocked(group, 120) {
			t.Fatal("a value of the round's first eight was declined before it was measured")
		}
		cache.admitLocked(group, 120, cheap, false, true, capacity)
	}
	cache.admitLocked(group, 120, write, false, false, capacity)
	for range 4 {
		if !cache.refusedReadbackLocked(group, 120) {
			t.Fatal("a value past the round's first eight was left to be measured")
		}
	}
	refusal := cache.refused[group]
	mean := int64(costly+(measured-1)*cheap) / measured
	want := int64(mark + page + write + costly + (measured-1)*cheap + 4*mean)
	if refusal.size != want || refusal.members != measured+7 {
		t.Fatalf("the round is charged %d over %d values, want %d over %d: its segments and write as they cost, the values read "+
			"back past its first eight at the mean of those, %d", refusal.size, refusal.members, want, measured+7, mean)
	}
}

// compactChargeOf is what a compact member of writer's series is charged.
func compactChargeOf(t *testing.T, writer *cacheRig) int64 {
	t.Helper()
	probe := segmentRig(t, writer.backend, 1<<20, nil)
	probe.loadRequest(segment.WithSource(context.Background(), readbackCover(probe.now, true, 0)),
		execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 1), OwnerFence: second.fence})
	stats := probe.store.StateCacheStats()
	if stats.Entries[StateCacheEntryCompact] != 1 || stats.Bytes <= 0 {
		t.Fatalf("the probe kept %v, %d bytes; want one compact member", stats.Entries, stats.Bytes)
	}
	return stats.Bytes
}
