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
	"encoding/binary"
	"encoding/json"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A member's value read back is kept compact - its Plan's Levels once, its
// own part beside them, at a fraction of its bytes' charge - and served as
// the bytes read back decode: the same view, witnessed by the same digest, so
// a write after it lands on the stored bytes. The write's entry replaces it.
func TestAMembersValueReadBackIsKeptCompactAndServedAsItsBytes(t *testing.T) {
	writer := written(t)
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, 0, 1, 2))
	reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 3), OwnerFence: second.fence})
	stats := reader.store.StateCacheStats()
	if stats.Entries[StateCacheEntryCompact] != 3 || stats.Entries[StateCacheEntryReadback] != 0 {
		t.Fatalf("entries %v, declined %v; want the three members kept compact", stats.Entries, stats.CompactDeclined)
	}
	key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	if err != nil {
		t.Fatal(err)
	}
	if packed := entryCost(key, writer.backend.values[key]); 3*packed <= 2*stats.Bytes {
		t.Fatalf("three compact members charged %d, against %d each as bytes: want well under", stats.Bytes, packed)
	}
	reader.now = reader.now.Add(time.Minute)
	served, reads := reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: third.contract(), Items: readbackItems(third, 3),
		OwnerFence: third.fence})
	if reads != 0 || reader.lookups(StateCacheHit) != 3 {
		t.Fatalf("the next round: %d MGETs, %d hits; want the members served compact", reads, reader.lookups(StateCacheHit))
	}
	if stored := segmentRig(t, writer.backend, 0, nil).viewsUnder(ctx, third, readbackItems(third, 3)); !reflect.DeepEqual(served, stored) {
		t.Fatalf("served %+v,\nthe store reads %+v", served, stored)
	}
	mutations := make([]execution.StateMutation, 3)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), third.version(), 1, "")
	}
	fence := testApplyFence()
	fence.Fence = third.fence
	result, err := reader.store.ApplyRuntimeFenced(context.Background(),
		execution.StateApplyRequest{Contract: third.contract(), Retention: testRetention(), Items: mutations}, fence)
	if err != nil {
		t.Fatal(err)
	}
	requireAllStatus(t, result, execution.StateApplied)
	// The writes are kept compact in their place (014 next9 1).
	if stats := reader.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact] != 3 || stats.Entries[StateCacheEntryWritten] != 0 {
		t.Fatalf("after the write: entries %v, want the writes' kept compact in place of the ones read back", stats.Entries)
	}
}

// reframed is a framed record's bytes with its header written another way -
// indented - which decodes to the same view and rebuilds to other bytes.
func reframed(t *testing.T, raw []byte) []byte {
	t.Helper()
	header, schema, points, err := parsePackedFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	indented, err := json.MarshalIndent(header, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(packedFrameMagic), schema, packedFrameCodecNone)
	out = binary.AppendUvarint(out, uint64(len(indented)))
	return append(append(out, indented...), points...)
}

// A value that does not rebuild to the bytes read back - its header written
// another way, by another build - is kept as its bytes, counted by why, and
// served as any bytes kept are.
func TestAValueThatDoesNotRebuildToItsBytesIsKeptAsThem(t *testing.T) {
	writer := written(t)
	key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	if err != nil {
		t.Fatal(err)
	}
	writer.backend.values[key] = reframed(t, writer.backend.values[key])
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, 0))
	reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 1), OwnerFence: second.fence})
	stats := reader.store.StateCacheStats()
	if stats.Entries[StateCacheEntryReadback] != 1 || stats.CompactDeclined[StateCacheCompactRebuiltBytes] != 1 {
		t.Fatalf("entries %v, declined %v; want it kept as its bytes for rebuilding to others", stats.Entries, stats.CompactDeclined)
	}
	reader.now = reader.now.Add(time.Minute)
	served, reads := reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: third.contract(), Items: readbackItems(third, 1),
		OwnerFence: third.fence})
	stored := segmentRig(t, writer.backend, 0, nil).viewsUnder(ctx, third, readbackItems(third, 1))
	if reads != 0 || !reflect.DeepEqual(served, stored) {
		t.Fatalf("%d MGETs, served %+v,\nthe store reads %+v", reads, served, stored)
	}
}

// A compact member's charge holds what it takes of the heap: twenty thousand
// of them, of one Level and a point each, taken in and measured after a
// collection, take no more than they are charged and not less than two
// thirds of it. The reading is what one member is charged and holds.
func TestACompactMembersChargeHoldsWhatItTakesOfTheHeap(t *testing.T) {
	const members = 20000
	writer := newCacheRig(t, 0)
	mutations := make([]execution.StateMutation, members)
	items := make([]execution.StatePreflightItem, members)
	cover := coverByIdentity{}
	for index := range mutations {
		identity := seriesIdentity(index)
		mutations[index] = seriesMutation(t, identity, first.version(), 0, "")
		items[index] = execution.StatePreflightItem{Identity: identity, ApplyVersion: second.version()}
		cover[identity] = segment.Coverage{Entry: segment.Entry{Digest: identity.SeriesIdentityDigest, Start: 60}, Position: 60,
			Interval: 60, Retention: 5, Leaves: true, Renewal: segment.Renewal{At: writer.now.UnixMilli(), TTL: time.Hour.Milliseconds()}}
	}
	for start := 0; start < members; start += 4000 {
		batch := mutations[start : start+4000]
		load := make([]execution.StatePreflightItem, len(batch))
		for index, mutation := range batch {
			load[index] = execution.StatePreflightItem{Identity: mutation.Identity, ApplyVersion: first.version()}
		}
		if _, err := writer.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: first.contract(), Items: load,
			OwnerFence: first.fence}); err != nil {
			t.Fatal(err)
		}
		fence := testApplyFence()
		fence.Fence = first.fence
		if _, err := writer.store.ApplyRuntimeFenced(context.Background(), execution.StateApplyRequest{Contract: first.contract(),
			Retention: testRetention(), Items: batch}, fence); err != nil {
			t.Fatal(err)
		}
	}
	reader := segmentRig(t, writer.backend, 1<<30, nil)
	ctx := segment.WithSource(context.Background(), cover)
	// What the writer built is garbage before the reading starts, not
	// during it: collected after, it reads as the cache holding less than
	// nothing.
	mutations, writer.store = nil, nil
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	for start := 0; start < members; start += 4000 {
		if _, err := reader.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: items[start : start+4000],
			OwnerFence: second.fence}); err != nil {
			t.Fatal(err)
		}
	}
	// The reads' witnesses are the apply path's, not the cache's.
	reader.store.DropRuntimeWitnesses(frozenRef().Slot.QueryGroup, second.at)
	runtime.GC()
	runtime.ReadMemStats(&after)
	stats := reader.store.StateCacheStats()
	if stats.Entries[StateCacheEntryCompact] != members {
		t.Fatalf("entries %v, declined %v; want every member compact", stats.Entries, stats.CompactDeclined)
	}
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("a compact member: charged %d bytes, holds %d of the heap", stats.Bytes/members, held/members)
	if held > stats.Bytes || 3*held < 2*stats.Bytes {
		t.Fatalf("%d members charged %d bytes and hold %d of the heap, want the charge at or above the heap and within half of it",
			members, stats.Bytes, held)
	}
	runtime.KeepAlive(reader)
	runtime.KeepAlive(items)
	runtime.KeepAlive(cover)
}

// A Plan's sampled round reads every value the cache would serve from the
// store all the same and checks it against what the cache held (014 next7
// 3.6): a match is counted and kept; a value the store no longer holds - a
// writer the cache never heard of wrote over it - is counted a mismatch by
// the first field the two part at, let go, and the round reads the store's.
func TestASampledRoundChecksWhatTheCacheHoldsAgainstTheStore(t *testing.T) {
	writer := written(t)
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, 0, 1, 2))
	reader.loadRequest(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, 3), OwnerFence: second.fence})
	// The sampled round writes its members whole: their coverage says so.
	ctx = segment.WithSource(context.Background(), readbackCover(reader.now, false, 0, 1, 2))
	sampled := func(s slot) []execution.StatePreflightItem {
		items := readbackItems(s, 3)
		for index := range items {
			items[index].VerifyCached = true
		}
		return items
	}
	reader.now = reader.now.Add(time.Minute)
	loaded, err := reader.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: third.contract(), Items: sampled(third),
		OwnerFence: third.fence})
	if err != nil {
		t.Fatal(err)
	}
	stats := reader.store.StateCacheStats()
	if loaded.CacheVerify.Matched != 3 || loaded.CacheVerify.Mismatched != 0 || stats.Verified[StateCacheVerifyMatch] != 3 ||
		stats.Entries[StateCacheEntryCompact] != 3 {
		t.Fatalf("a sampled round over the store's own values: %+v, verified %v, entries %v; want three matches kept",
			loaded.CacheVerify, stats.Verified, stats.Entries)
	}

	// Another writer moves series 1 on, past the cache's back.
	other := newCacheRig(t, 0)
	other.backend = writer.backend
	router, err := NewFixedRouter("state-01", writer.backend)
	if err != nil {
		t.Fatal(err)
	}
	other.store, err = NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute, FenceKeys: fixedFenceKeys{testFenceKeys()},
		Now: func() time.Time { return other.now }})
	if err != nil {
		t.Fatal(err)
	}
	moved := []execution.StatePreflightItem{{Identity: seriesIdentity(1), ApplyVersion: third.version()}}
	if _, err := other.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: third.contract(), Items: moved,
		OwnerFence: third.fence}); err != nil {
		t.Fatal(err)
	}
	fence := testApplyFence()
	fence.Fence = third.fence
	result, err := other.store.ApplyRuntimeFenced(context.Background(), execution.StateApplyRequest{Contract: third.contract(),
		Retention: testRetention(), Items: []execution.StateMutation{seriesMutation(t, seriesIdentity(1), third.version(), 1, "")}}, fence)
	if err != nil {
		t.Fatal(err)
	}
	requireAllStatus(t, result, execution.StateApplied)

	fourth := slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	reader.now = reader.now.Add(time.Minute)
	loaded, err = reader.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: fourth.contract(), Items: sampled(fourth),
		OwnerFence: fourth.fence})
	if err != nil {
		t.Fatal(err)
	}
	stats = reader.store.StateCacheStats()
	first := loaded.CacheVerify.First
	if loaded.CacheVerify.Matched != 2 || loaded.CacheVerify.Mismatched != 1 || first == nil || first.Kind != StateCacheEntryCompact ||
		first.Field != "BlobRevision" || first.Plan != seriesIdentity(1).Plan {
		t.Fatalf("a sampled round after another writer: %+v (first %+v); want series 1 a compact mismatch at its revision", loaded.CacheVerify, first)
	}
	if stats.Verified[StateCacheVerifyMismatch] != 1 || stats.StateDepartures[StateCacheDepartMismatch] != 1 || stats.Entries[StateCacheEntryCompact] != 2 {
		t.Fatalf("verified %v, departures %v, entries %v; want the one let go", stats.Verified, stats.StateDepartures, stats.Entries)
	}
	stored := segmentRig(t, writer.backend, 0, nil).viewsUnder(ctx, fourth, readbackItems(fourth, 3))
	if !reflect.DeepEqual(loaded.Items, stored) {
		t.Fatalf("the sampled round read %+v,\nthe store holds %+v", loaded.Items, stored)
	}
}
