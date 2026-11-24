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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A value this process writes is taken in compact unchecked (014 next9 1,
// the second step): parsed once from the encoder's bytes, its frame not
// rebuilt, its view not decoded twice, its digest the write's. What keeps
// that safe is held here from the store's own entry points, the writes
// handed the frames admission encodes, as a Slot's are - with, for the
// bytes a defect of the compact form would leave, a frame written another
// way (reframed): the same record, which no compact member rebuilds to.
// The reason these read is a literal, so that each runs against the build
// before it, and fails there.

const writtenDivergedReason = "written_diverged"

// framedWrite is the frame admission hands the write of series index at
// revision, written another way when reframe says so.
func framedWrite(t *testing.T, s slot, index int, revision uint64, reframe bool) (execution.StateMutation, *execution.EncodedStateFrame) {
	t.Helper()
	mutation := seriesMutation(t, seriesIdentity(index), s.version(), revision, "")
	encoded, legacy, err := encodeRuntimePackedCounted(mutation, revision+1)
	if err != nil {
		t.Fatal(err)
	}
	if reframe {
		encoded = reframed(t, encoded)
	}
	return mutation, &execution.EncodedStateFrame{MutationDigest: mutation.MutationDigest, Revision: revision + 1, Bytes: encoded,
		LegacyRecordIDs: legacy}
}

// roundFramed loads the series, then writes them at revision with the
// frames admission would hand the write - but for the series reframe names,
// whose frame is written another way - and returns the bytes each write
// stored, held to be its frame's.
func (rig *cacheRig) roundFramed(s slot, series int, revision uint64, reframe func(int) bool) [][]byte {
	rig.t.Helper()
	mutations := make([]execution.StateMutation, series)
	frames := make([]*execution.EncodedStateFrame, series)
	for index := range mutations {
		mutations[index], frames[index] = framedWrite(rig.t, s, index, revision, reframe(index))
	}
	if _, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(),
		Items: readbackItems(s, series), OwnerFence: s.fence}); err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	fence := testApplyFence()
	fence.Fence = s.fence
	result, err := rig.store.ApplyRuntimeFenced(context.Background(), execution.StateApplyRequest{Contract: s.contract(),
		Retention: testRetention(), Items: mutations, Frames: frames}, fence)
	if err != nil {
		rig.t.Fatalf("ApplyRuntimeFenced() error = %v", err)
	}
	requireAllStatus(rig.t, result, execution.StateApplied)
	stored := make([][]byte, series)
	for index := range stored {
		key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(index))
		if err != nil {
			rig.t.Fatal(err)
		}
		if stored[index] = rig.backend.values[key]; !bytes.Equal(stored[index], frames[index].Bytes) {
			rig.t.Fatalf("fixture: series %d stored other bytes than the frame its write was handed", index)
		}
	}
	return stored
}

// requireServedAsStored holds what the cache serves of the series at s to
// what the store holds, read by a store without a cache.
func (rig *cacheRig) requireServedAsStored(s slot, series int) {
	rig.t.Helper()
	served := rig.views(s, readbackItems(s, series))
	if stored := segmentRig(rig.t, rig.backend, 0, nil).views(s, readbackItems(s, series)); !reflect.DeepEqual(served, stored) {
		rig.t.Fatalf("served %+v,\nthe store reads %+v", served, stored)
	}
}

// checkedAtWrite is whether a value written with these bytes is one in
// 1024 checked as it is taken in: the low ten bits of its digest's first
// two bytes zero.
func checkedAtWrite(raw []byte) bool {
	sum := sha1.Sum(raw)
	return binary.BigEndian.Uint16(sum[:2])%1024 == 0
}

// The first value written of a template in its Plan's table is checked as
// it is taken in: one that does not rebuild to its bytes is a defect of the
// compact form, counted once and kept as its bytes, and every value written
// after it is checked - the rest of the round kept compact by the check. The
// round after is served as the store holds it. Before, every value written
// was checked and the one written another way kept as its bytes, uncounted.
func TestTheFirstValueWrittenOfATemplateIsCheckedAsItIsTakenIn(t *testing.T) {
	const series = 4
	rig := newCacheRig(t, 1<<20)
	rig.roundFramed(first, series, 0, func(index int) bool { return index == 0 })
	stats := rig.store.StateCacheStats()
	if stats.CompactDeclined[writtenDivergedReason] != 1 || stats.Entries[StateCacheEntryWritten] != 1 ||
		stats.Entries[StateCacheEntryCompact] != series-1 {
		t.Fatalf("declined %v, entries %v; want one defect found, its value kept as its bytes and the %d after it compact",
			stats.CompactDeclined, stats.Entries, series-1)
	}
	rig.now = rig.now.Add(time.Minute)
	rig.requireServedAsStored(second, series)
}

// Once a template is held, one value written in 1024 is checked as it is
// taken in, and the rest are not. A round writing every series another way
// is found at the first value picked: the values before it, kept compact
// unchecked, go with every other compact member, the rest of the round is
// checked and kept as its bytes, and the round after is served as the store
// holds it. Before, every value written another way was kept as its bytes,
// none counted.
func TestOnceATemplateIsHeldOneValueWrittenInATousandIsChecked(t *testing.T) {
	const series = 4096
	rig := newCacheRig(t, 1<<28)
	rig.roundFramed(first, series, 0, func(int) bool { return false })
	if stats := rig.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact] != series || stats.CompactDeclined[writtenDivergedReason] != 0 {
		t.Fatalf("fixture: the first round left %v, declined %v; want every series compact", stats.Entries, stats.CompactDeclined)
	}
	rig.now = rig.now.Add(time.Minute)
	stored := rig.roundFramed(second, series, 1, func(int) bool { return true })
	at := -1
	for index, raw := range stored {
		if checkedAtWrite(raw) {
			at = index
			break
		}
	}
	if at < 0 {
		t.Fatalf("fixture: none of the %d values written is one the rule picks", series)
	}
	stats := rig.store.StateCacheStats()
	if stats.CompactDeclined[writtenDivergedReason] != 1 || stats.Entries[StateCacheEntryCompact] != 0 ||
		stats.Entries[StateCacheEntryWritten] != int64(series-at) || stats.StateDepartures[StateCacheDepartMismatch] != series {
		t.Fatalf("the first value picked is series %d: declined %v, entries %v, departures %v; want one defect, every compact "+
			"member let go with it and the %d from it on kept as their bytes", at, stats.CompactDeclined, stats.Entries,
			stats.StateDepartures, series-at)
	}
	rig.now = rig.now.Add(time.Minute)
	rig.requireServedAsStored(third, series)
}

// The sampled round of the day rebuilds every compact member whose digest is
// the stored bytes' and holds it to them. Members written unchecked match.
// One written another way - its template held, its value not one picked, so
// taken in unchecked - is found there: a mismatch at no field of its view,
// and a defect of the compact form, every compact member let go with it; the
// round goes on with what the store holds. Before, the one written another
// way was kept as its bytes, and the sampled round matched it.
func TestTheSampledRoundHoldsEveryCompactMemberToItsStoredBytes(t *testing.T) {
	const series = 4
	fourth := slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	rig := newCacheRig(t, 1<<20)
	rig.roundFramed(first, series, 0, func(int) bool { return false })
	sampledRound := func(s slot) execution.StatePreflightResult {
		t.Helper()
		items := readbackItems(s, series)
		for index := range items {
			items[index].VerifyCached = true
		}
		loaded, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(), Items: items,
			OwnerFence: s.fence})
		if err != nil {
			t.Fatal(err)
		}
		return loaded
	}
	rig.now = rig.now.Add(time.Minute)
	if loaded := sampledRound(second); loaded.CacheVerify.Matched != series || loaded.CacheVerify.Mismatched != 0 {
		t.Fatalf("a sampled round over values written unchecked: %+v; want all %d matched", loaded.CacheVerify, series)
	}

	// The last series not picked is written another way: the ones before it
	// are checked and matched first.
	lossy := -1
	for index := series - 1; index > 0 && lossy < 0; index-- {
		if _, frame := framedWrite(t, third, index, 1, true); !checkedAtWrite(frame.Bytes) {
			lossy = index
		}
	}
	if lossy < 0 {
		t.Fatal("fixture: every series written another way is one the rule picks")
	}
	rig.now = rig.now.Add(time.Minute)
	rig.roundFramed(third, series, 1, func(index int) bool { return index == lossy })
	rig.now = rig.now.Add(time.Minute)
	loaded := sampledRound(fourth)
	stats := rig.store.StateCacheStats()
	found := loaded.CacheVerify.First
	if loaded.CacheVerify.Mismatched != 1 || found == nil || found.Kind != StateCacheEntryCompact || found.Field != "bytes" ||
		found.Plan != seriesIdentity(lossy).Plan {
		t.Fatalf("a sampled round over a member written another way: %+v (first %+v); want series %d a compact mismatch at no field",
			loaded.CacheVerify, found, lossy)
	}
	if stats.CompactDeclined[writtenDivergedReason] != 1 || stats.Entries[StateCacheEntryCompact] != 0 {
		t.Fatalf("declined %v, entries %v; want the defect counted and every compact member let go", stats.CompactDeclined, stats.Entries)
	}
	if stored := segmentRig(t, rig.backend, 0, nil).views(fourth, readbackItems(fourth, series)); !reflect.DeepEqual(loaded.Items, stored) {
		t.Fatalf("the sampled round went on with %+v,\nthe store reads %+v", loaded.Items, stored)
	}
}
