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
	"runtime"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A value written with the frame its admission encoded - as a Slot's writes
// are - is taken into the State cache compact from the header its encoder
// framed it from (014 next9 1, the third step): every one is kept compact,
// none found to part from its bytes, the first of its template checked; the
// sampled round matches each against the bytes stored, and the round after
// is served as the store holds it.
func TestAValueWrittenWithItsAdmittedFrameIsTakenInFromItsEncodersHeader(t *testing.T) {
	const series = 64
	ctx := context.Background()
	rig := newCacheRig(t, 1<<20)
	mutations := make([]execution.StateMutation, series)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), first.version(), 0, "")
	}
	admitted, err := rig.store.AdmitRuntime(ctx, execution.StateApplyRequest{Contract: first.contract(), Retention: testRetention(),
		Items: mutations})
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]*execution.EncodedStateFrame, series)
	for index, item := range admitted.Items {
		if item.Frame == nil || item.Frame.Parts == nil || item.Frame.Parts.RetainedBytes() == 0 {
			t.Fatalf("fixture: admission framed series %d as %+v; want a frame with the parts it was framed from", index, item.Frame)
		}
		frames[index] = item.Frame
	}
	if _, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: first.contract(), Items: readbackItems(first, series),
		OwnerFence: first.fence}); err != nil {
		t.Fatal(err)
	}
	fence := testApplyFence()
	fence.Fence = first.fence
	result, err := rig.store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: first.contract(), Retention: testRetention(),
		Items: mutations, Frames: frames}, fence)
	if err != nil {
		t.Fatal(err)
	}
	requireAllStatus(t, result, execution.StateApplied)
	if stats := rig.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact] != series ||
		stats.CompactDeclined[StateCacheCompactWrittenDiverged] != 0 {
		t.Fatalf("entries %v, declined %v; want every value compact and none parted from its bytes", stats.Entries, stats.CompactDeclined)
	}
	items := readbackItems(second, series)
	for index := range items {
		items[index].VerifyCached = true
	}
	rig.now = rig.now.Add(time.Minute)
	loaded, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: items,
		OwnerFence: second.fence})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CacheVerify.Matched != series || loaded.CacheVerify.Mismatched != 0 {
		t.Fatalf("a sampled round over values taken in from their headers: %+v; want all %d matched", loaded.CacheVerify, series)
	}
	rig.now = rig.now.Add(time.Minute)
	rig.requireServedAsStored(third, series)
}

// What the parts a frame was framed from hold of the heap beside its bytes
// is what they are charged: twenty thousand, of one Level each, kept beside
// the mutations whose strings their headers share, take no more of the heap
// than charged and not less than two thirds of it.
func TestTheEncodersPartsChargeHoldsWhatTheyTakeOfTheHeap(t *testing.T) {
	const frames = 20000
	mutations := make([]execution.StateMutation, frames)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), first.version(), 0, "")
	}
	kept := make([]*packedParts, frames)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	for index, mutation := range mutations {
		_, parts, _, err := encodeRuntimePackedParts(mutation, 1)
		if err != nil {
			t.Fatal(err)
		}
		kept[index] = parts
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	charged := int64(0)
	for _, parts := range kept {
		charged += int64(parts.RetainedBytes())
	}
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("a frame's parts: charged %d bytes, hold %d of the heap", charged/frames, held/frames)
	if held > charged || 3*held < 2*charged {
		t.Fatalf("%d frames' parts charged %d bytes and hold %d of the heap, want the charge at or above the heap and within half of it",
			frames, charged, held)
	}
	runtime.KeepAlive(mutations)
	runtime.KeepAlive(kept)
}

// A value written with the parts its encoder framed it from is taken in
// without parsing its header back out of its bytes (014 next9 1, the third
// step): for one of a template held and not picked, the intake with its
// parts makes at least as many fewer allocations than without them as a
// parse of its frame makes.
func TestAValueWrittenWithItsPartsIsNotParsedBackOutOfItsBytes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cache := newStateCache(1<<20, time.Minute, func() time.Time { return now })
	cache.applied(writtenEntry(t, second, 0, now), nil)
	var entry stateCacheEntry
	for index := 1; index < 16 && entry.key == ""; index++ {
		if candidate := writtenEntry(t, second, index, now); !checkedAtWrite(candidate.raw) {
			entry = candidate
		}
	}
	if entry.key == "" {
		t.Fatal("fixture: every value written is one the rule picks")
	}
	encoded, parts, _, err := encodeRuntimePackedParts(seriesMutation(t, entry.identity, second.version(), 0, ""), 1)
	if err != nil || !bytes.Equal(encoded, entry.raw) {
		t.Fatalf("fixture: the parts' frame is not the value written (%v)", err)
	}
	if member, checked := cache.writtenCompact(entry, parts); member == nil || checked {
		t.Fatalf("fixture: a value of a template held, not picked, was made %v, checked %v; want a member unchecked", member, checked)
	}
	parse := testing.AllocsPerRun(100, func() {
		if _, _, _, err := parsePackedFrame(entry.raw); err != nil {
			t.Fatal(err)
		}
	})
	withParts := testing.AllocsPerRun(100, func() { cache.writtenCompact(entry, parts) })
	without := testing.AllocsPerRun(100, func() { cache.writtenCompact(entry, nil) })
	t.Logf("allocations: a parse %.0f, the intake with parts %.0f, without %.0f", parse, withParts, without)
	if without-withParts < parse {
		t.Fatalf("the intake with parts made %.0f allocations and without them %.0f; want it spared at least a parse's %.0f",
			withParts, without, parse)
	}
}
