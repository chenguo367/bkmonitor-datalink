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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A Plan's write renews the State keys of the members it names departed to
// the segments' full lifetime with the mark, in the same script, and
// answers how many of them were already gone; a write naming none renews no
// member's key.
func TestASegmentWriteRenewsTheDepartedMembersKeysToTheFullLifetime(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	ctx := context.Background()
	key := segmentPlanOf("9")
	departed, gone, unnamed := execution.StateKeyIdentity{Plan: key.Plan, StateGeneration: key.Generation, SeriesIdentityDigest: seriesIdentity(0).SeriesIdentityDigest},
		execution.StateKeyIdentity{Plan: key.Plan, StateGeneration: key.Generation, SeriesIdentityDigest: seriesIdentity(1).SeriesIdentityDigest},
		execution.StateKeyIdentity{Plan: key.Plan, StateGeneration: key.Generation, SeriesIdentityDigest: seriesIdentity(2).SeriesIdentityDigest}
	keyOf := func(identity execution.StateKeyIdentity) string {
		t.Helper()
		name, err := RuntimeStateKeyV3("alarmd", identity)
		if err != nil {
			t.Fatal(err)
		}
		return name
	}
	for _, identity := range []execution.StateKeyIdentity{departed, unnamed} {
		if err := rig.clients[0].Set(ctx, keyOf(identity), "state", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// A page removed in the same script: the departing members' keys come
	// after the removed pages' among the script's keys.
	write := planWrite(t, key, map[int][]byte{0: []byte("page")}, 1, 2, 120)
	// The gone one first: a script that reads the members' keys from the
	// wrong place would then miss the renewed one.
	write.Departed = []execution.SeriesIdentityDigest{gone.SeriesIdentityDigest, departed.SeriesIdentityDigest}
	result, err := store.WriteSegments(ctx, segment.WriteRequest{Contract: frozenRef(), Fence: testApplyFence(), Plans: []segment.PlanWrite{write}})
	if err != nil || result.Plans[0] != segment.WriteApplied {
		t.Fatalf("WriteSegments() = (%+v, %v), want applied", result, err)
	}
	if len(result.DepartedGone) != 1 || result.DepartedGone[0] != 1 {
		t.Fatalf("departed keys found gone %v, want the one never written", result.DepartedGone)
	}
	life, err := rig.clients[0].PTTL(ctx, keyOf(departed)).Result()
	if err != nil || life <= 2*time.Minute {
		t.Fatalf("the departed member's key lives %v (%v), want the segments' full lifetime, past its minute", life, err)
	}
	pageLife, err := rig.clients[0].PTTL(ctx, func() string {
		mark, _ := SegmentMarkKeyV1("alarmd", key)
		return mark
	}()).Result()
	if err != nil || life < pageLife-time.Second {
		t.Fatalf("the departed member's key lives %v, the mark %v: want the same lifetime", life, pageLife)
	}
	if left, err := rig.clients[0].PTTL(ctx, keyOf(unnamed)).Result(); err != nil || left > time.Minute {
		t.Fatalf("a member the write did not name lives %v (%v), want its own minute", left, err)
	}
}

// departedOf is count members of key's Plan, by digest.
func departedOf(key segment.PlanKey, count int) []execution.SeriesIdentityDigest {
	digests := make([]execution.SeriesIdentityDigest, 0, count)
	for index := 0; index < count; index++ {
		digests = append(digests, seriesIdentity(index).SeriesIdentityDigest)
	}
	return digests
}

// The departing members' keys go in chunks of their own before the mark's,
// at most segment.RenewKeys keys and a script's bytes each, their bytes
// counted; the mark's chunk carries none and is last.
func TestTheDepartingMembersKeysAreCutIntoChunksBeforeTheMark(t *testing.T) {
	store := newSegmentRedis(t).store(t, 0)
	key := segmentPlanOf("9")
	write := planWrite(t, key, map[int][]byte{0: []byte("page")}, 1, 0, 120)
	write.Departed = departedOf(key, 600)
	mark, err := SegmentMarkKeyV1("alarmd", key)
	if err != nil {
		t.Fatal(err)
	}
	chunks, _, err := store.segmentChunks(write, mark)
	if err != nil {
		t.Fatal(err)
	}
	last := chunks[len(chunks)-1]
	if last.mark == nil || len(last.departing) != 0 {
		t.Fatalf("the last chunk has mark %v and %d departing keys, want the mark alone of them", last.mark != nil, len(last.departing))
	}
	departing := 0
	for _, chunk := range chunks[:len(chunks)-1] {
		if chunk.mark != nil {
			t.Fatal("a chunk before the last carries the mark")
		}
		if len(chunk.departing) > segment.RenewKeys || chunk.bytes > fencedScriptBytes {
			t.Fatalf("a chunk of %d departing keys and %d bytes, past %d keys or %d bytes", len(chunk.departing), chunk.bytes,
				segment.RenewKeys, fencedScriptBytes)
		}
		bytes := 0
		for _, page := range chunk.pages {
			bytes += len(page.Value)
		}
		for _, name := range chunk.departing {
			bytes += len(name)
		}
		if len(chunk.departing) > 0 && chunk.bytes != bytes {
			t.Fatalf("a chunk counts %d bytes, carries %d", chunk.bytes, bytes)
		}
		departing += len(chunk.departing)
	}
	if departing != 600 || len(chunks) < 4 {
		t.Fatalf("%d departing keys in %d chunks, want all 600 in at least three before the mark's", departing, len(chunks))
	}
}

// Six hundred departing members, past a chunk: every key the write named is
// renewed, those gone counted; under a stale fence nothing lands - no mark,
// no key renewed - and the Plan is not applied.
func TestManyDepartingMembersAreRenewedAcrossChunksAndAStaleFenceLandsNothing(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	ctx := context.Background()
	key := segmentPlanOf("9")
	digests := departedOf(key, 600)
	names := make([]string, len(digests))
	for index, digest := range digests {
		name, err := RuntimeStateKeyV3("alarmd", execution.StateKeyIdentity{Plan: key.Plan, StateGeneration: key.Generation, SeriesIdentityDigest: digest})
		if err != nil {
			t.Fatal(err)
		}
		names[index] = name
		if index < 500 {
			rig.clients[0].Set(ctx, name, "state", time.Minute)
		}
	}
	write := planWrite(t, key, map[int][]byte{0: []byte("page")}, 1, 0, 120)
	write.Departed = digests
	stale := testApplyFence()
	stale.Fence.OwnerEpoch = 99
	refused, err := store.WriteSegments(ctx, segment.WriteRequest{Contract: frozenRef(), Fence: stale, Plans: []segment.PlanWrite{write}})
	if err != nil || refused.Plans[0] == segment.WriteApplied {
		t.Fatalf("a stale fence's write = (%+v, %v), want refused", refused, err)
	}
	mark, _ := SegmentMarkKeyV1("alarmd", key)
	if rig.clients[0].Exists(ctx, mark).Val() != 0 {
		t.Fatal("a stale fence's write left a mark")
	}
	if life := rig.clients[0].PTTL(ctx, names[0]).Val(); life > time.Minute {
		t.Fatalf("a stale fence's write renewed a departing key to %v", life)
	}
	result, err := store.WriteSegments(ctx, segment.WriteRequest{Contract: frozenRef(), Fence: testApplyFence(), Plans: []segment.PlanWrite{write}})
	if err != nil || result.Plans[0] != segment.WriteApplied || result.DepartedGone[0] != 100 {
		t.Fatalf("WriteSegments() = (%+v, %v), want applied with the hundred never written found gone", result, err)
	}
	for index, name := range names[:500] {
		if life := rig.clients[0].PTTL(ctx, name).Val(); life <= 2*time.Minute {
			t.Fatalf("departing key %d lives %v, want the full lifetime", index, life)
		}
	}
	if rig.clients[0].Exists(ctx, mark).Val() != 1 {
		t.Fatal("the write left no mark")
	}
}
