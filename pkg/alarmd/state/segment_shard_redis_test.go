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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A Plan not split keeps, word for word, the segment key names the shadow
// has written under (taken from the build before pieces were keyed): its
// readings before and after compare.
func TestAPlanNotSplitKeepsItsSegmentKeyNames(t *testing.T) {
	identity := stateIdentityV2()
	key := segment.PlanKey{Plan: identity.Plan, Generation: identity.StateGeneration}
	mark, err := SegmentMarkKeyV1("alarmd", key)
	if err != nil {
		t.Fatal(err)
	}
	page, err := SegmentPageKeyV1("alarmd", key, 3)
	if err != nil {
		t.Fatal(err)
	}
	const (
		wantMark = "alarmd:segment-mark:v2:63b4c0476f5aa004794df8453c4d90a6:2:9:460f4e23194b12e06e68789d9d81564d"
		wantPage = "alarmd:segment-page:v2:63b4c0476f5aa004794df8453c4d90a6:2:9:460f4e23194b12e06e68789d9d81564d:00000000000000000000000000000003"
	)
	if mark != wantMark || page != wantPage {
		t.Fatalf("a Plan not split is keyed\n%s\n%s\nwant\n%s\n%s", mark, page, wantMark, wantPage)
	}
}

// segmentPieceOf is the Plan of segmentPlanOf(strategy) as piece index of
// two, by a matcher of its own.
func segmentPieceOf(strategy string, index int) segment.PlanKey {
	key := segmentPlanOf(strategy)
	key.Shard = execution.ShardRef{Dimension: "bk_target_ip", Index: index, Count: 2,
		MatcherDigest: string(seriesDigest("matcher-" + string(rune('0'+index))))}
	return key
}

// Two pieces of one split strategy and the strategy not split are three
// Plans' segments: their marks and pages - each page its own - are three
// sets of keys none of which another's kind matches, each written and read back as its own, each
// its own cache entries - one let go leaves the others served - and each
// its own reading and last use. A piece that is not one is refused.
func TestEachPieceOfASplitStrategyHasItsOwnSegments(t *testing.T) {
	rig := newSegmentRedis(t)
	ctx := context.Background()
	unsplit, first, second := segmentPlanOf("9"), segmentPieceOf("9", 0), segmentPieceOf("9", 1)
	keys := []segment.PlanKey{unsplit, first, second}
	seen := map[string]segment.PlanKey{}
	for _, key := range keys {
		mark, err := SegmentMarkKeyV1("alarmd", key)
		if err != nil {
			t.Fatal(err)
		}
		page, err := SegmentPageKeyV1("alarmd", key, 0)
		if err != nil {
			t.Fatal(err)
		}
		next, err := SegmentPageKeyV1("alarmd", key, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{mark, page, next} {
			if other, taken := seen[name]; taken {
				t.Fatalf("%s names %+v's segments and %+v's", name, other, key)
			}
			seen[name] = key
		}
		if !isSegmentMarkKey(mark) || isSegmentMarkKey(page) {
			t.Fatalf("%+v: mark %s and page %s told apart wrongly", key.Shard, mark, page)
		}
	}
	broken := first
	broken.Shard.Count = 1
	if _, err := SegmentMarkKeyV1("alarmd", broken); err == nil {
		t.Fatal("a piece of one is keyed")
	}
	if _, err := SegmentPageKeyV1("alarmd", broken, 0); err == nil {
		t.Fatal("a piece of one's page is keyed")
	}

	writer := rig.store(t, 1<<20)
	var writes []segment.PlanWrite
	for index, key := range keys {
		writes = append(writes, planWrite(t, key, map[int][]byte{0: []byte{byte('u' + index)}}, 1, 0, int64(60*(index+1))))
	}
	if results := writeSegments(t, writer, testApplyFence(), writes...); !reflect.DeepEqual(results, []string{segment.WriteApplied, segment.WriteApplied, segment.WriteApplied}) {
		t.Fatalf("writes: %v", results)
	}
	cold := rig.store(t, 1<<20)
	for index, stored := range readSegments(t, cold, testApplyFence().Fence, keys...) {
		if stored.Key != keys[index] || len(stored.Pages) != 1 || string(stored.Pages[0]) != string(rune('u'+index)) {
			t.Fatalf("read back %+v, want %+v's own page", stored, keys[index])
		}
	}
	if stats := writer.SegmentStats(); stats.Plans != 3 || stats.Pages != 3 {
		t.Fatalf("the writer's cache holds %+v, want three Plans of one page", stats)
	}
	writer.InvalidateSegments(frozenRef().Slot.QueryGroup, first, segment.ReadRenewMissing)
	stored := readSegments(t, writer, testApplyFence().Fence, keys...)
	if stored[1].Source != segment.SourceRedis || stored[1].Reason != segment.ReadRenewMissing ||
		stored[0].Source != segment.SourceCache || stored[2].Source != segment.SourceCache {
		t.Fatalf("after one piece's cache was let go: %+v / %+v / %+v, want only that piece read from Redis",
			stored[0].Reason, stored[1].Reason, stored[2].Reason)
	}
	reading, err := writer.PlanSegments(ctx, frozenRef().Slot.QueryGroup, keys)
	if err != nil {
		t.Fatal(err)
	}
	for index, plan := range reading.Plans {
		var wantShard *execution.ShardRef
		if !keys[index].Shard.IsZero() {
			shard := keys[index].Shard
			wantShard = &shard
		}
		if !reflect.DeepEqual(plan.Shard, wantShard) || plan.Position != int64(60*(index+1)) || plan.Last == nil ||
			plan.Last.ReadReason != stored[index].Reason {
			t.Fatalf("piece %d read %+v (last %+v), want its own mark and its own last read", index, plan, plan.Last)
		}
	}
}
