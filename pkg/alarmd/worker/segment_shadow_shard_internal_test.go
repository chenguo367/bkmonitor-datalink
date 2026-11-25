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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// Two pieces of one split strategy due in one Slot are two Plans' segments,
// each read by its piece: the one with a mark stands at it, the other
// starts fresh, and neither reads the other's. A Plan not split is keyed as
// it always was, with no piece.
func TestEachPieceOfASplitStrategyReadsItsOwnSegments(t *testing.T) {
	store := newMemorySegments()
	request := shadowRequest(execution.OperationNormal, 120)
	first, second := shadowDue, shadowDue
	first.Shard = execution.ShardRef{Dimension: "bk_target_ip", Index: 0, Count: 2, MatcherDigest: strings.Repeat("a", 64)}
	second.Shard = execution.ShardRef{Dimension: "bk_target_ip", Index: 1, Count: 2, MatcherDigest: strings.Repeat("b", 64)}
	page, err := segment.EncodePage(nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := segment.EncodeMark(segment.Mark{Position: 60, Pages: 1, Renewed: []segment.Renewal{{At: 1, TTL: 60_000}}, Sizes: []int{len(page)}})
	if err != nil {
		t.Fatal(err)
	}
	firstKey, secondKey := shadowKey(first, first.StateGeneration, segment.FamilyReal), shadowKey(second, second.StateGeneration, segment.FamilyReal)
	store.marks[firstKey], store.pages[firstKey] = raw, map[int][]byte{0: page}
	lifetime := segment.Lifetime{Interval: time.Minute}
	slot := loadSegmentSlot(context.Background(), store, request, []segmentDue{{due: first, retention: 5, lifetime: lifetime},
		{due: second, retention: 5, lifetime: lifetime}})
	if slot == nil || len(slot.plans) != 2 {
		t.Fatalf("two pieces read %+v, want two Plans' segments", slot)
	}
	if read := slot.plans[firstKey]; read == nil || !read.hadMark || read.position != 60 || read.key != firstKey {
		t.Fatalf("the piece with a mark read %+v, want it at its mark", read)
	}
	if read := slot.plans[secondKey]; read == nil || read.hadMark || !read.fresh || read.key != secondKey {
		t.Fatalf("the other piece read %+v, want it fresh, not at its sibling's mark", read)
	}
	if unsplit := shadowKey(shadowDue, shadowDue.StateGeneration, segment.FamilyReal); unsplit != (segment.PlanKey{Plan: shadowDue.Identity, Generation: shadowDue.StateGeneration}) {
		t.Fatalf("a Plan not split keyed %+v", unsplit)
	}
	if segmentPlanName(firstKey) == segmentPlanName(secondKey) || segmentPlanName(shadowKey(shadowDue, "g", segment.FamilyReal)) != "tenant/2/9/g" {
		t.Fatalf("pages staggered by %q and %q, and %q for a Plan not split", segmentPlanName(firstKey), segmentPlanName(secondKey),
			segmentPlanName(shadowKey(shadowDue, "g", segment.FamilyReal)))
	}
}
