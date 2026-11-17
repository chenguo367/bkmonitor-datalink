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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

func (rig *cacheRig) viewsUnder(ctx context.Context, s slot, items []execution.StatePreflightItem) []execution.RuntimeStateView {
	rig.t.Helper()
	loaded, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence})
	if err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	return loaded.Items
}

// The segments a Slot read reach the store with the Slot's context: a store
// told none extends a member's read by the segments its reader carries -
// from the cache, from the store and as a carried generation alike - and
// reads it as stored under a context without them. Every member's read is
// counted by what its extension did: extended where points were made, within
// where its own points reach the segment's end; a series no member is not
// counted.
func TestTheSegmentsAReaderCarriesExtendEveryReadOfAMember(t *testing.T) {
	member, other, within := seriesIdentity(0), seriesIdentity(1), seriesIdentity(2)
	cover := coverByIdentity{
		member: {Entry: segment.Entry{Digest: member.SeriesIdentityDigest, Start: 60}, Position: 240, Interval: 60, Retention: 5},
		within: {Entry: segment.Entry{Digest: within.SeriesIdentityDigest, Start: 60}, Position: 60, Interval: 60, Retention: 5},
	}
	ctx := segment.WithSource(context.Background(), cover)
	items := []execution.StatePreflightItem{{Identity: member, ApplyVersion: second.version()}, {Identity: other, ApplyVersion: second.version()},
		{Identity: within, ApplyVersion: second.version()}}
	cached := segmentRig(t, newPipelineMemoryBackend(), 1<<20, nil)
	cached.round(first, 3, 0)
	cached.now = cached.now.Add(time.Minute)
	fromCache := cached.viewsUnder(ctx, second, items)
	if cached.lookups(StateCacheHit) != 3 {
		t.Fatalf("%d cache hits, want every series served from the cache", cached.lookups(StateCacheHit))
	}
	if got := sourceTimes(fromCache[0]); !reflect.DeepEqual(got, []int64{60, 120, 180, 240}) || fromCache[0].LastProcessedEventTime != 240 {
		t.Fatalf("the member read from the cache at %v, processed to %d; want its point and three made up to the mark",
			got, fromCache[0].LastProcessedEventTime)
	}
	if got := sourceTimes(fromCache[1]); !reflect.DeepEqual(got, []int64{60}) {
		t.Fatalf("a series no member read at %v, want its one stored point", got)
	}
	if got := sourceTimes(fromCache[2]); !reflect.DeepEqual(got, []int64{60}) {
		t.Fatalf("a member within its segment read at %v, want its one stored point", got)
	}
	if extends := cached.store.SegmentStats().Extends; extends[segment.Extended] != 1 || extends[segment.Within] != 1 ||
		len(extends) != len(segment.Extensions) {
		t.Fatalf("extensions %v, want one extended and one within, every cell present", extends)
	}
	fromStore := segmentRig(t, cached.backend, 0, nil).viewsUnder(ctx, second, items)
	if !reflect.DeepEqual(fromStore[0].History, fromCache[0].History) || fromStore[0].LastProcessedEventTime != fromCache[0].LastProcessedEventTime {
		t.Fatalf("the member read from the store %+v,\nfrom the cache %+v", fromStore[0], fromCache[0])
	}
	next := member
	next.StateGeneration = "next-generation"
	carried := segmentRig(t, cached.backend, 0, nil).viewsUnder(ctx, second,
		[]execution.StatePreflightItem{{Identity: next, ApplyVersion: second.version(), CarryFrom: member.StateGeneration}})
	if carried[0].Carried == nil || !reflect.DeepEqual(carried[0].Carried.History, fromStore[0].History) {
		t.Fatalf("the carried generation read %+v, want the member's record as the store reads it", carried[0].Carried)
	}
	if plain := segmentRig(t, cached.backend, 0, nil).viewsUnder(context.Background(), second, items); !reflect.DeepEqual(sourceTimes(plain[0]), []int64{60}) {
		t.Fatalf("a reader carrying no segments read the member at %v, want as stored", sourceTimes(plain[0]))
	}
}
