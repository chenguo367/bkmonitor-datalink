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

// A round that writes its whole Query Group back - as a Plan's sampled round
// writes every member - does not put the group out of a cache sized by its
// members kept compact (014 next9 1): the members read back and held compact,
// a budget a quarter over them, the whole group written, and the group stays,
// nothing leaving over the budget, every member served the round after.
// Before, the round's writes were kept as their bytes, some four times a
// compact member, and the group left whole, read back by MGET until a round
// brought it back compact - then out again at the next sampled round.
func TestARoundWritingItsWholeGroupBackDoesNotPutItOut(t *testing.T) {
	const members = 40
	indexes := make([]int, members)
	for index := range indexes {
		indexes[index] = index
	}
	writer := newCacheRig(t, 0)
	writer.round(first, members, 0)
	readBackAll := func(rig *cacheRig) StateCacheStats {
		t.Helper()
		ctx := segment.WithSource(context.Background(), readbackCover(rig.now, true, indexes...))
		rig.loadRequest(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: readbackItems(second, members),
			OwnerFence: second.fence})
		return rig.store.StateCacheStats()
	}
	// What the members take compact, read off a cache that holds them all.
	held := readBackAll(segmentRig(t, writer.backend, 1<<20, nil))
	if readBack(held) != members {
		t.Fatalf("fixture: %v entries read back, want all %d members", held.Entries, members)
	}
	rig := segmentRig(t, writer.backend, held.Bytes*5/4, nil)
	if stats := readBackAll(rig); readBack(stats) != members || stats.QueryGroups != 1 {
		t.Fatalf("fixture: the cache sized a quarter over its members holds %v in %d groups, want all %d", stats.Entries,
			stats.QueryGroups, members)
	}

	rig.now = rig.now.Add(time.Minute)
	rig.round(third, members, 1)
	stats := rig.store.StateCacheStats()
	if left := stats.StateDepartures[StateCacheDepartOverBudget]; left != 0 || stats.QueryGroups != 1 ||
		stats.Entries[StateCacheEntryCompact]+stats.Entries[StateCacheEntryWritten] != members {
		t.Fatalf("after the round that wrote the whole group: %d left over the budget, %d groups, entries %v; want the group held whole",
			left, stats.QueryGroups, stats.Entries)
	}
	rig.now = rig.now.Add(time.Minute)
	hits := rig.lookups(StateCacheHit)
	if reads := rig.load(slot{at: 240, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, members); reads != 0 ||
		rig.lookups(StateCacheHit)-hits != members {
		t.Fatalf("the round after read %d times from the store and was served %d of %d", reads, rig.lookups(StateCacheHit)-hits, members)
	}
}
