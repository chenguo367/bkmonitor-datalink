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

// cellsOf is the cache's cells with entries, as its stats read them.
func cellsOf(rig *cacheRig) map[StateCacheCell]StateCacheCellCount {
	cells := map[StateCacheCell]StateCacheCellCount{}
	for cell, counted := range rig.store.StateCacheStats().Cells {
		if counted.Entries != 0 || counted.Bytes != 0 {
			cells[cell] = counted
		}
	}
	return cells
}

// loadAt is one preflight of series [0, n) at s, under ctx, the clock a
// minute on.
func loadAt(t *testing.T, rig *cacheRig, ctx context.Context, s slot, n int, generation execution.StateGeneration) {
	t.Helper()
	rig.now = rig.now.Add(time.Minute)
	items := readbackItems(s, n)
	if generation != "" {
		for index := range items {
			items[index].Identity.StateGeneration = generation
		}
	}
	rig.loadRequest(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence})
}

// A Query Group's State entries are counted by cell at the sweep of each
// Slot's first read, as they were in the round before it (014 n8): touched
// when that round looked them up, wrote or read them back, current when it
// read their own generation of their Plan; a later Slot's count replaces the
// earlier, and the bytes are each entry's charge.
func TestAGroupsEntriesAreCountedByCellAtEachSlotsSweep(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	at := func(evaluation execution.EvaluationTime) slot {
		return slot{at: evaluation, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	}
	ctx := context.Background()
	// The round that reads both, then a round that reads the first alone.
	loadAt(t, rig, ctx, at(120), 2, "")
	loadAt(t, rig, ctx, at(180), 1, "")
	loadAt(t, rig, ctx, at(240), 1, "")
	cost := rig.store.StateCacheStats().Bytes / 2
	// Written compact (014 next9 1).
	touched := StateCacheCell{Kind: StateCacheEntryCompact, Generation: StateCacheGenerationCurrent, Touched: true}
	untouched := touched
	untouched.Touched = false
	want := map[StateCacheCell]StateCacheCellCount{touched: {Entries: 1, Bytes: cost}, untouched: {Entries: 1, Bytes: cost}}
	if got := cellsOf(rig); !equalCells(got, want) {
		t.Fatalf("after a round that read the first series alone the cells are %v, want %v", got, want)
	}
	// The Plan read under another generation: the entry left is of the
	// other. The second series, untouched since the round at 120, is let go
	// at 300's sweep, three minutes on: "untouched for two intervals, it
	// leaves" (stateCacheEntry.touched), the Plan's one-minute interval its
	// write's retention (keepCompactLocked; sweep's untouched rule).
	loadAt(t, rig, ctx, at(300), 1, "generation-b")
	loadAt(t, rig, ctx, at(360), 1, "generation-b")
	entries := int64(0)
	for cell, counted := range cellsOf(rig) {
		if cell.Generation != StateCacheGenerationOther {
			t.Fatalf("after rounds that read another generation the cells are %v, want the entry left other", cellsOf(rig))
		}
		entries += counted.Entries
	}
	if untouched := rig.store.StateCacheStats().StateDepartures[StateCacheDepartUntouched]; entries != 1 || untouched != 1 {
		t.Fatalf("after rounds that read another generation %d entries are counted and %d let go untouched, want one of each", entries,
			untouched)
	}
	// A round that reads nothing of the Plan: the generation its round
	// before read is not kept past that round's count.
	fresh := newCacheRig(t, 1<<20)
	fresh.round(first, 2, 0)
	loadAt(t, fresh, ctx, at(120), 2, "")
	fresh.now = fresh.now.Add(time.Minute)
	fresh.store.cache.sweep(at(180).contract().Slot)
	fresh.now = fresh.now.Add(time.Minute)
	fresh.store.cache.sweep(at(240).contract().Slot)
	entries = 0
	for cell, counted := range cellsOf(fresh) {
		if cell.Generation != StateCacheGenerationOther {
			t.Fatalf("after a round that read nothing of the Plan the cells are %v, want every entry other", cellsOf(fresh))
		}
		entries += counted.Entries
	}
	if entries != 2 {
		t.Fatalf("after a round that read nothing of the Plan %d entries are counted, want both", entries)
	}
}

// A lookup marks what it knows of the entry: a split strategy's piece as
// sharded, and, under a Slot's coverage, whether it is an open member; a
// lookup with no coverage leaves its membership. A value read back is an open
// member.
func TestALookupMarksAnEntrySplitOrAMemberAndAReadBackIsAMember(t *testing.T) {
	rig := segmentRig(t, newCacheRig(t, 0).backend, 1<<20, nil)
	rig.round(first, 2, 0)
	at := func(evaluation execution.EvaluationTime, sharded bool) slot {
		return slot{at: evaluation, epoch: 1, segmentStart: 60, fence: fenceOf(3), sharded: sharded}
	}
	covered := segment.WithSource(context.Background(), readbackCover(rig.now, false, 0))
	loadAt(t, rig, covered, at(120, false), 2, "")
	loadAt(t, rig, context.Background(), at(180, false), 2, "")
	cells := cellsOf(rig)
	member := StateCacheCell{Kind: StateCacheEntryCompact, Generation: StateCacheGenerationCurrent, Member: true, Touched: true}
	other := member
	other.Member = false
	if cells[member].Entries != 1 || cells[other].Entries != 1 {
		t.Fatalf("cells %v; want the covered member marked and the other not, the uncovered lookup changing neither", cells)
	}
	loadAt(t, rig, context.Background(), at(240, true), 2, "")
	if cells := cellsOf(rig); cells[StateCacheCell{Kind: StateCacheEntryCompact, Generation: StateCacheGenerationCurrent, Member: true,
		Touched: true}].Entries != 1 {
		t.Fatalf("after a round of lookups with no coverage the cells are %v, want the member still marked", cells)
	}
	loadAt(t, rig, context.Background(), at(300, true), 2, "")
	for cell := range cellsOf(rig) {
		if !cell.Sharded {
			t.Fatalf("after lookups of a split strategy's pieces the cells are %v, want every entry sharded", cellsOf(rig))
		}
	}
	writer := written(t)
	reader := segmentRig(t, writer.backend, 1<<20, nil)
	ctx := segment.WithSource(context.Background(), readbackCover(reader.now, true, 0))
	loadAt(t, reader, ctx, second, 1, "")
	loadAt(t, reader, ctx, at(240, false), 1, "")
	compact := StateCacheCell{Kind: StateCacheEntryCompact, Generation: StateCacheGenerationCurrent, Member: true, Touched: true}
	if got := cellsOf(reader); got[compact].Entries != 1 {
		t.Fatalf("a value read back counts as %v, want a compact open member", got)
	}
}

// Only a Slot later than the group's latest counted one counts: an earlier
// Slot's sweep - a supplement's, its reads before the round's own - sweeps
// and leaves the count. A group the cache lets go is counted nowhere.
func TestAnEarlierSlotsSweepLeavesTheCountAndAGroupLetGoIsNotCounted(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	at := func(evaluation execution.EvaluationTime) slot {
		return slot{at: evaluation, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	}
	ctx := context.Background()
	loadAt(t, rig, ctx, at(180), 2, "")
	loadAt(t, rig, ctx, at(240), 2, "")
	// The next Slot's first read counts the round that touched both; a
	// supplement's earlier Slot sweeps before the round's lookups have.
	rig.now = rig.now.Add(time.Minute)
	rig.store.cache.sweep(at(300).contract().Slot)
	counted := cellsOf(rig)
	rig.store.cache.sweep(at(120).contract().Slot)
	if got := cellsOf(rig); !equalCells(got, counted) {
		t.Fatalf("an earlier Slot's sweep changed the cells from %v to %v", counted, got)
	}
	rig.store.DropStateCacheQueryGroup(at(240).contract().Slot.QueryGroup)
	if got := cellsOf(rig); len(got) != 0 {
		t.Fatalf("the group let go is still counted: %v", got)
	}
}

// lookThenWrite is a round of series [0, n) at s as production runs it: the
// preflight's lookups under ctx, then the round's write through the store,
// every series coming back APPLIED.
func lookThenWrite(t *testing.T, rig *cacheRig, ctx context.Context, s slot, n int) {
	t.Helper()
	rig.now = rig.now.Add(time.Minute)
	rig.loadRequest(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: readbackItems(s, n), OwnerFence: s.fence})
	mutations := make([]execution.StateMutation, n)
	for index := range mutations {
		mutations[index] = seriesMutation(t, seriesIdentity(index), s.version(), 1, "")
	}
	fence := testApplyFence()
	fence.Fence = s.fence
	result, err := rig.store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: s.contract(), Retention: testRetention(),
		Items: mutations}, fence)
	if err != nil {
		t.Fatalf("ApplyRuntimeFenced() error = %v", err)
	}
	for _, item := range result.Items {
		if item.Status != execution.StateApplied {
			t.Fatalf("the round's write came back %v, want APPLIED", item.Status)
		}
	}
}

// A round's write keeps what its lookups marked of the entry or compact
// member it replaces (014 n8): the lookups mark a covered member, a split
// strategy's piece, a member read back compact; the round's write through
// the store comes back APPLIED and replaces it; the next Slot's sweep,
// before its own lookups, counts the written entry as marked.
func TestARoundsWriteKeepsWhatItsLookupsMarkedOfTheEntryItReplaces(t *testing.T) {
	at := func(evaluation execution.EvaluationTime, sharded bool) slot {
		return slot{at: evaluation, epoch: 1, segmentStart: 60, fence: fenceOf(3), sharded: sharded}
	}
	// The write kept compact (014 next9 1).
	cell := StateCacheCell{Kind: StateCacheEntryCompact, Generation: StateCacheGenerationCurrent, Touched: true}
	t.Run("a covered member", func(t *testing.T) {
		rig := newCacheRig(t, 1<<20)
		rig.round(first, 1, 0)
		lookThenWrite(t, rig, segment.WithSource(context.Background(), readbackCover(rig.now, false, 0)), at(120, false), 1)
		loadAt(t, rig, context.Background(), at(180, false), 1, "")
		want := cell
		want.Member = true
		if got := cellsOf(rig); !equalCells(got, map[StateCacheCell]StateCacheCellCount{want: {Entries: 1, Bytes: rig.store.StateCacheStats().Bytes}}) {
			t.Fatalf("after the member's round wrote it the cells are %v, want it a written open member", got)
		}
	})
	t.Run("a split strategy's piece", func(t *testing.T) {
		rig := newCacheRig(t, 1<<20)
		rig.round(first, 1, 0)
		lookThenWrite(t, rig, context.Background(), at(120, true), 1)
		loadAt(t, rig, context.Background(), at(180, true), 1, "")
		want := cell
		want.Sharded = true
		if got := cellsOf(rig); !equalCells(got, map[StateCacheCell]StateCacheCellCount{want: {Entries: 1, Bytes: rig.store.StateCacheStats().Bytes}}) {
			t.Fatalf("after the piece's round wrote it the cells are %v, want it a written piece", got)
		}
	})
	t.Run("a member read back compact", func(t *testing.T) {
		writer := written(t)
		reader := segmentRig(t, writer.backend, 1<<20, nil)
		loadAt(t, reader, segment.WithSource(context.Background(), readbackCover(reader.now, true, 0)), second, 1, "")
		if stats := reader.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact] != 1 {
			t.Fatalf("the member read back is held as %v, want compact", stats.Entries)
		}
		// Its next round writes it, under no coverage: what it is comes from
		// the compact member it replaces.
		lookThenWrite(t, reader, context.Background(), at(180, false), 1)
		loadAt(t, reader, context.Background(), at(240, false), 1, "")
		want := cell
		want.Member = true
		if got := cellsOf(reader); !equalCells(got, map[StateCacheCell]StateCacheCellCount{want: {Entries: 1,
			Bytes: reader.store.StateCacheStats().Bytes}}) {
			t.Fatalf("after the compact member's round wrote it the cells are %v, want it a written open member", got)
		}
	})
}

func equalCells(got, want map[StateCacheCell]StateCacheCellCount) bool {
	if len(got) != len(want) {
		return false
	}
	for cell, counted := range want {
		if got[cell] != counted {
			return false
		}
	}
	return true
}
