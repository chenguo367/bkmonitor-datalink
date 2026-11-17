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

// readingMark is a mark of pages pages written by the round at round, its
// renewals told apart by page.
func readingMark(t *testing.T, round execution.EvaluationTime, position int64, pages int) (segment.Mark, []byte) {
	t.Helper()
	mark := segment.Mark{Version: execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: round}, Position: position,
		Pages: pages, Renewed: make([]segment.Renewal, pages), Sizes: make([]int, pages)}
	for page := range mark.Renewed {
		mark.Renewed[page] = segment.Renewal{At: 1_000 + int64(page), TTL: 60_000 + int64(page)}
	}
	raw, err := segment.EncodeMark(mark)
	if err != nil {
		t.Fatal(err)
	}
	return mark, raw
}

// An object's Plans' segments are read for a reader by their marks alone:
// any process reads them, one MGET of marks a target, unfenced, keeping
// nothing in its cache and counting no Slot's read - a mark absent or not a
// mark said so - and the process that read and wrote them says what it last
// did with each, under the generation it did it, until it lets the Query
// Group go.
func TestAnObjectsPlanSegmentsAreTheMarksAndWhatThisProcessLastDidWithThem(t *testing.T) {
	rig := newSegmentRedis(t)
	ctx := context.Background()
	at := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return at }
	group := frozenRef().Slot.QueryGroup
	writer := rig.storeAt(t, 1<<20, clock)
	onFirst, onSecond := segmentPlanOf("9"), segmentPlanOf("10")
	absent, unreadable := segmentPlanOf("9"), segmentPlanOf("9")
	absent.Generation, unreadable.Generation = "generation-absent", "generation-unreadable"
	firstMark, firstRaw := readingMark(t, 1_799_999_940, 1_799_999_880, 2)
	secondMark, secondRaw := readingMark(t, 1_799_999_940, 1_799_999_820, 4)
	results := writeSegments(t, writer, testApplyFence(),
		segment.PlanWrite{Key: onFirst, Lifetime: segmentLifetime(), Pages: map[int][]byte{0: []byte("a"), 1: []byte("b")},
			Mark: firstRaw, PageCount: 2, SkipMode: segment.SkipModeSkip, Skipped: 3},
		segment.PlanWrite{Key: onSecond, Lifetime: segmentLifetime(), Pages: map[int][]byte{0: []byte("a"), 1: []byte("b"),
			2: []byte("c"), 3: []byte("d")}, Mark: secondRaw, PageCount: 4, SkipMode: segment.SkipModeSampled})
	if results[0] != segment.WriteApplied || results[1] != segment.WriteApplied {
		t.Fatalf("writes: %v", results)
	}
	garbage, _ := SegmentMarkKeyV1("alarmd", unreadable)
	rig.clients[0].Set(ctx, garbage, "not a mark", time.Minute)
	keys := []segment.PlanKey{onFirst, onSecond, absent, unreadable}

	// Another process: the marks as Redis holds them, nothing of its own.
	reader := rig.storeAt(t, 1<<20, clock)
	before := [2]int64{rig.targets[0].mgets.Load(), rig.targets[1].mgets.Load()}
	reading, err := reader.PlanSegments(ctx, group, keys)
	if err != nil {
		t.Fatal(err)
	}
	if mgets := [2]int64{rig.targets[0].mgets.Load() - before[0], rig.targets[1].mgets.Load() - before[1]}; mgets != [2]int64{1, 1} {
		t.Fatalf("the marks read in %v MGETs a target, want one each: no page is read", mgets)
	}
	want := []segment.PlanReading{
		{Plan: onFirst.Plan, Generation: onFirst.Generation, Mark: segment.MarkFound, Round: firstMark.Version.EvaluationTime,
			Position: firstMark.Position, Pages: 2, Renewed: firstMark.Renewed, RenewedTotal: 2},
		{Plan: onSecond.Plan, Generation: onSecond.Generation, Mark: segment.MarkFound, Round: secondMark.Version.EvaluationTime,
			Position: secondMark.Position, Pages: 4, Renewed: secondMark.Renewed, RenewedTotal: 4},
		{Plan: absent.Plan, Generation: absent.Generation, Mark: segment.MarkAbsent},
	}
	if !reflect.DeepEqual(reading.Plans[:3], want) {
		t.Fatalf("another process read %+v, want %+v", reading.Plans[:3], want)
	}
	if got := reading.Plans[3]; got.Mark != segment.MarkUnreadable || got.Unreadable == "" || got.Pages != 0 || got.Last != nil {
		t.Fatalf("a key holding no mark read %+v, want unreadable with why", got)
	}
	if reading.LastRecorded || reading.QueryGroup != group {
		t.Fatalf("another process's reading %+v says it did something with the segments", reading)
	}
	if stats := reader.SegmentStats(); stats.Plans != 0 || stats.Pages != 0 || stats.Reads[[2]string{segment.SourceRedis, segment.ReadCold}] != 0 {
		t.Fatalf("a reader's read kept or counted %+v: it is no Slot's", stats)
	}

	// The writer: its writes' results, then its Slot's read.
	wrote := at
	reading, err = writer.PlanSegments(ctx, group, keys)
	if err != nil {
		t.Fatal(err)
	}
	if !reading.LastRecorded || reading.Plans[2].Last != nil || reading.Plans[3].Last != nil {
		t.Fatalf("the writer's reading %+v, want its two Plans' uses and none for the others", reading)
	}
	if last := reading.Plans[0].Last; !reflect.DeepEqual(last, &segment.LastUse{WriteResult: segment.WriteApplied, WriteAt: &wrote,
		SkipMode: segment.SkipModeSkip, Skipped: 3}) {
		t.Fatalf("the writer's last use of the first Plan %+v, want its applied write alone, with its round's skip", last)
	}
	at = at.Add(time.Minute)
	read := at
	readSegments(t, writer, testApplyFence().Fence, onFirst, onSecond)
	reading, _ = writer.PlanSegments(ctx, group, keys)
	if last := reading.Plans[1].Last; !reflect.DeepEqual(last, &segment.LastUse{ReadSource: segment.SourceCache,
		ReadReason: segment.ReadHit, ReadAt: &read, WriteResult: segment.WriteApplied, WriteAt: &wrote, SkipMode: segment.SkipModeSampled}) {
		t.Fatalf("after a read the cache answered: %+v", last)
	}

	// A read cut off is every asked Plan's, failed; the reader's own read
	// of the marks fails whole.
	cold := rig.storeAt(t, 0, clock)
	rig.targets[0].failMGet.Store(true)
	if _, err := cold.ReadSegments(ctx, segment.ReadRequest{Contract: frozenRef(), OwnerFence: testApplyFence().Fence,
		Plans: []segment.ReadPlan{{Key: onFirst, Lifetime: segmentLifetime()}}}); err == nil {
		t.Fatal("a read cut off answered")
	}
	if _, err := cold.PlanSegments(ctx, group, keys); err == nil {
		t.Fatal("the marks' read cut off answered")
	}
	rig.targets[0].failMGet.Store(false)
	reading, _ = cold.PlanSegments(ctx, group, keys)
	if last := reading.Plans[0].Last; last == nil || last.ReadSource != segment.SourceRedis || last.ReadReason != segment.ReadFailed || last.WriteResult != "" {
		t.Fatalf("after a read cut off: %+v, want it failed", last)
	}

	// Another generation of the Plan is another use; the release lets go
	// of them all.
	moved := onFirst
	moved.Generation = "generation-moved"
	reading, _ = writer.PlanSegments(ctx, group, []segment.PlanKey{moved, onSecond})
	if reading.Plans[0].Last != nil || reading.Plans[1].Last == nil {
		t.Fatalf("a moved generation's reading %+v, want nothing done under it and the other Plan kept", reading.Plans)
	}
	writer.DropStateCacheQueryGroup(group)
	reading, _ = writer.PlanSegments(ctx, group, keys)
	if reading.LastRecorded || reading.Plans[0].Last != nil || reading.Plans[0].Mark != segment.MarkFound {
		t.Fatalf("after the release: %+v, want the marks and nothing of this process's", reading)
	}
}

// What this process last did with a Plan its Query Group's Slots no longer
// read goes once a Slot's read has not asked for it for two of its
// intervals, as an untouched cache entry does: kept up to two intervals
// untouched, gone past them, while the Plans still read stay.
func TestAPlansLastUseGoesTwoIntervalsAfterItsQueryGroupStopsReadingIt(t *testing.T) {
	rig := newSegmentRedis(t)
	ctx := context.Background()
	at := time.Unix(1_800_000_000, 0)
	store := rig.storeAt(t, 1<<20, func() time.Time { return at })
	group := frozenRef().Slot.QueryGroup
	kept, left := segmentPlanOf("9"), segmentPlanOf("10")
	writeSegments(t, store, testApplyFence(), planWrite(t, kept, map[int][]byte{0: []byte("a")}, 1, 0, 60),
		planWrite(t, left, map[int][]byte{0: []byte("b")}, 1, 0, 60))
	readSegments(t, store, testApplyFence().Fence, kept, left)
	for _, step := range []struct {
		after time.Duration
		gone  bool
	}{
		{after: 90 * time.Second},
		{after: 2 * time.Minute},
		{after: 2*time.Minute + time.Second, gone: true},
	} {
		at = time.Unix(1_800_000_000, 0).Add(step.after)
		readSegments(t, store, testApplyFence().Fence, kept)
		reading, err := store.PlanSegments(ctx, group, []segment.PlanKey{kept, left})
		if err != nil {
			t.Fatal(err)
		}
		if reading.Plans[0].Last == nil || reading.Plans[0].Last.ReadAt == nil || !reading.Plans[0].Last.ReadAt.Equal(at) {
			t.Fatalf("%s on: the Plan still read has %+v", step.after, reading.Plans[0].Last)
		}
		if (reading.Plans[1].Last == nil) != step.gone {
			t.Fatalf("%s after its last read the Plan no longer read has %+v, want gone %v", step.after, reading.Plans[1].Last, step.gone)
		}
	}
}
