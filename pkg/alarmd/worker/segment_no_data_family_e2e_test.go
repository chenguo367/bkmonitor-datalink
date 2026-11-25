// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The no-data family's segments (014 next9 ⑥) through Execute on the real
// store, the shadow and the skip as production runs them: a Plan with
// no-data detection keeps the synthetic series of its no-data view in a book
// of their own, left under the Leader's two words and read through that
// book as a round that wrote them whole would have read them.

// noDataFamilyRigs is three rigs of one Plan with no-data detection: one
// dispatched with no word, writing every State; one with the segment skip
// word alone; one with both words.
type noDataFamilyRigs struct {
	writing, realOnly, skipping *shadowRig
}

func newNoDataFamilyRigs(t *testing.T, hosts int, continuous uint32) *noDataFamilyRigs {
	t.Helper()
	noData := &contract.NoDataConfigV1{Continuous: continuous, Level: 2}
	rigs := &noDataFamilyRigs{
		writing:  newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 1, noData, 16<<20),
		realOnly: newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 1, noData, 16<<20),
		skipping: newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 1, noData, 16<<20),
	}
	rigs.realOnly.skipWord.Store(7)
	rigs.skipping.skipWord.Store(7)
	rigs.skipping.skipNoDataWord.Store(7)
	return rigs
}

func (rigs *noDataFamilyRigs) each(fn func(name string, rig *shadowRig)) {
	fn("writing", rigs.writing)
	fn("real-only", rigs.realOnly)
	fn("skipping", rigs.skipping)
}

// step runs the Slot at at on every rig: each completes, and each sends the
// same number of events.
func (rigs *noDataFamilyRigs) step(t *testing.T, at execution.EvaluationTime, absent []string, partial bool) {
	t.Helper()
	sent := map[string]int{}
	rigs.each(func(name string, rig *shadowRig) {
		before := rig.ports.eventCount
		if completed, err := rig.run(at, absent, nil, partial); !completed && !partial {
			t.Fatalf("%s Slot at %d: err=%v", name, at, err)
		}
		sent[name] = rig.ports.eventCount - before
	})
	if sent["skipping"] != sent["writing"] || sent["real-only"] != sent["writing"] {
		t.Fatalf("at %d the rigs sent %v events, want the same", at, sent)
	}
}

func skippedSoFar(rig *shadowRig) int64 {
	rig.observed.mu.Lock()
	defer rig.observed.mu.Unlock()
	return rig.observed.notWritten[observability.StateNotWrittenSegmentMemberSkipped]
}

// sameLastWrites is that the two rigs last wrote every series alike: the
// history it read - the points the segments made for the rounds left, as
// written ones - the round's own points, its Levels and guard.
func sameLastWrites(t *testing.T, got, want *shadowRig) {
	t.Helper()
	got.store.mu.Lock()
	want.store.mu.Lock()
	defer got.store.mu.Unlock()
	defer want.store.mu.Unlock()
	if len(want.store.written) == 0 || len(got.store.written) != len(want.store.written) {
		t.Fatalf("%d series written, want %d", len(got.store.written), len(want.store.written))
	}
	for identity, wanted := range want.store.written {
		written, found := got.store.written[identity]
		if !found {
			t.Fatalf("%s never written", identity.SeriesIdentityDigest)
		}
		if !reflect.DeepEqual(written.BaseHistory, wanted.BaseHistory) || !reflect.DeepEqual(written.Points, wanted.Points) ||
			!reflect.DeepEqual(written.Levels, wanted.Levels) || !reflect.DeepEqual(written.SeriesGuard, wanted.SeriesGuard) {
			t.Fatalf("%s last written %+v + %+v Levels %+v\nwant %+v + %+v Levels %+v", identity.SeriesIdentityDigest,
				written.BaseHistory, written.Points, written.Levels, wanted.BaseHistory, wanted.Points, wanted.Levels)
		}
	}
}

// A steady no-data series joins its family's page and is left unwritten
// under both words, never under the segment skip word alone; it compares a
// match round after round, no mismatch, and is kept compact in the State
// cache; and once the words go away, the round written whole from what its
// family's book made leaves every series as a rig that never left one wrote
// it (014 next9 ⑥ §6 cases 1, 2, 5, 6).
// Continuous 3 holds the no-data view to three points while the real series
// keep one: read by the real family's retention, the made read would be
// short of the write (R2).
func TestAStableNoDataSeriesIsLeftInItsOwnFamilyAndReadsAsWrittenWhole(t *testing.T) {
	for _, continuous := range []uint32{1, 3} {
		t.Run(fmt.Sprint("continuous=", continuous), func(t *testing.T) {
			rigs := newNoDataFamilyRigs(t, 3, continuous)
			at := frozenContract().Slot.EvaluationTime
			for round := 0; round < 10; round++ {
				at += 60
				rigs.step(t, at, nil, false)
			}
			skipping, realOnly := rigs.skipping, rigs.realOnly
			if open, _ := skipping.storedMembersOf(at, segment.FamilyNoData); open != 1 {
				t.Fatalf("%d open members on the no-data family's page, want the whole-item group", open)
			}
			if matches := skipping.observed.family(observability.SegmentFamilyNoData, "outcome/"+string(observability.SegmentShadowMatch)); matches == 0 {
				t.Fatal("the no-data family compared no match")
			}
			for name, rig := range map[string]*shadowRig{"skipping": skipping, "real-only": realOnly} {
				if mismatches := rig.observed.family(observability.SegmentFamilyNoData, "outcome/mismatch"); mismatches != 0 {
					t.Fatalf("the %s rig's no-data family compared %d mismatches", name, mismatches)
				}
			}
			// Under both words the no-data family's rounds skip; under the
			// segment skip word alone they are off, and its series written.
			if skips := skipping.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip); skips == 0 {
				t.Fatal("no round of the no-data family skipped under both words")
			}
			if skips := realOnly.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeSkip); skips != 0 {
				t.Fatalf("%d rounds of the no-data family skipped under the segment skip word alone", skips)
			}
			if off := realOnly.observed.family(observability.SegmentFamilyNoData, "round/"+segment.SkipModeOff); off == 0 {
				t.Fatal("the no-data family's rounds under the segment skip word alone are not counted off")
			}
			if left, leftReal := skippedSoFar(skipping), skippedSoFar(realOnly); left <= leftReal || leftReal == 0 {
				t.Fatalf("left %d members under both words, %d under the segment skip word alone: want the no-data series left beside the real",
					left, leftReal)
			}
			// The State cache keeps the no-data series compact beside the
			// real ones, none declined (§6 case 5): its template is its own
			// by its Levels, what the Plan's members share the same for both.
			stats := skipping.store.StateCacheStats()
			var declined uint64
			for _, count := range stats.CompactDeclined {
				declined += count
			}
			if stats.Entries[state.StateCacheEntryCompact] < 3+1 || declined != 0 {
				t.Fatalf("cache entries %v, compact declined %v: want the three real series and the no-data one compact, none declined",
					stats.Entries, stats.CompactDeclined)
			}
			// The words go away: the round writes every State, the series
			// left from what their books made.
			skipping.skipWord.Store(0)
			skipping.skipNoDataWord.Store(0)
			realOnly.skipWord.Store(0)
			at += 60
			rigs.step(t, at, nil, false)
			sameLastWrites(t, skipping, rigs.writing)
			sameLastWrites(t, realOnly, rigs.writing)
		})
	}
}

// The no-data family's piece has its sampled round of the day on a day of
// its own (014 next9 ⑥ 2.2): every synthetic member written whole and
// compared, counted under round=sampled, family=no_data.
func TestTheNoDataFamilysSampledRoundComparesItsMembersWhole(t *testing.T) {
	start := frozenContract().Slot.EvaluationTime
	sampled := execution.EvaluationTime(0)
	for at := start + 600; at < start+600+86400; at += 60 {
		if worker.SegmentSampledRoundOf(planIdentity(), "state-v1", segment.FamilyNoData, int64(at), 60) &&
			!worker.SegmentSampledRoundOf(planIdentity(), "state-v1", segment.FamilyReal, int64(at), 60) {
			sampled = at
			break
		}
	}
	if sampled == 0 {
		t.Fatal("fixture: the no-data family's piece has no sampled round apart from the real one's in a day")
	}
	rigs := newNoDataFamilyRigs(t, 3, 1)
	at := sampled - 600
	for at < sampled {
		at += 60
		rigs.step(t, at, nil, false)
	}
	if matches := rigs.skipping.observed.family(observability.SegmentFamilyNoData,
		"outcome/"+string(observability.SegmentShadowMatch)+"/"+observability.SegmentRoundSampled); matches == 0 {
		t.Fatal("the no-data family's sampled round compared no match")
	}
	if samples := rigs.skipping.observed.family(observability.SegmentFamilyNoData, "sample"); samples != 1 {
		t.Fatalf("%d sampled rounds of the no-data family, want the one", samples)
	}
}

// A no-data group gone absent - its value 1 - is written that round and
// taken off its page, its alert raised as by the rig that writes every
// round; a round not complete that does not see the group produces no
// synthetic series and closes the family's members, and the round back
// writes them whole: what every rig wrote is the same (014 next9 ⑥ §3, §6
// cases 3, 4).
func TestANoDataSeriesThatTurnsOrGoesUnreadIsWrittenAsByARigThatNeverLeftIt(t *testing.T) {
	rigs := newNoDataFamilyRigs(t, 2, 1)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 8; round++ {
		at += 60
		rigs.step(t, at, nil, false)
	}
	if open, _ := rigs.skipping.storedMembersOf(at, segment.FamilyNoData); open != 1 {
		t.Fatalf("%d open members on the no-data family's page before the round goes incomplete, want 1", open)
	}
	// Not complete, and the group not seen in what came back: its absence
	// cannot be judged (UNAVAILABLE), no synthetic series, the member
	// closes. A PARTIAL round that sees the group judges it present.
	at += 60
	rigs.step(t, at, []string{"host-00", "host-01"}, true)
	if departed := rigs.skipping.observed.family(observability.SegmentFamilyNoData, "member/"+string(observability.SegmentShadowDeparted)); departed == 0 {
		t.Fatal("the round not complete closed no member of the no-data family")
	}
	for round := 0; round < 4; round++ {
		at += 60
		rigs.step(t, at, nil, false)
	}
	// Every host gone: the whole-item group is absent, its value 1.
	before := rigs.writing.ports.eventCount
	at += 60
	rigs.step(t, at, []string{"host-00", "host-01"}, false)
	if rigs.writing.ports.eventCount == before {
		t.Fatal("fixture: the group gone absent raised no event")
	}
	if open, _ := rigs.skipping.storedMembersOf(at, segment.FamilyNoData); open != 0 {
		t.Fatalf("%d open members on the no-data family's page after its group went absent, want none", open)
	}
	rigs.skipping.skipWord.Store(0)
	rigs.skipping.skipNoDataWord.Store(0)
	rigs.realOnly.skipWord.Store(0)
	at += 60
	rigs.step(t, at, nil, false)
	sameLastWrites(t, rigs.skipping, rigs.writing)
}
