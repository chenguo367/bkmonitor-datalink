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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A value written back is kept compact when it can be, by the rule a value
// read back is (014 next9 1, the first step of writes kept compact), and as
// its bytes when it cannot - a record carrying a series guard: a whole
// round's writes are held compact and served the round after, and the one
// that does not compact is held all the same. Before, every value written
// was kept as its bytes, some four times a compact one, and a group that
// wrote a whole round into a cache sized by its compact members was put out
// over the budget by it.
func TestAWrittenValueIsKeptCompactWhenItCanBeAndAsItsBytesWhenItCannot(t *testing.T) {
	const series = 12
	rig := newCacheRig(t, 1<<20)
	rig.round(first, series, 0)
	stats := rig.store.StateCacheStats()
	if stats.Entries[StateCacheEntryCompact] != series || stats.Entries[StateCacheEntryWritten] != 0 {
		t.Fatalf("after a round writing %d series the cache holds %v, want every one compact", series, stats.Entries)
	}
	hits := rig.lookups(StateCacheHit)
	rig.now = rig.now.Add(time.Minute)
	if reads, _ := rig.round(second, series, 1); reads != 0 || rig.lookups(StateCacheHit)-hits != series {
		t.Fatalf("the round after read %d times from the store and was served %d, want all %d served", reads,
			rig.lookups(StateCacheHit)-hits, series)
	}

	// One more series, its record carrying a series guard, which no compact
	// member holds.
	ctx := context.Background()
	identity := seriesIdentity(series)
	at := int64(second.version().EvaluationTime)
	guarded, err := execution.BuildStateMutation(execution.StateMutation{Identity: identity, ApplyVersion: second.version(),
		AffectedRecords: []execution.RecordAnchor{derivedAnchor(t, identity, at)},
		SeriesGuard: &execution.StateGuardFact{Status: execution.HistoryWarming,
			ReasonCode: execution.ReasonCode(contract.ReasonHistoryWarming), WarmupRequirementRef: "series-warm"},
		Levels: []execution.RuntimeLevelStateMutation{{LevelID: 1, LevelStateCompatibility: "compat", HistoryCompleteness: execution.HistoryFull,
			WarmupRequirementRef: "warm", LastProcessedEventTime: at}},
		Points: []execution.StateHistoryPoint{derivedPoint(t, identity, at, "detect", execution.LevelFactNormal)}})
	if err != nil {
		t.Fatal(err)
	}
	items := []execution.StatePreflightItem{{Identity: identity, ApplyVersion: second.version()}}
	if _, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: second.contract(), Items: items,
		OwnerFence: second.fence}); err != nil {
		t.Fatal(err)
	}
	fence := testApplyFence()
	fence.Fence = second.fence
	result, err := rig.store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: second.contract(), Retention: testRetention(),
		Items: []execution.StateMutation{guarded}}, fence)
	if err != nil || len(result.Items) != 1 || result.Items[0].Status != execution.StateApplied {
		t.Fatalf("fixture: the guarded write answered %+v, %v", result.Items, err)
	}
	if stats := rig.store.StateCacheStats(); stats.Entries[StateCacheEntryWritten] != 1 || stats.Entries[StateCacheEntryCompact] != series {
		t.Fatalf("after a write carrying a series guard the cache holds %v, want it as its bytes beside the %d compact", stats.Entries,
			series)
	}
}

// A value written back compact that no round reads any more is let go
// untouched two of its Plan's intervals after a round last touched it, as a
// value kept as its bytes is (014 next9 1): with no write before it kept as
// bytes to read its interval from - written once, then read by a round that
// leaves it unwritten, as a stable member is - it takes the interval its
// write's retention names. Three minutes after that read - past two
// one-minute intervals, inside the values' lifetime - the next read's sweep
// lets every one of them go. Before, such a value had no interval, and it
// stayed until it aged out.
func TestAValueWrittenCompactThatNoRoundReadsIsLetGoUntouched(t *testing.T) {
	const series = 6
	rig := newCacheRig(t, 1<<20)
	rig.round(first, series, 0)
	rig.now = rig.now.Add(time.Minute)
	hits := rig.lookups(StateCacheHit)
	if reads := rig.load(second, series); reads != 0 || rig.lookups(StateCacheHit)-hits != series {
		t.Fatalf("fixture: the round after read %d times from the store and was served %d of %d", reads,
			rig.lookups(StateCacheHit)-hits, series)
	}
	if stats := rig.store.StateCacheStats(); stats.Entries[StateCacheEntryCompact] != series {
		t.Fatalf("fixture: the cache holds %v, want all %d compact", stats.Entries, series)
	}
	rig.now = rig.now.Add(3 * time.Minute)
	rig.load(slot{at: 300, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, 1)
	stats := rig.store.StateCacheStats()
	if untouched := stats.StateDepartures[StateCacheDepartUntouched]; untouched != series || stats.StateDepartures[StateCacheDepartExpired] != 0 {
		t.Fatalf("%d values let go untouched (departures %v), want all %d, none aged out", untouched, stats.StateDepartures, series)
	}
}
