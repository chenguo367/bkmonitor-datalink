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
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The write-through State cache, one rule of fourth-batch design §4.3 at a
// time, from the store's own entry points: LoadRuntime reads back only what
// the rules say it must, and a write over an external change is restored
// only when the stored bytes are older than what this process wrote.

type cacheRig struct {
	t       *testing.T
	backend *pipelineMemoryBackend
	store   *ExecutionStore
	now     time.Time
	begun   bool
}

func newCacheRig(t *testing.T, budget int64) *cacheRig {
	t.Helper()
	rig := &cacheRig{t: t, backend: newPipelineMemoryBackend(), now: time.Unix(1_700_000_000, 0), begun: true}
	router, err := NewFixedRouter("state-01", rig.backend)
	if err != nil {
		t.Fatal(err)
	}
	rig.store, err = NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute,
		FenceKeys: fixedFenceKeys{testFenceKeys()}, StateCacheBudgetBytes: budget, StateCacheReplayAge: time.Minute,
		Now: func() time.Time { return rig.now },
		RestoreProgressCheck: func(context.Context, execution.QueryGroupIdentity, execution.EvaluationTime) (bool, error) {
			return rig.begun, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

// slot is one Slot of the Query Group "q" under the given epoch and Segment
// start.
type slot struct {
	at           execution.EvaluationTime
	epoch        execution.StateApplyEpoch
	segmentStart execution.EvaluationTime
	fence        execution.OwnerFence
	sharded      bool
}

func (s slot) contract() execution.FrozenExecutionContractRef {
	ref := frozenRef()
	ref.Slot.EvaluationTime, ref.ScheduleSegmentStart = s.at, s.segmentStart
	return ref
}

func (s slot) version() execution.ApplyVersion {
	return execution.ApplyVersion{StateApplyEpoch: s.epoch, EvaluationTime: s.at, SlotDigest: "slot"}
}

func fenceOf(epoch uint64) execution.OwnerFence {
	return execution.OwnerFence{QueryGroup: frozenRef().Slot.QueryGroup, OwnerID: "worker-1", OwnerEpoch: epoch, LeaseToken: "lease-token"}
}

// round loads the series, then writes them at revision, and returns how many
// MGETs the load took and the apply's result.
func (rig *cacheRig) round(s slot, series int, revision uint64) (int, execution.StateApplyResult) {
	rig.t.Helper()
	mutations := make([]execution.StateMutation, series)
	items := make([]execution.StatePreflightItem, series)
	for index := range mutations {
		mutations[index] = seriesMutation(rig.t, seriesIdentity(index), s.version(), revision, "")
		items[index] = execution.StatePreflightItem{Identity: mutations[index].Identity, ApplyVersion: s.version(), Sharded: s.sharded}
	}
	before := rig.backend.mgetCalls
	if _, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence}); err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	reads := rig.backend.mgetCalls - before
	fence := testApplyFence()
	fence.Fence = s.fence
	result, err := rig.store.ApplyRuntimeFenced(context.Background(),
		execution.StateApplyRequest{Contract: s.contract(), Retention: testRetention(), Items: mutations}, fence)
	if err != nil {
		rig.t.Fatalf("ApplyRuntimeFenced() error = %v", err)
	}
	return reads, result
}

// load reads the series as a Slot of a frozen Plan does, writing nothing,
// and returns how many reads reached the store.
func (rig *cacheRig) load(s slot, series int) int {
	rig.t.Helper()
	items := make([]execution.StatePreflightItem, series)
	for index := range items {
		items[index] = execution.StatePreflightItem{Identity: seriesIdentity(index), ApplyVersion: s.version(), Sharded: s.sharded}
	}
	before := rig.backend.mgetCalls
	if _, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence}); err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	return rig.backend.mgetCalls - before
}

func (rig *cacheRig) lookups(result string) uint64 {
	return rig.store.StateCacheStats().Lookups[result]
}

var (
	first  = slot{at: 60, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	second = slot{at: 120, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
)

func TestAForwardSlotUnderTheSameFenceAndActivationIsServedFromTheCache(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	if reads, result := rig.round(first, 4, 0); reads == 0 {
		t.Fatal("the first Slot read nothing back, with nothing cached")
	} else {
		requireAllStatus(t, result, execution.StateApplied)
	}
	rig.now = rig.now.Add(time.Minute)
	reads, result := rig.round(second, 4, 1)
	requireAllStatus(t, result, execution.StateApplied)
	if reads != 0 || rig.lookups(StateCacheHit) != 4 {
		t.Fatalf("the next Slot read %d times, %d hits; want every series served from the cache", reads, rig.lookups(StateCacheHit))
	}
}

func TestEachRuleThatDoesNotHoldReadsTheStoreBack(t *testing.T) {
	for name, test := range map[string]struct {
		next slot
		word string
	}{
		"another owner fence": {slot{at: 120, epoch: 1, segmentStart: 60, fence: fenceOf(4)}, StateCacheMissFence},
		"the same Slot again": {slot{at: 60, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, StateCacheMissReplay},
		"another epoch":       {slot{at: 120, epoch: 2, segmentStart: 60, fence: fenceOf(3)}, StateCacheMissActivation},
		"a split strategy":    {slot{at: 120, epoch: 1, segmentStart: 60, fence: fenceOf(3), sharded: true}, StateCacheMissSharded},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newCacheRig(t, 1<<20)
			rig.round(first, 2, 0)
			rig.now = rig.now.Add(time.Minute)
			revision := uint64(1)
			if test.word == StateCacheMissReplay {
				revision = 0
			}
			if reads, _ := rig.round(test.next, 2, revision); reads == 0 || rig.lookups(test.word) != 2 || rig.lookups(StateCacheHit) != 0 {
				t.Fatalf("reads %d, %s %d, hits %d; want the store read back under %s",
					reads, test.word, rig.lookups(test.word), rig.lookups(StateCacheHit), test.word)
			}
		})
	}
}

// A frozen series is read every Slot and never written, so its entry ages
// while it is touched: served up to its usable life (the write's TTL less
// the replay age) and not past it, where the key it answers for may have
// expired in the store (rule 3). It leaves under expired, not untouched. The
// series beside it is no longer in the answer when their life ends, and
// leaves all the same: the sweep at the start of the group's read takes out
// what is past its life, asked for or not.
func TestAFrozenSeriesIsServedUntilItsEntrysUsableLifeAndNotPast(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	ttl, err := rig.store.runtimeTTL(testRetention(), 0)
	if err != nil {
		t.Fatal(err)
	}
	usableUntil := rig.now.Add(ttl - time.Minute)
	at := first.at
	for {
		rig.now = rig.now.Add(time.Minute)
		at += 60
		asked := 2
		if !rig.now.Before(usableUntil) {
			asked = 1
		}
		hits := rig.lookups(StateCacheHit)
		reads := rig.load(slot{at: at, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, asked)
		served := rig.lookups(StateCacheHit) - hits
		if rig.now.Before(usableUntil) {
			if served != 2 || reads != 0 {
				t.Fatalf("at %s before the usable life ends, %d served and %d reads; want both served", usableUntil.Sub(rig.now), served, reads)
			}
			continue
		}
		departures := rig.store.StateCacheStats().Departures
		if served != 0 || reads == 0 || departures[StateCacheDepartExpired] != 2 || departures[StateCacheDepartUntouched] != 0 {
			t.Fatalf("%s past the usable life, %d served, %d reads, departures %v; want the store read back and both out under expired, the one not asked for too",
				rig.now.Sub(usableUntil), served, reads, departures)
		}
		return
	}
}

// A frozen series' renewal that finds its key gone takes the key out of the
// cache (rule 4): the next Slot reads it absent from the store instead of
// being served the bytes it would answer for. The key beside it, still
// there, is served.
func TestARenewalThatFindsTheKeyGoneTakesItOut(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	gone, err := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	if err != nil {
		t.Fatal(err)
	}
	delete(rig.backend.values, gone)
	ttl, err := rig.store.runtimeTTL(testRetention(), 0)
	if err != nil {
		t.Fatal(err)
	}
	renewal, err := rig.store.RenewFrozenRuntime(context.Background(), execution.FrozenStateRenewalRequest{
		Contract: frozenRef(), Retention: testRetention(),
		Now: time.Unix(int64(first.at), 0).Add(ttl - time.Second),
		Items: []execution.FrozenSeriesState{{Identity: seriesIdentity(0), LastApplied: first.at,
			Representation: execution.StateRepresentationFramed}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if renewal.Items[0].Outcome != execution.FrozenRenewalMissing {
		t.Fatalf("fixture: the renewal ended %q, want %q", renewal.Items[0].Outcome, execution.FrozenRenewalMissing)
	}
	if got := rig.store.StateCacheStats().Departures[StateCacheDepartRenewalMissing]; got != 1 {
		t.Fatalf("departures{renewal_missing} = %d, want the gone key out", got)
	}
	rig.now = rig.now.Add(time.Minute)
	absent := rig.lookups(StateCacheMissAbsent)
	if reads, _ := rig.round(second, 2, 1); reads == 0 || rig.lookups(StateCacheMissAbsent) != absent+1 ||
		rig.lookups(StateCacheHit) != 1 {
		t.Fatalf("reads %d, miss_absent +%d, hits %d; want the gone key read from the store and the other served",
			reads, rig.lookups(StateCacheMissAbsent)-absent, rig.lookups(StateCacheHit))
	}
}

// The Segment start stamps the activation beside the epoch: an epoch reused
// - the catalog's counter lost with the Redis that held it - under another
// Segment is not taken for the same activation (per 11-09).
func TestAReusedEpochUnderAnotherSegmentIsAnotherActivation(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	rig.now = rig.now.Add(time.Minute)
	if reads, _ := rig.round(slot{at: 120, epoch: 1, segmentStart: 120, fence: fenceOf(3)}, 2, 1); reads == 0 || rig.lookups(StateCacheMissActivation) != 2 {
		t.Fatalf("reads %d, miss_activation %d; want the store read back", reads, rig.lookups(StateCacheMissActivation))
	}
}

// A split strategy's Plan is read back every round, its second round too.
func TestASplitStrategysSeriesAreNeverServedFromTheCache(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	sharded := first
	sharded.sharded = true
	firstReads, _ := rig.round(sharded, 2, 0)
	rig.now = rig.now.Add(time.Minute)
	next := second
	next.sharded = true
	if reads, _ := rig.round(next, 2, 1); reads != firstReads || rig.lookups(StateCacheHit) != 0 || rig.lookups(StateCacheMissSharded) != 4 {
		t.Fatalf("second round read %d times (first %d), hits %d, miss_sharded %d; want the reads unchanged",
			reads, firstReads, rig.lookups(StateCacheHit), rig.lookups(StateCacheMissSharded))
	}
}

// A write whose outcome is unknown leaves no key of its request served.
func TestAWriteInDoubtIsReadBackNextRound(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	rig.now = rig.now.Add(time.Minute)
	rig.backend.failPipeline = errors.New("connection reset")
	rig.round(second, 2, 1)
	rig.backend.failPipeline = nil
	rig.now = rig.now.Add(time.Minute)
	third := slot{at: 180, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	// Two absent the first round, nothing cached yet, and two after the doubt.
	if reads, _ := rig.round(third, 2, 1); reads == 0 || rig.lookups(StateCacheMissAbsent) != 4 {
		t.Fatalf("reads %d, miss_absent %d; want the store read back after the write in doubt", reads, rig.lookups(StateCacheMissAbsent))
	}
}

// One key of a request ends in doubt and the other is written: the written
// one is not kept either, since the request as a whole may have landed in
// part, and both leave under unknown, not conflict.
func TestAWriteInDoubtTakesOutTheKeysWrittenBesideIt(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 2, 0)
	doubted, err := RuntimeStateKeyV3("alarmd", seriesIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	rig.now = rig.now.Add(time.Minute)
	rig.backend.failKey = doubted
	if _, result := rig.round(second, 2, 1); result.Items[0].Status != execution.StateApplied ||
		result.Items[1].Status != execution.StateApplyRetryable {
		t.Fatalf("fixture: the round ended %s / %s, want one written and one in doubt", result.Items[0].Status, result.Items[1].Status)
	}
	rig.backend.failKey = ""
	departures := rig.store.StateCacheStats().Departures
	if departures[StateCacheDepartUnknown] != 2 || departures[StateCacheDepartConflict] != 0 {
		t.Fatalf("departures %v, want both keys out under unknown", departures)
	}
	rig.now = rig.now.Add(time.Minute)
	third := slot{at: 180, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	hits := rig.lookups(StateCacheHit)
	if reads, _ := rig.round(third, 2, 2); reads == 0 || rig.lookups(StateCacheHit) != hits {
		t.Fatalf("reads %d, %d served from the cache; want the written key read back too", reads, rig.lookups(StateCacheHit)-hits)
	}
}

// A key the cache served that was deleted underneath it is written back in
// the same Slot, expecting it gone.
func TestADeletedKeyIsWrittenBackInTheSameSlot(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 1, 0)
	key, err := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	if err != nil {
		t.Fatal(err)
	}
	delete(rig.backend.values, key)
	rig.now = rig.now.Add(time.Minute)
	if _, result := rig.round(second, 1, 1); result.Items[0].Status != execution.StateApplied {
		t.Fatalf("after the delete the write ended %s, want it restored", result.Items[0].Status)
	}
	if _, found := rig.backend.values[key]; !found || rig.store.StateCacheStats().Restored[StateCacheRestoreMissing] != 1 {
		t.Fatalf("key restored %v, restored{missing} %d", found, rig.store.StateCacheStats().Restored[StateCacheRestoreMissing])
	}
}

// Bytes no older than what this process wrote are a legitimate writer's:
// the write is an ordinary conflict, and those bytes stay.
func TestANewerStoredValueIsNotWrittenOver(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 1, 0)
	key, _ := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	// Another writer's record, at a later Slot than the cache's.
	other := newCacheRig(t, 0)
	other.round(slot{at: 90, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, 1, 0)
	newer := append([]byte(nil), other.backend.values[key]...)
	rig.backend.values[key] = newer
	rig.now = rig.now.Add(time.Minute)
	if _, result := rig.round(second, 1, 1); result.Items[0].Status == execution.StateApplied {
		t.Fatal("a newer stored value was written over")
	}
	if string(rig.backend.values[key]) != string(newer) || rig.store.StateCacheStats().Declined[StateCacheDeclineNewerVersion] != 1 {
		t.Fatalf("stored bytes kept %v, declined{newer_version} %d", string(rig.backend.values[key]) == string(newer),
			rig.store.StateCacheStats().Declined[StateCacheDeclineNewerVersion])
	}
}

// Bytes older than what this process wrote - a rollback - are written over
// only while the Progress still has this Slot begun.
func TestARolledBackKeyIsWrittenBackOnlyWhileTheProgressHasTheSlot(t *testing.T) {
	for _, begun := range []bool{true, false} {
		rig := newCacheRig(t, 1<<20)
		rig.begun = begun
		rig.round(first, 1, 0)
		key, _ := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
		older := newCacheRig(t, 0)
		older.round(slot{at: 30, epoch: 1, segmentStart: 30, fence: fenceOf(3)}, 1, 0)
		rig.backend.values[key] = append([]byte(nil), older.backend.values[key]...)
		rig.now = rig.now.Add(time.Minute)
		_, result := rig.round(second, 1, 1)
		stats := rig.store.StateCacheStats()
		if begun && (result.Items[0].Status != execution.StateApplied || stats.Restored[StateCacheRestoreRolledBack] != 1) {
			t.Fatalf("Progress at the Slot: write %s, restored{rolled_back} %d; want restored", result.Items[0].Status, stats.Restored[StateCacheRestoreRolledBack])
		}
		if !begun && (result.Items[0].Status == execution.StateApplied || stats.Declined[StateCacheDeclineProgressRolledBack] != 1) {
			t.Fatalf("Progress rolled back too: write %s, declined{progress_rolled_back} %d; want no restore", result.Items[0].Status, stats.Declined[StateCacheDeclineProgressRolledBack])
		}
	}
}

// Full, the cache takes in no new Query Group and keeps the resident one; its
// charge never passes the budget.
func TestAFullCacheKeepsItsResidentsAndTakesInNoNewQueryGroup(t *testing.T) {
	rig := newCacheRig(t, 1)
	rig.round(first, 2, 0)
	if stats := rig.store.StateCacheStats(); stats.Bytes > stats.BudgetBytes || stats.QueryGroups != 0 {
		t.Fatalf("charge %d of %d, %d groups; want nothing taken in past the budget", stats.Bytes, stats.BudgetBytes, stats.QueryGroups)
	}
	rig.now = rig.now.Add(time.Minute)
	if reads, _ := rig.round(second, 2, 1); reads == 0 || rig.lookups(StateCacheMissOverBudget) != 2 {
		t.Fatalf("reads %d, miss_over_budget %d; want the group kept out and read back", reads, rig.lookups(StateCacheMissOverBudget))
	}
}

// Every departure gives its charge back: a Query Group released leaves the
// cache at zero.
func TestADepartureGivesItsChargeBack(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 3, 0)
	if rig.store.StateCacheStats().Bytes == 0 {
		t.Fatal("nothing was charged")
	}
	rig.store.DropStateCacheQueryGroup(frozenRef().Slot.QueryGroup)
	if stats := rig.store.StateCacheStats(); stats.Bytes != 0 || stats.QueryGroups != 0 || stats.Departures[StateCacheDepartReleased] != 3 {
		t.Fatalf("after release: charge %d, groups %d, departures{released} %d", stats.Bytes, stats.QueryGroups, stats.Departures[StateCacheDepartReleased])
	}
}

// A Plan moved to another Query Group and back: meanwhile the other holder
// wrote the key, and the fence here never changed. The Plan's activation did,
// so the Slot after the move back reads the store - the other holder's bytes,
// not the history cached before the move (rule 6).
func TestAPlanMovedAwayAndBackReadsWhatTheOtherHolderWrote(t *testing.T) {
	rig := newCacheRig(t, 1<<20)
	rig.round(first, 1, 0)
	key, _ := RuntimeStateKeyV3("alarmd", seriesIdentity(0))
	other := newCacheRig(t, 0)
	other.round(slot{at: 90, epoch: 2, segmentStart: 90, fence: fenceOf(9)}, 1, 0)
	rig.backend.values[key] = append([]byte(nil), other.backend.values[key]...)
	rig.now = rig.now.Add(time.Minute)
	back := slot{at: 120, epoch: 3, segmentStart: 120, fence: fenceOf(3)}
	items := []execution.StatePreflightItem{{Identity: seriesIdentity(0), ApplyVersion: back.version()}}
	loaded, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: back.contract(), Items: items, OwnerFence: back.fence})
	if err != nil {
		t.Fatal(err)
	}
	if rig.lookups(StateCacheMissActivation) != 1 || loaded.Items[0].PersistedApplyVersion.EvaluationTime != 90 {
		t.Fatalf("miss_activation %d, read the record of Slot %d; want the other holder's, of Slot 90",
			rig.lookups(StateCacheMissActivation), loaded.Items[0].PersistedApplyVersion.EvaluationTime)
	}
}
