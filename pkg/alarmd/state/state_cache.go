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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The write-through State cache (0.6 fourth-batch design §4): the packed
// bytes this process last wrote successfully under each runtime key, served
// to LoadRuntime instead of reading them back while the rules below say the
// store still holds exactly those bytes. A write's compare-and-set compares
// against the stored bytes, never against these, so a cache that is wrong
// costs a conflict and a re-read; the rules exist so that an evaluation never
// runs on history the store does not hold, since its events go out first.

// Lookup results, the closed vocabulary of state_cache_lookups_total.
const (
	StateCacheHit            = "hit"
	StateCacheMissAbsent     = "miss_absent"
	StateCacheMissFence      = "miss_fence"
	StateCacheMissUnknown    = "miss_unknown"
	StateCacheMissExpired    = "miss_expired"
	StateCacheMissReplay     = "miss_replay"
	StateCacheMissActivation = "miss_activation"
	StateCacheMissSharded    = "miss_sharded"
	StateCacheMissOverBudget = "miss_over_budget"
	StateCacheMissYielded    = "miss_yielded"
)

// StateCacheLookupResults lists them, for the metric's cells.
var StateCacheLookupResults = []string{StateCacheHit, StateCacheMissAbsent, StateCacheMissFence, StateCacheMissUnknown,
	StateCacheMissExpired, StateCacheMissReplay, StateCacheMissActivation, StateCacheMissSharded, StateCacheMissOverBudget,
	StateCacheMissYielded}

// Departure reasons, the closed vocabulary of state_cache_departures_total.
const (
	StateCacheDepartReleased       = "released"
	StateCacheDepartUnknown        = "unknown"
	StateCacheDepartConflict       = "conflict"
	StateCacheDepartExpired        = "expired"
	StateCacheDepartRenewalMissing = "renewal_missing"
	StateCacheDepartUntouched      = "untouched"
	// StateCacheDepartOverBudget is a Query Group that did not fit the
	// cache's budget.
	StateCacheDepartOverBudget = "over_budget"
	// StateCacheDepartYielded is a Query Group that fit the budget but not
	// what the Slots left of the retained pool: given back for a Slot that
	// needed the room, or not taken in past it.
	StateCacheDepartYielded = "yielded"
	// StateCacheDepartMismatch is a value the sampled round's check found
	// the store no longer held (014 next7 3.6).
	StateCacheDepartMismatch = "mismatch"
)

// StateCacheDepartureReasons lists them.
var StateCacheDepartureReasons = []string{StateCacheDepartReleased, StateCacheDepartUnknown, StateCacheDepartConflict,
	StateCacheDepartExpired, StateCacheDepartRenewalMissing, StateCacheDepartUntouched, StateCacheDepartOverBudget, StateCacheDepartYielded,
	StateCacheDepartMismatch}

// What the sampled round's check of a value the cache held found (014
// next7 3.6), closed: state_cache_readback_verify_total.
const (
	StateCacheVerifyMatch    = "match"
	StateCacheVerifyMismatch = "mismatch"
)

// StateCacheVerifyResults lists them.
var StateCacheVerifyResults = []string{StateCacheVerifyMatch, StateCacheVerifyMismatch}

// What a read asking for a Query Group's sweep did, closed:
// state_cache_sweeps_total. walked is a Slot's first read, which walks the
// group's entries; already_swept a later read of a Slot whose group the cache
// has swept.
const (
	StateCacheSweepWalked       = "walked"
	StateCacheSweepAlreadySwept = "already_swept"
)

// StateCacheSweepResults lists them.
var StateCacheSweepResults = []string{StateCacheSweepWalked, StateCacheSweepAlreadySwept}

// Entry kinds, closed: a Runtime State value, or a Plan's stable segment - a
// mark or a member page (014 B3). The two share the cache's budget.
const (
	StateCacheKindState   = "state"
	StateCacheKindSegment = "segment"
)

// StateCacheKinds lists them.
var StateCacheKinds = []string{StateCacheKindState, StateCacheKindSegment}

// What became of a value a preflight read back, as a candidate entry of the
// cache (014 next7 3.1), closed: kept, taken in (or judged against the budget
// as a write is); declined_not_member, not an open member of its Plan's
// stable segment as the Slot read it, or a Plan of a split strategy;
// declined_operation, a member its round writes whole - not a normal round,
// the segment skip not followed, the Plan's sampled round, a stopped Plan, a
// page past half its life unrenewed; declined_no_renewal, a member left out
// whose page's last renewal leaves its key less than the replay age. Counted
// for every record a Slot's own preflight found, under its fence.
const (
	StateCacheReadbackKept              = "kept"
	StateCacheReadbackDeclinedNotMember = "declined_not_member"
	StateCacheReadbackDeclinedOperation = "declined_operation"
	StateCacheReadbackDeclinedNoRenewal = "declined_no_renewal"
)

// StateCacheReadbackResults lists them.
var StateCacheReadbackResults = []string{StateCacheReadbackKept, StateCacheReadbackDeclinedNotMember,
	StateCacheReadbackDeclinedOperation, StateCacheReadbackDeclinedNoRenewal}

// What a page renewal did to the State entries of its members (014 next7
// 3.2), closed: renewed, their life moved to the renewal's; dropped_missing,
// let go because the renewal found keys of the chunk gone.
const (
	StateCacheMemberRenewed        = "renewed"
	StateCacheMemberDroppedMissing = "dropped_missing"
)

// StateCacheMemberRenewalResults lists them.
var StateCacheMemberRenewalResults = []string{StateCacheMemberRenewed, StateCacheMemberDroppedMissing}

// Why a member its round leaves unwritten was read back rather than served
// (014 next7 3.4), closed: absent, nothing kept; yielded and over_budget, its
// Query Group kept out; expired; activation, its Plan's activation changed.
const (
	StateCacheFallbackAbsent     = "absent"
	StateCacheFallbackYielded    = "yielded"
	StateCacheFallbackOverBudget = "over_budget"
	StateCacheFallbackExpired    = "expired"
	StateCacheFallbackActivation = "activation"
)

// StateCacheFallbackReasons lists them.
var StateCacheFallbackReasons = []string{StateCacheFallbackAbsent, StateCacheFallbackYielded, StateCacheFallbackOverBudget,
	StateCacheFallbackExpired, StateCacheFallbackActivation}

// stateCacheFallbacks is the fallback reason of each lookup word that has one.
var stateCacheFallbacks = map[string]string{StateCacheMissAbsent: StateCacheFallbackAbsent,
	StateCacheMissYielded: StateCacheFallbackYielded, StateCacheMissOverBudget: StateCacheFallbackOverBudget,
	StateCacheMissExpired: StateCacheFallbackExpired, StateCacheMissActivation: StateCacheFallbackActivation}

// The State entries by where their bytes came from, closed: written, what
// this process wrote; readback, what a preflight read back (014 next7 3.1);
// compact, a read-back value kept in its compact form (3.3).
const (
	StateCacheEntryWritten  = "written"
	StateCacheEntryReadback = "readback"
	StateCacheEntryCompact  = "compact"
)

// StateCacheEntryKinds lists them.
var StateCacheEntryKinds = []string{StateCacheEntryWritten, StateCacheEntryReadback, StateCacheEntryCompact}

// Restore causes and refusals (§4.4).
const (
	StateCacheRestoreMissing            = "missing"
	StateCacheRestoreRolledBack         = "rolled_back"
	StateCacheDeclineProgressRolledBack = "progress_rolled_back"
	StateCacheDeclineNewerVersion       = "newer_version"
)

// StateCacheRestoreCauses and StateCacheDeclineReasons list them.
var (
	StateCacheRestoreCauses  = []string{StateCacheRestoreMissing, StateCacheRestoreRolledBack}
	StateCacheDeclineReasons = []string{StateCacheDeclineProgressRolledBack, StateCacheDeclineNewerVersion}
)

// stateCacheEntryOverhead is each entry's fixed heap beyond its key and
// value: the entry struct, two digest strings and the map slots (design
// §4.5.1, from the probe; the calibration test holds the sum to the heap).
const stateCacheEntryOverhead = 400

type stateCacheEntry struct {
	key   string
	group execution.QueryGroupIdentity
	// identity is a State entry's series, which its compact form is kept by
	// (014 next7 3.3): a write or a drop of the key takes either out.
	identity execution.StateKeyIdentity
	raw      []byte
	// digest is the stored bytes' SHA-1 as the write's witness names it:
	// what this process last wrote under the key.
	digest       string
	applyVersion execution.ApplyVersion
	// fence is the owner fence the write ran under (rule 1); epoch and
	// segmentStart the Plan's activation it ran under (rule 6).
	fence        execution.OwnerFence
	epoch        execution.StateApplyEpoch
	segmentStart execution.EvaluationTime
	// slot is the Slot the write was for (rule 5).
	slot execution.EvaluationTime
	// usableUntil is the write time plus its TTL less the replay age (rule
	// 3): past it the store's key may be gone, so the entry is.
	usableUntil time.Time
	// touched is when the entry was last read or written, and interval the
	// time between its last two writes: untouched for two intervals, it
	// leaves.
	touched, written time.Time
	interval         time.Duration
	cost             int64
	// segment is an entry of a Plan's segments, a mark or a page, and
	// segmentInterval its Plan's evaluation interval: a segment entry is
	// untouched past two of those, as the Plan's round reads it every one.
	segment         bool
	segmentInterval time.Duration
	// readback is a State value a preflight read back rather than one this
	// process wrote (014 next7 3.1): its digest is the stored bytes', its
	// applyVersion the stored record's, and its interval its Plan's.
	readback bool
}

type stateCacheGroup struct {
	entries map[string]*stateCacheEntry
	// compact is the group's Plans' compact members (014 next7 3.3), charged
	// to bytes with the entries.
	compact map[compactPlanKey]*compactPlan
	bytes   int64
}

// empty is a group that holds nothing.
func (group *stateCacheGroup) empty() bool {
	return len(group.entries) == 0 && len(group.compact) == 0
}

// stateCacheRefusal is a Query Group kept out of the cache. bound is the
// departure word for what it did not fit, as of its latest Slot: the budget
// (over_budget) or the pool's room (yielded). size is what the group wrote
// for slot, its latest Slot: once a write for a later Slot arrives, size is
// what one whole round of it costs, and the group comes back only when that
// fits.
type stateCacheRefusal struct {
	bound string
	slot  execution.EvaluationTime
	size  int64
}

// missOf is the lookup word for a key of the group while it is kept out.
func (refusal *stateCacheRefusal) missOf() string {
	if refusal.bound == StateCacheDepartYielded {
		return StateCacheMissYielded
	}
	return StateCacheMissOverBudget
}

// StateCacheStats is what the cache's metrics read.
type StateCacheStats struct {
	Bytes, BudgetBytes int64
	QueryGroups        int
	Lookups            map[string]uint64
	Departures         map[string]uint64
	Restored           map[string]uint64
	Declined           map[string]uint64
	// SegmentBytes is the stable segments' entries' share of Bytes, read
	// with it under the cache's lock: the rest are Runtime State values.
	// StateDepartures and SegmentDepartures are each kind's departures,
	// counted apart, and Departures their sum: a reading taken while
	// entries leave never makes one kind's count of the other's.
	SegmentBytes                       int64
	StateDepartures, SegmentDepartures map[string]uint64
	// Entries is the State entries by kind (StateCacheEntryKinds);
	// Readback, MemberRenewals and Fallbacks the counts of
	// StateCacheReadbackResults, StateCacheMemberRenewalResults and
	// StateCacheFallbackReasons.
	Entries                             map[string]int64
	Readback, MemberRenewals, Fallbacks map[string]uint64
	// CompactDeclined is the counts of StateCacheCompactDeclineReasons,
	// Verified those of StateCacheVerifyResults, Sweeps those of
	// StateCacheSweepResults; SweptEntries the entries and compact members
	// the sweeps walked.
	CompactDeclined, Verified, Sweeps map[string]uint64
	SweptEntries                      uint64
}

// stateCache is per ExecutionStore. Nil is a disabled cache: every method is
// safe on it and every lookup misses without counting.
//
// It holds bytes of the retained pool only while no Slot has reserved them:
// it takes in an entry only within its budget and within the pool's room
// (room), and gives Query Groups back whole when a Slot's reservation needs
// the bytes (YieldRetainedBytes), so it never takes a share from detection.
type stateCache struct {
	mu     sync.Mutex
	budget int64
	// room is what no Slot has reserved of the retained pool, read before
	// mu is taken: the pool's owner calls into the cache under its own lock.
	// Unset, the cache is held to its budget alone.
	room atomic.Pointer[func() uint64]
	// held mirrors bytes for the pool's owner, which reads it on every
	// reservation without taking mu.
	held      atomic.Int64
	replayAge time.Duration
	now       func() time.Time
	byKey     map[string]*stateCacheEntry
	groups    map[execution.QueryGroupIdentity]*stateCacheGroup
	// refused names the Query Groups kept out: one whose write did not fit
	// the budget or the pool's room, or one given back for a Slot. It stays
	// out until one whole round of it fits beside the resident ones, judged
	// at the first write of each later Slot, and the resident ones stay in,
	// so a working set larger than the budget is served at budget ÷ working
	// set rather than thrashing as an LRU would on a fixed rotation, and a
	// group larger than the room is not taken in write by write and given
	// back whole each round.
	refused map[execution.QueryGroupIdentity]*stateCacheRefusal
	// segmentNotes is what is remembered of the Query Groups' segments past
	// their entries (segmentNotes).
	segmentNotes map[execution.QueryGroupIdentity]*segmentNotes
	// swept is the Slot each Query Group was last swept for: a Slot's reads
	// - its segments, then its State in calls of at most a batch's keys -
	// sweep its group at the first, not at each (sweep).
	swept                       map[execution.QueryGroupIdentity]execution.SlotIdentity
	bytes                       int64
	lookups, restored, declined map[string]*atomic.Uint64
	// stateDepartures and segmentDepartures count each kind's departures
	// apart (departuresOf); segmentBytes is the segments' entries' share of
	// bytes.
	segmentBytes                       int64
	stateDepartures, segmentDepartures map[string]*atomic.Uint64
	// entries is the State entries held by kind, under mu; readback,
	// memberRenewals and fallbacks count what became of read-back values,
	// of members' entries at a page renewal, and of members read back.
	entries                             map[string]int64
	readback, memberRenewals, fallbacks map[string]*atomic.Uint64
	// compactPlans is every Plan generation's compact members, by Query
	// Group; compactDeclined counts the values kept as their bytes instead.
	compactPlans    map[compactPlanKey]*compactPlan
	compactDeclined map[string]*atomic.Uint64
	// verified counts the sampled round's checks by result.
	verified map[string]*atomic.Uint64
	// sweeps counts the reads that asked for a sweep by
	// StateCacheSweepResults; sweptEntries the entries and compact members
	// the walks went through.
	sweeps       map[string]*atomic.Uint64
	sweptEntries atomic.Uint64
}

func newStateCache(budget int64, replayAge time.Duration, now func() time.Time) *stateCache {
	if budget <= 0 {
		return nil
	}
	counters := func(words []string) map[string]*atomic.Uint64 {
		out := make(map[string]*atomic.Uint64, len(words))
		for _, word := range words {
			out[word] = &atomic.Uint64{}
		}
		return out
	}
	return &stateCache{budget: budget, replayAge: replayAge, now: now,
		byKey: map[string]*stateCacheEntry{}, groups: map[execution.QueryGroupIdentity]*stateCacheGroup{},
		refused: map[execution.QueryGroupIdentity]*stateCacheRefusal{}, lookups: counters(StateCacheLookupResults),
		segmentNotes: map[execution.QueryGroupIdentity]*segmentNotes{}, swept: map[execution.QueryGroupIdentity]execution.SlotIdentity{},
		stateDepartures: counters(StateCacheDepartureReasons), segmentDepartures: counters(StateCacheDepartureReasons),
		restored: counters(StateCacheRestoreCauses), declined: counters(StateCacheDeclineReasons),
		entries: map[string]int64{}, readback: counters(StateCacheReadbackResults),
		memberRenewals: counters(StateCacheMemberRenewalResults), fallbacks: counters(StateCacheFallbackReasons),
		compactPlans: map[compactPlanKey]*compactPlan{}, compactDeclined: counters(StateCacheCompactDeclineReasons),
		verified: counters(StateCacheVerifyResults), sweeps: counters(StateCacheSweepResults)}
}

// entryKind is a State entry's kind; "" for a segment's.
func entryKind(entry *stateCacheEntry) string {
	switch {
	case entry.segment:
		return ""
	case entry.readback:
		return StateCacheEntryReadback
	}
	return StateCacheEntryWritten
}

// sizeClass rounds a length up to the allocator's size class, close enough:
// powers of two below 1 KiB, eighths of the power of two above.
func sizeClass(length int) int64 {
	if length <= 0 {
		return 0
	}
	size := int64(8)
	for size < int64(length) {
		size <<= 1
	}
	if size <= 1024 {
		return size
	}
	step := size / 16
	rounded := (int64(length) + step - 1) / step * step
	return rounded
}

func entryCost(key string, raw []byte) int64 {
	return sizeClass(len(raw)) + sizeClass(len(key)) + stateCacheEntryOverhead
}

// lookup is the cached record for key, when every rule holds for this read
// - its bytes' entry, or its compact member - and the result word either
// way. A compact member is never changed once taken in, but for its life
// and its touch: the caller builds its view outside the lock.
func (cache *stateCache) lookup(key string, request execution.StatePreflightRequest, item execution.StatePreflightItem) (*stateCacheEntry, *compactMember, string) {
	if cache == nil {
		return nil, nil, ""
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	group := request.Contract.Slot.QueryGroup
	result, member := cache.judgeLocked(key, request, item, group)
	cache.lookups[result].Add(1)
	if result != StateCacheHit {
		return nil, nil, result
	}
	if member != nil {
		member.touched = cache.now()
		return nil, member, result
	}
	entry := cache.byKey[key]
	entry.touched = cache.now()
	return entry, nil, result
}

func (cache *stateCache) judgeLocked(key string, request execution.StatePreflightRequest, item execution.StatePreflightItem,
	group execution.QueryGroupIdentity) (string, *compactMember) {
	if item.Sharded {
		// The key carries no shard: two pieces of a split strategy whose
		// selections met would write it under one epoch (§4.3, rule 6).
		return StateCacheMissSharded, nil
	}
	if refusal := cache.refused[group]; refusal != nil {
		return refusal.missOf(), nil
	}
	entry := cache.byKey[key]
	if entry == nil {
		return cache.judgeCompactLocked(request, item, group)
	}
	now := cache.now()
	switch {
	case !now.Before(entry.usableUntil):
		cache.departLocked(entry, StateCacheDepartExpired)
		return StateCacheMissExpired, nil
	case request.OwnerFence.IsZero() || entry.fence != request.OwnerFence:
		return StateCacheMissFence, nil
	case request.Contract.Slot.EvaluationTime <= entry.slot:
		// A replay, retry or older Slot reads the store, as the replay
		// judgement always has (rule 5).
		return StateCacheMissReplay, nil
	case entry.epoch != item.ApplyVersion.StateApplyEpoch || entry.segmentStart != request.Contract.ScheduleSegmentStart:
		return StateCacheMissActivation, nil
	}
	return StateCacheHit, nil
}

// lastWritten is the digest and version the cache holds under key - what
// this process last wrote there, whatever Query Group wrote it, or read back
// of a member under group, compact or not (014 next7 4: the restore judges a
// read-back value as a written one).
func (cache *stateCache) lastWritten(key string, group execution.QueryGroupIdentity, identity execution.StateKeyIdentity) (*stateCacheEntry, bool) {
	if cache == nil {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, found := cache.byKey[key]; found {
		copied := *entry
		return &copied, true
	}
	if plan, member := cache.compactOfLocked(group, identity); member != nil {
		return &stateCacheEntry{key: key, group: group, identity: identity, digest: member.digest(), applyVersion: member.applyVersion,
			fence: plan.fence}, true
	}
	return nil, false
}

// applied records a write that came back APPLIED.
func (cache *stateCache) applied(entry stateCacheEntry) {
	cache.insert(entry, true)
}

// keepReadback takes in a value a preflight read back (014 next7 3.1),
// within the budget and the pool's room as a write's: the caller judged it
// one the store holds until entry.usableUntil, and gave it its Plan's
// interval and the series' identity. Kept compact when it rebuilds to its
// bytes (3.3), as its bytes otherwise, the reason counted.
func (cache *stateCache) keepReadback(entry stateCacheEntry) {
	if cache == nil {
		return
	}
	member, declined := compactOf(entry.identity, entry.raw)
	room := cache.roomNow()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if member == nil {
		cache.compactDeclined[declined].Add(1)
		entry.readback = true
		cache.insertLocked(entry, false, room)
		return
	}
	cache.keepCompactLocked(entry, member, room)
}

// insert takes in an entry; measured has the interval read from the write
// before it under the same key, for an entry that leaves untouched past two.
func (cache *stateCache) insert(entry stateCacheEntry, measured bool) {
	if cache == nil {
		return
	}
	room := cache.roomNow()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.insertLocked(entry, measured, room)
}

// insertLocked is insert under the lock, room read before it was taken.
func (cache *stateCache) insertLocked(entry stateCacheEntry, measured bool, room int64) {
	now := cache.now()
	entry.cost = entryCost(entry.key, entry.raw)
	entry.touched, entry.written = now, now
	if previous := cache.byKey[entry.key]; previous != nil {
		if measured && previous.group == entry.group && previous.written.Before(now) {
			entry.interval = now.Sub(previous.written)
		}
		cache.removeLocked(previous)
	}
	if !entry.segment {
		// Its compact form, kept while its round left it unwritten.
		if plan, member := cache.compactOfLocked(entry.group, entry.identity); member != nil {
			cache.removeCompactLocked(plan, member)
		}
	}
	if !cache.admitLocked(entry.group, entry.slot, entry.cost, entry.segment, min(cache.budget, room)) {
		return
	}
	stored := entry
	stored.raw = append([]byte(nil), entry.raw...)
	group := cache.groupLocked(entry.group)
	group.entries[entry.key] = &stored
	group.bytes += stored.cost
	cache.byKey[entry.key] = &stored
	cache.bytes += stored.cost
	if stored.segment {
		cache.segmentBytes += stored.cost
	}
	if kind := entryKind(&stored); kind != "" {
		cache.entries[kind]++
	}
	cache.held.Store(cache.bytes)
}

// groupLocked is the group's record, made when it holds nothing yet.
func (cache *stateCache) groupLocked(group execution.QueryGroupIdentity) *stateCacheGroup {
	held := cache.groups[group]
	if held == nil {
		held = &stateCacheGroup{entries: map[string]*stateCacheEntry{}, compact: map[compactPlanKey]*compactPlan{}}
		cache.groups[group] = held
	}
	return held
}

// admitLocked is whether the group may take in cost more for slot within
// capacity. A group kept out is measured a round at a time: what it would
// hold for its latest Slot adds to the round, and the first of a later Slot
// closes it, taking the group back only when the whole round fits. A
// resident group that would grow past capacity leaves whole; one not
// resident does not come in. Either way it is read back, and what it would
// hold for this Slot starts the round it is measured by.
func (cache *stateCache) admitLocked(group execution.QueryGroupIdentity, slot execution.EvaluationTime, cost int64, segment bool,
	capacity int64) bool {
	if refusal := cache.refused[group]; refusal != nil {
		switch {
		case slot == refusal.slot:
			refusal.size += cost
			return false
		case slot < refusal.slot:
			return false
		}
		round := refusal.size
		refusal.slot, refusal.size = slot, cost
		if cache.bytes+round > capacity {
			refusal.bound = cache.boundOf(cache.bytes + round)
			return false
		}
		delete(cache.refused, group)
	}
	if cache.bytes+cost <= capacity {
		return true
	}
	bound := cache.boundOf(cache.bytes + cost)
	refusal := &stateCacheRefusal{bound: bound, slot: slot, size: cost}
	if held := cache.groups[group]; held != nil {
		cache.departGroupLocked(held, bound, func(residentSlot execution.EvaluationTime, residentCost int64) {
			if residentSlot == slot {
				refusal.size += residentCost
			}
		})
	} else {
		cache.departuresOf(segment)[bound].Add(1)
	}
	cache.refused[group] = refusal
	return false
}

// departGroupLocked lets every entry and compact member of the group go for
// reason, telling each's Slot and charge to seen first.
func (cache *stateCache) departGroupLocked(held *stateCacheGroup, reason string, seen func(execution.EvaluationTime, int64)) {
	for _, entry := range held.entries {
		if seen != nil {
			seen(entry.slot, entry.cost)
		}
		cache.departLocked(entry, reason)
	}
	for _, plan := range held.compact {
		for _, member := range plan.members {
			if seen != nil {
				seen(member.slot, member.cost)
			}
			cache.departCompactLocked(plan, member, reason)
		}
	}
}

// stateCacheBudgetMargin is the room the budget leaves above what the held
// series were measured to take, in quarters: a quarter more (014 next7 3.4).
const stateCacheBudgetMargin = 5

// resize sets the budget to what the series this process holds take in the
// cache (014 next7 3.4): heldSeries times the charge of a State entry as the
// cache measures it now, a quarter more, beside what the segments hold;
// never less than floor, and floor while nothing is held or measured. The
// budget moves with what is held, as the series do; the pool's room and the
// soft limit (yield) bound it from above as before.
func (cache *stateCache) resize(heldSeries uint64, floor int64) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	budget := floor
	entries := int64(0)
	for _, kind := range StateCacheEntryKinds {
		entries += cache.entries[kind]
	}
	if heldSeries > 0 && entries > 0 {
		perEntry := (cache.bytes - cache.segmentBytes) / entries
		series := int64(min(heldSeries, uint64(math.MaxInt64/max(perEntry*stateCacheBudgetMargin, 1))))
		budget = max(floor, series*perEntry*stateCacheBudgetMargin/4+cache.segmentBytes)
	}
	cache.budget = budget
}

// boundOf is the departure word for what bytes did not fit: past the budget
// it is over_budget, within it only the pool's room was short, yielded.
func (cache *stateCache) boundOf(bytes int64) string {
	if bytes > cache.budget {
		return StateCacheDepartOverBudget
	}
	return StateCacheDepartYielded
}

// roomNow is what the cache may hold of the retained pool now, read before
// mu is taken.
func (cache *stateCache) roomNow() int64 {
	room := cache.room.Load()
	if room == nil {
		return math.MaxInt64
	}
	return int64(min((*room)(), uint64(math.MaxInt64)))
}

// BorrowRetainedWithin has the cache take in no more than room says no Slot
// has reserved of the retained pool.
func (cache *stateCache) BorrowRetainedWithin(room func() uint64) {
	if cache != nil && room != nil {
		cache.room.Store(&room)
	}
}

// BorrowedRetainedBytes is what the cache holds of the retained pool.
func (cache *stateCache) BorrowedRetainedBytes() uint64 {
	if cache == nil {
		return 0
	}
	return uint64(max(cache.held.Load(), 0))
}

// YieldRetainedBytes gives at least need bytes back to the retained pool,
// or all it holds, a Query Group at a time and the largest first, so as few
// as can be are read back. A group given back stays out, as one over the
// budget does, until a whole round of it fits again, measured from what it
// held for its latest Slot.
func (cache *stateCache) YieldRetainedBytes(need uint64) {
	if cache == nil || need == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	groups := make([]execution.QueryGroupIdentity, 0, len(cache.groups))
	for group := range cache.groups {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(left, right int) bool {
		if cache.groups[groups[left]].bytes != cache.groups[groups[right]].bytes {
			return cache.groups[groups[left]].bytes > cache.groups[groups[right]].bytes
		}
		return groups[left] < groups[right]
	})
	freed := uint64(0)
	for _, group := range groups {
		if freed >= need {
			return
		}
		held := cache.groups[group]
		freed += uint64(held.bytes)
		refusal := &stateCacheRefusal{bound: StateCacheDepartYielded}
		cache.departGroupLocked(held, StateCacheDepartYielded, func(slot execution.EvaluationTime, cost int64) {
			switch {
			case slot > refusal.slot:
				refusal.slot, refusal.size = slot, cost
			case slot == refusal.slot:
				refusal.size += cost
			}
		})
		cache.refused[group] = refusal
	}
}

// drop removes key for reason, if it is held.
func (cache *stateCache) drop(key, reason string) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry := cache.byKey[key]; entry != nil {
		cache.departLocked(entry, reason)
	}
}

// dropState removes a State key's entry, or its member's compact form under
// group, for reason, if either is held.
func (cache *stateCache) dropState(key string, group execution.QueryGroupIdentity, identity execution.StateKeyIdentity, reason string) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry := cache.byKey[key]; entry != nil {
		cache.departLocked(entry, reason)
	}
	if plan, member := cache.compactOfLocked(group, identity); member != nil {
		cache.departCompactLocked(plan, member, reason)
	}
}

// DropQueryGroup removes every entry of a Query Group this process stops,
// loses, declines or hands over (rule 1: the fence would refuse them anyway).
func (cache *stateCache) dropQueryGroup(group execution.QueryGroupIdentity) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if held := cache.groups[group]; held != nil {
		cache.departGroupLocked(held, StateCacheDepartReleased, nil)
	}
	delete(cache.refused, group)
	delete(cache.segmentNotes, group)
	delete(cache.swept, group)
}

// sweep removes a Query Group's entries past their life or untouched for
// two of their own intervals: an entry is dropped by an active sweep, not
// left until it is asked for. It walks every entry of the group, so it is
// done once for a Slot, at its first read: a Slot of n series reads its
// State in about n ÷ a batch's keys calls, and a sweep at each walked the
// group that many times under the cache's lock (014 n8 (i) measurement:
// 3.9 s of a 100,000-series Slot's CPU). A lookup judges an entry's life
// itself (judgeLocked), so a Slot's later reads lose nothing by it.
func (cache *stateCache) sweep(slot execution.SlotIdentity) {
	if cache == nil {
		return
	}
	group := slot.QueryGroup
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if last, swept := cache.swept[group]; swept && last == slot {
		cache.sweeps[StateCacheSweepAlreadySwept].Add(1)
		return
	}
	cache.swept[group] = slot
	cache.sweeps[StateCacheSweepWalked].Add(1)
	now := cache.now()
	// A group with no entries left may still hold notes of its segments.
	held := cache.groups[group]
	if held == nil {
		held = &stateCacheGroup{}
	}
	walked := uint64(len(held.entries))
	for _, plan := range held.compact {
		walked += uint64(len(plan.members))
	}
	cache.sweptEntries.Add(walked)
	for _, entry := range held.entries {
		interval := entry.interval
		if entry.segment {
			interval = entry.segmentInterval
		}
		switch {
		case !now.Before(entry.usableUntil):
			cache.departLocked(entry, StateCacheDepartExpired)
		case interval > 0 && now.Sub(entry.touched) > 2*interval:
			cache.departLocked(entry, StateCacheDepartUntouched)
		}
	}
	for _, plan := range held.compact {
		for _, member := range plan.members {
			switch {
			case !now.Before(member.usableUntil):
				cache.departCompactLocked(plan, member, StateCacheDepartExpired)
			case plan.interval > 0 && now.Sub(member.touched) > 2*plan.interval:
				cache.departCompactLocked(plan, member, StateCacheDepartUntouched)
			}
		}
	}
	if notes := cache.segmentNotes[group]; notes != nil {
		for mark, at := range notes.evicted {
			if now.Sub(at) >= segmentNoteLife {
				delete(notes.evicted, mark)
			}
		}
		for mark, invalid := range notes.invalid {
			// Kept while the mark could still be served: a Plan of a long
			// interval keeps its entries for days, each round touching
			// them, and the note is all that sends its next read to Redis.
			// Its memory is bounded by the marks the cache holds.
			if entry := cache.byKey[mark]; (entry == nil || !entry.segment) && now.Sub(invalid.at) >= segmentNoteLife {
				delete(notes.invalid, mark)
			}
		}
	}
}

// departuresOf is the departure counters of an entry's kind: a segment's or
// a Runtime State value's.
func (cache *stateCache) departuresOf(segment bool) map[string]*atomic.Uint64 {
	if segment {
		return cache.segmentDepartures
	}
	return cache.stateDepartures
}

func (cache *stateCache) departLocked(entry *stateCacheEntry, reason string) {
	cache.removeLocked(entry)
	cache.departuresOf(entry.segment)[reason].Add(1)
	if entry.segment && reason != StateCacheDepartReleased && isSegmentMarkKey(entry.key) {
		// Let go for the budget, the room or its life, not by the Plan: its
		// next read is an eviction's, not a cold one's.
		cache.notesLocked(entry.group).evicted[entry.key] = cache.now()
	}
}

func (cache *stateCache) removeLocked(entry *stateCacheEntry) {
	if cache.byKey[entry.key] != entry {
		return
	}
	delete(cache.byKey, entry.key)
	cache.bytes -= entry.cost
	if entry.segment {
		cache.segmentBytes -= entry.cost
	}
	if kind := entryKind(entry); kind != "" {
		cache.entries[kind]--
	}
	cache.held.Store(cache.bytes)
	if group := cache.groups[entry.group]; group != nil {
		delete(group.entries, entry.key)
		group.bytes -= entry.cost
		if group.empty() {
			delete(cache.groups, entry.group)
		}
	}
}

// memberRenewed moves the State entries of the keys a page renewal renewed
// under fence, every one found, to usableUntil (014 next7 3.2): the renewal
// gave each key its life from before it was sent. Set, not raised: the
// renewal's life replaces a longer one a write gave.
func (cache *stateCache) memberRenewed(keys []string, identities []execution.StateKeyIdentity, fence execution.OwnerFence, usableUntil time.Time) {
	if cache == nil || len(keys) == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	renewed := uint64(0)
	for index, key := range keys {
		if entry := cache.byKey[key]; entry != nil && !entry.segment && entry.fence == fence {
			entry.usableUntil = usableUntil
			renewed++
		}
		if index < len(identities) {
			if plan, member := cache.compactOfLocked(fence.QueryGroup, identities[index]); member != nil && plan.fence == fence {
				member.usableUntil = usableUntil
				renewed++
			}
		}
	}
	cache.memberRenewals[StateCacheMemberRenewed].Add(renewed)
}

// memberMissing lets go of the State entries of the keys a page renewal
// found some gone of, under fence: which ones is not known (rule 4).
func (cache *stateCache) memberMissing(keys []string, identities []execution.StateKeyIdentity, fence execution.OwnerFence) {
	if cache == nil || len(keys) == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	dropped := uint64(0)
	for index, key := range keys {
		if entry := cache.byKey[key]; entry != nil && !entry.segment && entry.fence == fence {
			cache.departLocked(entry, StateCacheDepartRenewalMissing)
			dropped++
		}
		if index < len(identities) {
			if plan, member := cache.compactOfLocked(fence.QueryGroup, identities[index]); member != nil && plan.fence == fence {
				cache.departCompactLocked(plan, member, StateCacheDepartRenewalMissing)
				dropped++
			}
		}
	}
	cache.memberRenewals[StateCacheMemberDroppedMissing].Add(dropped)
}

// countReadback counts what became of a value read back.
func (cache *stateCache) countReadback(result string) {
	if cache != nil {
		cache.readback[result].Add(1)
	}
}

// countFallback counts a member its round leaves unwritten read back for
// the lookup word, when the word is one of StateCacheFallbackReasons'.
func (cache *stateCache) countFallback(lookup string) {
	if cache == nil {
		return
	}
	if reason, ok := stateCacheFallbacks[lookup]; ok {
		cache.fallbacks[reason].Add(1)
	}
}

func (cache *stateCache) countRestored(cause string) {
	if cache != nil {
		cache.restored[cause].Add(1)
	}
}

func (cache *stateCache) countDeclined(reason string) {
	if cache != nil {
		cache.declined[reason].Add(1)
	}
}

func (cache *stateCache) stats() StateCacheStats {
	if cache == nil {
		return StateCacheStats{}
	}
	read := func(counters map[string]*atomic.Uint64) map[string]uint64 {
		out := make(map[string]uint64, len(counters))
		for word, counter := range counters {
			out[word] = counter.Load()
		}
		return out
	}
	cache.mu.Lock()
	bytes, segmentBytes, groups, budget := cache.bytes, cache.segmentBytes, len(cache.groups), cache.budget
	entries := make(map[string]int64, len(StateCacheEntryKinds))
	for _, kind := range StateCacheEntryKinds {
		entries[kind] = cache.entries[kind]
	}
	cache.mu.Unlock()
	stats := StateCacheStats{Bytes: bytes, BudgetBytes: budget, QueryGroups: groups,
		Lookups: read(cache.lookups), Restored: read(cache.restored), Declined: read(cache.declined), SegmentBytes: segmentBytes,
		Entries: entries, Readback: read(cache.readback), MemberRenewals: read(cache.memberRenewals), Fallbacks: read(cache.fallbacks),
		CompactDeclined: read(cache.compactDeclined), Verified: read(cache.verified), Sweeps: read(cache.sweeps),
		SweptEntries: cache.sweptEntries.Load()}
	stats.StateDepartures, stats.SegmentDepartures = read(cache.stateDepartures), read(cache.segmentDepartures)
	stats.Departures = make(map[string]uint64, len(stats.StateDepartures))
	for reason, count := range stats.StateDepartures {
		stats.Departures[reason] = count + stats.SegmentDepartures[reason]
	}
	return stats
}

// StateCacheStats reports the cache's sizes and counts; the zero value when
// the cache is off.
func (store *ExecutionStore) StateCacheStats() StateCacheStats { return store.cache.stats() }

// SizeStateCache sets the State cache's budget to what heldSeries take in it
// (014 next7 3.4), never below the budget the store was opened with, and
// gives back what the soft limit is passed by (yield, past the line: the
// live heap after the last collection and what the detection budgets may
// still take over the limit), the largest Query Groups first. Called once a
// minute; a no-op without the cache.
func (store *ExecutionStore) SizeStateCache(heldSeries uint64, pastLimit uint64) {
	if store == nil || store.cache == nil {
		return
	}
	store.cache.resize(heldSeries, store.options.StateCacheBudgetBytes)
	if pastLimit > 0 {
		store.cache.YieldRetainedBytes(pastLimit)
	}
}

// StateCacheBorrower is the cache as the retained pool's borrower; nil when
// the cache is off.
func (store *ExecutionStore) StateCacheBorrower() execution.RetainedBytesBorrower {
	if store == nil || store.cache == nil {
		return nil
	}
	return store.cache
}

// DropStateCacheQueryGroup removes a Query Group's entries when this process
// stops holding it, and what it last did with the group's segments.
func (store *ExecutionStore) DropStateCacheQueryGroup(group execution.QueryGroupIdentity) {
	store.cache.dropQueryGroup(group)
	store.uses.drop(group)
}

// The cache's share in a Plan's segments (014 B3 3.2): the mark and the
// pages kept as entries of the Plan's Query Group, under the budget and the
// pool's room the State entries are kept under, read once a Slot before the
// State. A segment key is the Plan's own generation's, written only by the
// Query Group's owner under its fence: an entry is served while its fence is
// the Slot's and its life lasts, and the replay and activation rules of a
// State key do not apply - the mark's position tells one round from another.

// segmentNotes is what the cache remembers of a Query Group's segments past
// their entries: the marks it let go for its budget, the pool's room or
// their life (evicted), so their next read is told from a cold one; and the
// Plans whose last write is in doubt or found a key gone (invalid), served
// from Redis until read again, with what the write in doubt tried to leave.
type segmentNotes struct {
	evicted map[string]time.Time
	invalid map[string]segmentInvalid
}

type segmentInvalid struct {
	reason string
	// at is when the Plan's cache was let go: a note no read clears - the
	// Plan deleted or moved to a new generation - leaves with the sweep
	// once its mark is out of the cache and past segmentNoteLife.
	at time.Time
	// attempted is, by key, the SHA-1 of the bytes a write in doubt tried
	// to leave there, "" for a removal: a later read finding them is the
	// write landed, not drift.
	attempted map[string]string
}

// segmentNoteLife is how long an eviction is remembered: past it the Plan's
// next read is cold, as it would be from a cache that never held it.
const segmentNoteLife = 6 * time.Hour

func (cache *stateCache) notesLocked(group execution.QueryGroupIdentity) *segmentNotes {
	notes := cache.segmentNotes[group]
	if notes == nil {
		notes = &segmentNotes{evicted: map[string]time.Time{}, invalid: map[string]segmentInvalid{}}
		cache.segmentNotes[group] = notes
	}
	return notes
}

// segmentLookup answers a segment key from the cache: its bytes, nil for a
// mark kept as absent, when an entry is there, its life left and its fence
// the Slot's; and otherwise the lookup word of why not.
func (cache *stateCache) segmentLookup(key string, fence execution.OwnerFence, group execution.QueryGroupIdentity) ([]byte, string) {
	if cache == nil {
		return nil, StateCacheMissAbsent
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if refusal := cache.refused[group]; refusal != nil {
		return nil, refusal.missOf()
	}
	entry := cache.byKey[key]
	switch {
	case entry == nil || !entry.segment:
		return nil, StateCacheMissAbsent
	case !cache.now().Before(entry.usableUntil):
		cache.departLocked(entry, StateCacheDepartExpired)
		return nil, StateCacheMissExpired
	case fence.IsZero() || entry.fence != fence:
		return nil, StateCacheMissFence
	}
	entry.touched = cache.now()
	if len(entry.raw) == 0 {
		return nil, StateCacheHit
	}
	return entry.raw, StateCacheHit
}

// segmentPeek is a segment key's bytes as the cache holds them under fence,
// its life past or not: what a read from Redis is compared with (drift).
func (cache *stateCache) segmentPeek(key string, fence execution.OwnerFence) ([]byte, bool) {
	if cache == nil {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.byKey[key]
	if entry == nil || !entry.segment || entry.fence != fence {
		return nil, false
	}
	if len(entry.raw) == 0 {
		return nil, true
	}
	return entry.raw, true
}

// segmentKept keeps a segment key's bytes, read or written under fence - a
// mark absent from the store kept as no bytes - as a State write's are kept:
// within the budget and the pool's room, the Query Group's entries together,
// leaving untouched for two of the Plan's intervals.
func (cache *stateCache) segmentKept(key string, raw []byte, group execution.QueryGroupIdentity, fence execution.OwnerFence,
	slot execution.EvaluationTime, usableUntil time.Time, interval time.Duration) {
	if cache == nil {
		return
	}
	if raw == nil {
		raw = []byte{}
	}
	cache.applied(stateCacheEntry{key: key, group: group, raw: raw, fence: fence, slot: slot, usableUntil: usableUntil,
		segment: true, segmentInterval: interval})
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if notes := cache.segmentNotes[group]; notes != nil {
		delete(notes.evicted, key)
	}
}

// segmentRenew gives the cached segment keys under fence the life until
// usableUntil, touched now: the keys a mark's write renewed in the store.
func (cache *stateCache) segmentRenew(keys []string, fence execution.OwnerFence, usableUntil time.Time) {
	if cache == nil || len(keys) == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	now := cache.now()
	for _, key := range keys {
		if entry := cache.byKey[key]; entry != nil && entry.segment && entry.fence == fence {
			entry.usableUntil, entry.touched = usableUntil, now
		}
	}
}

// segmentDrop lets a segment key go, as the Plan's own doing.
func (cache *stateCache) segmentDrop(key string) {
	cache.drop(key, StateCacheDepartReleased)
}

// segmentEvicted is whether the cache let the mark go for its budget, the
// pool's room or its life within segmentNoteLife.
func (cache *stateCache) segmentEvicted(group execution.QueryGroupIdentity, mark string) bool {
	if cache == nil {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	notes := cache.segmentNotes[group]
	if notes == nil {
		return false
	}
	at, ok := notes.evicted[mark]
	return ok && cache.now().Sub(at) < segmentNoteLife
}

// segmentInvalidate serves a Plan's segments from Redis until they are read
// again, counted under reason; attempted is what a write in doubt tried to
// leave.
func (cache *stateCache) segmentInvalidate(group execution.QueryGroupIdentity, mark, reason string, attempted map[string]string) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.notesLocked(group).invalid[mark] = segmentInvalid{reason: reason, attempted: attempted, at: cache.now()}
}

// segmentInvalidOf is why a Plan's segments are served from Redis, when they
// are.
func (cache *stateCache) segmentInvalidOf(group execution.QueryGroupIdentity, mark string) (segmentInvalid, bool) {
	if cache == nil {
		return segmentInvalid{}, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	notes := cache.segmentNotes[group]
	if notes == nil {
		return segmentInvalid{}, false
	}
	invalid, ok := notes.invalid[mark]
	return invalid, ok
}

// segmentReread clears a Plan's invalid note: it was read from Redis.
func (cache *stateCache) segmentReread(group execution.QueryGroupIdentity, mark string) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if notes := cache.segmentNotes[group]; notes != nil {
		delete(notes.invalid, mark)
	}
}

// segmentHeld is the marks, pages and bytes the cache holds of the segments.
func (cache *stateCache) segmentHeld() (plans, pages, bytes int64) {
	if cache == nil {
		return 0, 0, 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	bytes = cache.segmentBytes
	for key, entry := range cache.byKey {
		if !entry.segment {
			continue
		}
		if isSegmentMarkKey(key) {
			plans++
		} else {
			pages++
		}
	}
	return plans, pages, bytes
}
