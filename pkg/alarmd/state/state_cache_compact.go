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
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A stable member's read-back value is kept compact (014 next7 3.3): most of
// a framed record is its JSON header, and every member of one Plan's
// generation carries the same Plan, generation and Levels - their static
// fields and detect fingerprints - in it. The cache keeps those once per
// Plan (compactTemplate) and, per member, only what moves: its digest as
// bytes, its revision, versions and times, its points' bytes as stored, and
// its bytes' SHA-1. A member's entry is found by its digest in its Plan's
// table under its Query Group, not by its 150-character key.
//
// Kept compact only when the member rebuilds to the very bytes read back and
// decodes to the very view they decode to, checked as it is taken in; any
// other is kept as its bytes (compact_declined). A lookup builds the view
// from the template and the member without the JSON, and hands it on to be
// finished - extended, classified, witnessed - as bytes read are.

// Why a read-back value was kept as its bytes rather than compact, closed:
// state_cache_compact_declined_total.
const (
	// StateCacheCompactLegacyFrame: the frame of a record carrying stored
	// record ids (the second frame schema).
	StateCacheCompactLegacyFrame = "legacy_frame"
	// StateCacheCompactSeriesGuard: a record carrying a series guard.
	StateCacheCompactSeriesGuard = "series_guard"
	// StateCacheCompactDigestShape: a digest that is not 64 lowercase
	// hexadecimal characters.
	StateCacheCompactDigestShape = "digest_shape"
	// StateCacheCompactRebuiltBytes: the member rebuilt to other bytes than
	// were read back - a header written another way, by another build.
	StateCacheCompactRebuiltBytes = "rebuilt_bytes"
	// StateCacheCompactRebuiltView: the member decoded to another view than
	// the bytes read back.
	StateCacheCompactRebuiltView = "rebuilt_view"
	// StateCacheCompactUnreadable: the bytes did not parse as a frame.
	StateCacheCompactUnreadable = "unreadable"
)

// StateCacheCompactDeclineReasons lists them.
var StateCacheCompactDeclineReasons = []string{StateCacheCompactLegacyFrame, StateCacheCompactSeriesGuard,
	StateCacheCompactDigestShape, StateCacheCompactRebuiltBytes, StateCacheCompactRebuiltView, StateCacheCompactUnreadable}

// compactMemberOverhead is a compact member's fixed heap beyond its points
// and Level times: the member struct, its Slot digest, and its slot in its
// Plan's table (014 next7 3.3). The heap probe read 399 bytes a member of
// one Level and one point; the calibration test holds the sum to the heap.
const compactMemberOverhead = 384

type compactDigest [32]byte

// compactPlanKey names one Plan generation's compact members under one
// Query Group.
type compactPlanKey struct {
	group      execution.QueryGroupIdentity
	plan       execution.PlanIdentity
	generation execution.StateGeneration
}

// compactPlan is one Plan generation's compact members under one Query
// Group, kept under one fence and one activation: a member read under
// another replaces them all, as each would be judged stale (rules 1 and 6).
type compactPlan struct {
	key          compactPlanKey
	fence        execution.OwnerFence
	epoch        execution.StateApplyEpoch
	segmentStart execution.EvaluationTime
	interval     time.Duration
	templates    map[string]*compactTemplate
	members      map[compactDigest]*compactMember
	// slotDigests holds each Slot digest the members' versions name once.
	slotDigests map[execution.SlotIdentityDigest]execution.SlotIdentityDigest
	bytes       int64
}

// compactTemplate is what a Plan generation's members share of their
// header: each Level's static fields and detect fingerprint, in Level
// order. Members whose Levels differ in any of them share another.
type compactTemplate struct {
	signature string
	levels    []packedLevel
	levelIDs  []uint32
	prints    []string
	members   int
}

// compactMember is one member's own part of its record and its entry's
// bookkeeping. Never changed once taken in, but for its life and its touch.
type compactMember struct {
	template       *compactTemplate
	series         compactDigest
	revision       uint64
	applyVersion   execution.ApplyVersion
	mutationDigest compactDigest
	lastEventTime  int64
	// times is each Level's LastProcessedEventTime and LastRealPointTime,
	// in the template's Level order.
	times       []int64
	pointCount  int
	points      []byte
	witness     [20]byte
	slot        execution.EvaluationTime
	usableUntil time.Time
	touched     time.Time
	cost        int64
}

func digestBytes(digest string) (compactDigest, bool) {
	var out compactDigest
	if len(digest) != 64 {
		return out, false
	}
	if _, err := hex.Decode(out[:], []byte(digest)); err != nil || hex.EncodeToString(out[:]) != digest {
		return out, false
	}
	return out, true
}

// digest is the member's stored bytes' digest, as ExpectedValueDigest names
// it.
func (member *compactMember) digest() string {
	return hex.EncodeToString(member.witness[:])
}

// view is the member's record decoded, as decodeRuntimePacked decodes its
// bytes, under identity, the member's own: built anew each time, so a reader
// that changes it changes nothing kept.
func (member *compactMember) view(identity execution.StateKeyIdentity) (execution.RuntimeStateView, error) {
	template := member.template
	levels := make([]execution.RuntimeLevelStateView, len(template.levels))
	for index, level := range template.levels {
		levels[index] = execution.RuntimeLevelStateView{LevelID: level.Mutation.LevelID,
			GapReasonOriginSlot: level.Mutation.GapReasonOriginSlot, LevelStateCompatibility: level.Mutation.LevelStateCompatibility,
			HistoryCompleteness: level.Mutation.HistoryCompleteness, GapReasonCode: level.Mutation.GapReasonCode,
			WarmupRequirementRef: level.Mutation.WarmupRequirementRef, LastProcessedEventTime: member.times[2*index],
			LastRealPointTime: member.times[2*index+1]}
	}
	history, err := decodePackedHistory(member.points, template.levelIDs, template.prints, member.pointCount, identity.SeriesIdentityDigest, nil)
	if err != nil {
		return execution.RuntimeStateView{}, err
	}
	return execution.RuntimeStateView{Identity: identity, BlobRevision: member.revision, PersistedApplyVersion: member.applyVersion,
		PersistedMutationDigest: execution.MutationDigest(hex.EncodeToString(member.mutationDigest[:])),
		LastProcessedEventTime:  member.lastEventTime, Levels: levels, History: history}, nil
}

// frame is the member's record rebuilt under identity, as
// encodeRuntimePackedCounted frames it.
func (member *compactMember) frame(identity execution.StateKeyIdentity) ([]byte, error) {
	template := member.template
	header := packedHeader{Schema: executionStateSchemaV3, Identity: identity, BlobRevision: member.revision,
		ApplyVersion: member.applyVersion, MutationDigest: execution.MutationDigest(hex.EncodeToString(member.mutationDigest[:])),
		LastEventTime: member.lastEventTime, Levels: make([]packedLevel, len(template.levels)), PointCount: member.pointCount}
	for index, level := range template.levels {
		level.Mutation.LastProcessedEventTime, level.Mutation.LastRealPointTime = member.times[2*index], member.times[2*index+1]
		header.Levels[index] = level
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 0, len(headerBytes)+len(member.points)+packedFrameHeaderLen+binary.MaxVarintLen64)
	buffer = append(buffer, packedFrameMagic...)
	buffer = append(buffer, packedFrameSchemaV1, packedFrameCodecNone)
	buffer = appendUvarint(buffer, uint64(len(headerBytes)))
	buffer = append(buffer, headerBytes...)
	return append(buffer, member.points...), nil
}

// compactOf is the member raw holds, compact, when it rebuilds to raw and
// decodes to the view raw decodes to; otherwise the reason it is kept as its
// bytes. Built outside the cache's lock, on a template of its own: taking it
// in shares the Plan's template of the same signature and Slot digest
// (keepCompactLocked).
func compactOf(identity execution.StateKeyIdentity, raw []byte) (*compactMember, string) {
	header, schema, points, err := parsePackedFrame(raw)
	switch {
	case err != nil:
		return nil, StateCacheCompactUnreadable
	case schema != packedFrameSchemaV1 || len(header.LegacyRecordIDs) > 0:
		return nil, StateCacheCompactLegacyFrame
	case header.SeriesGuard != nil:
		return nil, StateCacheCompactSeriesGuard
	}
	series, seriesOK := digestBytes(string(identity.SeriesIdentityDigest))
	mutation, mutationOK := digestBytes(string(header.MutationDigest))
	if !seriesOK || !mutationOK {
		return nil, StateCacheCompactDigestShape
	}
	statics := make([]packedLevel, len(header.Levels))
	times := make([]int64, 2*len(header.Levels))
	for index, level := range header.Levels {
		times[2*index], times[2*index+1] = level.Mutation.LastProcessedEventTime, level.Mutation.LastRealPointTime
		level.Mutation.LastProcessedEventTime, level.Mutation.LastRealPointTime = 0, 0
		statics[index] = level
	}
	signature, err := json.Marshal(statics)
	if err != nil {
		return nil, StateCacheCompactUnreadable
	}
	template := &compactTemplate{signature: string(signature), levels: statics,
		levelIDs: make([]uint32, len(statics)), prints: make([]string, len(statics))}
	for index, level := range statics {
		template.levelIDs[index], template.prints[index] = level.Mutation.LevelID, level.DetectFingerprint
	}
	member := &compactMember{template: template, series: series, revision: header.BlobRevision, applyVersion: header.ApplyVersion,
		mutationDigest: mutation, lastEventTime: header.LastEventTime, times: times, pointCount: header.PointCount,
		points: append([]byte(nil), points...), witness: sha1.Sum(raw)}
	if rebuilt, err := member.frame(identity); err != nil || !bytes.Equal(rebuilt, raw) {
		return nil, StateCacheCompactRebuiltBytes
	}
	decoded, err := decodedForCompare(raw, identity)
	if err != nil {
		return nil, StateCacheCompactUnreadable
	}
	if view, err := member.view(identity); err != nil || !reflect.DeepEqual(view, decoded) {
		return nil, StateCacheCompactRebuiltView
	}
	member.cost = compactMemberOverhead + sizeClass(len(member.points)) + sizeClass(8*len(times))
	return member, ""
}

// decodedForCompare is a framed record's view as its bytes hold it, before
// any segment extends it: what the State cache compares what it keeps with
// - a compact member as it is taken in, a value against the store's in the
// sampled round's check - and hands to no reader. Every view a reader gets
// is decodeRuntimeCovered's.
func decodedForCompare(raw []byte, identity execution.StateKeyIdentity) (execution.RuntimeStateView, error) {
	return decodeRuntimePacked(raw, identity)
}

// compactOfLocked is the series' compact member under group, and its Plan's
// table, when either is held.
func (cache *stateCache) compactOfLocked(group execution.QueryGroupIdentity, identity execution.StateKeyIdentity) (*compactPlan, *compactMember) {
	plan := cache.compactPlans[compactPlanKey{group: group, plan: identity.Plan, generation: identity.StateGeneration}]
	if plan == nil {
		return nil, nil
	}
	series, ok := digestBytes(string(identity.SeriesIdentityDigest))
	if !ok {
		return plan, nil
	}
	return plan, plan.members[series]
}

// judgeCompactLocked is the lookup word for a series with no entry of its
// bytes, by the rules of §4.3 over its compact member, and the member on a
// hit.
func (cache *stateCache) judgeCompactLocked(request execution.StatePreflightRequest, item execution.StatePreflightItem,
	group execution.QueryGroupIdentity) (string, *compactMember) {
	plan, member := cache.compactOfLocked(group, item.Identity)
	if member == nil {
		return StateCacheMissAbsent, nil
	}
	switch {
	case !cache.now().Before(member.usableUntil):
		cache.departCompactLocked(plan, member, StateCacheDepartExpired)
		return StateCacheMissExpired, nil
	case request.OwnerFence.IsZero() || plan.fence != request.OwnerFence:
		return StateCacheMissFence, nil
	case request.Contract.Slot.EvaluationTime <= member.slot:
		return StateCacheMissReplay, nil
	case plan.epoch != item.ApplyVersion.StateApplyEpoch || plan.segmentStart != request.Contract.ScheduleSegmentStart:
		return StateCacheMissActivation, nil
	}
	return StateCacheHit, member
}

// keepCompactLocked takes in a read-back value compact, stamped as entry
// says, within the budget and the pool's room as any entry: what was kept of
// the series before goes, and a Plan table under another fence or
// activation goes whole - no read under this one is served what it holds
// (rules 1 and 6). The member shares its Plan's template of the same
// signature and its Slot digest.
func (cache *stateCache) keepCompactLocked(entry stateCacheEntry, member *compactMember, room int64) {
	member.slot, member.usableUntil, member.touched = entry.slot, entry.usableUntil, cache.now()
	if previous := cache.byKey[entry.key]; previous != nil {
		cache.removeLocked(previous)
	}
	key := compactPlanKey{group: entry.group, plan: entry.identity.Plan, generation: entry.identity.StateGeneration}
	if plan := cache.compactPlans[key]; plan != nil {
		if old := plan.members[member.series]; old != nil {
			cache.removeCompactLocked(plan, old)
		}
		if plan.fence != entry.fence || plan.epoch != entry.epoch || plan.segmentStart != entry.segmentStart {
			for _, stale := range plan.members {
				cache.departCompactLocked(plan, stale, StateCacheDepartReleased)
			}
		}
	}
	if !cache.admitLocked(entry.group, entry.slot, member.cost, false, min(cache.budget, room)) {
		return
	}
	group := cache.groupLocked(entry.group)
	plan := cache.compactPlans[key]
	if plan == nil {
		plan = &compactPlan{key: key, fence: entry.fence, epoch: entry.epoch, segmentStart: entry.segmentStart, interval: entry.interval,
			templates: map[string]*compactTemplate{}, members: map[compactDigest]*compactMember{},
			slotDigests: map[execution.SlotIdentityDigest]execution.SlotIdentityDigest{}}
		cache.compactPlans[key] = plan
		group.compact[key] = plan
	}
	if template := plan.templates[member.template.signature]; template != nil {
		member.template = template
	} else {
		plan.templates[member.template.signature] = member.template
	}
	member.template.members++
	if digest, ok := plan.slotDigests[member.applyVersion.SlotDigest]; ok {
		member.applyVersion.SlotDigest = digest
	} else {
		plan.slotDigests[member.applyVersion.SlotDigest] = member.applyVersion.SlotDigest
	}
	plan.members[member.series] = member
	plan.bytes += member.cost
	group.bytes += member.cost
	cache.bytes += member.cost
	cache.entries[StateCacheEntryCompact]++
	cache.held.Store(cache.bytes)
}

// removeCompactLocked takes a compact member out and gives its charge back,
// its Plan's table and its group with it once they hold nothing.
func (cache *stateCache) removeCompactLocked(plan *compactPlan, member *compactMember) {
	if plan.members[member.series] != member {
		return
	}
	delete(plan.members, member.series)
	plan.bytes -= member.cost
	member.template.members--
	if member.template.members == 0 && plan.templates[member.template.signature] == member.template {
		delete(plan.templates, member.template.signature)
	}
	cache.bytes -= member.cost
	cache.entries[StateCacheEntryCompact]--
	cache.held.Store(cache.bytes)
	group := cache.groups[plan.key.group]
	if len(plan.members) == 0 {
		delete(cache.compactPlans, plan.key)
		if group != nil {
			delete(group.compact, plan.key)
		}
	}
	if group != nil {
		group.bytes -= member.cost
		if group.empty() {
			delete(cache.groups, plan.key.group)
		}
	}
}

// departCompactLocked takes a compact member out for reason, counted as a
// State entry's departure.
func (cache *stateCache) departCompactLocked(plan *compactPlan, member *compactMember, reason string) {
	cache.removeCompactLocked(plan, member)
	cache.stateDepartures[reason].Add(1)
}
