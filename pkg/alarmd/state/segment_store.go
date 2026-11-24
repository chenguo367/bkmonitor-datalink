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
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A Plan's stable segments (014 B3) beside its State: the mark, one key, and
// the member pages, one key each, under the Plan's own generation, on the
// target its State is routed to. Their kinds are their own, so no State, gap
// or no-data key shares a name with them; a page's number is its suffix, zero
// padded to the width executionKey keeps, so page 1 and page 10 are two
// names.
const (
	segmentMarkKind = "segment-mark"
	segmentPageKind = "segment-page"
)

// SegmentMarkKeyV1 names a Plan's segment mark. A piece of a split strategy
// has its own, by the rule the gap marker and the no-data memory keep
// (shardedKind): the kind with "-shard" and the piece's matcher digest in
// the suffix slot. A Plan not split keeps the name it always had.
func SegmentMarkKeyV1(prefix string, key segment.PlanKey) (string, error) {
	if err := validatePlanIdentity(prefix, key.Plan, key.Generation); err != nil {
		return "", err
	}
	kind, suffix, err := shardedKind(segmentMarkKind, key.Shard)
	if err != nil {
		return "", err
	}
	return executionKey(prefix, kind, key.Plan, key.Generation, suffix), nil
}

// SegmentPageKeyV1 names a Plan's member page page: the mark's kind and
// suffix slot with "page" for "mark", then the page number, zero padded to
// the width executionKey keeps. A Plan not split leaves the suffix slot
// empty, so its pages keep the names they always had.
func SegmentPageKeyV1(prefix string, key segment.PlanKey, page int) (string, error) {
	if err := validatePlanIdentity(prefix, key.Plan, key.Generation); err != nil {
		return "", err
	}
	if page < 0 {
		return "", identityError("a page number is not negative")
	}
	number := fmt.Sprintf("%032d", page)
	kind, suffix, err := shardedKind(segmentPageKind, key.Shard)
	if err != nil {
		return "", err
	}
	return executionKey(prefix, kind, key.Plan, key.Generation, suffix) + ":" + number, nil
}

// isSegmentMarkKey is whether a key is a mark's, a piece's or not, by its
// kind.
func isSegmentMarkKey(key string) bool {
	return strings.Contains(key, ":"+segmentMarkKind+":") || strings.Contains(key, ":"+segmentMarkKind+"-shard:")
}

// segmentStats are the segments' counts, kept beside the cache so a store
// with the cache off counts its reads too.
type segmentStats struct {
	reads   map[[2]string]*atomic.Uint64
	writes  map[string]*atomic.Uint64
	drift   atomic.Uint64
	extends map[segment.Extension]*atomic.Uint64
}

func newSegmentStats() *segmentStats {
	stats := &segmentStats{reads: map[[2]string]*atomic.Uint64{}, writes: map[string]*atomic.Uint64{},
		extends: map[segment.Extension]*atomic.Uint64{}}
	for _, extension := range segment.Extensions {
		stats.extends[extension] = &atomic.Uint64{}
	}
	stats.reads[[2]string{segment.SourceCache, segment.ReadHit}] = &atomic.Uint64{}
	for _, reason := range segment.ReadReasons {
		stats.reads[[2]string{segment.SourceRedis, reason}] = &atomic.Uint64{}
	}
	for _, result := range segment.WriteResults {
		stats.writes[result] = &atomic.Uint64{}
	}
	return stats
}

// extended counts a member's read by what its extension did; an extension
// outside the closed list is not one segment.Extend gives.
func (stats *segmentStats) extended(extension segment.Extension) {
	if counter := stats.extends[extension]; counter != nil {
		counter.Add(1)
	}
}

// SegmentStats is what the segments' metrics read.
type SegmentStats struct {
	// Reads is a Plan's reads by source and reason, Writes a Plan's writes
	// by result; Drift is the keys a read from Redis under the fence they
	// were cached under found other than the cache held them.
	Reads  map[[2]string]uint64
	Writes map[string]uint64
	Drift  uint64
	// Extends is a member's reads by what their extension did (segment.Extend):
	// extended, normal points made where its segment covers; within, its
	// own points reach the segment's end; and the refusals, read as stored.
	Extends map[segment.Extension]uint64
	// Plans, Pages and Bytes are what the cache holds of the segments: the
	// marks, the pages, and their charge, a share of state_cache_bytes.
	Plans, Pages, Bytes int64
}

// SegmentStats reports the segments' counts and what the cache holds of them.
func (store *ExecutionStore) SegmentStats() SegmentStats {
	stats := SegmentStats{Reads: map[[2]string]uint64{}, Writes: map[string]uint64{}, Drift: store.segments.drift.Load()}
	for cell, counter := range store.segments.reads {
		stats.Reads[cell] = counter.Load()
	}
	for result, counter := range store.segments.writes {
		stats.Writes[result] = counter.Load()
	}
	stats.Extends = map[segment.Extension]uint64{}
	for extension, counter := range store.segments.extends {
		stats.Extends[extension] = counter.Load()
	}
	stats.Plans, stats.Pages, stats.Bytes = store.cache.segmentHeld()
	return stats
}

func segmentDigest(raw []byte) string {
	sum := sha1.Sum(raw)
	return hex.EncodeToString(sum[:])
}

// segmentLife is how long a Plan's segment keys are served from the cache:
// the life its write gives them, less the replay age, as a State entry's
// (rule 3). A key read from Redis may have less left; every round's write
// renews the pages it does not carry and says which it found gone, and
// those let the Plan's cache go (renew_missing), so a cached key is never
// served past the next write's word on it. Zero is not cached.
func (store *ExecutionStore) segmentLife(lifetime segment.Lifetime) (time.Duration, time.Duration, bool) {
	ttl, err := store.runtimeTTL(lifetime.Retention, lifetime.Horizon)
	if err != nil || ttl <= 0 {
		return 0, 0, false
	}
	return ttl, ttl - store.options.StateCacheReplayAge, true
}

// ReadSegments reads the segments of a Slot's Plans: each from the cache
// when its mark and every page it names are there under the Slot's fence,
// the rest from Redis a storage target at a time - every mark in one MGET,
// then every page they name in MGETs cut by the bytes a read carries - so a
// Slot's cold Plans cost two round trips a target, not two a Plan. A key read
// from Redis is compared with what the cache held of it (drift) and kept.
func (store *ExecutionStore) ReadSegments(ctx context.Context, request segment.ReadRequest) (segment.ReadResult, error) {
	result := segment.ReadResult{Plans: make([]segment.Stored, len(request.Plans))}
	group := request.Contract.Slot.QueryGroup
	store.cache.sweep(request.Contract.Slot)
	type cold struct {
		index  int
		mark   string
		reason string
	}
	byTarget := map[string][]cold{}
	backends := map[string]Backend{}
	var order []string
	for index, plan := range request.Plans {
		mark, err := SegmentMarkKeyV1(store.options.Prefix, plan.Key)
		if err != nil {
			return segment.ReadResult{}, err
		}
		target, err := store.options.Router.Route(plan.Key.Plan.TenantID, plan.Key.Plan.StrategyID)
		if err != nil {
			return segment.ReadResult{}, err
		}
		ttl, _, _ := store.segmentLife(plan.Lifetime)
		if stored, reason, hit := store.cachedSegments(plan.Key, mark, request.OwnerFence, group); hit {
			stored.Key, stored.Source, stored.Reason, stored.TTL = plan.Key, segment.SourceCache, segment.ReadHit, ttl
			result.Plans[index] = stored
			continue
		} else {
			if _, seen := byTarget[target.Name]; !seen {
				order = append(order, target.Name)
				backends[target.Name] = target.Backend
			}
			byTarget[target.Name] = append(byTarget[target.Name], cold{index: index, mark: mark, reason: reason})
		}
	}
	for _, name := range order {
		plans, backend := byTarget[name], backends[name]
		markKeys := make([]string, len(plans))
		for position, plan := range plans {
			markKeys[position] = plan.mark
		}
		marks, err := store.segmentMGet(ctx, backend, StateReadSegmentMark, markKeys, nil)
		var pageKeys [][]string
		var flat []string
		var sizes []int
		if err == nil {
			pageKeys = make([][]string, len(plans))
			for position, plan := range plans {
				key := request.Plans[plan.index].Key
				for page, size := range segmentPageSizes(marks[position]) {
					pageKey, keyErr := SegmentPageKeyV1(store.options.Prefix, key, page)
					if keyErr != nil {
						return segment.ReadResult{}, keyErr
					}
					pageKeys[position] = append(pageKeys[position], pageKey)
					flat = append(flat, pageKey)
					sizes = append(sizes, size)
				}
			}
		}
		var pages [][]byte
		if err == nil {
			pages, err = store.segmentMGet(ctx, backend, StateReadSegmentPage, flat, sizes)
		}
		if err != nil {
			// The Slot has no segments at all: every Plan of the request is
			// counted failed, those served from the cache and those of the
			// targets read before alike.
			store.segments.reads[[2]string{segment.SourceRedis, segment.ReadFailed}].Add(uint64(len(request.Plans)))
			store.uses.readFailed(group, request.Plans, store.options.Now())
			return segment.ReadResult{}, err
		}
		next := 0
		for position, plan := range plans {
			ttl, _, _ := store.segmentLife(request.Plans[plan.index].Lifetime)
			stored := segment.Stored{Key: request.Plans[plan.index].Key, Mark: marks[position], Source: segment.SourceRedis,
				Reason: plan.reason, TTL: ttl}
			stored.Pages = pages[next : next+len(pageKeys[position])]
			next += len(pageKeys[position])
			result.Plans[plan.index] = stored
			store.keepRead(request, request.Plans[plan.index], plan.mark, pageKeys[position], stored)
		}
	}
	for _, stored := range result.Plans {
		store.segments.reads[[2]string{stored.Source, stored.Reason}].Add(1)
	}
	store.uses.read(group, request.Plans, result.Plans, store.options.Now())
	return result, nil
}

// segmentReadBytes bounds the bytes one MGET of pages carries, by the sizes
// the marks name: what eight fenced write scripts would. A page past it is
// an MGET of its own.
const segmentReadBytes = 8 * fencedScriptBytes

// segmentReadKeys bounds the keys one MGET carries.
const segmentReadKeys = 1024

// segmentMGet reads keys in MGETs of at most segmentReadKeys keys and, given
// their sizes, segmentReadBytes bytes, each counted under purpose.
func (store *ExecutionStore) segmentMGet(ctx context.Context, backend Backend, purpose string, keys []string, sizes []int) ([][]byte, error) {
	values := make([][]byte, 0, len(keys))
	for start := 0; start < len(keys); {
		end, bytes := start, 0
		for end < len(keys) && end-start < segmentReadKeys {
			size := 0
			if sizes != nil {
				size = sizes[end]
			}
			if end > start && bytes+size > segmentReadBytes {
				break
			}
			bytes += size
			end++
		}
		chunk, err := store.mget(ctx, backend, purpose, keys[start:end])
		if err != nil {
			return nil, err
		}
		if len(chunk) != end-start {
			return nil, fmt.Errorf("state: MGET returned %d values for %d segment keys", len(chunk), end-start)
		}
		values = append(values, chunk...)
		start = end
	}
	return values, nil
}

// segmentPageSizes is the sizes of the pages a mark's bytes name, one a page;
// a mark that does not read names none, and the Plan's book refuses it.
func segmentPageSizes(raw []byte) []int {
	if raw == nil {
		return nil
	}
	mark, err := segment.DecodeMark(raw)
	if err != nil {
		return nil
	}
	return mark.Sizes
}

// segmentPageCount is the pages a mark's bytes name.
func segmentPageCount(raw []byte) int {
	return len(segmentPageSizes(raw))
}

// cachedSegments answers a Plan's segments from the cache when its mark and
// every page the mark names are there under fence; and otherwise says why
// they are read from Redis.
func (store *ExecutionStore) cachedSegments(key segment.PlanKey, mark string, fence execution.OwnerFence,
	group execution.QueryGroupIdentity) (segment.Stored, string, bool) {
	if store.cache == nil {
		return segment.Stored{}, segment.ReadCold, false
	}
	if invalid, ok := store.cache.segmentInvalidOf(group, mark); ok {
		return segment.Stored{}, invalid.reason, false
	}
	why := func(word string, kept bool) string {
		switch word {
		case StateCacheMissFence:
			return segment.ReadFenceChanged
		case StateCacheMissOverBudget, StateCacheMissYielded, StateCacheMissExpired:
			return segment.ReadEvicted
		}
		if kept || store.cache.segmentEvicted(group, mark) {
			return segment.ReadEvicted
		}
		return segment.ReadCold
	}
	raw, word := store.cache.segmentLookup(mark, fence, group)
	if word != StateCacheHit {
		return segment.Stored{}, why(word, false), false
	}
	stored := segment.Stored{Mark: raw}
	for page := 0; page < segmentPageCount(raw); page++ {
		pageKey, err := SegmentPageKeyV1(store.options.Prefix, key, page)
		if err != nil {
			return segment.Stored{}, segment.ReadCold, false
		}
		bytes, word := store.cache.segmentLookup(pageKey, fence, group)
		if word != StateCacheHit {
			// The mark was kept and a page of it was not.
			return segment.Stored{}, why(word, true), false
		}
		stored.Pages = append(stored.Pages, bytes)
	}
	return stored, "", true
}

// keepRead takes a Plan's segments read from Redis: each key against what
// the cache held of it under the Slot's fence (drift) - bytes other than a
// write in doubt tried to leave, or a key gone that no renewal reported
// gone - and then into the cache.
func (store *ExecutionStore) keepRead(request segment.ReadRequest, plan segment.ReadPlan, mark string, pageKeys []string,
	stored segment.Stored) {
	group := request.Contract.Slot.QueryGroup
	keys := append([]string{mark}, pageKeys...)
	values := append([][]byte{stored.Mark}, stored.Pages...)
	invalid, _ := store.cache.segmentInvalidOf(group, mark)
	for index, key := range keys {
		cached, ok := store.cache.segmentPeek(key, request.OwnerFence)
		if !ok {
			continue
		}
		read := values[index]
		switch {
		case read == nil && cached == nil:
		case read == nil:
			if attempted, tried := invalid.attempted[key]; (!tried || attempted != "") && invalid.reason != segment.ReadRenewMissing {
				store.segments.drift.Add(1)
			}
		case !bytes.Equal(read, cached):
			if attempted, tried := invalid.attempted[key]; !tried || attempted != segmentDigest(read) {
				store.segments.drift.Add(1)
			}
		}
	}
	store.cache.segmentReread(group, mark)
	if store.cache == nil || request.OwnerFence.IsZero() {
		return
	}
	_, life, ok := store.segmentLife(plan.Lifetime)
	if !ok || life <= 0 {
		return
	}
	usableUntil := store.options.Now().Add(life)
	for index, key := range keys {
		if values[index] == nil && index > 0 {
			store.cache.segmentDrop(key)
			continue
		}
		store.cache.segmentKept(key, values[index], group, request.OwnerFence, request.Contract.Slot.EvaluationTime,
			usableUntil, plan.Interval)
	}
}

// InvalidateSegments serves a Plan of group's segments from Redis until they
// are read again, counted under reason: unknown_result, renew_missing.
func (store *ExecutionStore) InvalidateSegments(group execution.QueryGroupIdentity, key segment.PlanKey, reason string) {
	mark, err := SegmentMarkKeyV1(store.options.Prefix, key)
	if err == nil {
		store.cache.segmentInvalidate(group, mark, reason, nil)
	}
}

// segmentWriteChunk is one script's worth of a Plan's write: pages to write,
// and, on the last, the mark with the pages kept alive and those removed.
type segmentWriteChunk struct {
	pages  []BackendWrite
	mark   *BackendWrite
	keep   []string
	remove []string
	// departing is the State keys of the members the round closed, renewed
	// to the full lifetime before the mark (segment.PlanWrite.Departed), in
	// chunks of their own.
	departing []string
	bytes     int
}

// segmentPlanWrite is one Plan's write as its target's backend sends it: its
// chunks, in order, with the life they give.
type segmentPlanWrite struct {
	ttl    time.Duration
	chunks []segmentWriteChunk
}

// segmentPlanOutcome is how one Plan's write came back: its result, and the
// pages its mark kept alive that were already gone.
type segmentPlanOutcome struct {
	result  string
	missing int
	// departedGone is the departing members' keys the write found gone.
	departedGone int
}

// SegmentBackend writes the Plans' segments under the owner fence: each
// Plan's chunks in order, the mark on its last, each chunk refused whole by
// a stale fence; the Plans of one call in as few round trips as their bytes
// allow.
type SegmentBackend interface {
	WriteSegments(ctx context.Context, guard *FenceGuard, plans []segmentPlanWrite) []segmentPlanOutcome
}

// WriteSegments writes a Slot's Plans' rounds of their segments, a target at
// a time: each Plan's pages before its mark, so a write cut off between
// leaves the pages the old mark reads as it did (014 B3 3.3 step 6). An
// applied write is kept in the cache; one that came back in doubt or
// refused otherwise than by the fence lets the Plan's cache go
// (unknown_result), and one whose mark found a kept page gone too
// (renew_missing).
func (store *ExecutionStore) WriteSegments(ctx context.Context, request segment.WriteRequest) (segment.WriteResult, error) {
	result := segment.WriteResult{Plans: make([]string, len(request.Plans)), DepartedGone: make([]int, len(request.Plans))}
	defer func() {
		for _, word := range result.Plans {
			store.segments.writes[word].Add(1)
		}
		store.uses.wrote(request.Contract.Slot.QueryGroup, request.Plans, result.Plans, store.options.Now())
	}()
	for index := range result.Plans {
		result.Plans[index] = segment.WriteFailed
	}
	// The fence is checked as a State write's is (ApplyRuntimeFenced): a
	// write without one that holds is refused, not written unfenced.
	if err := request.Fence.Validate(request.Contract); err != nil {
		return result, fmt.Errorf("state: invalid segment write fence: %w", err)
	}
	group := request.Contract.Slot.QueryGroup
	var guard *FenceGuard
	if store.options.FenceKeys != nil {
		guard = &FenceGuard{Keys: store.options.FenceKeys.FenceKeys(request.Fence.Fence.QueryGroup),
			OwnerID: request.Fence.Fence.OwnerID, OwnerEpoch: request.Fence.Fence.OwnerEpoch,
			LeaseToken: request.Fence.Fence.LeaseToken, ContentScope: request.Fence.ContentScope}
	}
	type pending struct {
		index   int
		mark    string
		written map[string][]byte
		kept    []string
		life    time.Duration
	}
	byTarget := map[string][]pending{}
	writes := map[string][]segmentPlanWrite{}
	backends := map[string]SegmentBackend{}
	var order []string
	for index, plan := range request.Plans {
		target, err := store.options.Router.Route(plan.Key.Plan.TenantID, plan.Key.Plan.StrategyID)
		if err != nil {
			continue
		}
		backend, ok := target.Backend.(SegmentBackend)
		if !ok {
			continue
		}
		ttl, life, ok := store.segmentLife(plan.Lifetime)
		if !ok {
			continue
		}
		mark, err := SegmentMarkKeyV1(store.options.Prefix, plan.Key)
		if err != nil {
			continue
		}
		chunks, written, err := store.segmentChunks(plan, mark)
		if err != nil {
			continue
		}
		if _, seen := byTarget[target.Name]; !seen {
			order = append(order, target.Name)
			backends[target.Name] = backend
		}
		byTarget[target.Name] = append(byTarget[target.Name], pending{index: index, mark: mark, written: written,
			kept: chunks[len(chunks)-1].keep, life: life})
		writes[target.Name] = append(writes[target.Name], segmentPlanWrite{ttl: ttl, chunks: chunks})
	}
	now := store.options.Now()
	fence := request.Fence.Fence
	for _, name := range order {
		outcomes := backends[name].WriteSegments(ctx, guard, writes[name])
		for position, plan := range byTarget[name] {
			outcome := outcomes[position]
			result.Plans[plan.index] = outcome.result
			result.DepartedGone[plan.index] = outcome.departedGone
			if outcome.result != segment.WriteApplied {
				// Not applied whole - in doubt, refused otherwise, or refused
				// by the fence at a chunk after earlier ones landed - so what
				// the store holds of it is not known: the Plan's cache goes,
				// with what the write tried to leave, and its next read is
				// from Redis.
				attempted := make(map[string]string, len(plan.written))
				for writtenKey, raw := range plan.written {
					if raw == nil {
						attempted[writtenKey] = ""
						continue
					}
					attempted[writtenKey] = segmentDigest(raw)
				}
				store.cache.segmentInvalidate(group, plan.mark, segment.ReadUnknownResult, attempted)
				continue
			}
			for writtenKey, raw := range plan.written {
				if raw == nil {
					store.cache.segmentDrop(writtenKey)
					continue
				}
				if plan.life <= 0 {
					continue
				}
				store.cache.segmentKept(writtenKey, raw, group, fence, request.Contract.Slot.EvaluationTime,
					now.Add(plan.life), request.Plans[plan.index].Interval)
			}
			if outcome.missing > 0 {
				store.cache.segmentInvalidate(group, plan.mark, segment.ReadRenewMissing, nil)
				continue
			}
			// The pages the mark kept alive were renewed by it, every one
			// found: their cached bytes, this owner's own under this fence,
			// live as long as the keys now do.
			store.cache.segmentRenew(plan.kept, fence, now.Add(plan.life))
		}
	}
	return result, nil
}

// segmentChunks cuts a Plan's write into scripts: its pages by the bytes a
// script carries, a page past the bound alone, the mark on the last with the
// pages it keeps alive and those it removes. written is every key the write
// leaves, nil for one it removes.
func (store *ExecutionStore) segmentChunks(plan segment.PlanWrite, mark string) ([]segmentWriteChunk, map[string][]byte, error) {
	if plan.Mark == nil || plan.PageCount < 1 {
		return nil, nil, errors.New("state: a segment write has a mark and a page")
	}
	written := map[string][]byte{mark: plan.Mark}
	var chunks []segmentWriteChunk
	current := segmentWriteChunk{}
	pageKey := func(page int) (string, error) {
		return SegmentPageKeyV1(store.options.Prefix, plan.Key, page)
	}
	for page := 0; page < plan.PageCount; page++ {
		key, err := pageKey(page)
		if err != nil {
			return nil, nil, err
		}
		raw, changed := plan.Pages[page]
		if !changed {
			current.keep = append(current.keep, key)
			continue
		}
		if raw == nil {
			return nil, nil, fmt.Errorf("state: segment page %d written without bytes", page)
		}
		if len(current.pages) > 0 && current.bytes+len(raw) > fencedScriptBytes {
			chunks = append(chunks, segmentWriteChunk{pages: current.pages, bytes: current.bytes})
			current.pages, current.bytes = nil, 0
		}
		current.pages = append(current.pages, BackendWrite{Key: key, Value: raw})
		current.bytes += len(raw)
		written[key] = raw
	}
	for page := plan.PageCount; page < plan.PriorCount; page++ {
		key, err := pageKey(page)
		if err != nil {
			return nil, nil, err
		}
		current.remove = append(current.remove, key)
		written[key] = nil
	}
	current.mark = &BackendWrite{Key: mark, Value: plan.Mark}
	current.bytes += len(plan.Mark)
	// The departing members' keys in chunks of their own, before the mark's:
	// at most segment.RenewKeys keys and a script's bytes each, as a page
	// renewal is cut. PEXPIRE is idempotent, so one chunk landing and a later
	// one refused leaves nothing wrong; the mark, last, lands only after
	// every one of them.
	departing := segmentWriteChunk{}
	for _, digest := range plan.Departed {
		key, err := RuntimeStateKeyV3(store.options.Prefix, execution.StateKeyIdentity{Plan: plan.Key.Plan,
			StateGeneration: plan.Key.Generation, SeriesIdentityDigest: digest})
		if err != nil {
			return nil, nil, err
		}
		if len(departing.departing) == segment.RenewKeys || (len(departing.departing) > 0 && departing.bytes+len(key) > fencedScriptBytes) {
			chunks = append(chunks, departing)
			departing = segmentWriteChunk{}
		}
		departing.departing = append(departing.departing, key)
		departing.bytes += len(key)
	}
	if len(departing.departing) > 0 {
		chunks = append(chunks, departing)
	}
	return append(chunks, current), written, nil
}

// RenewSegments renews a Slot's due pages (014 B3 3.5), a storage target at
// a time: each Plan's members' State keys - their representation's, the
// framed one every view is read in - cut into scripts of at most
// segment.RenewKeys keys, each under the fence. A page is renewed when every
// script carrying its members answered; any other answer leaves it as it
// was, to be renewed whole next round (rule 3). Keys found gone let the
// Plan's cache go (renew_missing): its pages may name a member Redis no
// longer has.
func (store *ExecutionStore) RenewSegments(ctx context.Context, request segment.RenewRequest) (segment.RenewResult, error) {
	result := segment.RenewResult{Plans: make([]segment.PlanRenewed, len(request.Plans))}
	for index, plan := range request.Plans {
		result.Plans[index].Renewed = make([]bool, len(plan.Pages))
	}
	if err := request.Fence.Validate(request.Contract); err != nil {
		return result, fmt.Errorf("state: invalid segment renewal fence: %w", err)
	}
	var guard *FenceGuard
	if store.options.FenceKeys != nil {
		guard = &FenceGuard{Keys: store.options.FenceKeys.FenceKeys(request.Fence.Fence.QueryGroup),
			OwnerID: request.Fence.Fence.OwnerID, OwnerEpoch: request.Fence.Fence.OwnerEpoch,
			LeaseToken: request.Fence.Fence.LeaseToken, ContentScope: request.Fence.ContentScope}
	}
	// carried is the pages of a Plan one chunk holds members of.
	type carried struct {
		plan  int
		pages []int
	}
	chunks := map[string][]segmentRenewChunk{}
	owners := map[string][]carried{}
	backends := map[string]SegmentRenewBackend{}
	var order []string
	failed := make([]map[int]bool, len(request.Plans))
	for index, plan := range request.Plans {
		failed[index] = map[int]bool{}
		fail := func() {
			for page := range plan.Pages {
				failed[index][page] = true
			}
		}
		target, err := store.options.Router.Route(plan.Key.Plan.TenantID, plan.Key.Plan.StrategyID)
		if err != nil {
			fail()
			continue
		}
		backend, ok := target.Backend.(SegmentRenewBackend)
		ttl, _, lived := store.segmentLife(plan.Lifetime)
		if !ok || !lived {
			fail()
			continue
		}
		if _, seen := backends[target.Name]; !seen {
			order = append(order, target.Name)
			backends[target.Name] = backend
		}
		chunk, owner := segmentRenewChunk{ttl: ttl}, carried{plan: index}
		flush := func() {
			if len(chunk.keys) > 0 {
				chunks[target.Name] = append(chunks[target.Name], chunk)
				owners[target.Name] = append(owners[target.Name], owner)
			}
			chunk, owner = segmentRenewChunk{ttl: ttl}, carried{plan: index}
		}
		for page, renewal := range plan.Pages {
			for _, digest := range renewal.Members {
				identity := execution.StateKeyIdentity{Plan: plan.Key.Plan, StateGeneration: plan.Key.Generation, SeriesIdentityDigest: digest}
				key, err := RuntimeStateKeyV3(store.options.Prefix, identity)
				if err != nil {
					failed[index][page] = true
					continue
				}
				if len(chunk.keys) == segment.RenewKeys {
					flush()
				}
				if len(owner.pages) == 0 || owner.pages[len(owner.pages)-1] != page {
					owner.pages = append(owner.pages, page)
				}
				chunk.keys, chunk.identities = append(chunk.keys, key), append(chunk.identities, identity)
			}
		}
		flush()
	}
	// Read before any script is sent: every key a script renews lives at
	// least this plus its ttl, what the State entries of its members are
	// moved to (014 next7 3.2).
	renewedAt := store.options.Now()
	for _, name := range order {
		outcomes := backends[name].RenewSegments(ctx, guard, chunks[name])
		for position, outcome := range outcomes {
			owner := owners[name][position]
			plan := &result.Plans[owner.plan]
			if outcome.result != segment.WriteApplied {
				// Refused by the fence, or not answered: the entries are left
				// as they are, the fence's rules let go of them.
				for _, page := range owner.pages {
					failed[owner.plan][page] = true
				}
				continue
			}
			if chunk := chunks[name][position]; outcome.missing > 0 {
				store.cache.memberMissing(chunk.keys, chunk.identities, request.Fence.Fence)
			} else {
				store.cache.memberRenewed(chunk.keys, chunk.identities, request.Fence.Fence,
					renewedAt.Add(chunk.ttl-store.options.StateCacheReplayAge))
			}
			plan.Missing += outcome.missing
			plan.Elapsed = append(plan.Elapsed, outcome.elapsed)
			if outcome.elapsed > segment.RenewScriptBudget {
				plan.OverBudget++
			}
		}
	}
	for index, plan := range request.Plans {
		for page := range plan.Pages {
			result.Plans[index].Renewed[page] = !failed[index][page]
		}
		if result.Plans[index].Missing > 0 {
			store.InvalidateSegments(request.Contract.Slot.QueryGroup, plan.Key, segment.ReadRenewMissing)
		}
	}
	return result, nil
}
