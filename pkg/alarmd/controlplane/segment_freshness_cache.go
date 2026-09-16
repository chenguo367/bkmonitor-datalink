// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The freshness check runs once per frozen Slot, and the two reads it needs
// are the two most expensive things a per-Slot path can ask Redis for: the
// whole catalog manifest, and the publication pointer.
//
// Unbounded, that was measured: about 197 Slots a second on one replica, a
// manifest of 760 KB, and Redis egress on the shared instance went from 25 to
// 165-175 MB a second between a release at 01:25Z and the next day. The
// backend sharing that instance queued 220 thousand tasks behind it, and the
// instance restarted on memory a few hours later, losing keys.
//
// Both reads are bounded here. Nothing about the answer changes: the manifest
// of one publication is immutable, and the pointer is re-read the moment a
// Slot disagrees with the one this process has.

// catalogManifestCacheEntries is how many publications' manifests one process
// keeps decoded.
//
// Two, because a publication cutover leaves the fleet reading two revisions at
// once until every Query Group's Segment has been recut, and a cache of one
// would miss on every Slot of whichever revision it is not holding -- which is
// exactly the window this check exists to observe.
const catalogManifestCacheEntries = 2

// latestPublicationMemoFor bounds how old the publication pointer this check
// compares against may be.
//
// It buys a bounded lag in one direction and nothing in the other, and both
// are worth writing down because the metric is read as a level.
//
// Lagging: a publication this process has not read yet leaves Segments that
// have become stale reading current, for at most this long. The condition
// lasts until somebody recuts those Segments, so the finding is delayed rather
// than lost.
//
// Not lagging: a Segment a cutover has just recut is newer than the memo and
// would read stale -- noise at the one moment the metric is being looked at.
// That direction is answered instead of tolerated: a Slot that disagrees with
// the memo makes this process re-read the pointer once, which either finds the
// publication it had not seen or confirms the disagreement is real. Once per
// memo, so a fleet that really is stale -- where every Slot disagrees -- pays
// one small read per window rather than one per Slot.
const latestPublicationMemoFor = 5 * time.Second

// LatestPublicationMemoForTest is the window, so a test drives it rather than
// restating the number and passing whatever it is set to.
const LatestPublicationMemoForTest = latestPublicationMemoFor

// catalogManifestCache keeps the decoded manifest of recent publications.
//
// There is nothing to invalidate. A SnapshotRevision is derived from the
// content it names, and the write refuses to store different bytes under a
// revision that already exists, so one revision is one manifest for the life
// of the process. Entries are dropped because two are enough, not because
// they go out of date.
type catalogManifestCache struct {
	mu      sync.RWMutex
	entries []catalogManifestEntry
}

type catalogManifestEntry struct {
	revision execution.SnapshotRevision
	manifest CatalogManifest
}

func (cache *catalogManifestCache) lookup(revision execution.SnapshotRevision) (CatalogManifest, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	for _, entry := range cache.entries {
		if entry.revision == revision {
			return entry.manifest, true
		}
	}
	return CatalogManifest{}, false
}

func (cache *catalogManifestCache) store(revision execution.SnapshotRevision, manifest CatalogManifest) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, entry := range cache.entries {
		if entry.revision == revision {
			return
		}
	}
	cache.entries = append([]catalogManifestEntry{{revision: revision, manifest: manifest}}, cache.entries...)
	if len(cache.entries) > catalogManifestCacheEntries {
		cache.entries = cache.entries[:catalogManifestCacheEntries]
	}
}

// latestPublicationMemo is the publication pointer this process last read and
// when it read it.
type latestPublicationMemo struct {
	mu        sync.Mutex
	reference SnapshotPublicationRef
	readAt    time.Time
	known     bool
	// rechecked says a Slot has already made this reading pay for a
	// disagreement. One per reading: without it, a fleet whose Segments really
	// are stale disagrees on every Slot and restores the per-Slot read.
	rechecked bool
}

// freshnessManifest serves the manifest of one publication for the per-Slot
// freshness check, from this process where it can.
//
// The hit and the miss are both counted, on the same series the Worker's other
// object reads use. The hit rate is the whole point of this cache, and a hit
// count with no miss count beside it cannot be told from a cache nobody calls.
func (repository *RedisCatalogRepository) freshnessManifest(
	ctx context.Context, revision execution.SnapshotRevision,
) (CatalogManifest, error) {
	if manifest, ok := repository.manifestCache.lookup(revision); ok {
		repository.observeObjectRead(ctx, "manifest", "hit")
		return manifest, nil
	}
	manifest, err := repository.LoadCatalogManifest(ctx, revision)
	if err != nil {
		repository.observeObjectRead(ctx, "manifest", "missing")
		return CatalogManifest{}, err
	}
	repository.observeObjectRead(ctx, "manifest", "miss")
	repository.manifestCache.store(revision, manifest)
	return manifest, nil
}

// freshnessPublication serves the publication pointer for the per-Slot
// freshness check. It reads Redis only when this process has no answer or its
// answer is older than one window.
func (repository *RedisCatalogRepository) freshnessPublication(
	ctx context.Context,
) (SnapshotPublicationRef, error) {
	memo := &repository.latestPublication
	memo.mu.Lock()
	defer memo.mu.Unlock()
	if memo.known && repository.freshnessNow().Sub(memo.readAt) < latestPublicationMemoFor {
		return memo.reference, nil
	}
	return repository.readLatestPublicationLocked(ctx, memo)
}

// refreshFreshnessPublication is what a Slot that disagrees with the
// publication this process holds asks before the disagreement is reported, and
// it reports whether the pointer turned out to be a different one.
//
// A Segment recut by a cutover is newer than a memo, and reporting it stale
// would be this cache's own doing rather than a finding. One re-read per
// memo tells the two apart, and the once is what keeps a genuinely stale
// fleet -- where every Slot disagrees -- from restoring the per-Slot read.
func (repository *RedisCatalogRepository) refreshFreshnessPublication(ctx context.Context) bool {
	memo := &repository.latestPublication
	memo.mu.Lock()
	defer memo.mu.Unlock()
	if memo.rechecked {
		return false
	}
	previous, had := memo.reference, memo.known
	reference, err := repository.readLatestPublicationLocked(ctx, memo)
	if err != nil {
		return false
	}
	memo.rechecked = true
	return !had || reference != previous
}

func (repository *RedisCatalogRepository) readLatestPublicationLocked(
	ctx context.Context, memo *latestPublicationMemo,
) (SnapshotPublicationRef, error) {
	reference, err := repository.LoadLatestPublication(ctx)
	if err != nil {
		// A failed read leaves no memo, so the next Slot reads again. The
		// alternative -- keeping the last good reference past a failure --
		// would report a comparison against a publication this process can no
		// longer confirm, which is the one answer this check must not give.
		memo.known, memo.rechecked = false, false
		return SnapshotPublicationRef{}, err
	}
	memo.reference, memo.readAt, memo.known, memo.rechecked = reference, repository.freshnessNow(), true, false
	return reference, nil
}

// freshnessNow is the clock the memo ages against. It is separate so a test
// can drive the window rather than sleep through it.
func (repository *RedisCatalogRepository) freshnessNow() time.Time {
	if repository.freshnessClock != nil {
		return repository.freshnessClock()
	}
	return time.Now()
}

// SetFreshnessClockForTest drives the publication memo's window.
func SetFreshnessClockForTest(repository *RedisCatalogRepository, now func() time.Time) {
	repository.freshnessClock = now
}
