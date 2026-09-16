// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// controlGetCountingHook counts GET commands by which control-plane key they
// name, so a test can say how many round trips one Slot cost rather than how
// many the code looks like it makes.
type controlGetCountingHook struct {
	mu     sync.Mutex
	counts map[string]int
}

func newControlGetCountingHook() *controlGetCountingHook {
	return &controlGetCountingHook{counts: map[string]int{}}
}

func (hook *controlGetCountingHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if strings.ToLower(cmd.Name()) != "get" || len(cmd.Args()) < 2 {
		return ctx, nil
	}
	key, ok := cmd.Args()[1].(string)
	if !ok {
		return ctx, nil
	}
	kind := ""
	switch {
	case strings.Contains(key, ":manifest:"):
		kind = "manifest"
	case strings.HasSuffix(key, ":latest_publication"):
		kind = "latest_publication"
	default:
		return ctx, nil
	}
	hook.mu.Lock()
	hook.counts[kind]++
	hook.mu.Unlock()
	return ctx, nil
}

func (*controlGetCountingHook) AfterProcess(context.Context, redis.Cmder) error { return nil }

func (*controlGetCountingHook) BeforeProcessPipeline(ctx context.Context, _ []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (*controlGetCountingHook) AfterProcessPipeline(context.Context, []redis.Cmder) error { return nil }

func (hook *controlGetCountingHook) count(kind string) int {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	return hook.counts[kind]
}

func (hook *controlGetCountingHook) reset() {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	hook.counts = map[string]int{}
}

// Freezing many Slots against one publication reads the manifest once.
//
// This is a bandwidth statement, not a tidiness one. The freshness check runs
// on every frozen Slot; the manifest it reads was 760 KB on the deployment
// that found this, and about 197 Slots a second were being frozen. Read per
// Slot that is 150 MB a second out of a Redis instance shared with the rest of
// the platform, which is what it did: egress went from 25 to 165-175 MB/s from
// the release that added the check, the backend sharing the instance queued
// 220 thousand tasks behind it, and the instance later restarted on memory and
// came back without its keys.
//
// The count is taken at the socket rather than from the cache's own counters,
// because a cache reporting hits is not evidence that anything left the
// process less often.
func TestFreezingManySlotsAgainstOnePublicationReadsTheManifestOnce(t *testing.T) {
	fixture := newCountedFreshnessFixture(t, "freshness-reads")

	const slots = 12
	for round := 0; round < slots; round++ {
		fixture.freeze(t)
	}

	if got := fixture.gets.count("manifest"); got != 1 {
		t.Fatalf("manifest GETs = %d over %d frozen Slots, want 1. A manifest is immutable under its "+
			"revision, so every read after the first fetched bytes this process already had -- and the "+
			"bytes are what took the shared Redis instance down", got, slots)
	}
	if got := fixture.gets.count("latest_publication"); got != 1 {
		t.Fatalf("latest publication GETs = %d over %d frozen Slots, want 1 inside one memo window. The "+
			"pointer changes when a publication happens, minutes apart; reading it per Slot is 197 round "+
			"trips a second to be told the same answer", got, slots)
	}
	if got := fixture.states[controlplane.SegmentContentCurrent]; got != slots {
		t.Fatalf("current = %d over %d Slots, want every one: serving the comparison from this process "+
			"must not change the answer it gives; states=%+v", got, slots, fixture.states)
	}
}

// A Segment left on an older publication is reported stale once the memo is
// re-read, and the Slots behind it cost nothing more.
//
// Two things are pinned. The memo must expire -- an answer kept forever is not
// a memo, and nothing else in this process would ever notice a publication.
// And a fleet that really is stale disagrees on every Slot, which is the one
// case where a re-read on disagreement could restore the per-Slot round trip
// this change removes: the whole burst has to share one.
func TestASegmentLeftOnAnOlderPublicationIsStaleOncePastTheMemo(t *testing.T) {
	fixture := newCountedFreshnessFixture(t, "freshness-reads-republish")
	fixture.freeze(t)
	fixture.gets.reset()

	// The source gains a no-data Plan and the leader publishes it. The Segment
	// is not recut, so it still names the old objects.
	fixture.republish(t, noDataSourceCatalog(t))
	fixture.advance(controlplane.LatestPublicationMemoForTest + time.Second)
	fixture.freeze(t)

	if got := fixture.states[controlplane.SegmentContentStale]; got != 1 {
		t.Fatalf("stale = %d, want 1: the Segment names the objects of a publication that is no longer "+
			"the latest, and serving the comparison from this process must not lose that; states=%+v",
			got, fixture.states)
	}
	if got := fixture.gets.count("manifest"); got != 1 {
		t.Fatalf("manifest GETs = %d, want 1: the new revision's manifest is not in this process yet", got)
	}

	fixture.gets.reset()
	for round := 0; round < 8; round++ {
		fixture.freeze(t)
	}
	if got := fixture.gets.count("latest_publication"); got != 0 {
		t.Fatalf("latest publication GETs = %d over eight more disagreeing Slots inside one window, "+
			"want 0: a fleet that really is stale disagrees on every Slot, and the re-read a "+
			"disagreement pays for is once per reading, not once per Slot", got)
	}
	if got := fixture.gets.count("manifest"); got != 0 {
		t.Fatalf("manifest GETs = %d over eight more stale Slots, want 0", got)
	}
}

// A Segment a cutover has just recut is not reported stale by this process's
// own memo.
//
// This is the direction the memo would get wrong, and it would get it wrong at
// exactly the moment the metric is being read: a publication recuts the
// Segments, the recut Segments name the new objects, and a comparison against
// the pointer this process read a few seconds ago calls every one of them
// stale. A Slot that disagrees re-reads the pointer once for this reason, so
// the recut Segment is compared with the publication it was cut from.
func TestARecutSegmentIsNotCalledStaleByTheMemo(t *testing.T) {
	fixture := newCountedFreshnessFixture(t, "freshness-reads-recut")
	fixture.freeze(t)
	fixture.gets.reset()

	// Publish and activate, which cuts a Segment onto the new content at the
	// next boundary, all inside one memo window.
	fixture.republishAndRecut(t, noDataSourceCatalog(t), 120)
	fixture.freezeAt(t, 120)

	if got := fixture.states[controlplane.SegmentContentStale]; got != 0 {
		t.Fatalf("stale = %d, want 0: this Segment was recut onto the publication that is now the "+
			"latest, so the only thing that could call it stale is the pointer this process memoized "+
			"before the cutover; states=%+v", got, fixture.states)
	}
	if got := fixture.states[controlplane.SegmentContentCurrent]; got != 2 {
		t.Fatalf("current = %d, want both Slots; states=%+v", got, fixture.states)
	}
}

// countedFreshnessFixture is newNoDataHopFixture with the socket counted and
// the memo's clock under the test's control.
type countedFreshnessFixture struct {
	*noDataHopFixture
	gets  *controlGetCountingHook
	clock time.Time
}

func newCountedFreshnessFixture(t *testing.T, prefix string) *countedFreshnessFixture {
	t.Helper()
	hook := newControlGetCountingHook()
	fixture := newNoDataHopFixtureWithHook(t, prefix, validCatalog(t, 80), hook)
	counted := &countedFreshnessFixture{noDataHopFixture: fixture, gets: hook, clock: time.Unix(1_000_000, 0)}
	controlplane.SetFreshnessClockForTest(fixture.repository, func() time.Time { return counted.clock })
	// The publication and the manifest read while the fixture was being built
	// are not what this measures.
	hook.reset()
	return counted
}

func (f *countedFreshnessFixture) advance(by time.Duration) { f.clock = f.clock.Add(by) }

// republishAndRecut is a cutover: the leader publishes new content and the
// activation reconciler cuts every open Segment onto it, which is what
// republish on its own deliberately does not do.
func (f *countedFreshnessFixture) republishAndRecut(
	t *testing.T, catalog controlplane.Catalog, boundary int64,
) {
	t.Helper()
	ctx := context.Background()
	snapshot, _, err := f.repository.PublishCatalog(ctx, catalog)
	if err != nil {
		t.Fatal(err)
	}
	// A Segment is cut at a boundary after the open one, so the reconciler's
	// clock moves first. The memo's clock does not: the point of this test is
	// a cutover inside one memo window.
	*f.now = time.Unix(boundary, 0)
	if _, err := f.reconciler.Ensure(ctx, snapshot.Publication); err != nil {
		t.Fatal(err)
	}
}
