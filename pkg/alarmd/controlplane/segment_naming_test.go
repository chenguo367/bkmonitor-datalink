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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Segment names the object the publication's manifest names.
//
// This is the invariant that was broken, and breaking it cost eleven hours.
// The Segment's digest used to be derived from the group handed to the cutter,
// and that group has been through the object store and back, while the
// manifest's digest was computed from the Catalog the leader built. Two
// derivations of one name. They agree exactly as long as assembly returns what
// was published -- and when assembly dropped a field, every Segment cut from
// an assembled group named an object the manifest did not, the cutover refused
// the mismatch on every later round, and the fleet stopped taking up published
// content while every other signal stayed healthy.
//
// The Segment copies the name now, so a field lost in assembly can no longer
// change one. This test holds the property from the outside: whatever the
// assembly does, the Segment and the manifest agree.
func TestASegmentNamesTheObjectTheManifestNames(t *testing.T) {
	for name, build := range map[string]func(*testing.T) controlplane.Catalog{
		"a catalog whose Plans detect no-data": noDataSourceCatalog,
		"a catalog whose Plans do not":         func(t *testing.T) controlplane.Catalog { return validCatalog(t, 80) },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newNoDataHopFixture(t, "segment-naming-"+shortPrefix(name), build(t))
			ctx := context.Background()

			manifest, err := fixture.repository.LoadCatalogManifest(ctx, publishedRevision(t, fixture))
			if err != nil {
				t.Fatal(err)
			}
			schedule, err := fixture.runtime.ReadFrozenSchedule(ctx, fixture.group, 60)
			if err != nil {
				t.Fatal(err)
			}
			var named string
			for _, entry := range manifest.QueryGroups {
				if entry.QueryGroup == fixture.group {
					named = string(entry.ObjectDigest)
				}
			}
			if named == "" {
				t.Fatalf("the manifest names no object for %s", fixture.group)
			}
			if got := string(schedule.Segment.ObjectDigest); got != named {
				t.Fatalf("the Segment names %s and the manifest names %s. A Segment that names an object "+
					"the publication does not is refused by every later cutover, and the fleet stops "+
					"taking up published content with nothing else reporting a problem", got, named)
			}
		})
	}
}

func publishedRevision(t *testing.T, fixture *noDataHopFixture) execution.SnapshotRevision {
	t.Helper()
	publication, err := fixture.repository.LoadLatestPublication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return publication.SnapshotRevision
}

func shortPrefix(name string) string {
	if len(name) > 12 {
		name = name[:12]
	}
	out := make([]rune, 0, len(name))
	for _, r := range name {
		if r == ' ' {
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

// A Segment naming content neither the activation nor the publication names is
// repaired, not refused forever.
//
// This is production's state, reproduced: an open Segment whose digest is a
// third value, because it was cut from a group that had been through an
// assembly which dropped a field. Refusing it -- what the code did until now --
// leaves the Segment exactly where it is and fails the same way on every later
// round, so the fleet goes on executing content nobody published for as long as
// that lasts. It lasted eleven hours. A refusal that cannot be recovered from
// protects nothing.
func TestASegmentNamingContentNobodyNamesIsRepaired(t *testing.T) {
	fixture := newNoDataHopFixture(t, "segment-repair", validCatalog(t, 80))
	ctx := context.Background()

	// Put the open Segment on a digest neither side names, the way a Segment
	// cut from a lossy assembly was.
	foreign := execution.ObjectDigest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err := controlplane.SetOpenSegmentObjectDigestForTest(ctx, fixture.repository, fixture.group, foreign); err != nil {
		t.Fatal(err)
	}
	// Not read back through the runtime here: that read is cached, and the
	// cutover reads the timeline for update, uncached. The repair decision
	// below is what says the fixture landed.

	decisions := map[string]int{}
	fixture.repository.ConfigureObserver(observability.ObserverFunc(
		func(_ context.Context, observation observability.Observation) {
			if facts := observation.ScheduleCutover; facts != nil {
				for decision, count := range facts.QueryGroups {
					decisions[decision] += count
				}
			}
		}))

	// The second cutover cuts at a later boundary; at the same instant the open
	// Segment already starts at, it is refused for that reason before the
	// digest is ever compared.
	*fixture.now = fixture.now.Add(2 * time.Minute)
	second, _, err := fixture.repository.PublishCatalog(ctx, noDataSourceCatalog(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reconciler.Ensure(ctx, second.Publication); err != nil {
		t.Fatalf("the cutover refused a Segment nobody names: %v. Refusing leaves it there and fails the "+
			"same way next round, which is how a fleet stays on unpublished content indefinitely", err)
	}

	if decisions["repaired_foreign"] == 0 {
		t.Fatalf("decisions = %+v, want the repair counted: it is rare and worth seeing even once it "+
			"heals itself", decisions)
	}
	// And the Segment the cutover left behind names what the publication
	// names, read straight from the timeline rather than through the cache.
	after, err := controlplane.OpenSegmentObjectDigestForTest(ctx, fixture.repository, fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	if after == foreign {
		t.Fatal("the Segment still names the content nobody published")
	}
}

// A Segment already naming what this publication names is adopted, not cut
// again.
//
// That is a previous attempt which wrote its Segments and did not land its
// activation: the Segment is already where the cutover would put it. Cutting
// again would close a Segment onto itself; refusing it -- what the code did --
// would stall on work that is already done. Adoption is the idempotent answer,
// and it is counted separately because it says an earlier attempt half
// completed, which is worth knowing even though it recovers.
func TestASegmentAlreadyNamingThisPublicationIsAdopted(t *testing.T) {
	fixture := newNoDataHopFixture(t, "segment-adopt", validCatalog(t, 80))
	ctx := context.Background()

	// What the next publication will name for this Query Group.
	next := noDataSourceCatalog(t)
	nextDigest, err := controlplane.DeriveQueryGroupObjectDigest(next.QueryGroups[0])
	if err != nil {
		t.Fatal(err)
	}
	// The open Segment is already on it, output contexts and all, as a
	// half-completed cutover leaves it. The contexts matter: with them moved
	// too the cutover takes its "nothing changed" path, and without them its
	// "contexts were revised" path. Both must count the adoption, and they are
	// two separate lines of code.
	nextRefs := make([]execution.OutputContextRef, 0, len(next.QueryGroups[0].Plans))
	for _, plan := range next.QueryGroups[0].Plans {
		contextDigest, err := controlplane.DeriveOutputContextDigest(plan)
		if err != nil {
			t.Fatal(err)
		}
		nextRefs = append(nextRefs, execution.OutputContextRef{Plan: plan.Identity, Digest: contextDigest})
	}
	if err := controlplane.SetOpenSegmentObjectDigestForTest(
		ctx, fixture.repository, fixture.group, nextDigest, nextRefs...); err != nil {
		t.Fatal(err)
	}

	decisions := map[string]int{}
	fixture.repository.ConfigureObserver(observability.ObserverFunc(
		func(_ context.Context, observation observability.Observation) {
			if facts := observation.ScheduleCutover; facts != nil {
				for decision, count := range facts.QueryGroups {
					decisions[decision] += count
				}
			}
		}))

	*fixture.now = fixture.now.Add(2 * time.Minute)
	published, _, err := fixture.repository.PublishCatalog(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.reconciler.Ensure(ctx, published.Publication); err != nil {
		t.Fatalf("the cutover refused a Segment that already names what it was about to cut to: %v", err)
	}

	if decisions["adopted_current"] == 0 {
		t.Fatalf("decisions = %+v, want the adoption counted; it says an earlier attempt wrote its "+
			"Segments and did not land its activation", decisions)
	}
	if decisions["repaired_foreign"] != 0 {
		t.Fatalf("decisions = %+v, want no repair: the Segment names exactly what was published, which "+
			"is the one case that needs nothing done to it", decisions)
	}
	// And it is not filed as an ordinary keep or revise. Whether the output
	// contexts also moved decides which of those two the Segment would
	// otherwise be counted as, and neither of them says an earlier attempt
	// half completed -- which is the only thing this decision is for.
	if decisions["kept"] != 0 || decisions["revised"] != 0 {
		t.Fatalf("decisions = %+v, want the adoption counted as itself rather than as the steady state "+
			"nobody reads", decisions)
	}
	after, err := controlplane.OpenSegmentObjectDigestForTest(ctx, fixture.repository, fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	if after != nextDigest {
		t.Fatalf("the adopted Segment now names %s, want the %s it already named", after, nextDigest)
	}
}
