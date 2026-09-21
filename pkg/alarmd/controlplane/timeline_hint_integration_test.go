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
)

// A timeline read that carries the Worker's revision hint is answered from
// the cache when the cached body is at that revision, with no header probe;
// by one read of the body when it is not cached; and by the header path when
// the body turns out to be at another revision - a stale hint costs a probe
// and never returns a Segment from the wrong timeline.
func TestAHintedTimelineReadSkipsTheHeaderUntilTheHintGoesStale(t *testing.T) {
	client := newControlplaneRedis(t)
	repository, err := controlplane.NewRedisCatalogRepository(client, "alarmd:control:hint", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	oldCatalog := validCatalog(t, 80)
	oldSnapshot, _, err := repository.PublishCatalog(ctx, oldCatalog)
	if err != nil {
		t.Fatal(err)
	}
	queryGroup := oldCatalog.QueryGroups[0].Identity
	oldOpen := frozenSchedule(t, oldSnapshot.Publication, oldCatalog.QueryGroups[0], 60, nil)
	oldActivation := activationState(t, 1, oldSnapshot, oldOpen, nil)
	if err := repository.CompareAndSetInitialScheduleActivation(ctx, controlplane.ActivationExpectation{}, oldActivation,
		[]execution.InitialScheduleActivationFact{{Segment: oldOpen.Segment}}); err != nil {
		t.Fatal(err)
	}
	compiler, stateSemantics := runtimePlanCompiler(t)
	runtime, err := controlplane.NewRedisCatalogRuntime(repository, compiler, stateSemantics, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	hinted := controlplane.WithTimelineRevisionHint(ctx, 1)

	// First hinted read: nothing cached, one body read, no header.
	before := repository.ControlReadCacheStats()
	if _, err := runtime.ReadFrozenSchedule(hinted, queryGroup, 120); err != nil {
		t.Fatal(err)
	}
	after := repository.ControlReadCacheStats()
	if after.HintedTimeline.Misses != before.HintedTimeline.Misses+1 || after.HintedTimeline.Hits != before.HintedTimeline.Hits {
		t.Fatalf("first hinted read: hinted %+v -> %+v, want one miss", before.HintedTimeline, after.HintedTimeline)
	}
	if after.Version.Hits+after.Version.Misses+after.Version.Refreshes != before.Version.Hits+before.Version.Misses+before.Version.Refreshes {
		t.Fatalf("a hinted read probed the header: version reads %+v -> %+v", before.Version, after.Version)
	}
	// Second hinted read: the cached body is at the hinted revision, no
	// Redis command at all.
	before = after
	if _, err := runtime.ReadFrozenSchedule(hinted, queryGroup, 120); err != nil {
		t.Fatal(err)
	}
	after = repository.ControlReadCacheStats()
	if after.HintedTimeline.Hits != before.HintedTimeline.Hits+1 || after.HintedTimeline.Misses != before.HintedTimeline.Misses {
		t.Fatalf("second hinted read: hinted %+v -> %+v, want one hit", before.HintedTimeline, after.HintedTimeline)
	}
	if after.Version.Hits+after.Version.Misses+after.Version.Refreshes != before.Version.Hits+before.Version.Misses+before.Version.Refreshes {
		t.Fatalf("a hinted cache hit probed the header: version reads %+v -> %+v", before.Version, after.Version)
	}

	// The timeline moves to revision 2 by a cutover; the Worker still hints
	// 1. The read must not answer with the old Segment: it falls to the
	// header path and returns the timeline as it is.
	newCatalog := catalogWithSchedule(t, oldCatalog, 120, 30)
	newSnapshot, _, err := repository.PublishCatalog(ctx, newCatalog)
	if err != nil {
		t.Fatal(err)
	}
	boundary := execution.EvaluationTime(180)
	oldClosed := frozenSchedule(t, oldSnapshot.Publication, oldCatalog.QueryGroups[0], 60, &boundary)
	newOpen := frozenSchedule(t, newSnapshot.Publication, newCatalog.QueryGroups[0], boundary, nil)
	newActivation := activationState(t, 2, newSnapshot, newOpen, nil)
	if err := repository.CompareAndSetScheduleCutover(ctx, controlplane.ActivationExpectation{
		RecordRevision: oldActivation.RecordRevision, Current: oldActivation.Current,
	}, newActivation, []execution.ScheduleCutoverFact{{OldSegment: oldClosed.Segment, NewSegment: newOpen.Segment}}); err != nil {
		t.Fatal(err)
	}
	before = repository.ControlReadCacheStats()
	loaded, err := runtime.ReadFrozenSchedule(hinted, queryGroup, boundary)
	if err != nil || loaded.Segment.Start != boundary {
		t.Fatalf("stale hint read = (%+v, %v), want the open Segment from the timeline as it is now", loaded.Segment, err)
	}
	after = repository.ControlReadCacheStats()
	if after.HintedTimeline.Refreshes != before.HintedTimeline.Refreshes+1 {
		t.Fatalf("a stale hint was not counted as one: hinted %+v -> %+v", before.HintedTimeline, after.HintedTimeline)
	}
	// With the right hint the moved timeline is served hinted again.
	before = after
	if _, err := runtime.ReadFrozenSchedule(controlplane.WithTimelineRevisionHint(ctx, 2), queryGroup, boundary); err != nil {
		t.Fatal(err)
	}
	after = repository.ControlReadCacheStats()
	if after.HintedTimeline.Hits+after.HintedTimeline.Misses != before.HintedTimeline.Hits+before.HintedTimeline.Misses+1 {
		t.Fatalf("the right hint after a move was not served hinted: %+v -> %+v", before.HintedTimeline, after.HintedTimeline)
	}
}
