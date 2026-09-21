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
	"errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A timeline revision hint is the Worker's proof, for one Query Group, that
// the timeline it wants is at a known revision: the installed executable
// view previews the number and the lease's renewal brought the same number
// back from the Assignment record (decision-016 batch 4, judgement 3). With
// it, a timeline read is answered from the cache when the cached body is at
// that revision and by one read of the body otherwise - never by the
// activation header, which is the poll this replaces. A body that comes back
// at another revision is not the timeline the hint was about: the read
// falls back to the header path, so a stale hint costs a probe and never a
// wrong Segment.
type timelineRevisionHintKey struct{}

// WithTimelineRevisionHint carries the hint for the Query Group the caller
// is about to read. Zero is no hint.
func WithTimelineRevisionHint(ctx context.Context, revision uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if revision == 0 {
		return ctx
	}
	return context.WithValue(ctx, timelineRevisionHintKey{}, revision)
}

func timelineRevisionHint(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	revision, _ := ctx.Value(timelineRevisionHintKey{}).(uint64)
	return revision
}

// loadScheduleTimelineAtRevision answers a hinted read: the cached body when
// it is at revision, else one read of the body. The body read is stored
// under whatever version the cache is on, because the hint is its own
// freshness proof. A body at another revision is returned with
// errTimelineRevisionMoved so the caller can take the header path.
func (repository *RedisCatalogRepository) loadScheduleTimelineAtRevision(
	ctx context.Context,
	queryGroup execution.QueryGroupIdentity,
	revision uint64,
) (persistedScheduleTimeline, error) {
	counters := &repository.controlReads.hinted
	if timeline, ok := repository.controlCache.lookupTimelineAtRevision(queryGroup, revision); ok {
		counters.hits.Add(1)
		return timeline, nil
	}
	timeline, payload, err := repository.readScheduleTimeline(ctx, queryGroup)
	if err != nil {
		return persistedScheduleTimeline{}, err
	}
	if timeline.RecordRevision != revision {
		counters.refreshes.Add(1)
		return persistedScheduleTimeline{}, errTimelineRevisionMoved
	}
	counters.misses.Add(1)
	repository.controlCache.storeTimelineAtCurrentVersion(queryGroup, timeline, len(payload))
	return timeline, nil
}

var errTimelineRevisionMoved = errors.New("alarmd controlplane: the timeline is not at the hinted revision")

// TimelineRevisionHint reads the hint a context carries, zero for none. For
// the callers that build the context and want to see what they built.
func TimelineRevisionHint(ctx context.Context) uint64 {
	return timelineRevisionHint(ctx)
}
