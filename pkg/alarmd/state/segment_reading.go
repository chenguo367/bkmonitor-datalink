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
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// segmentUses is what this process last did with each Plan's segments, by
// Query Group: one entry a Plan's piece, its latest generation's, replaced
// when the generation moves. An entry a Slot's read of its Query Group did
// not ask for, untouched for two of its own intervals - a Plan or piece the
// Query Group no longer runs - goes at that read, as an untouched State
// cache entry does; a Query Group's go with its release
// (DropStateCacheQueryGroup). Bounded by the Plans of the Query Groups this
// process holds; kept whether or not the State cache is on.
type segmentUses struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]map[segmentPiece]*segmentUse
}

// segmentPiece is a Plan's piece: the Plan and its shard, zero for a Plan
// not split.
type segmentPiece struct {
	plan  execution.PlanIdentity
	shard execution.ShardRef
}

// segmentUse is one piece's entry: its generation, what was last done with
// its segments, and when and by what interval it was last read or written.
type segmentUse struct {
	generation execution.StateGeneration
	last       segment.LastUse
	touched    time.Time
	interval   time.Duration
}

func newSegmentUses() *segmentUses {
	return &segmentUses{byGroup: map[execution.QueryGroupIdentity]map[segmentPiece]*segmentUse{}}
}

// useLocked is the Plan's entry under its key's generation, made or started
// over when that generation is not the one kept, touched at at by a round of
// interval.
func (uses *segmentUses) useLocked(group execution.QueryGroupIdentity, key segment.PlanKey, interval time.Duration, at time.Time) *segmentUse {
	plans := uses.byGroup[group]
	if plans == nil {
		plans = map[segmentPiece]*segmentUse{}
		uses.byGroup[group] = plans
	}
	piece := segmentPiece{plan: key.Plan, shard: key.Shard}
	use := plans[piece]
	if use == nil || use.generation != key.Generation {
		use = &segmentUse{generation: key.Generation}
		plans[piece] = use
	}
	use.touched, use.interval = at, interval
	return use
}

// read notes a Slot's read of its Plans' segments - asked, each Plan of the
// request, and as it came back, stored, in request order - and lets go of
// the Query Group's entries nothing touched for two of their intervals: the
// Plans read are touched now, so it is the ones the read did not ask for.
func (uses *segmentUses) read(group execution.QueryGroupIdentity, asked []segment.ReadPlan, stored []segment.Stored, at time.Time) {
	uses.mu.Lock()
	defer uses.mu.Unlock()
	for index, plan := range asked {
		use := uses.useLocked(group, plan.Key, plan.Lifetime.Interval, at)
		when := at
		use.last.ReadSource, use.last.ReadReason, use.last.ReadAt = stored[index].Source, stored[index].Reason, &when
	}
	for piece, use := range uses.byGroup[group] {
		if use.interval > 0 && at.Sub(use.touched) > 2*use.interval {
			delete(uses.byGroup[group], piece)
		}
	}
}

// readFailed notes a Slot's read that failed whole, for every Plan it asked.
func (uses *segmentUses) readFailed(group execution.QueryGroupIdentity, plans []segment.ReadPlan, at time.Time) {
	stored := make([]segment.Stored, len(plans))
	for index := range plans {
		stored[index] = segment.Stored{Source: segment.SourceRedis, Reason: segment.ReadFailed}
	}
	uses.read(group, plans, stored, at)
}

// wrote notes a Slot's write of its Plans' rounds, each by its result.
func (uses *segmentUses) wrote(group execution.QueryGroupIdentity, plans []segment.PlanWrite, results []string, at time.Time) {
	uses.mu.Lock()
	defer uses.mu.Unlock()
	for index, plan := range plans {
		use := uses.useLocked(group, plan.Key, plan.Lifetime.Interval, at)
		when := at
		use.last.WriteResult, use.last.WriteAt = results[index], &when
		use.last.SkipMode, use.last.Skipped = plan.SkipMode, plan.Skipped
	}
}

func (uses *segmentUses) drop(group execution.QueryGroupIdentity) {
	uses.mu.Lock()
	defer uses.mu.Unlock()
	delete(uses.byGroup, group)
}

// holds is whether this process holds anything it did with the Query
// Group's segments.
func (uses *segmentUses) holds(group execution.QueryGroupIdentity) bool {
	uses.mu.Lock()
	defer uses.mu.Unlock()
	_, held := uses.byGroup[group]
	return held
}

// lastOf is what this process last did with the Plan's segments under the
// key's generation; nil for nothing.
func (uses *segmentUses) lastOf(group execution.QueryGroupIdentity, key segment.PlanKey) *segment.LastUse {
	uses.mu.Lock()
	defer uses.mu.Unlock()
	use := uses.byGroup[group][segmentPiece{plan: key.Plan, shard: key.Shard}]
	if use == nil || use.generation != key.Generation {
		return nil
	}
	last := use.last
	return &last
}

// PlanSegments reads a Query Group's Plans' segments for a reader of the
// object: each Plan's mark from Redis - one MGET of marks a storage target,
// unfenced, nothing taken from the cache or kept in it, no page read - and
// what this process last did with the Plan's segments. keys are the Plans
// of its open schedule Segment, bounded by the caller, who sets Total to
// the Segment's count. A failed read is the error, nothing else.
func (store *ExecutionStore) PlanSegments(ctx context.Context, group execution.QueryGroupIdentity, keys []segment.PlanKey) (segment.PlansReading, error) {
	reading := segment.PlansReading{QueryGroup: group, Plans: make([]segment.PlanReading, len(keys)), Total: len(keys)}
	byTarget := map[string][]int{}
	backends := map[string]Backend{}
	markKeys := make([]string, len(keys))
	var order []string
	for index, key := range keys {
		mark, err := SegmentMarkKeyV1(store.options.Prefix, key)
		if err != nil {
			return segment.PlansReading{}, err
		}
		target, err := store.options.Router.Route(key.Plan.TenantID, key.Plan.StrategyID)
		if err != nil {
			return segment.PlansReading{}, err
		}
		if _, seen := byTarget[target.Name]; !seen {
			order = append(order, target.Name)
			backends[target.Name] = target.Backend
		}
		byTarget[target.Name] = append(byTarget[target.Name], index)
		markKeys[index] = mark
	}
	for _, name := range order {
		indexes := byTarget[name]
		names := make([]string, len(indexes))
		for position, index := range indexes {
			names[position] = markKeys[index]
		}
		marks, err := segmentMGet(ctx, backends[name], names, nil)
		if err != nil {
			return segment.PlansReading{}, err
		}
		for position, index := range indexes {
			reading.Plans[index] = segment.ReadMark(keys[index], marks[position])
		}
	}
	for index, key := range keys {
		reading.Plans[index].Last = store.uses.lastOf(group, key)
	}
	reading.LastRecorded = store.uses.holds(group)
	return reading, nil
}
