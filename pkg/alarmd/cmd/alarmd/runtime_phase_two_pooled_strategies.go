// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

// frozenScheduleReader is the catalog a Runner reads its frozen schedules
// from (productionPhaseTwoSlotCatalog), as the pooled rows' strategies are
// read from it.
type frozenScheduleReader interface {
	ReadFrozenSchedule(context.Context, execution.QueryGroupIdentity, execution.EvaluationTime) (execution.FrozenQueryGroupSchedule, error)
}

const (
	// pooledStrategiesBudget bounds the frozen schedules one publish reads,
	// as a publish's Progress restore is bounded: a restart's whole pool is
	// read over a few publishes, not in one burst at the catalog.
	pooledStrategiesBudget = fleetRestoreBudgetPerPublish
	// pooledStrategiesBound is how long one publish waits for the schedules
	// it reads; a read past it leaves its row unread this publish, and
	// pooledStrategiesReadTimeout how long that read may still run for a
	// later publish to take.
	pooledStrategiesBound       = 2 * time.Second
	pooledStrategiesReadTimeout = 30 * time.Second
)

// pooledStrategies names the strategies of a pooled row no round named one
// of - a Query Group restored into the degraded pool runs none until it is
// probed - from the frozen schedule the pool holds it under: the Segment at
// the pool's segment start, taken only when it is of the pool's schedule
// revision. Each schedule is read once, kept while a row asks for it, and a
// read is bounded by the publish it was asked in, so a stalled catalog
// leaves rows unread and never holds the snapshot.
type pooledStrategies struct {
	catalog            frozenScheduleReader
	budget             int
	bound, readTimeout time.Duration
	reads              map[pooledStrategiesKey]*pooledStrategiesRead
}

type pooledStrategiesKey struct {
	queryGroup   execution.QueryGroupIdentity
	revision     execution.ScheduleRevision
	segmentStart execution.EvaluationTime
}

// pooledStrategiesRead is one schedule's read: done once it came back, with
// its Plans, or the reason they cannot be named.
type pooledStrategiesRead struct {
	done   chan struct{}
	plans  []execution.FrozenPlanSchedule
	reason string
}

func newPooledStrategies(catalog frozenScheduleReader) *pooledStrategies {
	if catalog == nil {
		return nil
	}
	return &pooledStrategies{catalog: catalog, budget: pooledStrategiesBudget, bound: pooledStrategiesBound,
		readTimeout: pooledStrategiesReadTimeout, reads: map[pooledStrategiesKey]*pooledStrategiesRead{}}
}

// apply names the strategies of the pooled rows that name none, or why it
// could not; every other row is left as the tracker made it. A schedule no
// row asked for is let go.
func (fill *pooledStrategies) apply(ctx context.Context, rows []fleet.Anomaly) {
	if fill == nil {
		return
	}
	bound, cancel := context.WithTimeout(ctx, fill.bound)
	defer cancel()
	asked := map[pooledStrategiesKey]struct{}{}
	started := 0
	for index := range rows {
		row := &rows[index]
		pool := row.QueryCooldown
		if pool == nil || len(row.Strategies) != 0 {
			continue
		}
		if pool.ScheduleRevision == "" || pool.SegmentStart == 0 {
			fleet.MarkStrategiesUnfilled(row, fleet.StrategiesFromScheduleRevisionUnknown)
			continue
		}
		key := pooledStrategiesKey{queryGroup: execution.QueryGroupIdentity(row.QueryGroup),
			revision: execution.ScheduleRevision(pool.ScheduleRevision), segmentStart: execution.EvaluationTime(pool.SegmentStart)}
		asked[key] = struct{}{}
		read := fill.reads[key]
		if read == nil {
			if started >= fill.budget || bound.Err() != nil {
				fleet.MarkStrategiesUnfilled(row, fleet.StrategiesFromSchedulePending)
				continue
			}
			read = fill.start(ctx, key)
			fill.reads[key] = read
			started++
		}
		// A read already back is taken whatever is left of the bound.
		select {
		case <-read.done:
		default:
			select {
			case <-read.done:
			case <-bound.Done():
				fleet.MarkStrategiesUnfilled(row, fleet.StrategiesFromScheduleUnread)
				continue
			}
		}
		switch read.reason {
		case fleet.StrategiesFromSchedule:
			fleet.FillScheduleStrategies(row, read.plans)
		case fleet.StrategiesFromScheduleUnread:
			// Read again on a later publish: a failed read says nothing of
			// the schedule.
			delete(fill.reads, key)
			fleet.MarkStrategiesUnfilled(row, read.reason)
		default:
			fleet.MarkStrategiesUnfilled(row, read.reason)
		}
	}
	for key := range fill.reads {
		if _, kept := asked[key]; !kept {
			delete(fill.reads, key)
		}
	}
}

// start reads one schedule on its own, under its own deadline: a read the
// publish stops waiting for still comes back for a later one, and one read
// of a schedule runs at a time.
func (fill *pooledStrategies) start(ctx context.Context, key pooledStrategiesKey) *pooledStrategiesRead {
	read := &pooledStrategiesRead{done: make(chan struct{})}
	go func() {
		defer close(read.done)
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fill.readTimeout)
		defer cancel()
		schedule, err := fill.catalog.ReadFrozenSchedule(readCtx, key.queryGroup, key.segmentStart)
		switch {
		case err != nil:
			read.reason = fleet.StrategiesFromScheduleUnread
		case schedule.Segment.ScheduleRevision != key.revision || schedule.Segment.Start != key.segmentStart:
			read.reason = fleet.StrategiesFromScheduleRevisionMoved
		default:
			read.plans, read.reason = schedule.Plans, fleet.StrategiesFromSchedule
		}
	}()
	return read
}
