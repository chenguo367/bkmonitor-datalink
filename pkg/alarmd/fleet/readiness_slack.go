// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"cmp"
	"slices"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// MaxReadinessSlack is the most objects one snapshot, and the deployment's
// health, name by their readiness slack.
const MaxReadinessSlack = 32

// ReadinessSlackBounds are the upper bounds of the histogram an object's
// slack is kept in, slot_readiness_slack_seconds' own: the p99 read from it
// is the bound of the bucket it falls in, an approximation.
var ReadinessSlackBounds = []float64{0.1, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300}

// readinessBucketSeconds and readinessBuckets are the window an object's
// slack is kept over: twelve five-minute buckets, an hour whatever its
// period, its memory set by the buckets alone.
const (
	readinessBucketSeconds = 300
	readinessBuckets       = 12
)

// readinessBucket is one five-minute bucket of an object's normal Slots:
// how many arrived, how many with slack over 0.5 s and 2 s, the largest
// slack and its histogram (ReadinessSlackBounds, the last cell past them),
// how many arrived under no unified readiness moment and how many under
// none at all, the longest Slot's wall time, and the time and times its
// dispatches waited for an execution slot.
type readinessBucket struct {
	start                int64
	slots, over05, over2 uint32
	notUnified, none     uint32
	slackMax             float64
	histogram            [12]uint32
	wallMax              time.Duration
	waitSeconds          float64
	waits                uint32
}

// readinessWindow is an object's buckets, a ring indexed by the bucket's
// start, and what its period reads as: the interval its queries were
// budgeted by, else the least distance between two Slots it read.
type readinessWindow struct {
	buckets       [readinessBuckets]readinessBucket
	interval      int64
	lastSlot      int64
	leastDistance int64
}

func (window *readinessWindow) bucket(at time.Time) *readinessBucket {
	start := at.Unix() / readinessBucketSeconds * readinessBucketSeconds
	b := &window.buckets[(start/readinessBucketSeconds)%readinessBuckets]
	if b.start != start {
		*b = readinessBucket{start: start}
	}
	return b
}

// noteReadiness keeps what an object's normal Slots say of their timing:
// the slack each read its data with, the wall time each took, and what its
// dispatch waited for an execution slot, bucketed by when they were seen;
// and its period, from the interval its queries were budgeted by. Under the
// tracker's lock.
func (tracker *Tracker) noteReadiness(queryGroup string, slot int64, observation observability.Observation) {
	state := tracker.groups[queryGroup]
	if state == nil {
		if len(tracker.groups) >= tracker.maxTracked {
			tracker.noteDropped(TrackerDroppedObservation, queryGroup)
			return
		}
		state = &queryGroupState{strategies: map[StrategyRef]struct{}{}}
		tracker.groups[queryGroup] = state
	}
	if state.readiness == nil {
		state.readiness = &readinessWindow{}
	}
	window := state.readiness
	now := tracker.now()
	switch observation.Stage {
	case observability.StageQueryBudgetResolved:
		if timing := observation.QueryTiming; timing != nil && timing.IntervalSeconds > 0 {
			window.interval = timing.IntervalSeconds
		}
	case observability.StageSlotReadinessArrival:
		facts := observation.SlotReadiness
		if facts == nil || observation.Operation != observability.OperationNormal {
			return
		}
		if slot > window.lastSlot {
			if window.lastSlot > 0 && (window.leastDistance == 0 || slot-window.lastSlot < window.leastDistance) {
				window.leastDistance = slot - window.lastSlot
			}
			window.lastSlot = slot
		}
		b := window.bucket(now)
		b.slots++
		if facts.Boundary != observability.ReadinessBoundaryUnified {
			b.notUnified++
		}
		if facts.Boundary == observability.ReadinessBoundaryNone {
			b.none++
		}
		if !facts.Slack {
			return
		}
		slack := facts.SlackSeconds
		b.slackMax = max(b.slackMax, slack)
		if slack > 0.5 {
			b.over05++
		}
		if slack > 2 {
			b.over2++
		}
		cell, _ := slices.BinarySearch(ReadinessSlackBounds, slack)
		b.histogram[cell]++
	case observability.StageSlotStarted:
		if wait := observation.ExecutionSlotWait; wait != nil && observation.Operation == observability.OperationNormal && wait.Waited() {
			b := window.bucket(now)
			b.waits++
			b.waitSeconds += wait.WaitMillis / 1000
		}
	case observability.StageSlotCompleted:
		if observation.Operation == observability.OperationNormal && observation.Duration > 0 {
			b := window.bucket(now)
			b.wallMax = max(b.wallMax, observation.Duration)
		}
	}
}

// ReadinessSlackFacts is one object's normal Slots over the last hour, or
// since WindowStart when this process has run less: how many read their
// data, how many with slack past 0.5 s and 2 s, the largest slack and its
// p99 (the bound of the histogram bucket it falls in, an approximation,
// never past the largest), the readiness boundary they read under (unified
// when every one had one moment, none when none waited on data, mixed
// otherwise), its period and the series its Plans bound, the longest
// Slot's wall time, and what its dispatches waited for an execution slot.
// Replica is the replica that holds it, set on the deployment's list.
type ReadinessSlackFacts struct {
	QueryGroup      string     `json:"query_group"`
	PeriodSeconds   *int64     `json:"period_seconds"`
	Series          uint64     `json:"series"`
	Slots           uint32     `json:"slots"`
	SlotsOverHalf   uint32     `json:"slots_over_0_5s"`
	SlotsOver2      uint32     `json:"slots_over_2s"`
	SlackMaxSeconds float64    `json:"slack_max_seconds"`
	SlackP99Seconds float64    `json:"slack_p99_seconds"`
	Boundary        string     `json:"boundary"`
	WallMaxSeconds  float64    `json:"wall_max_seconds"`
	SlotWaitSeconds float64    `json:"slot_wait_seconds"`
	SlotWaits       uint32     `json:"slot_waits"`
	WindowStart     *time.Time `json:"window_start,omitempty"`
	Replica         string     `json:"replica,omitempty"`
}

// ReadinessSlack is the held objects' normal Slots over the last hour - or
// since startedAt, when the process has run less, which each row then says -
// the most Slots with slack past 2 s first, then the largest slack, then by
// name, at most MaxReadinessSlack: which objects the slack past 2 s comes
// from. An object with no normal Slot read in the window has no row. series
// counts the Plans seen within within, as SeriesMatchedIn does.
func (tracker *Tracker) ReadinessSlack(held []string, startedAt time.Time, within time.Duration) []ReadinessSlackFacts {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	now := tracker.now()
	from := now.Unix()/readinessBucketSeconds*readinessBucketSeconds - (readinessBuckets-1)*readinessBucketSeconds
	var windowStart *time.Time
	if !startedAt.IsZero() && startedAt.After(now.Add(-time.Hour)) {
		started := startedAt
		windowStart = &started
	}
	since := now.Add(-within)
	var rows []ReadinessSlackFacts
	for _, queryGroup := range held {
		state := tracker.groups[queryGroup]
		if state == nil || state.readiness == nil {
			continue
		}
		window := state.readiness
		var merged readinessBucket
		for _, b := range window.buckets {
			if b.start < from || b.slots == 0 && b.waits == 0 && b.wallMax == 0 {
				continue
			}
			merged.slots += b.slots
			merged.over05 += b.over05
			merged.over2 += b.over2
			merged.notUnified += b.notUnified
			merged.none += b.none
			merged.slackMax = max(merged.slackMax, b.slackMax)
			merged.wallMax = max(merged.wallMax, b.wallMax)
			merged.waitSeconds += b.waitSeconds
			merged.waits += b.waits
			for cell, count := range b.histogram {
				merged.histogram[cell] += count
			}
		}
		if merged.slots == 0 {
			continue
		}
		row := ReadinessSlackFacts{QueryGroup: queryGroup, Series: tracker.seriesMatchedLocked(queryGroup, since),
			Slots: merged.slots, SlotsOverHalf: merged.over05, SlotsOver2: merged.over2, SlackMaxSeconds: merged.slackMax,
			SlackP99Seconds: histogramP99(merged.histogram, merged.slackMax), WallMaxSeconds: merged.wallMax.Seconds(),
			SlotWaitSeconds: merged.waitSeconds, SlotWaits: merged.waits, WindowStart: windowStart,
			Boundary: observability.ReadinessBoundaryMixed}
		switch {
		case merged.notUnified == 0:
			row.Boundary = observability.ReadinessBoundaryUnified
		case merged.none == merged.slots:
			row.Boundary = observability.ReadinessBoundaryNone
		}
		if period := cmp.Or(window.interval, window.leastDistance); period > 0 {
			row.PeriodSeconds = &period
		}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, mostSlackFirst)
	return rows[:min(len(rows), MaxReadinessSlack)]
}

// histogramP99 is the bound of the bucket the 99th percentile falls in -
// the nearest rank, ceil(0.99 n) - never past largest, which also stands
// for the cell past the last bound; zero with no slack measured.
func histogramP99(histogram [12]uint32, largest float64) float64 {
	total := uint32(0)
	for _, count := range histogram {
		total += count
	}
	if total == 0 {
		return 0
	}
	rank := (uint64(total)*99 + 99) / 100
	seen := uint64(0)
	for cell, count := range histogram {
		seen += uint64(count)
		if seen >= rank {
			if cell < len(ReadinessSlackBounds) {
				return min(ReadinessSlackBounds[cell], largest)
			}
			return largest
		}
	}
	return largest
}

// mostSlackFirst orders objects by their Slots with slack past 2 s, the most
// first, then by their largest slack, then by replica and name.
func mostSlackFirst(a, b ReadinessSlackFacts) int {
	return cmp.Or(cmp.Compare(b.SlotsOver2, a.SlotsOver2), cmp.Compare(b.SlackMaxSeconds, a.SlackMaxSeconds),
		cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.QueryGroup, b.QueryGroup))
}

// aggregateReadinessSlack puts the replicas' objects together, each naming
// its replica, ordered and bounded as each replica's: one read of the
// deployment names the objects the slack past 2 s comes from.
func aggregateReadinessSlack(view *View, snapshots []Snapshot) {
	var all []ReadinessSlackFacts
	for _, snapshot := range snapshots {
		for _, row := range snapshot.ReadinessSlack {
			row.Replica = snapshot.Replica
			all = append(all, row)
		}
	}
	slices.SortFunc(all, mostSlackFirst)
	view.ReadinessSlack = all[:min(len(all), MaxReadinessSlack)]
}
