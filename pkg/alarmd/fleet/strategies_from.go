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
	model "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Where a row's strategies came from, when not from the rounds this replica
// watched (Anomaly.StrategiesFrom, empty then). A Query Group restored into
// the degraded pool runs no round until it is probed, so its row names no
// strategy of its own: the replica reads the Plans of the schedule the pool
// holds it under from the frozen schedule. A pooled row that could not be
// read says why, by name, and names no strategy - never zero strategies.
const (
	// StrategiesFromSchedule: read from the frozen schedule the pool holds
	// the Query Group under.
	StrategiesFromSchedule = "schedule"
	// StrategiesFromScheduleUnread: the frozen schedule could not be read,
	// or not within the publish's bound.
	StrategiesFromScheduleUnread = "schedule_unread"
	// StrategiesFromScheduleRevisionMoved: the Segment at the pool's start
	// is of another schedule revision now; the Query Group's next Slot moves
	// the pool to it and probes.
	StrategiesFromScheduleRevisionMoved = "schedule_revision_moved"
	// StrategiesFromScheduleRevisionUnknown: the pool's line named no
	// schedule, a build before it did.
	StrategiesFromScheduleRevisionUnknown = "schedule_revision_unknown"
	// StrategiesFromSchedulePending: not read this publish, its reads spent
	// on others; read on a later one.
	StrategiesFromSchedulePending = "schedule_pending"
)

// StrategiesUnfilled is every reason a pooled row's strategies were not read
// from its schedule, in the order the pages list them.
var StrategiesUnfilled = []string{StrategiesFromScheduleUnread, StrategiesFromScheduleRevisionMoved,
	StrategiesFromScheduleRevisionUnknown, StrategiesFromSchedulePending}

// StrategiesFromValues is every value Anomaly.StrategiesFrom takes but the
// empty one.
func StrategiesFromValues() []string {
	return append([]string{StrategiesFromSchedule}, StrategiesUnfilled...)
}

// strategiesUnfilledReason is the reason a row names no strategy, when it is
// a pooled row whose schedule was not read.
func strategiesUnfilledReason(row Anomaly) (string, bool) {
	if len(row.Strategies) != 0 {
		return "", false
	}
	for _, reason := range StrategiesUnfilled {
		if row.StrategiesFrom == reason {
			return reason, true
		}
	}
	return "", false
}

// FillScheduleStrategies names on a pooled row the strategies of the Plans
// its schedule serves: each strategy once, in order, the list cut at the
// bound a row's list has (maxStrategiesPerQueryGroup), and StrategiesTotal
// all of them, so a count is taken over every Plan and a cut list is told
// from a whole one.
func FillScheduleStrategies(row *Anomaly, plans []model.FrozenPlanSchedule) {
	known := map[StrategyRef]struct{}{}
	for _, plan := range plans {
		known[StrategyRef{StrategyID: plan.Identity.StrategyID, BusinessID: plan.Identity.BusinessID}] = struct{}{}
	}
	all := make([]StrategyRef, 0, len(known))
	for ref := range known {
		all = append(all, ref)
	}
	sortStrategies(all)
	row.Strategies = all[:min(len(all), maxStrategiesPerQueryGroup):min(len(all), maxStrategiesPerQueryGroup)]
	row.StrategiesTotal, row.StrategiesFrom = len(all), StrategiesFromSchedule
}

// MarkStrategiesUnfilled names why a pooled row's schedule was not read: one
// of StrategiesUnfilled.
func MarkStrategiesUnfilled(row *Anomaly, reason string) {
	row.Strategies, row.StrategiesTotal, row.StrategiesFrom = nil, 0, reason
}

// StrategiesUnfilledCounts is the rows naming no strategy whose schedule was
// not read, one count for each of StrategiesUnfilled.
type StrategiesUnfilledCounts struct {
	Unread          int `json:"schedule_unread,omitempty"`
	RevisionMoved   int `json:"schedule_revision_moved,omitempty"`
	RevisionUnknown int `json:"schedule_revision_unknown,omitempty"`
	Pending         int `json:"schedule_pending,omitempty"`
}

// Of is the count of one reason, zero for a value that is none.
func (counts StrategiesUnfilledCounts) Of(reason string) int {
	switch reason {
	case StrategiesFromScheduleUnread:
		return counts.Unread
	case StrategiesFromScheduleRevisionMoved:
		return counts.RevisionMoved
	case StrategiesFromScheduleRevisionUnknown:
		return counts.RevisionUnknown
	case StrategiesFromSchedulePending:
		return counts.Pending
	}
	return 0
}

func (counts *StrategiesUnfilledCounts) add(reason string, count int) {
	switch reason {
	case StrategiesFromScheduleUnread:
		counts.Unread += count
	case StrategiesFromScheduleRevisionMoved:
		counts.RevisionMoved += count
	case StrategiesFromScheduleRevisionUnknown:
		counts.RevisionUnknown += count
	case StrategiesFromSchedulePending:
		counts.Pending += count
	}
}

// unlistedStrategies is how many of a row's strategies its cut list does not
// name.
func unlistedStrategies(row Anomaly) int {
	return max(row.StrategiesTotal-len(row.Strategies), 0)
}
