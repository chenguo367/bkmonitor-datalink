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
)

// MaxLookbackUncovered is the most owned objects without a fresh lookback
// measurement one snapshot names; the count of all of them, by why, is
// alarmd_lookback_uncovered_groups.
const MaxLookbackUncovered = 32

// LookbackUncoveredFacts is one owned object the late-data lookback has no
// fresh measurement of: why (the lookback's closed reasons), since when the
// process has had state of it and when its next sample may be taken, both
// zero for one it has not seen, and the series its Plans bound, which the
// snapshot ranks them by. Replica is the replica that holds it, set on the
// deployment's list.
type LookbackUncoveredFacts struct {
	QueryGroup string    `json:"query_group"`
	Reason     string    `json:"reason"`
	Since      time.Time `json:"since"`
	NextAt     time.Time `json:"next_at"`
	Series     uint64    `json:"series"`
	Replica    string    `json:"replica,omitempty"`
}

// LargestUncovered is the given objects with the series each one's Plans
// bound, as SeriesMatchedIn counts them for the State cache - Plans seen
// within the window - the most first, at most MaxLookbackUncovered.
func (tracker *Tracker) LargestUncovered(uncovered []LookbackUncoveredFacts, within time.Duration) []LookbackUncoveredFacts {
	if len(uncovered) == 0 {
		return nil
	}
	ranked := append([]LookbackUncoveredFacts(nil), uncovered...)
	tracker.mu.Lock()
	since := tracker.now().Add(-within)
	for index := range ranked {
		ranked[index].Series = tracker.seriesMatchedLocked(ranked[index].QueryGroup, since)
	}
	tracker.mu.Unlock()
	slices.SortFunc(ranked, largestUncoveredFirst)
	return ranked[:min(len(ranked), MaxLookbackUncovered)]
}

// largestUncoveredFirst orders objects by the series their Plans bound, the
// most first, then by replica and Query Group.
func largestUncoveredFirst(a, b LookbackUncoveredFacts) int {
	return cmp.Or(cmp.Compare(b.Series, a.Series), cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.QueryGroup, b.QueryGroup))
}

// aggregateLookbackUncovered puts the replicas' uncovered objects together,
// each naming its replica, the largest first, within the bound: one read of
// the deployment names the groups its lookbacks do not measure.
func aggregateLookbackUncovered(view *View, snapshots []Snapshot) {
	var all []LookbackUncoveredFacts
	for _, snapshot := range snapshots {
		for _, row := range snapshot.LookbackUncovered {
			row.Replica = snapshot.Replica
			all = append(all, row)
		}
	}
	slices.SortFunc(all, largestUncoveredFirst)
	view.LookbackUncovered = all[:min(len(all), MaxLookbackUncovered)]
}
