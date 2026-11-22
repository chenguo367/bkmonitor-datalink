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

// MaxFallbackWritesKept is the most fallback_write lines one replica keeps,
// the latest; MaxFallbackWrites the most the deployment's health names
// across the replicas. Lines, not fallbacks: a Plan writes one in the
// shorter of its page's two lifetimes, and its fallbacks between are
// counted on its next line (Suppressed). The counter
// state_segment_renewal_total{fallback_write} counts every fallback, and has
// no Plan.
const (
	MaxFallbackWritesKept = 16
	MaxFallbackWrites     = 32
)

// FallbackWriteFacts is one page a round found unrenewed for half its
// lifetime because renewals did not keep it alive, and wrote whole: its
// fallback line's facts (the Plan, its piece and generation, the page, the
// times that decided it, the lines it held back), the object and the Slot
// it was found in, and when this replica saw it. Replica is the replica
// that wrote it, set on the deployment's list.
//
// It is what a fallback_write's line says, kept: a line is one of the
// worker's log, read back only while it is among the last lines, and the
// count says how many but not which Plan. One row is one line, so the
// latest MaxFallbackWritesKept rows are the latest lines, not the latest
// fallbacks: a Plan's fallbacks within its line's window are not rows of
// their own, they are its next line's Suppressed. A fallback by design
// (fallback_unrenewable) is a success and is not kept.
type FallbackWriteFacts struct {
	At             time.Time `json:"at"`
	QueryGroup     string    `json:"query_group"`
	EvaluationTime int64     `json:"evaluation_time"`
	observability.SegmentFallbackFacts
	Replica string `json:"replica,omitempty"`
}

// noteFallbackWrite keeps a fallback_write line's facts, the latest
// MaxFallbackWritesKept, counting those it pushes out. The caller holds
// tracker.mu.
func (tracker *Tracker) noteFallbackWrite(queryGroup string, evaluationTime int64, facts *observability.SegmentFallbackFacts) {
	if facts == nil || facts.Result != observability.SegmentRenewalFallbackWrite {
		return
	}
	tracker.fallbackWrites = append(tracker.fallbackWrites, FallbackWriteFacts{At: tracker.now(), QueryGroup: queryGroup,
		EvaluationTime: evaluationTime, SegmentFallbackFacts: *facts})
	if over := len(tracker.fallbackWrites) - MaxFallbackWritesKept; over > 0 {
		tracker.fallbackWrites = append(tracker.fallbackWrites[:0], tracker.fallbackWrites[over:]...)
		tracker.fallbackWritesEvicted += uint64(over)
	}
}

// FallbackWrites is the fallback_write pages this replica kept, the latest
// first, and how many older ones were pushed out.
func (tracker *Tracker) FallbackWrites() ([]FallbackWriteFacts, uint64) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if len(tracker.fallbackWrites) == 0 {
		return nil, tracker.fallbackWritesEvicted
	}
	kept := slices.Clone(tracker.fallbackWrites)
	slices.Reverse(kept)
	return kept, tracker.fallbackWritesEvicted
}

// latestFallbackWriteFirst orders the pages by when they were seen, the
// latest first, then by replica, object, Slot and page.
func latestFallbackWriteFirst(a, b FallbackWriteFacts) int {
	return cmp.Or(b.At.Compare(a.At), cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.QueryGroup, b.QueryGroup),
		cmp.Compare(b.EvaluationTime, a.EvaluationTime), cmp.Compare(a.Page, b.Page))
}

// aggregateFallbackWrites puts the replicas' fallback_write pages together,
// each naming its replica, the latest first, within MaxFallbackWrites: one
// read of the deployment names the Plans whose renewals did not keep a page
// alive. Evicted is what the replicas pushed out and what the bound left out.
func aggregateFallbackWrites(view *View, snapshots []Snapshot) {
	var all []FallbackWriteFacts
	var evicted uint64
	for _, snapshot := range snapshots {
		evicted += snapshot.FallbackWritesEvicted
		for _, row := range snapshot.FallbackWrites {
			row.Replica = snapshot.Replica
			all = append(all, row)
		}
	}
	slices.SortFunc(all, latestFallbackWriteFirst)
	if over := len(all) - MaxFallbackWrites; over > 0 {
		evicted += uint64(over)
		all = all[:MaxFallbackWrites]
	}
	view.FallbackWrites, view.FallbackWritesEvicted = all, evicted
}
