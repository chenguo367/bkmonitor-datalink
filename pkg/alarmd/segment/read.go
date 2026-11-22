// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package segment

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Coverage is what a series' read is extended by: its entry, the position of
// the mark of its Plan, and the Plan's grid - the interval between points, in
// seconds, and the points a record retains.
type Coverage struct {
	Entry     Entry
	Position  int64
	Interval  int64
	Retention uint32
	// Leaves is whether the round leaves the member's State write out, as
	// far as its Slot can tell before evaluating the series: an open member
	// of its Plan, read in a normal round that follows the segment skip and
	// is not its Plan's sampled round, the Plan not stopped, its page not
	// written whole for want of renewal. Renewal is then its page's last
	// renewal in the mark the Slot read, which the member's key outlives:
	// how long a value of it read back stays the store's (014 next7 3.1).
	Leaves  bool
	Renewal Renewal
}

// Extension is what Extend did with a read, closed: extended, points made
// normal up to the segment's end; within, nothing to extend - the State's own
// points reach the end; and why one that should have been was not, the read
// left as stored and so a gap where the points are missing, never normal.
type Extension string

const (
	Extended            Extension = "extended"
	Within              Extension = "within"
	RefusedNotFound     Extension = "refused_not_found"
	RefusedOffGrid      Extension = "refused_off_grid"
	RefusedFingerprint  Extension = "refused_fingerprint"
	RefusedRecordID     Extension = "refused_record_id"
	RefusedRecordClash  Extension = "refused_record_clash"
	RefusedBadCoverage  Extension = "refused_bad_coverage"
	RefusedNoLevelsRead Extension = "refused_no_levels"
)

// Extensions is every Extension, for a counter's cells.
var Extensions = []Extension{Extended, Within, RefusedNotFound, RefusedOffGrid, RefusedFingerprint,
	RefusedRecordID, RefusedRecordClash, RefusedBadCoverage, RefusedNoLevelsRead}

// Extend is a series' stored read as its Plan's segments read it (014 B3
// 3.2), its key found: the State's own points up to their last, E; then, where
// the series' segment covers - after the later of its start and E, up to U,
// the mark's position for an open segment and the earlier of it and the
// close for a closed one - a normal point at every interval, every Level of
// the record present, valid and not anomalous; and no further. U is never
// "now": a mark that stopped (a crash, a failed write, an old build taking
// over) leaves what nobody decided as a gap. The Levels' and the record's
// processed event time read as U, and the last real point of a Level that
// keeps one - only a Level written to end on no data does; every other
// writes zero, which a made point leaves as it is. What
// Extend does not change is the version the State carries: which of it and
// the mark's is the effective one is the reader's, not the read's.
//
// A key not found is no read to extend (MISSING_WARMING, rule 1: a member
// whose key expired is not read as normal). Points Extend cannot make the
// way the record would hold them - off the Plan's grid, a Level with no
// fingerprint, a record id the series does not derive or one a stored point
// already holds - leave the read as stored, the refusal named.
func Extend(view execution.RuntimeStateView, coverage Coverage) (execution.RuntimeStateView, Extension) {
	points, end, outcome := Made(view, coverage)
	if outcome != Extended {
		return view, outcome
	}
	history := make([]execution.StateHistoryPoint, 0, min(int(coverage.Retention), len(view.History)+len(points)))
	err := execution.WalkMergedHistory(view.History, points, coverage.Retention, func(point execution.StateHistoryPoint) error {
		history = append(history, point)
		return nil
	})
	if err != nil {
		// The one error the merge gives: a stored point at a made point's
		// source time under another record id (HistoryRecordIdentityConflict).
		return view, RefusedRecordClash
	}
	extended := view
	extended.History = history
	extended.LastProcessedEventTime = end
	extended.Levels = make([]execution.RuntimeLevelStateView, len(view.Levels))
	for index, level := range view.Levels {
		level.LastProcessedEventTime = end
		if level.LastRealPointTime != 0 {
			level.LastRealPointTime = end
		}
		extended.Levels[index] = level
	}
	return extended, Extended
}

// Made is the points Extend makes for a read and the end it reads to: the
// normal point at every interval the segment covers after the State's own,
// the latest the record retains, each with the record id the series derives
// there and its Levels' fingerprints. Extend merges them into the stored
// history; a reader that has the stored history beside it already - the
// shadow comparing a round's write against them - need not copy it.
func Made(view execution.RuntimeStateView, coverage Coverage) ([]execution.StateHistoryPoint, int64, Extension) {
	switch view.Status {
	case execution.StateFoundReady, execution.StateFoundWarming, execution.StateFoundGapped:
	default:
		return nil, 0, RefusedNotFound
	}
	if coverage.Interval <= 0 || coverage.Retention == 0 || coverage.Position < 0 || coverage.Entry.Start < 0 {
		return nil, 0, RefusedBadCoverage
	}
	end := coverage.Position
	if !coverage.Entry.Open() {
		end = min(coverage.Entry.Closed, coverage.Position)
	}
	from := max(coverage.Entry.Start, view.LastProcessedEventTime)
	if end <= from {
		return nil, end, Within
	}
	if (end-from)%coverage.Interval != 0 {
		return nil, end, RefusedOffGrid
	}
	if len(view.Levels) == 0 {
		return nil, end, RefusedNoLevelsRead
	}
	fingerprints, ok := levelFingerprints(view)
	if !ok {
		return nil, end, RefusedFingerprint
	}
	deriver, err := contract.NewRecordIDDeriverV2(string(view.Identity.SeriesIdentityDigest))
	if err != nil {
		return nil, end, RefusedRecordID
	}
	// Only the points the record can retain: those before them would be
	// dropped by the merge as they are made.
	count := (end - from) / coverage.Interval
	if count > int64(coverage.Retention) {
		count = int64(coverage.Retention)
	}
	points := make([]execution.StateHistoryPoint, 0, count)
	for position := end - (count-1)*coverage.Interval; position <= end; position += coverage.Interval {
		recordID, err := deriver.Derive(position)
		if err != nil {
			return nil, end, RefusedRecordID
		}
		facts := make([]execution.StateLevelFact, 0, len(view.Levels))
		for index, level := range view.Levels {
			facts = append(facts, execution.StateLevelFact{LevelID: level.LevelID, DetectFingerprint: fingerprints[index],
				Result: execution.LevelFactNormal})
		}
		points = append(points, execution.StateHistoryPoint{RecordID: recordID, SourceTime: position, Levels: facts})
	}
	return points, end, Extended
}

// levelFingerprints is each Level's detect fingerprint, in the order of the
// view's Levels, off the latest stored point that has a fact of it: a record
// holds one a Level, and a made point carries the same.
func levelFingerprints(view execution.RuntimeStateView) ([]string, bool) {
	fingerprints := make([]string, len(view.Levels))
	found := 0
	for point := len(view.History) - 1; point >= 0 && found < len(view.Levels); point-- {
		for _, fact := range view.History[point].Levels {
			for index, level := range view.Levels {
				if level.LevelID == fact.LevelID && fingerprints[index] == "" && fact.DetectFingerprint != "" {
					fingerprints[index] = fact.DetectFingerprint
					found++
				}
			}
		}
	}
	return fingerprints, found == len(view.Levels)
}
