// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import (
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// Absence values. A no-data group is a series with one point per Slot, and the
// point is the answer rather than a measurement: one for absent, zero for
// present. The trigger then counts absences the way it counts any other
// anomaly, which is what lets the whole path below this be the ordinary one.
const (
	AbsentValue  = 1
	PresentValue = 0
)

// SyntheticSeries is one no-data group as a series the ordinary evaluation can
// read: an identity, one point, and the dimensions the event will carry.
//
// Its dimensions always hold the no-data tag, which is what keeps this key away
// from the real series of the same item. They are the same dimensions the
// identity is built from, so a reader of the event and a reader of the state
// are looking at one object.
type SyntheticSeries struct {
	Group Group
	// Value is AbsentValue or PresentValue.
	Value int
	// Periods is how many periods the event says this group has been without
	// data. It is zero for a present group, which reports nothing.
	Periods int64
	// SourceTime is the point's own time: the period this Slot is deciding,
	// which is one period behind the Slot's evaluation time. The backend's
	// anomaly carries the same, and its record_id and anomaly_id are built
	// from it.
	SourceTime int64
}

// Dimensions returns what the event carries: the group's own pairs plus the
// tag, as JSON values ready for the Python-compatible identity hash. The tag is
// the JSON true that count_md5 sees as "True".
func (series SyntheticSeries) Dimensions() map[string]string {
	dimensions := make(map[string]string, len(series.Group.dimensions)+1)
	for _, dimension := range series.Group.dimensions {
		dimensions[dimension.Name] = dimension.Value
	}
	dimensions[contract.NoDataDimensionTag] = "true"
	return dimensions
}

// SyntheticInput is what turning verdicts into series needs beyond the verdicts.
type SyntheticInput struct {
	EvaluationTime int64
	PeriodSeconds  int64
	Result         AbsenceResult
	// Memory is the memory the result produced, which is where the period
	// counts are read from.
	Memory map[string]GroupMemory
	// Roster is the expected set the verdicts were made against, for the groups
	// the verdicts name.
	Roster Roster
}

// SyntheticSeriesFor turns one Slot's verdicts into the series the ordinary
// evaluation path reads, in group-key order so a retry produces the same list.
//
// An UNAVAILABLE verdict produces no series at all. That is the completeness
// gate reaching all the way out: a round that did not see the whole period must
// not advance the trigger window, and a point valued zero would advance it as a
// recovery while a point valued one would advance it as an absence. Producing
// nothing leaves the window where it was, which is the only reading that says
// "this round has no evidence".
func SyntheticSeriesFor(input SyntheticInput) []SyntheticSeries {
	keys := make([]string, 0, len(input.Result.Verdicts))
	for key, verdict := range input.Result.Verdicts {
		if verdict == VerdictUnavailable {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	series := make([]SyntheticSeries, 0, len(keys))
	sourceTime := input.EvaluationTime - input.PeriodSeconds
	for _, key := range keys {
		group, ok := input.Roster.Groups[key]
		if !ok {
			// The only verdict that is not a roster group is the whole item.
			group = WholeItemGroup()
		}
		entry := SyntheticSeries{Group: group, SourceTime: sourceTime, Value: PresentValue}
		if input.Result.Verdicts[key] == VerdictAnomaly {
			entry.Value = AbsentValue
			entry.Periods = absentPeriods(input.Memory[key], input.EvaluationTime, input.PeriodSeconds)
		}
		series = append(series, entry)
	}
	return series
}

// absentPeriods is how many periods the event says this group has been without
// data, following the backend's two counts and its choice between them.
//
// The backend keeps two checkpoints and derives a number from each: one from
// the last point it saw, one from the first round it called this group absent.
// It reports the first when that is positive and the second otherwise, which is
// the case of a group that has never been seen at all.
//
// The two differ in what they measure, and the difference is worth stating
// because it decides what a cross-read will show. The backend's last-seen
// checkpoint holds the data point's own timestamp, not the round that saw it,
// which is why it can also report "and the data is N periods late". Here it is
// the round, because a Slot is evaluated after its readiness and a point that
// arrived within that window is not late - the lateness the backend reports is
// the wait alarmd already did. So the two agree for data that arrives on time
// and differ by the lag for data that does not, and that difference is a
// property of the two designs rather than an error in either.
func absentPeriods(memory GroupMemory, evaluationTime, period int64) int64 {
	if period <= 0 {
		return 0
	}
	if memory.LastSeen > 0 {
		if since := (evaluationTime - memory.LastSeen) / period; since > 0 {
			return since
		}
	}
	if memory.FirstAbsent > 0 {
		return (evaluationTime-memory.FirstAbsent)/period + 1
	}
	return 1
}
