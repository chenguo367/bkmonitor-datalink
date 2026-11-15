// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

// SeriesClass is what a series' round decided, read off its Levels' own
// results and windows: how many of the series a deployment writes every round
// decide nothing, how many are counting towards a trigger or a recovery, and
// how many changed state. It is the reading a write path that leaves steady
// series alone is sized from, so it is decided from this round's decisions
// and never from the Level metadata the stored record keeps, which advances
// every round whatever the series decided.
//
// A byte, not a string: every series in flight carries one
// (StateEvaluation.Class). The values are in rank order, so the class a
// series takes from its Levels and records is the largest.
type SeriesClass uint8

const (
	// SeriesClassNone is no class decided yet; it gives way to any.
	SeriesClassNone SeriesClass = iota
	// SeriesClassStableNormal is a Level that decided NORMAL or RECOVERY with
	// no anomaly in any position it read, its trigger window or its recovery
	// walk's.
	SeriesClassStableNormal
	// SeriesClassOther is a Level that decided nothing for any other reason
	// than its history: outside its effective time, or its fact unavailable.
	SeriesClassOther
	// SeriesClassWarming and SeriesClassGapped are a Level that decided
	// nothing because its history is still warming or gapped.
	SeriesClassWarming
	SeriesClassGapped
	// SeriesClassCountingTrigger is a Level that decided NORMAL or RECOVERY
	// with an anomaly in a position it read - its trigger window or the
	// recovery walk's - and no window meeting the trigger: counting towards
	// a trigger, or the anomalies of one it recovered from still retained.
	SeriesClassCountingTrigger
	// SeriesClassCountingRecovery is a Level that decided NORMAL while its
	// recovery walk stopped on a window that still meets the trigger, or
	// found its recovery due and withheld it with anomalies still read: by
	// the Level's own windows its alert is open.
	SeriesClassCountingRecovery
	// SeriesClassTransition is a Level that decided ABNORMAL. A RECOVERY is
	// classed as a NORMAL is (evaluation's levelClassOf): it is decided on
	// every quiet series, and the round that ends an alert is not told apart
	// from the ones after it.
	SeriesClassTransition
)

// SeriesClasses is every class a series can take, in rank order from the
// lowest.
var SeriesClasses = []SeriesClass{
	SeriesClassStableNormal, SeriesClassOther, SeriesClassWarming, SeriesClassGapped,
	SeriesClassCountingTrigger, SeriesClassCountingRecovery, SeriesClassTransition,
}

var seriesClassNames = map[SeriesClass]string{
	SeriesClassStableNormal:     "stable_normal",
	SeriesClassOther:            "other",
	SeriesClassWarming:          "warming",
	SeriesClassGapped:           "gapped",
	SeriesClassCountingTrigger:  "counting_trigger",
	SeriesClassCountingRecovery: "counting_recovery",
	SeriesClassTransition:       "transition",
}

// String is the class's name; none and an unknown value are other, the class
// that names no decision.
func (class SeriesClass) String() string {
	if name, known := seriesClassNames[class]; known {
		return name
	}
	return seriesClassNames[SeriesClassOther]
}

// HigherSeriesClass is the class a series takes from two of its Levels, or
// from two records of it in one Slot: the higher-ranked.
func HigherSeriesClass(left, right SeriesClass) SeriesClass {
	return max(left, right)
}
