// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"sort"
	"time"
)

// ReadEarlyFacts is what a row of KindReadBeforeComplete says for itself:
// the late-data lookback found the object's window read before its data
// was complete in two completed samples in a row. The strategy's time_delay
// is what moves the read, so the row is the strategy's, with the value
// that would have read those samples complete.
type ReadEarlyFacts struct {
	// StepSeconds is the object's data step; CurrentDelaySeconds the
	// time_delay its query runs under, as compiled - aligned up to the step,
	// and 60 for a log keyword query that sets none - and
	// SuggestedDelaySeconds that plus how much later than its first read the
	// samples' data was complete, aligned up to the step the same way.
	StepSeconds           int64 `json:"step_seconds"`
	CurrentDelaySeconds   int64 `json:"current_time_delay_seconds"`
	SuggestedDelaySeconds int64 `json:"suggested_time_delay_seconds"`
	// Since is when the samples in a row began, as this process saw them.
	Since time.Time `json:"since"`
	// Samples are the samples read from, newest last.
	Samples []ReadEarlySample `json:"samples"`
}

// ReadEarlySample is one sample: the Slot, how long after the window's end
// its first read was and its data complete, and the rung at which a series
// of the first read was first seen changed - or, for an empty first read,
// data first seen - with the buckets that changed there.
type ReadEarlySample struct {
	EvaluationTime       int64   `json:"evaluation_time"`
	FirstReadAgeSeconds  int64   `json:"first_read_age_seconds"`
	CompletionAgeSeconds int64   `json:"completion_age_seconds"`
	Rung                 string  `json:"rung,omitempty"`
	ChangedAgeSeconds    int64   `json:"changed_age_seconds,omitempty"`
	Buckets              []int64 `json:"buckets,omitempty"`
}

// ReadEarly is a row for every object the lookback reports as read early,
// with the strategies this process has seen evaluate on it, the furthest
// from its suggested time_delay first. Its rounds complete; its results are
// read from data that was not all there.
func (tracker *Tracker) ReadEarly(facts map[string]ReadEarlyFacts) []Anomaly {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	anomalies := make([]Anomaly, 0, len(facts))
	for queryGroup, reading := range facts {
		state := tracker.groups[queryGroup]
		if state == nil {
			// Never seen evaluate here: no strategy to name it under.
			continue
		}
		reading := reading
		anomaly := Anomaly{
			QueryGroup: queryGroup, Kind: KindReadBeforeComplete,
			Since: reading.Since, SinceFrom: SinceSnapshotContinuity, Replica: tracker.replica,
			ReadEarly: &reading, LastHealthyAt: state.lastHealthyAt,
		}
		for strategy := range state.strategies {
			anomaly.Strategies = append(anomaly.Strategies, strategy)
		}
		sortStrategies(anomaly.Strategies)
		anomalies = append(anomalies, anomaly)
	}
	sort.Slice(anomalies, func(left, right int) bool {
		l, r := anomalies[left].ReadEarly, anomalies[right].ReadEarly
		if lg, rg := l.SuggestedDelaySeconds-l.CurrentDelaySeconds, r.SuggestedDelaySeconds-r.CurrentDelaySeconds; lg != rg {
			return lg > rg
		}
		return anomalies[left].QueryGroup < anomalies[right].QueryGroup
	})
	return anomalies
}
