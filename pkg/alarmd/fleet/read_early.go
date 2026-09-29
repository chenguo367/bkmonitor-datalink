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

// MaxReadEarlySamples and MaxReadEarlyBuckets bound the evidence a row of
// KindReadBeforeComplete carries: the lookback keeps the last three samples
// of a run and names at most eight changed buckets on each, and the tracker
// keeps no more of what it is given.
const (
	MaxReadEarlySamples = 3
	MaxReadEarlyBuckets = 8
)

// ReadEarlySample is one sample a suggested time_delay rests on: its Slot;
// how long after the window's end its first read was, and its data complete
// - the age of the recheck that first read it whole; the rung its first
// read's series were first seen changed at, how long after the window's end
// that was, and the buckets that changed there, oldest first.
type ReadEarlySample struct {
	EvaluationTime       int64   `json:"evaluation_time"`
	FirstReadAgeSeconds  int64   `json:"first_read_age_seconds"`
	CompletionAgeSeconds int64   `json:"completion_age_seconds"`
	Rung                 string  `json:"rung,omitempty"`
	ChangedAgeSeconds    int64   `json:"changed_age_seconds"`
	Buckets              []int64 `json:"buckets,omitempty"`
}

// ReadEarlyFacts is what a row of KindReadBeforeComplete says for itself:
// the late-data lookback found the object's window read before its data
// was complete in two completed samples in a row. The strategy's time_delay
// is what moves the read, so the row is the strategy's, with the value
// that would have read those samples complete and the samples it is read
// from. The samples ride on the row, under 1 KB at their widest - three
// samples of eight ten-digit buckets - so the suggestion and what it rests
// on are one read wherever the row is read, on a deployment that reads rows
// and not the owning replica's lookback as well.
type ReadEarlyFacts struct {
	// StepSeconds is the object's data step; CurrentDelaySeconds the
	// time_delay its query runs under, as compiled - aligned up to the step,
	// and 60 for a log keyword query that sets none - and
	// SuggestedDelaySeconds that plus how much later than its first read the
	// samples' data was complete, aligned up to the step the same way. The
	// completion is the age of the recheck that first read the data whole,
	// so the suggestion is an upper bound: the data came between that
	// recheck and the one before it.
	StepSeconds           int64 `json:"step_seconds"`
	CurrentDelaySeconds   int64 `json:"current_time_delay_seconds"`
	SuggestedDelaySeconds int64 `json:"suggested_time_delay_seconds"`
	// Since is when the samples in a row began, as this process saw them.
	Since time.Time `json:"since"`
	// Samples is the evidence, newest last: at most MaxReadEarlySamples,
	// each naming at most MaxReadEarlyBuckets changed buckets.
	Samples []ReadEarlySample `json:"samples,omitempty"`
}

// bounded is the facts with their evidence cut to its bounds, copied so the
// row shares nothing with what it was given.
func (facts ReadEarlyFacts) bounded() ReadEarlyFacts {
	samples := facts.Samples
	if len(samples) > MaxReadEarlySamples {
		samples = samples[len(samples)-MaxReadEarlySamples:]
	}
	facts.Samples = nil
	for _, sample := range samples {
		if len(sample.Buckets) > MaxReadEarlyBuckets {
			sample.Buckets = sample.Buckets[:MaxReadEarlyBuckets]
		}
		sample.Buckets = append([]int64(nil), sample.Buckets...)
		facts.Samples = append(facts.Samples, sample)
	}
	return facts
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
		reading := reading.bounded()
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
