// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// counters are the lookback's cumulative counts since the process started,
// keyed by their closed words joined with "|". Every cell of every closed
// set exists from New: a zero is a count, and a missing cell would read as
// one.
type counters struct {
	firstReads map[string]uint64 // source: formal first reads seen
	samples    map[string]uint64 // source|outcome
	rechecks   map[string]uint64 // source|rung|outcome
	changed    map[string]uint64 // source|rung: compared windows that changed since the read before
	changes    map[string]uint64 // source|rung|class: changed buckets
	preempted  map[string]uint64 // source|rung: reads stopped for a formal query
	completion map[string]uint64 // source|age: finished samples by when the window was complete
	refusals   map[string]uint64 // reason: permits refused
	faults     map[string]uint64 // reason
}

func newCounters(sources, refusals []string) counters {
	c := counters{firstReads: map[string]uint64{}, samples: map[string]uint64{}, rechecks: map[string]uint64{},
		changed: map[string]uint64{}, changes: map[string]uint64{}, preempted: map[string]uint64{},
		completion: map[string]uint64{}, refusals: map[string]uint64{RefusedOther: 0}, faults: map[string]uint64{}}
	for _, reason := range refusals {
		c.refusals[reason] = 0
	}
	for _, fault := range Faults {
		c.faults[fault] = 0
	}
	for _, source := range sources {
		c.firstReads[source] = 0
		for _, outcome := range SampleOutcomes {
			c.samples[key2(source, outcome)] = 0
		}
		for _, age := range AgeBuckets {
			c.completion[key2(source, age)] = 0
		}
		for _, rung := range RungNames {
			c.changed[key2(source, rung)] = 0
			c.preempted[key2(source, rung)] = 0
			for _, outcome := range RecheckOutcomes {
				c.rechecks[key3(source, rung, outcome)] = 0
			}
			for _, class := range Changes {
				c.changes[key3(source, rung, class)] = 0
			}
		}
	}
	return c
}

func key2(a, b string) string    { return a + "|" + b }
func key3(a, b, c string) string { return a + "|" + b + "|" + c }

// Stats is the lookback as it stands.
type Stats struct {
	Coverage Coverage `json:"coverage"`
	// Sources: every counted source, the named ones and mixed, promql, other.
	Sources map[string]SourceStats `json:"sources"`
	// PermitRefusals: reason -> permits refused. A refused rung keeps its
	// window; one that finds no permit in it is counted as yielded.
	PermitRefusals map[string]uint64 `json:"permit_refusals"`
	// Faults: reason -> reads not kept for a defect. Always 0 in normal
	// running; anything else is a defect to fix.
	Faults map[string]uint64 `json:"faults"`
	// Pending is the samples in flight, one at most per Query Group, and
	// PendingBytes what their summaries hold: computed, not a budget.
	Pending      int `json:"pending"`
	PendingBytes int `json:"pending_bytes"`
	// Latest is the measured Query Groups whose data was complete latest,
	// latest first, at most maxLatest.
	Latest []GroupLateness `json:"latest"`
	// Recent is the latest rechecks that found a change, at most maxRecent.
	Recent []Recent `json:"recent"`
}

// Coverage is how many of the Query Groups this process owns have a
// measurement that is fresh - a sample in flight, or one finished within
// twice the time a sample of it takes - out of how many it owns. The aim is
// every one.
type Coverage struct {
	Owned   int     `json:"owned"`
	Covered int     `json:"covered"`
	Ratio   float64 `json:"ratio"`
}

// SourceStats is one source: what its data does, how it is rechecked, and
// what the rechecking costs.
type SourceStats struct {
	// FirstReads is the formal first reads seen; Rechecks over it is the
	// query volume the lookback adds.
	FirstReads uint64 `json:"first_reads"`
	// Samples: outcome -> count.
	Samples map[string]uint64 `json:"samples"`
	// Rechecks: rung -> outcome -> count. Only compared is a window observed.
	Rechecks map[string]map[string]uint64 `json:"rechecks"`
	// ChangedWindows: rung -> compared windows that changed since the read
	// before; over Rechecks[rung][compared] it is the share of windows still
	// arriving at that rung.
	ChangedWindows map[string]uint64 `json:"changed_windows"`
	// Changes: rung -> class -> changed buckets.
	Changes map[string]map[string]uint64 `json:"changes"`
	// Preempted: rung -> reads stopped for a formal query and tried again.
	Preempted map[string]uint64 `json:"preempted"`
	// Completion: age past the window's end -> finished samples whose data
	// was complete by then; MaxCompletionSeconds the latest seen.
	Completion           map[string]uint64 `json:"completion"`
	MaxCompletionSeconds int64             `json:"max_completion_seconds"`
	// Depth is how many rungs a sample of it reads now, RestSteps how long a
	// Query Group rests between samples, in its steps.
	Depth     int     `json:"depth"`
	RestSteps float64 `json:"rest_steps"`
}

// GroupLateness is one Query Group's last measurement.
type GroupLateness struct {
	QueryGroup        execution.QueryGroupIdentity `json:"query_group"`
	Source            string                       `json:"source"`
	CompletionSeconds int64                        `json:"completion_seconds"`
	StepSeconds       int64                        `json:"step_seconds"`
	MeasuredAt        time.Time                    `json:"measured_at"`
}

// Stats reads the lookback now. Ownership is asked outside the engine's
// lock: the Runner set that answers it calls Forget while holding its own.
func (engine *Engine) Stats() Stats {
	stats := Stats{Sources: map[string]SourceStats{}, PermitRefusals: map[string]uint64{}, Faults: map[string]uint64{},
		Latest: []GroupLateness{}, Recent: []Recent{}}
	if engine == nil {
		return stats
	}
	now := engine.options.Now()
	type candidate struct {
		queryGroup execution.QueryGroupIdentity
		fresh      bool
		lateness   *GroupLateness
	}
	engine.mu.Lock()
	candidates := make([]candidate, 0, len(engine.groups))
	for queryGroup, state := range engine.groups {
		entry := candidate{queryGroup: queryGroup}
		switch {
		case state.capturing || state.sample != nil:
			entry.fresh = true
		case state.measured:
			source := engine.sources[state.source]
			cycle := time.Duration((source.rest+RungSteps[source.depth-1])*float64(state.step)) + state.period
			entry.fresh = now.Sub(state.measuredAt) <= 2*cycle
		}
		if state.measured {
			entry.lateness = &GroupLateness{QueryGroup: queryGroup, Source: state.source,
				CompletionSeconds: int64(state.completion / time.Second), StepSeconds: int64(state.step / time.Second),
				MeasuredAt: state.measuredAt}
		}
		if state.sample != nil {
			stats.Pending++
			stats.PendingBytes += state.sample.last.bytes()
		}
		candidates = append(candidates, entry)
	}
	for _, source := range engine.labels {
		state := engine.sources[source]
		entry := SourceStats{FirstReads: engine.counts.firstReads[source], Samples: map[string]uint64{},
			Rechecks: map[string]map[string]uint64{}, ChangedWindows: map[string]uint64{},
			Changes: map[string]map[string]uint64{}, Preempted: map[string]uint64{}, Completion: map[string]uint64{},
			MaxCompletionSeconds: int64(state.maxCompletion / time.Second), Depth: state.depth, RestSteps: state.rest}
		for _, outcome := range SampleOutcomes {
			entry.Samples[outcome] = engine.counts.samples[key2(source, outcome)]
		}
		for _, age := range AgeBuckets {
			entry.Completion[age] = engine.counts.completion[key2(source, age)]
		}
		for _, rung := range RungNames {
			entry.ChangedWindows[rung] = engine.counts.changed[key2(source, rung)]
			entry.Preempted[rung] = engine.counts.preempted[key2(source, rung)]
			entry.Rechecks[rung] = map[string]uint64{}
			for _, outcome := range RecheckOutcomes {
				entry.Rechecks[rung][outcome] = engine.counts.rechecks[key3(source, rung, outcome)]
			}
			entry.Changes[rung] = map[string]uint64{}
			for _, class := range Changes {
				entry.Changes[rung][class] = engine.counts.changes[key3(source, rung, class)]
			}
		}
		stats.Sources[source] = entry
	}
	for reason, n := range engine.counts.refusals {
		stats.PermitRefusals[reason] = n
	}
	for reason, n := range engine.counts.faults {
		stats.Faults[reason] = n
	}
	stats.Recent = append(stats.Recent, engine.recent...)
	engine.mu.Unlock()

	stats.Coverage.Owned = engine.options.Owned()
	for _, entry := range candidates {
		if !engine.options.Owns(entry.queryGroup) {
			continue
		}
		if entry.fresh {
			stats.Coverage.Covered++
		}
		if entry.lateness != nil {
			stats.Latest = append(stats.Latest, *entry.lateness)
		}
	}
	if stats.Coverage.Owned > 0 {
		stats.Coverage.Ratio = float64(stats.Coverage.Covered) / float64(stats.Coverage.Owned)
	}
	sort.Slice(stats.Latest, func(i, j int) bool {
		if stats.Latest[i].CompletionSeconds != stats.Latest[j].CompletionSeconds {
			return stats.Latest[i].CompletionSeconds > stats.Latest[j].CompletionSeconds
		}
		return stats.Latest[i].QueryGroup < stats.Latest[j].QueryGroup
	})
	if len(stats.Latest) > maxLatest {
		stats.Latest = stats.Latest[:maxLatest]
	}
	return stats
}
