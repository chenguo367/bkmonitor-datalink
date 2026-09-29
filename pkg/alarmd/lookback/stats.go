// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import "strings"

// counters are the lookback's cumulative counts since the process started,
// keyed by their closed words joined with "|". Every cell of every closed
// set exists from New: a zero is a count, and a missing cell would read as
// one.
type counters struct {
	samples     map[string]uint64 // source|outcome
	rechecks    map[string]uint64 // source|tier|outcome
	buckets     map[string]uint64 // source|tier: (series, bucket) pairs compared
	windows     map[string]uint64 // source|tier|differed ("yes"/"no")
	differences map[string]uint64 // source|tier|class
	judgments   map[string]uint64 // source|tier|class
	series      map[string]uint64 // source|tier|new or vanished
	ages        map[string]uint64 // source|age|differed
	preempted   map[string]uint64 // source|tier: reads stopped for a formal query
	refusals    map[string]uint64 // reason: permits refused
}

func newCounters(refusals []string) counters {
	c := counters{samples: map[string]uint64{}, rechecks: map[string]uint64{}, buckets: map[string]uint64{},
		windows: map[string]uint64{}, differences: map[string]uint64{}, judgments: map[string]uint64{},
		series: map[string]uint64{}, ages: map[string]uint64{}, preempted: map[string]uint64{},
		refusals: map[string]uint64{RefusedOther: 0}}
	for _, reason := range refusals {
		c.refusals[reason] = 0
	}
	for _, source := range Sources {
		for _, outcome := range SampleOutcomes {
			c.samples[key2(source, outcome)] = 0
		}
		for _, age := range AgeBuckets {
			c.ages[key3(source, age, "yes")], c.ages[key3(source, age, "no")] = 0, 0
		}
		for _, tier := range TierNames {
			c.buckets[key2(source, tier)] = 0
			c.preempted[key2(source, tier)] = 0
			c.windows[key3(source, tier, "yes")], c.windows[key3(source, tier, "no")] = 0, 0
			c.series[key3(source, tier, "new")], c.series[key3(source, tier, "vanished")] = 0, 0
			for _, outcome := range RecheckOutcomes {
				c.rechecks[key3(source, tier, outcome)] = 0
			}
			for _, class := range Differences {
				c.differences[key3(source, tier, class)] = 0
			}
			for _, class := range Judgments {
				c.judgments[key3(source, tier, class)] = 0
			}
		}
	}
	return c
}

func key2(a, b string) string    { return a + "|" + b }
func key3(a, b, c string) string { return a + "|" + b + "|" + c }

func (c counters) record(source, tier, age string, result comparison) {
	c.buckets[key2(source, tier)] += uint64(result.buckets)
	differed := "no"
	if result.differsInWindow {
		differed = "yes"
	}
	c.windows[key3(source, tier, differed)]++
	c.ages[key3(source, age, differed)]++
	for class, n := range result.differences {
		c.differences[key3(source, tier, class)] += uint64(n)
	}
	for class, n := range result.judgments {
		c.judgments[key3(source, tier, class)] += uint64(n)
	}
	c.series[key3(source, tier, "new")] += uint64(result.newSeries)
	c.series[key3(source, tier, "vanished")] += uint64(result.vanishedSeries)
}

// Stats is the lookback as it stands: every counter by its words, what is
// waiting, and the recent differing rechecks.
type Stats struct {
	// Samples: source -> outcome -> count.
	Samples map[string]map[string]uint64 `json:"samples"`
	// Rechecks: source -> tier -> outcome -> count. Only "compared" enters
	// the denominators below.
	Rechecks map[string]map[string]map[string]uint64 `json:"rechecks"`
	// ComparedBuckets: source -> tier -> (series, bucket) pairs compared,
	// the denominator of Differences.
	ComparedBuckets map[string]map[string]uint64 `json:"compared_buckets"`
	// ComparedWindows: source -> tier -> "yes"/"no" -> windows compared, by
	// whether anything in them differed.
	ComparedWindows map[string]map[string]map[string]uint64 `json:"compared_windows"`
	// Differences and Judgments: source -> tier -> class -> count. The
	// Judgments denominator is its own sum: (Plan, Level, series, bucket).
	Differences map[string]map[string]map[string]uint64 `json:"differences"`
	Judgments   map[string]map[string]map[string]uint64 `json:"judgments"`
	// Series: source -> tier -> "new"/"vanished" -> series. A new series had
	// no admission at the first read and is data quality only.
	Series map[string]map[string]map[string]uint64 `json:"series"`
	// ByAge: source -> age read -> "yes"/"no" -> windows: the delay profile.
	ByAge map[string]map[string]map[string]uint64 `json:"by_age"`
	// Preempted: source -> tier -> reads stopped because a formal query had
	// to wait for a permit. The tier is tried again within its window and
	// counted in Rechecks by what it finally comes to.
	Preempted map[string]map[string]uint64 `json:"preempted"`
	// PermitRefusals: reason -> permits refused. A refused tier keeps its
	// window; one that finds no permit in it is counted as yielded.
	PermitRefusals map[string]uint64 `json:"permit_refusals"`

	Pending      int      `json:"pending"`
	PendingBytes int      `json:"pending_bytes"`
	MemoryBytes  int      `json:"memory_bytes"`
	Recent       []Recent `json:"recent"`
}

// Stats reads the lookback now.
func (engine *Engine) Stats() Stats {
	stats := Stats{Samples: map[string]map[string]uint64{}, Rechecks: map[string]map[string]map[string]uint64{},
		ComparedBuckets: map[string]map[string]uint64{}, ComparedWindows: map[string]map[string]map[string]uint64{},
		Differences: map[string]map[string]map[string]uint64{}, Judgments: map[string]map[string]map[string]uint64{},
		Series: map[string]map[string]map[string]uint64{}, ByAge: map[string]map[string]map[string]uint64{},
		Preempted: map[string]map[string]uint64{}, PermitRefusals: map[string]uint64{}, Recent: []Recent{}}
	if engine == nil {
		return stats
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	two := func(from map[string]uint64, into map[string]map[string]uint64) {
		for key, n := range from {
			parts := strings.SplitN(key, "|", 2)
			if into[parts[0]] == nil {
				into[parts[0]] = map[string]uint64{}
			}
			into[parts[0]][parts[1]] = n
		}
	}
	three := func(from map[string]uint64, into map[string]map[string]map[string]uint64) {
		for key, n := range from {
			parts := strings.SplitN(key, "|", 3)
			if into[parts[0]] == nil {
				into[parts[0]] = map[string]map[string]uint64{}
			}
			if into[parts[0]][parts[1]] == nil {
				into[parts[0]][parts[1]] = map[string]uint64{}
			}
			into[parts[0]][parts[1]][parts[2]] = n
		}
	}
	two(engine.counts.samples, stats.Samples)
	three(engine.counts.rechecks, stats.Rechecks)
	two(engine.counts.buckets, stats.ComparedBuckets)
	three(engine.counts.windows, stats.ComparedWindows)
	three(engine.counts.differences, stats.Differences)
	three(engine.counts.judgments, stats.Judgments)
	three(engine.counts.series, stats.Series)
	three(engine.counts.ages, stats.ByAge)
	two(engine.counts.preempted, stats.Preempted)
	for reason, n := range engine.counts.refusals {
		stats.PermitRefusals[reason] = n
	}
	stats.Pending, stats.PendingBytes, stats.MemoryBytes = len(engine.pending), engine.bytes, engine.options.MemoryBytes
	stats.Recent = append(stats.Recent, engine.recent...)
	return stats
}
