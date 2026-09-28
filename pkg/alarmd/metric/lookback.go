// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
)

// lookbackCollector reads the late-data lookback at scrape time. A process
// that does not run it emits nothing: its families are registered, and a
// reader tells "not running" from "ran and saw nothing" by whether any
// series exists. A process that runs it emits every cell from the start.
type lookbackCollector struct {
	mu      sync.Mutex
	source  func() lookback.Stats
	samples *prometheus.Desc
	checks  *prometheus.Desc
	buckets *prometheus.Desc
	windows *prometheus.Desc
	diffs   *prometheus.Desc
	judged  *prometheus.Desc
	series  *prometheus.Desc
	ages    *prometheus.Desc
	pending *prometheus.Desc
}

func newLookbackCollector() *lookbackCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, name), help, labels, nil)
	}
	return &lookbackCollector{
		samples: desc("lookback_samples_total",
			"Sampled first reads of the late-data lookback by source and what became of them: captured, uncovered "+
				"(past the sample bounds), first_read_incomplete, memory_full, owner_lost, completed (every tier done).",
			"source", "outcome"),
		checks: desc("lookback_rechecks_total",
			"Rechecks by source, tier and outcome. Only compared enters the comparison counts; yielded, "+
				"recheck_failed, truncated, partial and owner_lost are windows not observed, never windows that did not change.",
			"source", "tier", "outcome"),
		buckets: desc("lookback_compared_buckets_total",
			"(series, bucket) pairs compared, by source and tier: the denominator of lookback_differences_total.",
			"source", "tier"),
		windows: desc("lookback_compared_windows_total",
			"Compared windows by source, tier and whether anything in them differed (yes/no).",
			"source", "tier", "differed"),
		diffs: desc("lookback_differences_total",
			"Compared (series, bucket) pairs by how the recheck differs from the first read. A bucket with no "+
				"record is not a 0: new_point, vanished_point, new_series and vanished_series are records that were or were not there.",
			"source", "tier", "class"),
		judged: desc("lookback_judgments_total",
			"How each Level's static-threshold verdict moved between the reads, per (Plan, Level, series, bucket), "+
				"for Plans that admitted the series at the first read; its denominator is its own sum.",
			"source", "tier", "class"),
		series: desc("lookback_series_total",
			"Series the recheck found that the first read did not have (new), and the reverse (vanished).",
			"source", "tier", "kind"),
		ages: desc("lookback_windows_by_age_total",
			"Compared windows by how long after the window's end the recheck actually read, and whether they differed: "+
				"the delay profile, by the age read rather than the tier planned.",
			"source", "age", "differed"),
		pending: desc("lookback_pending",
			"Samples waiting for their next tier, and the bytes charged for them against memory_bytes, the share.",
			"what"),
	}
}

func (c *lookbackCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.samples, c.checks, c.buckets, c.windows, c.diffs, c.judged, c.series, c.ages, c.pending} {
		ch <- desc
	}
}

func (c *lookbackCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.source
	c.mu.Unlock()
	if source == nil {
		return
	}
	stats := source()
	counter := func(desc *prometheus.Desc, value uint64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(value), labels...)
	}
	for _, source := range lookback.Sources {
		for _, outcome := range lookback.SampleOutcomes {
			counter(c.samples, stats.Samples[source][outcome], source, outcome)
		}
		for _, age := range lookback.AgeBuckets {
			for _, differed := range []string{"yes", "no"} {
				counter(c.ages, stats.ByAge[source][age][differed], source, age, differed)
			}
		}
		for _, tier := range lookback.TierNames {
			for _, outcome := range lookback.RecheckOutcomes {
				counter(c.checks, stats.Rechecks[source][tier][outcome], source, tier, outcome)
			}
			counter(c.buckets, stats.ComparedBuckets[source][tier], source, tier)
			for _, differed := range []string{"yes", "no"} {
				counter(c.windows, stats.ComparedWindows[source][tier][differed], source, tier, differed)
			}
			for _, class := range lookback.Differences {
				counter(c.diffs, stats.Differences[source][tier][class], source, tier, class)
			}
			for _, class := range lookback.Judgments {
				counter(c.judged, stats.Judgments[source][tier][class], source, tier, class)
			}
			for _, kind := range []string{"new", "vanished"} {
				counter(c.series, stats.Series[source][tier][kind], source, tier, kind)
			}
		}
	}
	for what, value := range map[string]int{"samples": stats.Pending, "bytes": stats.PendingBytes, "memory_bytes": stats.MemoryBytes} {
		ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(value), what)
	}
}

// SetLookbackSource binds the process's lookback to the collector. A process
// that does not run the lookback never binds it and emits nothing.
func (r *Recorder) SetLookbackSource(source func() lookback.Stats) {
	if r == nil || r.phaseTwo.lookback == nil {
		return
	}
	r.phaseTwo.lookback.mu.Lock()
	r.phaseTwo.lookback.source = source
	r.phaseTwo.lookback.mu.Unlock()
}
