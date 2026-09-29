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
	mu         sync.Mutex
	source     func() lookback.Stats
	firstReads *prometheus.Desc
	samples    *prometheus.Desc
	checks     *prometheus.Desc
	changed    *prometheus.Desc
	changes    *prometheus.Desc
	completion *prometheus.Desc
	latest     *prometheus.Desc
	depth      *prometheus.Desc
	rest       *prometheus.Desc
	coverage   *prometheus.Desc
	pending    *prometheus.Desc
	yields     *prometheus.Desc
	refused    *prometheus.Desc
	faults     *prometheus.Desc
}

func newLookbackCollector() *lookbackCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, name), help, labels, nil)
	}
	return &lookbackCollector{
		firstReads: desc("lookback_first_reads_total",
			"Formal first reads seen, by source: the denominator of the query volume the lookback adds "+
				"(lookback_rechecks_total over it).", "source"),
		samples: desc("lookback_samples_total",
			"First reads taken as a Query Group's sample, by source and what became of them: captured, "+
				"first_read_incomplete, owner_lost, completed (every planned rung read or passed), fault.",
			"source", "outcome"),
		checks: desc("lookback_rechecks_total",
			"Rechecks by source, rung (its moment in the Query Group's data steps) and outcome. Only compared is a "+
				"window observed; yielded, recheck_failed, partial and owner_lost are windows not observed.",
			"source", "rung", "outcome"),
		changed: desc("lookback_changed_windows_total",
			"Compared windows that changed since the read before, by source and rung: over the compared rechecks "+
				"of that rung, the share of windows whose data was still arriving.", "source", "rung"),
		changes: desc("lookback_changes_total",
			"Buckets of compared windows by how they changed since the read before: points_added, points_removed, "+
				"series_changed (as many points from other series), values_changed.", "source", "rung", "class"),
		completion: desc("lookback_completion_total",
			"Finished samples by when their window's data was complete, as its age past the window's end: the last "+
				"rung that changed, or the first read when none did.", "source", "age"),
		latest: desc("lookback_completion_max_seconds",
			"The latest any window of the source was complete, in seconds past its end, since the process started.",
			"source"),
		depth: desc("lookback_rung_depth",
			"How many rungs a sample of the source reads now: one past the last rung its data still changed at.",
			"source"),
		rest: desc("lookback_rest_steps",
			"How long a Query Group of the source rests between two samples, in its data steps: doubling while its "+
				"data is complete at the first read, up to 64.", "source"),
		coverage: desc("lookback_coverage",
			"The Query Groups this process owns, and how many of them have a fresh measurement; the aim is all.",
			"what"),
		pending: desc("lookback_pending",
			"Samples in flight, one at most per owned Query Group, and the bytes their summaries hold.", "what"),
		yields: desc("lookback_preemptions_total",
			"Recheck reads stopped because a formal query had to wait for a query permit, by source and rung. "+
				"The rung is tried again within its window and counted in lookback_rechecks_total by what it comes to.",
			"source", "rung"),
		refused: desc("lookback_permit_refusals_total",
			"Lookback query permits refused, by reason: waiters (a formal query is waiting), lookback_limit, headroom "+
				"(granting it would leave the formal queries too little), disabled. A refused rung keeps its window.",
			"reason"),
		faults: desc("lookback_faults_total",
			"Reads not kept for a defect, by reason. Normal running never meets one; any count is a defect to fix.",
			"reason"),
	}
}

func (c *lookbackCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.firstReads, c.samples, c.checks, c.changed, c.changes, c.completion,
		c.latest, c.depth, c.rest, c.coverage, c.pending, c.yields, c.refused, c.faults} {
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
	gauge := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}
	for name, source := range stats.Sources {
		counter(c.firstReads, source.FirstReads, name)
		for _, outcome := range lookback.SampleOutcomes {
			counter(c.samples, source.Samples[outcome], name, outcome)
		}
		for _, age := range lookback.AgeBuckets {
			counter(c.completion, source.Completion[age], name, age)
		}
		for _, rung := range lookback.RungNames {
			for _, outcome := range lookback.RecheckOutcomes {
				counter(c.checks, source.Rechecks[rung][outcome], name, rung, outcome)
			}
			counter(c.changed, source.ChangedWindows[rung], name, rung)
			for _, class := range lookback.Changes {
				counter(c.changes, source.Changes[rung][class], name, rung, class)
			}
			counter(c.yields, source.Preempted[rung], name, rung)
		}
		gauge(c.latest, float64(source.MaxCompletionSeconds), name)
		gauge(c.depth, float64(source.Depth), name)
		gauge(c.rest, source.RestSteps, name)
	}
	for reason, n := range stats.PermitRefusals {
		counter(c.refused, n, reason)
	}
	for reason, n := range stats.Faults {
		counter(c.faults, n, reason)
	}
	gauge(c.coverage, float64(stats.Coverage.Owned), "owned")
	gauge(c.coverage, float64(stats.Coverage.Covered), "covered")
	gauge(c.pending, float64(stats.Pending), "samples")
	gauge(c.pending, float64(stats.PendingBytes), "bytes")
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
