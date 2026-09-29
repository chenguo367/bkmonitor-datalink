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
	probes     *prometheus.Desc
	classes    *prometheus.Desc
	readEarly  *prometheus.Desc
	seriesLate *prometheus.Desc
	empty      *prometheus.Desc
	emptyAt    *prometheus.Desc
	latest     *prometheus.Desc
	groups     *prometheus.Desc
	rest       *prometheus.Desc
	readBytes  *prometheus.Desc
	checkBytes *prometheus.Desc
	unknown    *prometheus.Desc
	coverage   *prometheus.Desc
	pending    *prometheus.Desc
	yields     *prometheus.Desc
	refused    *prometheus.Desc
	faults     *prometheus.Desc
	// yieldReleases, yieldSeconds and yieldMax: how long reads asked to
	// yield took to give their permits back.
	yieldReleases *prometheus.Desc
	yieldSeconds  *prometheus.Desc
	yieldMax      *prometheus.Desc
	// The directed reads of series_late Query Groups and the supplements of
	// their Slots.
	supplementWindows    *prometheus.Desc
	supplementUnobserved *prometheus.Desc
	supplementSeries     *prometheus.Desc
	supplementPoints     *prometheus.Desc
	directedBytes        *prometheus.Desc
}

func newLookbackCollector() *lookbackCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, name), help, labels, nil)
	}
	return &lookbackCollector{
		firstReads: desc("lookback_first_reads_total",
			"Formal first reads seen, by source - the data source the Query Group reads, labelled as the directory "+
				"labels its Query Groups, so every lookback family reads beside them: the denominator of the query "+
				"volume the lookback adds (lookback_rechecks_total over it).", "source"),
		samples: desc("lookback_samples_total",
			"First reads taken as a Query Group's sample, by source and what became of them: captured, "+
				"first_read_incomplete, owner_lost, completed (its completion observed), unobserved (its last rungs not "+
				"read), probe_changed (its deep recheck found data arriving after the rungs its group read), fault. Only "+
				"completed enters lookback_completion_total.",
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
		probes: desc("lookback_probes_total",
			"Deep rechecks - a sample read once more at the deepest rung after the rungs its group reads, one sample "+
				"in four and a group's first - by source and outcome: clean, changed (data arrived after those rungs; "+
				"the group then reads every rung and settles), unobserved (not read; the next sample is probed).",
			"source", "outcome"),
		classes: desc("lookback_sample_classes_total",
			"Completed samples by what their rungs found against the first read, by source: window_read_early (the "+
				"first read was empty and data came later, or a series it had came back changed or not at all - the "+
				"strategy's time_delay moves the read), series_late (every series it had came back as it was, and "+
				"others came later - supplementary detection fills them), complete.", "source", "class"),
		readEarly: desc("lookback_read_early_groups",
			"The source's Query Groups whose window was read early in two completed samples in a row, each reported "+
				"with the time_delay that would have read it complete (lookback.get read_early).", "source"),
		seriesLate: desc("lookback_series_late_groups",
			"The source's Query Groups some of whose series were seen coming later than the first read.", "source"),
		supplementWindows: desc("lookback_supplement_windows_total",
			"Slots of series_late Query Groups read again for their late series, by source and what they came to: "+
				"supplemented (the supplement ran), nothing_late, flight_busy (the group's own Slot was executing), "+
				"contract_expired, failed, unobserved (not read). Coverage is supplemented over supplemented and "+
				"unobserved.", "source", "outcome"),
		supplementUnobserved: desc("lookback_supplement_unobserved_total",
			"Slots of series_late Query Groups not read again, by source and why: yielded, read_failed, "+
				"first_read_incomplete, multi_query, memory_refused.", "source", "reason"),
		supplementSeries: desc("lookback_supplement_series_total",
			"(Plan, series) pairs the supplements that ran were given, by source and what each came to: candidates "+
				"in all, and admitted, crossed_t, no_data_fact, config_drift, input_incomplete, withheld.",
			"source", "outcome"),
		supplementPoints: desc("lookback_supplement_points_total",
			"Points the admitted series of the supplements were evaluated on, by source.", "source"),
		directedBytes: desc("lookback_directed_read_bytes_total",
			"Bytes the directed reads delivered, by source: a Slot's frozen query each, read against "+
				"lookback_first_read_bytes_total over lookback_first_reads_total.", "source"),
		empty: desc("lookback_empty_first_reads_total",
			"Completed samples whose first read was complete and held no point, by source and whether their data "+
				"arrived at a later rung (arrived) or never did (stayed_empty); arrived over completed samples is the "+
				"share of windows empty when first read that were not.", "source", "outcome"),
		emptyAt: desc("lookback_empty_first_read_completion_total",
			"Those empty first reads whose data arrived later, by when their window's data was complete, as its age "+
				"past the window's end.", "source", "age"),
		latest: desc("lookback_completion_max_seconds",
			"The latest any window of the source was complete, in seconds past its end, since the process started.",
			"source"),
		groups: desc("lookback_groups",
			"The source's Query Groups by how many rungs each reads now - one past the last rung its own data still "+
				"changed at; each group learns that from its own samples.", "source", "depth"),
		rest: desc("lookback_rest_seconds",
			"How long the source's Query Groups rest between two samples on average: doubling while their lateness "+
				"holds, at most an hour.", "source"),
		readBytes: desc("lookback_first_read_bytes_total",
			"Bytes the formal first reads delivered, by source, counted as the rechecks' are.", "source"),
		checkBytes: desc("lookback_recheck_bytes_total",
			"Bytes the rechecks read back, by source: over lookback_first_read_bytes_total, the query bytes the "+
				"lookback adds.", "source"),
		unknown: desc("lookback_unknown_lookback_total",
			"Samples whose query's lookback (a window, a range, an offset) could not be read, rechecked from one "+
				"step before the window's tail instead.", "source"),
		coverage: desc("lookback_coverage",
			"The Query Groups this process owns, and how many of them have a fresh measurement; the aim is all.",
			"what"),
		pending: desc("lookback_pending",
			"Samples in flight - one at most per owned Query Group, and one waiting for its deep recheck - and the "+
				"bytes their summaries hold.", "what"),
		yields: desc("lookback_preemptions_total",
			"Recheck reads stopped because a formal query had to wait for a query permit, by source and rung. "+
				"The rung is tried again within its window and counted in lookback_rechecks_total by what it comes to.",
			"source", "rung"),
		refused: desc("lookback_permit_refusals_total",
			"Lookback query permits refused, by reason: waiters (a formal query is waiting), full (every process "+
				"permit is held), disabled. A refused rung keeps its window.",
			"reason"),
		yieldReleases: desc("lookback_yield_releases_total",
			"Recheck reads a waiting formal query asked to yield that gave their permit back, by source.", "source"),
		yieldSeconds: desc("lookback_yield_release_seconds_total",
			"How long those reads took to give their permits back, in all, by source: over "+
				"lookback_yield_releases_total, the mean wait a formal query owes the lookback once it has to wait. "+
				"A read still holding its permit RecheckTimeout after the yield is a yield_overdue fault.", "source"),
		yieldMax: desc("lookback_yield_release_max_seconds",
			"The longest any of those reads took to give its permit back since the process started, by source.",
			"source"),
		faults: desc("lookback_faults_total",
			"Reads not kept for a defect, by reason. Normal running never meets one; any count is a defect to fix.",
			"reason"),
	}
}

func (c *lookbackCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.firstReads, c.samples, c.checks, c.changed, c.changes, c.completion,
		c.probes, c.classes, c.readEarly, c.seriesLate, c.supplementWindows, c.supplementUnobserved, c.supplementSeries,
		c.supplementPoints, c.directedBytes, c.empty, c.emptyAt, c.latest, c.groups, c.rest, c.readBytes, c.checkBytes, c.unknown, c.coverage,
		c.pending, c.yields, c.refused, c.faults, c.yieldReleases, c.yieldSeconds, c.yieldMax} {
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
			counter(c.emptyAt, source.EmptyFirstReadCompletion[age], name, age)
		}
		for _, outcome := range lookback.ProbeOutcomes {
			counter(c.probes, source.Probes[outcome], name, outcome)
		}
		for _, class := range lookback.SampleClasses {
			counter(c.classes, source.Classes[class], name, class)
		}
		gauge(c.readEarly, float64(source.ReadEarlyGroups), name)
		gauge(c.seriesLate, float64(source.SeriesLateGroups), name)
		for _, outcome := range lookback.DirectedOutcomes {
			counter(c.supplementWindows, source.SupplementWindows[outcome], name, outcome)
		}
		for _, reason := range lookback.DirectedUnobservedReasons {
			counter(c.supplementUnobserved, source.SupplementUnobserved[reason], name, reason)
		}
		for _, outcome := range lookback.SupplementSeriesOutcomes {
			counter(c.supplementSeries, source.SupplementSeries[outcome], name, outcome)
		}
		counter(c.supplementPoints, source.SupplementPoints, name)
		counter(c.directedBytes, source.DirectedReadBytes, name)
		for _, outcome := range lookback.EmptyFirstReadOutcomes {
			counter(c.empty, source.EmptyFirstReads[outcome], name, outcome)
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
		for _, depth := range lookback.DepthLabels {
			gauge(c.groups, float64(source.DepthGroups[depth]), name, depth)
		}
		gauge(c.rest, source.MeanRestSeconds, name)
		counter(c.readBytes, source.FirstReadBytes, name)
		counter(c.checkBytes, source.RecheckBytes, name)
		counter(c.unknown, source.UnknownLookback, name)
		counter(c.yieldReleases, source.YieldReleases, name)
		ch <- prometheus.MustNewConstMetric(c.yieldSeconds, prometheus.CounterValue, source.YieldReleaseSeconds, name)
		gauge(c.yieldMax, source.YieldReleaseMaxSeconds, name)
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
