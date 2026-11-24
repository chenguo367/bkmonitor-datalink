// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"slices"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The shadow of the Plans' stable segments (014 B3, first stage): every cell
// of every family exists from the start, so a zero is a reading.
type segmentShadowMetrics struct {
	outcomes, members, pageWrites, pageBytes, skipped, renewals, skipFlagIgnored, samples, degraded, notSkipped, rounds, carries *prometheus.CounterVec
	renewScript                                                                                                                  prometheus.Histogram
}

// segmentRenewScriptBuckets are dense about the renewal script's 2.5 ms
// budget (segment.RenewScriptBudget), where the over_budget bar is read; 2.2
// ms is the budget less the slow log's margin over the script's own clock,
// rounded down to a bucket, the bound the slow log's share is read from.
var segmentRenewScriptBuckets = []float64{0.0005, 0.001, 0.0015, 0.002, 0.0022, 0.00225, 0.0024, 0.0025, 0.005}

func newSegmentShadowMetrics() segmentShadowMetrics {
	metrics := segmentShadowMetrics{
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_shadow_total",
			Help: "Open members of a Plan's stable segments present in a round, as the shadow kept in process " +
				"compared them - every State still written - by outcome and the Plan's period class (lt_1m, 1m, " +
				"le_5m, le_1h, le_1d, gt_1d). match: the points the segment makes and the Level state it reads " +
				"as are what the round wrote. exception_*: a member the skip would have written whole and taken " +
				"off its page, by the first reason - events (it sent events), unsteady (not decided steady and " +
				"normal with its stored Levels unchanged), level (a Level not FULL), off_grid (its points off the " +
				"mark's grid), returned (a closed member back), short_of_mark (its last point short of the " +
				"round's mark): read exception_events for whether quiet members send events. mismatch_*: a member the skip would have left " +
				"unwritten, once each field the made read differs in from the write: a skip would read it " +
				"otherwise than written. mismatch_unmade: the points could not be made. round: sampled, the " +
				"Plan's piece's sampled round of the day (014 B3 9.2), every member written whole and compared; " +
				"normal, any other. Zero mismatches over every period class, read as sum without(round) - series " +
				"from a process before the label carry none and sum the same - is the shadow stage's evidence; " +
				"with members skipped, mismatch_* in round=\"sampled\" is the third stage's alarm reading.",
		}, []string{"outcome", "period_class", "round"}),
		members: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_shadow_members_total",
			Help: "What the shadow of a Plan's stable segments did with its pages' membership, by event and the " +
				"Plan's period class. joined: a series written and decided steady and normal at the mark. " +
				"departed: an open member absent from the round, closed at the mark before. expired: a closed " +
				"member past its window and its key's life. reset: a Plan's shadow started over, its members " +
				"counted - a retry of a Slot an earlier attempt had written some series of leaves the round's " +
				"membership unknown.",
		}, []string{"event", "period_class"}),
		pageWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_shadow_page_writes_total",
			Help: "Member pages a round of the shadow would have rewritten - a join, a close, a removal or an " +
				"expiry on them, or their count changed - by the Plan's period class: the page write the skip " +
				"adds, read beside state_segment_shadow_page_bytes_total.",
		}, []string{"period_class"}),
		pageBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_shadow_page_bytes_total",
			Help: "Bytes of the member pages a round of the shadow would have rewritten, each page encoded whole " +
				"as it would be written, by the Plan's period class.",
		}, []string{"period_class"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_shadow_skipped_total",
			Help: "Slots run without the shadow of their Plans' stable segments, their State written as ever, by " +
				"reason: read_failed (the Plans' segments could not be read: state_segment_read_total" +
				"{reason=\"failed\"} counts the Plans), book_refused (the retained pool did not take the books " +
				"the Slot read, reserved as its State is, or the members a Plan's round would join, who then join " +
				"a later round; refused quietly, the Slot itself goes on).",
		}, []string{"reason"}),
		renewals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_renewal_total",
			Help: "The renewals of a Plan's member pages due - a quarter of the shorter of the lifetime their last " +
				"renewal gave and this round's since it (014 B3 3.5) - by result: renewed and failed, pages whose " +
				"scripts all answered or not (a failed page's renewal time stays, and it is renewed whole next " +
				"round); missing, member State keys a renewal found gone (not made again: they read " +
				"MISSING_WARMING); over_budget, renewal scripts of at most 256 keys that ran past 2.5 ms by the " +
				"server's clock; fallback_write, pages unrenewed for half their lifetime, whose members are written " +
				"whole - in the second stage counted only, every State being written; fallback_unrenewable, the same of " +
				"a page no earlier round could have renewed - unrenewed for less than its Plan's interval past the " +
				"quarter lifetime it falls due by - by design: a lifetime of two intervals or less (a Plan keeping one " +
				"point, or one its horizon cuts short), or a new page, made staggered, of a Plan whose keys live under " +
				"four intervals; fallback_stalled, the same of a Plan whose rounds stopped - the Slot's evaluation time " +
				"past its mark's round by more than its interval, or the round a retry or a replay late for its time (a " +
				"replay short of time is the normal round in its place) - whatever its " +
				"renewals did. " +
				"missing and fallback_write zero, and over_budget at most 1e-4 of the state_segment_renew scripts " +
				"that ran (redis_script_total{script=\"state_segment_renew\",command=~\"eval|evalsha\",outcome=\"ok\"}, " +
				"one per script, pipeline members included, over the same window), over " +
				"every lifetime tier is the third stage's bar: over_budget is the script's own clock and a lower bound, " +
				"the server's slow log the reading of its tail; fallback_unrenewable and fallback_stalled are not read in it.",
		}, []string{"result"}),
		skipFlagIgnored: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_skip_flag_ignored_total",
			Help: "Slots dispatched with the Assignment's segment skip word (014 B3 stage 3) that did not follow it, " +
				"their stable members written whole, by reason: epoch_mismatch (the word names a Leader other than " +
				"the one whose view the worker has installed - set before a Leader change and not yet restated, or " +
				"left by a Leader a rollback replaced), no_view (the worker has installed no view). Nonzero for a " +
				"short while after a Leader change is expected; growing without one is a word nobody restates.",
		}, []string{"reason"}),
		samples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_sample_total",
			Help: "Plans' rounds that were their piece's sampled round of the day (014 B3 9.2), whether the round " +
				"skips or not, by the Plan's period class: about one a day for each piece of each Plan, every round " +
				"for a Plan run once a day or less often. In a sampled round no member is left unwritten - every " +
				"one is written whole and compared, its outcomes under state_segment_shadow_total" +
				"{round=\"sampled\"}. Flat while Plans run is a sampled round that never comes.",
		}, []string{"period_class"}),
		degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_degraded_total",
			Help: "Plans whose stable segments a Slot could not use as written (014 B3 9.7) - their members read as " +
				"stored, every one written whole, the book started over - by reason: unread (the segments, or the " +
				"generation's carried from, did not read in a retry of a Slot dispatched with a segment skip word, its " +
				"first attempt failed SEGMENT_READ_FAILED), book_refused (the pool did not take the books in such a " +
				"retry), undecodable (the stored segments did not decode), write_absent (the round's write was not " +
				"made); a Plan once a Slot. stopped: a Plan degraded a second time in its owner's term, its members " +
				"no longer left unwritten on this replica until the owner changes - plan_segments.last skip_mode " +
				"\"stopped\" names it. Zero is the reading; undecodable or stopped growing is segments being lost.",
		}, []string{"reason"}),
		carries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_carry_total",
			Help: "Reads of the stable segments of the State generation a Plan carries from - a Plan edited to a new " +
				"generation reads a series with no record of its own under the old one, and extends it by the old " +
				"book - a Plan once each load of a Slot's segments, a retry's and a supplement's too, by what they " +
				"read as: carried (the book read), absent (no book under the " +
				"old generation), undecodable (it did not decode: a page of it gone, the old generation's pages no " +
				"longer renewed since the edit, 014 B4 H8 - its Plan is degraded undecodable, its members read as " +
				"stored, GAPPED or WARMING, never normal), read_failed (the read did not answer). Zero without edits; " +
				"undecodable after one is how often H8 happens.",
		}, []string{"result"}),
		notSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_match_not_skipped_total",
			Help: "Open members of a Plan's stable segments the shadow compared as a match and wrote all the same, by " +
				"the first gate that kept them written and the Plan's period class. The gates, in the order the round " +
				"checks them: slot_skip_off (the Slot follows no segment skip word), write_failed (an earlier attempt's " +
				"segment write did not land), not_normal_operation (the Slot is neither a normal one nor a replay short of " +
				"time - one sent before its deadline for a read that would not fit, the normal round in its place), " +
				"plan_stopped (the " +
				"Plan degraded twice in its owner's term), sampled (the Plan's sampled round of the day), ttl_zero (the " +
				"Plan's segments have no lifetime to be written with), fallback_page (the member's page went unrenewed " +
				"for half its lifetime). other is none of these and a defect: a match is written only for one of them. " +
				"With state_series_not_written_total{reason=\"segment_member_skipped\"} it adds up to " +
				"state_segment_shadow_total{outcome=\"match\"} summed over round, a replayed round aside.",
		}, []string{"gate", "period_class"}),
		rounds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "state_segment_rounds_total",
			Help: "Plans' rounds of Slots whose output landed, by the skip mode each ran under and the Plan's period " +
				"class. The modes: off (the Slot follows no segment skip word), write_failed, operation (neither a normal " +
				"Slot nor a replay short of time), stopped, retried (an attempt that found the Plan's series applied in part and started its shadow " +
				"over: it compares nothing), sampled - every member written whole - and skip, members compared a match " +
				"left unwritten: the rounds state_segment_match_not_skipped_total and the members left are read against. " +
				"Counted by the attempts that fold, as the matches are; a replay of a round already folded is none.",
		}, []string{"mode", "period_class"}),
	}
	metrics.renewScript = prometheus.NewHistogram(prometheus.HistogramOpts{
		Subsystem: metricSubsystem, Name: "state_segment_renew_script_seconds",
		Help: "How long each state_segment_renew script that renewed its keys ran, by the server's clock as the " +
			"script measures it, from its first TIME to its last: the same reading over_budget counts past 2.5 ms " +
			"(state_segment_renewal_total), with no Redis call of its own. A script a stale fence refused ran no " +
			"renewal and is not counted, so the count is the scripts that ran less those refusals. The script's " +
			"clock misses what the server spends before its first line and after its last: against the server's " +
			"slow log for the same 11,200 scripts of 1 to 256 keys on Redis 5.0.9, the slow log was longer by " +
			"256 us at p99.99 and 262 us at most (delta_max). A script the slow log puts past 2.5 ms ran past " +
			"2.5 ms less delta_max, 2.238 ms, by its own clock, so the share of scripts past the 0.0022 bucket " +
			"bounds from above the share past 2.5 ms the slow log would show. " +
			"The 0.002 bucket is the conservative reading: it holds for a margin up to 500 us, twice the one " +
			"measured on an idle host, for a server whose core is slower. " +
			"Both shares within the bar is the bar met; the 0.0022 share alone within it is met per the " +
			"calibration, with less than twofold headroom.",
		Buckets: segmentRenewScriptBuckets,
	})
	for _, reason := range observability.AllSegmentDegradations() {
		metrics.degraded.WithLabelValues(reason)
	}
	for _, result := range observability.AllSegmentCarries() {
		metrics.carries.WithLabelValues(result)
	}
	for _, reason := range observability.AllSegmentSkipIgnoredReasons() {
		metrics.skipFlagIgnored.WithLabelValues(reason)
	}
	for _, result := range observability.AllSegmentRenewalResults() {
		metrics.renewals.WithLabelValues(result)
	}
	for _, reason := range observability.AllSegmentShadowSkips() {
		metrics.skipped.WithLabelValues(reason)
	}
	for _, class := range observability.AllSegmentPeriodClasses() {
		for _, outcome := range observability.AllSegmentShadowOutcomes() {
			for _, round := range observability.AllSegmentShadowRounds() {
				metrics.outcomes.WithLabelValues(string(outcome), class, round)
			}
		}
		metrics.samples.WithLabelValues(class)
		for _, event := range observability.AllSegmentShadowEvents() {
			metrics.members.WithLabelValues(string(event), class)
		}
		metrics.pageWrites.WithLabelValues(class)
		metrics.pageBytes.WithLabelValues(class)
		for _, gate := range observability.AllSegmentNotSkippedGates() {
			metrics.notSkipped.WithLabelValues(gate, class)
		}
		for _, mode := range segment.SkipModes() {
			metrics.rounds.WithLabelValues(mode, class)
		}
	}
	return metrics
}

func (m segmentShadowMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.outcomes, m.members, m.pageWrites, m.pageBytes, m.skipped, m.renewals, m.skipFlagIgnored, m.samples,
		m.degraded, m.notSkipped, m.rounds, m.renewScript, m.carries}
}

func (m segmentShadowMetrics) observe(facts *observability.SegmentShadowFacts) {
	if facts == nil || facts.Empty() {
		return
	}
	for key, count := range facts.Outcomes {
		m.outcomes.WithLabelValues(string(key.Outcome), key.PeriodClass, key.Round).Add(float64(count))
	}
	for key, count := range facts.Members {
		m.members.WithLabelValues(string(key.Event), key.PeriodClass).Add(float64(count))
	}
	for class, count := range facts.PageWrites {
		m.pageWrites.WithLabelValues(class).Add(float64(count))
		m.pageBytes.WithLabelValues(class).Add(float64(facts.PageBytes[class]))
	}
	for reason, count := range facts.Skipped {
		m.skipped.WithLabelValues(reason).Add(float64(count))
	}
	for result, count := range facts.Renewals {
		m.renewals.WithLabelValues(result).Add(float64(count))
	}
	for _, elapsed := range facts.RenewScripts {
		m.renewScript.Observe(elapsed.Seconds())
	}
	for reason, count := range facts.SkipFlagIgnored {
		m.skipFlagIgnored.WithLabelValues(reason).Add(float64(count))
	}
	for class, count := range facts.Samples {
		m.samples.WithLabelValues(class).Add(float64(count))
	}
	for reason, count := range facts.Degraded {
		m.degraded.WithLabelValues(reason).Add(float64(count))
	}
	for result, count := range facts.Carries {
		m.carries.WithLabelValues(result).Add(float64(count))
	}
	for key, count := range facts.NotSkipped {
		m.notSkipped.WithLabelValues(key.Gate, key.PeriodClass).Add(float64(count))
	}
	modes := segment.SkipModes()
	for key, count := range facts.Rounds {
		// The round's mode is one of segment.SkipModes by construction
		// (segmentShadowRound.skipMode); a word outside them is not given a
		// cell no reader expects.
		if slices.Contains(modes, key.Mode) {
			m.rounds.WithLabelValues(key.Mode, key.PeriodClass).Add(float64(count))
		}
	}
}
