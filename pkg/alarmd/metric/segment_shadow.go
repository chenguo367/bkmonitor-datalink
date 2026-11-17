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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The shadow of the Plans' stable segments (014 B3, first stage): every cell
// of every family exists from the start, so a zero is a reading.
type segmentShadowMetrics struct {
	outcomes, members, pageWrites, pageBytes *prometheus.CounterVec
}

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
				"otherwise than written. mismatch_unmade: the points could not be made. Zero mismatches over " +
				"every period class is the shadow stage's evidence.",
		}, []string{"outcome", "period_class"}),
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
	}
	for _, class := range observability.AllSegmentPeriodClasses() {
		for _, outcome := range observability.AllSegmentShadowOutcomes() {
			metrics.outcomes.WithLabelValues(string(outcome), class)
		}
		for _, event := range observability.AllSegmentShadowEvents() {
			metrics.members.WithLabelValues(string(event), class)
		}
		metrics.pageWrites.WithLabelValues(class)
		metrics.pageBytes.WithLabelValues(class)
	}
	return metrics
}

func (m segmentShadowMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.outcomes, m.members, m.pageWrites, m.pageBytes}
}

func (m segmentShadowMetrics) observe(facts *observability.SegmentShadowFacts) {
	if facts == nil || facts.Empty() {
		return
	}
	for key, count := range facts.Outcomes {
		m.outcomes.WithLabelValues(string(key.Outcome), key.PeriodClass).Add(float64(count))
	}
	for key, count := range facts.Members {
		m.members.WithLabelValues(string(key.Event), key.PeriodClass).Add(float64(count))
	}
	for class, count := range facts.PageWrites {
		m.pageWrites.WithLabelValues(class).Add(float64(count))
		m.pageBytes.WithLabelValues(class).Add(float64(facts.PageBytes[class]))
	}
}
