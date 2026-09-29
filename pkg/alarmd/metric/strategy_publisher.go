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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// strategyPublisherCollector reports the strategy cache publisher's record of
// its own runs, as this process read it beside the active set: which
// publisher and version it says it is, and what its runs said. Read at
// scrape from the source's own state; nothing is kept here.
type strategyPublisherCollector struct {
	mu      sync.Mutex
	source  controlplane.PublisherReportSource
	info    *prometheus.Desc
	reports *prometheus.Desc
}

func newStrategyPublisherCollector() *strategyPublisherCollector {
	descriptor := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, name), help, labels, nil)
	}
	return &strategyPublisherCollector{
		info: descriptor("strategy_publisher_info",
			"1, labelled with what the strategy cache's publisher last said about itself in its record beside "+
				"the active set: state provided, not_provided (the publisher leaves no record, which is not an "+
				"error) or unreadable, and when provided the writer and version it names, each at most 64 "+
				"bytes. The publisher's word, best effort: what it runs is read from the platform side. One "+
				"series; absent on a process that has not read the active set, which every follower is.",
			"state", "writer", "version"),
		reports: descriptor("strategy_publisher_reports_total",
			"Distinct publisher records this process read, by the outcome the record states (published, "+
				"blocked, failed, other) and its reason. One record is read on every read of the active set "+
				"until the publisher writes the next and counted once, so the rate is the publisher's runs as "+
				"this process saw them: a publisher that stopped writing reads as a flat line, and one that "+
				"blocks every run as a rising blocked series with its reason. The reason is the record's own "+
				"word when it is one (an upper-case identifier, at most 16 distinct per process) and other "+
				"otherwise; a failed run's reason is an exception's class name and is read from the fleet "+
				"page, not from here. Empty for a record without one.",
			"outcome", "reason"),
	}
}

func (c *strategyPublisherCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	ch <- c.reports
}

func (c *strategyPublisherCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.source
	c.mu.Unlock()
	if source == nil {
		return
	}
	report, read := source.PublisherReport()
	if !read {
		return
	}
	writer, version := "", ""
	if report.State == controlplane.PublisherReportProvided {
		writer, version = report.Writer, report.Version
	}
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1, report.State, writer, version)
	for _, count := range source.PublisherReportCounts() {
		ch <- prometheus.MustNewConstMetric(c.reports, prometheus.CounterValue, float64(count.Count), count.Outcome, count.Reason)
	}
}

// SetStrategyPublisherSource binds the strategy source whose publisher record
// the collector reports. Until it is bound, and on a source that reads no
// record, the collector emits nothing.
func (r *Recorder) SetStrategyPublisherSource(source controlplane.PublisherReportSource) {
	if r == nil || r.phaseTwo.strategyPublisher == nil {
		return
	}
	r.phaseTwo.strategyPublisher.mu.Lock()
	r.phaseTwo.strategyPublisher.source = source
	r.phaseTwo.strategyPublisher.mu.Unlock()
}
