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
	"errors"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// SegmentStoreSource reads the Plans' segments' counts and what the State
// cache holds of them, at the scrape.
type SegmentStoreSource func() state.SegmentStats

// segmentStoreCollector reports the Plans' stable segments (014 B3, second
// stage) from the execution store: every cell of every closed vocabulary from
// the start, so a zero is a reading.
type segmentStoreCollector struct {
	source                      SegmentStoreSource
	reads, writes, drift, cache *prometheus.Desc
}

func (c *segmentStoreCollector) Describe(descriptions chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.reads, c.writes, c.drift, c.cache} {
		descriptions <- desc
	}
}

func (c *segmentStoreCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := c.source()
	metrics <- prometheus.MustNewConstMetric(c.reads, prometheus.CounterValue,
		float64(stats.Reads[[2]string{segment.SourceCache, segment.ReadHit}]), segment.SourceCache, segment.ReadHit)
	for _, reason := range segment.ReadReasons {
		metrics <- prometheus.MustNewConstMetric(c.reads, prometheus.CounterValue,
			float64(stats.Reads[[2]string{segment.SourceRedis, reason}]), segment.SourceRedis, reason)
	}
	for _, result := range segment.WriteResults {
		metrics <- prometheus.MustNewConstMetric(c.writes, prometheus.CounterValue, float64(stats.Writes[result]), result)
	}
	metrics <- prometheus.MustNewConstMetric(c.drift, prometheus.CounterValue, float64(stats.Drift))
	metrics <- prometheus.MustNewConstMetric(c.cache, prometheus.GaugeValue, float64(stats.Plans), "plans")
	metrics <- prometheus.MustNewConstMetric(c.cache, prometheus.GaugeValue, float64(stats.Pages), "pages")
	metrics <- prometheus.MustNewConstMetric(c.cache, prometheus.GaugeValue, float64(stats.Bytes), "bytes")
}

// BindSegmentStore registers the segments' families, read from the
// execution store at the scrape. Bound once.
func (r *Recorder) BindSegmentStore(source SegmentStoreSource) error {
	if r == nil || r.registry == nil {
		return errors.New("metric: initialized recorder is required")
	}
	if source == nil {
		return errors.New("metric: segment store source is required")
	}
	r.segmentStoreMu.Lock()
	defer r.segmentStoreMu.Unlock()
	if r.segmentStoreBound {
		return errors.New("metric: segment store source is already bound")
	}
	descriptor := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, name), help, labels, nil)
	}
	collector := &segmentStoreCollector{source: source,
		reads: descriptor("state_segment_read_total",
			"A Plan's stable segments (014 B3: its mark and member pages) read once a Slot before its State, a "+
				"Plan a read, by source and reason: cache/hit, served from the State cache under the Slot's owner "+
				"fence; redis and why - cold (nothing kept: a first read, a restart, a cache off), fence_changed "+
				"(kept under another fence), evicted (let go for the cache's budget, the pool's room or its life), "+
				"unknown_result (a write of it did not apply whole: in doubt, refused, or refused by the fence after "+
				"its first chunk landed), renew_missing (a write or renewal found one of "+
				"its keys gone), failed (the read failed and the Slot ran without its shadow). Steady, every read is "+
				"a hit; the cold ones of a Slot cost two round trips a storage target.", "source", "reason"),
		writes: descriptor("state_segment_write_total",
			"A Plan's round of its segments written before its Progress - the pages it changed, then the mark, "+
				"the pages it did not carry renewed - a Plan a write, by result: applied, stale_owner and "+
				"content_moved (the owner fence refused a chunk of it: the first, nothing written, or a later one, "+
				"the earlier landed), failed (refused otherwise or not sent), unknown (its outcome not known). In "+
				"the second stage a write that did not apply is counted and lets the Plan's cache go; the Slot goes "+
				"on, its State written whole.", "result"),
		drift: descriptor("state_segment_cache_drift_total",
			"Segment keys a read from Redis under the fence the State cache kept them under found other than "+
				"kept - other bytes, or gone with no write or renewal saying so - beyond what a write in doubt tried "+
				"to leave. The cache's claim that its owner is the keys' only writer, read against the store: zero "+
				"is the second stage's bar."),
		cache: descriptor("worker_segment_shadow_memory",
			"What the State cache holds of the Plans' stable segments on this worker, at the scrape, by kind: "+
				"plans, the marks; pages, the member pages; bytes, their charge (value and key at their size "+
				"classes plus the entry overhead), a share of state_cache_bytes under its budget. A Plan's book "+
				"is decoded from these for the Slot that reads it and not kept past it.", "kind"),
	}
	if err := r.registry.Register(collector); err != nil {
		return err
	}
	r.segmentStoreBound = true
	return nil
}
