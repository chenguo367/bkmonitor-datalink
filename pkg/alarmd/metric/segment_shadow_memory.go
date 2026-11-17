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
)

// SegmentShadowMemoryReading is the books the shadow of the Plans' stable
// segments keeps in process, read at the scrape: the Plans with one, their
// open and closed entries and the bytes they are estimated to hold.
type SegmentShadowMemoryReading struct {
	Plans, Open, Closed, Bytes uint64
}

// SegmentShadowMemorySource reads the shadow's books at scrape time.
type SegmentShadowMemorySource func() SegmentShadowMemoryReading

type segmentShadowMemoryCollector struct {
	source SegmentShadowMemorySource
	memory *prometheus.Desc
}

func (c *segmentShadowMemoryCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.memory
}

func (c *segmentShadowMemoryCollector) Collect(metrics chan<- prometheus.Metric) {
	reading := c.source()
	metrics <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(reading.Plans), "plans")
	metrics <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(reading.Open), "entries_open")
	metrics <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(reading.Closed), "entries_closed")
	metrics <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(reading.Bytes), "bytes_estimated")
}

// BindSegmentShadowMemory registers the shadow's books reading. Bound once,
// to the coordinator that keeps them.
func (r *Recorder) BindSegmentShadowMemory(source SegmentShadowMemorySource) error {
	if r == nil || r.registry == nil {
		return errors.New("metric: initialized recorder is required")
	}
	if source == nil {
		return errors.New("metric: segment shadow memory source is required")
	}
	r.segmentShadowMemoryMu.Lock()
	defer r.segmentShadowMemoryMu.Unlock()
	if r.segmentShadowMemoryBound {
		return errors.New("metric: segment shadow memory source is already bound")
	}
	collector := &segmentShadowMemoryCollector{source: source, memory: prometheus.NewDesc(
		prometheus.BuildFQName("", metricSubsystem, "worker_segment_shadow_memory"),
		"The books the shadow of the Plans' stable segments keeps in process on this worker (014 B3, first "+
			"stage), at the scrape, by kind: plans, the Plans with a book; entries_open and entries_closed, "+
			"their members by whether the segment is open, as each Plan's last fold left them; "+
			"bytes_estimated, entries at about 160 bytes each - the digest as the key and in the entry, its "+
			"two times, the map's share - and a Plan's book at 256: an estimate, to read against the live heap. "+
			"plans rising with the owned Query Groups standing still is books no round folds; entries_closed "+
			"rising, closed members that do not expire.",
		[]string{"kind"}, nil)}
	if err := r.registry.Register(collector); err != nil {
		return err
	}
	r.segmentShadowMemoryBound = true
	return nil
}
