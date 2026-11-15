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

// SliceMemoryReading is the sliced-input memory a worker keeps, read at the
// scrape: how many Query Groups it remembers, how many of those its idle
// sweep cannot reach, and what the memories hold.
type SliceMemoryReading struct {
	// Remembered is every Query Group with a memory; Unswept those of them
	// the sweep cannot drop - not among the Query Groups it walks, or walked
	// with no period known.
	Remembered, Unswept uint64
	// Leaves, LeafSeries and Deferred are summed over the memories: the
	// leaves of each kept shape, the entries of each one's per-slice series
	// and of its deferred slices.
	Leaves, LeafSeries, Deferred uint64
}

// SliceMemorySource reads the sliced-input memory at scrape time.
type SliceMemorySource func() SliceMemoryReading

type sliceMemoryCollector struct {
	source  SliceMemorySource
	groups  *prometheus.Desc
	entries *prometheus.Desc
}

func newSliceMemoryCollector(source SliceMemorySource) *sliceMemoryCollector {
	return &sliceMemoryCollector{
		source: source,
		groups: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "worker_slice_memory_groups"),
			"Query Groups this worker keeps a sliced-input memory for, at the scrape, by scope. remembered is "+
				"every one; unswept is those of them the idle sweep cannot drop, because the sweep walks the "+
				"Query Groups whose Slots started here and skips the ones whose period is not known. unswept "+
				"rising with remembered while the owned Query Groups stand still is memory only a restart "+
				"lets go; flat, the sweep keeps up. Read with worker_slice_memory_entries for what the "+
				"memories hold.",
			[]string{"scope"}, nil),
		entries: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "worker_slice_memory_entries"),
			"What this worker's sliced-input memories hold, at the scrape, summed over them, by kind. leaves "+
				"is the leaves of each kept shape; leaf_series the entries of each one's series by slice; "+
				"deferred the slices each one defers for a timeout. Entries, not bytes: an estimate of what the "+
				"memories retain, which grows with each entry, to read against the live heap.",
			[]string{"kind"}, nil),
	}
}

func (c *sliceMemoryCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.groups
	descriptions <- c.entries
}

func (c *sliceMemoryCollector) Collect(metrics chan<- prometheus.Metric) {
	reading := c.source()
	metrics <- prometheus.MustNewConstMetric(c.groups, prometheus.GaugeValue, float64(reading.Remembered), "remembered")
	metrics <- prometheus.MustNewConstMetric(c.groups, prometheus.GaugeValue, float64(reading.Unswept), "unswept")
	metrics <- prometheus.MustNewConstMetric(c.entries, prometheus.GaugeValue, float64(reading.Leaves), "leaves")
	metrics <- prometheus.MustNewConstMetric(c.entries, prometheus.GaugeValue, float64(reading.LeafSeries), "leaf_series")
	metrics <- prometheus.MustNewConstMetric(c.entries, prometheus.GaugeValue, float64(reading.Deferred), "deferred")
}

// BindSliceMemory registers the sliced-input memory reading. Bound once, to
// the coordinator that keeps the memory.
func (r *Recorder) BindSliceMemory(source SliceMemorySource) error {
	if r == nil || r.registry == nil {
		return errors.New("metric: initialized recorder is required")
	}
	if source == nil {
		return errors.New("metric: slice memory source is required")
	}
	r.sliceMemoryMu.Lock()
	defer r.sliceMemoryMu.Unlock()
	if r.sliceMemoryBound {
		return errors.New("metric: slice memory source is already bound")
	}
	if err := r.registry.Register(newSliceMemoryCollector(source)); err != nil {
		return err
	}
	r.sliceMemoryBound = true
	return nil
}
