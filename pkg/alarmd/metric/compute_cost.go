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

// ComputeCostBands are the bands an owned Query Group's compute cost is
// counted in, closed: none, no rate derived yet - no Slot here, or none
// measured - or none spent; then by its evaluation and State wall per second
// of schedule, the cost the heartbeat reports it by (CostPerSecondMilli,
// whose unit is a millisecond a second).
var ComputeCostBands = []string{"none", "lt_10ms", "lt_100ms", "lt_1s", "ge_1s"}

// ComputeCostBand is the band of a cost in thousandths of a second of compute
// per second of schedule.
func ComputeCostBand(milli uint64) string {
	switch {
	case milli == 0:
		return "none"
	case milli < 10:
		return "lt_10ms"
	case milli < 100:
		return "lt_100ms"
	case milli < 1000:
		return "lt_1s"
	default:
		return "ge_1s"
	}
}

// ComputeCostReading is this worker's owned Query Groups' compute cost at its
// last heartbeat: by band, how many and their summed cost in seconds a second.
type ComputeCostReading struct {
	Groups  map[string]uint64
	Seconds map[string]float64
}

// ComputeCostSource reads it at the scrape.
type ComputeCostSource func() ComputeCostReading

type computeCostCollector struct {
	source         ComputeCostSource
	seconds, count *prometheus.Desc
}

func (c *computeCostCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.seconds
	descriptions <- c.count
}

func (c *computeCostCollector) Collect(metrics chan<- prometheus.Metric) {
	reading := c.source()
	for _, band := range ComputeCostBands {
		metrics <- prometheus.MustNewConstMetric(c.seconds, prometheus.GaugeValue, reading.Seconds[band], band)
		metrics <- prometheus.MustNewConstMetric(c.count, prometheus.GaugeValue, float64(reading.Groups[band]), band)
	}
}

// BindComputeCost registers the owned Query Groups' compute cost, read at the
// scrape. Bound once, to the worker's cost source.
func (r *Recorder) BindComputeCost(source ComputeCostSource) error {
	if r == nil || r.registry == nil {
		return errors.New("metric: initialized recorder is required")
	}
	if source == nil {
		return errors.New("metric: compute cost source is required")
	}
	r.computeCostMu.Lock()
	defer r.computeCostMu.Unlock()
	if r.computeCostBound {
		return errors.New("metric: compute cost source is already bound")
	}
	collector := &computeCostCollector{source: source,
		seconds: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "worker_compute_cost"),
			"The Query Groups this worker owns, their compute cost summed by band: evaluation and State wall "+
				"per second of schedule, smoothed, as each heartbeat reports a Query Group's cost to the Leader "+
				"(CostPerSecondMilli; query wall, which is the backend waiting, is not in it). In seconds a "+
				"second; the bands' sum is the worker's whole, read against its CPU to judge the cost as a "+
				"placement weight (014 C1). Bands: none (no rate yet: no Slot measured here), lt_10ms, lt_100ms, "+
				"lt_1s, ge_1s, by the Query Group's own cost. As of the last heartbeat, ten seconds.",
			[]string{"band"}, nil),
		count: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "worker_compute_cost_query_groups"),
			"The Query Groups this worker owns, counted by the band of their compute cost "+
				"(worker_compute_cost): how many carry the whole, and how many have no rate yet.",
			[]string{"band"}, nil)}
	if err := r.registry.Register(collector); err != nil {
		return err
	}
	r.computeCostBound = true
	return nil
}
