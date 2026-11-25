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

// WindowedSlotsReading is the Slot attempts a worker applied window by
// window, by how each ended, read at the scrape.
type WindowedSlotsReading struct {
	Completed, Failed, Retried uint64
}

// WindowedSlotsSource reads them at scrape time.
type WindowedSlotsSource func() WindowedSlotsReading

type windowedSlotsCollector struct {
	source WindowedSlotsSource
	total  *prometheus.Desc
}

func newWindowedSlotsCollector(source WindowedSlotsSource) *windowedSlotsCollector {
	return &windowedSlotsCollector{source: source,
		total: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "slot_windowed_total"),
			"Slot attempts this worker applied window by window - one window or more written on the Slot's apply "+
				"goroutine - by how each ended: completed, the Slot completed; retried, the attempt returned no error "+
				"and did not complete, asking for another (an activation or output retry); failed, it returned an "+
				"error. A Slot applied whole is in none of them. The window pipeline's phase seconds "+
				"(slot_apply_wait_seconds_total, slot_land_seconds_total, slot_apply_overlap_seconds_total) are the "+
				"completed ones' alone, so completed is their denominator: apply_wait over completed is a windowed "+
				"Slot's mean wait on its apply goroutine, and a zero apply_wait reads as zero only beside completed "+
				"above zero (015 O1). Counted from the process's start, every cell from the start.",
			[]string{"outcome"}, nil)}
}

func (c *windowedSlotsCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.total
}

func (c *windowedSlotsCollector) Collect(metrics chan<- prometheus.Metric) {
	reading := c.source()
	metrics <- prometheus.MustNewConstMetric(c.total, prometheus.CounterValue, float64(reading.Completed), "completed")
	metrics <- prometheus.MustNewConstMetric(c.total, prometheus.CounterValue, float64(reading.Failed), "failed")
	metrics <- prometheus.MustNewConstMetric(c.total, prometheus.CounterValue, float64(reading.Retried), "retried")
}

// BindWindowedSlots registers the windowed attempts' reading. Bound once, to
// the coordinator that counts them.
func (r *Recorder) BindWindowedSlots(source WindowedSlotsSource) error {
	if r == nil || r.registry == nil {
		return errors.New("metric: initialized recorder is required")
	}
	if source == nil {
		return errors.New("metric: windowed slots source is required")
	}
	r.windowedSlotsMu.Lock()
	defer r.windowedSlotsMu.Unlock()
	if r.windowedSlotsBound {
		return errors.New("metric: windowed slots source is already bound")
	}
	if err := r.registry.Register(newWindowedSlotsCollector(source)); err != nil {
		return err
	}
	r.windowedSlotsBound = true
	return nil
}
