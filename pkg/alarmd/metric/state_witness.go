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
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// stateWitnessCollector reports the runtime witness cache of the execution
// store bound to it. Nothing charges that cache to a budget, so these two are
// the only place its size is read; a process with no store bound publishes
// neither.
type stateWitnessCollector struct {
	mu                           sync.Mutex
	source                       func() state.RuntimeWitnessStats
	items, bytes                 *prometheus.Desc
	releasedItems, releasedBytes *prometheus.Desc
}

// The two ways a witness leaves the cache untaken, the outcome label of the
// released families: as the window that read its key lands, or as its
// attempt ends.
const (
	stateWitnessReleasedWindowLanded = "window_landed"
	stateWitnessReleasedAttemptEnded = "attempt_ended"
)

func newStateWitnessCollector() *stateWitnessCollector {
	descriptor := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, name), help, nil, nil)
	}
	return &stateWitnessCollector{
		items: descriptor("state_witness_items",
			"Preflight witnesses held in the runtime witness cache, kept from a Slot's load of Runtime State to its "+
				"apply so the write compares without reading the value again. No budget charges the cache. It is "+
				"bounded at 2^18 witnesses and 4096 Query Groups, the least recently filled Query Group evicted "+
				"first; a Slot's witnesses leave as its writes take them, the rest as the window that read their "+
				"keys lands or, left after the last, as the attempt ends (state_witness_released_total), and a "+
				"Query Group's when this worker releases its lease - so between rounds it reads zero, and above "+
				"zero only while an attempt runs: a window's worth, not a round's. Read with "+
				"state_witness_bytes against the resident heap term in the sizing table."),
		bytes: descriptor("state_witness_bytes",
			"The runtime witness cache's estimated heap, a fixed 320 B per witness for its map entry plus the "+
				"digests it alone keeps alive, each rounded to its allocation. No budget charges the cache. It is "+
				"bounded at 2^18 witnesses; a Slot's witnesses leave as each window that read their keys lands, "+
				"the rest as its attempt ends, and a Query Group's when this worker releases its lease, so between "+
				"rounds it reads zero. Read it against the "+
				"resident heap term in the sizing table. It is an estimate from the "+
				"layout, not a measurement, and leaves out the Plan identity every series of a Plan shares."),
		releasedItems: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "state_witness_released_total"),
			"Preflight witnesses the runtime witness cache let go of without a write taking them, by outcome. "+
				"window_landed is counted by window as each window of a Slot written in windows lands: the keys it "+
				"read that none of its writes took - members its round left unwritten, a Plan held back or "+
				"replaced, a mutation admission refused, a series found already applied or superseded - let go of "+
				"then rather than kept to the attempt's end beside the share the Slot's slices are cut by. "+
				"attempt_ended is counted apart, as an attempt ends: what is left then - its last window's keys, a "+
				"window the attempt ended before it landed, a Slot not written in windows. A witness a write takes "+
				"is in neither; one a released lease or the cache's bound drops is in neither. Read with "+
				"state_witness_released_bytes_total.", []string{"outcome"}, nil),
		releasedBytes: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "state_witness_released_bytes_total"),
			"The estimated heap of the witnesses state_witness_released_total counts, by the same outcome, as "+
				"state_witness_bytes counts a witness: window_landed counted by window as each lands, "+
				"attempt_ended apart as each attempt ends.", []string{"outcome"}, nil),
	}
}

// SetStateWitnessSource binds the collector to the live execution store.
func (r *Recorder) SetStateWitnessSource(source func() state.RuntimeWitnessStats) {
	if r == nil || r.phaseTwo.stateWitness == nil {
		return
	}
	r.phaseTwo.stateWitness.mu.Lock()
	r.phaseTwo.stateWitness.source = source
	r.phaseTwo.stateWitness.mu.Unlock()
}

func (c *stateWitnessCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.items
	ch <- c.bytes
	ch <- c.releasedItems
	ch <- c.releasedBytes
}

func (c *stateWitnessCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.source
	c.mu.Unlock()
	if source == nil {
		return
	}
	stats := source()
	ch <- prometheus.MustNewConstMetric(c.items, prometheus.GaugeValue, float64(stats.Items))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes))
	for _, released := range []struct {
		outcome string
		count   state.RuntimeWitnessRelease
	}{{stateWitnessReleasedWindowLanded, stats.Landed}, {stateWitnessReleasedAttemptEnded, stats.Ended}} {
		ch <- prometheus.MustNewConstMetric(c.releasedItems, prometheus.CounterValue, float64(released.count.Items), released.outcome)
		ch <- prometheus.MustNewConstMetric(c.releasedBytes, prometheus.CounterValue, float64(released.count.Bytes), released.outcome)
	}
}
