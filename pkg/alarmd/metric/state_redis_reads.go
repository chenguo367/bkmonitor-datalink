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

// stateRedisReadPurposes is what each purpose of the State store's reads
// counts, for the families' HELP.
const stateRedisReadPurposes = "runtime (a Slot's State preflight: the keys the State cache did not serve), runtime_verify " +
	"(a preflight batch carrying keys a Plan's sampled round reads back to check against what the cache served; its keys " +
	"are those checked, the batch's others counted under runtime), carry (the preflight's second pass: the previous " +
	"generation's key of each series an edited Plan carries), gap_load (a Slot's read of each due Plan's gap key, one " +
	"key a read), gap_apply (the read of a gap key before it is written), sequential (the read of a State key a write goes " +
	"key by key for: no preflight witness, or a key twice in one request), segment_mark and segment_page (a Slot's reads of " +
	"its Plans' stable segment marks and member pages the cache did not serve), plan_segments (the Plans' marks read for " +
	"an object page, outside any Slot), other (a purpose this build does not name, which should never be: non-zero is a " +
	"defect)"

// stateRedisReadsCollector reports the State store's MGETs by purpose
// (014 next9 1): a Slot's MGETs were about 2.25 times its admissions, and
// which reads they were could not be told apart. A process with no store
// bound publishes neither family.
type stateRedisReadsCollector struct {
	mu          sync.Mutex
	source      func() state.StateReadStats
	calls, keys *prometheus.Desc
}

func newStateRedisReadsCollector() *stateRedisReadsCollector {
	descriptor := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, name), help, []string{"command", "purpose"}, nil)
	}
	return &stateRedisReadsCollector{
		calls: descriptor("state_redis_reads_total",
			"Reads the State store sent to Redis, by command - mget, every one of them - and purpose, each call that "+
				"reached the backend, answered or not: "+stateRedisReadPurposes+". A Slot's keys of a purpose are "+
				"state_redis_read_keys_total of it over the Slots. Not counted: the MGET the State batch write's script "+
				"sends inside Redis, one each batch it writes, so Redis's own mget count is these reads and the "+
				"state_fenced_batch_write script's calls together. Every cell exists from the start."),
		keys: descriptor("state_redis_read_keys_total",
			"Keys the State store's reads asked Redis for, by command and purpose as state_redis_reads_total counts "+
				"the reads: "+stateRedisReadPurposes+". Every cell exists from the start."),
	}
}

// SetStateRedisReadsSource binds the collector to the live execution store.
func (r *Recorder) SetStateRedisReadsSource(source func() state.StateReadStats) {
	if r == nil || r.phaseTwo.stateRedisReads == nil {
		return
	}
	r.phaseTwo.stateRedisReads.mu.Lock()
	r.phaseTwo.stateRedisReads.source = source
	r.phaseTwo.stateRedisReads.mu.Unlock()
}

func (c *stateRedisReadsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.calls
	ch <- c.keys
}

func (c *stateRedisReadsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.source
	c.mu.Unlock()
	if source == nil {
		return
	}
	stats := source()
	for _, purpose := range state.StateReadPurposes {
		ch <- prometheus.MustNewConstMetric(c.calls, prometheus.CounterValue, float64(stats.Calls[purpose]), "mget", purpose)
		ch <- prometheus.MustNewConstMetric(c.keys, prometheus.CounterValue, float64(stats.Keys[purpose]), "mget", purpose)
	}
}
