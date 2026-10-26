// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// recoveryRecordCollector reports the fleet's record of the recoveries
// alarmd sent, as this process uses it.
type recoveryRecordCollector struct {
	mu     sync.Mutex
	counts func() map[string]uint64
	desc   *prometheus.Desc
}

func newRecoveryRecordCollector() *recoveryRecordCollector {
	return &recoveryRecordCollector{desc: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "recovery_record_total"),
		"The fleet's record of the reopens alarmd sent - an ABNORMAL acknowledged after a RECOVERY of the same "+
			"fingerprint - which holds back a close decided from the link's evidence when a recovery of its "+
			"fingerprint was sent after that evidence was read and an ABNORMAL after that (L40), as this process "+
			"used it: noted counts reopens the sink acknowledged and the record took, note_failed reopens it could "+
			"not take - a close decided after one of them is not held back by it - read_failed reads of the record "+
			"that failed, each holding back every close it was asked about (recovery_unknown in the close's own "+
			"outcomes), and forgotten recoveries this process let go of, past its bound, before an ABNORMAL could "+
			"follow them - a reopen after one is not recorded. Every cell exists from the start so a zero is a "+
			"reading and not an absence.",
		[]string{"outcome"}, nil)}
}

// SetRecoveryRecordSource binds the collector to the record's counts. Safe
// before or after registration; a nil recorder is a no-op.
func (r *Recorder) SetRecoveryRecordSource(counts func() map[string]uint64) {
	if r == nil || r.phaseTwo.recoveryRecord == nil {
		return
	}
	r.phaseTwo.recoveryRecord.mu.Lock()
	r.phaseTwo.recoveryRecord.counts = counts
	r.phaseTwo.recoveryRecord.mu.Unlock()
}

func (c *recoveryRecordCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *recoveryRecordCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.counts
	c.mu.Unlock()
	if source == nil {
		return
	}
	counts := source()
	for _, outcome := range openalerts.RecoveryRecordOutcomes {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, float64(counts[outcome]), outcome)
	}
}
