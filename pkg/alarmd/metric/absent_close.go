// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
)

// absentCloseCollector reports the control leader's difference against the
// strategies that no longer exist: what each round decided, and the numbers
// it decided on.
//
// The second family is the point. A zero in the first one means "nothing was
// absent" on one round and "the round refused to decide" on another, and
// those need different people. The denominators say which.
type absentCloseCollector struct {
	mu         sync.Mutex
	outcomes   func() map[string]uint64
	difference func() map[string]int
	outcomeIs  *prometheus.Desc
	sides      *prometheus.Desc
}

// differenceSides is the closed list of denominators, so every one of them
// has a cell from the first scrape.
var differenceSides = []string{"departed", "with_open_alerts", "candidates", "snapshot_strategies",
	"published_strategies", "returned", "unreadable_index"}

func newAbsentCloseCollector() *absentCloseCollector {
	return &absentCloseCollector{
		outcomeIs: prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, "absent_strategy_close_total"),
			"What the control leader's difference against deleted strategies did, by outcome. Counted per "+
				"strategy except alert_closed, metadata_missing, producer_foreign and producer_unknown, which count "+
				"alerts. The refusal words (snapshot_unusable, snapshot_empty, snapshot_stale, snapshot_shrunk, "+
				"difference_too_large) count whole rounds that decided nothing, and none counts the rounds that "+
				"decided; they are the denominator every other reading is read against. Every cell exists from the "+
				"start so a zero is a reading and not an absence.", []string{"outcome"}, nil),
		sides: prometheus.NewDesc(prometheus.BuildFQName(metricNamespace, metricSubsystem, "absent_strategy_difference"),
			"The sizes the last round decided on: departed is the strategies the catalog let go and still "+
				"remembers, with_open_alerts how many of those the alert link still holds an alert for, candidates "+
				"the difference itself, snapshot_strategies and published_strategies what it was judged against, "+
				"returned the departed strategies the source lists again, and unreadable_index the ones whose alert "+
				"index could not be read. A candidates of zero beside a departed of zero is a quiet deployment; "+
				"beside a large departed it is a difference that refused.", []string{"side"}, nil),
	}
}

// SetAbsentCloseSource binds the collector to the loop's counts. Safe before
// or after registration; a nil recorder is a no-op.
func (r *Recorder) SetAbsentCloseSource(outcomes func() map[string]uint64, difference func() map[string]int) {
	if r == nil || r.phaseTwo.absentClose == nil {
		return
	}
	r.phaseTwo.absentClose.mu.Lock()
	r.phaseTwo.absentClose.outcomes = outcomes
	r.phaseTwo.absentClose.difference = difference
	r.phaseTwo.absentClose.mu.Unlock()
}

func (c *absentCloseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.outcomeIs
	ch <- c.sides
}

func (c *absentCloseCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	outcomes, difference := c.outcomes, c.difference
	c.mu.Unlock()
	if outcomes == nil || difference == nil {
		return
	}
	counts := outcomes()
	for _, outcome := range absentalerts.Outcomes {
		ch <- prometheus.MustNewConstMetric(c.outcomeIs, prometheus.CounterValue, float64(counts[outcome]), outcome)
	}
	for _, refusal := range absentalerts.Refusals {
		ch <- prometheus.MustNewConstMetric(c.outcomeIs, prometheus.CounterValue, float64(counts[refusal]), refusal)
	}
	sizes := difference()
	for _, side := range differenceSides {
		ch <- prometheus.MustNewConstMetric(c.sides, prometheus.GaugeValue, float64(sizes[side]), side)
	}
}
