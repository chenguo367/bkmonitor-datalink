// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package metric

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// canonicalEncodingCollector reads the shared canonical encoder at scrape
// time: how many encodings the single-pass form answered and how many it
// handed back to the established path, and how the series delivery digests
// assembled from their shared parts compared with the generic digest on the
// one in 4096 that is checked.
type canonicalEncodingCollector struct {
	calls       *prometheus.Desc
	records     *prometheus.Desc
	divergences *prometheus.Desc
	checked     *prometheus.Desc
}

func newCanonicalEncodingCollector() *canonicalEncodingCollector {
	name := func(suffix string) string {
		return prometheus.BuildFQName("", metricSubsystem, "canonical_encoding_"+suffix)
	}
	return &canonicalEncodingCollector{
		calls: prometheus.NewDesc(name("calls_total"),
			"Canonical encodings by what answered them: planned, a closed type's value written from its type's plan; "+
				"served, the single-pass form; or declined, by it back to the established path.",
			[]string{"outcome"}, nil),
		records: prometheus.NewDesc(name("records_shadow_total"),
			"Series delivery digests assembled from a series' shared parts that were checked against the "+
				"generic canonical digest, by result: agreed, or differed. A difference returns the generic "+
				"digest, so it is a finding and not a wrong answer; one in 4096 assemblies is checked.",
			[]string{"outcome"}, nil),
		divergences: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "canonical_encoder_divergences_total"),
			"Calls of a direct encoder of the canonical form whose bytes differed from the canonical encoding's, by "+
				"encoder: closed_type, a closed type's plan; dimension_fields, a dimension identity's fields; "+
				"record_parts, a series' shared record parts; scalar, a number or literal a query answered with. One "+
				"call in 1024 of each is checked (record_parts: one series digest in 4096). Should be 0. Not 0 is an "+
				"encoder that gave some input other bytes than the canonical encoding does: this process has turned "+
				"it off and takes the canonical encoding since, and the digests it gave before - State mutation "+
				"digests, the skip's decisions, event identities - may be wrong. The canonical_encoder line names the "+
				"encoder, the Go type, what was compared and the field path at the first byte that differs: the "+
				"encoder is fixed there. A dimension_fields or scalar difference is named, field and all, on that "+
				"encoder's own line; one in record_parts' own map and string writes is said only on the record_parts "+
				"line, which compares digests (compared=digest): the series digest differs as a whole, no path.",
			[]string{"encoder"}, nil),
		checked: prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, "canonical_encoder_checked_total"),
			"Calls of a direct encoder of the canonical form checked against the canonical encoding, by encoder (as "+
				"canonical_encoder_divergences_total): what that count is out of.",
			[]string{"encoder"}, nil),
	}
}

func (c *canonicalEncodingCollector) Describe(out chan<- *prometheus.Desc) {
	out <- c.calls
	out <- c.records
	out <- c.divergences
	out <- c.checked
}

func (c *canonicalEncodingCollector) Collect(out chan<- prometheus.Metric) {
	served, declined := contract.CanonicalStreamCounts()
	out <- prometheus.MustNewConstMetric(c.calls, prometheus.CounterValue, float64(served), "served")
	out <- prometheus.MustNewConstMetric(c.calls, prometheus.CounterValue, float64(declined), "declined")
	compared, differed := contract.ReadRecordsDigestShadowCounts()
	out <- prometheus.MustNewConstMetric(c.records, prometheus.CounterValue, float64(compared-differed), "agreed")
	out <- prometheus.MustNewConstMetric(c.records, prometheus.CounterValue, float64(differed), "differed")
	out <- prometheus.MustNewConstMetric(c.calls, prometheus.CounterValue, float64(contract.CanonicalPlanCounts()), "planned")
	counts := contract.CanonicalEncoderCounts()
	for _, encoder := range contract.CanonicalEncoders() {
		out <- prometheus.MustNewConstMetric(c.checked, prometheus.CounterValue, float64(counts[encoder][0]), encoder)
		out <- prometheus.MustNewConstMetric(c.divergences, prometheus.CounterValue, float64(counts[encoder][1]), encoder)
	}
}
