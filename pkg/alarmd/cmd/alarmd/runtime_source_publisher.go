// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

// sourcePublisherFleetFacts is the strategy source's publisher record as this
// process's control rounds last read it, for the fleet snapshot. Nil on a
// source that does not read one, on a process whose rounds have not read the
// active set - every follower - and once the last read is older than
// controlplane.SourceStalenessBound: a leader that handed over keeps its last
// record, and it is not the publisher's present word.
func sourcePublisherFleetFacts(source controlplane.StrategySource, now func() time.Time) func() *fleet.SourcePublisherReport {
	reader, ok := source.(controlplane.PublisherReportSource)
	if !ok {
		return nil
	}
	return func() *fleet.SourcePublisherReport {
		report, read := reader.PublisherReport()
		if !read || !controlplane.PublisherReportCurrent(report.ReadAt, now()) {
			return nil
		}
		return sourcePublisherFactsOf(report)
	}
}

func sourcePublisherFactsOf(report controlplane.PublisherReport) *fleet.SourcePublisherReport {
	facts := &fleet.SourcePublisherReport{State: report.State, Label: fleet.SourcePublisherLabels[report.State],
		Detail: report.Detail, ReadAt: report.ReadAt, Writer: report.Writer, Version: report.Version,
		Outcome: report.Outcome, Reason: report.Reason, Strategies: report.Strategies, Rejected: report.Rejected,
		Generation: report.Generation}
	facts.WrittenAgeSeconds = ageAt(report.ReadAt, report.WrittenAt)
	if backfill := report.Backfill; backfill != nil {
		facts.Backfill = &fleet.SourcePublisherBackfill{Source: backfill.Source, StrategySets: backfill.StrategySets,
			Written: backfill.Written, Failed: backfill.Failed, Start: backfill.Start, LegacyMax: backfill.LegacyMax,
			Summary: backfill.Summary, UpdatedAgeSeconds: ageAt(report.ReadAt, backfill.UpdatedAt)}
	}
	return facts
}

// ageAt is how long before at the record says it was written, in whole
// seconds, or nil when it does not say. A time after at reads as 0: the
// writer's clock is ahead, not the record from the future.
func ageAt(at, written time.Time) *int64 {
	if written.IsZero() {
		return nil
	}
	age := int64(max(at.Sub(written), 0) / time.Second)
	return &age
}
