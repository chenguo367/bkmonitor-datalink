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
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

type publisherReportingSource struct {
	controlplane.StrategySource
	report controlplane.PublisherReport
	read   bool
}

func (source publisherReportingSource) PublisherReport() (controlplane.PublisherReport, bool) {
	return source.report, source.read
}

func (publisherReportingSource) PublisherReportCounts() []controlplane.PublisherReportCount {
	return nil
}

type plainStrategySource struct{}

func (plainStrategySource) ActiveStrategyIDs(context.Context) ([]string, error) { return nil, nil }

func (plainStrategySource) Strategies(context.Context, []string) ([]controlplane.SourceStrategy, error) {
	return nil, nil
}

func TestSourcePublisherFactsCarryTheRecordWithItsLabelAndAges(t *testing.T) {
	readAt := time.Unix(1700000600, 0)
	strategies, start := int64(45), int64(1000000)
	facts := sourcePublisherFactsOf(controlplane.PublisherReport{State: controlplane.PublisherReportProvided, ReadAt: readAt,
		Writer: "example-publisher", Version: "5.3.0", Outcome: "published", WrittenAt: time.Unix(1700000540, 0),
		Strategies: &strategies, Backfill: &controlplane.PublisherBackfill{Source: "backfill", Start: &start,
			Summary: "ok", UpdatedAt: time.Unix(1699990600, 0)}})
	if facts.Label != "写方自述" || facts.State != "provided" || facts.Outcome != "published" || *facts.Strategies != 45 {
		t.Fatalf("facts = %+v", facts)
	}
	if facts.WrittenAgeSeconds == nil || *facts.WrittenAgeSeconds != 60 {
		t.Fatalf("written age = %v, want 60", facts.WrittenAgeSeconds)
	}
	if facts.Backfill == nil || *facts.Backfill.Start != 1000000 || facts.Backfill.UpdatedAgeSeconds == nil ||
		*facts.Backfill.UpdatedAgeSeconds != 10000 {
		t.Fatalf("backfill = %+v", facts.Backfill)
	}
	// A record that says nothing about when it was written has no age, and
	// a writer whose clock runs ahead reads as just written, not negative.
	unknown := sourcePublisherFactsOf(controlplane.PublisherReport{State: controlplane.PublisherReportNotProvided, ReadAt: readAt})
	if unknown.Label != "写方未提供" || unknown.WrittenAgeSeconds != nil || unknown.Backfill != nil {
		t.Fatalf("not provided = %+v", unknown)
	}
	ahead := sourcePublisherFactsOf(controlplane.PublisherReport{State: controlplane.PublisherReportProvided, ReadAt: readAt,
		WrittenAt: readAt.Add(5 * time.Second)})
	if ahead.WrittenAgeSeconds == nil || *ahead.WrittenAgeSeconds != 0 {
		t.Fatalf("a record from a clock ahead = %v, want 0", ahead.WrittenAgeSeconds)
	}
	unreadable := sourcePublisherFactsOf(controlplane.PublisherReport{State: controlplane.PublisherReportUnreadable,
		Detail: controlplane.PublisherReportSchemaVersion, ReadAt: readAt})
	if unreadable.Label != "写方自述读不出" || unreadable.Detail != "schema_version" {
		t.Fatalf("unreadable = %+v", unreadable)
	}
}

func TestSourcePublisherFleetFactsSayNothingBeforeARead(t *testing.T) {
	if sourcePublisherFleetFacts(plainStrategySource{}) != nil {
		t.Fatal("a source that reads no publisher record got a reader")
	}
	reader := sourcePublisherFleetFacts(publisherReportingSource{})
	if reader == nil || reader() != nil {
		t.Fatal("a source that has not read the active set published a record")
	}
	read := sourcePublisherFleetFacts(publisherReportingSource{read: true,
		report: controlplane.PublisherReport{State: controlplane.PublisherReportNotProvided}})
	if facts := read(); facts == nil || facts.State != "not_provided" {
		t.Fatalf("facts = %+v, want not_provided", facts)
	}
}

// The production source is one that reads the record: the reader is wired
// and the collector is bound through the same interface.
func TestTheProductionStrategySourceReadsThePublisherRecord(t *testing.T) {
	var source controlplane.StrategySource = &controlplane.LegacyRedisStrategySource{}
	if _, ok := source.(controlplane.PublisherReportSource); !ok {
		t.Fatal("LegacyRedisStrategySource does not implement PublisherReportSource")
	}
}
