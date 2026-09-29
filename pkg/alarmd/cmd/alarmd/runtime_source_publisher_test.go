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
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
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
	now := func() time.Time { return time.Unix(1700000600, 0) }
	if sourcePublisherFleetFacts(plainStrategySource{}, now) != nil {
		t.Fatal("a source that reads no publisher record got a reader")
	}
	reader := sourcePublisherFleetFacts(publisherReportingSource{}, now)
	if reader == nil || reader() != nil {
		t.Fatal("a source that has not read the active set published a record")
	}
	read := sourcePublisherFleetFacts(publisherReportingSource{read: true,
		report: controlplane.PublisherReport{State: controlplane.PublisherReportNotProvided, ReadAt: now().Add(-time.Minute)}}, now)
	if facts := read(); facts == nil || facts.State != "not_provided" {
		t.Fatalf("facts = %+v, want not_provided", facts)
	}
	// A leader that handed over keeps its last record; past the bound it is
	// not published as the publisher's present word.
	stale := sourcePublisherFleetFacts(publisherReportingSource{read: true,
		report: controlplane.PublisherReport{State: controlplane.PublisherReportProvided,
			ReadAt: now().Add(-controlplane.SourceStalenessBound - time.Second)}}, now)
	if facts := stale(); facts != nil {
		t.Fatalf("a read older than the bound was published: %+v", facts)
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

// publisherSeries is every strategy_publisher series the recorder exposes,
// as name plus labels plus value, so two scrapes compare as text.
func publisherSeries(t *testing.T, recorder *metric.Recorder) []string {
	t.Helper()
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var series []string
	for _, family := range families {
		name := family.GetName()
		if name != "bkmonitor_alarmd_strategy_publisher_info" && name != "bkmonitor_alarmd_strategy_publisher_reports_total" {
			continue
		}
		for _, m := range family.Metric {
			line := name
			for _, label := range m.Label {
				line += " " + label.GetName() + "=" + label.GetValue()
			}
			line += " " + strconv.FormatFloat(m.GetGauge().GetValue()+m.GetCounter().GetValue(), 'f', -1, 64)
			series = append(series, line)
		}
	}
	sort.Strings(series)
	return series
}

// A follower answering a diagnosis reads the active set through the same
// source its control rounds would use. Before this, that read recorded the
// publisher's record: the follower's info series stayed on that one read
// for good and the record was counted a second time beside the leader's.
func TestADiagnosisReadLeavesThePublisherMetricsAsTheyWere(t *testing.T) {
	_, client, refused := startOwnPhaseTwoRedis(t, redistest.Server(t), "")
	if refused != "" {
		t.Skip(refused)
	}
	ctx := context.Background()
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[7]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	record := `{"schema_version":1,"writer":"example-publisher","version":"5.3.0","written_at":1700000000,` +
		`"outcome":"blocked","reason":"SYSTEMIC_REJECTION"}`
	if err := client.Set(ctx, "bkmonitor.cache.publisher", record, 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	recorder := metric.NewRecorder(metric.BuildInfo{})
	recorder.SetStrategyPublisherSource(source)
	before := publisherSeries(t, recorder)
	if len(before) != 0 {
		t.Fatalf("a process that has read nothing exposes %v", before)
	}
	ids, err := diagnosisUniverse(source)(ctx)
	if err != nil || len(ids) != 1 {
		t.Fatalf("diagnosis universe = %v, %v", ids, err)
	}
	if after := publisherSeries(t, recorder); len(after) != 0 {
		t.Fatalf("a diagnosis read changed the publisher metrics: %v", after)
	}
	if _, read := source.PublisherReport(); read {
		t.Fatal("a diagnosis read recorded the publisher's record")
	}
	if facts := sourcePublisherFleetFacts(source, time.Now)(); facts != nil {
		t.Fatalf("a diagnosis read put the record on the snapshot: %+v", facts)
	}
	// The control round's own read is the one that speaks for the publisher.
	if _, err := source.ActiveStrategyIDs(ctx); err != nil {
		t.Fatal(err)
	}
	if got := publisherSeries(t, recorder); len(got) != 2 {
		t.Fatalf("after a control read = %v, want the info series and one count", got)
	}
}
