// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// commandNameHook records the name of every command the client sends.
type commandNameHook struct {
	mu    sync.Mutex
	names []string
}

func (hook *commandNameHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	hook.mu.Lock()
	hook.names = append(hook.names, strings.ToLower(cmd.Name()))
	hook.mu.Unlock()
	return ctx, nil
}

func (*commandNameHook) AfterProcess(context.Context, redis.Cmder) error { return nil }

func (*commandNameHook) BeforeProcessPipeline(ctx context.Context, _ []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (*commandNameHook) AfterProcessPipeline(context.Context, []redis.Cmder) error { return nil }

func (hook *commandNameHook) take() []string {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	names := hook.names
	hook.names = nil
	return names
}

// publishedRecord is the record a publisher writes after a run that
// published, with a backfill summary, in the publisher's own key order.
const publishedRecord = `{"schema_version":1,"writer":"example-strategy-publisher","version":"5.3.0","commit":"",` +
	`"written_at":1700000000,"outcome":"published","reason":"","strategies":45,"rejected":2,"generation":7,` +
	`"backfill":{"source":"backfill","strategy_sets":44,"written":43,"failed":1,"start":1000000,"legacy_max":999,` +
	`"summary":"StrategySet 44：写入 43、失败 1","updated_at":1699990000}}`

func TestLegacySourceReadsThePublisherRecordInTheSameRoundTripAsTheActiveSet(t *testing.T) {
	client := newControlplaneRedis(t)
	ctx := context.Background()
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[1,2]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "bkmonitor.cache.publisher", publishedRecord, 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	if _, read := source.PublisherReport(); read {
		t.Fatal("a source that has not read the active set reports a publisher record")
	}
	hook := &commandNameHook{}
	client.AddHook(hook)
	ids, err := source.ActiveStrategyIDs(ctx)
	if err != nil || strings.Join(ids, ",") != "1,2" {
		t.Fatalf("ActiveStrategyIDs = %v, %v", ids, err)
	}
	// Zero added commands: the record rides on the read the active set
	// always took.
	if names := hook.take(); strings.Join(names, ",") != "mget" {
		t.Fatalf("reading the active set sent %v, want one mget", names)
	}
	report, read := source.PublisherReport()
	if !read {
		t.Fatal("no publisher record after reading the active set")
	}
	if report.State != controlplane.PublisherReportProvided || report.Detail != "" {
		t.Fatalf("state = %q detail %q", report.State, report.Detail)
	}
	if report.Writer != "example-strategy-publisher" || report.Version != "5.3.0" || report.Outcome != "published" ||
		report.Reason != "" || !report.WrittenAt.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("report = %+v", report)
	}
	if deref(report.Strategies) != 45 || deref(report.Rejected) != 2 || deref(report.Generation) != 7 {
		t.Fatalf("counts = %v %v %v", report.Strategies, report.Rejected, report.Generation)
	}
	backfill := report.Backfill
	if backfill == nil || backfill.Source != "backfill" || deref(backfill.StrategySets) != 44 || deref(backfill.Written) != 43 ||
		deref(backfill.Failed) != 1 || deref(backfill.Start) != 1000000 || deref(backfill.LegacyMax) != 999 ||
		backfill.Summary != "StrategySet 44：写入 43、失败 1" || !backfill.UpdatedAt.Equal(time.Unix(1699990000, 0)) {
		t.Fatalf("backfill = %+v", backfill)
	}
	if report.ReadAt.IsZero() {
		t.Fatal("the report does not say when it was read")
	}
}

func TestLegacySourceKeepsThePublisherRecordWhenTheActiveSetIsRefused(t *testing.T) {
	// The round the record is for: the publisher is running and refusing to
	// publish, the active set has expired, and the round fails at it.
	client := newControlplaneRedis(t)
	ctx := context.Background()
	blocked := `{"schema_version":1,"writer":"example-strategy-publisher","version":"5.3.0","written_at":1700000000,` +
		`"outcome":"blocked","reason":"SPLIT_RECORDS_NOT_BACKFILLED","strategies":0,"rejected":3,"generation":null,"backfill":null}`
	if err := client.Set(ctx, "bkmonitor.cache.publisher", blocked, 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ActiveStrategyIDs(ctx); !errors.Is(err, controlplane.ErrLegacySourceIncomplete) {
		t.Fatalf("an absent active set = %v, want ErrLegacySourceIncomplete", err)
	}
	report, read := source.PublisherReport()
	if !read || report.State != controlplane.PublisherReportProvided || report.Outcome != "blocked" ||
		report.Reason != "SPLIT_RECORDS_NOT_BACKFILLED" {
		t.Fatalf("report after a refused active set = %+v, %v", report, read)
	}
	if deref(report.Strategies) != 0 || report.Strategies == nil || report.Generation != nil || report.Backfill != nil {
		t.Fatalf("null and zero are not kept apart: strategies %v generation %v backfill %v",
			report.Strategies, report.Generation, report.Backfill)
	}
	// The same for an active set that is there and does not decode.
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "not json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "bkmonitor.cache.publisher", publishedRecord, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ActiveStrategyIDs(ctx); !errors.Is(err, controlplane.ErrLegacySourceIncomplete) {
		t.Fatalf("an undecodable active set = %v, want ErrLegacySourceIncomplete", err)
	}
	if report, _ := source.PublisherReport(); report.Outcome != "published" {
		t.Fatalf("the record was not refreshed on a refused read: %+v", report)
	}
}

func TestLegacySourceSaysThePublisherLeftNoRecordRatherThanFailing(t *testing.T) {
	client := newControlplaneRedis(t)
	ctx := context.Background()
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[3]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := source.ActiveStrategyIDs(ctx)
	if err != nil || strings.Join(ids, ",") != "3" {
		t.Fatalf("a publisher without a record changed the active set: %v, %v", ids, err)
	}
	report, read := source.PublisherReport()
	if !read || report.State != controlplane.PublisherReportNotProvided || report.Detail != "" || report.Outcome != "" {
		t.Fatalf("no record = %+v, %v; want not_provided", report, read)
	}
	if err := client.Set(ctx, "bkmonitor.cache.publisher", "", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ActiveStrategyIDs(ctx); err != nil {
		t.Fatal(err)
	}
	if report, _ := source.PublisherReport(); report.State != controlplane.PublisherReportNotProvided {
		t.Fatalf("an empty record = %+v; want not_provided", report)
	}
	if counts := source.PublisherReportCounts(); len(counts) != 0 {
		t.Fatalf("a publisher without a record was counted: %+v", counts)
	}
}

func TestLegacySourceNamesAnUnreadablePublisherRecordAndStillReadsTheActiveSet(t *testing.T) {
	tests := []struct {
		name, record, detail string
	}{
		{"not json", "published", controlplane.PublisherReportNotJSON},
		{"not an object", `[1,2]`, controlplane.PublisherReportFieldType},
		{"a count that is text", `{"schema_version":1,"outcome":"published","strategies":"45"}`, controlplane.PublisherReportFieldType},
		{"no schema version", `{"outcome":"published"}`, controlplane.PublisherReportSchemaVersion},
		{"a later schema version", `{"schema_version":2,"outcome":"published"}`, controlplane.PublisherReportSchemaVersion},
		{"too large", `{"schema_version":1,"reason":"` + strings.Repeat("x", controlplane.PublisherReportMaxBytes) + `"}`,
			controlplane.PublisherReportTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newControlplaneRedis(t)
			ctx := context.Background()
			if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[4]", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.Set(ctx, "bkmonitor.cache.publisher", test.record, 0).Err(); err != nil {
				t.Fatal(err)
			}
			source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
			if err != nil {
				t.Fatal(err)
			}
			if ids, err := source.ActiveStrategyIDs(ctx); err != nil || strings.Join(ids, ",") != "4" {
				t.Fatalf("an unreadable record changed the active set: %v, %v", ids, err)
			}
			report, _ := source.PublisherReport()
			if report.State != controlplane.PublisherReportUnreadable || report.Detail != test.detail {
				t.Fatalf("state %q detail %q, want unreadable %q", report.State, report.Detail, test.detail)
			}
			if report.Outcome != "" || report.Reason != "" {
				t.Fatalf("an unreadable record carries fields: %+v", report)
			}
		})
	}
}

func TestLegacySourceToleratesUnknownFieldsAndBoundsTheText(t *testing.T) {
	client := newControlplaneRedis(t)
	ctx := context.Background()
	long := strings.Repeat("写", 400)
	record := fmt.Sprintf(`{"schema_version":1,"writer":%q,"outcome":"failed","reason":%q,"written_at":1700000000,`+
		`"added_later":{"x":1},"backfill":{"summary":%q}}`, strings.Repeat("w", 100), "Error"+long, long)
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[5]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "bkmonitor.cache.publisher", record, 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ActiveStrategyIDs(ctx); err != nil {
		t.Fatal(err)
	}
	report, _ := source.PublisherReport()
	if report.State != controlplane.PublisherReportProvided {
		t.Fatalf("a field this reader does not know made the record unreadable: %+v", report)
	}
	for name, text := range map[string]string{"writer": report.Writer, "reason": report.Reason, "summary": report.Backfill.Summary} {
		if !strings.HasSuffix(text, "...") || !utf8.ValidString(text) {
			t.Fatalf("%s was not cut on a rune boundary with a marker: %q", name, text)
		}
	}
	if len(report.Writer) > 64+3 || len(report.Reason) > 128+3 || len(report.Backfill.Summary) > 1024+3 {
		t.Fatalf("bounds not kept: writer %d reason %d summary %d", len(report.Writer), len(report.Reason), len(report.Backfill.Summary))
	}
	if report.Backfill.UpdatedAt != (time.Time{}) || report.Backfill.Written != nil {
		t.Fatalf("fields the backfill did not carry were filled: %+v", report.Backfill)
	}
}

func TestLegacySourceCountsEachPublisherRecordOnceUnderBoundedLabels(t *testing.T) {
	client := newControlplaneRedis(t)
	ctx := context.Background()
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", "[6]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := controlplane.NewLegacyRedisStrategySource(client, "bkmonitor.cache")
	if err != nil {
		t.Fatal(err)
	}
	write := func(writtenAt int, outcome, reason string) {
		t.Helper()
		record := fmt.Sprintf(`{"schema_version":1,"written_at":%d,"outcome":%q,"reason":%q}`, writtenAt, outcome, reason)
		if err := client.Set(ctx, "bkmonitor.cache.publisher", record, 0).Err(); err != nil {
			t.Fatal(err)
		}
		// Every round reads the active set at least once, a full read four
		// times: the same record read again is not another run.
		for range 3 {
			if _, err := source.ActiveStrategyIDs(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(100, "published", "")
	write(160, "published", "")
	write(220, "blocked", "SPLIT_RECORDS_NOT_BACKFILLED")
	write(280, "failed", "ProgrammingError")
	write(340, "renamed", "")
	got := countsText(source.PublisherReportCounts())
	want := "blocked/SPLIT_RECORDS_NOT_BACKFILLED=1 failed/other=1 other/=1 published/=2"
	if got != want {
		t.Fatalf("counts = %s, want %s", got, want)
	}
	// A publisher that names a new reason every run cannot grow the label
	// set: past the limit its reasons are counted as other.
	for index := range controlplane.PublisherReasonLabelLimit + 5 {
		write(400+index*60, "blocked", fmt.Sprintf("REASON_%02d", index))
	}
	named, other := 0, uint64(0)
	for _, count := range source.PublisherReportCounts() {
		switch {
		case count.Reason == "other" && count.Outcome == "blocked":
			other = count.Count
		case count.Reason != "" && count.Reason != "other":
			named++
		}
	}
	if named != controlplane.PublisherReasonLabelLimit || other != 6 {
		t.Fatalf("named reasons %d (want %d), blocked/other %d (want 6)", named, controlplane.PublisherReasonLabelLimit, other)
	}
}

func countsText(counts []controlplane.PublisherReportCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%s/%s=%d", count.Outcome, count.Reason, count.Count))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func deref(value *int64) int64 {
	if value == nil {
		return -1
	}
	return *value
}
