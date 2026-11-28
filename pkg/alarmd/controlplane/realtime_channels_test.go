// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

// writerChannels is testdata/writer-realtime-channels.json: the hash the strategy publisher wrote for the channel rows its
// test set up, and those rows. The rows are the expectation; the hash is what this reader reads.
type writerChannels struct {
	Hash map[string]string `json:"hash"`
	Rows []struct {
		Tenant        string                             `json:"bk_tenant_id"`
		Table         string                             `json:"table_id"`
		Topic         string                             `json:"topic"`
		KafkaCluster  string                             `json:"kafka_cluster"`
		SchemaVersion int                                `json:"schema_version"`
		SourceKind    string                             `json:"source_kind"`
		Phase         string                             `json:"phase"`
		AppliedItems  []controlplane.RealtimeItemRef     `json:"applied_items"`
		AppliedAt     *int64                             `json:"applied_at"`
		RefusedItems  []controlplane.RealtimeRefusedItem `json:"refused_items"`
	} `json:"rows"`
}

func loadWriterChannels(t *testing.T) writerChannels {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "writer-realtime-channels.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file writerChannels
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Rows) == 0 {
		t.Fatal("the writer fixture sets up no channel")
	}
	return file
}

func TestTheReaderReadsWhatThePublisherWrote(t *testing.T) {
	writer := loadWriterChannels(t)
	read := controlplane.DecodeRealtimeChannels(writer.Hash)
	if read.Result != controlplane.RealtimeChannelsOK || len(read.Refused) != 0 {
		t.Fatalf("read %s, refused %+v", read.Result, read.Refused)
	}
	if len(read.Channels) != len(writer.Rows) {
		t.Fatalf("%d channels for %d rows", len(read.Channels), len(writer.Rows))
	}
	for _, row := range writer.Rows {
		channel, ok := read.Channels[controlplane.RealtimeChannelKey{Tenant: row.Tenant, ResultTable: row.Table}]
		if !ok {
			t.Fatalf("no channel for %s|%s", row.Tenant, row.Table)
		}
		var appliedAt time.Time
		if row.AppliedAt != nil && *row.AppliedAt > 0 {
			appliedAt = time.Unix(*row.AppliedAt, 0).UTC()
		}
		if channel.Topic != row.Topic || channel.KafkaCluster != row.KafkaCluster || channel.SchemaVersion != row.SchemaVersion ||
			channel.SourceKind != row.SourceKind || channel.Phase != row.Phase || !channel.AppliedAt.Equal(appliedAt) {
			t.Fatalf("channel %+v for row %+v", channel, row)
		}
		if !sameJSON(t, channel.AppliedItems, row.AppliedItems) || !sameJSON(t, channel.RefusedItems, row.RefusedItems) {
			t.Fatalf("items %+v / %+v for row %+v", channel.AppliedItems, channel.RefusedItems, row)
		}
	}
}

func sameJSON(t *testing.T, a, b any) bool {
	t.Helper()
	left, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return string(left) == string(right) || (string(left) == "null" && string(right) == "[]") || (string(left) == "[]" && string(right) == "null")
}

func TestTheHashIsReadUnderItsSchemaOrNotAtAll(t *testing.T) {
	writer := loadWriterChannels(t)
	if read := controlplane.DecodeRealtimeChannels(nil); read.Result != controlplane.RealtimeChannelsAbsent || len(read.Channels) != 0 {
		t.Fatalf("no hash read as %+v", read)
	}
	withoutSchema := map[string]string{}
	for name, value := range writer.Hash {
		if name != "__schema__" {
			withoutSchema[name] = value
		}
	}
	if read := controlplane.DecodeRealtimeChannels(withoutSchema); read.Result != controlplane.RealtimeChannelsSchemaUnsupported || len(read.Channels) != 0 {
		t.Fatalf("a hash without __schema__ read as %+v", read)
	}
	otherSchema := map[string]string{"__schema__": "2"}
	for name, value := range withoutSchema {
		otherSchema[name] = value
	}
	if read := controlplane.DecodeRealtimeChannels(otherSchema); read.Result != controlplane.RealtimeChannelsSchemaUnsupported || len(read.Channels) != 0 {
		t.Fatalf("a hash of another schema read as %+v", read)
	}
	// The default state: a writer with no channel still writes the hash, with its schema only.
	if read := controlplane.DecodeRealtimeChannels(map[string]string{"__schema__": "1"}); read.Result != controlplane.RealtimeChannelsOK || len(read.Channels) != 0 {
		t.Fatalf("an empty channel hash read as %+v", read)
	}
}

func TestEachRecordIsRefusedByNameAndTheOthersStillRead(t *testing.T) {
	writer := loadWriterChannels(t)
	var sample map[string]any
	var sampleField string
	for name, value := range writer.Hash {
		if name == "__schema__" {
			continue
		}
		sampleField = name
		if err := json.Unmarshal([]byte(value), &sample); err != nil {
			t.Fatal(err)
		}
		break
	}
	with := func(change func(map[string]any)) string {
		record := map[string]any{}
		for name, value := range sample {
			record[name] = value
		}
		change(record)
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	cases := []struct {
		name  string
		field string
		value string
		want  string
	}{
		{"field without a separator", "system", with(func(map[string]any) {}), controlplane.RealtimeChannelRecordMalformed},
		{"field without a tenant", "|2_demo.base", with(func(map[string]any) {}), controlplane.RealtimeChannelRecordMalformed},
		{"field with two separators", "system|a|b", with(func(map[string]any) {}), controlplane.RealtimeChannelRecordMalformed},
		{"value not JSON", "system|bad.json", "{", controlplane.RealtimeChannelRecordMalformed},
		{"topic missing", "system|no.topic", with(func(r map[string]any) { delete(r, "topic") }), controlplane.RealtimeChannelRecordMalformed},
		{"source kind missing", "system|no.kind", with(func(r map[string]any) { delete(r, "source_kind") }), controlplane.RealtimeChannelRecordMalformed},
		{"topic of another type", "system|topic.type", with(func(r map[string]any) { r["topic"] = 7 }), controlplane.RealtimeChannelRecordMalformed},
		{"applied item without its md5", "system|item.md5", with(func(r map[string]any) {
			r["applied_items"] = []any{map[string]any{"strategy_id": 1, "item_id": 1}}
		}), controlplane.RealtimeChannelRecordMalformed},
		{"refused item without its reason", "system|item.reason", with(func(r map[string]any) {
			r["refused_items"] = []any{map[string]any{"strategy_id": 1, "item_id": 1, "query_md5": "m"}}
		}), controlplane.RealtimeChannelRecordMalformed},
		{"another table shape", "system|v2.shape", with(func(r map[string]any) { r["schema_version"] = 2 }), controlplane.RealtimeChannelRecordSchemaVersionUnsupported},
		{"unknown source kind", "system|basereport.kind", with(func(r map[string]any) { r["source_kind"] = "basereport" }), controlplane.RealtimeChannelRecordSourceKindUnsupported},
	}
	hash := map[string]string{"__schema__": "1", sampleField: writer.Hash[sampleField]}
	for _, tc := range cases {
		hash[tc.field] = tc.value
	}
	read := controlplane.DecodeRealtimeChannels(hash)
	if read.Result != controlplane.RealtimeChannelsOK {
		t.Fatalf("hash read as %s", read.Result)
	}
	if _, ok := read.Channels[keyOf(t, sampleField)]; !ok || len(read.Channels) != 1 {
		t.Fatalf("the good record was not read beside the refused ones: %+v", read.Channels)
	}
	refused := map[string]string{}
	for _, refusal := range read.Refused {
		refused[refusal.Field] = refusal.Result
	}
	for _, tc := range cases {
		if refused[tc.field] != tc.want {
			t.Fatalf("%s: refused as %q, want %q", tc.name, refused[tc.field], tc.want)
		}
	}
}

func keyOf(t *testing.T, field string) controlplane.RealtimeChannelKey {
	t.Helper()
	tenant, table, ok := strings.Cut(field, "|")
	if !ok {
		t.Fatalf("field %q", field)
	}
	return controlplane.RealtimeChannelKey{Tenant: tenant, ResultTable: table}
}

func TestARecordMayNameNoSourceKindYetButOnlyAKnownOne(t *testing.T) {
	// A channel whose source is not resolved yet is published with an empty kind; any other kind is one realtime input
	// projects.
	writer := loadWriterChannels(t)
	var field string
	for name := range writer.Hash {
		if name != "__schema__" {
			field = name
			break
		}
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(writer.Hash[field]), &record); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"", "k8s", "custom_report", "plugin_flat", "plugin_nested"} {
		record["source_kind"] = kind
		encoded, _ := json.Marshal(record)
		read := controlplane.DecodeRealtimeChannels(map[string]string{"__schema__": "1", field: string(encoded)})
		if len(read.Channels) != 1 {
			t.Fatalf("source kind %q refused: %+v", kind, read.Refused)
		}
	}
}

func TestReadResultsAreClosed(t *testing.T) {
	want := []string{"absent", "failed", "ok", "record_malformed", "record_schema_version_unsupported", "record_source_kind_unsupported",
		"schema_unsupported"}
	got := append([]string(nil), controlplane.RealtimeChannelReadResults()...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("results %v, want %v", got, want)
	}
}

func TestTheChannelsAreReadFromTheStrategyCacheKey(t *testing.T) {
	server := redistest.Start(t)
	if server == nil {
		return
	}
	client := redis.NewClient(&redis.Options{Addr: server.Addr, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	const prefix = "demo.cache"
	read, err := controlplane.ReadRealtimeChannels(ctx, client, prefix)
	if err != nil || read.Result != controlplane.RealtimeChannelsAbsent {
		t.Fatalf("no key read as %+v, %v", read, err)
	}
	writer := loadWriterChannels(t)
	values := make([]any, 0, 2*len(writer.Hash))
	for name, value := range writer.Hash {
		values = append(values, name, value)
	}
	if err := client.HSet(ctx, prefix+".realtime_channels", values...).Err(); err != nil {
		t.Fatal(err)
	}
	read, err = controlplane.ReadRealtimeChannels(ctx, client, prefix)
	if err != nil || read.Result != controlplane.RealtimeChannelsOK || len(read.Channels) != len(writer.Rows) {
		t.Fatalf("the written hash read as %+v, %v", read, err)
	}
	_ = client.Close()
	if read, err := controlplane.ReadRealtimeChannels(ctx, client, prefix); err == nil || read.Result != controlplane.RealtimeChannelsFailed {
		t.Fatalf("a failed read came back as %+v, %v", read, err)
	}
}
