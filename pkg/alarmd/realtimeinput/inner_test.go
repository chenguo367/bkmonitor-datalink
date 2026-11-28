// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeinput_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/linkedin/goavro/v2"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeinput"
)

// expectFile is testdata/inner/expect.json: the rows of each recorded message as an independent Avro reader decoded them when
// the message was captured.
type expectFile struct {
	Messages map[string]struct {
		Kind    string           `json:"kind"`
		Records int              `json:"records"`
		Rows    []map[string]any `json:"rows"`
	} `json:"messages"`
}

func loadExpect(t *testing.T) expectFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "inner", "expect.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expect expectFile
	if err := json.Unmarshal(raw, &expect); err != nil {
		t.Fatal(err)
	}
	if len(expect.Messages) == 0 {
		t.Fatal("expect.json lists no messages")
	}
	return expect
}

func readMessage(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(filepath.Join("testdata", "inner", name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// The fields a row carries besides the point itself: everything the capture's reader saw as text, less the point's own text
// fields and the two display times Databus adds.
var notNamed = map[string]bool{"metric": true, "dimensions": true, "target": true, "localTime": true, "dtEventTime": true}

// rowText renders a decoded row the way the expectation is rendered, so the two compare as sets: the capture's reader walked
// each batch back to front, the decoder in message order.
func rowText(metric string, value float64, eventMs int64, dimensions, target string, hasTarget bool, named map[string]string) string {
	keys := make([]string, 0, len(named))
	for key := range named {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+named[key])
	}
	targetText := "<absent>"
	if hasTarget {
		targetText = target
	}
	return fmt.Sprintf("%s|%v|%d|%s|%s|%s", metric, value, eventMs, dimensions, targetText, strings.Join(parts, ","))
}

func expectedRowText(t *testing.T, row map[string]any) string {
	t.Helper()
	named := map[string]string{}
	for key, value := range row {
		if text, ok := value.(string); ok && !notNamed[key] {
			named[key] = text
		}
	}
	target, hasTarget := row["target"].(string)
	return rowText(row["metric"].(string), row["value"].(float64), int64(row["dtEventTimeStamp"].(float64)),
		row["dimensions"].(string), target, hasTarget, named)
}

func decodedRowText(t *testing.T, row realtimeinput.Row) string {
	t.Helper()
	if row.Metric == nil || row.Value == nil || row.EventTimeMs == nil || row.Dimensions == nil {
		t.Fatalf("a recorded row decoded without its point fields: %+v", row)
	}
	target, hasTarget := "", row.Target != nil
	if hasTarget {
		target = *row.Target
	}
	return rowText(*row.Metric, *row.Value, *row.EventTimeMs, *row.Dimensions, target, hasTarget, row.Named)
}

func TestDecodeInnerReadsWhatDatabusWrote(t *testing.T) {
	expect := loadExpect(t)
	kinds := map[string]int{}
	for name, message := range expect.Messages {
		kinds[message.Kind]++
		rows, err := realtimeinput.DecodeInner(readMessage(t, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := make([]string, 0, len(message.Rows))
		for _, row := range message.Rows {
			want = append(want, expectedRowText(t, row))
		}
		got := make([]string, 0, len(rows))
		for _, row := range rows {
			got = append(got, decodedRowText(t, row))
		}
		sort.Strings(want)
		sort.Strings(got)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s rows:\n got  %q\n want %q", name, got, want)
		}
	}
	for _, kind := range []string{"k8s", "custom_report"} {
		if kinds[kind] == 0 {
			t.Fatalf("no recorded %s message", kind)
		}
	}
}

func TestDecodeInnerTakesTheEventTimeNotTheRowTime(t *testing.T) {
	// The four sources write `time` in different units (plugins in seconds); dtEventTimeStamp is milliseconds for all of them.
	expect := loadExpect(t)
	for name, message := range expect.Messages {
		rows, err := realtimeinput.DecodeInner(readMessage(t, name))
		if err != nil {
			t.Fatal(err)
		}
		want := map[int64]bool{}
		for _, row := range message.Rows {
			want[int64(row["dtEventTimeStamp"].(float64))] = true
		}
		for _, row := range rows {
			if !want[*row.EventTimeMs] {
				t.Fatalf("%s: event time %d is not a recorded dtEventTimeStamp", name, *row.EventTimeMs)
			}
		}
	}
}

func avroBody(t *testing.T, value []byte) []byte {
	t.Helper()
	runes := []rune(string(value))
	body := make([]byte, len(runes))
	for i, r := range runes {
		if r > 0xff {
			t.Fatalf("recorded message is not latin-1 text at %d", i)
		}
		body[i] = byte(r)
	}
	return body
}

func asText(body []byte) []byte {
	var out bytes.Buffer
	for _, b := range body {
		out.WriteRune(rune(b))
	}
	return out.Bytes()
}

func failureReason(t *testing.T, err error) string {
	t.Helper()
	var failure *realtimeinput.DecodeFailure
	if !errors.As(err, &failure) {
		t.Fatalf("not a named decode failure: %v", err)
	}
	return failure.Reason
}

func TestDecodeInnerNamesEveryFailure(t *testing.T) {
	recorded := readMessage(t, "k8s-1.bin")
	body := avroBody(t, recorded)

	otherCodec := bytes.Replace(body, []byte("snappy"), []byte("brotli"), 1)
	if bytes.Equal(otherCodec, body) {
		t.Fatal("recorded header names no snappy codec")
	}
	corrupt := append([]byte(nil), body...)
	corrupt[len(corrupt)-20] ^= 0xff

	other, err := goavro.NewCodec(`{"type":"record","name":"other","fields":[{"name":"x","type":"long"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	var written bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &written, Codec: other, CompressionName: goavro.CompressionSnappyLabel})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append([]any{map[string]any{"x": int64(1)}}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		value []byte
		want  string
	}{
		{"container bytes not carried as text", body, realtimeinput.DecodeNotLatin1Text},
		{"text beyond latin-1", []byte("中"), realtimeinput.DecodeNotLatin1Text},
		{"text that is no container", []byte("hello"), realtimeinput.DecodeNotAvroContainer},
		{"empty message", nil, realtimeinput.DecodeNotAvroContainer},
		{"a codec the reader does not know", asText(otherCodec), realtimeinput.DecodeCodecUnsupported},
		{"a block whose bytes changed", asText(corrupt), realtimeinput.DecodeBlockCorrupt},
		{"a block cut short", asText(body[:len(body)-10]), realtimeinput.DecodeBlockCorrupt},
		{"another schema", asText(written.Bytes()), realtimeinput.DecodeSchemaUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := realtimeinput.DecodeInner(tc.value)
			if err == nil {
				t.Fatalf("decoded %d rows", len(rows))
			}
			if got := failureReason(t, err); got != tc.want {
				t.Fatalf("reason %q, want %q (%v)", got, tc.want, err)
			}
		})
	}
}

func TestDecodeFailureReasonsAreClosed(t *testing.T) {
	want := []string{"block_corrupt", "codec_unsupported", "not_avro_container", "not_latin1_text", "schema_unsupported"}
	got := append([]string(nil), realtimeinput.DecodeFailureReasons()...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("reasons %v, want %v", got, want)
	}
}

// writeContainer writes rows under a row schema as Databus does: an Avro container (snappy) whose kafkamsg record carries
// the rows in _value_, its bytes carried as latin-1 characters in UTF-8.
func writeContainer(t *testing.T, rowFields string, rows []map[string]any) []byte {
	t.Helper()
	schema := `{"type":"record","name":"kafkamsg","fields":[{"name":"_tagTime_","type":"long","default":0},` +
		`{"name":"_metricTag_","type":"string"},{"name":"_value_","type":{"type":"array","items":{"type":"record","name":"msg",` +
		`"fields":[` + rowFields + `]}}}]}`
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	var written bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &written, Codec: codec, CompressionName: goavro.CompressionSnappyLabel})
	if err != nil {
		t.Fatal(err)
	}
	items := make([]any, 0, len(rows))
	for _, row := range rows {
		items = append(items, row)
	}
	if err := writer.Append([]any{map[string]any{"_tagTime_": int64(0), "_metricTag_": "", "_value_": items}}); err != nil {
		t.Fatal(err)
	}
	return asText(written.Bytes())
}

func TestDecodeInnerReadsFieldsByNameWhateverTheSchemaCarries(t *testing.T) {
	// The schema carries only the fields the channel's rules assign, in the order they are listed, and may carry fields
	// this reader does not know: a known field is read wherever it stands, an absent one stays unset, an unknown text field
	// is carried by name and any other unknown field is left.
	fields := `{"name":"extra_long","type":["null","long"]},{"name":"dimensions","type":["null","string"]},` +
		`{"name":"metric","type":["null","string"]},{"name":"extra_text","type":["null","string"]},` +
		`{"name":"dtEventTimeStamp","type":"long"},{"name":"value","type":["null","double"]}`
	rows, err := realtimeinput.DecodeInner(writeContainer(t, fields, []map[string]any{{
		"extra_long": goavro.Union("long", int64(7)), "dimensions": goavro.Union("string", `{"a":"b"}`),
		"metric": goavro.Union("string", "cpu"), "extra_text": goavro.Union("string", "x"),
		"dtEventTimeStamp": int64(1700000000000), "value": goavro.Union("double", 0.5),
	}}))
	if err != nil {
		t.Fatal(err)
	}
	row := rows[0]
	if *row.Metric != "cpu" || *row.Value != 0.5 || *row.EventTimeMs != 1700000000000 || *row.Dimensions != `{"a":"b"}` || row.Target != nil {
		t.Fatalf("row %+v", row)
	}
	if len(row.Named) != 1 || row.Named["extra_text"] != "x" {
		t.Fatalf("named %v, want only the unknown text field", row.Named)
	}

	// A row without the event time decodes; the point it cannot make is refused by name when it is projected.
	short := `{"name":"metric","type":["null","string"]},{"name":"value","type":["null","double"]},{"name":"dimensions","type":["null","string"]}`
	rows, err = realtimeinput.DecodeInner(writeContainer(t, short, []map[string]any{{
		"metric": goavro.Union("string", "cpu"), "value": goavro.Union("double", 0.5), "dimensions": nil,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].EventTimeMs != nil || rows[0].Dimensions != nil {
		t.Fatalf("row %+v", rows[0])
	}
	_, err = realtimeinput.Project(realtimeinput.SourceK8s, rows[0])
	var bad *realtimeinput.BadPoint
	if !errors.As(err, &bad) || bad.Reason != realtimeinput.BadPointTimeMissing {
		t.Fatalf("projection %v", err)
	}
}

// A field the rules write has one type in every message the writer's schema describes: a known field of another type
// (a value as text, dimensions as a map, an agent field as a number) is the whole container's schema, refused as
// schema_unsupported, never read as absent. A field the rules do not name may have any type.
func TestAKnownFieldOfAnotherTypeRefusesTheMessage(t *testing.T) {
	for name, field := range map[string]struct {
		schema string
		row    map[string]any
	}{
		"value as text":           {`{"name":"value","type":["null","string"]}`, map[string]any{"value": goavro.Union("string", "0.5")}},
		"dimensions as a map":     {`{"name":"dimensions","type":{"type":"map","values":"string"}}`, map[string]any{"dimensions": map[string]any{"a": "b"}}},
		"target as a number":      {`{"name":"target","type":["null","long"]}`, map[string]any{"target": goavro.Union("long", int64(1))}},
		"host id as a number":     {`{"name":"bk_host_id","type":"long"}`, map[string]any{"bk_host_id": int64(7)}},
		"event time as text":      {`{"name":"dtEventTimeStamp","type":"string"}`, map[string]any{"dtEventTimeStamp": "1700000000000"}},
		"metric as a one-key map": {`{"name":"metric","type":{"type":"map","values":"string"}}`, map[string]any{"metric": map[string]any{"string": "cpu"}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := realtimeinput.DecodeInner(writeContainer(t, field.schema, []map[string]any{field.row}))
			if reason := failureReason(t, err); reason != realtimeinput.DecodeSchemaUnsupported {
				t.Fatalf("reason %q", reason)
			}
		})
	}
}

// containerBlock locates a container's first block, read from the container format itself: where its size is and where
// its data starts, and the size it declares.
func containerBlock(t *testing.T, body []byte) (sizeAt, dataAt int, size int64) {
	t.Helper()
	sync := body[len(body)-16:]
	start := bytes.Index(body, sync) + 16
	if start < 16 {
		t.Fatal("no sync marker")
	}
	_, n := varint(body[start:])
	sizeAt = start + n
	size, m := varint(body[sizeAt:])
	return sizeAt, sizeAt + m, size
}

func varint(data []byte) (int64, int) {
	var value uint64
	for i, c := range data {
		value |= uint64(c&0x7f) << (7 * uint(i))
		if c&0x80 == 0 {
			return int64(value>>1) ^ -int64(value&1), i + 1
		}
	}
	return 0, 0
}

func zigzag(value int64) []byte {
	encoded := uint64((value << 1) ^ (value >> 63))
	var out []byte
	for encoded >= 0x80 {
		out = append(out, byte(encoded)|0x80)
		encoded >>= 7
	}
	return append(out, byte(encoded))
}

func uvarint(value uint64) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

// unsignedLen is the length of the unsigned varint data starts with.
func unsignedLen(data []byte) int {
	for i, c := range data {
		if c&0x80 == 0 {
			return i + 1
		}
	}
	return 0
}

// A block that declares more than the message holds, or a snappy block that declares a decoded length far beyond any
// message, is corrupt: refused by name before anything that size is made, however the header was damaged.
func TestABlockDeclaringMoreThanTheMessageHoldsIsCorrupt(t *testing.T) {
	body := avroBody(t, writeContainer(t, `{"name":"metric","type":"string"}`, []map[string]any{{"metric": "cpu"}}))
	sizeAt, dataAt, size := containerBlock(t, body)
	declared := int64(256 << 20)
	oversized := append(append(append([]byte{}, body[:sizeAt]...), zigzag(declared)...), body[dataAt:]...)
	// The snappy block starts with its decoded length as an unsigned varint; the block's size follows the longer prefix.
	prefix := unsignedLen(body[dataAt:])
	longer := uvarint(256 << 20)
	inflated := append([]byte{}, body[:sizeAt]...)
	inflated = append(inflated, zigzag(size-int64(prefix)+int64(len(longer)))...)
	inflated = append(inflated, longer...)
	inflated = append(inflated, body[dataAt+prefix:]...)
	for name, damaged := range map[string][]byte{"block size": oversized, "decoded length": inflated} {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			_, err := realtimeinput.DecodeInner(asText(damaged))
			runtime.ReadMemStats(&after)
			if reason := failureReason(t, err); reason != realtimeinput.DecodeBlockCorrupt {
				t.Fatalf("reason %q", reason)
			}
			if grown := after.TotalAlloc - before.TotalAlloc; grown > 16<<20 {
				t.Fatalf("decoding a damaged block allocated %d bytes", grown)
			}
		})
	}
}

// A field the schema does not declare a union is read as it is: a map with one entry is a map, not a union's branch,
// so it is no text to carry by name; a field the schema declares a union is unwrapped.
func TestOnlyAFieldDeclaredAUnionIsUnwrapped(t *testing.T) {
	fields := `{"name":"metric","type":"string"},{"name":"labels","type":{"type":"map","values":"string"}},` +
		`{"name":"note","type":["null","string"]},{"name":"dtEventTimeStamp","type":"long"},{"name":"value","type":"double"}`
	rows, err := realtimeinput.DecodeInner(writeContainer(t, fields, []map[string]any{{
		"metric": "cpu", "labels": map[string]any{"string": "x"}, "note": goavro.Union("string", "y"),
		"dtEventTimeStamp": int64(1700000000000), "value": 0.5,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if named := rows[0].Named; len(named) != 1 || named["note"] != "y" {
		t.Fatalf("named %v, want the union's text and not the map's entry", named)
	}
}

// A block not followed by the container's sync marker is corrupt: what follows it is not a block this message's writer
// wrote.
func TestABlockNotFollowedByTheSyncMarkerIsCorrupt(t *testing.T) {
	body := avroBody(t, writeContainer(t, `{"name":"metric","type":"string"}`, []map[string]any{{"metric": "cpu"}}))
	damaged := append([]byte{}, body...)
	damaged[len(damaged)-1] ^= 0xff
	_, err := realtimeinput.DecodeInner(asText(damaged))
	if reason := failureReason(t, err); reason != realtimeinput.DecodeBlockCorrupt {
		t.Fatalf("reason %q, want %q", reason, realtimeinput.DecodeBlockCorrupt)
	}
}
