// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package realtimeinput reads the realtime channel's inner topic (design 016): the cleaned rows BKBase Databus writes for a
// result table that has realtime items, consumed without a group and placed by time.
package realtimeinput

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/linkedin/goavro/v2"
)

// The reasons a message is not decoded. The set is closed: every failure is one of these.
const (
	// DecodeNotLatin1Text: the message is not UTF-8 text whose characters are all below 256, which is how Databus carries
	// the container's bytes.
	DecodeNotLatin1Text = "not_latin1_text"
	// DecodeNotAvroContainer: the bytes are not an Avro object container (no magic, no readable header).
	DecodeNotAvroContainer = "not_avro_container"
	// DecodeCodecUnsupported: the container's header names a compression the reader does not have.
	DecodeCodecUnsupported = "codec_unsupported"
	// DecodeBlockCorrupt: a block does not read back: its bytes changed, its checksum does not hold, or it is cut short.
	DecodeBlockCorrupt = "block_corrupt"
	// DecodeSchemaUnsupported: the records are not a batch of rows (`_value_`, an array of records) as the channel writes.
	DecodeSchemaUnsupported = "schema_unsupported"
)

// DecodeFailureReasons is the closed set of decode failure reasons.
func DecodeFailureReasons() []string {
	return []string{DecodeNotLatin1Text, DecodeNotAvroContainer, DecodeCodecUnsupported, DecodeBlockCorrupt, DecodeSchemaUnsupported}
}

// DecodeFailure is a message that is not decoded, with its named reason.
type DecodeFailure struct {
	Reason string
	Err    error
}

func (f *DecodeFailure) Error() string {
	if f.Err == nil {
		return "realtime inner message: " + f.Reason
	}
	return fmt.Sprintf("realtime inner message: %s: %v", f.Reason, f.Err)
}

func (f *DecodeFailure) Unwrap() error { return f.Err }

// Row is one row of an inner message, its fields read by name. A field the channel's rules do not assign is absent from the
// message's schema and stays nil here; a field the schema has but the row leaves null is nil as well.
type Row struct {
	Metric *string
	Value  *float64
	// EventTimeMs is dtEventTimeStamp, milliseconds for every source; the row's own `time` is not read, since plugins write it
	// in seconds and the others in milliseconds.
	EventTimeMs *int64
	Dimensions  *string
	Target      *string
	// Named holds the row's other text fields by name: the fields the rules carry from the raw message's root, such as the
	// reporting agent's identity. The two display times Databus adds are not among them.
	Named map[string]string
}

// DecodeInner decodes one inner topic message into its rows. Databus writes an Avro object container (snappy) and carries its
// bytes as latin-1 characters in UTF-8 text, so the text is decoded as UTF-8 and every character taken back as the byte it
// stands for before the container is read.
func DecodeInner(value []byte) ([]Row, error) {
	body, err := latin1Bytes(value)
	if err != nil {
		return nil, &DecodeFailure{Reason: DecodeNotLatin1Text, Err: err}
	}
	if reason, err := checkContainerHeader(body); err != nil {
		return nil, &DecodeFailure{Reason: reason, Err: err}
	}
	reader, err := goavro.NewOCFReader(bytes.NewReader(body))
	if err != nil {
		return nil, &DecodeFailure{Reason: DecodeNotAvroContainer, Err: err}
	}
	unions, err := checkBatchSchema(reader.Codec().Schema())
	if err != nil {
		return nil, &DecodeFailure{Reason: DecodeSchemaUnsupported, Err: err}
	}
	var rows []Row
	for reader.Scan() {
		datum, err := reader.Read()
		if err != nil {
			return nil, &DecodeFailure{Reason: DecodeBlockCorrupt, Err: err}
		}
		record, ok := datum.(map[string]any)
		if !ok {
			return nil, &DecodeFailure{Reason: DecodeSchemaUnsupported, Err: fmt.Errorf("record is %T", datum)}
		}
		batch, ok := record[batchField].([]any)
		if !ok {
			return nil, &DecodeFailure{Reason: DecodeSchemaUnsupported, Err: fmt.Errorf("%s is %T", batchField, record[batchField])}
		}
		for _, item := range batch {
			fields, ok := item.(map[string]any)
			if !ok {
				return nil, &DecodeFailure{Reason: DecodeSchemaUnsupported, Err: fmt.Errorf("row is %T", item)}
			}
			rows = append(rows, rowOf(fields, unions))
		}
	}
	if err := reader.Err(); err != nil {
		return nil, &DecodeFailure{Reason: DecodeBlockCorrupt, Err: err}
	}
	return rows, nil
}

// batchField is the container record's field holding a batch of rows.
const batchField = "_value_"

// displayTimes are the two text times Databus adds to each row; they are not carried fields.
var displayTimes = map[string]bool{"localTime": true, "dtEventTime": true}

func latin1Bytes(value []byte) ([]byte, error) {
	body := make([]byte, 0, len(value))
	for offset := 0; offset < len(value); {
		r, size := utf8.DecodeRune(value[offset:])
		if r == utf8.RuneError && size <= 1 {
			return nil, fmt.Errorf("not UTF-8 at byte %d", offset)
		}
		if r > 0xff {
			return nil, fmt.Errorf("character U+%04X at byte %d is beyond latin-1", r, offset)
		}
		body = append(body, byte(r))
		offset += size
	}
	return body, nil
}

var containerMagic = []byte("Obj\x01")

// knownCodecs are the compressions the container reader has (goavro: null, deflate, snappy).
var knownCodecs = map[string]bool{goavro.CompressionNullLabel: true, goavro.CompressionDeflateLabel: true, goavro.CompressionSnappyLabel: true}

// checkContainerHeader reads the container's magic and metadata map ahead of the reader, so that a compression the reader does
// not have is named as such rather than read off the reader's error text.
func checkContainerHeader(body []byte) (string, error) {
	if !bytes.HasPrefix(body, containerMagic) {
		return DecodeNotAvroContainer, errors.New("no container magic")
	}
	header := avroBytes{data: body, offset: len(containerMagic)}
	codec := goavro.CompressionNullLabel
	for {
		count, err := header.long()
		if err != nil {
			return DecodeNotAvroContainer, err
		}
		if count == 0 {
			break
		}
		if count < 0 {
			// A negative block count is followed by the block's size in bytes.
			count = -count
			if _, err := header.long(); err != nil {
				return DecodeNotAvroContainer, err
			}
		}
		for ; count > 0; count-- {
			key, err := header.bytes()
			if err != nil {
				return DecodeNotAvroContainer, err
			}
			value, err := header.bytes()
			if err != nil {
				return DecodeNotAvroContainer, err
			}
			if string(key) == "avro.codec" {
				codec = string(value)
			}
		}
	}
	if !knownCodecs[codec] {
		return DecodeCodecUnsupported, fmt.Errorf("compression %q", codec)
	}
	if err := checkBlocks(header, codec); err != nil {
		return DecodeBlockCorrupt, err
	}
	return "", nil
}

// maxDecodedBlockBytes bounds what one snappy block may declare it decodes to. An inner message is one Databus batch, a
// few kilobytes to the topic's message limit; the bound is far above that and only stops a damaged length from allocating
// gigabytes before the block's checksum is read.
const maxDecodedBlockBytes = 64 << 20

// checkBlocks walks the container's blocks after the header: each block's size within what the message has left, room
// for the sync marker after it, and a snappy block's declared decoded length within maxDecodedBlockBytes. The reader
// allocates what a block declares before it can tell the block is damaged. Whether each marker equals the header's is the
// reader's own check, and the damage it finds reads as block_corrupt all the same; this walk only steps over them.
func checkBlocks(header avroBytes, codec string) error {
	if len(header.data)-header.offset < syncSize {
		return errors.New("no sync marker")
	}
	header.offset += syncSize
	for header.offset < len(header.data) {
		if _, err := header.long(); err != nil {
			return err
		}
		size, err := header.long()
		if err != nil {
			return err
		}
		left := int64(len(header.data) - header.offset - syncSize)
		if size < 0 || size > left {
			return fmt.Errorf("block declares %d bytes, %d left", size, left)
		}
		block := header.data[header.offset : header.offset+int(size)]
		if codec == goavro.CompressionSnappyLabel {
			decoded, ok := snappyDecodedLen(block)
			if !ok || decoded > maxDecodedBlockBytes {
				return fmt.Errorf("snappy block declares %d decoded bytes", decoded)
			}
		}
		header.offset += int(size) + syncSize
	}
	return nil
}

// syncSize is the length of the container's sync marker.
const syncSize = 16

// snappyDecodedLen is the decoded length a snappy block starts with, an unsigned varint.
func snappyDecodedLen(block []byte) (uint64, bool) {
	var value uint64
	for index := 0; index < len(block) && index < 10; index++ {
		value |= uint64(block[index]&0x7f) << (7 * uint(index))
		if block[index]&0x80 == 0 {
			return value, true
		}
	}
	return 0, false
}

// avroBytes reads Avro's zig-zag variable-length longs and length-prefixed bytes.
type avroBytes struct {
	data   []byte
	offset int
}

func (b *avroBytes) long() (int64, error) {
	var value uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if b.offset >= len(b.data) {
			return 0, errors.New("header cut short")
		}
		c := b.data[b.offset]
		b.offset++
		value |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return int64(value>>1) ^ -int64(value&1), nil
		}
	}
	return 0, errors.New("header long too long")
}

func (b *avroBytes) bytes() ([]byte, error) {
	size, err := b.long()
	if err != nil {
		return nil, err
	}
	if size < 0 || int64(len(b.data)-b.offset) < size {
		return nil, errors.New("header cut short")
	}
	out := b.data[b.offset : b.offset+int(size)]
	b.offset += int(size)
	return out, nil
}

// knownFieldTypes are the types the rules write each known row field as (design 006 C2): a field the rules name in another
// type is the container's schema, not a row's value. A field may be that type or a union of it with null.
var knownFieldTypes = map[string][]string{
	"metric": {"string"}, "dimensions": {"string"}, "target": {"string"}, "localTime": {"string"}, "dtEventTime": {"string"},
	"value": {"double", "float"}, "dtEventTimeStamp": {"long", "int"}, "time": {"long", "int"},
	envelopeIP: {"string"}, envelopeAgentID: {"string"}, envelopeHostID: {"string"}, "bk_cloud_id": {"string"},
	envelopeSupplier: {"string"}, envelopeCloud: {"string"}, envelopeCMDBLevel: {"string"},
}

// fieldType is a field's primitive type and whether it is a union with null; "" for any other shape.
func fieldType(raw json.RawMessage) (string, bool) {
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name, false
	}
	var object struct {
		Type json.RawMessage `json:"type"`
	}
	if json.Unmarshal(raw, &object) == nil && object.Type != nil {
		var primitive string
		if json.Unmarshal(object.Type, &primitive) == nil && primitive != "map" && primitive != "array" && primitive != "record" &&
			primitive != "enum" && primitive != "fixed" {
			return primitive, false
		}
		return "", false
	}
	var branches []json.RawMessage
	if json.Unmarshal(raw, &branches) != nil {
		return "", false
	}
	single := ""
	for _, branch := range branches {
		name, _ := fieldType(branch)
		switch {
		case name == "null":
		case single == "" && name != "":
			single = name
		default:
			return "", true
		}
	}
	return single, true
}

// checkBatchSchema accepts a container whose record carries its rows in `_value_`, an array of records whose known fields
// have the types the rules write (knownFieldTypes). It says which of the row's fields are unions, whose decoded values are
// their branch.
func checkBatchSchema(schema string) (map[string]bool, error) {
	var top struct {
		Type   any `json:"type"`
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schema), &top); err != nil {
		return nil, err
	}
	if top.Type != "record" {
		return nil, fmt.Errorf("top-level type %v", top.Type)
	}
	for _, field := range top.Fields {
		if field.Name != batchField {
			continue
		}
		var array struct {
			Type  string `json:"type"`
			Items struct {
				Type   string `json:"type"`
				Fields []struct {
					Name string          `json:"name"`
					Type json.RawMessage `json:"type"`
				} `json:"fields"`
			} `json:"items"`
		}
		if err := json.Unmarshal(field.Type, &array); err != nil || array.Type != "array" || array.Items.Type != "record" {
			return nil, fmt.Errorf("%s is not an array of records", batchField)
		}
		unions := map[string]bool{}
		for _, row := range array.Items.Fields {
			name, union := fieldType(row.Type)
			unions[row.Name] = union
			if want, known := knownFieldTypes[row.Name]; known && !containsType(want, name) {
				return nil, fmt.Errorf("field %s is %s, the rules write %v", row.Name, row.Type, want)
			}
		}
		return unions, nil
	}
	return nil, fmt.Errorf("no %s field", batchField)
}

func containsType(types []string, name string) bool {
	for _, known := range types {
		if known == name {
			return true
		}
	}
	return false
}

// rowOf reads a row's fields by name; a union field's branch is taken as its value (unions says which fields are unions)
// and null as absent.
func rowOf(fields map[string]any, unions map[string]bool) Row {
	var row Row
	for name, raw := range fields {
		value := raw
		if unions[name] {
			value = unionValue(raw)
		}
		switch name {
		case "metric":
			row.Metric = stringOf(value)
		case "value":
			row.Value = floatOf(value)
		case "dtEventTimeStamp":
			row.EventTimeMs = longOf(value)
		case "dimensions":
			row.Dimensions = stringOf(value)
		case "target":
			row.Target = stringOf(value)
		default:
			if text := stringOf(value); text != nil && !displayTimes[name] {
				if row.Named == nil {
					row.Named = map[string]string{}
				}
				row.Named[name] = *text
			}
		}
	}
	return row
}

// unionValue takes a decoded union's single branch (goavro decodes a non-null union value as {type name: value}).
func unionValue(raw any) any {
	if branch, ok := raw.(map[string]any); ok && len(branch) == 1 {
		for _, value := range branch {
			return value
		}
	}
	return raw
}

func stringOf(value any) *string {
	if text, ok := value.(string); ok {
		return &text
	}
	return nil
}

func floatOf(value any) *float64 {
	switch number := value.(type) {
	case float64:
		return &number
	case float32:
		widened := float64(number)
		return &widened
	}
	return nil
}

func longOf(value any) *int64 {
	switch number := value.(type) {
	case int64:
		return &number
	case int32:
		widened := int64(number)
		return &widened
	}
	return nil
}
