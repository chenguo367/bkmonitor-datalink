// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

import (
	"encoding/json"
	"slices"
	"unicode/utf8"
	"unsafe"
)

// The canonical form of the per-series shapes that carry raw JSON, written by
// their fixed shape (014 next9 ⑤ E2, E3): a dimension identity's fields, and
// a record's values and dimensions - a map of names to scalars. Each scalar
// is what the single-pass form writes of it, the form CanonicalJSONV2 reads
// every value back with; a value that is not a scalar, or anything the
// single-pass form declines, takes CanonicalJSONV2 whole. Checked against it
// one call in 1024 (v2_canonical_shadow.go).

// stringBytes is text's bytes, read only, not copied.
func stringBytes(text string) []byte {
	return unsafe.Slice(unsafe.StringData(text), len(text))
}

// canonicalDimensionFieldsV2 is CanonicalJSONV2(fields) for fields whose
// names EncodeDimensionIdentityV2 checked - non-empty valid UTF-8, sorted,
// unique - and whose values it checked are scalars.
func canonicalDimensionFieldsV2(fields []DimensionFieldV2) ([]byte, error) {
	if !canonicalEncoderOn(CanonicalEncoderDimensionFields) {
		return CanonicalJSONV2(fields)
	}
	out, ok := appendDimensionFieldsV2(make([]byte, 0, 2+48*len(fields)), fields)
	if !ok {
		return CanonicalJSONV2(fields)
	}
	if !canonicalEncoderSampled(CanonicalEncoderDimensionFields) {
		return out, nil
	}
	established, err := CanonicalJSONV2(fields)
	canonicalEncoderChecked(CanonicalEncoderDimensionFields, "[]contract.DimensionFieldV2", out, established)
	return established, err
}

// appendDimensionFieldsV2 is the fields as the canonical form writes them:
// an array of objects, name before value.
func appendDimensionFieldsV2(dst []byte, fields []DimensionFieldV2) ([]byte, bool) {
	dst = append(dst, '[')
	for index, field := range fields {
		if index > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"name":`...)
		dst = appendCanonicalStringV2(dst, stringBytes(field.Name))
		dst = append(dst, `,"value":`...)
		var ok bool
		if dst, ok = canonicalStreamV2(dst, field.Value); !ok {
			return dst, false
		}
		dst = append(dst, '}')
	}
	return append(dst, ']'), true
}

// canonicalScalarMapV2 is CanonicalJSONV2(values) for a map of names to
// scalars, false for one it does not write: a value not a scalar, a name not
// valid UTF-8 (whose canonical order is that of its replacement).
func canonicalScalarMapV2(dst []byte, values map[string]json.RawMessage) ([]byte, bool) {
	if values == nil {
		return append(dst, "null"...), true
	}
	names := make([]string, 0, len(values))
	for name := range values {
		if !utf8.ValidString(name) {
			return dst, false
		}
		names = append(names, name)
	}
	slices.Sort(names)
	dst = append(dst, '{')
	for index, name := range names {
		if index > 0 {
			dst = append(dst, ',')
		}
		dst = appendCanonicalStringV2(dst, stringBytes(name))
		dst = append(dst, ':')
		value := values[name]
		if !isCanonicalScalarJSONV2(value) {
			return dst, false
		}
		var ok bool
		if dst, ok = canonicalStreamV2(dst, value); !ok {
			return dst, false
		}
	}
	return append(dst, '}'), true
}

// canonicalRecordPartV2 is the canonical form of one of a record's parts -
// its values or dimensions - written directly while the record parts
// encoder is on, CanonicalJSONV2 otherwise or for a map it does not write.
func canonicalRecordPartV2(values map[string]json.RawMessage) ([]byte, error) {
	if canonicalEncoderOn(CanonicalEncoderRecordParts) {
		if out, ok := canonicalScalarMapV2(make([]byte, 0, 64), values); ok {
			return out, nil
		}
	}
	return CanonicalJSONV2(values)
}

// canonicalRecordStringV2 is the canonical form of a record's id or business
// id, a string, written directly while the record parts encoder is on.
func canonicalRecordStringV2(text string) ([]byte, error) {
	if canonicalEncoderOn(CanonicalEncoderRecordParts) {
		return appendCanonicalStringV2(make([]byte, 0, len(text)+2), stringBytes(text)), nil
	}
	return CanonicalJSONV2(text)
}

// CanonicalScalarV2 is CanonicalJSONV2(raw) for a value a query answered
// with: a number or a literal written as the single-pass form writes it,
// anything else - a string, a structure, what the form declines - by
// CanonicalJSONV2. A number or a literal is ASCII, so the checks
// CanonicalJSONV2 makes before the single-pass form (UTF-8, a byte order
// mark, surrogate escapes) pass of themselves.
func CanonicalScalarV2(raw json.RawMessage) ([]byte, error) {
	if canonicalEncoderOn(CanonicalEncoderScalar) && startsNumberOrLiteral(raw) {
		if out, ok := canonicalStreamV2(make([]byte, 0, len(raw)), raw); ok {
			if !canonicalEncoderSampled(CanonicalEncoderScalar) {
				return out, nil
			}
			established, err := CanonicalJSONV2(raw)
			canonicalEncoderChecked(CanonicalEncoderScalar, "json.RawMessage", out, established)
			return established, err
		}
	}
	return CanonicalJSONV2(raw)
}

// startsNumberOrLiteral is whether raw's first byte past JSON whitespace
// begins a number or a literal.
func startsNumberOrLiteral(raw []byte) bool {
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '-', 't', 'f', 'n', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return true
		}
		return false
	}
	return false
}

// isCanonicalSignedDecimal is canonicalSignedDecimalPattern,
// ^(?:0|-?[1-9][0-9]*)$, byte by byte: zero, or a non-zero digit after an
// optional minus and digits after it.
func isCanonicalSignedDecimal(text string) bool {
	if text == "0" {
		return true
	}
	if len(text) > 0 && text[0] == '-' {
		text = text[1:]
	}
	if len(text) == 0 || text[0] < '1' || text[0] > '9' {
		return false
	}
	for index := 1; index < len(text); index++ {
		if text[index] < '0' || text[index] > '9' {
			return false
		}
	}
	return true
}
