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
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unsafe"
)

// The canonical form of a value of a closed type written straight from the
// value (014 next9 ⑤ E1).
//
// For a closed type CanonicalJSONV2 has the standard encoder write the
// value's JSON, checks it, and has the single-pass form read it back and
// write it again: keys sorted, strings re-escaped, number tokens kept. A
// closed type admits no map, interface, raw fragment, number type, byte
// slice, embedded field or marshaler of its own (canonicalClosedType), so
// what that round trip does is fixed by the type: the fields in the bytewise
// order of their JSON names, each string as appendCanonicalStringV2 writes
// it, each number as the standard encoder formats it. The plan is that
// order and those formats, built once a type; the value is written through
// it, the same bytes without the round trip.
//
// What the plan copies of the standard encoder, from go1.26.2's
// encoding/json: a field's name and its omitempty (typeFields, isValidTag,
// isEmptyValue), the float format (floatEncoder), null for a nil pointer or
// slice. What it does not copy it does not write: a type with a field tagged
// ,string or omitzero, a name two fields share, a slice of bytes (written as
// base64), has no plan; a value with a NaN or an infinity is not written.
// Those take the established path, which answers them as it always did.
//
// Two encoders of one form are two places for it to drift: the sampled
// comparison (v2_canonical_shadow.go) checks this one against the
// established path, and a change to the canonical rules changes both.

// canonicalPlan is how one type's values are written.
type canonicalPlan struct {
	kind reflect.Kind
	// bits is a float's size.
	bits int
	// fields is a struct's, in the bytewise order of their names.
	fields []canonicalPlanField
	// elem is a pointer's, a slice's or an array's element.
	elem *canonicalPlan
}

type canonicalPlanField struct {
	index int
	// name is the field's JSON name, the bytes the canonical form orders
	// members by; key is the name as the canonical form writes it, the colon
	// after it.
	name      string
	key       []byte
	omitEmpty bool
	plan      *canonicalPlan
}

// buildCanonicalPlan is the plan of a closed type, false for one the plan
// does not write.
func buildCanonicalPlan(t reflect.Type) (*canonicalPlan, bool) {
	switch t.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return &canonicalPlan{kind: t.Kind()}, true
	case reflect.Float32:
		return &canonicalPlan{kind: t.Kind(), bits: 32}, true
	case reflect.Float64:
		return &canonicalPlan{kind: t.Kind(), bits: 64}, true
	case reflect.Slice:
		// A slice of bytes the standard encoder writes as base64.
		if t.Elem().Kind() == reflect.Uint8 {
			return nil, false
		}
		fallthrough
	case reflect.Pointer, reflect.Array:
		elem, ok := buildCanonicalPlan(t.Elem())
		if !ok {
			return nil, false
		}
		return &canonicalPlan{kind: t.Kind(), elem: elem}, true
	case reflect.Struct:
		plan := &canonicalPlan{kind: reflect.Struct}
		names := map[string]bool{}
		for index := 0; index < t.NumField(); index++ {
			field := t.Field(index)
			if field.Anonymous {
				return nil, false
			}
			if !field.IsExported() {
				continue
			}
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, options, _ := strings.Cut(tag, ",")
			if !isValidJSONTagName(name) {
				name = ""
			}
			if name == "" {
				name = field.Name
			}
			omitEmpty := false
			for _, option := range strings.Split(options, ",") {
				switch option {
				case "omitempty":
					omitEmpty = true
				case "string", "omitzero":
					return nil, false
				}
			}
			if names[name] {
				return nil, false
			}
			names[name] = true
			elem, ok := buildCanonicalPlan(field.Type)
			if !ok {
				return nil, false
			}
			key := append(appendCanonicalStringV2(nil, []byte(name)), ':')
			plan.fields = append(plan.fields, canonicalPlanField{index: index, name: name, key: key, omitEmpty: omitEmpty, plan: elem})
		}
		// By the names, as the canonical form orders an object's members
		// (canonicalStreamV2 by the decoded keys), not by the written keys: a
		// name another begins with, followed there by a space or a '!' -
		// below the closing quote - comes after it written and before it as
		// itself ("a b" and "a").
		slices.SortFunc(plan.fields, func(left, right canonicalPlanField) int {
			return strings.Compare(left.name, right.name)
		})
		return plan, true
	}
	return nil, false
}

// isValidJSONTagName is encoding/json's isValidTag.
func isValidJSONTagName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", c):
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}

// isEmptyJSONValue is encoding/json's isEmptyValue.
func isEmptyJSONValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

// write appends value's canonical form to dst, false for a value the plan
// does not write.
func (plan *canonicalPlan) write(dst []byte, value reflect.Value) ([]byte, bool) {
	switch plan.kind {
	case reflect.Bool:
		if value.Bool() {
			return append(dst, "true"...), true
		}
		return append(dst, "false"...), true
	case reflect.String:
		text := value.String()
		return appendCanonicalStringV2(dst, unsafe.Slice(unsafe.StringData(text), len(text))), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(dst, value.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(dst, value.Uint(), 10), true
	case reflect.Float32, reflect.Float64:
		return appendJSONFloat(dst, value.Float(), plan.bits)
	case reflect.Pointer:
		if value.IsNil() {
			return append(dst, "null"...), true
		}
		return plan.elem.write(dst, value.Elem())
	case reflect.Slice:
		if value.IsNil() {
			return append(dst, "null"...), true
		}
		fallthrough
	case reflect.Array:
		dst = append(dst, '[')
		for index := 0; index < value.Len(); index++ {
			if index > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = plan.elem.write(dst, value.Index(index)); !ok {
				return dst, false
			}
		}
		return append(dst, ']'), true
	case reflect.Struct:
		dst = append(dst, '{')
		first := true
		for _, field := range plan.fields {
			fieldValue := value.Field(field.index)
			if field.omitEmpty && isEmptyJSONValue(fieldValue) {
				continue
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = append(dst, field.key...)
			var ok bool
			if dst, ok = field.plan.write(dst, fieldValue); !ok {
				return dst, false
			}
		}
		return append(dst, '}'), true
	}
	return dst, false
}

// appendJSONFloat is encoding/json's floatEncoder: ES6 number formatting, an
// exponent past 1e21 or under 1e-6, the exponent's leading zero dropped. A
// NaN or an infinity, which the encoder refuses, is not written.
func appendJSONFloat(dst []byte, value float64, bits int) ([]byte, bool) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return dst, false
	}
	abs := math.Abs(value)
	format := byte('f')
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) || bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	start := len(dst)
	dst = strconv.AppendFloat(dst, value, format, -1, bits)
	if format == 'e' {
		n := len(dst) - start
		if n >= 4 && dst[len(dst)-4] == 'e' && dst[len(dst)-3] == '-' && dst[len(dst)-2] == '0' {
			dst[len(dst)-2] = dst[len(dst)-1]
			dst = dst[:len(dst)-1]
		}
	}
	return dst, true
}

// canonicalPlannedV2 is the value's canonical form written from its type's
// plan, false when it has none, the encoder is off or the plan does not write
// the value. A sampled call compares it with canonicalReadBackV2 and returns
// that; one the read-back refuses is a difference, and the caller then takes
// the read-back's refusal.
func canonicalPlannedV2(value any) ([]byte, bool) {
	valueType := reflect.TypeOf(value)
	plan := canonicalPlanCached(valueType)
	if plan == nil || !canonicalEncoderOn(CanonicalEncoderClosedType) {
		return nil, false
	}
	out, ok := plan.write(make([]byte, 0, 128), reflect.ValueOf(value))
	if !ok {
		return nil, false
	}
	canonicalPlanServed.Add(1)
	if !canonicalEncoderSampled(CanonicalEncoderClosedType) {
		return out, true
	}
	established, err := canonicalReadBackV2(value)
	canonicalEncoderChecked(CanonicalEncoderClosedType, valueType.String(), out, established)
	if err != nil {
		return nil, false
	}
	return established, true
}
