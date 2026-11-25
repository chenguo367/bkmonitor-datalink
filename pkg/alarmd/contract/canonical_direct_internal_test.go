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
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The direct encoders of the canonical form against the read-back they
// replace (014 next9 ⑤ §6 cases 1, 3, 4, 7), over random values.

var randomRunes = []string{"a", "Z", "0", " ", "<", ">", "&", "\"", "\\", "/", "\x00", "\x01", "\x1f", "\x7f", "\t", "\n",
	" ", " ", "é", "日", "🙂", "\xff", "\xc0", "\xed\xa0\x80", "u2028", "\\u2028"}

func randomText(rng *rand.Rand) string {
	var text strings.Builder
	for count := rng.IntN(8); count > 0; count-- {
		text.WriteString(randomRunes[rng.IntN(len(randomRunes))])
	}
	return text.String()
}

var randomFloats = []float64{0, math.Copysign(0, -1), 1e21, 1e20, 1e-7, 1e-6, 0.1, -2.5e-8, 123.456, math.MaxFloat64,
	math.SmallestNonzeroFloat64, float64(math.MaxFloat32), 1 << 53, math.NaN(), math.Inf(1)}

// randomFill sets value's exported fields at random, every kind a closed type
// has: nil and set pointers, nil, empty and full slices, numbers at their
// edges.
func randomFill(rng *rand.Rand, value reflect.Value) {
	switch value.Kind() {
	case reflect.String:
		value.SetString(randomText(rng))
	case reflect.Bool:
		value.SetBool(rng.IntN(2) == 1)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number := []int64{0, -1, 1, math.MinInt64, math.MaxInt64, rng.Int64() - rng.Int64()}[rng.IntN(6)]
		for value.OverflowInt(number) {
			number /= 7
		}
		value.SetInt(number)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		number := []uint64{0, 1, math.MaxUint64, rng.Uint64()}[rng.IntN(4)]
		for value.OverflowUint(number) {
			number /= 7
		}
		value.SetUint(number)
	case reflect.Float32, reflect.Float64:
		value.SetFloat(randomFloats[rng.IntN(len(randomFloats))] * float64(rng.IntN(3)))
	case reflect.Pointer:
		if rng.IntN(3) > 0 {
			pointer := reflect.New(value.Type().Elem())
			randomFill(rng, pointer.Elem())
			value.Set(pointer)
		}
	case reflect.Slice:
		switch rng.IntN(4) {
		case 0:
			return
		case 1:
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		default:
			slice := reflect.MakeSlice(value.Type(), 1+rng.IntN(3), 3)
			for index := 0; index < slice.Len(); index++ {
				randomFill(rng, slice.Index(index))
			}
			value.Set(slice)
		}
	case reflect.Array:
		for index := 0; index < value.Len(); index++ {
			randomFill(rng, value.Index(index))
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if field := value.Field(index); field.CanSet() {
				randomFill(rng, field)
			}
		}
	}
}

// A closed type's value written from its plan is the read-back's bytes, and
// a value the read-back refuses is not written (a NaN or an infinity): its
// refusal stays the read-back's. Every closed type of the golden table and
// those of this package that reach the encoder.
func TestThePlanWritesTheCanonicalFormOfRandomValuesOfEveryClosedType(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 2026))
	samples := []any{goldenNode{}, goldenLeaf{}, []goldenLeaf{}, &goldenNode{}, [3]int8{}, "", uint16(0), float32(0), true,
		StateCompatibilityInputV1{}, LevelDetectSemanticV1{}, []string{}, [][]float64{}}
	for _, sample := range samples {
		sampleType := reflect.TypeOf(sample)
		plan := canonicalPlanCached(sampleType)
		if plan == nil {
			t.Fatalf("%s has no plan", sampleType)
		}
		for round := 0; round < 400; round++ {
			value := reflect.New(sampleType).Elem()
			randomFill(rng, value)
			written, ok := plan.write(nil, value)
			want, err := canonicalReadBackV2(value.Interface())
			switch {
			case err != nil && ok:
				t.Fatalf("%s %#v: the plan wrote %q, the read-back refused it: %v", sampleType, value.Interface(), written, err)
			case err == nil && (!ok || !bytes.Equal(written, want)):
				t.Fatalf("%s %#v: the plan wrote %q (%t)\nwant %q", sampleType, value.Interface(), written, ok, want)
			}
		}
	}
}

// The plan orders fields by their names, as the canonical form orders an
// object's members, not by the names as written: "a b" and "a!" begin with
// "a" and go on with a byte below the closing quote, so written they would
// come before it. Each of the three as a struct's only fields, in every
// declared order.
func TestThePlanOrdersFieldsByTheirNamesNotAsWritten(t *testing.T) {
	type declared struct {
		Short  string `json:"a"`
		Spaced string `json:"a b"`
		Bang   string `json:"a!"`
	}
	type reversed struct {
		Bang   string `json:"a!"`
		Spaced string `json:"a b"`
		Short  string `json:"a"`
	}
	for _, value := range []any{declared{"1", "2", "3"}, reversed{"3", "2", "1"}, &declared{Spaced: "x"}} {
		plan := canonicalPlanCached(reflect.TypeOf(value))
		if plan == nil {
			t.Fatalf("%T has no plan", value)
		}
		written, ok := plan.write(nil, reflect.ValueOf(value))
		want, err := canonicalReadBackV2(value)
		if err != nil || !ok || !bytes.Equal(written, want) {
			t.Fatalf("%T: the plan wrote %s (%t), the read-back %s (%v)", value, written, ok, want, err)
		}
	}
}

// A type the plan does not write has none, and takes the read-back: a field
// tagged ,string or omitzero, a name two fields share, a slice of bytes, a
// map, an interface, a marshaler.
func TestATypeThePlanDoesNotWriteHasNoPlan(t *testing.T) {
	type quoted struct {
		N int `json:"n,string"`
	}
	type omitZero struct {
		N int `json:"n,omitzero"`
	}
	// Two fields of one name, made at run time: written in source, vet
	// refuses the repeated tag.
	shared := reflect.StructOf([]reflect.StructField{{Name: "A", Type: reflect.TypeOf(0), Tag: `json:"x"`},
		{Name: "B", Type: reflect.TypeOf(0), Tag: `json:"x"`}})
	type bytesNamed []byte
	type withBytes struct{ B bytesNamed }
	samples := []reflect.Type{shared}
	for _, sample := range []any{quoted{}, omitZero{}, withBytes{}, bytesNamed{}, map[string]int{}, json.RawMessage(`1`),
		[]any{}, json.Number("1"), DimensionFieldV2{}} {
		samples = append(samples, reflect.TypeOf(sample))
	}
	for _, sample := range samples {
		if plan := canonicalPlanCached(sample); plan != nil {
			t.Fatalf("%s has a plan", sample)
		}
	}
}

var randomScalars = []string{`1`, `-0`, `1.50`, `1E2`, `-2.5e-8`, `true`, `false`, `null`, `"text"`, `"a<b"`, `" "`,
	`"\\"`, `"日本"`, `  5 `, `"🙂"`, `0`, `123456789012345678901234567890`}

// The fields of a dimension identity and a record's map of scalars written by
// their shape are what CanonicalJSONV2 writes of them; a value not a scalar,
// or a name not valid UTF-8, is not written.
func TestTheDirectEncodersWriteTheCanonicalFormOfRandomFieldsAndMaps(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 2026))
	for round := 0; round < 600; round++ {
		names := map[string]bool{}
		for count := rng.IntN(5); count > 0; count-- {
			if name := randomText(rng); name != "" && !strings.ContainsAny(name, "\xff\xc0\xed") {
				names[name] = true
			}
		}
		sorted := make([]string, 0, len(names))
		for name := range names {
			sorted = append(sorted, name)
		}
		sort.Strings(sorted)
		fields := make([]DimensionFieldV2, 0, len(sorted))
		values := map[string]json.RawMessage{}
		for _, name := range sorted {
			value := json.RawMessage(randomScalars[rng.IntN(len(randomScalars))])
			fields = append(fields, DimensionFieldV2{Name: name, Value: value})
			values[name] = value
		}
		written, ok := appendDimensionFieldsV2(nil, fields)
		want, err := CanonicalJSONV2(fields)
		if err != nil || !ok || !bytes.Equal(written, want) {
			t.Fatalf("fields %+v: wrote %q (%t)\nwant %q (%v)", fields, written, ok, want, err)
		}
		written, ok = canonicalScalarMapV2(nil, values)
		want, err = CanonicalJSONV2(values)
		if err != nil || !ok || !bytes.Equal(written, want) {
			t.Fatalf("values %v: wrote %q (%t)\nwant %q (%v)", values, written, ok, want, err)
		}
	}
	if _, ok := canonicalScalarMapV2(nil, map[string]json.RawMessage{"a": json.RawMessage(`{"b":1}`)}); ok {
		t.Fatal("a map with a value not a scalar was written")
	}
	if _, ok := canonicalScalarMapV2(nil, map[string]json.RawMessage{"\xff": json.RawMessage(`1`)}); ok {
		t.Fatal("a map with a name not valid UTF-8 was written")
	}
	if written, ok := canonicalScalarMapV2(nil, nil); !ok || string(written) != "null" {
		t.Fatalf("a nil map wrote %q", written)
	}
}

// A raw value written as a scalar is what CanonicalJSONV2 writes of it, a
// refusal its refusal.
func TestCanonicalScalarIsTheCanonicalFormOfARawValue(t *testing.T) {
	corpus := append(append([]string(nil), randomScalars...), `01`, `1 2`, `1e400`, `-`, `tru`, `nul`, `{"a":1}`, `[1]`, ` `, ``,
		`"\ud800"`, "\"\xff\"", "\xef\xbb\xbf1", `1.`, `.5`, `+1`)
	for _, raw := range corpus {
		got, gotErr := CanonicalScalarV2(json.RawMessage(raw))
		want, wantErr := CanonicalJSONV2(json.RawMessage(raw))
		if !bytes.Equal(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatalf("%q: wrote %q (%v), want %q (%v)", raw, got, gotErr, want, wantErr)
		}
	}
}

// The byte-by-byte check takes the decimal pattern's set: boundaries, and
// every string of its alphabet up to five bytes.
func TestTheSignedDecimalCheckTakesWhatThePatternTakes(t *testing.T) {
	for text, want := range map[string]bool{"0": true, "-0": false, "01": false, "-1": true, "+1": false, "": false, "-": false,
		"123456789012345678901234567890": true, "١": false, "1a": false, " 1": false} {
		if got := isCanonicalSignedDecimal(text); got != want || got != canonicalSignedDecimalPattern.MatchString(text) {
			t.Fatalf("%q: %t, want %t", text, got, want)
		}
	}
	alphabet := []string{"-", "+", "0", "1", "9", "a"}
	var walk func(prefix string)
	walk = func(prefix string) {
		if isCanonicalSignedDecimal(prefix) != canonicalSignedDecimalPattern.MatchString(prefix) {
			t.Fatalf("%q: the check and the pattern disagree", prefix)
		}
		if len(prefix) == 5 {
			return
		}
		for _, next := range alphabet {
			walk(prefix + next)
		}
	}
	walk("")
}

// divergingPlanType is a closed type given a plan that writes it wrong.
type divergingPlanType struct {
	Alpha string `json:"alpha"`
	Zebra string `json:"zebra"`
}

// An encoder that writes other bytes than the canonical encoding is caught on
// a checked call: counted, said once - the encoder, the type and the field
// path at the first byte that differs, no value - and off in this process;
// the established bytes are returned then and on every call after.
func TestADivergingEncoderIsCountedSaidOnceAndTurnedOff(t *testing.T) {
	sampleType := reflect.TypeOf(divergingPlanType{})
	right, ok := buildCanonicalPlan(sampleType)
	if !ok {
		t.Fatal("fixture: no plan")
	}
	wrong := *right
	wrong.fields = []canonicalPlanField{right.fields[1], right.fields[0]}
	canonicalPlanCache.Store(sampleType, &wrong)
	shadow := canonicalEncoderShadows[CanonicalEncoderClosedType]
	stride := canonicalEncoderShadowStride
	canonicalEncoderShadowStride = 1
	var said []CanonicalEncoderDivergence
	SetCanonicalEncoderDivergenceReporter(func(divergence CanonicalEncoderDivergence) { said = append(said, divergence) })
	before := shadow.diverged.Load()
	t.Cleanup(func() {
		canonicalPlanCache.Delete(sampleType)
		canonicalEncoderShadowStride = stride
		shadow.off.Store(false)
		shadow.said.Store(false)
		SetCanonicalEncoderDivergenceReporter(nil)
	})
	value := divergingPlanType{Alpha: "secret-a", Zebra: "secret-z"}
	want, err := canonicalReadBackV2(value)
	if err != nil {
		t.Fatal(err)
	}
	for call := 0; call < 3; call++ {
		got, err := CanonicalJSONV2(value)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("call %d: %q (%v), want the established %q", call, got, err, want)
		}
	}
	if diverged := shadow.diverged.Load() - before; diverged != 1 || !shadow.off.Load() {
		t.Fatalf("diverged %d, off %t: want one, and the encoder off", diverged, shadow.off.Load())
	}
	if len(said) != 1 || said[0].Encoder != CanonicalEncoderClosedType || said[0].Type != sampleType.String() ||
		said[0].Compared != CanonicalComparedBytes || said[0].Offset != 2 || said[0].Path != "alpha" {
		t.Fatalf("said %+v, want one line naming the encoder, the type, canonical bytes compared and the path alpha at byte 2", said)
	}
	if line := fmt.Sprintf("%+v", said[0]); strings.Contains(line, "secret") {
		t.Fatalf("the line carries a value: %s", line)
	}
}
