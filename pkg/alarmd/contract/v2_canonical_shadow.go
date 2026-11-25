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
	"strconv"
	"sync"
	"sync/atomic"
)

// The direct encoders of the canonical form checked against it (014 next9
// ⑤ §5): one call in canonicalEncoderShadowStride of each also takes the
// established path and compares the bytes. Any difference is counted under
// the encoder's name, said once a process in one bounded line naming the
// encoder, the Go type, what was compared and, in canonical bytes, the field
// path at the first byte that differs (a digest says only that it differs
// as a whole), and turns the encoder off in this process: every later call takes the
// established path, so what a wrong encoder wrote is bounded by the calls
// before its first sampled difference. No switch: the established path is
// always there to fall back to.

// The encoders, a closed set (CanonicalEncoders).
const (
	// CanonicalEncoderClosedType is the plan encoder of a closed type
	// (v2_canonical_plan.go).
	CanonicalEncoderClosedType = "closed_type"
	// CanonicalEncoderDimensionFields is the direct encoding of a dimension
	// identity's fields.
	CanonicalEncoderDimensionFields = "dimension_fields"
	// CanonicalEncoderRecordParts is the direct encoding of a series' shared
	// record parts and each record's id and values.
	CanonicalEncoderRecordParts = "record_parts"
	// CanonicalEncoderScalar is the direct encoding of a number or a literal
	// a query answered with.
	CanonicalEncoderScalar = "scalar"
)

// CanonicalEncoders is every encoder.
func CanonicalEncoders() []string {
	return []string{CanonicalEncoderClosedType, CanonicalEncoderDimensionFields, CanonicalEncoderRecordParts, CanonicalEncoderScalar}
}

// canonicalEncoderShadowStride is one call in this many checked. A check
// costs one established encoding, the thing the encoder saves: at one in
// 1024 it gives back about a thousandth of what the encoder saves.
var canonicalEncoderShadowStride uint64 = 1024

type canonicalEncoderShadow struct {
	tick, compared, diverged atomic.Uint64
	off                      atomic.Bool
	said                     atomic.Bool
}

var canonicalEncoderShadows = func() map[string]*canonicalEncoderShadow {
	shadows := map[string]*canonicalEncoderShadow{}
	for _, encoder := range CanonicalEncoders() {
		shadows[encoder] = &canonicalEncoderShadow{}
	}
	return shadows
}()

// canonicalEncoderOn is whether the encoder still serves in this process.
func canonicalEncoderOn(encoder string) bool {
	return !canonicalEncoderShadows[encoder].off.Load()
}

// canonicalEncoderSampled is whether this call of the encoder is checked.
func canonicalEncoderSampled(encoder string) bool {
	stride := canonicalEncoderShadowStride
	return stride != 0 && canonicalEncoderShadows[encoder].tick.Add(1)%stride == 0
}

// canonicalEncoderChecked records a checked call: served is what the
// encoder wrote, established what the established path wrote. A difference
// is counted, said once and turns the encoder off; the caller returns the
// established bytes either way.
func canonicalEncoderChecked(encoder, typeName string, served, established []byte) {
	shadow := canonicalEncoderShadows[encoder]
	shadow.compared.Add(1)
	if bytes.Equal(served, established) {
		return
	}
	shadow.diverged.Add(1)
	shadow.off.Store(true)
	if !shadow.said.CompareAndSwap(false, true) {
		return
	}
	offset := firstDifference(served, established)
	reportCanonicalEncoderDivergence(CanonicalEncoderDivergence{Encoder: encoder, Type: boundedText(typeName, 120),
		Compared: CanonicalComparedBytes, Offset: offset, Path: canonicalPathAt(established, offset)})
}

// canonicalDigestChecked counts a check of encoder that compares digests, not
// canonical bytes - the record parts answer to the series digest's own
// check: the digest assembled from them against the one the canonical
// encoding of the records gives. A difference is said as the digest's, a
// whole that differs: the offset is the hex string's and names no field,
// and the path is empty. A difference of the dimension fields or scalar
// encoders is named, field and all, on their own checks; one in the record
// parts' own writes - the values and dimensions maps, the record and business
// ids - is said only here, as the digest's.
func canonicalDigestChecked(encoder, typeName, served, established string) {
	shadow := canonicalEncoderShadows[encoder]
	shadow.compared.Add(1)
	if served == established {
		return
	}
	shadow.diverged.Add(1)
	shadow.off.Store(true)
	if !shadow.said.CompareAndSwap(false, true) {
		return
	}
	reportCanonicalEncoderDivergence(CanonicalEncoderDivergence{Encoder: encoder, Type: boundedText(typeName, 120),
		Compared: CanonicalComparedDigest, Offset: firstDifference([]byte(served), []byte(established))})
}

// firstDifference is the index of the first byte the two differ at, or the
// shorter's length.
func firstDifference(served, established []byte) int {
	offset := 0
	for offset < len(served) && offset < len(established) && served[offset] == established[offset] {
		offset++
	}
	return offset
}

// CanonicalEncoderCounts is, by encoder, how many calls were checked and on
// how many the encoder differed from the established path.
func CanonicalEncoderCounts() map[string][2]uint64 {
	counts := make(map[string][2]uint64, len(canonicalEncoderShadows))
	for encoder, shadow := range canonicalEncoderShadows {
		counts[encoder] = [2]uint64{shadow.compared.Load(), shadow.diverged.Load()}
	}
	return counts
}

// CanonicalEncoderDivergence is the first difference an encoder made in this
// process: the encoder, the Go type it encoded, what was compared, the byte
// at which the two first differ and the field path there, in the
// established encoding - canonical bytes (CanonicalComparedBytes), or a
// digest (CanonicalComparedDigest), whose offset is its hex string's and
// which has no path: the digest differs as a whole. No value of the input is
// in it.
type CanonicalEncoderDivergence struct {
	Encoder  string
	Type     string
	Compared string
	Offset   int
	Path     string
}

// What a divergence compared (CanonicalEncoderDivergence.Compared).
const (
	CanonicalComparedBytes  = "canonical_bytes"
	CanonicalComparedDigest = "digest"
)

var (
	canonicalDivergenceMu       sync.Mutex
	canonicalDivergenceReporter func(CanonicalEncoderDivergence)
)

// SetCanonicalEncoderDivergenceReporter installs what hears of an encoder's
// first difference. Nil removes it.
func SetCanonicalEncoderDivergenceReporter(report func(CanonicalEncoderDivergence)) {
	canonicalDivergenceMu.Lock()
	defer canonicalDivergenceMu.Unlock()
	canonicalDivergenceReporter = report
}

func reportCanonicalEncoderDivergence(divergence CanonicalEncoderDivergence) {
	canonicalDivergenceMu.Lock()
	report := canonicalDivergenceReporter
	canonicalDivergenceMu.Unlock()
	if report != nil {
		report(divergence)
	}
}

// boundedText is text cut to at most limit bytes.
func boundedText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}

// canonicalPathAt is the path, in canonical JSON doc, of the value the byte
// at offset belongs to: object keys and array indexes from the root, as in
// points[3].levels[0].value, at most 160 bytes. Keys are the document's own;
// no value's text is in it. A document that is not canonical JSON gives the
// path as far as it could be read.
func canonicalPathAt(doc []byte, offset int) string {
	type frame struct {
		array bool
		index int
		key   string
	}
	var stack []frame
	path := func() string {
		var out []byte
		for _, level := range stack {
			if level.array {
				out = append(out, '[')
				out = strconv.AppendInt(out, int64(level.index), 10)
				out = append(out, ']')
				continue
			}
			if level.key == "" {
				continue
			}
			if len(out) > 0 {
				out = append(out, '.')
			}
			out = append(out, level.key...)
		}
		return boundedText(string(out), 160)
	}
	for position := 0; position < len(doc) && position < offset; position++ {
		switch doc[position] {
		case '{':
			stack = append(stack, frame{})
		case '[':
			stack = append(stack, frame{array: true})
		case '}', ']':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case ',':
			if len(stack) > 0 {
				if stack[len(stack)-1].array {
					stack[len(stack)-1].index++
				} else {
					stack[len(stack)-1].key = ""
				}
			}
		case '"':
			end := position + 1
			for end < len(doc) && doc[end] != '"' {
				if doc[end] == '\\' {
					end++
				}
				end++
			}
			text := ""
			if end <= len(doc) {
				text = string(doc[position+1 : min(end, len(doc))])
			}
			// A key is a string followed by a colon.
			if end+1 < len(doc) && doc[end+1] == ':' && len(stack) > 0 && !stack[len(stack)-1].array {
				stack[len(stack)-1].key = text
			}
			if end >= offset {
				return path()
			}
			position = end
		}
	}
	return path()
}
