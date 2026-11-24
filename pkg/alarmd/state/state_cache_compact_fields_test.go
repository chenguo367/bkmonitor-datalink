// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// framedOf frames header and points as the encoder does.
func framedOf(t *testing.T, header packedHeader, points []byte) []byte {
	t.Helper()
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(packedFrameMagic), packedFrameSchemaV1, packedFrameCodecNone)
	out = binary.AppendUvarint(out, uint64(len(headerBytes)))
	return append(append(out, headerBytes...), points...)
}

// headerLeaves visits every field of value a header's JSON carries, depth
// first: a struct's exported fields, a non-empty slice's elements, and
// anything else - an empty slice, a pointer, a scalar - as one leaf.
func headerLeaves(value reflect.Value, path string, visit func(string, reflect.Value)) {
	switch value.Kind() {
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if field := value.Type().Field(index); field.IsExported() {
				headerLeaves(value.Field(index), path+"."+field.Name, visit)
			}
		}
	case reflect.Slice:
		if value.Len() == 0 {
			visit(path, value)
			return
		}
		for index := 0; index < value.Len(); index++ {
			headerLeaves(value.Index(index), fmt.Sprintf("%s[%d]", path, index), visit)
		}
	default:
		visit(path, value)
	}
}

// vary gives a leaf another value than it has: another digest for a digest,
// a string lengthened, a number moved, a flag flipped, a pointer or a slice
// given one.
func vary(t *testing.T, path string, value reflect.Value) {
	t.Helper()
	switch value.Kind() {
	case reflect.String:
		current := value.String()
		if len(current) == 64 && strings.Trim(current, "0123456789abcdef") == "" {
			other := strings.Repeat("e", 64)
			if current == other {
				other = strings.Repeat("f", 64)
			}
			value.SetString(other)
			return
		}
		value.SetString(current + "-other")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(value.Int() + 7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(value.Uint() + 7)
	case reflect.Bool:
		value.SetBool(!value.Bool())
	case reflect.Pointer:
		if !value.IsNil() {
			t.Fatalf("%s: a pointer the base sets; vary what it points to here", path)
		}
		value.Set(reflect.New(value.Type().Elem()))
	case reflect.Slice:
		value.Set(reflect.MakeSlice(value.Type(), 1, 1))
	default:
		t.Fatalf("%s: a %s this test does not vary; give it another value here", path, value.Kind())
	}
}

// Every field a framed record's header carries is held by the compact member
// taken in from it, or declines the record by name: each one, given another
// value in turn - a field added later included, with nothing to list - is
// framed as the encoder frames it, and the member the check builds of it
// rebuilds to it and decodes to the view it decodes to; and the member a
// value written is taken in as, parsed and unchecked (014 next9 1, the
// second step), is that very member, or declines it as the check does. A
// member that drops a field - the parse, the frame or the view - fails here
// rather than at a value written unchecked. A frame only the decode refuses,
// a point count its points do not have, is not one the encoder writes.
func TestEveryFieldAFrameCarriesIsHeldByItsCompactMember(t *testing.T) {
	identity := seriesIdentity(0)
	base, _, err := encodeRuntimePackedCounted(seriesMutation(t, identity, first.version(), 0, ""), 1)
	if err != nil {
		t.Fatal(err)
	}
	header, _, points, err := parsePackedFrame(base)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(framedOf(t, header, points), base) {
		t.Fatal("fixture: the header framed again is not the encoder's frame")
	}
	held := func(name string, raw []byte) {
		t.Helper()
		checked, why := compactOf(identity, raw)
		if why == StateCacheCompactRebuiltBytes || why == StateCacheCompactRebuiltView {
			t.Errorf("%s: the compact member does not hold it (%s)", name, why)
			return
		}
		parsed, parsedWhy := compactParsed(identity, raw, sha1.Sum(raw))
		switch {
		case checked != nil && !reflect.DeepEqual(parsed, checked):
			t.Errorf("%s: taken in unchecked as %+v (%q), the check builds %+v", name, parsed, parsedWhy, checked)
		case checked == nil && why != StateCacheCompactUnreadable && parsedWhy != why:
			t.Errorf("%s: the check declines it as %s, taken in unchecked as %q", name, why, parsedWhy)
		}
	}
	if checked, why := compactOf(identity, base); checked == nil {
		t.Fatalf("fixture: the encoder's frame is not kept compact (%s)", why)
	}
	held("the encoder's frame", base)
	leaves := 0
	headerLeaves(reflect.ValueOf(&header).Elem(), "header", func(string, reflect.Value) { leaves++ })
	for leaf := 0; leaf < leaves; leaf++ {
		varied, _, _, err := parsePackedFrame(base)
		if err != nil {
			t.Fatal(err)
		}
		name, at := "", 0
		headerLeaves(reflect.ValueOf(&varied).Elem(), "header", func(path string, value reflect.Value) {
			if at == leaf {
				name = path
				vary(t, path, value)
			}
			at++
		})
		held(name, framedOf(t, varied, points))
	}
	if leaves < 20 {
		t.Fatalf("fixture: %d fields walked; want every field of the header and of its Level", leaves)
	}
}

// The first defect of the compact form a process finds is handed on every
// preflight after it, as it was found - its Plan, the kind and where the
// member parted - and on none before; each reader is handed a copy.
func TestTheFirstDefectOfTheCompactFormIsHandedOnEveryPreflightAfterIt(t *testing.T) {
	const series = 4
	rig := newCacheRig(t, 1<<20)
	load := func(s slot) execution.StatePreflightResult {
		t.Helper()
		loaded, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(),
			Items: readbackItems(s, series), OwnerFence: s.fence})
		if err != nil {
			t.Fatal(err)
		}
		return loaded
	}
	if loaded := load(first); loaded.CacheDiverged != nil {
		t.Fatalf("before any defect a preflight is handed %+v", loaded.CacheDiverged)
	}
	rig.roundFramed(first, series, 0, func(index int) bool { return index == 0 })
	want := execution.StateCacheMismatch{Plan: seriesIdentity(0).Plan, Kind: StateCacheEntryCompact, Field: StateCacheCompactRebuiltBytes}
	for _, s := range []slot{second, third} {
		rig.now = rig.now.Add(time.Minute)
		loaded := load(s)
		if loaded.CacheDiverged == nil || *loaded.CacheDiverged != want {
			t.Fatalf("the preflight at %d is handed %+v, want %+v", s.at, loaded.CacheDiverged, want)
		}
		loaded.CacheDiverged.Field = "changed by its reader"
	}
}

// writtenEntry is the entry a write of series index at s stages for the
// State cache, as the pipelined write builds it.
func writtenEntry(t *testing.T, s slot, index int, now time.Time) stateCacheEntry {
	t.Helper()
	identity := seriesIdentity(index)
	mutation := seriesMutation(t, identity, s.version(), 0, "")
	encoded, _, err := encodeRuntimePackedCounted(mutation, 1)
	if err != nil {
		t.Fatal(err)
	}
	key, err := RuntimeStateKeyV3("alarmd", identity)
	if err != nil {
		t.Fatal(err)
	}
	return stateCacheEntry{key: key, group: s.contract().Slot.QueryGroup, identity: identity, raw: encoded,
		digest: ExpectedValueDigest(encoded), applyVersion: mutation.ApplyVersion, fence: s.fence, epoch: s.epoch,
		segmentStart: s.segmentStart, slot: s.at, usableUntil: now.Add(time.Hour), planInterval: time.Minute}
}

// A value written that was taken in unchecked while another write - on a
// Slot of its own - found a defect of the compact form is kept as its
// bytes: the defect let every compact member go, and none is kept
// unchecked after it.
func TestAValueTakenInUncheckedAsADefectIsFoundIsKeptAsItsBytes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cache := newStateCache(1<<20, time.Minute, func() time.Time { return now })
	cache.applied(writtenEntry(t, second, 0, now))
	if stats := cache.stats(); stats.Entries[StateCacheEntryCompact] != 1 {
		t.Fatalf("fixture: the first value written left %v, want it compact", stats.Entries)
	}
	var entry stateCacheEntry
	for index := 1; index < 16 && entry.key == ""; index++ {
		if candidate := writtenEntry(t, second, index, now); !checkedAtWrite(candidate.raw) {
			entry = candidate
		}
	}
	if entry.key == "" {
		t.Fatal("fixture: every value written is one the rule picks")
	}
	member, checked := cache.writtenCompact(entry)
	if member == nil || checked {
		t.Fatalf("fixture: a value of a template held, not picked, was made %v, checked %v; want a member unchecked", member, checked)
	}
	cache.writtenDiverged(seriesIdentity(0), "found by another write")
	cache.keepWritten(entry, member, checked)
	if stats := cache.stats(); stats.Entries[StateCacheEntryCompact] != 0 || stats.Entries[StateCacheEntryWritten] != 1 {
		t.Fatalf("after the defect the value taken in unchecked left %v; want it kept as its bytes, no member", stats.Entries)
	}
}
