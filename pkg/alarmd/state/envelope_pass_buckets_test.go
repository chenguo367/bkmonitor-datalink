// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The second pass reports which of the four it found, and one round holds all
// of them at once.
//
// The total on its own cannot say when the migration is over: a series with no
// record at all has no frame either, so it goes through the second pass and
// keeps going through it for ever. On the release that first carried the total
// a round read 131 of 256 that way, and nothing in the number said how many of
// the 131 were the older representation. The two corruption counts were
// invisible in it for the same reason -- a damaged frame the older record
// rescued read exactly like a new series arriving, and they send a reader to
// opposite places.
//
// One round with all four shapes present, because the failure this pins is
// miscounting one shape as another: a case with a single shape in it passes
// against a counter that files everything under one bucket.
func TestTheSecondPassSaysWhichOfTheFourItFound(t *testing.T) {
	version := applyVersion()
	backend := newPipelineMemoryBackend()
	store := newBatchStore(t, backend, nil)

	const (
		stock  = iota // no frame, the envelope answers: the migration stock
		fresh         // no frame, no envelope either: a series with no record yet
		rotten        // no frame, the envelope has bytes that do not read
		saved         // frame present and corrupt, the envelope answers
		lost          // frame present and corrupt, nothing answers
		framed        // a healthy frame, which never reaches the second pass
		shapes
	)
	items := make([]execution.StatePreflightItem, shapes)
	for shape := range items {
		identity := seriesIdentity(shape)
		items[shape] = execution.StatePreflightItem{Identity: identity, ApplyVersion: version}
		envelopeKey, _ := RuntimeStateKeyV2("alarmd", identity)
		framedKey, _ := RuntimeStateKeyV3("alarmd", identity)
		switch shape {
		case stock:
			backend.values[envelopeKey], _ = encodeRuntime(seriesMutation(t, identity, version, 0, "env"), 7)
		case fresh:
			// Nothing written at all.
		case rotten:
			backend.values[envelopeKey] = []byte("not-an-envelope")
		case saved:
			backend.values[framedKey] = []byte("not-a-frame")
			backend.values[envelopeKey], _ = encodeRuntime(seriesMutation(t, identity, version, 0, "env"), 7)
		case lost:
			backend.values[framedKey] = []byte("not-a-frame")
		case framed:
			backend.values[framedKey], _ = encodeRuntimePacked(seriesMutation(t, identity, version, 0, ""), 3)
		}
	}

	loaded, err := store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: frozenRef(), Items: items})
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]struct{ have, want int }{
		"EnvelopeAnswered":    {loaded.EnvelopeAnswered, 1},
		"EnvelopeCorrupt":     {loaded.EnvelopeCorrupt, 1},
		"NoRecordYet":         {loaded.NoRecordYet, 1},
		"FrameCorruptRescued": {loaded.FrameCorruptRescued, 1},
		"FrameCorruptLost":    {loaded.FrameCorruptLost, 1},
	} {
		if got.have != got.want {
			t.Errorf("%s = %d, want %d (all five shapes are in this round exactly once)", name, got.have, got.want)
		}
	}
	// The four account for the second pass and nothing else: the healthy frame
	// is answered in the first pass and must not appear in any of them.
	if sum := loaded.EnvelopeAnswered + loaded.EnvelopeCorrupt + loaded.NoRecordYet + loaded.FrameCorruptRescued + loaded.FrameCorruptLost; sum != loaded.EnvelopeReads {
		t.Fatalf("the five sum to %d against %d series that needed the second read: a shape is being counted twice "+
			"or not at all", sum, loaded.EnvelopeReads)
	}
	if loaded.EnvelopeReads != shapes-1 {
		t.Fatalf("EnvelopeReads = %d, want %d: the healthy frame answered in the first pass and should not have "+
			"reached the second", loaded.EnvelopeReads, shapes-1)
	}
}

// The count that must reach zero is not moved by the ones that never do.
//
// This is the reading the migration is declared over on, so it gets its own
// case: a deployment that is done migrating still creates series for ever, and
// a bucket that counted those would never let anyone say the compatibility
// read can go.
func TestANewSeriesDoesNotCountAsTheOlderRepresentation(t *testing.T) {
	version := applyVersion()
	backend := newPipelineMemoryBackend()
	store := newBatchStore(t, backend, nil)
	items := make([]execution.StatePreflightItem, 3)
	for index := range items {
		items[index] = execution.StatePreflightItem{Identity: seriesIdentity(index), ApplyVersion: version}
	}

	loaded, err := store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: frozenRef(), Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.EnvelopeAnswered != 0 {
		t.Fatalf("EnvelopeAnswered = %d on a Query Group that has never written state: the migration could never be "+
			"declared over on a deployment that creates series", loaded.EnvelopeAnswered)
	}
	if loaded.NoRecordYet != len(items) || loaded.EnvelopeReads != len(items) {
		t.Fatalf("NoRecordYet = %d, EnvelopeReads = %d, want %d each: these series went through the second pass and "+
			"are the reason the total cannot be the indicator", loaded.NoRecordYet, loaded.EnvelopeReads, len(items))
	}
}
