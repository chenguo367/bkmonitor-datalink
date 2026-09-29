// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"math"
	"testing"
	"unsafe"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// An object whose window reaches back a hundred rounds keeps a hundred
// rounds, where sixteen used to leave the older holes NOT_IN_MEMORY and the
// row this side's for good. The rounds kept are every one from where the
// worker says the windows start, and none older.
func TestAnObjectKeepsTheRoundsItsWindowsReachBackTo(t *testing.T) {
	tracker := newTracker(t, &clock{at: now})
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-long"})
	const period, first = int64(60), int64(6000)
	// A hundred and twenty rounds, the windows reaching back a hundred.
	for i := int64(0); i < 120; i++ {
		end := first + i*period
		round(ctx, tracker, end+period, "FULL_COMPLETED", "", "", primary("FULL", "DATA"),
			&observability.HistoryCoverageFacts{Levels: 1, End: end, WindowStart: end - 99*period})
	}
	last := first + 119*period
	start := last - 99*period
	short := &observability.HistoryCoverageFacts{Levels: 1, Short: 1, WorstValid: 97, WorstRequired: 100, End: last, WindowStart: start,
		Windows: []observability.HistoryWindowFact{{Series: "c", Level: 1, Valid: 97, Required: 100, End: last,
			Missing: []int64{start, start + period, last - period}, MissingTotal: 3}}}
	for i := 0; i < 10; i++ {
		round(ctx, tracker, last+period, "COMPLETED_WITH_UNAVAILABLE", "LEVEL_OUTCOME_UNKNOWN", "HISTORY_GAPPED", primary("FULL", "DATA"), short)
	}
	rows := anyColumn(tracker)
	if len(rows) != 1 || rows[0].Coverage == nil || len(rows[0].Coverage.Windows) != 1 {
		t.Fatalf("rows = %+v, want the one object with its one named window", rows)
	}
	coverage := rows[0].Coverage
	if got := coverage.Windows[0].HolesBy; got != (WindowHoleCounts{AnsweredWithoutSeries: 3}) {
		t.Fatalf("holes by cause = %+v, want every minute of the hundred-round window read against its round", got)
	}
	// The hundred rounds from the window start to the last minute, and the
	// ten short rounds of that same minute after them.
	if coverage.RoundsRemembered != 110 || coverage.RoundsKept != coverage.RoundsRemembered {
		t.Fatalf("rounds remembered/kept = %d/%d, want 110 kept by the window", coverage.RoundsRemembered, coverage.RoundsKept)
	}
	state := tracker.groups["qg-long"]
	if state == nil || state.rounds[0].end != start {
		t.Fatalf("the oldest round kept is at %d, want the window start %d", state.rounds[0].end, start)
	}
}

// Rounds are kept in minute order, whatever order they arrive in: a Slot
// replayed after later ones lands at its minute and is found there.
func TestAReplayedRoundLandsAtItsMinute(t *testing.T) {
	state := &queryGroupState{}
	for _, end := range []int64{600, 660, 720} {
		rememberRound(state, end+60, "FULL_COMPLETED", "", &observability.HistoryCoverageFacts{Levels: 1, End: end, WindowStart: 300},
			primary("FULL", "DATA"))
	}
	rememberRound(state, 540, "COMPLETED_WITH_UNAVAILABLE", "QUERY_TIMEOUT", &observability.HistoryCoverageFacts{Levels: 1, End: 480},
		primary("PARTIAL", "DATA"))
	for i := 1; i < len(state.rounds); i++ {
		if state.rounds[i-1].end > state.rounds[i].end {
			t.Fatalf("rounds out of minute order: %+v", state.rounds)
		}
	}
	got, found := roundAt(state.rounds, 480)
	if !found || got.answer != primaryNotWhole || got.reasonWord() != "QUERY_TIMEOUT" {
		t.Fatalf("round at 480 = %+v (found %v), want the replayed incomplete round", got, found)
	}
	// Past the window start nothing older is kept; the replayed round is
	// dropped once the windows start after it.
	rememberRound(state, 840, "FULL_COMPLETED", "", &observability.HistoryCoverageFacts{Levels: 1, End: 780, WindowStart: 600},
		primary("FULL", "DATA"))
	if _, found := roundAt(state.rounds, 480); found || state.rounds[0].end != 600 {
		t.Fatalf("rounds = %+v, want none before the window start 600", state.rounds)
	}
}

// A remembered round is sixteen bytes and holds no pointer: a day-long
// window keeps a day of them per object.
func TestARememberedRoundIsSixteenBytes(t *testing.T) {
	if size := unsafe.Sizeof(roundMark{}); size != 16 {
		t.Fatalf("roundMark is %d bytes, want 16", size)
	}
}

// The words a round is filed under are held once for the process: the same
// word is the same index, and a table that has run out of indexes reads a
// new word as none rather than overwriting one.
func TestTheRoundWordsAreHeldOnce(t *testing.T) {
	table := &roundWords{index: map[string]uint16{"": 0}, words: []string{""}}
	first, again := table.of("QUERY_TIMEOUT"), table.of("QUERY_TIMEOUT")
	if first == 0 || first != again || table.word(first) != "QUERY_TIMEOUT" {
		t.Fatalf("indexes %d and %d for one word, reading back %q", first, again, table.word(first))
	}
	for len(table.words) <= math.MaxUint16 {
		table.words = append(table.words, "")
	}
	if index := table.of("A_WORD_TOO_MANY"); index != 0 || table.word(first) != "QUERY_TIMEOUT" {
		t.Fatalf("a full table gave index %d and read the first word back as %q", index, table.word(first))
	}
}

// The memory reading counts each object under the least bucket its kept
// rounds fit, the edges inclusive; the bytes are the slices' capacity, what
// the heap holds, not their length; and only the objects whose worker named
// a window start count as window-sized.
func TestTheRoundMemoryReadingCountsEachObjectOnce(t *testing.T) {
	tracker := newTracker(t, &clock{at: now})
	kept := map[string][2]int{
		"a": {16, 16}, "b": {17, 32}, "c": {64, 64}, "d": {65, 128},
		"e": {256, 256}, "f": {257, 512}, "g": {1440, 1440}, "h": {1441, 2048},
	}
	for key, lengths := range kept {
		tracker.groups[key] = &queryGroupState{rounds: make([]roundMark, lengths[0], lengths[1])}
	}
	tracker.groups["a"].windowStart = 0
	tracker.groups["b"].windowStart = 600
	tracker.groups["h"].windowStart = 60
	got := tracker.RoundMemory()
	want := map[string]int{"le_16": 1, "le_64": 2, "le_256": 2, "le_1440": 2, "gt_1440": 1}
	for _, bucket := range RoundMemoryBuckets {
		if got.Objects[bucket] != want[bucket] {
			t.Fatalf("objects = %v, want %v", got.Objects, want)
		}
	}
	rounds, capacity := 0, 0
	for _, lengths := range kept {
		rounds += lengths[0]
		capacity += lengths[1]
	}
	if got.Rounds != rounds || got.Bytes != uint64(capacity)*16 || got.MaxRounds != 1441 || got.WindowSized != 2 {
		t.Fatalf("reading = %+v, want %d rounds, %d bytes, most 1441, two window-sized", got, rounds, capacity*16)
	}
	if empty := (*Tracker)(nil).RoundMemory(); len(empty.Objects) != len(RoundMemoryBuckets) || empty.Rounds != 0 {
		t.Fatalf("nil tracker reading = %+v, want every bucket at zero", empty)
	}
}
