// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package readhold

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A group held at its limit runs no earlier-read trial: its data arrives
// past the longest hold it may have, so an earlier read agreeing with the
// formal read says both came before the data. However long it stays quiet
// and however many reads agree, the hold is not lowered, and each trial not
// run is counted as at_limit.
func TestAGroupHeldAtItsLimitIsNotTriedLower(t *testing.T) {
	c, _, now := controllerFixture(t)
	spec := groupSpec("qg", 0)
	spec.HoldLimit = 10 * time.Minute
	prepare(t, c, spec)
	observeEarly(t, c, "qg", 36*time.Minute+spec.SettlingWait, 0)
	ctx := context.Background()
	if _, err := c.SlotReadHold(ctx, scheduleFor(t, "qg", 60, nil), 120, holdFence("qg")); err != nil {
		t.Fatal(err)
	}
	if record, _ := c.Reading("qg"); !record.HeldAtLimit() || c.ReadHold("qg") != 10*time.Minute {
		t.Fatalf("fixture: record %+v hold %v, want held at the 10 minute limit", record, c.ReadHold("qg"))
	}
	*now = now.Add(200 * time.Hour)
	if hold, ok := c.EarlierCandidate("qg"); ok {
		t.Fatalf("a group held at its limit offers a %v trial", hold)
	}
	for index := range EarlierMatchesRequired {
		if err := c.EarlierRead(ctx, EarlierEvidence{Contract: holdContract("qg", 180+execution.EvaluationTime(index*60), 600_000),
			CandidateHold: 5 * time.Minute, Observed: true, Equal: true}); err != nil {
			t.Fatal(err)
		}
	}
	if c.ReadHold("qg") != 10*time.Minute {
		t.Fatalf("agreeing reads at the limit lowered the hold to %v", c.ReadHold("qg"))
	}
	if record, _ := c.Reading("qg"); record.EarlierMatches != 0 || record.PendingHoldMillis != nil && *record.PendingHoldMillis != 600_000 {
		t.Fatalf("trials at the limit were taken as evidence: %+v", record)
	}
	if got := c.Stats().EarlierSkipped; got[EarlierSkipAtLimit] != 1 || len(got) != len(EarlierSkipReasons) {
		t.Fatalf("skipped trials %v, want one at_limit", got)
	}
}

// The quiet a lowering waits for is the longest gap between read-early
// arrivals within the record's TTL window, not over its whole life: a 150 h
// gap holds every lowering to 150 h while it is within the window, and once
// it has aged out a source late every 2 h is judged by its 2 h gaps.
func TestALongGapAgesOutOfTheQuietBar(t *testing.T) {
	c, _, now := controllerFixture(t)
	prepare(t, c, groupSpec("qg", 0))
	ctx := context.Background()
	slot := execution.EvaluationTime(120)
	earlier := func() {
		t.Helper()
		for range EarlierMatchesRequired {
			slot += 60
			if err := c.EarlierRead(ctx, EarlierEvidence{Contract: holdContract("qg", slot, 150_000), CandidateHold: 75 * time.Second,
				Observed: true, Equal: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	observeEarly(t, c, "qg", 180*time.Second, 0)
	*now = now.Add(150 * time.Hour)
	observeEarly(t, c, "qg", 180*time.Second, 150_000)
	for range 3 {
		*now = now.Add(2 * time.Hour)
		observeEarly(t, c, "qg", 180*time.Second, 150_000)
	}
	*now = now.Add(3 * time.Hour)
	earlier()
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatalf("lowered to %v 3 hours after a 150 hour gap still within the window", c.ReadHold("qg"))
	}
	if reading, _ := c.Reading("qg"); quietBar(reading, now.UnixMilli()) != 150*time.Hour.Milliseconds() {
		t.Fatalf("bar within the window %v, want 150 hours (record %+v)", time.Duration(quietBar(reading, now.UnixMilli()))*time.Millisecond, reading)
	}
	for range 8 * 12 {
		*now = now.Add(2 * time.Hour)
		observeEarly(t, c, "qg", 180*time.Second, 150_000)
	}
	reading, _ := c.Reading("qg")
	if bar := quietBar(reading, now.UnixMilli()); bar != 2*time.Hour.Milliseconds() {
		t.Fatalf("bar after the 150 hour gap left the window %v, want the recent 2 hours (record %+v)", time.Duration(bar)*time.Millisecond, reading)
	}
	if len(reading.EarlyGaps) > earlyGapDays {
		t.Fatalf("kept %d days of gaps, want at most %d", len(reading.EarlyGaps), earlyGapDays)
	}
	*now = now.Add(3 * time.Hour)
	earlier()
	if c.ReadHold("qg") != 75*time.Second {
		t.Fatalf("3 quiet hours past a 2 hour bar left the hold at %v, want lowered to 75 s", c.ReadHold("qg"))
	}
}

// A record from a build that kept only the all-time longest gap is read as
// that gap ended on the day of its last read-early arrival: it holds the bar
// for the window from there and then ages out like any other.
func TestAnAllTimeGapFromAnEarlierBuildAgesOutFromItsLastArrival(t *testing.T) {
	last := 10*dayMillis + 3_600_000
	record, err := Decode([]byte(`{"hold_ms":150000,"since_slot":60,"last_early_ms":` + strconv.FormatInt(last, 10) + `,"max_early_interval_ms":540000000}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		at   int64
		want int64
	}{
		{last, 540_000_000},
		{last + int64(earlyGapDays-1)*dayMillis, 540_000_000},
		{last + int64(earlyGapDays)*dayMillis, SingleEventQuiet.Milliseconds()},
	} {
		if got := quietBar(record, test.at); got != test.want {
			t.Errorf("bar at day %d = %d, want %d", utcDay(test.at), got, test.want)
		}
	}
	gaps := withEarlyGap(earlyGapsOf(record), last+dayMillis, 7_200_000)
	if len(gaps) != 2 || gaps[0] != (EarlyGap{Day: 10, Millis: 540_000_000}) || gaps[1] != (EarlyGap{Day: 11, Millis: 7_200_000}) {
		t.Fatalf("gaps after one more %v", gaps)
	}
}

// A group restored from a record an earlier build wrote moves its all-time
// gap into EarlyGaps on its next read-early arrival, and writes the old
// field no more: the gap is kept as one ending on its last arrival's day,
// beside the gap that arrival ends.
func TestARestoredAllTimeGapMovesIntoEarlyGaps(t *testing.T) {
	c, store, now := controllerFixture(t)
	last := now.UnixMilli()
	store.values["qg"] = []byte(`{"hold_ms":150000,"since_slot":60,"segment_start":60,"last_early_ms":` + strconv.FormatInt(last, 10) +
		`,"max_early_interval_ms":540000000}`)
	prepare(t, c, groupSpec("qg", 0))
	*now = now.Add(26 * time.Hour)
	observeEarly(t, c, "qg", 180*time.Second, 150_000)
	reading, _ := c.Reading("qg")
	want := []EarlyGap{{Day: utcDay(last), Millis: 540_000_000}, {Day: utcDay(now.UnixMilli()), Millis: 26 * time.Hour.Milliseconds()}}
	if reading.MaxEarlyIntervalMillis != 0 || len(reading.EarlyGaps) != 2 || reading.EarlyGaps[0] != want[0] || reading.EarlyGaps[1] != want[1] {
		t.Fatalf("restored record after one arrival %+v, want gaps %v and no all-time gap", reading, want)
	}
	stored, err := Decode(store.values["qg"])
	if err != nil || stored.MaxEarlyIntervalMillis != 0 || len(stored.EarlyGaps) != 2 {
		t.Fatalf("written record %s (%v)", store.values["qg"], err)
	}
}

// Gaps keep the longest of each day, and only the days within the window.
func TestEarlyGapsKeepTheLongestOfEachDayInTheWindow(t *testing.T) {
	gaps := withEarlyGap(nil, 3*dayMillis, 1_000)
	gaps = withEarlyGap(gaps, 3*dayMillis+5, 4_000)
	gaps = withEarlyGap(gaps, 3*dayMillis+9, 2_000)
	if len(gaps) != 1 || gaps[0] != (EarlyGap{Day: 3, Millis: 4_000}) {
		t.Fatalf("one day's gaps %v, want its longest", gaps)
	}
	gaps = withEarlyGap(gaps, int64(3+earlyGapDays-1)*dayMillis, 1_000)
	if len(gaps) != 2 {
		t.Fatalf("a gap within the window was dropped: %v", gaps)
	}
	gaps = withEarlyGap(gaps, int64(3+earlyGapDays)*dayMillis, 1_000)
	if len(gaps) != 2 || gaps[0].Day != int64(3+earlyGapDays-1) {
		t.Fatalf("a gap past the window was kept: %v", gaps)
	}
}

// A record's gaps are at most a window of days, each a positive gap, on
// strictly later days.
func TestDecodeRefusesMalformedEarlyGaps(t *testing.T) {
	if _, err := Decode([]byte(`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":1,"ms":5},{"day":2,"ms":7}]}`)); err != nil {
		t.Fatalf("well-formed gaps refused: %v", err)
	}
	for _, raw := range []string{
		`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":-1,"ms":5}]}`,
		`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":1,"ms":0}]}`,
		`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":2,"ms":5},{"day":2,"ms":5}]}`,
		`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":3,"ms":5},{"day":2,"ms":5}]}`,
		`{"hold_ms":1,"since_slot":60,"early_gaps":[{"day":1,"ms":1},{"day":2,"ms":1},{"day":3,"ms":1},{"day":4,"ms":1},{"day":5,"ms":1},{"day":6,"ms":1},{"day":7,"ms":1},{"day":8,"ms":1}]}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Errorf("Decode(%s) accepted", raw)
		}
	}
}
