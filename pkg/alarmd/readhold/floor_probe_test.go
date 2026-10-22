// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package readhold

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// stuckGroup is a group whose search below its hold has run out, as the
// stuck groups on the test deployment were: held 120 s, its floor at 100 s
// learned at the fixture's start, the data a 30 s step - no candidate is left
// between the two.
func stuckGroup(t *testing.T, record string) (*Controller, *memoryControl, *time.Time) {
	t.Helper()
	c, store, now := controllerFixture(t)
	spec := groupSpec("qg", 0)
	spec.Step = 30 * time.Second
	if record == "" {
		record = fmt.Sprintf(`{"hold_ms":120000,"since_slot":60,"lowered":true,"lowered_from_ms":240000,`+
			`"earlier_floor_ms":100000,"earlier_floor_at_ms":%d}`, now.UnixMilli())
	}
	store.values["qg"] = []byte(record)
	prepare(t, c, spec)
	if got, tries := c.EarlierCandidate("qg"); tries {
		t.Fatalf("fixture: candidate %v, want the search run out", got)
	}
	return c, store, now
}

func probeEnd(t *testing.T, c *Controller, slot execution.EvaluationTime, floor, hold time.Duration, passed bool) {
	t.Helper()
	if err := c.FloorProbeResult(context.Background(), FloorProbeEvidence{Contract: holdContract("qg", slot, hold.Milliseconds()),
		Floor: floor, Hold: hold, Passed: passed}); err != nil {
		t.Fatal(err)
	}
}

// A floor stands a day before its first probe; each probe that fails
// relearns it where it was and doubles how long it stands - two days, four,
// then the week a record lives, and no longer.
func TestAFloorStandsADayAndEachFailedProbeDoublesIt(t *testing.T) {
	c, _, now := stuckGroup(t, "")
	for index, standing := range []time.Duration{24 * time.Hour, 48 * time.Hour, 96 * time.Hour, 7 * 24 * time.Hour, 7 * 24 * time.Hour} {
		learned := *now
		*now = learned.Add(standing - time.Millisecond)
		if floor, probing := c.FloorProbe("qg"); probing {
			t.Fatalf("probe %d: probed at %v a millisecond before the floor stood %v", index, floor, standing)
		}
		*now = learned.Add(standing)
		floor, probing := c.FloorProbe("qg")
		if !probing || floor != 100*time.Second {
			t.Fatalf("probe %d: %v %v once the floor stood %v, want the floor at 100 s", index, floor, probing, standing)
		}
		probeEnd(t, c, execution.EvaluationTime(600+index*60), 100*time.Second, 120*time.Second, false)
		record, _ := c.Reading("qg")
		if record.EarlierFloorMillis == nil || *record.EarlierFloorMillis != 100_000 || record.EarlierFloorAtMillis != now.UnixMilli() ||
			c.ReadHold("qg") != 120*time.Second {
			t.Fatalf("probe %d failed: floor %v at %d, hold %v; want the floor where it was, dated now, the hold unmoved",
				index, record.EarlierFloorMillis, record.EarlierFloorAtMillis, c.ReadHold("qg"))
		}
	}
	if got := c.Stats().FloorProbes[FloorProbeRelearned]; got != 5 {
		t.Fatalf("relearned %d, want 5", got)
	}
}

// A probe that passes lowers the hold to the floor - pending, frozen a step
// a Slot as any lowering is - marked verified, so no probation follows it and
// a process that takes the group over resumes none: LastLowering has none.
// The floor is the hold now, and the search goes on below it from half.
func TestAProbeThatPassesLowersTheHoldToItsFloorVerified(t *testing.T) {
	c, store, now := stuckGroup(t, "")
	*now = now.Add(FloorStanding)
	if _, probing := c.FloorProbe("qg"); !probing {
		t.Fatal("fixture: no probe once the floor stood")
	}
	writes := len(store.requests)
	probeEnd(t, c, 600, 100*time.Second, 120*time.Second, true)
	if got := len(store.requests) - writes; got != 1 {
		t.Fatalf("a passed probe wrote %d times, want once", got)
	}
	record, _ := c.Reading("qg")
	if c.ReadHold("qg") != 100*time.Second || !record.Lowered || !record.LoweredVerified || record.LoweredFromMillis != 120_000 {
		t.Fatalf("after a passed probe: hold %v, lowered %v verified %v from %d; want 100 s, verified, from 120 s",
			c.ReadHold("qg"), record.Lowered, record.LoweredVerified, record.LoweredFromMillis)
	}
	if record.EarlierFloorMillis != nil || record.EarlierFloorRetests != 0 {
		t.Fatalf("the floor %v, retests %d after the hold came down to it; want none", record.EarlierFloorMillis, record.EarlierFloorRetests)
	}
	if got, tries := c.EarlierCandidate("qg"); !tries || got != 50*time.Second {
		t.Fatalf("the search below the new hold tries %v %v, want half of 100 s", got, tries)
	}
	if !c.LoweringVerified("qg") || c.Stats().FloorProbes[FloorProbeLowered] != 1 {
		t.Fatalf("verified %v, lowered counted %d", c.LoweringVerified("qg"), c.Stats().FloorProbes[FloorProbeLowered])
	}
	if _, err := c.SlotReadHold(context.Background(), scheduleEvery(t, "qg", 60, 60), 660, holdFence("qg")); err != nil {
		t.Fatal(err)
	}
	if slot, lowered := c.LastLowering("qg"); lowered {
		t.Fatalf("a verified lowering reads as one to resume probation from, at %d", slot)
	}
	// A raise undoes it as it does any lowering, and the next is not verified.
	observeEarly(t, c, "qg", 200*time.Second, 100_000)
	if record, _ := c.Reading("qg"); record.LoweredVerified || c.LoweringVerified("qg") {
		t.Fatal("a raise after the verified lowering left it verified")
	}
}

// A trial's lowering is never verified: probation follows it as before, and
// a process that takes the group over resumes that probation.
func TestATrialsLoweringIsNotVerified(t *testing.T) {
	c, _, now := controllerFixture(t)
	spec := groupSpec("qg", 0)
	spec.Step = time.Minute
	prepare(t, c, spec)
	observeEarly(t, c, "qg", 10*time.Minute+spec.SettlingWait, 0)
	*now = now.Add(time.Hour)
	for index := range EarlierMatchesRequired {
		if err := c.EarlierRead(context.Background(), EarlierEvidence{Contract: holdContract("qg", execution.EvaluationTime(120+index), 600_000),
			CandidateHold: 5 * time.Minute, Observed: true, Equal: true}); err != nil {
			t.Fatal(err)
		}
	}
	if c.ReadHold("qg") != 5*time.Minute || c.LoweringVerified("qg") {
		t.Fatalf("a trial's lowering: hold %v verified %v, want 5 minutes, not verified", c.ReadHold("qg"), c.LoweringVerified("qg"))
	}
}

// A probe's end is taken only against the record it read under: a hold or
// floor not the probe's, or a search reopened by a raise, changes nothing and
// is counted stale.
func TestAProbeEndAgainstAnotherRecordIsStale(t *testing.T) {
	for _, test := range []struct {
		name  string
		move  func(c *Controller)
		floor time.Duration
		hold  time.Duration
	}{
		{"another floor", nil, 90 * time.Second, 120 * time.Second},
		{"another hold", nil, 100 * time.Second, 130 * time.Second},
		{"raised since", func(c *Controller) { observeEarly(t, c, "qg", 400*time.Second, 120_000) }, 100 * time.Second, 120 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, store, now := stuckGroup(t, "")
			*now = now.Add(FloorStanding)
			if test.move != nil {
				test.move(c)
			}
			before, writes := c.ReadHold("qg"), len(store.requests)
			probeEnd(t, c, 600, test.floor, test.hold, true)
			if c.ReadHold("qg") != before || len(store.requests) != writes || c.Stats().FloorProbes[FloorProbeStale] != 1 {
				t.Fatalf("a stale end: hold %v (was %v), %d writes, stale %d; want nothing changed and one stale",
					c.ReadHold("qg"), before, len(store.requests)-writes, c.Stats().FloorProbes[FloorProbeStale])
			}
		})
	}
}

// A floor's failed probes are forgotten whenever the floor is: a raise clears
// it, a floor taking another value starts over, and the same value keeps its
// count.
func TestAFloorsStandingStartsOverWhenItIsClearedOrChanges(t *testing.T) {
	t.Run("raise", func(t *testing.T) {
		c, _, now := stuckGroup(t, "")
		*now = now.Add(FloorStanding)
		probeEnd(t, c, 600, 100*time.Second, 120*time.Second, false)
		observeEarly(t, c, "qg", 400*time.Second, 120_000)
		if record, _ := c.Reading("qg"); record.EarlierFloorRetests != 0 {
			t.Fatalf("retests %d after a raise, want none", record.EarlierFloorRetests)
		}
	})
	t.Run("value", func(t *testing.T) {
		floor := int64(100_000)
		record := Record{EarlierFloorMillis: &floor, EarlierFloorRetests: 2}
		setFloor(&record, 100_000, 5)
		if record.EarlierFloorRetests != 2 {
			t.Fatalf("the same floor relearned: retests %d, want 2 kept", record.EarlierFloorRetests)
		}
		setFloor(&record, 110_000, 6)
		if record.EarlierFloorRetests != 0 {
			t.Fatalf("a floor of another value: retests %d, want none", record.EarlierFloorRetests)
		}
	})
}

// A floor written before floors were dated is dated at its first reading,
// within the day before it, at a point its group's identity fixes: the
// groups such floors hold start their probes spread over the first day, not
// in the first round together, and a group starts at the same point in any
// process that reads it at the same moment.
func TestAnUndatedFloorIsProbedWithinADayAtAPointItsGroupFixes(t *testing.T) {
	undated := `{"hold_ms":120000,"since_slot":60,"earlier_floor_ms":100000}`
	eligible := func(qg execution.QueryGroupIdentity) time.Duration {
		t.Helper()
		c, store, now := controllerFixture(t)
		spec := groupSpec(qg, 0)
		spec.Step = 30 * time.Second
		store.values[qg] = []byte(undated)
		prepare(t, c, spec)
		start := *now
		for step := time.Duration(0); step <= FloorStanding; step += 10 * time.Minute {
			*now = start.Add(step)
			if _, probing := c.FloorProbe(qg); probing {
				return step
			}
		}
		t.Fatalf("%s: an undated floor not probed within a day of its first reading", qg)
		return 0
	}
	hours := map[time.Duration]bool{}
	for index := range 24 {
		qg := execution.QueryGroupIdentity(fmt.Sprintf("qg-%d", index))
		at := eligible(qg)
		if again := eligible(qg); again != at {
			t.Fatalf("%s probed %v after its first reading in one process and %v in another", qg, at, again)
		}
		hours[at.Truncate(time.Hour)] = true
	}
	if len(hours) < 8 {
		t.Fatalf("24 groups' first probes fell in %d hours of the day, want them spread", len(hours))
	}
}

// A record's failed probes and its verified lowering decode, and a negative
// count is no record.
func TestAFloorsProbeFieldsDecode(t *testing.T) {
	record, err := Decode([]byte(`{"hold_ms":100000,"since_slot":60,"earlier_floor_retests":2,"lowered":true,"lowered_verified":true}`))
	if err != nil || record.EarlierFloorRetests != 2 || !record.LoweredVerified {
		t.Fatalf("decoded %+v %v", record, err)
	}
	if _, err := Decode([]byte(`{"hold_ms":100000,"since_slot":60,"earlier_floor_retests":-1}`)); err == nil {
		t.Fatal("a negative retest count decoded")
	}
}
