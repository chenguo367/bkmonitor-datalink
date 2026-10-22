// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// stuckUnderFloor brings the fixture's group where the stuck groups on the test deployment were: its data measured
// landing arrival late, its trials lower its hold to none, its data lands late again right after - the hold goes
// back to where it came from and none is its search's floor - and its trials lower it halfway to that floor, where
// the gap is no wider than the step: the search has run out. It returns the hold it is stuck at.
func stuckUnderFloor(t *testing.T, f *cutoverStallFixture, arrival time.Duration) time.Duration {
	t.Helper()
	holds := f.bundle.dependencies.ReadHolds
	if lowest, raised, outcomes, overtaken, _ := heldTrialRun(t, f, arrival, 12*time.Hour, false,
		func(_, lowest, _ time.Duration, _ map[string]uint64) bool { return lowest == 0 }); lowest != 0 {
		t.Fatalf("fixture: the hold %v never came down to none: trials %v, overtaken by %v", raised, outcomes, overtaken)
	}
	steppedAt := time.Duration(-1)
	_, raised, outcomes, overtaken, _ := heldTrialRun(t, f, arrival, 12*time.Hour, false,
		func(elapsed, lowest, raised time.Duration, _ map[string]uint64) bool {
			if steppedAt < 0 && lowest < raised {
				steppedAt = elapsed
			}
			return steppedAt >= 0 && elapsed >= steppedAt+10*time.Minute
		})
	hold := holds.controller.ReadHold(f.queryGroup)
	if candidate, tries := holds.controller.EarlierCandidate(f.queryGroup); tries || hold != raised/2 {
		t.Fatalf("fixture: from %v the hold came to %v, candidate %v %v; want stuck halfway to a floor of none: trials %v, overtaken by %v",
			raised, hold, candidate, tries, outcomes, overtaken)
	}
	return hold
}

// ageFloor moves the date the group's floor was learned at back by age, in the record the controller keeps, and has
// the controller read it again: a day of standing is a day of Slots the fixture would run one by one, and the
// standing reads nothing of them but that date.
func ageFloor(t *testing.T, f *cutoverStallFixture, age time.Duration) {
	t.Helper()
	ctx := context.Background()
	key := productionPhaseTwoPrefix(f.cfg.Redis.StatePrefix, "ownership") + ":{" + ownership.ControlHashTag(f.queryGroup) + "}:" +
		productionPhaseTwoPrefix(f.cfg.Redis.StatePrefix, "schedule") + ":" + readhold.Namespace
	raw, err := f.redisClient.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	record := map[string]any{}
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if _, dated := record["earlier_floor_at_ms"]; !dated {
		t.Fatalf("fixture: the record %s has no dated floor", raw)
	}
	record["earlier_floor_at_ms"] = f.now().Add(-age).UnixMilli()
	aged, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.redisClient.Set(ctx, key, aged, redis.KeepTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.bundle.dependencies.ReadHolds.controller.RestoreBatch(ctx, []execution.QueryGroupIdentity{f.queryGroup}); err != nil {
		t.Fatal(err)
	}
}

// heldRounds runs the fixture's group as heldTrialRun does, without raising its hold first: each Slot when the
// dispatcher would run it, the early reads due started and answered before the clock moves, and the lookback's due
// rechecks read every lookbackTick of the fixture's clock, as its own loop reads them on production's. It stops
// when done says so or after span, and returns the Slots skipped unevaluated.
func heldRounds(t *testing.T, f *cutoverStallFixture, span time.Duration, done func(elapsed time.Duration) bool) (skipped int) {
	t.Helper()
	ctx := context.Background()
	engine := f.bundle.dependencies.Lookback
	const step = 5 * time.Second
	immediate := 0
	started := f.now()
	lastStep := started
	for deadline := started.Add(span); f.now().Before(deadline); {
		if done(f.now().Sub(started)) {
			return skipped
		}
		if !f.runner.NextReadyAt().After(f.now()) {
			result, _, err := f.runner.RunOne(ctx)
			if errors.Is(err, ownership.ErrStaleFence) {
				t.Fatalf("RunOne at %s: the group's lease ran out on the fixture's clock, and no Slot after it runs: %v",
					f.now().UTC().Format(time.TimeOnly), err)
			}
			if err != nil {
				t.Logf("RunOne at %s: %v", f.now().UTC().Format(time.TimeOnly), err)
			}
			if result.ReasonCode == "GAP_SKIPPED" {
				skipped++
			}
		}
		if f.now().Sub(lastStep) >= lookbackTick {
			engine.Step(ctx)
			lastStep = f.now()
		}
		engine.StepEarly(ctx)
		testwait.Until(t, "the early reads started answering", func() bool {
			busy, _ := engine.Busy(f.queryGroup)
			return busy.ReadsInFlight == 0
		})
		wake := f.runner.NextReadyAt()
		if !wake.After(f.now()) && immediate < 5 {
			immediate++
			continue
		}
		immediate = 0
		next := f.now().Add(step)
		if wake.After(f.now()) && wake.Before(next) {
			next = wake.Add(time.Millisecond)
		}
		f.clock.Store(next.UnixMilli())
	}
	t.Fatalf("the run did not end within %v", span)
	return skipped
}

// probeCounts is the probes' events and rounds on the fixture's lookback, summed over its sources, only those that
// happened.
func probeCounts(engine *lookback.Engine) (events, rounds map[string]uint64) {
	events, rounds = map[string]uint64{}, map[string]uint64{}
	for _, source := range engine.Stats().Sources {
		for event, count := range source.FloorProbes {
			if count > 0 {
				events[event] += count
			}
		}
		for outcome, count := range source.FloorProbeRounds {
			if count > 0 {
				rounds[outcome] += count
			}
		}
	}
	return events, rounds
}

func probationRereads(engine *lookback.Engine) uint64 {
	total := uint64(0)
	for _, source := range engine.Stats().Sources {
		for _, count := range source.ProbationRereads {
			total += count
		}
	}
	return total
}

// A group stuck above a floor of none, its floor stood a day, has every round read at the floor beside its formal
// read at the hold; once lookback.ProbationRounds of them read the same, the hold comes down to none. Until then
// every round is frozen at the hold - none is decided on a read at the floor - and no probation follows the
// lowering: the probe read those rounds already. A hold past the period less the reread bound has each Slot's
// round reserved at the Slot before it, and passes the same way. No Slot is skipped on the way.
func TestAStuckGroupsFloorIsProbedAndItsHoldComesDownWithNoRoundReadEarly(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name    string
		arrival time.Duration
	}{
		{"under the period less the reread bound", 96500 * time.Millisecond},
		{"past it, rounds reserved a Slot ahead", 117500 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := startCutoverFixture(t, leasesOutliveTheRun)
			engine, holds := f.bundle.dependencies.Lookback, f.bundle.dependencies.ReadHolds
			hold := stuckUnderFloor(t, f, c.arrival)
			if _, probing := holds.controller.FloorProbe(f.queryGroup); probing {
				t.Fatal("a floor learned now was probed before it stood")
			}
			ageFloor(t, f, readhold.FloorStanding)
			loweredAt, reread := time.Duration(-1), uint64(0)
			skipped := heldRounds(t, f, 8*time.Hour, func(elapsed time.Duration) bool {
				now := holds.controller.ReadHold(f.queryGroup)
				events, _ := probeCounts(engine)
				switch {
				case loweredAt < 0 && now != hold && events[lookback.FloorProbePassed] == 0:
					t.Fatalf("the hold moved to %v before the probe passed", now)
				case loweredAt < 0 && now != hold:
					loweredAt, reread = elapsed, probationRereads(engine)
				}
				return loweredAt >= 0 && elapsed >= loweredAt+30*time.Minute
			})
			events, rounds := probeCounts(engine)
			t.Logf("held %v: probe events %v, rounds %v, lowered %v into the run", hold, events, rounds, loweredAt)
			if events[lookback.FloorProbePassed] != 1 || events[lookback.FloorProbeFailed] != 0 ||
				holds.controller.Stats().FloorProbes[readhold.FloorProbeLowered] != 1 {
				t.Fatalf("probe events %v, controller %v; want one passed and lowered", events, holds.controller.Stats().FloorProbes)
			}
			if rounds[lookback.FloorProbeClean] < uint64(lookback.ProbationRounds) || rounds[lookback.FloorProbeDifferent] != 0 {
				t.Fatalf("probe rounds %v, want at least %d clean and none different", rounds, lookback.ProbationRounds)
			}
			if got := holds.controller.ReadHold(f.queryGroup); got != 0 || !holds.controller.LoweringVerified(f.queryGroup) {
				t.Fatalf("after the probe the hold is %v, verified %v; want none, verified", got, holds.controller.LoweringVerified(f.queryGroup))
			}
			if _, probation := engine.InProbation(f.queryGroup); probation || probationRereads(engine) != reread {
				t.Fatalf("in probation %v, %d rounds reread since the lowering; want none", probation, probationRereads(engine)-reread)
			}
			if skipped != 0 {
				t.Fatalf("%d Slots skipped unevaluated as the hold came down from %v", skipped, hold)
			}
		})
	}
}

// A group whose data lands near its floor - one of its series, every fifth round, twenty seconds after the window,
// past a read at the floor and before the formal read at the hold - has its probe fail at the first such round: the
// hold stays where it was, every round is frozen there, and the floor is relearned where it was, dated then, to
// stand two days before the next probe.
func TestAProbeOfAFloorTheDataLandsNearFailsAndTheFloorStandsLonger(t *testing.T) {
	t.Parallel()
	f := startCutoverFixture(t, leasesOutliveTheRun)
	var jitter atomic.Bool
	f.uqHosts = func(end int64) []string {
		if jitter.Load() && end%300 == 0 && !f.now().Before(time.Unix(end, 0).Add(20*time.Second)) {
			return []string{"127.0.0.1", "127.0.0.2"}
		}
		return []string{"127.0.0.1"}
	}
	engine, holds := f.bundle.dependencies.Lookback, f.bundle.dependencies.ReadHolds
	hold := stuckUnderFloor(t, f, 96500*time.Millisecond)
	ageFloor(t, f, readhold.FloorStanding)
	jitter.Store(true)
	failedAt := time.Time{}
	heldRounds(t, f, 4*time.Hour, func(time.Duration) bool {
		if now := holds.controller.ReadHold(f.queryGroup); now != hold {
			t.Fatalf("the hold moved to %v during the probe", now)
		}
		if events, _ := probeCounts(engine); events[lookback.FloorProbeFailed] > 0 && failedAt.IsZero() {
			failedAt = f.now()
		}
		return !failedAt.IsZero()
	})
	events, rounds := probeCounts(engine)
	t.Logf("held %v: probe events %v, rounds %v, failed at %s", hold, events, rounds, failedAt.UTC().Format(time.TimeOnly))
	if events[lookback.FloorProbeFailed] != 1 || events[lookback.FloorProbePassed] != 0 || rounds[lookback.FloorProbeDifferent] != 1 ||
		holds.controller.Stats().FloorProbes[readhold.FloorProbeRelearned] != 1 {
		t.Fatalf("probe events %v, rounds %v, controller %v; want one failed on one different round, relearned",
			events, rounds, holds.controller.Stats().FloorProbes)
	}
	record, _ := holds.controller.Reading(f.queryGroup)
	if record.EarlierFloorMillis == nil || *record.EarlierFloorMillis != 0 || record.EarlierFloorRetests != 1 ||
		record.EarlierFloorAtMillis < failedAt.Add(-time.Minute).UnixMilli() {
		t.Fatalf("after the failed probe the floor is %v, retests %d, learned at %d; want none again, once retested, dated at the failure %d",
			record.EarlierFloorMillis, record.EarlierFloorRetests, record.EarlierFloorAtMillis, failedAt.UnixMilli())
	}
	if _, probing := holds.controller.FloorProbe(f.queryGroup); probing {
		t.Fatal("the floor is probed again at once after a failed probe")
	}
}
