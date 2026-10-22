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
	"errors"
	"hash/fnv"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// FloorStanding is how long a floor stands before a shadow probe reads the
// floor itself: the search below the hold has run out
// (execution.CandidateReadHold) and the probes between the floor and the
// hold have come within MidpointResolution of it (probeCandidate), and the
// floor - the largest hold shown too early - may no longer be too early now
// that the data comes in sooner. Each such probe that fails doubles the
// standing, up to RecordTTL.
//
// A probe reads every round at the floor beside the formal read at the hold,
// which stays where it is, and lowers the hold to the floor only once
// lookback.ProbationRounds rounds in a row read the same at both: no round is
// decided on data read early to find out (the design of the floor's shadow
// probe, development-designs 004).
const FloorStanding = 24 * time.Hour

// How the controller took a shadow probe's end, closed: lowered the hold to
// the hold the probe read clean; floor_raised, a round read differently at a
// hold between the floor and the hold, which is the floor now; relearned the
// floor itself a round read differently at, its standing doubled; stale, the
// record no longer the one the probe began under - the hold or floor moved,
// the search reopened, the group taken to its limit - so the probe says
// nothing about it.
const (
	FloorProbeLowered     = "lowered"
	FloorProbeFloorRaised = "floor_raised"
	FloorProbeRelearned   = "relearned"
	FloorProbeStale       = "stale"
)

// FloorProbeOutcomes is every way a probe's end is taken.
var FloorProbeOutcomes = []string{FloorProbeLowered, FloorProbeFloorRaised, FloorProbeRelearned, FloorProbeStale}

// MidpointResolution is how near the hold the floor has to be before a probe
// reads the floor itself rather than the midpoint between them. A probe read
// starts as long before its point as a read of the query takes - the
// lookback's reread bound, never under its RecheckTimeout of 5 s - so two
// holds nearer than that are read at overlapping moments, and a midpoint
// between them would tell nothing either does.
//
// Below a step a hold still decides what a read sees: a source that shows a
// point only some seconds after its timestamp - VictoriaMetrics' 30 s
// latency offset - reads a window whole only from that many seconds after
// it, wherever the window's own points fall in their step. The trials stop a
// step above the floor (execution.CandidateReadHold); the probes go on from
// there by halves, each half read clean ProbationRounds rounds in a row
// before the hold comes down to it.
const MidpointResolution = 5 * time.Second

// maxFloorRetests caps the failed probes counted: from the third on the
// standing is past RecordTTL already.
const maxFloorRetests = 8

// floorStanding is how long a floor stands after retests failed probes of
// it: FloorStanding doubled per failure, never past RecordTTL.
func floorStanding(retests int) time.Duration {
	standing := FloorStanding
	for range min(retests, maxFloorRetests) {
		standing *= 2
	}
	return min(standing, RecordTTL)
}

// undatedFloorAge is a share of a day the group's identity fixes: how far
// before the day it is first read on a floor written before floors were dated
// is taken as learned (undatedFloorAt).
func undatedFloorAge(qg execution.QueryGroupIdentity) time.Duration {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(qg))
	// FNV's high bits barely move between identities that differ in their
	// last characters; the finalizer spreads every bit of it over all of
	// them, as the lookback's sample spread does.
	x := hash.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return time.Duration(float64(x>>11) / (1 << 53) * float64(FloorStanding))
}

// undatedFloorAt is when a floor written before floors were dated is taken
// as learned, read at now: the start of now's UTC day less the group's
// undatedFloorAge, so the groups stuck under such floors when this build
// arrives start their probes spread over that day rather than in its first
// round together, and every process that reads the floor that day - a
// restart, a takeover - dates it alike until a write keeps the date; and no
// earlier than the Slot the group's hold was frozen at, so a hold that came
// to it within the day still waits from there.
func undatedFloorAt(qg execution.QueryGroupIdentity, record Record, now int64) int64 {
	return max(utcDay(now)*dayMillis-undatedFloorAge(qg).Milliseconds(), int64(record.SinceSlot)*1000)
}

// probeCandidate is the hold a shadow probe of record reads its rounds at
// now, whether that is the floor itself, and false when there is none to
// probe. A probe needs the group held, not held at its limit, and its search
// below the hold run out at a floor under it. While the floor is more than
// MidpointResolution below the hold the probe reads the midpoint between
// them, which no read has tried, at once; nearer, it reads the floor, once
// the floor has stood floorStanding of its failed probes.
func probeCandidate(record Record, step time.Duration, now int64) (candidate int64, atFloor, ok bool) {
	hold := current(record)
	if hold <= 0 || record.EarlierFloorMillis == nil || *record.EarlierFloorMillis >= hold || atLimitHeld(record, now) {
		return 0, false, false
	}
	if _, tries := nextCandidate(record, step, now); tries {
		return 0, false, false
	}
	floor := *record.EarlierFloorMillis
	if hold-floor > MidpointResolution.Milliseconds() {
		return floor + (hold-floor)/2, false, true
	}
	if now-record.EarlierFloorAtMillis < floorStanding(record.EarlierFloorRetests).Milliseconds() {
		return 0, false, false
	}
	return floor, true, true
}

// FloorProbe is the hold the group's shadow probe reads its rounds at - the
// midpoint between its floor and its hold, or the floor itself - and false
// when there is none to probe (probeCandidate). A floor written before
// floors were dated is dated here, at its first reading (undatedFloorAt); the
// date is kept in memory and written with the record's next change.
func (controller *Controller) FloorProbe(qg execution.QueryGroupIdentity) (time.Duration, bool) {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.loaded {
		return 0, false
	}
	now := controller.options.Now()
	if state.record.EarlierFloorMillis != nil && state.record.EarlierFloorAtMillis == 0 {
		state.record.EarlierFloorAtMillis = undatedFloorAt(qg, state.record, now.UnixMilli())
	}
	candidate, _, ok := probeCandidate(state.record, state.spec.Step, now.UnixMilli())
	return time.Duration(candidate) * time.Millisecond, ok
}

// LoweringVerified says the group's last lowering was to a floor a shadow
// probe read clean first (Record.LoweredVerified): the rounds it frozen
// lower are not read again in probation.
func (controller *Controller) LoweringVerified(qg execution.QueryGroupIdentity) bool {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.loaded && state.record.LoweredVerified
}

// FloorProbeEvidence is a shadow probe's end: the hold its rounds were read
// at (Floor: the candidate FloorProbe gave, the floor or a midpoint above
// it) and the hold their formal reads were read at, and whether
// lookback.ProbationRounds rounds in a row read the same at both (Passed) or
// one read differently. Contract is the Slot of its last round.
type FloorProbeEvidence struct {
	Contract execution.FrozenExecutionContractRef
	Floor    time.Duration
	Hold     time.Duration
	Passed   bool
}

// FloorProbeResult takes a shadow probe's end. One that passed lowers the
// hold to the hold it read at, as a trial's lowering does but marked
// verified - no probation follows it. At a midpoint the floor stays and the
// next probe reads the midpoint below; at the floor the floor is the hold
// now, and the search goes on below it from half. One that failed at a
// midpoint makes the midpoint the floor, dated now; at the floor it relearns
// the floor where it was, dated now, its standing doubled. One whose record
// is no longer the one it read under changes nothing.
func (controller *Controller) FloorProbeResult(ctx context.Context, evidence FloorProbeEvidence) error {
	if evidence.Contract.Validate() != nil || evidence.Floor < 0 || evidence.Hold <= evidence.Floor {
		return errors.New("alarmd readhold: invalid floor probe evidence")
	}
	qg := evidence.Contract.Slot.QueryGroup
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ready(state); err != nil {
		return err
	}
	if evidence.Contract.ScheduleSegmentStart < state.record.SegmentStart || state.record.Closed {
		return nil
	}
	now := controller.options.Now().UnixMilli()
	candidate, atFloor, ok := probeCandidate(state.record, state.spec.Step, now)
	if !ok || candidate != evidence.Floor.Milliseconds() || current(state.record) != evidence.Hold.Milliseconds() {
		controller.counted(controller.floorProbes, FloorProbeStale)
		return nil
	}
	next := clone(state.record)
	var outcome string
	switch {
	case evidence.Passed:
		outcome = FloorProbeLowered
		next.LoweredFromMillis = current(state.record)
		next.PendingHoldMillis = &candidate
		next.Lowered, next.LoweredVerified, next.EarlierMatches, next.QuietSinceMillis = true, true, 0, now
		next.ArrivalAgeMillis = state.spec.Delay.Milliseconds() + state.spec.SettlingWait.Milliseconds() + candidate
		next.ArrivalAgeSeed = ""
		if atFloor {
			// The floor is the hold now: the search below it starts from half.
			clearFloor(&next)
		}
	case atFloor:
		outcome = FloorProbeRelearned
		next.EarlierFloorAtMillis = now
		next.EarlierFloorRetests = min(next.EarlierFloorRetests+1, maxFloorRetests)
	default:
		// Read differently at the midpoint: too early there too. The floor
		// moves up to it, a value no probe has failed at, so it starts its
		// count over (setFloor), and the next probe reads the midpoint above
		// without standing first.
		outcome = FloorProbeFloorRaised
		setFloor(&next, candidate, now)
	}
	if err := controller.persist(ctx, qg, state, next, nil); err != nil {
		return err
	}
	controller.counted(controller.floorProbes, outcome)
	return nil
}
