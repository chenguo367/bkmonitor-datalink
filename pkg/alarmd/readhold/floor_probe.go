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

// FloorStanding is how long a floor stands before its first shadow probe:
// the search below the hold has run out (execution.CandidateReadHold), and
// the floor - the largest candidate shown too early - may no longer be too
// early now that the data comes in sooner. Each probe of it that fails
// doubles the standing, up to RecordTTL.
//
// A probe reads every round at the floor beside the formal read at the hold,
// which stays where it is, and lowers the hold to the floor only once
// lookback.ProbationRounds rounds in a row read the same at both: no round is
// decided on data read early to find out (the design of the floor's shadow
// probe, development-designs 004).
const FloorStanding = 24 * time.Hour

// How the controller took a shadow probe's end, closed: lowered the hold to
// the floor it read clean; relearned the floor a round read differently at,
// its standing doubled; stale, the record no longer the one the probe began
// under - the hold or floor moved, the search reopened, the group taken to its
// limit - so the probe says nothing about it.
const (
	FloorProbeLowered   = "lowered"
	FloorProbeRelearned = "relearned"
	FloorProbeStale     = "stale"
)

// FloorProbeOutcomes is every way a probe's end is taken.
var FloorProbeOutcomes = []string{FloorProbeLowered, FloorProbeRelearned, FloorProbeStale}

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

// undatedFloorAge is how long ago a floor written before floors were dated
// is taken as learned, at the first reading of it: a share of a day the
// group's identity fixes, so the groups stuck under such floors when this
// build arrives start their probes spread over its first day rather than in
// its first round together, and a group's share is the same in every
// process that reads it.
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

// probeFloor is the floor a shadow probe of record would read at now, and
// false when there is none to probe: the group held, not held at its limit,
// its search below the hold run out at a floor under it, and that floor
// standing for floorStanding of its failed probes.
func probeFloor(record Record, step time.Duration, now int64) (int64, bool) {
	hold := current(record)
	if hold <= 0 || record.EarlierFloorMillis == nil || *record.EarlierFloorMillis >= hold || atLimitHeld(record, now) {
		return 0, false
	}
	if _, tries := nextCandidate(record, step, now); tries {
		return 0, false
	}
	if now-record.EarlierFloorAtMillis < floorStanding(record.EarlierFloorRetests).Milliseconds() {
		return 0, false
	}
	return *record.EarlierFloorMillis, true
}

// FloorProbe is the floor the group's shadow probe reads its rounds at, and
// false when there is none to probe (probeFloor). A floor written before
// floors were dated is dated here, at its first reading, undatedFloorAge
// ago; the date is kept in memory and written with the record's next change.
func (controller *Controller) FloorProbe(qg execution.QueryGroupIdentity) (time.Duration, bool) {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.loaded {
		return 0, false
	}
	now := controller.options.Now()
	if state.record.EarlierFloorMillis != nil && state.record.EarlierFloorAtMillis == 0 {
		state.record.EarlierFloorAtMillis = now.Add(-undatedFloorAge(qg)).UnixMilli()
	}
	floor, ok := probeFloor(state.record, state.spec.Step, now.UnixMilli())
	return time.Duration(floor) * time.Millisecond, ok
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

// FloorProbeEvidence is a shadow probe's end: the floor its rounds were read
// at and the hold their formal reads were read at, and whether
// lookback.ProbationRounds rounds in a row read the same at both (Passed) or
// one read differently. Contract is the Slot of its last round.
type FloorProbeEvidence struct {
	Contract execution.FrozenExecutionContractRef
	Floor    time.Duration
	Hold     time.Duration
	Passed   bool
}

// FloorProbeResult takes a shadow probe's end. One that passed lowers the
// hold to the floor, as a trial's lowering does but marked verified - no
// probation follows it - and the search goes on below the floor, which is
// now the hold. One that failed relearns the floor where it was: dated now,
// its standing doubled. One whose record is no longer the one it read under
// changes nothing.
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
	floor, ok := probeFloor(state.record, state.spec.Step, now)
	if !ok || floor != evidence.Floor.Milliseconds() || current(state.record) != evidence.Hold.Milliseconds() {
		controller.counted(controller.floorProbes, FloorProbeStale)
		return nil
	}
	next := clone(state.record)
	outcome := FloorProbeRelearned
	if evidence.Passed {
		outcome = FloorProbeLowered
		next.LoweredFromMillis = current(state.record)
		next.PendingHoldMillis = &floor
		next.Lowered, next.LoweredVerified, next.EarlierMatches, next.QuietSinceMillis = true, true, 0, now
		next.ArrivalAgeMillis = state.spec.Delay.Milliseconds() + state.spec.SettlingWait.Milliseconds() + floor
		next.ArrivalAgeSeed = ""
		// The floor is the hold now: the search below it starts from half.
		clearFloor(&next)
	} else {
		next.EarlierFloorAtMillis = now
		next.EarlierFloorRetests = min(next.EarlierFloorRetests+1, maxFloorRetests)
	}
	if err := controller.persist(ctx, qg, state, next, nil); err != nil {
		return err
	}
	controller.counted(controller.floorProbes, outcome)
	return nil
}
