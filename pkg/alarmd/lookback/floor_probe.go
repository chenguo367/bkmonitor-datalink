// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"container/heap"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A shadow probe of a Query Group's floor: once its search below the hold has
// run out at a floor that has stood long enough (readhold.Controller.
// FloorProbe), each of its rounds is read at the floor as well as at the
// hold. The formal read stays at the hold, and the round is decided on it;
// the read at the floor is compared with it, whole, as a trial is
// (completeEarlier). ProbationRounds rounds in a row that read the same
// lower the hold to the floor without a probation after it, the probe having
// read those rounds already; one that reads differently ends the probe, and
// the floor stands longer before the next. No round is decided on data read
// early to find out.
//
// A probe read rides no sample: each round has its own, reserved at the
// Slot's first attempt (Prepare) - for a hold past the period less the reread
// bound, at the first attempt of a Slot before it, as a trial is reserved
// ahead (aheadProbeLocked) - so a group has as many in flight as Slots a hold
// spans, and one more.

// How a probe round came out, closed: clean read the same at the floor as the
// formal read did at the hold; different did not; the rest are no evidence
// and neither advance nor end the probe - skipped_for_permits was never read,
// no permit being free; read_failed was read and did not come back whole;
// unclassified could not be compared, the formal read not whole or a series
// table refused; overtaken was not read in time, or its Slot froze otherwise
// than it was reserved for.
const (
	FloorProbeClean             = "clean"
	FloorProbeDifferent         = "different"
	FloorProbeSkippedForPermits = "skipped_for_permits"
	FloorProbeReadFailed        = "read_failed"
	FloorProbeUnclassified      = "unclassified"
	FloorProbeOvertaken         = "overtaken"
)

// FloorProbeRoundOutcomes is every probe round outcome.
var FloorProbeRoundOutcomes = []string{FloorProbeClean, FloorProbeDifferent, FloorProbeSkippedForPermits,
	FloorProbeReadFailed, FloorProbeUnclassified, FloorProbeOvertaken}

// What happened to a probe, closed: started; passed, ProbationRounds rounds
// in a row clean; failed, a round different; hold_moved, the hold, the floor
// or the search under them moved, which ends it unfinished; owner_lost, the
// group left this process. A probe in flight when its process stops ends
// with none of them: started rising while none of the ends does is probes
// that do not live long enough to finish.
const (
	FloorProbeStarted   = "started"
	FloorProbePassed    = "passed"
	FloorProbeFailed    = "failed"
	FloorProbeHoldMoved = "hold_moved"
	FloorProbeOwnerLost = "owner_lost"
)

// FloorProbeEvents is every probe event.
var FloorProbeEvents = []string{FloorProbeStarted, FloorProbePassed, FloorProbeFailed, FloorProbeHoldMoved, FloorProbeOwnerLost}

// floorProbeState is a Query Group's probe: the floor its rounds are read at,
// the hold their formal reads are frozen with, its clean rounds in a row, the
// Slot it started at and the latest whose first attempt it saw, and its
// reservations by Slot.
type floorProbeState struct {
	floor    time.Duration
	hold     time.Duration
	clean    int
	started  execution.EvaluationTime
	prepared execution.EvaluationTime
	reserved map[execution.EvaluationTime]*earlierSample
}

// FloorProbeEnd is how a probe ended (FloorProbeEvents but started): the Slot
// of the round that ended it - only the Query Group for one that left the
// process - its floor and hold, its clean rounds and the Slot it started at.
type FloorProbeEnd struct {
	Contract execution.FrozenExecutionContractRef
	Source   string
	Event    string
	Floor    time.Duration
	Hold     time.Duration
	Clean    int
	Started  execution.EvaluationTime
}

// GroupFloorProbe is a Query Group's probe now, for its group reading.
type GroupFloorProbe struct {
	FloorMillis int64                    `json:"floor_ms"`
	HoldMillis  int64                    `json:"hold_ms"`
	Clean       int                      `json:"clean_rounds"`
	Started     execution.EvaluationTime `json:"started_slot"`
	Reserved    int                      `json:"reserved"`
}

// floorProbe asks the controller, outside the engine's lock, for the floor
// the group's probe reads at; false with no controller to ask.
func (engine *Engine) floorProbe(queryGroup execution.QueryGroupIdentity) (time.Duration, bool) {
	if engine.options.FloorProbe == nil {
		return 0, false
	}
	return engine.options.FloorProbe(queryGroup)
}

// prepareProbe is Prepare for a Slot of a group whose probe reads at floor:
// the Slot is counted floor_probe, not no_candidate, and once, at its first
// attempt, it takes the reservation made for it ahead and reserves the read
// of a round - its own while its point is still ahead, otherwise the first
// Slot on whose point is.
func (engine *Engine) prepareProbe(query Query, now time.Time, hold, base, floor time.Duration) {
	engine.mu.Lock()
	state := engine.groupLocked(query, now)
	slot := query.Contract.Slot.EvaluationTime
	probe := state.floorProbe
	if probe != nil && (probe.floor != floor || probe.hold != base) {
		engine.endProbeLocked(state, FloorProbeHoldMoved, query.Contract)
		probe = nil
	}
	if probe == nil {
		probe = &floorProbeState{floor: floor, hold: base, started: slot, reserved: map[execution.EvaluationTime]*earlierSample{}}
		state.floorProbe = probe
		engine.counts.floorProbes[key2(state.source, FloorProbeStarted)]++
	}
	engine.skipLocked(state, query, EarlierSkipFloorProbe)
	if slot <= probe.prepared {
		// A readiness retry, or a Slot replayed out of order: its first
		// attempt decided it.
		engine.mu.Unlock()
		return
	}
	probe.prepared = slot
	// A Slot before this one whose round was never read formally - not
	// prepared again, its read not begun - will not be.
	for at, earlier := range probe.reserved {
		if at < slot && !earlier.formal {
			engine.dropProbeRoundLocked(probe, earlier, OvertakenNextSlot)
		}
	}
	own := probe.reserved[slot]
	if own != nil && own.ahead {
		// Reserved ahead for this Slot: read for its frozen query only when
		// the Slot's window, hold and readiness are those it was moved to.
		own.ahead = false
		if query.Spec.Digest == own.query.Spec.Digest && query.Contract.ReadHoldMillis == own.query.Contract.ReadHoldMillis &&
			query.ReadyAt.Equal(own.query.ReadyAt) {
			own.query = query
		} else {
			engine.dropProbeRoundLocked(probe, own, OvertakenHoldMoved)
			own = nil
		}
	}
	bound := state.rereadBound()
	baseline := query.ReadyAt.Add(-hold)
	readyAt := baseline.Add(floor)
	start := trialStart(readyAt, baseline, floor, bound)
	var reserved *earlierSample
	if own == nil && now.Before(start) {
		reserved = &earlierSample{query: query, source: state.source, candidateHold: floor, group: state, probe: probe,
			at: start, readyAt: readyAt, keptFrom: tailFrom(query.Spec.LogicalWindow, state.step, tailSteps)}
		probe.reserved[slot] = reserved
	} else {
		reserved = engine.aheadProbeLocked(state, probe, query, hold, base, floor, now)
	}
	if reserved != nil {
		heap.Push(&engine.earlyWork, earlyWork{at: reserved.at, trial: reserved})
	}
	engine.mu.Unlock()
	engine.publishProbeEnds()
	if reserved != nil {
		engine.poke()
	}
}

// aheadProbeLocked reserves the round of the first Slot on, k Slots ahead,
// whose read at the floor starts after now and that has none reserved yet,
// as aheadTrialLocked moves a trial; nil when there is none within the
// hold's reach. A group's reservations are no more than the Slots its hold
// and reread bound span, and one more: the Slots between a reservation and
// the first attempt that made it each made their own.
func (engine *Engine) aheadProbeLocked(state *group, probe *floorProbeState, query Query, hold, base, floor time.Duration, now time.Time) *earlierSample {
	step := int64(query.FollowingSlot - query.Contract.Slot.EvaluationTime)
	if query.FollowingSlot == 0 || step <= 0 {
		return nil
	}
	bound := state.rereadBound()
	limit := int((hold+bound)/(time.Duration(step)*time.Second)) + 2
	if len(probe.reserved) > limit {
		return nil
	}
	for slots := 1; slots <= limit; slots++ {
		shift := step * int64(slots)
		at := query.Contract.Slot.EvaluationTime + execution.EvaluationTime(shift)
		if probe.reserved[at] != nil {
			continue
		}
		baseline := query.ReadyAt.Add(-hold).Add(time.Duration(shift) * time.Second)
		readyAt := baseline.Add(floor)
		start := trialStart(readyAt, baseline, floor, bound)
		if !now.Before(start) {
			continue
		}
		spec, derived := shiftedSpec(query.Spec, shift)
		if !derived {
			return nil
		}
		next := query
		next.Spec, next.FollowingSlot = spec, 0
		next.Contract.Slot.EvaluationTime = at
		next.Contract.ReadHoldMillis = base.Milliseconds()
		next.ReadyAt = baseline.Add(base)
		reserved := &earlierSample{query: next, source: state.source, candidateHold: floor, group: state, probe: probe,
			ahead: true, aheadSlots: slots, at: start, readyAt: readyAt, keptFrom: tailFrom(next.Spec.LogicalWindow, state.step, tailSteps)}
		probe.reserved[at] = reserved
		return reserved
	}
	return nil
}

// dropProbeRoundLocked ends a reservation its Slot will not read formally, a
// round counted overtaken.
func (engine *Engine) dropProbeRoundLocked(probe *floorProbeState, round *earlierSample, path string) {
	round.overtake(path)
	if round.cancel != nil {
		round.cancel()
	}
	round.summary = nil
	delete(probe.reserved, round.query.Contract.Slot.EvaluationTime)
	engine.countProbeRoundLocked(round.group, round.source, FloorProbeOvertaken)
}

func (engine *Engine) countProbeRoundLocked(state *group, source, outcome string) {
	engine.counts.floorProbeRounds[key2(source, outcome)]++
	state.reading.probeRounds[wordIndex(FloorProbeRoundOutcomes, outcome)]++
}

// endProbeLocked ends the group's probe, if it has one, by event, at the Slot
// of contract: its reservations not yet read formally are dropped uncounted,
// the probe that made them being over.
func (engine *Engine) endProbeLocked(state *group, event string, contract execution.FrozenExecutionContractRef) {
	probe := state.floorProbe
	if probe == nil {
		return
	}
	state.floorProbe = nil
	for _, round := range probe.reserved {
		if !round.formal {
			round.overtake(OvertakenHoldMoved)
			if round.cancel != nil {
				round.cancel()
			}
			round.summary = nil
		}
	}
	engine.counts.floorProbes[key2(state.source, event)]++
	engine.probeEnds = append(engine.probeEnds, FloorProbeEnd{Contract: contract, Source: state.source, Event: event,
		Floor: probe.floor, Hold: probe.hold, Clean: probe.clean, Started: probe.started})
}

// publishProbeEnds hands the kept probe ends to OnFloorProbe, outside the
// engine's lock.
func (engine *Engine) publishProbeEnds() {
	engine.mu.Lock()
	ends := engine.probeEnds
	engine.probeEnds = nil
	engine.mu.Unlock()
	if engine.options.OnFloorProbe == nil {
		return
	}
	for _, end := range ends {
		engine.options.OnFloorProbe(end)
	}
}

// attachProbeLocked has the formal first read of a Slot take the probe round
// reserved for it, which it is then compared with (completeFloorProbe): a
// round whose read at the floor has not finished by now is overtaken, the
// read at the hold having begun.
func (engine *Engine) attachProbeLocked(state *group, read *Read, query Query) {
	probe := state.floorProbe
	if probe == nil {
		return
	}
	round := probe.reserved[query.Contract.Slot.EvaluationTime]
	if round == nil || round.ahead || round.formal || round.query.Spec.Digest != query.Spec.Digest {
		return
	}
	read.floorProbe, round.formal = round, true
	if !round.done {
		round.overtake(OvertakenFormalFirst)
		if round.cancel != nil {
			round.cancel()
		}
	}
}

// completeFloorProbe compares the probe round a formal first read took with
// what that read found, and counts it. ProbationRounds clean rounds in a row
// pass the probe, one different fails it; a round of another hold, or of a
// probe that has ended, counts and moves nothing.
func (read *Read) completeFloorProbe(completion execution.ProviderCompletion, err error) {
	round := read.floorProbe
	if round == nil {
		return
	}
	engine := read.engine
	qg := round.query.Contract.Slot.QueryGroup
	owned := engine.options.Owns(qg)
	engine.mu.Lock()
	outcome := FloorProbeClean
	switch {
	case !owned:
		outcome = ""
	case err != nil || completion.Completeness != execution.CompletenessFull || read.summary == nil || read.summary.faulted:
		outcome = FloorProbeUnclassified
	case round.outcome == EarlierPermitRefused:
		outcome = FloorProbeSkippedForPermits
	case round.outcome == EarlierMemoryRefused:
		outcome = FloorProbeUnclassified
	case round.outcome == EarlierReadFailed:
		outcome = FloorProbeReadFailed
	case round.outcome == EarlierOvertaken:
		outcome = FloorProbeOvertaken
	case len(compareSummaries(round.summary, trimSummary(read.summary.buckets, round.keptFrom))) > 0:
		outcome = FloorProbeDifferent
	}
	round.summary = nil
	probe := round.probe
	slot := round.query.Contract.Slot.EvaluationTime
	if probe.reserved[slot] == round {
		delete(probe.reserved, slot)
	}
	if outcome != "" {
		engine.countProbeRoundLocked(round.group, round.source, outcome)
	}
	if state := engine.groups[qg]; state != nil && state.floorProbe == probe && round.query.Contract.ReadHoldMillis == probe.hold.Milliseconds() {
		switch outcome {
		case FloorProbeClean:
			if probe.clean++; probe.clean >= engine.probationRounds() {
				engine.endProbeLocked(state, FloorProbePassed, round.query.Contract)
			}
		case FloorProbeDifferent:
			engine.endProbeLocked(state, FloorProbeFailed, round.query.Contract)
		}
	}
	engine.mu.Unlock()
	engine.publishProbeEnds()
}

// floorProbeGroupsLocked is, by source, the Query Groups with a probe now.
func (engine *Engine) floorProbeGroupsLocked() map[string]uint64 {
	groups := make(map[string]uint64, len(Sources))
	for _, state := range engine.groups {
		if state.floorProbe != nil {
			groups[state.source]++
		}
	}
	return groups
}
