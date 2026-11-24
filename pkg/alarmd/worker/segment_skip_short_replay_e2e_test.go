// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// shadowRoundCounts is what a rig's segments did in a span of rounds: the
// shadow's outcomes, the page renewals by result, the matches written all the
// same by gate, the Plans' rounds by skip mode, and the members left.
type shadowRoundCounts struct {
	outcomes        map[observability.SegmentShadowOutcome]int64
	renewals, gates map[string]int64
	modes           map[string]int64
	left            int64
}

func (rig *shadowRig) roundCounts() shadowRoundCounts {
	rig.t.Helper()
	_, left, gates, modes := rig.observed.matchLedger()
	rig.observed.mu.Lock()
	defer rig.observed.mu.Unlock()
	counts := shadowRoundCounts{outcomes: map[observability.SegmentShadowOutcome]int64{}, renewals: map[string]int64{},
		gates: gates, modes: modes, left: left}
	for outcome, count := range rig.observed.outcomes {
		counts.outcomes[outcome] = count
	}
	for result, count := range rig.observed.renewals {
		counts.renewals[result] = count
	}
	return counts
}

// since is what was counted from before to after, the cells that moved.
func (after shadowRoundCounts) since(before shadowRoundCounts) shadowRoundCounts {
	moved := shadowRoundCounts{outcomes: map[observability.SegmentShadowOutcome]int64{}, renewals: map[string]int64{},
		gates: map[string]int64{}, modes: map[string]int64{}, left: after.left - before.left}
	for outcome, count := range after.outcomes {
		if count != before.outcomes[outcome] {
			moved.outcomes[outcome] = count - before.outcomes[outcome]
		}
	}
	for _, pair := range []struct{ now, then, into map[string]int64 }{
		{after.renewals, before.renewals, moved.renewals}, {after.gates, before.gates, moved.gates},
		{after.modes, before.modes, moved.modes}} {
		for key, count := range pair.now {
			if count != pair.then[key] {
				pair.into[key] = count - pair.then[key]
			}
		}
	}
	return moved
}

// A replay short of time - distance one, age zero, sent to replay only for a
// read that would not fit what was left of its time - leaves its stable
// members' State writes out as the normal round in its place would (014 next9
// 2): two rigs alike, one round a replay short of time on the one and a
// normal round on the other, leave the same members; and the normal round
// after renews the pages and compares the members on both alike, so leaving
// them in the replay changed nothing the segments go on from. A replay past
// its deadline writes its members whole. Before, every replay wrote its
// members whole, and a group diverted round after round wrote its group
// whole each round, which made each round slower and the next more likely
// to be diverted.
func TestAReplayShortOfTimeLeavesItsMembersAsTheNormalRoundWouldAndTheRoundAfterIsAlike(t *testing.T) {
	const hosts = 6
	replaying := newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 0, nil, 16<<20)
	normal := newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rigs := []*shadowRig{replaying, normal}
	at := frozenContract().Slot.EvaluationTime
	round := func() {
		t.Helper()
		at += 60
		for worker.SegmentSampledRound(planIdentity(), "state-v1", int64(at), 60) {
			at += 60
		}
		for _, rig := range rigs {
			if completed, err := rig.run(at, nil, nil, false); !completed {
				t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
			}
		}
	}
	for _, rig := range rigs {
		rig.skipWord.Store(7)
	}
	for attempt := 0; attempt < 10 && (replaying.skippedSoFar() == 0 || normal.skippedSoFar() == 0); attempt++ {
		round()
	}
	if replaying.skippedSoFar() == 0 {
		t.Fatal("fixture: no round left a member")
	}

	before := []shadowRoundCounts{replaying.roundCounts(), normal.roundCounts()}
	replaying.replay.Store(&shadowReplay{distance: 1, ageMillis: 0})
	round()
	replaying.replay.Store(nil)
	short, kept := replaying.roundCounts().since(before[0]), normal.roundCounts().since(before[1])
	if short.left == 0 || short.left != kept.left || short.gates[observability.SegmentNotSkippedNotNormal] != 0 {
		t.Fatalf("the replay short of time left %d members (gates %v), the normal round %d; want as many left, none for its operation",
			short.left, short.gates, kept.left)
	}

	before = []shadowRoundCounts{replaying.roundCounts(), normal.roundCounts()}
	round()
	after, alike := replaying.roundCounts().since(before[0]), normal.roundCounts().since(before[1])
	if !reflect.DeepEqual(after, alike) {
		t.Fatalf("the normal round after the replay short of time: %+v; after a normal round: %+v; want them alike", after, alike)
	}

	// A replay past its deadline writes its members whole.
	before = []shadowRoundCounts{replaying.roundCounts(), normal.roundCounts()}
	replaying.replay.Store(&shadowReplay{distance: 1, ageMillis: 5_000})
	round()
	replaying.replay.Store(nil)
	late := replaying.roundCounts().since(before[0])
	if late.left != 0 || late.gates[observability.SegmentNotSkippedNotNormal] == 0 {
		t.Fatalf("the replay past its deadline left %d members (gates %v), want none, every match written for its operation", late.left,
			late.gates)
	}
}

// A replay short of time whose page falls back - left unrenewed past half
// its lifetime by the worker's clock, its Plan's rounds one interval apart -
// counts fallback_write, as the normal round in its place would (014 next9
// 2): it was sent before its deadline, not late for its time, and the third
// stage's bar reads fallback_write. A replay past its deadline counts the
// same fallback stalled. Before, every replay's fallback was counted
// stalled, so a group diverted round after round never showed its pages
// falling back.
func TestAReplayShortOfTimeWhosePageFallsBackCountsAFallbackWrite(t *testing.T) {
	for _, c := range []struct {
		name    string
		replay  shadowReplay
		counted string
	}{
		{"short of time", shadowReplay{distance: 1, ageMillis: 0}, observability.SegmentRenewalFallbackWrite},
		{"past its deadline", shadowReplay{distance: 1, ageMillis: 5_000}, observability.SegmentRenewalFallbackStalled},
	} {
		t.Run(c.name, func(t *testing.T) {
			rig := newShadowRigWith(t, 4, 0, shadowRecoveryOff, 0, nil, 16<<20)
			at := frozenContract().Slot.EvaluationTime
			for round := 0; round < 3; round++ {
				at += 60
				if completed, err := rig.run(at, nil, nil, false); !completed {
					t.Fatalf("Slot at %d: err=%v", at, err)
				}
			}
			ttl := rig.storedMark(at).Renewed[0].TTL
			rig.wallAhead.Store(int64(time.Duration(ttl/2)*time.Millisecond + time.Minute))
			replay := c.replay
			rig.replay.Store(&replay)
			at += 60
			if completed, err := rig.run(at, nil, nil, false); !completed {
				t.Fatalf("the replay the page fell back in: err=%v", err)
			}
			written := rig.observed.renewal(observability.SegmentRenewalFallbackWrite)
			stalled := rig.observed.renewal(observability.SegmentRenewalFallbackStalled)
			if written+stalled != 1 || rig.observed.renewal(c.counted) != 1 {
				t.Fatalf("the replay counted %d fallback_write and %d fallback_stalled, want its one page %s", written, stalled, c.counted)
			}
		})
	}
}
