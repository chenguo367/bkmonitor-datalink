// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A member compared a match and written all the same is counted under the
// first reason its round wrote it whole, in the order the round checks
// them - the Slot's reasons, then the Plan's, then the member's page - each
// reason alone and with every later one holding too: the earliest wins. A
// round with none of them leaves its matches, and none is other.
func TestAMatchWrittenAllTheSameIsCountedUnderTheFirstGateThatHeld(t *testing.T) {
	digest := execution.SeriesIdentityDigest(strings.Repeat("a", 64))
	// conditions are the gates' conditions, in their order; set turns the
	// index-th on.
	type conditions struct {
		skipOff, writeFailed, notNormal, stopped, sampled, ttlZero, fallback bool
	}
	build := func(c conditions) (*segmentShadowRound, *segmentShadowPlanRound) {
		slot := &segmentSlot{skip: !c.skipOff, writeFailed: c.writeFailed}
		state := &segmentShadowPlan{stopped: c.stopped, ttl: time.Hour, hadMark: true, mark: segment.Mark{Pages: 1}}
		if c.ttlZero {
			state.ttl = 0
		}
		if c.fallback {
			state.fallback = map[int]bool{0: true}
		}
		round := &segmentShadowRound{slot: slot, skip: slot.skip && !slot.writeFailed && !c.notNormal}
		return round, &segmentShadowPlanRound{state: state, sample: c.sampled}
	}
	set := func(c *conditions, index int) {
		switch index {
		case 0:
			c.skipOff = true
		case 1:
			c.writeFailed = true
		case 2:
			c.notNormal = true
		case 3:
			c.stopped = true
		case 4:
			c.sampled = true
		case 5:
			c.ttlZero = true
		case 6:
			c.fallback = true
		}
	}
	gates := observability.AllSegmentNotSkippedGates()
	if len(gates) != 8 || gates[len(gates)-1] != observability.SegmentNotSkippedOther {
		t.Fatalf("gates %v, want seven and other last", gates)
	}
	for index, want := range gates[:7] {
		// The gate alone, then with every later one too.
		for _, alsoLater := range []bool{false, true} {
			var c conditions
			set(&c, index)
			if alsoLater {
				for later := index + 1; later < 7; later++ {
					set(&c, later)
				}
			}
			round, planRound := build(c)
			if got := round.notSkippedGate(planRound, digest); got != want {
				t.Errorf("gate %s (later ones too: %t) counted %s", want, alsoLater, got)
			}
		}
	}
	// stopped and sampled both: the Plan's stop is checked first.
	round, planRound := build(conditions{stopped: true, sampled: true})
	if got := round.notSkippedGate(planRound, digest); got != observability.SegmentNotSkippedPlanStopped {
		t.Errorf("a stopped Plan's sampled round counted %s, want plan_stopped", got)
	}
	// None of them: the round leaves its matches, and a match it wrote would
	// be a gate the list misses.
	round, planRound = build(conditions{})
	if got := round.notSkippedGate(planRound, digest); got != observability.SegmentNotSkippedOther {
		t.Errorf("a round that skips counted a written match as %s", got)
	}
}

// The gates are what skipMode says of a Plan's round, in its order: each of
// skipMode's reasons to write every member is a gate of the same name, so a
// round's mode and its matches' gate never disagree.
func TestTheGatesFollowTheRoundsSkipMode(t *testing.T) {
	digest := execution.SeriesIdentityDigest(strings.Repeat("b", 64))
	for _, c := range []struct {
		mode, gate string
		round      *segmentShadowRound
		planRound  *segmentShadowPlanRound
	}{
		{segment.SkipModeOff, observability.SegmentNotSkippedSlotSkipOff,
			&segmentShadowRound{slot: &segmentSlot{}}, &segmentShadowPlanRound{state: &segmentShadowPlan{ttl: time.Hour}}},
		{segment.SkipModeWriteFailed, observability.SegmentNotSkippedWriteFailed,
			&segmentShadowRound{slot: &segmentSlot{skip: true, writeFailed: true}}, &segmentShadowPlanRound{state: &segmentShadowPlan{ttl: time.Hour}}},
		{segment.SkipModeOperation, observability.SegmentNotSkippedNotNormal,
			&segmentShadowRound{slot: &segmentSlot{skip: true}}, &segmentShadowPlanRound{state: &segmentShadowPlan{ttl: time.Hour}}},
		{segment.SkipModeStopped, observability.SegmentNotSkippedPlanStopped,
			&segmentShadowRound{slot: &segmentSlot{skip: true}, skip: true}, &segmentShadowPlanRound{state: &segmentShadowPlan{stopped: true, ttl: time.Hour}}},
		{segment.SkipModeSampled, observability.SegmentNotSkippedSampled,
			&segmentShadowRound{slot: &segmentSlot{skip: true}, skip: true}, &segmentShadowPlanRound{state: &segmentShadowPlan{ttl: time.Hour}, sample: true}},
	} {
		if got := c.round.skipMode(c.planRound); got != c.mode {
			t.Errorf("mode %s read as %s", c.mode, got)
		}
		if got := c.round.notSkippedGate(c.planRound, digest); got != c.gate {
			t.Errorf("a round in mode %s counted its match under %s, want %s", c.mode, got, c.gate)
		}
	}
}

// Only a match is given a gate: a member whose write the made read does not
// reproduce - here in one field alone, so its comparison has one outcome -
// is a mismatch, and no gate counts it. The gates add up to the matches
// when no member is left.
func TestOnlyAMatchWrittenAllTheSameIsGivenAGate(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200), steady(t, shadowSeries(1), 1200)}, false)
	changed := steady(t, shadowSeries(1), 1260)
	changed.write.Levels[0].LastRealPointTime = 1200
	facts := runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1260), changed}, false)
	if got := mismatches(facts); len(got) != 1 || got[observability.SegmentShadowMismatchLastReal] != 1 {
		t.Fatalf("mismatches %v, want series 1's last_real alone", got)
	}
	gates := int64(0)
	for key, count := range facts.NotSkipped {
		if key.Gate != observability.SegmentNotSkippedSlotSkipOff {
			t.Errorf("%d counted under %s, want slot_skip_off: the round follows no word", count, key.Gate)
		}
		gates += count
	}
	if matches := outcome(facts, observability.SegmentShadowMatch); matches != 1 || gates != matches {
		t.Fatalf("%d matches, %d under a gate: want series 0's one match and it alone gated", matches, gates)
	}
}

// A normal attempt that finds a Plan's series applied in part, its Slot
// following the skip, starts the Plan's shadow over: its round is counted
// under retried, comparing nothing and giving no gate.
func TestARoundThatStartsItsPlanOverIsCountedRetried(t *testing.T) {
	shadow := newMemorySegments()
	runShadow(t, shadow, []shadowRoundOf{steady(t, shadowSeries(0), 1200)}, false)
	shadow.at += 60
	request := shadowRequest(execution.OperationNormal, shadow.at)
	slot := shadowSlot(t, shadow, request, shadowDue)
	slot.skip = true
	round := newSegmentShadowRound(request, slot)
	round.retried(shadowDue)
	written := steady(t, shadowSeries(0), 1260)
	round.observe(shadowDue, segment.FamilyReal, written.view, written.write, written.class, false)
	planRound := round.plans[shadowKey(shadowDue, shadowDue.StateGeneration, segment.FamilyReal)]
	if mode := round.skipMode(planRound); mode != segment.SkipModeRetried {
		t.Fatalf("the round ran under %s, want retried", mode)
	}
	facts := foldShadow(t, shadow, round, request, shadowDue, nil)
	if len(facts.Rounds) != 1 || facts.Rounds[observability.SegmentRoundKey{Mode: segment.SkipModeRetried, PeriodClass: planRound.class}] != 1 ||
		len(facts.NotSkipped) != 0 || outcome(facts, observability.SegmentShadowMatch) != 0 {
		t.Fatalf("rounds %v, gates %v, %d matches: want one retried round and nothing compared", facts.Rounds, facts.NotSkipped,
			outcome(facts, observability.SegmentShadowMatch))
	}
}
