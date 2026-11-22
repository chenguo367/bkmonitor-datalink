// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package metric

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// The segment skip word's ignored Slots are counted by reason, every cell
// present at zero from the start.
func TestASegmentSkipWordNotFollowedIsCountedByReason(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	for _, reason := range observability.AllSegmentSkipIgnoredReasons() {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_skip_flag_ignored_total", map[string]string{"reason": reason}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("cell %s: %v, want one at zero", reason, samples)
		}
	}
	var facts observability.SegmentShadowFacts
	facts.IgnoreSkipFlag(observability.SegmentSkipIgnoredEpochMismatch)
	facts.IgnoreSkipFlag(observability.SegmentSkipIgnoredEpochMismatch)
	facts.IgnoreSkipFlag("not_a_reason")
	recorder.Observe(context.Background(), observability.NormalizeObservation(observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.OperationNormal, SegmentShadow: &facts,
	}))
	if samples := redisSamples(t, recorder, "alarmd_state_segment_skip_flag_ignored_total", map[string]string{"reason": "not_a_reason"}); len(samples) != 0 {
		t.Fatalf("a reason outside the list made a cell: %v", samples)
	}
	for reason, want := range map[string]float64{observability.SegmentSkipIgnoredEpochMismatch: 2, observability.SegmentSkipIgnoredNoView: 0} {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_skip_flag_ignored_total", map[string]string{"reason": reason}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != want {
			t.Fatalf("cell %s: %v, want %v", reason, samples, want)
		}
	}
}

// The sampled rounds are counted by period class and the shadow's outcomes
// by round, every cell present at zero from the start; an outcome of a
// round neither word names is counted normal.
func TestTheSampledRoundsAndTheShadowsRoundsAreCountedEveryCellFromTheStart(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	for _, class := range observability.AllSegmentPeriodClasses() {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_sample_total", map[string]string{"period_class": class}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("sample cell %s: %v, want one at zero", class, samples)
		}
		for _, round := range observability.AllSegmentShadowRounds() {
			if samples := redisSamples(t, recorder, "alarmd_state_segment_shadow_total", map[string]string{"period_class": class,
				"outcome": string(observability.SegmentShadowMatch), "round": round}); len(samples) != 1 || samples[0].GetCounter().GetValue() != 0 {
				t.Fatalf("shadow cell %s %s: %v, want one at zero", class, round, samples)
			}
		}
	}
	var facts observability.SegmentShadowFacts
	facts.Sample(observability.SegmentPeriodMinute)
	facts.Outcome(observability.SegmentShadowMatch, observability.SegmentPeriodMinute, observability.SegmentRoundSampled, 3)
	facts.Outcome(observability.SegmentShadowMatch, observability.SegmentPeriodMinute, "not_a_round", 2)
	recorder.Observe(context.Background(), observability.NormalizeObservation(observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.OperationNormal, SegmentShadow: &facts,
	}))
	if samples := redisSamples(t, recorder, "alarmd_state_segment_sample_total", map[string]string{"period_class": observability.SegmentPeriodMinute}); len(samples) != 1 ||
		samples[0].GetCounter().GetValue() != 1 {
		t.Fatalf("the sample counted %v, want one", samples)
	}
	for round, want := range map[string]float64{observability.SegmentRoundSampled: 3, observability.SegmentRoundNormal: 2} {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_shadow_total", map[string]string{"period_class": observability.SegmentPeriodMinute,
			"outcome": string(observability.SegmentShadowMatch), "round": round}); len(samples) != 1 || samples[0].GetCounter().GetValue() != want {
			t.Fatalf("round %s: %v, want %v", round, samples, want)
		}
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_shadow_total", map[string]string{"round": "not_a_round"}); len(samples) != 0 {
		t.Fatalf("a round outside the list made a cell: %v", samples)
	}
}

// The Plans degraded are counted by why, every cell present at zero from
// the start; a why outside the list makes no cell.
func TestTheDegradedPlansAreCountedByWhyEveryCellFromTheStart(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	for _, why := range observability.AllSegmentDegradations() {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_degraded_total", map[string]string{"reason": why}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("cell %s: %v, want one at zero", why, samples)
		}
	}
	var facts observability.SegmentShadowFacts
	facts.Degrade(observability.SegmentDegradedUndecodable)
	facts.Degrade(observability.SegmentDegradedStopped)
	facts.Degrade("not_a_reason")
	recorder.Observe(context.Background(), observability.NormalizeObservation(observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.OperationNormal, SegmentShadow: &facts,
	}))
	for why, want := range map[string]float64{observability.SegmentDegradedUndecodable: 1, observability.SegmentDegradedStopped: 1,
		observability.SegmentDegradedUnread: 0} {
		if samples := redisSamples(t, recorder, "alarmd_state_segment_degraded_total", map[string]string{"reason": why}); len(samples) != 1 ||
			samples[0].GetCounter().GetValue() != want {
			t.Fatalf("cell %s: %v, want %v", why, samples, want)
		}
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_degraded_total", map[string]string{"reason": "not_a_reason"}); len(samples) != 0 {
		t.Fatalf("a why outside the list made a cell: %v", samples)
	}
}

// The State cache's charge and departures are served by kind, State values
// and segments, every reason's cell of each from the start; each kind's
// departures are served as the cache counted them, never taken from their
// sum.
func TestTheStateCacheServesStateValuesAndSegmentsApart(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	recorder.SetStateCacheSource(func() state.StateCacheStats {
		// The sum behind its kinds, as a total read a moment before them
		// would be: each kind is served as it was counted, never taken
		// from the sum.
		return state.StateCacheStats{Bytes: 100, SegmentBytes: 30, BudgetBytes: 1000,
			Departures:        map[string]uint64{state.StateCacheDepartReleased: 1, state.StateCacheDepartOverBudget: 3},
			StateDepartures:   map[string]uint64{state.StateCacheDepartReleased: 3},
			SegmentDepartures: map[string]uint64{state.StateCacheDepartReleased: 2, state.StateCacheDepartOverBudget: 4}}
	})
	for kind, want := range map[string]float64{state.StateCacheKindState: 70, state.StateCacheKindSegment: 30} {
		if samples := redisSamples(t, recorder, "alarmd_state_cache_bytes", map[string]string{"kind": kind}); len(samples) != 1 ||
			samples[0].GetGauge().GetValue() != want {
			t.Fatalf("bytes of %s: %v, want %v", kind, samples, want)
		}
	}
	for _, reason := range state.StateCacheDepartureReasons {
		want := map[string]float64{state.StateCacheKindState: 0, state.StateCacheKindSegment: 0}
		switch reason {
		case state.StateCacheDepartReleased:
			want = map[string]float64{state.StateCacheKindState: 3, state.StateCacheKindSegment: 2}
		case state.StateCacheDepartOverBudget:
			want = map[string]float64{state.StateCacheKindState: 0, state.StateCacheKindSegment: 4}
		}
		for kind, value := range want {
			if samples := redisSamples(t, recorder, "alarmd_state_cache_departures_total", map[string]string{"reason": reason, "kind": kind}); len(samples) != 1 ||
				samples[0].GetCounter().GetValue() != value {
				t.Fatalf("departures %s of %s: %v, want %v", reason, kind, samples, value)
			}
		}
	}
}

// A match written all the same is counted by its gate and every Plan's round
// by its skip mode, each by period class, every cell present at zero from the
// start; a gate outside the list is counted other, and a mode outside
// segment.SkipModes makes no cell.
func TestTheMatchesNotSkippedAndTheRoundsAreCountedEveryCellFromTheStart(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	cell := func(name string, labels map[string]string) float64 {
		t.Helper()
		samples := redisSamples(t, recorder, name, labels)
		if len(samples) != 1 {
			t.Fatalf("%s %v: %d cells, want one", name, labels, len(samples))
		}
		return samples[0].GetCounter().GetValue()
	}
	for _, class := range observability.AllSegmentPeriodClasses() {
		for _, gate := range observability.AllSegmentNotSkippedGates() {
			if got := cell("alarmd_state_segment_match_not_skipped_total", map[string]string{"gate": gate, "period_class": class}); got != 0 {
				t.Fatalf("gate %s %s starts at %v", gate, class, got)
			}
		}
		for _, mode := range segment.SkipModes() {
			if got := cell("alarmd_state_segment_rounds_total", map[string]string{"mode": mode, "period_class": class}); got != 0 {
				t.Fatalf("mode %s %s starts at %v", mode, class, got)
			}
		}
	}
	var facts observability.SegmentShadowFacts
	facts.NotSkip(observability.SegmentNotSkippedSampled, observability.SegmentPeriodClass(60), 3)
	facts.NotSkip("not_a_gate", observability.SegmentPeriodClass(60), 2)
	facts.Round(segment.SkipModeSkip, observability.SegmentPeriodClass(60))
	facts.Round(segment.SkipModeSkip, observability.SegmentPeriodClass(60))
	facts.Round("not_a_mode", observability.SegmentPeriodClass(60))
	recorder.Observe(context.Background(), observability.NormalizeObservation(observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.OperationNormal, SegmentShadow: &facts,
	}))
	minute := observability.SegmentPeriodClass(60)
	for gate, want := range map[string]float64{observability.SegmentNotSkippedSampled: 3, observability.SegmentNotSkippedOther: 2,
		observability.SegmentNotSkippedSlotSkipOff: 0} {
		if got := cell("alarmd_state_segment_match_not_skipped_total", map[string]string{"gate": gate, "period_class": minute}); got != want {
			t.Fatalf("gate %s: %v, want %v", gate, got, want)
		}
	}
	if got := cell("alarmd_state_segment_rounds_total", map[string]string{"mode": segment.SkipModeSkip, "period_class": minute}); got != 2 {
		t.Fatalf("rounds skip: %v, want 2", got)
	}
	if samples := redisSamples(t, recorder, "alarmd_state_segment_rounds_total", map[string]string{"mode": "not_a_mode"}); len(samples) != 0 {
		t.Fatalf("a mode outside the list made a cell: %v", samples)
	}
}
