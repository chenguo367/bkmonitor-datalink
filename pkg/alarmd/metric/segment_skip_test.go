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
