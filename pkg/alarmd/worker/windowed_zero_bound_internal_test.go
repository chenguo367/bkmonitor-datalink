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
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A window cut while the window bound is zero - what the Slot holds outside
// the window at half its share or more, so each series merged is a window of
// its own - is counted by the retained phase holding the most of that
// outside (slot_window_zero_bound_cuts_total): input when the inputs are
// most, state when the State held outside the window is. A window cut with
// the bound above zero is not counted.
func TestAWindowCutAtAZeroBoundIsCountedByThePhaseHoldingMostOutsideIt(t *testing.T) {
	for _, c := range []struct {
		name    string
		hold    func(*testing.T, *windowedFixture, uint64)
		outside uint64
		counted [4]uint64
	}{
		{"input past half", func(t *testing.T, fixture *windowedFixture, bytes uint64) { fixture.streamInput(t, "held", bytes) }, 1, [4]uint64{1, 0, 0, 0}},
		{"state past half", func(_ *testing.T, fixture *windowedFixture, bytes uint64) {
			fixture.stream.retainBytes(retainPhaseState, bytes)
		}, 1, [4]uint64{0, 0, 0, 1}},
		{"within half", func(t *testing.T, fixture *windowedFixture, bytes uint64) { fixture.streamInput(t, "held", bytes) }, 0, [4]uint64{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			fixture := newWindowedFixture(t)
			stream := fixture.stream
			share := stream.coordinator.qgShareBytes()
			// Past half the share by a byte, or a quarter of it.
			bytes := share/2 + c.outside
			if c.outside == 0 {
				bytes = share / 4
			}
			c.hold(t, fixture, bytes)
			fixture.written("series-1")
			fixture.startWindows(t)
			stream.effects.states, stream.windows.bytes = 1, 1
			next := execution.EvaluationResult{Contract: stream.header.Contract, Plans: []execution.PlanEvaluationResult{{
				Plan: fixture.due.Identity, StateResults: []execution.StateEvaluation{{Mutation: execution.StateMutation{
					Identity: fixture.key("series-2"), ApplyVersion: fixture.version}}}}}}
			if err := stream.windowBeforeMerge(context.Background(), next, mergeBytes{retained: share / 4}); err != nil {
				t.Fatal(err)
			}
			if stream.windows.zeroBoundCuts != c.counted {
				t.Fatalf("windows cut at a zero bound by phase = %v, want %v (bound %d)", stream.windows.zeroBoundCuts, c.counted,
					stream.windowBound())
			}
		})
	}
}
