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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The Slot's first slice fixes its window ceiling; a slice after it,
// completed once a boundary moved the deadline on, raises it (§7.5). A
// consumer of two raises it as one stream does: the ceiling and the windows
// it allows are the Slot's, whichever of its streams completed the first
// slice (015 C1).
func TestAConsumersSliceRaisesTheWindowCeilingAsOneStreamsDoes(t *testing.T) {
	deadline := time.UnixMilli(1_800_000_000_000)
	at := func(when time.Time) context.Context {
		marker := &execution.StageMarker{}
		marker.Begin(execution.SlotStageEvaluate)
		marker.Extend(when, when)
		return execution.WithStageMarker(context.Background(), marker)
	}
	ceiling := func(k int) (fixed, raised int64) {
		fixture := newWindowedFixture(t)
		dispatch := fixture.stream
		dispatch.fixWindowCeiling(at(deadline), 5)
		fixed = dispatch.windowCeiling.Load()
		next := dispatch
		if k > 1 {
			next = sideBySideStreams(dispatch, k, nil, nil)[0]
		}
		next.raiseSliceCeiling(at(deadline.Add(time.Minute)))
		return fixed, dispatch.windowCeiling.Load()
	}
	fixedOne, raisedOne := ceiling(1)
	fixedTwo, raisedTwo := ceiling(2)
	if raisedOne <= fixedOne {
		t.Fatalf("fixture: one stream's slice left the ceiling at %d, fixed at %d; want it raised", raisedOne, fixedOne)
	}
	if fixedTwo != fixedOne || raisedTwo != raisedOne {
		t.Fatalf("a consumer's slice took the ceiling from %d to %d, one stream's from %d to %d", fixedTwo, raisedTwo,
			fixedOne, raisedOne)
	}
}

// What a consumer's windows count of the window bound - the windows cut at a
// bound of zero, the bound's subtraction clamped - is on the Slot's
// completion row, beside the dispatch's: clamped says a window's give-back
// was counted twice, which each consumer's own windows can do.
func TestAConsumersWindowCountsReachTheCompletionRow(t *testing.T) {
	fixture := newWindowedFixture(t)
	dispatch := fixture.stream
	consumers := sideBySideStreams(dispatch, 2, nil, nil)
	dispatch.fill.zeroBoundCuts[0]++
	consumers[0].fill.zeroBoundCuts[0]++
	consumers[1].fill.zeroBoundCuts[3] += 2
	consumers[1].fill.clamped[0] = true
	usage := dispatch.budgetUsage()
	if usage.WindowZeroBoundCutsInput != 2 || usage.WindowZeroBoundCutsState != 2 || usage.WindowOutsideClamped != 1 ||
		usage.SliceOthersClamped != 0 {
		t.Fatalf("the completion row says %d input and %d state cuts at a zero bound, clamped %d and %d; want 2, 2, 1 and 0",
			usage.WindowZeroBoundCutsInput, usage.WindowZeroBoundCutsState, usage.WindowOutsideClamped, usage.SliceOthersClamped)
	}
}
