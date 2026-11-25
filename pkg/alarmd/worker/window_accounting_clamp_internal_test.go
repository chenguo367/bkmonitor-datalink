// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "testing"

// The window bound and the slice limit take off what the Slot holds the
// window or slice of its own, what the windows in flight give back as they
// land and what the apply side holds. Taking off more than the Slot holds is
// a give-back counted twice: the take-off is clamped and the Slot says so,
// by where - once a Slot, however often - on its completion row
// (slot_window_accounting_clamped_slots_total). Each where is set by its own
// take-off and by no other.
func TestAWindowAccountingThatTakesOffMoreThanTheSlotHoldsSaysWhere(t *testing.T) {
	for _, c := range []struct {
		name    string
		takeOff func(*streamedExecution) uint64
		set     [2]bool
	}{
		{"window_outside", (*streamedExecution).windowOutside, [2]bool{true, false}},
		{"slice_others", (*streamedExecution).sliceOthers, [2]bool{false, true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			co, _, _ := gapReservationFixture()
			stream := &streamedExecution{coordinator: co, slices: &sliceReading{}}
			stream.retainBytes(retainPhaseInput, 1000)
			stream.windows.pipe = &applyPipe{stream: stream}
			// Within what the Slot holds: nothing clamped.
			stream.windows.pipe.inflightRelease[retainPhaseInput] = 600
			if got := c.takeOff(stream); got != 400 || stream.windows.clamped != ([2]bool{}) {
				t.Fatalf("taking off 600 of 1000: %d, clamped %v; want 400 and nothing said", got, stream.windows.clamped)
			}
			// Past it, twice.
			stream.windows.pipe.inflightRelease[retainPhaseOutput] = 700
			for range 2 {
				if got := c.takeOff(stream); got != 0 {
					t.Fatalf("taking off 1300 of 1000: %d, want it clamped to 0", got)
				}
			}
			if stream.windows.clamped != c.set {
				t.Fatalf("clamped %v, want %v", stream.windows.clamped, c.set)
			}
			usage := stream.budgetUsage()
			if (usage.WindowOutsideClamped == 1) != c.set[0] || (usage.SliceOthersClamped == 1) != c.set[1] {
				t.Fatalf("the completion row says window_outside %d, slice_others %d; want %v", usage.WindowOutsideClamped,
					usage.SliceOthersClamped, c.set)
			}
			stream.windows.pipe = nil
			stream.releaseProvisional()
		})
	}
}
