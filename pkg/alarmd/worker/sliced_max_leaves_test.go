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
	"context"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Query Group last read at 76 slices' bytes - the 1.0 next release gate's
// 500,000 series at four cores, whose first Slot let its slices go at the
// half share each of two consumers reads in (015 C1) - starts its next Slot in
// slices counted from those bytes, and one of them past a slice's limit at k
// = 2 is split. With the tree's leaves at 64 the start was capped at the 61
// a Slot may start in, and that one split passed the 64: the Slot was
// refused INPUT_SLICE_LEAVES_EXHAUSTED, every Query Group of that size once a
// deploy, in its second round. Within 128 the Slot starts in the slices its
// bytes ask for, splits the one, and completes, as does the Slot after it.
//
// 300 hosts of 10 KiB, their last two characters spread over the fixed
// alphabet so that its classes cut them evenly, and one of 175 KiB: a slice
// may retain 192 KiB at k = 2 (slicedShare / 2 / 2 / 2), so a slice holding
// it and two of the others is let go and split by the next characters of
// what it delivered, five ways; the others' slices hold a few of them each,
// well within. 301 series, the fixture's Slot holding up to 400.
func TestAQueryGroupCountedPastTheOldLeavesAtTwoConsumersIsNotRefusedForThem(t *testing.T) {
	worker.SetSliceConsumersForTest(t, 2)
	before := slicedMaxSeries
	slicedMaxSeries = 400
	t.Cleanup(func() { slicedMaxSeries = before })
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 0, 301)
	for index := 0; index < 300; index++ {
		series = append(series, slicedSeries{
			host: fmt.Sprintf("h%03d%c%c", index, alphabet[(index*13)%len(alphabet)], alphabet[(index*17+5)%len(alphabet)]), padding: 10 << 10,
		})
	}
	series = append(series, slicedSeries{host: "large-0", padding: 175 << 10})
	fixture := newSlicedFixture(t, series, sliceableFacts(), 1_000)
	request := windowedRequest()
	worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
		[]string{"host"}, inputslice.Shape{}, 1, 76*slicedLimit, uint64(len(series)))
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if reason, refused := worker.InputSliceRefusal(err); refused {
		t.Fatalf("the Slot counted into 76 slices refused %s: result=%+v, want it completed", reason, result)
	}
	if err != nil || !result.Completed {
		t.Fatalf("the Slot counted into 76 slices: result=%+v err=%v, want it completed", result, err)
	}
	if result.Usage.InputSlices < 76 || result.Usage.InputSlicesDiscarded == 0 {
		t.Fatalf("the Slot read %d slices, %d let go: want at least the 76 its bytes ask for, and the large one's split",
			result.Usage.InputSlices, result.Usage.InputSlicesDiscarded)
	}
	result, err = fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("the Slot after: result=%+v err=%v, want it completed", result, err)
	}
}
