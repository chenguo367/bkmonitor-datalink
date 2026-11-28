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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Query Group two streams consumed starts its next Slot in slices counted
// for them: its last read's bytes over the 192 KiB a slice may hold with two
// (a quarter of the 1.5 MiB share over k), not over the 384 KiB one stream
// may. Counted for one, every slice it started in was twice what either
// consumer may hold, and was let go.
//
// 1,600 KiB last read: nine slices at least for two, five for one.
func TestAQueryGroupTwoStreamsConsumedStartsInSlicesCountedForThem(t *testing.T) {
	counted := func(k int) int {
		fixture := newSlicedFixture(t, slicedHosts(40, 40<<10), sliceableFacts(), 200)
		request := windowedRequest()
		group := request.Contract.Slot.QueryGroup
		worker.RememberSliceShapeForTest(fixture.coordinator, group, request.Contract.QueryRevision, []string{"host"},
			inputslice.Shape{}, 1, 40*(40<<10), 40)
		worker.RememberSliceConsumersForTest(fixture.coordinator, group, k)
		slices, held := worker.RestoredSlices(fixture.coordinator, request)
		if !held {
			t.Fatal("the memory held nothing of the Query Group")
		}
		return len(slices)
	}
	one, two := counted(1), counted(2)
	if two < 9 || two <= one {
		t.Fatalf("counted for one stream %d slices, for two %d: want at least the nine 1,600 KiB asks for at 192 KiB, "+
			"more than for one", one, two)
	}
}
