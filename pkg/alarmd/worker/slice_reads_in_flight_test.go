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
	"testing"
)

// A Slot read in slices counts every slice read it began - its own and its
// reads ahead - by the reads in flight as each began, and times each read's
// end from the one before's, through Execute (015 O1): the counts add up to
// the reads the provider was asked, and a Slot of several slices has a
// handoff for each read after its first.
func TestASlicedSlotCountsEveryReadItBeganByTheReadsInFlight(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(8, 1<<10), sliceableFacts(), 200)
	fixture.source.timeoutWhole = true
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	fixture.source.mu.Lock()
	fixture.source.timeoutWhole = false
	first := len(fixture.source.calls)
	fixture.source.mu.Unlock()
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed || result.Usage.InputSlices < 2 {
		t.Fatalf("result=%+v err=%v, want the Slot completed in slices", result, err)
	}
	fixture.source.mu.Lock()
	asked := len(fixture.source.calls) - first
	fixture.source.mu.Unlock()
	var began uint64
	for _, count := range result.Usage.InputSliceReadsInFlight {
		began += count
	}
	if began != uint64(asked) || began < result.Usage.InputSlices {
		t.Fatalf("reads by how many were in flight as each began %v (%d), the provider asked %d, slices %d: want every read counted once",
			result.Usage.InputSliceReadsInFlight, began, asked, result.Usage.InputSlices)
	}
}
