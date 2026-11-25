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
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Slot reads as many slices at once as half the process's query permits
// allow, four at most, counted in requests: each slice sends every query of
// the Slot, each taking a permit, so a Slot of several queries - an item
// detected over several inputs - never takes more than half the permits
// either. A Slot of one query reads as wide as it did.
func TestASlotsReadWidthIsCountedInRequests(t *testing.T) {
	for _, test := range []struct {
		permits, queries, want int
	}{
		{permits: 32, queries: 1, want: 4},
		{permits: 32, queries: 4, want: 4},
		{permits: 8, queries: 1, want: 4},
		{permits: 8, queries: 2, want: 2},
		{permits: 8, queries: 4, want: 1},
		{permits: 3, queries: 3, want: 1},
		{permits: 0, queries: 4, want: 2},
	} {
		coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{QueryPermits: test.permits}}
		got := coordinator.sliceReadWidth(test.queries)
		if got != test.want {
			t.Errorf("%d permits, %d queries a slice: width %d, want %d", test.permits, test.queries, got, test.want)
		}
		if test.permits > 1 && got*test.queries > max(test.permits/2, test.queries) {
			t.Errorf("%d permits, %d queries a slice: %d permits in flight, over half", test.permits, test.queries, got*test.queries)
		}
	}
}

// The requests counted are those carrying the series judged: the PRIMARY's
// and each other input of an item detected over several. A ring ratio's
// history is not one: its Slot reads as wide as before any input was counted,
// half the permits, four at most, on every count of permits.
func TestOnlyTheInputsOfAnItemDetectedOverSeveralNarrowItsSlot(t *testing.T) {
	header := func(roles ...execution.InputRole) execution.InternalExecutionHeader {
		var header execution.InternalExecutionHeader
		for index, role := range roles {
			ref := execution.LogicalQueryRef(fmt.Sprintf("query-%d", index))
			header.Requirements = append(header.Requirements, execution.DataRequirement{Role: role, LogicalQueryRef: ref})
			header.RequiredPhysicalQueries = append(header.RequiredPhysicalQueries,
				execution.PlannedPhysicalQueryRef{Digest: execution.PhysicalQueryDigest(ref)})
		}
		return header
	}
	ringRatio := header(execution.InputRolePrimary, execution.InputRoleAlgorithmDependency)
	multiDetect := header(execution.InputRolePrimary, execution.InputRoleMultiDetectInput, execution.InputRoleMultiDetectInput)
	if got := judgedRequests(ringRatio); got != 1 {
		t.Fatalf("a ring ratio's Slot counts %d requests, want its PRIMARY's alone", got)
	}
	if got := judgedRequests(multiDetect); got != 3 {
		t.Fatalf("an item over three inputs counts %d requests, want each input's", got)
	}
	for _, permits := range []int{2, 3, 4, 6, 8, 12, 16, 32} {
		coordinator := &SlotExecutionCoordinator{budget: ProvisionalBudget{QueryPermits: permits}}
		// The width before any request was counted (82da65729).
		before := min(4, max(1, permits/2))
		if got := coordinator.sliceReadWidth(judgedRequests(ringRatio)); got != before {
			t.Errorf("%d permits: a ring ratio reads %d slices at once, want %d as before", permits, got, before)
		}
		if got := coordinator.sliceReadWidth(judgedRequests(multiDetect)); got*3 > max(permits/2, 3) {
			t.Errorf("%d permits: an item over three inputs reads %d slices at once, over half the permits", permits, got)
		}
	}
}
