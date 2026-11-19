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

// Slots given up without a query are counted in Slots from their slot_skipped
// lines, by kind and cause: a range by the Slots it covers, a Slot alone by
// one, the pool's gap apart from a Query Group fallen behind. A Slot the
// Progress store dropped is not a query-free finalization and is not counted
// here; a cause outside the list is other. A range line that counts no Slot
// adds none, where it was read as one.
func TestSlotGapsAreCountedInSlotsByKindAndCause(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	for _, facts := range []observability.SlotSkippedFacts{
		{Path: observability.SlotSkippedExpiredRange, Reason: "GAP_SKIPPED", Cause: observability.CompletionCauseQueryCooldown, SlotCount: 4},
		{Path: observability.SlotSkippedQueryFree, Reason: "GAP_SKIPPED", Cause: observability.CompletionCauseQueryCooldown, SlotCount: 1},
		{Path: observability.SlotSkippedQueryFree, Reason: "SNAPSHOT_UNAVAILABLE", Cause: observability.CompletionCauseQueryCooldown, SlotCount: 1},
		{Path: observability.SlotSkippedExpiredRange, Reason: "GAP_SKIPPED", Cause: "EXPIRED_REPLAY", SlotCount: 3},
		{Path: observability.SlotSkippedExpiredRange, Reason: "GAP_SKIPPED", Cause: "EXPIRED_REPLAY", SlotCount: 0},
		{Path: observability.SlotSkippedQueryFree, Reason: "GAP_SKIPPED", Cause: "A_WORD_NOBODY_PUBLISHED", SlotCount: 1},
		{Path: observability.SlotSkippedProgressStore, Reason: "GAP_SKIPPED", Cause: "EXPIRED_REPLAY", SlotCount: 1},
	} {
		observability.ObserveSlotSkipped(context.Background(), r, observability.TraceFields{QueryGroupKey: "qg-101"},
			observability.OperationWrite, facts)
	}
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[[2]string]float64{}
	for _, family := range families {
		if family.GetName() != "alarmd_slot_gaps_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			var key [2]string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "kind":
					key[0] = label.GetValue()
				case "cause":
					key[1] = label.GetValue()
				}
			}
			got[key] = metric.GetCounter().GetValue()
		}
	}
	want := map[[2]string]float64{
		{"GAP_SKIPPED", "QUERY_COOLDOWN"}:          5,
		{"SNAPSHOT_UNAVAILABLE", "QUERY_COOLDOWN"}: 1,
		{"GAP_SKIPPED", "EXPIRED_REPLAY"}:          3,
		{"GAP_SKIPPED", "other"}:                   1,
	}
	if len(got) != (len(observability.QueryFreeCompletionKinds)+1)*(len(observability.QueryFreeCompletionCauses)+1) {
		t.Fatalf("%d cells, want every kind and cause from the start", len(got))
	}
	for key, value := range got {
		if value != want[key] {
			t.Fatalf("slot gaps %v = %v, want %v (all: %v)", key, value, want[key], got)
		}
	}
}
