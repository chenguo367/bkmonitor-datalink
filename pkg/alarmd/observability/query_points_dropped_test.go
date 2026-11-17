// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"bytes"
	"encoding/json"
	"testing"
)

// Every reason reads its own count, the total is all of them, and a line
// carries the drops only when there are any.
func TestEveryPointDropReasonReadsItsOwnCount(t *testing.T) {
	drops := PointDrops{OutsideRangeBefore: 1, OutsideRangeAfter: 2, RowMissingColumns: 4}
	sum := uint64(0)
	for index, reason := range PointDropReasons {
		if got := drops.Of(reason); got != 1<<index {
			t.Fatalf("%s reads %d, want %d", reason, got, 1<<index)
		}
		sum += drops.Of(reason)
	}
	if sum != drops.Total() || drops.Total() != 7 || drops.Of("other") != 0 {
		t.Fatalf("total %d, sum %d", drops.Total(), sum)
	}
	if drops.Add(drops) != (PointDrops{2, 4, 8}) || (PointDrops{}).Facts() != nil || drops.Facts() == nil || *drops.Facts() != drops {
		t.Fatal("Add or Facts")
	}
}

// The query line and the Slot's completion line carry the points let go, by
// reason; a query line that let none go carries no field.
func TestTheLinesCarryThePointsDropped(t *testing.T) {
	var output bytes.Buffer
	renderTo(&output, Observation{Component: ComponentAccess, Stage: StageQueryCompleted, Result: ResultSuccess,
		QueryPointsDropped: &PointDrops{OutsideRangeBefore: 3, OutsideRangeAfter: 1}})
	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode: %v; log=%s", err, output.String())
	}
	dropped, _ := event["points_dropped"].(map[string]any)
	if dropped["outside_range_before"] != float64(3) || dropped["outside_range_after"] != float64(1) {
		t.Fatalf("points_dropped = %#v; event=%#v", event["points_dropped"], event)
	}
	output.Reset()
	renderTo(&output, Observation{Component: ComponentAccess, Stage: StageQueryCompleted, Result: ResultSuccess})
	if bytes.Contains(output.Bytes(), []byte("points_dropped")) {
		t.Fatalf("a query that let nothing go names it: %s", output.String())
	}
	output.Reset()
	renderTo(&output, Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultSuccess,
		SlotBudgetUsage: &SlotBudgetUsageFacts{PointsDropped: PointDrops{OutsideRangeAfter: 2}}})
	if !bytes.Contains(output.Bytes(), []byte(`"points_dropped":{"outside_range_after":2}`)) {
		t.Fatalf("the completion line does not carry the Slot's drops: %s", output.String())
	}
}
