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
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Each shape outcome is counted from the completion row's field of its name
// - input_slice_shape_<outcome>, and sized_split from the row's
// input_slice_start_discarded, the one fact both say - each field set to a
// value of its own, so a field counted under another outcome's cell, or one
// counted nowhere, reads as the wrong number.
func TestEveryShapeOutcomeIsCountedFromTheRowFieldOfItsName(t *testing.T) {
	usage := &observability.SlotBudgetUsageFacts{}
	value := reflect.ValueOf(usage).Elem()
	want := map[string]float64{}
	for index, outcome := range observability.InputSliceShapeOutcomes {
		tag := "input_slice_shape_" + outcome
		if outcome == observability.InputSliceShapeSizedSplit {
			tag = "input_slice_start_discarded"
		}
		field, ok := fieldByJSON(value.Type(), tag)
		if !ok {
			t.Fatalf("outcome %q has no row field %q", outcome, tag)
		}
		value.FieldByIndex(field.Index).SetUint(uint64(index + 1))
		want[outcome] = float64(index + 1)
	}

	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	recorder.Observe(context.Background(), observability.Observation{
		Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
		Result: observability.ResultSuccess, SlotBudgetUsage: usage,
	})
	for outcome, count := range want {
		if got := redisCounterSum(t, recorder, "alarmd_worker_input_slice_shapes_total",
			map[string]string{"outcome": outcome}); got != count {
			t.Errorf("input_slice_shapes_total{outcome=%q} = %v, want %v", outcome, got, count)
		}
	}
	// A supplement completes no Slot: what it did is no Slot's shape.
	recorder.Observe(context.Background(), observability.Observation{
		Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
		Operation: observability.OperationSupplement, Result: observability.ResultSuccess, SlotBudgetUsage: usage,
	})
	if got := redisCounterSum(t, recorder, "alarmd_worker_input_slice_shapes_total",
		map[string]string{"outcome": observability.InputSliceShapeSizedKept}); got != want[observability.InputSliceShapeSizedKept] {
		t.Fatalf("a supplement's completion moved sized_kept to %v", got)
	}
}

func fieldByJSON(typ reflect.Type, tag string) (reflect.StructField, bool) {
	for index := 0; index < typ.NumField(); index++ {
		field := typ.Field(index)
		if name, _, _ := strings.Cut(field.Tag.Get("json"), ","); name == tag {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

// Every field a superseded projection can name has its cell from the start,
// and a word outside the list moves none.
func TestTheSupersededProjectionCountHasACellForEveryFieldAndNoOther(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	for _, field := range []string{"contract", "deadline", "keep_until", "due_plan_targets"} {
		if samples := redisSamples(t, recorder, "alarmd_progress_projection_superseded_total",
			map[string]string{"field": field}); len(samples) != 1 || samples[0].GetCounter().GetValue() != 0 {
			t.Fatalf("field %q: %d samples before any supersede, want one cell at zero", field, len(samples))
		}
	}
	recorder.RecordProgressProjectionSuperseded("deadline")
	recorder.RecordProgressProjectionSuperseded("deadline")
	recorder.RecordProgressProjectionSuperseded("slot")
	if got := redisCounterSum(t, recorder, "alarmd_progress_projection_superseded_total",
		map[string]string{"field": "deadline"}); got != 2 {
		t.Fatalf("deadline = %v, want 2", got)
	}
	if got := redisCounterSum(t, recorder, "alarmd_progress_projection_superseded_total", nil); got != 2 {
		t.Fatalf("all fields = %v, want 2: a field outside the list counted", got)
	}
}

// A Slot whose window accounting clamped is counted once under the reason its
// row names, each reason from its own field: window_outside from
// window_outside_clamped, slice_others from slice_others_clamped. A Slot that
// clamped nothing counts nothing.
func TestAClampedWindowAccountingIsCountedOnceASlotUnderItsReason(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	complete := func(usage *observability.SlotBudgetUsageFacts) {
		recorder.Observe(context.Background(), observability.Observation{
			Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
			Result: observability.ResultSuccess, SlotBudgetUsage: usage,
		})
	}
	count := func(reason string) float64 {
		return redisCounterSum(t, recorder, "alarmd_slot_window_accounting_clamped_slots_total", map[string]string{"reason": reason})
	}
	complete(&observability.SlotBudgetUsageFacts{})
	complete(&observability.SlotBudgetUsageFacts{WindowOutsideClamped: 1})
	complete(&observability.SlotBudgetUsageFacts{SliceOthersClamped: 1})
	complete(&observability.SlotBudgetUsageFacts{SliceOthersClamped: 1})
	if got := count("window_outside"); got != 1 {
		t.Fatalf("window_outside = %v, want the one Slot that clamped there", got)
	}
	if got := count("slice_others"); got != 2 {
		t.Fatalf("slice_others = %v, want the two Slots that clamped there", got)
	}
}
