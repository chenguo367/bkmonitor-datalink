// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The window measurement's line says every shape outcome
// (observability.InputSliceShapeOutcomes) from the completion row's field of
// its name, each field set to a value of its own: an outcome said from
// another's field, or one the line leaves out, reads as the wrong number - as
// the line of sixteen did once the list grew to twenty-three, indexed past its
// end.
func TestTheWindowMeasurementSaysEveryShapeOutcomeFromItsField(t *testing.T) {
	var usage observability.SlotBudgetUsageFacts
	value := reflect.ValueOf(&usage).Elem()
	fields := map[string][]int{}
	for _, field := range reflect.VisibleFields(value.Type()) {
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		fields[tag] = field.Index
	}
	for index, outcome := range observability.InputSliceShapeOutcomes {
		tag := "input_slice_shape_" + outcome
		if outcome == observability.InputSliceShapeSizedSplit {
			tag = "input_slice_start_discarded"
		}
		at, ok := fields[tag]
		if !ok {
			t.Fatalf("outcome %q has no row field %q", outcome, tag)
		}
		value.FieldByIndex(at).SetUint(uint64(100 + index))
	}
	line := windowMeasureShapesLine(usage)
	parts := strings.Split(line, ",")
	if len(parts) != len(observability.InputSliceShapeOutcomes) {
		t.Fatalf("the line says %d outcomes, want %d: %s", len(parts), len(observability.InputSliceShapeOutcomes), line)
	}
	for index, outcome := range observability.InputSliceShapeOutcomes {
		if want := fmt.Sprintf("%s:%d", outcome, 100+index); parts[index] != want {
			t.Errorf("outcome %d says %q, want %q", index, parts[index], want)
		}
	}
}
