// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inputslice_test

import (
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// A split the tree has no leaves left for says so, as its code
// (INPUT_SLICE_LEAVES_EXHAUSTED) does, and not that the values are
// concentrated; the other reasons say they are, as before. Either way the
// line carries the reason, the dimension and the slice.
func TestALeavesExhaustedSplitIsSaidSoNotAsConcentratedValues(t *testing.T) {
	slice := execution.InputSlice{Conditions: []execution.InputSliceCondition{{Dimension: "host", Pattern: "^a"}}}
	for _, test := range []struct {
		reason, says, not string
	}{
		{inputslice.ReasonMaxLeaves, "split leaves exhausted: max_leaves", "value concentrated"},
		{inputslice.ReasonSingleValue, "input value concentrated: single_value", "leaves exhausted"},
		{inputslice.ReasonConditionBudget, "input value concentrated: condition_budget", "leaves exhausted"},
	} {
		text := (&inputslice.ValueConcentratedError{Reason: test.reason, Dimension: "host", Slice: slice}).Error()
		if !strings.Contains(text, test.says) || strings.Contains(text, test.not) ||
			!strings.Contains(text, `on dimension "host", slice [host req ^a]`) {
			t.Fatalf("%s says %q, want %q and its dimension and slice, not %q", test.reason, text, test.says, test.not)
		}
	}
}
