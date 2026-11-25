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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A split the tree has no leaves left for is refused under its own word,
// INPUT_SLICE_LEAVES_EXHAUSTED, the tree's reason and dimension its detail;
// values no split separates, or a split's conditions past their budget,
// under INPUT_SLICE_VALUE_CONCENTRATED, as before.
func TestASplitWithNoLeavesLeftIsRefusedApartFromConcentratedValues(t *testing.T) {
	for _, test := range []struct {
		reason, want string
	}{
		{inputslice.ReasonMaxLeaves, contract.ReasonInputSliceLeavesExhausted},
		{inputslice.ReasonSingleValue, contract.ReasonInputSliceValueConcentrated},
		{inputslice.ReasonConditionBudget, contract.ReasonInputSliceValueConcentrated},
	} {
		err := fmt.Errorf("alarmd worker: split input slice: %w",
			&inputslice.ValueConcentratedError{Reason: test.reason, Dimension: "bk_target_ip"})
		if word, sliced := worker.InputSliceRefusal(err); !sliced || word != test.want {
			t.Fatalf("%s: refused as %q (%t), want %s", test.reason, word, sliced, test.want)
		}
		if !errors.Is(err, inputslice.ErrValueConcentrated) {
			t.Fatalf("%s: no longer an inputslice.ErrValueConcentrated", test.reason)
		}
	}
}

// A Slot with more series than the slices one Slot is read in can hold -
// each a third of the slice limit, three times the tree's leaves of them and
// more, every host its own value - is refused when a split has no leaves
// left: INPUT_SLICE_LEAVES_EXHAUSTED, the tree's reason its why, and not
// INPUT_SLICE_VALUE_CONCENTRATED, which a store answering every slice whole
// still is (TestASourceThatIgnoresTheSliceEndsValueConcentratedWithinKMax).
func TestASlotNeedingMoreSlicesThanTheTreeHasLeavesIsRefusedAsLeavesExhausted(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(3*inputslice.MaxLeaves+8, slicedLimit/3), sliceableFacts(), 400)
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	if err == nil || result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot refused", result, err)
	}
	if reason, ok := worker.InputSliceRefusal(err); !ok || reason != contract.ReasonInputSliceLeavesExhausted {
		t.Fatalf("refusal = %q, %v (err %v), want %s", reason, ok, err, contract.ReasonInputSliceLeavesExhausted)
	}
	if refusal, final := execution.ShareRefusalOf(err); !final || refusal.Reason != contract.ReasonInputSliceLeavesExhausted ||
		!strings.HasPrefix(refusal.Detail, inputslice.ReasonMaxLeaves+" on ") {
		t.Fatalf("share refusal of %v = %+v, %t; want a final one under %s, its why the tree's", err, refusal, final,
			contract.ReasonInputSliceLeavesExhausted)
	}
}
