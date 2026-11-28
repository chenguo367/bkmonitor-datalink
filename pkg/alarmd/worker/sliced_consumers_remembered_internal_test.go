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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// The k a Slot's slices were consumed with is what its Query Group's next
// Slot counts and holds its slices for: two side by side, one on its own
// stream. A Slot read whole says nothing of it, and the k remembered before
// is kept.
func TestTheStreamsASlotsSlicesWereConsumedByAreRememberedForItsNextSlot(t *testing.T) {
	request := execution.SlotExecutionRequest{Contract: execution.FrozenExecutionContractRef{
		Slot: execution.SlotIdentity{QueryGroup: "query-group", EvaluationTime: 1_788_000_000}}}
	group := request.Contract.Slot.QueryGroup
	for _, cell := range []struct {
		name                       string
		consumers                  int
		sliced                     bool
		remembered, wantRemembered int
	}{
		{"consumed two side by side", 2, true, 1, 2},
		{"consumed on its own stream", 0, true, 2, 1},
		{"read whole", 0, false, 2, 2},
	} {
		coordinator := &SlotExecutionCoordinator{}
		coordinator.slicedInput.remember(group, slicedReading{consumers: cell.remembered})
		reading := &sliceReading{consumers: cell.consumers, dimensions: []string{"host"}}
		if cell.sliced {
			tree, err := inputslice.New([]string{"host"}, 2, "host")
			if err != nil {
				t.Fatal(err)
			}
			reading.tree = tree
		}
		stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, slices: reading}}
		coordinator.rememberSlotInput(request, stream, false)
		if got := coordinator.slicedInput.lastInput(group).consumers; got != cell.wantRemembered {
			t.Fatalf("%s: the next Slot is held for %d consumers, want %d", cell.name, got, cell.wantRemembered)
		}
	}
}
