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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// letGoLines is the input_slice_let_go lines the fixture's Slots said.
func (fixture *slicedFixture) letGoLines() []observability.InputSliceLetGoFacts {
	slicedObservationsMu.Lock()
	defer slicedObservationsMu.Unlock()
	var lines []observability.InputSliceLetGoFacts
	for _, observation := range *fixture.observations {
		if observation.Stage == observability.StageInputSliceLetGo && observation.InputSliceLetGo != nil {
			lines = append(lines, *observation.InputSliceLetGo)
		}
	}
	return lines
}

// A slice let go as it streams is said on a line of its own: what it held,
// the batch that took it past, the limit and the limit it would have with
// nothing beside it, so a reading tells a slice let go for its size from one
// let go for what was held beside it. The Slot's usage says the k its slices
// were consumed with: one on its own stream, two side by side, none read
// whole.
func TestASliceLetGoIsSaidWithItsLimitAndTheSlotSaysItsConsumers(t *testing.T) {
	// 24 series of 40 KiB, 960 KiB, read whole past the 384 KiB a slice may
	// hold: let go and split, on the Slot's own stream.
	worker.SetSliceConsumersForTest(t, 1)
	fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	result, err := fixture.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	lines := fixture.letGoLines()
	if uint64(len(lines)) != result.Usage.InputSlicesDiscarded || len(lines) == 0 {
		t.Fatalf("%d let_go lines, %d slices let go: want a line a slice let go", len(lines), result.Usage.InputSlicesDiscarded)
	}
	first := lines[0]
	if first.Consumers != 1 || first.ShareBytes != slicedShare || first.OwnLimitBytes != slicedLimit ||
		first.LimitBytes > first.OwnLimitBytes || first.SliceBytes+first.BatchBytes <= first.LimitBytes || !first.ForItsSize {
		t.Fatalf("the whole read's let_go line %+v: want k 1, the share %d, its own limit %d, the slice and its batch past "+
			"the limit, let go for its size", first, slicedShare, slicedLimit)
	}
	if result.Usage.InputSliceConsumers != 1 {
		t.Fatalf("a Slot read in slices on its own stream says %d consumers, want 1", result.Usage.InputSliceConsumers)
	}

	worker.SetSliceConsumersForTest(t, 2)
	side := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	result, err = side.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed || result.Usage.InputSliceConsumers != 2 {
		t.Fatalf("side by side: result=%+v err=%v, want 2 consumers said", result, err)
	}

	whole := newSlicedFixture(t, slicedHosts(4, 1<<10), sliceableFacts(), 200)
	result, err = whole.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed || result.Usage.InputSliceConsumers != 0 || len(whole.letGoLines()) != 0 {
		t.Fatalf("read whole: result=%+v err=%v lines=%d, want no consumers said and no line", result, err, len(whole.letGoLines()))
	}
}
