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
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The window pipeline's parts of a completed Slot's clock are counters a
// scrape reads, not only fields of a completion row the log admission
// samples: each Slot applied window by window adds its apply_wait, land and
// apply_overlap, in seconds. A Slot applied whole, the three zero, adds
// nothing; nor does an attempt that failed, a supplement, or a completion
// that carries no timing.
func TestTheWindowPipelinesPartsOfCompletedSlotsAreCountedInSeconds(t *testing.T) {
	recorder := NewRecorder(BuildInfo{})
	ctx := context.Background()
	completed := func(timing *observability.SlotTimingFacts, operation observability.Operation, err error) {
		recorder.Observe(ctx, observability.Observation{
			Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted, Result: observability.ResultSuccess,
			Operation: operation, SlotTiming: timing, Err: err,
		})
	}
	completed(&observability.SlotTimingFacts{Slot: 9_000, Apply: 5_500, ApplyWait: 1_500, Land: 250, ApplyOverlap: 4_000},
		observability.OperationNormal, nil)
	completed(&observability.SlotTimingFacts{Slot: 3_000, Apply: 2_000, ApplyWait: 500, Land: 750, ApplyOverlap: 1_500},
		observability.OperationNormal, nil)
	// Applied whole: its writes are its Apply, in its own partition.
	completed(&observability.SlotTimingFacts{Slot: 4_000, Apply: 3_000}, observability.OperationNormal, nil)
	beside := &observability.SlotTimingFacts{Slot: 60_000, Apply: 60_000, ApplyWait: 60_000, Land: 60_000, ApplyOverlap: 60_000}
	completed(beside, observability.OperationNormal, errors.New("alarmd worker: query: failed"))
	completed(beside, observability.OperationSupplement, nil)
	completed(nil, observability.OperationNormal, nil)
	for name, want := range map[string]float64{
		"alarmd_slot_apply_wait_seconds_total":    2,
		"alarmd_slot_land_seconds_total":          1,
		"alarmd_slot_apply_overlap_seconds_total": 5.5,
	} {
		cells := gatherFamily(t, recorder, name)
		if len(cells) != 1 || cells[0].GetCounter().GetValue() != want {
			t.Fatalf("%s = %v, want one counter at %v seconds", name, cells, want)
		}
	}
}
