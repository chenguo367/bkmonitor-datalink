// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// A Slot carries the segment skip word of the Assignment record it read with
// its fence, and the Runner hands it to the executor on the request; a
// record without the word dispatches none. So does a Slot of a Query Group
// draining to its retired boundary: the word its record holds as it reads
// it, none once the Leader took it back, and its next Slot is written whole.
// The no-data family's word (014 next9 ⑥) travels beside it the same way.
func TestASlotCarriesTheSegmentSkipWordItsFenceReadToItsRequest(t *testing.T) {
	boundary := execution.EvaluationTime(600)
	for _, shape := range []struct {
		word uint64
		end  *execution.EvaluationTime
	}{{0, nil}, {9, nil}, {9, &boundary}, {0, &boundary}} {
		word := shape.word
		schedule := schedulerSchedule(t, 60, 60, shape.end, "snapshot-1", 1)
		catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
		record := testAssignment("worker-1", 3)
		record.SegmentSkip, record.SegmentSkipNoData = word, word+1
		session := &sequenceOwnerSession{fences: []execution.OwnerFence{testFence(7)}, records: []ownership.AssignmentRecord{record}}
		source := mustProductionSlotSource(t, session, catalog, &fakeProgressReader{result: missingProgress(), catalog: catalog}, time.Unix(200, 0))
		slot, due, _, err := source.Next(context.Background(), "query-group-1")
		if err != nil || !due {
			t.Fatalf("word %d: Next() due=%v error=%v", word, due, err)
		}
		if slot.Dispatch.SegmentSkip != word || slot.Dispatch.SegmentSkipNoData != word+1 {
			t.Fatalf("the Slot dispatched the words %d and %d, want the record's %d and %d", slot.Dispatch.SegmentSkip,
				slot.Dispatch.SegmentSkipNoData, word, word+1)
		}
		if shape.end != nil && slot.Contract.Slot.EvaluationTime >= *shape.end {
			t.Fatalf("the draining Query Group's Slot %d is not before its boundary %d", slot.Contract.Slot.EvaluationTime, *shape.end)
		}
	}
	clock := newMutableClock(time.Unix(200, 0))
	slot := frozenSlot("query-group-1")
	slot.Dispatch.SegmentSkip, slot.Dispatch.SegmentSkipNoData = 9, 10
	var requests []execution.SlotExecutionRequest
	executor := &segmentSkipRecordingExecutor{requests: &requests}
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner("query-group-1", &fakeSession{fence: slot.Dispatch.OwnerFence}, &fakeSlotSource{slot: slot}, executor, flights, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runner.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].SegmentSkip != 9 || requests[0].SegmentSkipNoData != 10 {
		t.Fatalf("requests %+v, want one carrying the words 9 and 10", requests)
	}
}

type segmentSkipRecordingExecutor struct {
	requests *[]execution.SlotExecutionRequest
}

func (executor *segmentSkipRecordingExecutor) Execute(_ context.Context, request execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
	*executor.requests = append(*executor.requests, request)
	return execution.SlotExecutionResult{Completed: true}, nil
}
