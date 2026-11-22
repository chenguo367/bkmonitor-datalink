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
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// carryStore answers a Plan's own segments with none, and the generation it
// carries from with carry.
type carryStore struct {
	segment.Store
	carry segment.Stored
}

func (store carryStore) ReadSegments(_ context.Context, request segment.ReadRequest) (segment.ReadResult, error) {
	result := segment.ReadResult{}
	for index, plan := range request.Plans {
		stored := segment.Stored{Key: plan.Key}
		if index > 0 {
			stored = store.carry
			stored.Key = plan.Key
		}
		result.Plans = append(result.Plans, stored)
	}
	return result, nil
}

// A Plan edited to a new generation reads its series with no record of
// their own under the old one, and that read of the old book is counted by
// what it read as (014 B4 H8): carried, absent, undecodable - a page of it
// gone, the old generation's pages renewed no more since the edit - or not
// read. Only an undecodable one degrades the Plan.
func TestACarriedGenerationsBookIsCountedByWhatItReadAs(t *testing.T) {
	due := shadowDue
	due.StateGeneration, due.StateCarry = "g1", &execution.StateCarry{From: "g0"}
	page, err := segment.EncodePage([]segment.Entry{{Digest: shadowSeries(0), Start: 60}})
	if err != nil {
		t.Fatal(err)
	}
	mark, err := segment.EncodeMark(segment.Mark{Version: execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 240, SlotDigest: "slot"}, Position: 240, Pages: 1, Sizes: []int{len(page)},
		Renewed: []segment.Renewal{{At: 1_700_000_000_000, TTL: 3_000_000}}})
	if err != nil {
		t.Fatal(err)
	}
	for want, carry := range map[string]segment.Stored{
		observability.SegmentCarryCarried:     {Mark: mark, Pages: [][]byte{page}},
		observability.SegmentCarryAbsent:      {},
		observability.SegmentCarryUndecodable: {Mark: mark, Pages: [][]byte{nil}},
		observability.SegmentCarryReadFailed:  {Reason: segment.ReadFailed},
	} {
		slot := loadSegmentSlot(context.Background(), carryStore{carry: carry}, execution.SlotExecutionRequest{},
			[]segmentDue{{due: due, retention: 5}})
		if slot == nil || !slices.Equal(slot.carries, []string{want}) {
			t.Fatalf("%s: the carry read as %v, want %s", want, slot.carries, want)
		}
		if degraded := len(slot.undecodable) > 0; degraded != (want == observability.SegmentCarryUndecodable) {
			t.Fatalf("%s: undecodable %v", want, slot.undecodable)
		}
		var facts observability.SegmentShadowFacts
		for _, result := range slot.carries {
			facts.Carry(result)
		}
		if facts.Carries[want] != 1 || len(facts.Carries) != 1 {
			t.Fatalf("%s: counted %v", want, facts.Carries)
		}
	}
}

// Through the coordinator's load of a Slot's segments, what a Plan's read of
// the generation it carries from read as reaches the Slot's shadow facts: an
// old book its rounds wrote is counted carried, a generation that left none
// absent.
func TestASlotsCarriedReadIsCountedInItsShadowFacts(t *testing.T) {
	// The rig's rounds write the book of shadowDue's generation, which the
	// Plan carries from.
	store, observed := newMemorySegments(), &shadowObservations{}
	member := shadowSeries(0)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1320)}, false)
	runShadowFor(t, store, shadowDue, []shadowRoundOf{steady(t, member, 1380)}, false)
	coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: store},
		Now: func() time.Time { return time.Unix(1440, 0) }, Observer: observed,
		InstalledControlEpoch: func() (uint64, bool) { return 7, true }}}
	for want, from := range map[string]execution.StateGeneration{
		observability.SegmentCarryCarried: shadowDue.StateGeneration,
		observability.SegmentCarryAbsent:  "gx",
	} {
		observed.observed = nil
		due := shadowDue
		due.StateGeneration, due.StateCarry = "g1", &execution.StateCarry{From: from}
		due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
		if _, _, err := coordinator.loadSegments(context.Background(), shadowRequest(execution.OperationNormal, 1440),
			execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{due}}); err != nil {
			t.Fatal(err)
		}
		counted := map[string]int64{}
		for _, observation := range observed.observed {
			if observation.SegmentShadow != nil {
				for result, count := range observation.SegmentShadow.Carries {
					counted[result] += count
				}
			}
		}
		if !reflect.DeepEqual(counted, map[string]int64{want: 1}) {
			t.Fatalf("carrying from %s: the Slot's facts counted %v, want %s once", from, counted, want)
		}
	}
}
