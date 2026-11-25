// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package progress

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A BeginSlot is put off to the Slot's first write (014 ⑩ F5) only when it
// would write the Slot's unfinished projection onto a record already at it,
// judged from the owner's record and with no read of the key: the record
// kept under the BeginSlot's fence, at its Slot, with no unfinished Slot and
// no unfinished range. Each of those not so, it is made at the start.
func TestABeginSlotIsPutOffOnlyOntoAKeptRecordAtItsSlotWithNothingUnfinished(t *testing.T) {
	const slot execution.EvaluationTime = 60
	fence := ownerFence(1)
	begin := execution.ProgressBeginRequest{Identity: ownerIdentity, OwnerFence: fence, Projection: progressProjectionAt(slot)}
	kept := func(t *testing.T, current execution.ScheduleProgress) ownerFixture {
		t.Helper()
		fixture := newOwnerFixture(t, current, fixedSlotResolver{interval: 60})
		// The slot source's Next read, under the owner's fence: the record
		// the owner keeps.
		if _, err := fixture.store.LoadProgressFor(context.Background(), ownerIdentity, fence); err != nil {
			t.Fatal(err)
		}
		return fixture
	}

	fixture := kept(t, cursorAt(slot))
	reads := fixture.control.readCount()
	if !fixture.store.BeginDeferrable(begin) {
		t.Fatal("a BeginSlot onto a kept record at its Slot with nothing unfinished was not put off")
	}
	if fixture.control.readCount() != reads {
		t.Fatal("deciding to put a BeginSlot off read the key")
	}

	unfinishedSlot := cursorAt(slot)
	projection := progressProjectionAt(slot)
	unfinishedSlot.UnfinishedSlot = &projection
	pending := rangeFixture(t)
	unfinishedRange := execution.ScheduleProgress{Identity: ownerIdentity, NextSlot: 60, UnfinishedRange: &pending}
	otherFence := begin
	otherFence.OwnerFence = ownerFence(2)
	for _, test := range []struct {
		name    string
		current execution.ScheduleProgress
		request execution.ProgressBeginRequest
	}{
		{name: "the record at another Slot", current: cursorAt(slot + 60), request: begin},
		{name: "an earlier attempt's unfinished Slot", current: unfinishedSlot, request: begin},
		{name: "an unfinished range", current: unfinishedRange, request: begin},
		{name: "the record kept under another fence", current: cursorAt(slot), request: otherFence},
	} {
		t.Run(test.name, func(t *testing.T) {
			if kept(t, test.current).store.BeginDeferrable(test.request) {
				t.Fatalf("a BeginSlot with %s was put off", test.name)
			}
		})
	}
	t.Run("no record kept", func(t *testing.T) {
		fixture := newOwnerFixture(t, cursorAt(slot), fixedSlotResolver{interval: 60})
		if fixture.store.BeginDeferrable(begin) {
			t.Fatal("a BeginSlot with no record kept was put off")
		}
	})
	t.Run("no record at all", func(t *testing.T) {
		fixture := newOwnerFixture(t, cursorAt(slot), fixedSlotResolver{interval: 60})
		fixture.control.missing = true
		if _, err := fixture.store.LoadProgressFor(context.Background(), ownerIdentity, fence); err != nil {
			t.Fatal(err)
		}
		if fixture.store.BeginDeferrable(begin) {
			t.Fatal("a BeginSlot onto no record was put off")
		}
	})
}
