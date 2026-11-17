// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An object's latest completed Slot's query phases are kept from its
// completion row and read back; a Slot that failed leaves the last completed
// one's, and an object no Slot completed for has none.
func TestAnObjectKeepsItsLatestCompletedSlotsQueryPhases(t *testing.T) {
	tracker := newTracker(t, &clock{at: now})
	if _, known := tracker.QueryPhases("qg-phases"); known {
		t.Fatal("an object no Slot completed for has query phases")
	}
	completed := slotCompleted("qg-phases", 1, 1, now)
	completed.SlotBudgetUsage.QueryWaitMillis, completed.SlotBudgetUsage.QueryBodyMillis = 3500, 1400
	completed.SlotBudgetUsage.QueryWaitMillisMax, completed.SlotBudgetUsage.QueryBodyMillisMax = 3000, 1200
	tracker.Observe(context.Background(), completed)
	want := QueryPhases{Slot: now.Unix(), WaitMillis: 3500, BodyMillis: 1400, WaitMillisMax: 3000, BodyMillisMax: 1200}
	if got, known := tracker.QueryPhases("qg-phases"); !known || got != want {
		t.Fatalf("query phases = %+v (%v), want %+v", got, known, want)
	}
	failed := slotCompleted("qg-phases", 1, 1, now.Add(time.Minute))
	failed.Err = errors.New("apply failed")
	failed.SlotBudgetUsage.QueryWaitMillis = 99
	tracker.Observe(context.Background(), failed)
	if got, _ := tracker.QueryPhases("qg-phases"); got != want {
		t.Fatalf("after a failed Slot the phases are %+v, want the completed one's %+v", got, want)
	}
}
