// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A read hold is how much later than its read a group must read for its data
// to have landed. The read is the group's own moment, settling wait plus its
// phase, so the hold is measured from there: measured from the settling wait
// alone it would hold every staggered group its phase too long.
func TestTheReadHoldIsMeasuredFromTheGroupsOwnReadMoment(t *testing.T) {
	ctx := context.Background()
	f, _, _, _ := delayEdited(t)
	schedule := f.initialSchedule
	spec, err := f.bundle.dependencies.ReadHolds.spec(ctx, schedule)
	if err != nil {
		t.Fatal(err)
	}
	cfg := f.bundle.dependencies.Config.PhaseTwo.Access
	var want time.Duration
	for i, plan := range schedule.Plans {
		budget := time.Duration(plan.Spec.CompletionOffsetSeconds())*time.Second - cfg.DownstreamExecutionReserve.Duration()
		wait := execution.ReadWaitWithinQueryBudget(schedule.Segment.QueryGroup, budget, cfg.MinReadyDelay.Duration())
		if i == 0 || wait < want {
			want = wait
		}
	}
	if spec.SettlingWait != want {
		t.Fatalf("hold measured from %v after the window, the group reads at %v", spec.SettlingWait, want)
	}
}
