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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The State cache's first defect of its compact form (014 next9 1) is
// handed on every preflight after it is found, and said once a process: one
// state_cache_diverged line over two Slots, naming the strategy, the kind
// and where the member parted.
func TestTheStateCachesFirstDefectIsSaidOnceAProcess(t *testing.T) {
	fixture := newFixture(t, true, "")
	plan := execution.PlanIdentity{TenantID: "tenant", BusinessID: "business", StrategyID: "strategy-diverged"}
	fixture.ports.stateCacheDiverged = &execution.StateCacheMismatch{Plan: plan, Kind: "compact", Field: "rebuilt_bytes"}
	for round := 0; round < 2; round++ {
		if result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal)); err != nil ||
			!result.Completed {
			t.Fatalf("Execute() %d result=%+v error=%v", round, result, err)
		}
	}
	if fixture.ports.stateLoadCalls < 2 {
		t.Fatalf("fixture: %d preflights over two Slots, want one a Slot at least", fixture.ports.stateLoadCalls)
	}
	var said []observability.Observation
	for _, observation := range *fixture.observations {
		if observation.Component == observability.ComponentState && observation.Stage == observability.StageStateCacheDiverged {
			said = append(said, observation)
		}
	}
	want := observability.StateCacheMismatchFacts{Count: 1, StrategyID: "strategy-diverged", Kind: "compact", Field: "rebuilt_bytes"}
	if len(said) != 1 || said[0].Result != observability.ResultFailed || said[0].StateCacheMismatch == nil ||
		*said[0].StateCacheMismatch != want {
		t.Fatalf("said %+v; want one failed state_cache_diverged line carrying %+v", said, want)
	}
}
