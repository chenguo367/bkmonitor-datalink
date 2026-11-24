// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "testing"

// A replay's distance and age go with the replay operation alone, the age
// never negative, and a replay is short of time at distance one and age zero
// only (014 next9 2).
func TestAReplaysDistanceAndAgeGoWithTheReplayAndSayWhetherItIsShortOfTime(t *testing.T) {
	base := func(operation Operation) SlotExecutionRequest {
		return SlotExecutionRequest{Contract: FrozenExecutionContractRef{
			Slot: SlotIdentity{QueryGroup: "query-group", EvaluationTime: 100}, SnapshotRevision: "snapshot", QueryRevision: "query",
			ScheduleRevision: "schedule", ScheduleSegmentStart: 60, DuePlanSetDigest: "plans"},
			DuePlanTargets: FrozenDuePlanTargets{DuePlanSetDigest: "plans", Plans: []PlanKey{{PlanIdentity: PlanIdentity{
				TenantID: "tenant", BusinessID: "2", StrategyID: "7"}}}},
			EarliestQueryDeadlineUnixMilli: 101_000, RecoveryUntilUnixMilli: 701_000, KeepUntilUnixMilli: 777_000,
			Operation: operation, AttemptNo: 1, ExpectedNextSlot: 100,
			OwnerFence: OwnerFence{QueryGroup: "query-group", OwnerID: "worker-1", OwnerEpoch: 1, LeaseToken: "token-1"}}
	}
	for _, c := range []struct {
		name      string
		operation Operation
		distance  uint32
		age       int64
		valid     bool
		short     bool
	}{
		{"a replay short of time", OperationReplay, 1, 0, true, true},
		{"a replay behind", OperationReplay, 2, 0, true, false},
		{"a replay past its deadline", OperationReplay, 1, 3_000, true, false},
		{"a replay of no facts", OperationReplay, 0, 0, true, false},
		{"a normal round with a distance", OperationNormal, 1, 0, false, false},
		{"a retry with an age", OperationRetry, 0, 1_000, false, false},
		{"a replay of a negative age", OperationReplay, 1, -1, false, false},
	} {
		request := base(c.operation)
		request.ReplayDistance, request.ReplayAgeMillis = c.distance, c.age
		if err := request.Validate(); (err == nil) != c.valid {
			t.Fatalf("%s: Validate() = %v, want valid %t", c.name, err, c.valid)
		}
		if got := request.ReplayShortOfTime(); got != c.short {
			t.Fatalf("%s: short of time %t, want %t", c.name, got, c.short)
		}
	}
}
