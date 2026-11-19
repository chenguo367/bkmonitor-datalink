// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"reflect"
	"testing"
)

// Whose a Slot is: each strategy once however many Plans it was split into,
// the few a line names sorted, and the counts over every Plan. Eight Plans of
// seven strategies over two businesses, frozen out of order: the line names
// 11 to 15 and counts seven and two. No Plans, no scope.
func TestThePlanScopeNamesEachStrategyOnceSortedAndCountsThemAll(t *testing.T) {
	plan := func(business, strategy string, shard int) PlanKey {
		return PlanKey{PlanIdentity: PlanIdentity{TenantID: "system", BusinessID: business, StrategyID: strategy}, ShardIndex: shard}
	}
	targets := FrozenDuePlanTargets{Plans: []PlanKey{
		plan("3", "17", 0), plan("2", "12", 0), plan("2", "11", 1), plan("3", "16", 0),
		plan("2", "11", 0), plan("3", "14", 0), plan("2", "15", 0), plan("3", "13", 0),
	}}
	got := targets.PlanScope()
	if got == nil || !reflect.DeepEqual(got.StrategyIDs, []string{"11", "12", "13", "14", "15"}) || got.StrategyCount != 7 ||
		!reflect.DeepEqual(got.BusinessIDs, []string{"2", "3"}) || got.BusinessCount != 2 {
		t.Fatalf("PlanScope() = %+v, want strategies 11-15 of 7 and businesses 2, 3 of 2", got)
	}
	targets.Plans[0], targets.Plans[7] = targets.Plans[7], targets.Plans[0]
	if again := targets.PlanScope(); !reflect.DeepEqual(again, got) {
		t.Fatalf("the same Plans in another order are named %+v, want %+v", again, got)
	}
	if scope := (FrozenDuePlanTargets{}).PlanScope(); scope != nil {
		t.Fatalf("no Plans scoped as %+v", scope)
	}
}
