// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A queried item's Plan object digests as it did before an item could be
// realtime - the same literal the no-data case pins, measured before either
// field - and a realtime item's moves: its metric and conditions are what
// the Slot judges its points by, execution content.
func TestARealtimeItemsPartIsInItsPlansExecutionDigestAndNoOtherPlans(t *testing.T) {
	const beforeTheField = "ad18d34f1af87e356277a6cb3cae7c6e74ff3b838b01fb5e3e9788783c3da06d"
	digest, err := contract.DeriveCanonicalDigestV2("probe", controlplane.QueryGroupPlanObject{PlanID: "1001"})
	if err != nil {
		t.Fatal(err)
	}
	if digest != beforeTheField {
		t.Fatalf("digest = %s, want %s: a queried item's Plan changed its execution content", digest, beforeTheField)
	}
	realtime, err := contract.DeriveCanonicalDigestV2("probe", controlplane.QueryGroupPlanObject{PlanID: "1001",
		Realtime: &contract.RealtimeItemV2{Metric: "cpu_usage"}})
	if err != nil {
		t.Fatal(err)
	}
	if realtime == digest {
		t.Fatal("a realtime item's part left its Plan's execution digest where it was")
	}
}

// A realtime item's part goes through publication whole: built into its
// Query Group's object, of the contract version that carries it, written and
// read back, and assembled into the Plan a Worker compiles, with the
// conditions it was given.
func TestARealtimeItemsPartGoesThroughPublication(t *testing.T) {
	item := contract.RealtimeItemV2{Item: contract.RealtimeItemKey{StrategyID: 1001, ItemID: 1, QueryMD5: "query-1"}, Metric: "cpu_usage", Conditions: [][]contract.RealtimeConditionV2{
		{{Key: "ip", Method: contract.RealtimeConditionEq, Values: []string{"127.0.0.1"}}},
		{{Key: "module", Method: contract.RealtimeConditionInclude, Values: []string{"db-.*", "cache"}}},
	}}
	identity := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "1001"}
	plan := controlplane.FrozenPlan{Identity: identity, Plan: contract.EvaluationPlanV2{PlanID: "1001",
		StrategyRef: contract.StrategyRefV2{TenantID: "tenant", StrategyID: "1001"}, Realtime: &item}}
	queried := controlplane.BuildQueryGroupObject(controlplane.QueryGroup{Identity: "query-group",
		Plans: []controlplane.FrozenPlan{{Identity: identity, Plan: contract.EvaluationPlanV2{PlanID: "1001"}}}})
	if queried.ContractVersion != "alarmd-query-group-object-v1" {
		t.Fatalf("a queried group's object is %s, want v1 as before", queried.ContractVersion)
	}
	built := controlplane.BuildQueryGroupObject(controlplane.QueryGroup{Identity: "query-group", Plans: []controlplane.FrozenPlan{plan}})
	// A reader that predates the part refuses the object by its version
	// rather than running the item as a queried one without its conditions.
	if built.ContractVersion != "alarmd-query-group-object-v5" {
		t.Fatalf("a realtime group's object is %s, want v5", built.ContractVersion)
	}
	encoded, err := json.Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	var object controlplane.QueryGroupObject
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(object.Plans[0].Realtime, &item) {
		t.Fatalf("object carries %+v, want %+v", object.Plans[0].Realtime, item)
	}
	group, err := controlplane.AssembleQueryGroup(object, map[execution.PlanIdentity]controlplane.OutputContextObject{
		identity: controlplane.BuildOutputContext(plan)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(group.Plans[0].Plan.Realtime, &item) {
		t.Fatalf("assembled Plan carries %+v, want %+v", group.Plans[0].Plan.Realtime, item)
	}
}
