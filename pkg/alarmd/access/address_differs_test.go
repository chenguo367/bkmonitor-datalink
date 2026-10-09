// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// differingAddressFuller stands for CMDB placing the record's host by id at
// another address than the record's own.
type differingAddressFuller struct{}

func (differingAddressFuller) Name() string { return "differing_address" }
func (differingAddressFuller) Fill(_ map[string]json.RawMessage, facts *admission.Facts) {
	facts.ReportedAddressDiffers = true
}

// A series whose host CMDB placed by id at another address than the record's
// is counted once for every Plan that admits it, under whether the query's
// grouping holds bk_target_ip - the grouping every Plan of the query builds
// its alert identity from, and the one Python's rewrite of that dimension
// reaches. A Plan that turns the series away raises no alert for it and is
// not counted.
func TestADifferingAddressIsCountedPerAdmittingPlanByItsGrouping(t *testing.T) {
	count := func(identityFields []string) []bool {
		t.Helper()
		_, frozen := frozenExecution(t)
		requirement := frozen.Requirements[0]
		admitted := requirement.Consumers[0].Consumer.Plan
		another, outside := admitted, admitted
		another.StrategyID, outside.StrategyID = admitted.StrategyID+"-another", admitted.StrategyID+"-outside"
		for _, plan := range []execution.PlanIdentity{another, outside} {
			consumer := requirement.Consumers[0]
			consumer.Consumer.Plan = plan
			requirement.Consumers = append(requirement.Consumers, consumer)
		}
		query := plannedQueryForTest(requirement)
		query.Spec.PlanFacts.Normalization.DatasetContract.IdentityFields = identityFields
		var counted []bool
		adapter := &seriesAdapter{
			consumer:  &admissionConsumer{},
			query:     query,
			attemptNo: 1,
			admission: admission.NewChain([]admission.Fuller{admission.IdentityFuller{}, differingAddressFuller{}},
				[]admission.Filter{admission.TargetScopeFilter{}}),
			addressDiffers: func(grouped bool) { counted = append(counted, grouped) },
			scopes: planScopes{
				admitted: {StrategyID: admitted.StrategyID},
				another:  {StrategyID: another.StrategyID},
				outside:  {StrategyID: outside.StrategyID, TargetScope: hostScope("192.0.2.2|0")},
			},
		}
		err := adapter.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{
			PhysicalQuery: adapter.query.Spec.Digest, CompletionRef: "result", Dataset: hostSeries(t, "192.0.2.1"),
			Delivery: execution.SeriesDelivery{PhysicalQuery: adapter.query.Spec.Digest,
				QueryRevision: adapter.query.Spec.PlanFacts.QueryRevision, Series: 1, Records: 1, Digest: "digest"},
		})
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		return counted
	}
	if got := count([]string{"bk_target_cloud_id", "bk_target_ip"}); !reflect.DeepEqual(got, []bool{true, true}) {
		t.Fatalf("grouped by bk_target_ip: counted %v, want the two admitting Plans under true and the outside one not at all", got)
	}
	if got := count([]string{"bk_agent_id"}); !reflect.DeepEqual(got, []bool{false, false}) {
		t.Fatalf("grouped by bk_agent_id: counted %v, want the two admitting Plans under false", got)
	}
}
