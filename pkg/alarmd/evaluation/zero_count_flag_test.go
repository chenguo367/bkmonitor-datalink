// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A zero is no observation for the plan's own primary when it reads event
// counts, or log counts with no grouping; a grouped log count, a time series,
// and another plan's primary leave it an observation.
func TestAZeroCountIsNoObservationOnlyForTheseSources(t *testing.T) {
	plan := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	other := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "8"}
	primary := func(consumer execution.PlanIdentity, events, unfilled bool) execution.DataRequirement {
		return execution.DataRequirement{Role: execution.InputRolePrimary, EventCounts: events, UnfilledCounts: unfilled,
			Consumers: []execution.DataRequirementConsumer{{Consumer: execution.ConsumerRef{Plan: consumer}}}}
	}
	ungrouped, grouped := zeroCountRecord(t, map[string]json.RawMessage{}), zeroCountRecord(t, map[string]json.RawMessage{"host": json.RawMessage(`"host-a"`)})
	for _, test := range []struct {
		name         string
		requirements []execution.DataRequirement
		record       execution.RecordView
		want         bool
	}{
		{"event count", []execution.DataRequirement{primary(plan, true, false)}, grouped, true},
		{"ungrouped log count", []execution.DataRequirement{primary(plan, false, true)}, ungrouped, true},
		{"grouped log count", []execution.DataRequirement{primary(plan, false, true)}, grouped, false},
		{"time series", []execution.DataRequirement{primary(plan, false, false)}, ungrouped, false},
		{"another plan's event count", []execution.DataRequirement{primary(other, true, false), primary(plan, false, false)}, ungrouped, false},
		{"an event count dependency", []execution.DataRequirement{{Role: execution.InputRoleAlgorithmDependency, EventCounts: true,
			Consumers: []execution.DataRequirementConsumer{{Consumer: execution.ConsumerRef{Plan: plan}}}}}, ungrouped, false},
	} {
		if got := zeroCountIsNoObservation(test.requirements, plan, test.record); got != test.want {
			t.Errorf("%s: zeroCountIsNoObservation = %t, want %t", test.name, got, test.want)
		}
	}
}

func zeroCountRecord(t *testing.T, dimensions map[string]json.RawMessage) execution.RecordView {
	t.Helper()
	dataset := execution.NewDataset([]contract.CanonicalRecordV2{{
		RecordID: strings.Repeat("b", 64), SourceTime: 1_788_000_000, BusinessID: "2",
		DimensionIdentity: contract.DimensionIdentityV2{Digest: strings.Repeat("c", 64)},
		Values:            map[string]json.RawMessage{"value": json.RawMessage(`0`)}, Dimensions: dimensions,
		ReceivedTime: 1_788_000_000,
	}})
	view, err := execution.NewDatasetView(dataset, []uint32{0})
	if err != nil {
		t.Fatal(err)
	}
	record, ok := view.Record(0)
	if !ok {
		t.Fatal("no record")
	}
	return record
}
