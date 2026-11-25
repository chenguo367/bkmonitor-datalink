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
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// A record only another input of an item detected over several holds anchors
// that round's State, and never an event: the round is undecided - its
// PRIMARY lacks the point - so an event on it is one the result contract
// refuses. The PRIMARY's own record anchors both.
func TestARoundAnotherInputAnchorsWritesStateAndSendsNoEvent(t *testing.T) {
	plan := PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	consumer := ConsumerRef{Plan: plan, LevelID: 1, HasLevel: true}
	fields := []contract.DimensionFieldV2{{Name: "host", Value: json.RawMessage(`"host-a"`)}}
	digest, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", fields)
	if err != nil {
		t.Fatal(err)
	}
	record := func(at int64) contract.CanonicalRecordV2 {
		id, err := contract.DeriveRecordIDV2(digest, at)
		if err != nil {
			t.Fatal(err)
		}
		return contract.CanonicalRecordV2{RecordID: id, SourceTime: at, BusinessID: "2", ReceivedTime: at,
			DimensionIdentity: contract.DimensionIdentityV2{Fields: fields, Digest: digest},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(`1`)}, Dimensions: map[string]json.RawMessage{}}
	}
	binding := func(role InputRole, records ...contract.CanonicalRecordV2) NamedInputBinding {
		dataset := NewDataset(records)
		ordinals := make([]uint32, dataset.Len())
		for index := range ordinals {
			ordinals[index] = uint32(index)
		}
		view, err := NewDatasetView(dataset, ordinals)
		if err != nil {
			t.Fatal(err)
		}
		return NamedInputBinding{Consumer: consumer, Role: role, Dataset: dataset, View: view, Completeness: CompletenessFull}
	}
	input := InternalExecution{Inputs: []NamedInputBinding{
		binding(InputRolePrimary, record(600)), binding(InputRoleMultiDetectInput, record(600), record(660)),
	}}
	series := SeriesIdentityDigest(digest)
	for _, test := range []struct {
		at           int64
		state, event bool
	}{{at: 600, state: true, event: true}, {at: 660, state: true, event: false}} {
		anchor := record(test.at)
		if got := primaryStateAnchorExists(input, plan, series, RecordAnchor{RecordID: anchor.RecordID, SourceTime: anchor.SourceTime}); got != test.state {
			t.Errorf("the record at %d anchors State: %t, want %t", test.at, got, test.state)
		}
		event := &contract.TriggerEventV1{RecordRef: contract.TriggerRecordRefV1{RecordID: anchor.RecordID, SourceTime: anchor.SourceTime, DimensionIdentityDigest: digest}}
		if got := primaryDatasetContainsEventRecord(input, plan, event); got != test.event {
			t.Errorf("the record at %d anchors an event: %t, want %t", test.at, got, test.event)
		}
	}
}
