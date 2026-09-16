// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A refusal about size says how big the record was and what it was measured
// against, and which of the two records it measured.
//
// Without the numbers the refusal is not actionable: a record a little over
// the bound is one object that has outgrown a single key, and one many times
// over is something else entirely, and the reason code alone reads the same
// for both. Which record matters for the same reason -- the record already in
// the store being too large is a bound that moved or another build's write,
// and the record this round would write being too large is this Plan having
// grown.
func TestNoDataApplySizeRefusalCarriesBothNumbers(t *testing.T) {
	backend := &casMemoryBackend{values: make(map[string][]byte)}
	store := generationStore(t, backend)
	ctx := context.Background()

	// Groups enough to put the encoded record past the fixture's 4096-byte
	// bound, built the way a real memory is: one entry per group the item has
	// seen, each holding its key and its clocks.
	groups := make([]execution.NoDataGroupMemory, 0, 64)
	for index := 0; index < 64; index++ {
		groups = append(groups, execution.NoDataGroupMemory{
			GroupKey:    fmt.Sprintf("component=flink,data_set_id=%d_clustered,__NO_DATA_DIMENSION__=true", index),
			LastSeen:    1789555680,
			FirstAbsent: 1789555380,
		})
	}
	applied, err := store.ApplyNoData(ctx, execution.NoDataApplyRequest{
		Contract: frozenRef(), Items: []execution.PlanNoDataMutation{noDataMutationV2(t, 0, groups...)},
	})
	if err != nil {
		t.Fatal(err)
	}
	item := applied.Items[0]
	if item.Status != execution.NoDataRejected ||
		item.ReasonCode != execution.ReasonCode(contract.ReasonStateBudgetExceeded) {
		t.Fatalf("apply = %+v, want a deterministic budget refusal", item)
	}
	if item.Size == nil {
		t.Fatal("a size refusal carried no measurement; STATE_BUDGET_EXCEEDED on its own is not actionable")
	}
	if item.Size.Record != execution.NoDataRecordNext {
		t.Fatalf("measured record = %q, want the one this round would write", item.Size.Record)
	}
	if item.Size.Limit != 4096 {
		t.Fatalf("limit = %d, want the store's bound", item.Size.Limit)
	}
	if item.Size.Bytes <= item.Size.Limit {
		t.Fatalf("bytes = %d, limit = %d; a refusal reported a size that fits", item.Size.Bytes, item.Size.Limit)
	}
	// Nothing was written. The refusal is the whole outcome, and a partial
	// write would leave a record whose revision no later mutation expects.
	key, err := PlanNoDataKeyV2("alarmd", noDataIdentityV2())
	if err != nil {
		t.Fatal(err)
	}
	if _, written := backend.values[key]; written {
		t.Fatal("a refused write left a record behind")
	}
}

// The record already in the store is measured before it is decoded, and says
// so.
//
// This is reachable only from outside this build's write path -- nothing it
// writes exceeds the bound -- which is exactly why it is worth telling apart:
// a reader who sees STORED knows the record was not put there by a write this
// build made under this bound, and has somewhere different to look.
func TestNoDataApplyNamesTheStoredRecordWhenItIsTheOneTooLarge(t *testing.T) {
	backend := &casMemoryBackend{values: make(map[string][]byte)}
	store := generationStore(t, backend)
	key, err := PlanNoDataKeyV2("alarmd", noDataIdentityV2())
	if err != nil {
		t.Fatal(err)
	}
	backend.values[key] = make([]byte, 5000)

	applied, err := store.ApplyNoData(context.Background(), execution.NoDataApplyRequest{
		Contract: frozenRef(),
		Items: []execution.PlanNoDataMutation{noDataMutationV2(t, 0,
			execution.NoDataGroupMemory{GroupKey: "a", FirstAbsent: 940})},
	})
	if err != nil {
		t.Fatal(err)
	}
	item := applied.Items[0]
	if item.Size == nil || item.Size.Record != execution.NoDataRecordStored ||
		item.Size.Bytes != 5000 || item.Size.Limit != 4096 {
		t.Fatalf("apply = %+v size = %+v, want the stored record measured at 5000 against 4096", item, item.Size)
	}
}

// Every shape a deterministic refusal takes is in the list the metric label is
// bounded by.
//
// The list is published from execution and read by the metric to pre-create
// its series and to bound its cardinality. A refusal shape the store can
// produce and the list does not hold would be a series nobody pre-created and
// a bound that is not one, so the check has to be against the store's actual
// output rather than against a second copy of the list.
func TestEveryNoDataRefusalShapeIsPublished(t *testing.T) {
	published := make(map[execution.NoDataRefusal]bool, len(execution.NoDataRefusals))
	for _, refusal := range execution.NoDataRefusals {
		published[refusal] = true
	}

	key, err := PlanNoDataKeyV2("alarmd", noDataIdentityV2())
	if err != nil {
		t.Fatal(err)
	}
	oneGroup := execution.NoDataGroupMemory{GroupKey: "a", FirstAbsent: 940}

	for _, test := range []struct {
		name  string
		build func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation)
	}{
		{
			name: "the record this round would write is too large",
			build: func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation) {
				groups := make([]execution.NoDataGroupMemory, 0, 64)
				for index := 0; index < 64; index++ {
					groups = append(groups, execution.NoDataGroupMemory{
						GroupKey: fmt.Sprintf("component=flink,data_set_id=%d,__NO_DATA_DIMENSION__=true", index),
						LastSeen: 1789555680, FirstAbsent: 1789555380,
					})
				}
				return generationStore(t, &casMemoryBackend{values: make(map[string][]byte)}),
					noDataMutationV2(t, 0, groups...)
			},
		},
		{
			name: "the record already stored is too large",
			build: func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation) {
				backend := &casMemoryBackend{values: map[string][]byte{key: make([]byte, 5000)}}
				return generationStore(t, backend), noDataMutationV2(t, 0, oneGroup)
			},
		},
		{
			name: "the mutation does not match its own digest",
			build: func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation) {
				mutation := noDataMutationV2(t, 0, oneGroup)
				mutation.MutationDigest = "not-the-digest"
				return generationStore(t, &casMemoryBackend{values: make(map[string][]byte)}), mutation
			},
		},
		{
			name: "the stored record is in a shape this build cannot read",
			build: func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation) {
				future, err := json.Marshal(noDataEnvelope{
					Schema: executionNoDataSchema, Version: execution.MaxSupportedNoDataMemorySchema + 1,
					Identity: noDataIdentityV2(), MarkerRevision: 9, ApplyVersion: applyVersion(),
					MutationDigest: "digest", ScheduleRevision: "plan-r1", RosterVersion: "HISTORY/1",
				})
				if err != nil {
					t.Fatal(err)
				}
				backend := &casMemoryBackend{values: map[string][]byte{key: future}}
				return generationStore(t, backend), noDataMutationV2(t, 9, oneGroup)
			},
		},
		{
			name: "the backend cannot compare and set",
			build: func(t *testing.T) (*ExecutionStore, execution.PlanNoDataMutation) {
				return capabilityStore(t, &readOnlyBackend{values: map[string][]byte{}}),
					noDataMutationV2(t, 0, oneGroup)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, mutation := test.build(t)
			applied, err := store.ApplyNoData(context.Background(), execution.NoDataApplyRequest{
				Contract: frozenRef(), Items: []execution.PlanNoDataMutation{mutation},
			})
			if err != nil {
				t.Fatal(err)
			}
			item := applied.Items[0]
			if item.Status != execution.NoDataRejected {
				t.Fatalf("apply = %+v, want a deterministic refusal", item)
			}
			shape := execution.NoDataRefusal{Reason: item.ReasonCode}
			if item.Size != nil {
				shape.Record = item.Size.Record
			}
			if !published[shape] {
				t.Fatalf("the store produced refusal %+v, which execution.NoDataRefusals does not hold: "+
					"the metric pre-creates its series and bounds its cardinality from that list", shape)
			}
		})
	}
}
