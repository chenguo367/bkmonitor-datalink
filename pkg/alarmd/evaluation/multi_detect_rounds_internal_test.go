// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// roundsBinding is a named input of the host-a series holding a point at
// each of times.
func roundsBinding(t *testing.T, name string, role execution.InputRole, times ...int64) (execution.NamedInputBinding, string) {
	t.Helper()
	fields := []contract.DimensionFieldV2{{Name: "host", Value: json.RawMessage(`"host-a"`)}}
	digest, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", fields)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]contract.CanonicalRecordV2, 0, len(times))
	for _, at := range times {
		id, err := contract.DeriveRecordIDV2(digest, at)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, contract.CanonicalRecordV2{RecordID: id, SourceTime: at, BusinessID: "2", ReceivedTime: at,
			DimensionIdentity: contract.DimensionIdentityV2{Fields: fields, Digest: digest},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(`1`)}, Dimensions: map[string]json.RawMessage{}})
	}
	dataset := execution.NewDataset(records)
	ordinals := make([]uint32, dataset.Len())
	for index := range ordinals {
		ordinals[index] = uint32(index)
	}
	view, err := execution.NewDatasetView(dataset, ordinals)
	if err != nil {
		t.Fatal(err)
	}
	return execution.NamedInputBinding{RequirementID: execution.RequirementID(name), DatasetName: execution.DatasetName(name), Role: role,
		Dataset: dataset, View: view, Completeness: execution.CompletenessFull, Disposition: execution.AccessAvailable}, digest
}

// A series of an item detected over several is judged at every time any of
// its inputs has a point of it - the PRIMARY's record where it has one, the
// other input's where it has none - so that a round an input is missing
// from is judged undecided, not left a hole; a series of one query is
// judged at its own records alone, as before.
func TestAMultiDetectSeriesIsJudgedWhereverAnInputHasAPoint(t *testing.T) {
	primary, digest := roundsBinding(t, "primary", execution.InputRolePrimary, 540, 600)
	memory, _ := roundsBinding(t, "input:memory", execution.InputRoleMultiDetectInput, 480, 600, 660)
	input := execution.SeriesEvaluationInputRequest{SeriesIdentity: execution.SeriesIdentityDigest(digest),
		Inputs: []execution.NamedInputBinding{primary, memory}}
	records, err := commonPrimaryRecords([]execution.SeriesEvaluationInputRequest{input})
	if err != nil {
		t.Fatal(err)
	}
	var times []int64
	for _, record := range records {
		times = append(times, record.SourceTime())
		want, _ := contract.DeriveRecordIDV2(digest, record.SourceTime())
		if record.RecordID() != want {
			t.Fatalf("the round at %d is record %s, want the series' own %s", record.SourceTime(), record.RecordID(), want)
		}
	}
	if len(times) != 4 || times[0] != 480 || times[1] != 540 || times[2] != 600 || times[3] != 660 {
		t.Fatalf("rounds at %v, want every time either input has a point: 480 540 600 660", times)
	}

	// The PRIMARY holds none of the series: it is judged at the other
	// input's points, each round to be undecided for the missing CPU.
	empty, _ := roundsBinding(t, "primary", execution.InputRolePrimary)
	input.Inputs = []execution.NamedInputBinding{empty, memory}
	records, err = commonPrimaryRecords([]execution.SeriesEvaluationInputRequest{input})
	if err != nil || len(records) != 3 {
		t.Fatalf("records %d err %v, want the memory input's three rounds", len(records), err)
	}

	// A series of one query: its own records, nothing added.
	dependency, _ := roundsBinding(t, "previous", execution.InputRoleAlgorithmDependency, 480)
	input.Inputs = []execution.NamedInputBinding{primary, dependency}
	records, err = commonPrimaryRecords([]execution.SeriesEvaluationInputRequest{input})
	if err != nil || len(records) != 2 {
		t.Fatalf("records %d err %v, want the PRIMARY's own two", len(records), err)
	}
}
