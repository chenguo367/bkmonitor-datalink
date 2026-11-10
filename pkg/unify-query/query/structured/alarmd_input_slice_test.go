// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package structured

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// alarmdInputSlicePartition is the conditions alarmd sends for a strategy of
// two OR groups (A=1 or B=2) unsliced and read in three slices of host,
// recorded on alarmd's side (pkg/alarmd/access/uq,
// TestTheSlicePartitionConditionsAreTheRecordedOnes).
var alarmdInputSlicePartition = filepath.Join("..", "..", "..", "alarmd", "access", "uq", "testdata", "input_slice_partition.json")

// matchesByLocalFilter is whether a series of labels is read by conditions
// under this service's own filter: the conditions as ToProm prepares them,
// split into OR groups, each group's conditions ANDed.
func matchesByLocalFilter(t *testing.T, conditions Conditions, series map[string]string) bool {
	t.Helper()
	groups := preparedGroups(t, conditions)
	matched, err := groups.MatchResultTableLabels(series)
	if err != nil {
		t.Fatal(err)
	}
	return matched
}

// matchesByPrometheusMatchers is the same read through the label matchers
// each group renders to, the form VictoriaMetrics evaluates: a missing label
// is the empty value.
func matchesByPrometheusMatchers(t *testing.T, conditions Conditions, series map[string]string) bool {
	t.Helper()
	for _, group := range preparedGroups(t, conditions) {
		all := true
		for _, field := range group {
			matcher, err := labels.NewMatcher(field.ToPromOperator(), field.DimensionName, field.Value[0])
			if err != nil {
				t.Fatal(err)
			}
			if !matcher.Matches(series[field.DimensionName]) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func preparedGroups(t *testing.T, conditions Conditions) AllConditions {
	t.Helper()
	prepared := Conditions{ConditionList: conditions.ConditionList}
	for _, field := range conditions.FieldList {
		field.Value = append([]string(nil), field.Value...)
		prepared.FieldList = append(prepared.FieldList, *field.ContainsToPromReg())
	}
	groups, err := prepared.AnalysisConditions()
	if err != nil {
		t.Fatal(err)
	}
	return groups
}

// The slices alarmd reads a Query Group in partition it under this
// service's own reading of the conditions: every series the unsliced query
// reads is read by exactly one slice, and a series it does not read by
// none, both under the local filter and under the Prometheus matchers. The
// series include an empty and a missing host, a host with a newline and
// multi-byte ones, and series each OR group reads alone.
func TestAlarmdInputSlicesPartitionTheUnslicedSeries(t *testing.T) {
	raw, err := os.ReadFile(alarmdInputSlicePartition)
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Unsliced Conditions   `json:"unsliced"`
		Slices   []Conditions `json:"slices"`
	}
	if err = json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded.Slices) < 2 {
		t.Fatalf("setup: %d slices recorded", len(recorded.Slices))
	}
	hosts := []*string{nil}
	for _, host := range []string{"", "a", "ba", "b", "ab", "x\na", "a\nx", "节点a", "节点", "\xff", "a\xff"} {
		host := host
		hosts = append(hosts, &host)
	}
	read := 0
	for _, groups := range []map[string]string{{"A": "1"}, {"B": "2"}, {"A": "1", "B": "2"}, {"A": "2", "B": "1"}, {}} {
		for _, host := range hosts {
			series := map[string]string{}
			for key, value := range groups {
				series[key] = value
			}
			if host != nil {
				series["host"] = *host
			}
			for name, matches := range map[string]func(*testing.T, Conditions, map[string]string) bool{
				"local": matchesByLocalFilter, "prometheus": matchesByPrometheusMatchers,
			} {
				in := 0
				for _, slice := range recorded.Slices {
					if matches(t, slice, series) {
						in++
					}
				}
				want := 0
				if matches(t, recorded.Unsliced, series) {
					want = 1
					read++
				}
				if in != want {
					t.Errorf("%s: series %q is in %d slices, want %d", name, series, in, want)
				}
			}
		}
	}
	if read == 0 {
		t.Fatal("setup: the unsliced query reads none of the series")
	}
}
