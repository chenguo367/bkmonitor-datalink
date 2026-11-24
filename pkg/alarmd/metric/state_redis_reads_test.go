// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// The State store's reads are absent until a store is bound, then every
// purpose's cell is served under command mget, with the store's own counts.
func TestStateRedisReadsReportTheBoundStoreByPurpose(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	calls, keys := "alarmd_state_redis_reads_total", "alarmd_state_redis_read_keys_total"
	if got := gatherFamily(t, r, calls); got != nil {
		t.Fatalf("%s before any source: %v, want absent", calls, got)
	}
	reading := state.StateReadStats{Calls: map[string]uint64{state.StateReadGapLoad: 4}, Keys: map[string]uint64{state.StateReadGapLoad: 4,
		state.StateReadRuntime: 9}}
	r.SetStateRedisReadsSource(func() state.StateReadStats { return reading })
	for _, family := range []struct {
		name string
		want map[string]float64
	}{
		{calls, map[string]float64{state.StateReadGapLoad: 4}},
		{keys, map[string]float64{state.StateReadGapLoad: 4, state.StateReadRuntime: 9}},
	} {
		got := gatherFamily(t, r, family.name)
		if len(got) != len(state.StateReadPurposes) {
			t.Fatalf("%s: %d cells, want one a purpose, %d", family.name, len(got), len(state.StateReadPurposes))
		}
		for _, sample := range got {
			labels := map[string]string{}
			for _, pair := range sample.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["command"] != "mget" || sample.GetCounter().GetValue() != family.want[labels["purpose"]] {
				t.Fatalf("%s{%v} = %v, want %v", family.name, labels, sample.GetCounter().GetValue(), family.want[labels["purpose"]])
			}
		}
	}
}
