// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// Through the production bundle on a real Redis, a Slot that reads its State
// in several calls - more series than a preflight batch's
// (execution.StatePreflightBatchItems) - walks its Query Group's cache
// entries once: the sweep at its first read, every later read of the Slot
// finding the group swept. A sweep at each read walked a group of n series
// n ÷ a batch's items times a Slot, under the cache's one lock.
func TestASlotReadInBatchesWalksItsQueryGroupsCacheOnce(t *testing.T) {
	hosts := execution.StatePreflightBatchItems + 44
	targets := make([]any, 0, hosts)
	series := make([]roundsSeries, 0, hosts)
	for index := range hosts {
		ip := fmt.Sprintf("10.0.%d.%d", index/250, index%250+1)
		targets = append(targets, map[string]any{"bk_target_ip": ip, "bk_target_cloud_id": 0})
		series = append(series, ipSeries(ip))
	}
	fixture := startRoundsFixture(t, roundsOptions{item: func(item map[string]any) {
		item["target"] = []any{[]any{map[string]any{"field": "bk_target_ip", "method": "eq", "value": targets}}}
	}})
	fixture.report(series...)
	sweeps := func(result string) float64 {
		t.Helper()
		got, served := servedSample(t, fixture.recorder, "alarmd_state_cache_sweeps_total", map[string]string{"result": result})
		if !served {
			t.Fatalf("/metrics serves no alarmd_state_cache_sweeps_total{result=%q}", result)
		}
		return got
	}
	// The first round fills the cache; the rounds after read every series,
	// in two calls.
	fixture.run(1)
	for round := int64(2); round <= 4; round++ {
		walked, already := sweeps(state.StateCacheSweepWalked), sweeps(state.StateCacheSweepAlreadySwept)
		fixture.run(round)
		if got := sweeps(state.StateCacheSweepWalked) - walked; got != 1 {
			t.Fatalf("round %d walked its Query Group's cache %v times, want once", round, got)
		}
		if got := sweeps(state.StateCacheSweepAlreadySwept) - already; got < 1 {
			t.Fatalf("round %d's reads after its first found the group swept %v times, want its State read in more than one call", round, got)
		}
	}
}
