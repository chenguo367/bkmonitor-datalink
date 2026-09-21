// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// Three objects on one replica, each inside its own share, together past the
// pool: the reading is the sum of each object's peak -- not its mean, which
// on a bimodal object is the wrong statistic -- against the pool the
// completion rows carry, with the window's pool refusals beside it, counted
// whether or not the refused object is one this summary tracks. Before any
// row has carried the pool's size the share is not a number.
func TestTheReplicasRetainedReadingIsTheSumOfPeaksAgainstThePoolWithItsRefusals(t *testing.T) {
	now := time.Unix(600, 0)
	c := NewCostSummary(CostSummaryOptions{ProcessID: "process-a", Window: time.Minute, GroupCapacity: 4, PlanCapacity: 8, MetadataBytes: 4096, TopN: 3, Now: func() time.Time { return now }})
	group := func(key, strategy string) CostGroup {
		return CostGroup{QueryGroupKey: key, QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", Members: []CostPlanIdentity{{StrategyID: strategy, BusinessID: "2"}}}
	}
	c.Reconcile([]CostGroup{group("bimodal", "4101"), group("steady", "4102"), group("quiet", "4103")}, true)
	ctx := context.Background()
	const mib = 1 << 20
	completed := func(key string, retained, limit uint64) {
		c.Observe(ctx, Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultSuccess, Duration: time.Second,
			Trace:           TraceFields{QueryGroupKey: key, EvaluationTime: 590},
			SlotBudgetUsage: &SlotBudgetUsageFacts{RetainedBytes: retained, RetainedBytesLimit: limit}})
	}
	refused := func(key, reason string, budget CapacityBudget) {
		c.Observe(ctx, Observation{Component: ComponentResource, Stage: StageResourceHard, Result: ResultPaused,
			ReasonCode: ReasonCode(reason), CapacityBudget: budget, Err: errors.New("budget"),
			Trace: TraceFields{QueryGroupKey: key, EvaluationTime: 590}})
	}

	// No row has carried the pool yet: a sum, and no share.
	completed("steady", 300*mib, 0)
	c.Publish(now)
	if r := c.Snapshot().Retained; r.PeakSumBytes != 300*mib || r.LimitKnown || r.PeakShare != 0 || r.GroupsWithPeak != 1 {
		t.Fatalf("before any row carried the pool: %+v, want the one peak, no limit and no share", r)
	}

	completed("bimodal", 175*mib, 1<<30)
	completed("bimodal", 404*mib, 1<<30)
	completed("steady", 300*mib, 1<<30)
	completed("quiet", 0, 1<<30)
	for i := 0; i < 13; i++ {
		// Pool refusals, some of them on an object this summary does not track.
		key := "bimodal"
		if i%3 == 0 {
			key = "stranger"
		}
		refused(key, contract.ReasonResourceHardStop, CapacityBudgetRetainedBytes)
	}
	refused("bimodal", contract.ReasonQGBudgetShareExceeded, CapacityBudgetRetainedBytes)
	// Neither of these is a refusal of this pool.
	refused("bimodal", contract.ReasonSlotBudgetExceeded, CapacityBudgetRetainedBytes)
	refused("bimodal", contract.ReasonResourceHardStop, CapacityBudgetSeries)
	c.Publish(now)
	s := c.Snapshot()
	r := s.Retained
	if r.PeakSumBytes != 704*mib || r.GroupsWithPeak != 2 {
		t.Fatalf("peak sum = %d over %d objects, want 704 MiB over 2: the bimodal object's 404, not its 289.5 mean, and nothing for the quiet one", r.PeakSumBytes, r.GroupsWithPeak)
	}
	if !r.LimitKnown || r.LimitBytes != 1<<30 || r.PeakShare != float64(704*mib)/float64(1<<30) {
		t.Fatalf("limit/share = %+v, want the pool the rows carried and 704/1024", r)
	}
	if r.HardStops != 13 || r.ShareStops != 1 {
		t.Fatalf("refusals = %d pool / %d share, want 13 / 1: the per-Slot cap and another budget's refusal are not this pool's", r.HardStops, r.ShareStops)
	}
	// The same numbers on the object rows, and the ranking that says which
	// objects the sum is made of, largest peak first.
	rows := map[string]CostContributor{}
	for _, row := range s.Contributors {
		if row.Scope == "query_group" {
			rows[row.Group.QueryGroupKey] = row
		}
	}
	if rows["bimodal"].Current.RetainedBytesPeak != 404*mib || rows["bimodal"].Current.RetainedHardStops != 8 || rows["bimodal"].Current.RetainedShareStops != 1 {
		t.Fatalf("bimodal row = %+v, want peak 404 MiB, 8 pool refusals of its own (5 went to the stranger), 1 share refusal", rows["bimodal"].Current)
	}
	for _, ranking := range s.Rankings {
		if ranking.Scope != "query_group" || ranking.Dimension != "retained_bytes_peak" {
			continue
		}
		if len(ranking.Indexes) != 2 || s.Contributors[ranking.Indexes[0]].Group.QueryGroupKey != "bimodal" || s.Contributors[ranking.Indexes[1]].Group.QueryGroupKey != "steady" {
			t.Fatalf("retained_bytes_peak ranking = %v, want bimodal then steady and no row for the object with no peak", ranking.Indexes)
		}
		return
	}
	t.Fatal("no retained_bytes_peak ranking")
}
