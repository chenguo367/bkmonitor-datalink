// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Query Group kept out whose rounds write every value back - no member
// left unwritten, so none read back to measure - is priced by its values
// written compact (014 next9 1): a value written compact is measured as one
// read back is, at what the cache would hold of it. A hundred and twenty
// points a value, normal and anomalous by turns, each past the compact
// estimate a quarter over (660 bytes), and kept out from its first write,
// the group comes in whole at the round after the first resize. Priced at
// the estimate, the budget never reached its round and it stayed out.
func TestAGroupKeptOutWhoseRoundsWriteEveryValueIsPricedByItsCompactWrites(t *testing.T) {
	// A floor under one value: the round is kept out from its first write,
	// nothing of it held to be measured as it leaves.
	const floor, series, points = 512, 20, 120
	const estimateAQuarterOver = 528 * 5 / 4
	rig := newCacheRig(t, floor)
	writeRound := func(s slot, revision uint64) {
		t.Helper()
		retention := []execution.StateRetentionRequirement{{LevelID: 1, RetentionPoints: points, EvaluationInterval: 10 * time.Second}}
		mutations := make([]execution.StateMutation, series)
		items := make([]execution.StatePreflightItem, series)
		for index := range mutations {
			mutations[index] = manyPointMutation(t, seriesIdentity(index), s.version(), revision, points)
			items[index] = execution.StatePreflightItem{Identity: mutations[index].Identity, ApplyVersion: s.version(), Sharded: s.sharded}
		}
		ctx := context.Background()
		if _, err := rig.store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence}); err != nil {
			t.Fatal(err)
		}
		fence := testApplyFence()
		fence.Fence = s.fence
		result, err := rig.store.ApplyRuntimeFenced(ctx, execution.StateApplyRequest{Contract: s.contract(), Retention: retention, Items: mutations}, fence)
		if err != nil {
			t.Fatal(err)
		}
		requireAllStatus(t, result, execution.StateApplied)
	}
	// Ten-second rounds, late enough for a hundred and twenty points before
	// them.
	early, late := slot{at: 7200, epoch: 1, segmentStart: 60, fence: fenceOf(3)}, slot{at: 7210, epoch: 1, segmentStart: 60, fence: fenceOf(3)}
	writeRound(early, 0)
	// Kept out from its first write: the one departure is that write's
	// refusal, no value held and let go after it.
	if stats := rig.store.StateCacheStats(); stats.QueryGroups != 0 || stats.StateDepartures[StateCacheDepartOverBudget] != 1 {
		t.Fatalf("fixture: a round of %d values written into a cache of %d bytes: %d groups, departures %v; want it kept out from "+
			"its first write, nothing held", series, floor, stats.QueryGroups, stats.StateDepartures)
	}
	rig.store.SizeStateCache(0)
	rig.now = rig.now.Add(10 * time.Second)
	writeRound(late, 1)
	stats := rig.store.StateCacheStats()
	if stats.QueryGroups != 1 || stats.Entries[StateCacheEntryCompact] != series {
		t.Fatalf("the round after the resize: %d groups, %v entries (budget %d); want the group in whole, compact", stats.QueryGroups,
			stats.Entries, stats.BudgetBytes)
	}
	if charge := (stats.Bytes - stats.SegmentBytes) / series; charge <= estimateAQuarterOver {
		t.Fatalf("fixture: a value of %d points held compact at %d bytes, not past the estimate a quarter over, %d", points, charge,
			estimateAQuarterOver)
	}
}

// manyPointMutation is seriesMutation's value with points points, ten
// seconds apart up to its version's evaluation time, normal and anomalous by
// turns.
func manyPointMutation(t *testing.T, identity execution.StateKeyIdentity, version execution.ApplyVersion, revision uint64,
	points int) execution.StateMutation {
	t.Helper()
	at := int64(version.EvaluationTime)
	history := make([]execution.StateHistoryPoint, points)
	for index := range history {
		result := execution.LevelFactNormal
		if index%2 == 1 {
			result = execution.LevelFactAnomalous
		}
		history[index] = derivedPoint(t, identity, at-int64(points-1-index)*10, "detect", result)
	}
	mutation, err := execution.BuildStateMutation(execution.StateMutation{Identity: identity, ExpectedBlobRevision: revision, ApplyVersion: version,
		AffectedRecords: []execution.RecordAnchor{derivedAnchor(t, identity, at)},
		Levels: []execution.RuntimeLevelStateMutation{{LevelID: 1, LevelStateCompatibility: "compat", HistoryCompleteness: execution.HistoryFull,
			WarmupRequirementRef: "warm", LastProcessedEventTime: at}},
		Points: history})
	if err != nil {
		t.Fatal(err)
	}
	return mutation
}
