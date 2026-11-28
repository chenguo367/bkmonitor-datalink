// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot read in slices whose slice read last takes it past its budget - its
// hosts over the second Plan's threshold at their five points, five events in
// one merge, a Slot of three at most - is replaced there: the Plans the
// evaluation had not got past answered with a gap. One stream finishing on
// that slice evaluates nothing more: not the no-data Plan's round, whose
// judgement says itself on a line of its own. Two consumers finishing on the
// slice numbered last do not either.
//
// Which Plans two consumers replace there is not one stream's: the stop that
// takes the Slot's results leaves the consumer of a slice below mid-way, and
// the Plan it was evaluating is not got past (015 C1 invariant 6, the
// exception). What holds whatever the timing: every series of every Plan is
// written or under the gap its Plan opens over the Slot.
func TestTwoConsumersPastTheBudgetInTheirLastSliceFinishAsOneDoes(t *testing.T) {
	at := frozenContract().Slot.EvaluationTime + 60
	specs := []steppedPlan{
		{threshold: "1000", noData: &contract.NoDataConfigV1{Continuous: 1, Level: 2, AggDimension: []string{"host"}}},
		{threshold: "100"},
	}
	// Hosts whose last characters spread over the cut's classes, and some of
	// the slice the cut puts last, which the Slot is read last in: one stream
	// finishes on that slice, two consumers on it as the slice numbered last.
	tree, err := inputslice.NewCut([]string{"host"}, 12, inputslice.SuffixCut("host"))
	if err != nil {
		t.Fatal(err)
	}
	leaves := tree.Pending()
	last := leaves[len(leaves)-1].Slice
	var hosts, lastHosts []string
	for _, series := range spreadHosts(120, 0) {
		hosts = append(hosts, series.host)
	}
	// The cut's last slice may hold only values as long as its depth (its
	// exhausted child): names of one, two and more characters are tried.
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	var names []string
	for _, first := range alphabet {
		names = append(names, string(first))
		for _, second := range alphabet {
			names = append(names, string(first)+string(second))
		}
	}
	for index := 0; index < len(alphabet)*len(alphabet); index++ {
		names = append(names, fmt.Sprintf("z%02d-%c%c", index, alphabet[index/len(alphabet)], alphabet[index%len(alphabet)]))
	}
	for _, host := range names {
		if in, err := last.Matches(map[string]string{"host": host}); err == nil && in && len(lastHosts) < 4 {
			lastHosts = append(lastHosts, host)
		}
	}
	if len(lastHosts) == 0 {
		t.Fatalf("fixture: no host name falls in the slice the cut puts last, %+v", last.Conditions)
	}
	hosts = append(hosts, lastHosts...)
	run := func(byCores bool, value map[string]string) (steppedSlot, []execution.PlanIdentity, int) {
		if byCores {
			worker.SetSliceConsumersByCoresForTest(t, 4)
		} else {
			worker.SetSliceConsumersForTest(t, 1)
		}
		// Hosts small enough that no slice is split past its limit, which two
		// consumers' halves of the share would otherwise do: a child of a split
		// is numbered after every slice, and the Slot finishes on it.
		rig, plans := steppedRigOf(t, specs, 120, 1<<10, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = 3 })
		// Started in the slices its last read left, each within its limit:
		// read in their order, the slice numbered last read last. Hosts
		// whose last characters spread over the cut's classes.
		rig.provider.hosts = append([]string(nil), hosts...)
		worker.RememberSliceShapeForTest(rig.coordinator, rig.base.Contract.Slot.QueryGroup, rig.base.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 120)
		var mu sync.Mutex
		judged := 0
		rig.observed.next = observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			if observation.Stage == observability.StageNoDataDecided && observation.NoDataAbsence != nil {
				mu.Lock()
				judged++
				mu.Unlock()
			}
		})
		slot := runSteppedSlot(rig, at, value)
		if slot.err != nil {
			t.Fatalf("by cores %v: %v", byCores, slot.err)
		}
		mu.Lock()
		defer mu.Unlock()
		return slot, plans, judged
	}
	steady, _, judged := run(false, nil)
	if judged != 1 {
		t.Fatalf("fixture: a steady Slot judged the no-data Plan's round %d times, want once", judged)
	}
	slices := slicesOf(steady)
	if len(slices) < 3 || len(steady.reads) != len(leaves) || steady.reads[len(steady.reads)-1].slice != last.Digest() {
		t.Fatalf("fixture: the Slot read %d slices, %d holding series, the last not the cut's last; want the %d it starts in, "+
			"three holding some at least", len(steady.reads), len(slices), len(leaves))
	}
	over := map[string]string{}
	for _, host := range lastHosts {
		over[host] = "200"
	}
	one, plans, judged := run(false, over)
	if !replacedAt(one, plans[1]) || replacedAt(one, plans[0]) || !one.result.Completed || judged != 0 {
		t.Fatalf("fixture: one stream's Slot, its last slice past the budget, answered the Plans %v past it (completed %v) and "+
			"judged the no-data round %d times; want the second answered in its last slice, nothing judged after", one.gaps,
			one.result.Completed, judged)
	}
	var consumed []int
	var barriers uint64
	worker.SetSlicesBySubstreamForTest(t, func(by []int, held uint64) { consumed, barriers = by, held })
	two, _, judged := run(true, over)
	var said []string
	for plan, statements := range two.gaps {
		for _, statement := range statements {
			for _, scope := range statement.Scopes {
				said = append(said, fmt.Sprintf("%s:%s:%s", plan.StrategyID, scope.Kind, scope.ReasonCode))
			}
		}
	}
	t.Logf("two consumers: consumed %v, %d barriers, %d events, gap statements %v, %d no-data rounds judged",
		consumed, barriers, len(two.sent), said, judged)
	if len(consumed) != 2 || consumed[0] == 0 || consumed[1] == 0 || consumed[0]+consumed[1] != len(leaves)-1 {
		t.Fatalf("fixture: two consumers consumed %v slices, want both some and every slice but the first, none split", consumed)
	}
	if !replacedAt(two, plans[1]) || !two.result.Completed {
		t.Fatalf("fixture: two consumers' Slot did not answer the second Plan past the budget (completed %v, gaps %v)",
			two.result.Completed, two.gaps)
	}
	if judged != 0 {
		t.Fatalf("two consumers, their slice numbered last replaced past the budget, judged the no-data round %d times after", judged)
	}
	for _, plan := range plans {
		var unwritten []string
		for _, read := range two.reads {
			for _, host := range read.hosts {
				if _, written := two.written[plan][shadowSeries(t, host)]; !written {
					unwritten = append(unwritten, host)
				}
			}
		}
		if len(unwritten) != 0 && !opened(two.gaps[plan]) {
			t.Fatalf("Plan %s wrote no State for %d series read and opened no gap: they were taken for written",
				plan.StrategyID, len(unwritten))
		}
	}
}

// opened reports whether the statements open a gap on some scope: the
// series they cover are guarded rather than taken for written.
func opened(statements []execution.PlanGapMutation) bool {
	for _, statement := range statements {
		for _, scope := range statement.Scopes {
			if scope.Kind == execution.GapOpen {
				return true
			}
		}
	}
	return false
}
