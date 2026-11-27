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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// overSlice is the hosts of the slice numbered at of the Slot the stepped
// rig's specs read first, every host under its thresholds, valued over the
// second Plan's threshold: what takes that slice past a cap of three events.
func overSlice(t *testing.T, specs []steppedPlan, at int) (map[string]string, [][]string) {
	t.Helper()
	rig, _ := steppedRigOf(t, specs, 120, 4<<10, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = 3 })
	steady := runSteppedSlot(rig, frozenContract().Slot.EvaluationTime+60, nil)
	if steady.err != nil {
		t.Fatal(steady.err)
	}
	slices := slicesOf(steady)
	if len(slices) < 4 {
		t.Fatalf("fixture: the Slot read %d slices holding series, want four at least", len(slices))
	}
	over := map[string]string{}
	for _, host := range slices[at] {
		over[host] = "200"
	}
	return over, slices
}

// replacedAt reports whether the Slot answered plan with a gap past its
// budget.
func replacedAt(slot steppedSlot, plan execution.PlanIdentity) bool {
	for _, statement := range slot.gaps[plan] {
		for _, scope := range statement.Scopes {
			if scope.Kind == execution.GapOpen && scope.ReasonCode == execution.ReasonCode(contract.ReasonExecutionBudgetExhausted) {
				return true
			}
		}
	}
	return false
}

// A Slot read in slices whose second slice takes it past its budget at its
// first Plan - that slice's hosts over its threshold at their five points,
// five events in one merge, a Slot of three at most - is replaced there. The
// second Plan, got past in the first slice, has series in the second too, and
// was not yet reached there: every series of it, and of the first, is either
// written or guarded by a gap its Plan opens, none left unwritten while its
// marker says nothing of them.
func TestAPlanGotPastInAnEarlierSliceIsGuardedWhenALaterSliceIsReplacedPastTheBudget(t *testing.T) {
	specs := []steppedPlan{{threshold: "100"}, {threshold: "100"}}
	over, _ := overSlice(t, specs, 1)
	rig, plans := steppedRigOf(t, specs, 120, 4<<10, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = 3 })
	slot := runSteppedSlot(rig, frozenContract().Slot.EvaluationTime+60, over)
	if slot.err != nil {
		t.Fatal(slot.err)
	}
	if !replacedAt(slot, plans[0]) {
		t.Fatalf("fixture: the first Plan was not answered past the budget (gaps %v)", slot.gaps)
	}
	for _, plan := range plans {
		var unwritten []string
		for _, read := range slot.reads {
			for _, host := range read.hosts {
				if _, written := slot.written[plan][shadowSeries(t, host)]; !written {
					unwritten = append(unwritten, host)
				}
			}
		}
		if len(unwritten) != 0 && !opened(slot.gaps[plan]) {
			t.Fatalf("Plan %s wrote no State for %d series read (%v) and opened no gap: they were taken for written",
				plan.StrategyID, len(unwritten), unwritten)
		}
	}
}

// A Slot read in slices whose first slice takes it past its budget, before
// any window was cut, is replaced whole: every Plan answered with a gap over
// the Slot, nothing it evaluated written. Its later slices are read, and
// write nothing of those Plans either: a Plan the replacement answered is
// answered for the Slot.
func TestASlotReplacedPastTheBudgetInItsFirstSliceWritesNothingOfItsLaterSlices(t *testing.T) {
	specs := []steppedPlan{{threshold: "100"}, {threshold: "100"}}
	over, slices := overSlice(t, specs, 0)
	rig, plans := steppedRigOf(t, specs, 120, 4<<10, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = 3 })
	slot := runSteppedSlot(rig, frozenContract().Slot.EvaluationTime+60, over)
	if slot.err != nil {
		t.Fatal(slot.err)
	}
	for _, plan := range plans {
		if !replacedAt(slot, plan) {
			t.Fatalf("fixture: Plan %s was not answered past the budget (gaps %v)", plan.StrategyID, slot.gaps)
		}
		var written []string
		for _, slice := range slices[1:] {
			for _, host := range slice {
				if _, wrote := slot.written[plan][shadowSeries(t, host)]; wrote {
					written = append(written, host)
				}
			}
		}
		if len(written) != 0 {
			t.Fatalf("Plan %s, answered with a gap over the Slot, wrote State for %d series of its later slices (%v)",
				plan.StrategyID, len(written), written)
		}
	}
}
