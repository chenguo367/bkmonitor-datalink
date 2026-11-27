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
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Plan replaced past the budget is answered UNAVAILABLE with a gap over its
// Slot, and the series of the slice that took it past are not written: that
// Slot's points of them are holes, under an open gap. A hole under an open
// gap is a round nobody read whole, not a quiet one, so the next Slot does
// not count it toward a recovery: a missing round is not a recovery (015 C1,
// the exception to invariant 6, which leaves the Plans replaced to timing).
//
// On the stepped rig, one Plan recovering after six windows without a
// trigger: its Slot A raises an alert on the hosts of its last slice, over
// the threshold at their five points; Slot B, a window on, takes the Slot past
// a cap of three events at the first of them and is replaced there; Slots C
// and D, each a window on, every host under the threshold, run under A's
// caps again (a recovery of a slice's alerts is more than three events, and
// a Slot replaced past that cap would hide it). C's walk finds
// three windows without a trigger and then B's holes, which it steps over: no
// recovery. Read as quiet, the holes would make six there. D finds six of its
// own: the alerts recover. No point is sent twice.
func TestAPlanReplacedPastTheBudgetDoesNotRecoverOnTheHolesOfItsReplacedSlot(t *testing.T) {
	specs := []steppedPlan{{threshold: "100", recovery: `{"enabled":true,"consecutive_windows":6}`}}
	rig, plans := steppedRigOf(t, specs, 120, 4<<10, nil)
	at := frozenContract().Slot.EvaluationTime + 60
	window := execution.EvaluationTime(60 * steppedPoints)
	steady := runSteppedSlot(rig, at, nil)
	if steady.err != nil || !steady.result.Completed {
		t.Fatalf("fixture: the steady Slot: result=%+v err=%v", steady.result, steady.err)
	}
	slices := slicesOf(steady)
	if len(slices) < 3 {
		t.Fatalf("fixture: the Slot read %d slices holding series, want three at least", len(slices))
	}
	over := map[string]string{}
	for _, host := range slices[len(slices)-1] {
		over[host] = "200"
	}
	// The hosts over the threshold, by series: what is asserted of, the other
	// hosts' quiet windows being a recovery the rig's open-alert port passes
	// for every series.
	overSeries := map[string]bool{}
	for host := range over {
		overSeries[string(shadowSeries(t, host))] = true
	}
	abnormal := func(slot steppedSlot) int {
		count := 0
		for _, event := range slot.sent {
			if overSeries[event.RecordRef.DimensionIdentityDigest] && event.EventKind != contract.TriggerEventRecovery {
				count++
			}
		}
		return count
	}
	opening := runSteppedSlot(rig, at+window, over)
	if opening.err != nil || !opening.result.Completed || abnormal(opening) != steppedPoints*len(over) {
		t.Fatalf("fixture: Slot A raised %d events of the hosts over the threshold (err %v), want five for each of its %d",
			abnormal(opening), opening.err, len(over))
	}
	// A new process, its Slots of three events at most.
	events := rig.budget.MaxEvents
	recapSteppedRig(t, rig, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = 3 })
	replaced := runSteppedSlot(rig, at+2*window, over)
	if replaced.err != nil || !replaced.result.Completed || !replacedAt(replaced, plans[0]) {
		t.Fatalf("fixture: Slot B was not replaced past its budget (result %+v, err %v, gaps %v)", replaced.result, replaced.err,
			replaced.gaps)
	}
	for host := range over {
		if _, written := replaced.written[plans[0]][shadowSeries(t, host)]; written {
			t.Fatalf("fixture: Slot B wrote the State of %s, a host of the slice it was replaced at", host)
		}
	}
	// Another new process, its caps those of Slot A's: a recovery of the
	// alerts in one slice is more events than three, and under that cap
	// Slot C would be replaced there and say nothing of it.
	recapSteppedRig(t, rig, func(budget *worker.ProvisionalBudget) { budget.MaxEvents = events })
	recoveries := func(slot steppedSlot) map[string]bool {
		recovered := map[string]bool{}
		for _, event := range slot.sent {
			if event.EventKind == contract.TriggerEventRecovery && overSeries[event.RecordRef.DimensionIdentityDigest] {
				recovered[event.RecordRef.DimensionIdentityDigest] = true
			}
		}
		return recovered
	}
	quiet := runSteppedSlot(rig, at+3*window, nil)
	if quiet.err != nil || !quiet.result.Completed {
		t.Fatalf("Slot C: result=%+v err=%v", quiet.result, quiet.err)
	}
	if recovered := recoveries(quiet); len(recovered) != 0 {
		t.Fatalf("Slot C recovered %d series, the replaced Slot's holes counted as windows without a trigger", len(recovered))
	}
	after := runSteppedSlot(rig, at+4*window, nil)
	if after.err != nil || !after.result.Completed {
		t.Fatalf("Slot D: result=%+v err=%v", after.result, after.err)
	}
	if recovered := recoveries(after); len(recovered) != len(over) {
		t.Fatalf("Slot D recovered %d series, want the %d whose alerts Slot A opened", len(recovered), len(over))
	}
	sent := map[string]bool{}
	for _, slot := range []steppedSlot{opening, replaced, quiet, after} {
		for _, event := range slot.sent {
			point := fmt.Sprintf("%s %s %d", event.EventKind, event.RecordRef.DimensionIdentityDigest, event.RecordRef.SourceTime)
			if sent[point] {
				t.Fatalf("the point %s was sent twice", point)
			}
			sent[point] = true
		}
	}
}

// recapSteppedRig makes the rig's coordinator again with its per-Slot caps
// edited: a new process over the same stores.
func recapSteppedRig(t *testing.T, rig *shadowRig, caps func(*worker.ProvisionalBudget)) {
	t.Helper()
	caps(&rig.budget)
	coordinator, err := worker.NewSlotExecutionCoordinator(rig.wiring, rig.budget)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = coordinator
}
