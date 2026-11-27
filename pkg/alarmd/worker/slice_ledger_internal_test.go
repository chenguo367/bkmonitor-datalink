// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// ledgerSlice is one slice's part for the ledger cases: series of plan a and
// b, its streamed binding of a, and completion bindings of a and b, b's
// answered as complete says.
func ledgerSlice(number, seriesA, seriesB int, complete execution.Completeness) (map[execution.PlanIdentity]int,
	[]execution.NamedInputBinding, []execution.NamedInputBinding) {
	a, b := execution.PlanIdentity{StrategyID: "a"}, execution.PlanIdentity{StrategyID: "b"}
	binding := func(plan execution.PlanIdentity, requirement string, completeness execution.Completeness) execution.NamedInputBinding {
		return execution.NamedInputBinding{Consumer: execution.ConsumerRef{Plan: plan, LevelID: 1, HasLevel: true},
			RequirementID: execution.RequirementID(requirement), Completeness: completeness,
			ProviderResult: execution.ProviderResultRef(fmt.Sprintf("slice-%d", number))}
	}
	return map[execution.PlanIdentity]int{a: seriesA, b: seriesB},
		[]execution.NamedInputBinding{binding(a, "r1", execution.CompletenessFull)},
		[]execution.NamedInputBinding{binding(a, "r1", execution.CompletenessFull), binding(b, "r2", complete)}
}

// What a slice reads of the Slot so far is the slices numbered up to its
// own, whatever order they completed in: the same series, the same bindings
// in the same order, and the lowest-numbered slice's completion bindings
// whole and every other's that did not complete. A slice let go before it
// completed holds a number nothing carries (015 C1 appendix A 2, 3).
func TestWhatASliceReadsSoFarIsTheSlicesNumberedUpToItsOwn(t *testing.T) {
	type part struct {
		number, a, b int
		complete     execution.Completeness
	}
	// Slice 1 was let go (split): its children are 3 and 4.
	parts := []part{{0, 4, 0, execution.CompletenessFull}, {2, 3, 0, execution.CompletenessPartial},
		{3, 2, 0, execution.CompletenessFull}, {4, 1, 0, execution.CompletenessFull}}
	record := func(order []int) *sliceLedger {
		ledger := &sliceLedger{}
		for _, index := range order {
			p := parts[index]
			series, streamed, completion := ledgerSlice(p.number, p.a, p.b, p.complete)
			ledger.complete(p.number, series, streamed, completion)
		}
		return ledger
	}
	inOrder, outOfOrder := record([]int{0, 1, 2, 3}), record([]int{3, 1, 0, 2})
	a, b := execution.PlanIdentity{StrategyID: "a"}, execution.PlanIdentity{StrategyID: "b"}
	for _, through := range []int{0, 1, 2, 3, 4} {
		wantSeries, wantBindings := inOrder.through(through)
		gotSeries, gotBindings := outOfOrder.through(through)
		if !reflect.DeepEqual(gotSeries, wantSeries) || !reflect.DeepEqual(gotBindings, wantBindings) {
			t.Fatalf("through %d out of order: series %v bindings %v, want %v %v", through, gotSeries, gotBindings, wantSeries, wantBindings)
		}
	}
	series, bindings := outOfOrder.through(3)
	if series[a] != 9 || series[b] != 0 {
		t.Fatalf("series through 3 = %v, want a 4+3+2 of slices 0, 2 and 3 and b none", series)
	}
	// Slice 0's completion bindings whole, slice 2's PARTIAL one of b, and
	// every slice's streamed binding of a: a's r1 four times - slice 0's
	// streamed and completion, 2's and 3's streamed - in slice order, then b.
	var got []string
	for _, binding := range bindings {
		got = append(got, fmt.Sprintf("%s/%s/%s/%s", binding.Consumer.Plan.StrategyID, binding.RequirementID,
			binding.ProviderResult, binding.Completeness))
	}
	want := []string{"a/r1/slice-0/FULL", "a/r1/slice-0/FULL", "a/r1/slice-2/FULL", "a/r1/slice-3/FULL",
		"b/r2/slice-0/FULL", "b/r2/slice-2/PARTIAL"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings through 3 = %v, want %v", got, want)
	}
}

// Slices completing side by side record into the ledger and read it at
// once: run with -race, the ledger is the one place they share.
func TestSlicesRecordAndReadTheLedgerSideBySide(t *testing.T) {
	ledger := &sliceLedger{}
	var wait sync.WaitGroup
	for number := range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			series, streamed, completion := ledgerSlice(number, 1, 1, execution.CompletenessFull)
			ledger.complete(number, series, streamed, completion)
			_, _ = ledger.through(number)
		}()
	}
	wait.Wait()
	if series, _ := ledger.through(7); series[execution.PlanIdentity{StrategyID: "a"}] != 8 {
		t.Fatalf("series through 7 = %v, want every slice's", series)
	}
}

// A slice's head waits until every slice numbered below it handed its own,
// had none, or was let go, and the Slot's first slice is the one none below
// completed: so heads go to the apply side in slice order whichever
// consumer gets to its head first (015 C1 section 5, step 3.2).
func TestASlicesHeadWaitsForTheHeadsBelowItAndTheFirstIsSaidByNumber(t *testing.T) {
	// A wait that would block ends at once on a context already ended.
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	var ledger sliceLedger
	ledger.noteHeaded(0)
	if err := ledger.waitHeadsBelow(ended, 2); err == nil {
		t.Fatal("slice 2's head went on with slice 1 neither headed nor let go")
	}
	ledger.letGo(1)
	if err := ledger.waitHeadsBelow(ended, 2); err != nil {
		t.Fatalf("slice 2's head waits (%v) with slice 0 headed and slice 1 let go", err)
	}

	var later sliceLedger
	waited := make(chan error, 1)
	go func() { waited <- later.waitHeadsBelow(testwait.Context(t), 1) }()
	later.noteHeaded(0)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-testwait.Expired(t):
		t.Fatal("a head waiting on slice 0 was not woken as slice 0 handed its own")
	}

	var below sliceLedger
	below.complete(0, nil, nil, nil)
	below.letGo(1)
	if first, err := below.waitBelow(ended, 2); err != nil || first {
		t.Fatalf("slice 2 above a completed slice 0 judged first=%v (%v), want not first", first, err)
	}
	var none sliceLedger
	none.letGo(0)
	if first, err := none.waitBelow(ended, 1); err != nil || !first {
		t.Fatalf("slice 1 above a slice let go judged first=%v (%v), want the Slot's first", first, err)
	}
}
