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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// steppedPoints is the stepped rig's PRIMARY window, in one-minute points.
const steppedPoints = 5

// steppedRig is the shadow rig - access, the real evaluator, the State store
// over redis-server, a State cache - over plans threshold Plans of one Query
// Group, strategies 7, 8, ..., each over a PRIMARY window of five points, the
// provider answering a point at every minute of it: hosts series of padding
// bytes, read in slices once the first Slot finds them past half its share.
// A host over the threshold at every point of the rig's first Slot - its five
// points all new, no State of it before - decides five events in the one
// merge of its series: what takes a Slot past a cap of fewer events at that
// series, whatever the windows hold. caps edits the per-Slot caps.
func steppedRig(t *testing.T, plans, hosts, padding int, caps func(*worker.ProvisionalBudget)) (*shadowRig, []execution.PlanIdentity) {
	t.Helper()
	specs := make([]steppedPlan, plans)
	for index := range specs {
		specs[index] = steppedPlan{threshold: "100"}
	}
	return steppedRigOf(t, specs, hosts, padding, caps)
}

// steppedPlan is one threshold Plan of the stepped rig: its threshold, its
// no-data detection, none when nil, and its recovery plan, off when empty.
type steppedPlan struct {
	threshold string
	noData    *contract.NoDataConfigV1
	recovery  string
}

func (spec steppedPlan) recoveryPlan() string {
	if spec.recovery == "" {
		return shadowRecoveryOff
	}
	return spec.recovery
}

// steppedRigOf is steppedRig over the Plans specs name, strategies 7, 8, ...
// in that order: the order a slice's series are evaluated in, Plan by Plan.
func steppedRigOf(t *testing.T, specs []steppedPlan, hosts, padding int, caps func(*worker.ProvisionalBudget)) (*shadowRig,
	[]execution.PlanIdentity) {
	t.Helper()
	facts := ringRatioQueryFacts(t)
	ref := string(execution.LogicalQueryRef(facts.QueryRevision))
	compiled, requirements := thresholdCompiledPlanOver(t, "7", ref, steppedPoints, specs[0].threshold, specs[0].noData,
		specs[0].recoveryPlan())
	rig := newShadowRigPlanned(t, hosts, padding, 64<<20, facts, compiled, requirements)
	rig.provider.stepped = true
	// A no-data store of the rig's own, holding nothing from before and
	// keeping what each Slot applied to it: a Slot and the next of another
	// rig share nothing.
	rig.noData = &recordingNoData{PlanNoDataStore: worker.SharedNoDataStore}
	rig.wiring.NoData = rig.noData
	// The gap markers in the store, as production keeps them, so a gap a Slot
	// opens is read by the next; and every event sent kept whole.
	rig.gapsSeen = &recordingGaps{GapGuardStore: rig.store.ExecutionStore}
	rig.wiring.GapGuard = rig.gapsSeen
	rig.eventsSent = &recordingEvents{EventSink: rig.ports}
	rig.wiring.Events = rig.eventsSent
	identities := []execution.PlanIdentity{rig.frozen.plan.DuePlans[0].Identity}
	for index, spec := range specs[1:] {
		next := rig.frozen.plan.DuePlans[0]
		next.Identity.StrategyID = fmt.Sprint(8 + index)
		next.CompiledPlan, _ = thresholdCompiledPlanOver(t, next.Identity.StrategyID, ref, steppedPoints, spec.threshold, spec.noData,
			spec.recoveryPlan())
		rig.frozen.plan.DuePlans = append(rig.frozen.plan.DuePlans, next)
		identities = append(identities, next.Identity)
	}
	// A State preflight batch holds at most what one call of the store
	// carries (hosts items, with its cache), as production's store bound
	// holds it; the query permits give a Slot a read width of four.
	rig.budget.MaxSeries, rig.budget.MaxStateMutations, rig.budget.QueryPermits = 1<<20, uint64(hosts), 16
	if caps != nil {
		caps(&rig.budget)
	}
	coordinator, err := worker.NewSlotExecutionCoordinator(rig.wiring, rig.budget)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = coordinator
	return rig, identities
}

// steppedSlot is what one Slot of the stepped rig read and wrote: its PRIMARY
// reads in order, the events written, the State written by Plan and series,
// and the gap statements by Plan.
type steppedSlot struct {
	result  execution.SlotExecutionResult
	err     error
	reads   []shadowRead
	events  []string
	sent    []contract.TriggerEventV1
	written map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]execution.MutationDigest
	gaps    map[execution.PlanIdentity][]execution.PlanGapMutation
	// noData is the no-data mutations applied, by Plan.
	noData map[execution.PlanIdentity]int
}

// runSteppedSlot runs the rig's Slot at evaluation, value the hosts' PRIMARY
// values at every point (60 when unnamed, under the threshold of 100).
func runSteppedSlot(rig *shadowRig, evaluation execution.EvaluationTime, value map[string]string) steppedSlot {
	rig.t.Helper()
	rig.store.mu.Lock()
	rig.store.written = nil
	rig.store.mu.Unlock()
	rig.ports.eventBatches, rig.ports.eventCount = nil, 0
	rig.gapsSeen.mu.Lock()
	rig.gapsSeen.applied = nil
	rig.gapsSeen.mu.Unlock()
	rig.eventsSent.mu.Lock()
	rig.eventsSent.sent = nil
	rig.eventsSent.mu.Unlock()
	rig.noData.mu.Lock()
	rig.noData.applied = nil
	rig.noData.mu.Unlock()
	rig.provider.mu.Lock()
	rig.provider.reads = nil
	rig.provider.mu.Unlock()
	result, err := executeShadowPlans(rig, evaluation, func(*execution.SlotExecutionRequest) {
		rig.provider.mu.Lock()
		rig.provider.value = value
		rig.provider.mu.Unlock()
	})
	slot := steppedSlot{result: result, err: err,
		written: map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]execution.MutationDigest{},
		gaps:    map[execution.PlanIdentity][]execution.PlanGapMutation{}, noData: map[execution.PlanIdentity]int{}}
	rig.noData.mu.Lock()
	for _, mutation := range rig.noData.applied {
		slot.noData[mutation.Identity.Plan]++
	}
	rig.noData.mu.Unlock()
	rig.provider.mu.Lock()
	slot.reads = append(slot.reads, rig.provider.reads...)
	rig.provider.mu.Unlock()
	for _, batch := range rig.ports.eventBatches {
		slot.events = append(slot.events, batch...)
	}
	rig.store.mu.Lock()
	for key, mutation := range rig.store.written {
		if slot.written[key.Plan] == nil {
			slot.written[key.Plan] = map[execution.SeriesIdentityDigest]execution.MutationDigest{}
		}
		slot.written[key.Plan][key.SeriesIdentityDigest] = mutation.MutationDigest
	}
	rig.store.mu.Unlock()
	rig.gapsSeen.mu.Lock()
	for _, mutation := range rig.gapsSeen.applied {
		slot.gaps[mutation.Identity.Plan] = append(slot.gaps[mutation.Identity.Plan], mutation)
	}
	rig.gapsSeen.mu.Unlock()
	rig.eventsSent.mu.Lock()
	slot.sent = append(slot.sent, rig.eventsSent.sent...)
	rig.eventsSent.mu.Unlock()
	return slot
}

// recordingNoData is a no-data store that holds nothing from before - every
// read finds no memory - and keeps every mutation applied to it.
type recordingNoData struct {
	execution.PlanNoDataStore
	mu      sync.Mutex
	applied []execution.PlanNoDataMutation
}

func (store *recordingNoData) ApplyNoData(_ context.Context, request execution.NoDataApplyRequest) (execution.NoDataApplyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := execution.NoDataApplyResult{Items: make([]execution.NoDataApplyItemResult, len(request.Items))}
	for index, mutation := range request.Items {
		store.applied = append(store.applied, mutation)
		result.Items[index] = execution.NoDataApplyItemResult{Identity: mutation.Identity, Status: execution.NoDataApplied}
	}
	return result, nil
}

// recordingGaps is the store's gap markers, keeping every statement applied.
type recordingGaps struct {
	execution.GapGuardStore
	mu      sync.Mutex
	applied []execution.PlanGapMutation
}

func (store *recordingGaps) ApplyGap(ctx context.Context, request execution.GapGuardApplyRequest) (execution.GapGuardApplyResult, error) {
	store.mu.Lock()
	store.applied = append(store.applied, request.Items...)
	store.mu.Unlock()
	return store.GapGuardStore.ApplyGap(ctx, request)
}

// recordingEvents is the event sink, keeping every event sent whole.
type recordingEvents struct {
	execution.EventSink
	mu   sync.Mutex
	sent []contract.TriggerEventV1
}

func (sink *recordingEvents) WriteBatch(ctx context.Context, events []contract.TriggerEventV1) error {
	sink.mu.Lock()
	sink.sent = append(sink.sent, events...)
	sink.mu.Unlock()
	return sink.EventSink.WriteBatch(ctx, events)
}

// slicesOf is the hosts of each slice a Slot read that held any, in the
// order read, the whole Query Group's read left out.
func slicesOf(slot steppedSlot) [][]string {
	var slices [][]string
	for _, read := range slot.reads {
		if !read.whole && len(read.hosts) != 0 {
			slices = append(slices, read.hosts)
		}
	}
	return slices
}

// The stepped rig's provider answers its PRIMARY window with a point at every
// minute of it, five, for every host; unstepped, one, at the window's start.
func TestTheSteppedRigsProviderAnswersAPointAtEveryMinuteOfItsWindow(t *testing.T) {
	for _, stepped := range []bool{true, false} {
		rig, _ := steppedRig(t, 1, 20, 0, nil)
		rig.provider.stepped = stepped
		slot := runSteppedSlot(rig, frozenContract().Slot.EvaluationTime+60, nil)
		if slot.err != nil {
			t.Fatal(slot.err)
		}
		want := map[bool]int{true: steppedPoints, false: 1}[stepped]
		if len(slot.reads) == 0 {
			t.Fatal("fixture: the Slot made no PRIMARY read")
		}
		for _, read := range slot.reads {
			if read.records != want*len(read.hosts) || len(read.hosts) == 0 {
				t.Fatalf("stepped %v: a read of %d hosts answered %d points, want %d each", stepped, len(read.hosts),
					read.records, want)
			}
		}
	}
}

// The stepped rig's first Slot - read whole, past half its share, and so in
// slices - every host under the threshold, raises no event: its five points
// are each decided and none is over, every series' State written. With one
// host over the threshold at its five, the same Slot raises five events and
// no other host any: that series' one merge carries them.
func TestASteadySliceOfTheSteppedRigRaisesNoEventAndAHostOverItFive(t *testing.T) {
	at := frozenContract().Slot.EvaluationTime + 60
	rig, _ := steppedRig(t, 1, 120, 4<<10, nil)
	steady := runSteppedSlot(rig, at, nil)
	if steady.err != nil || !steady.result.Completed {
		t.Fatalf("steady Slot: result=%+v err=%v", steady.result, steady.err)
	}
	if slices := slicesOf(steady); len(slices) < 3 {
		t.Fatalf("fixture: the Slot read %d slices holding series, want three at least", len(slices))
	}
	if len(steady.events) != 0 || len(steady.written[planIdentity()]) != 120 {
		t.Fatalf("a steady Slot raised %d events and wrote %d series' State, want none and all 120", len(steady.events),
			len(steady.written[planIdentity()]))
	}
	rig, _ = steppedRig(t, 1, 120, 4<<10, nil)
	over := runSteppedSlot(rig, at, map[string]string{"host-07": "200"})
	if over.err != nil || !over.result.Completed {
		t.Fatalf("a Slot of one host over the threshold: result=%+v err=%v", over.result, over.err)
	}
	host := string(shadowSeries(t, "host-07"))
	for _, event := range over.events {
		if event != host {
			t.Fatalf("a Slot of host-07 over the threshold raised an event of another series, %s", event)
		}
	}
	if len(over.events) != steppedPoints {
		t.Fatalf("one host over the threshold at its five points raised %d events, want five", len(over.events))
	}
}

// shadowSeries is the series digest the rig's provider gives host.
func shadowSeries(t *testing.T, host string) execution.SeriesIdentityDigest {
	t.Helper()
	digest, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", ringRatioFields(host))
	if err != nil {
		t.Fatal(err)
	}
	return execution.SeriesIdentityDigest(digest)
}
