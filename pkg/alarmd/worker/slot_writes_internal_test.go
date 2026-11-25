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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// ledgerAdmission counts the admitter's two halves.
type ledgerAdmission struct {
	fenceChecks, activationChecks int
	refuse                        error
}

func (admission *ledgerAdmission) Check(context.Context, execution.SideEffectAdmissionRequest) (execution.SideEffectAdmissionResult, error) {
	admission.fenceChecks++
	admission.activationChecks++
	return execution.SideEffectAdmissionResult{Admitted: true}, admission.refuse
}

func (admission *ledgerAdmission) CheckActivation(context.Context, execution.SideEffectAdmissionRequest) (execution.SideEffectAdmissionResult, error) {
	admission.activationChecks++
	return execution.SideEffectAdmissionResult{Admitted: true}, nil
}

func (admission *ledgerAdmission) CheckOwnerFence(context.Context, execution.FrozenExecutionContractRef, execution.OwnerFence) error {
	admission.fenceChecks++
	return admission.refuse
}

// ledgerProgress answers every BeginSlot with status, counted.
type ledgerProgress struct {
	execution.ProgressStore
	begins int
	status execution.ProgressCommitStatus
}

func (progress *ledgerProgress) BeginSlot(context.Context, execution.ProgressBeginRequest) (execution.ProgressBeginResult, error) {
	progress.begins++
	return execution.ProgressBeginResult{Status: progress.status}, nil
}

func ledgerCoordinator(admission *ledgerAdmission, progress *ledgerProgress) *SlotExecutionCoordinator {
	return &SlotExecutionCoordinator{ports: Ports{Admission: admission, Progress: progress, Observer: observability.NopObserver{}}}
}

func clearedUnfenced(ctx context.Context) bool {
	clearance, cleared := ctx.Value(writeClearanceContextKey{}).(writeClearance)
	return cleared && clearance.unfenced
}

// The fence before a write whose script does not run it is checked once a
// step: a step begins where a Plan is admitted - by its activation alone -
// or where a write is a step of its own, and a write whose script runs the
// fence needs no check (014 ⑩ F1). A context with no attempt's ledger is
// not checked here: its admission checked the fence.
func TestTheFenceIsCheckedOnceAStepBeforeTheUnfencedWrites(t *testing.T) {
	admission := &ledgerAdmission{}
	coordinator := ledgerCoordinator(admission, &ledgerProgress{status: execution.ProgressCommitted})
	request := shadowRequest(execution.OperationNormal, 60)
	ctx, _ := withSlotWrites(context.Background(), coordinator, request)
	write := func(unfenced bool) context.Context {
		t.Helper()
		cleared, err := coordinator.beforeWrite(ctx, unfenced)
		if err != nil {
			t.Fatal(err)
		}
		return cleared
	}
	if cleared := write(true); !clearedUnfenced(cleared) || admission.fenceChecks != 1 {
		t.Fatalf("the step's first unfenced write: checks %d, cleared %t", admission.fenceChecks, clearedUnfenced(cleared))
	}
	write(true)
	write(false)
	if admission.fenceChecks != 1 {
		t.Fatalf("the step's later writes checked the fence again: %d checks", admission.fenceChecks)
	}
	if err := coordinator.admitPlan(ctx, request, execution.PlanKey{}, 1); err != nil {
		t.Fatal(err)
	}
	if admission.activationChecks != 1 || admission.fenceChecks != 1 {
		t.Fatalf("a Plan's admission checked activation %d times and the fence %d, want its activation alone",
			admission.activationChecks, admission.fenceChecks)
	}
	write(true)
	if admission.fenceChecks != 2 {
		t.Fatalf("the admitted step's first unfenced write checked the fence %d times in all, want a check of its own", admission.fenceChecks)
	}
	fenceStep(ctx)
	write(true)
	if admission.fenceChecks != 3 {
		t.Fatalf("a step of its own checked the fence %d times in all, want a check of its own", admission.fenceChecks)
	}
	if cleared, err := coordinator.beforeWrite(context.Background(), true); err != nil || admission.fenceChecks != 3 || clearedUnfenced(cleared) {
		t.Fatalf("a write with no attempt's ledger: err %v, checks %d", err, admission.fenceChecks)
	}
}

// A fence the store refused is every later unfenced write's answer, asked
// once; a fenced write is left to its own script.
func TestARefusedFenceIsTheAnswerOfEveryLaterUnfencedWrite(t *testing.T) {
	admission := &ledgerAdmission{refuse: ownership.ErrStaleFence}
	coordinator := ledgerCoordinator(admission, &ledgerProgress{status: execution.ProgressCommitted})
	ctx, _ := withSlotWrites(context.Background(), coordinator, shadowRequest(execution.OperationNormal, 60))
	for range 2 {
		if _, err := coordinator.beforeWrite(ctx, true); !errors.Is(err, ownership.ErrStaleFence) {
			t.Fatalf("an unfenced write after a refused fence: %v", err)
		}
	}
	if admission.fenceChecks != 1 {
		t.Fatalf("a refused fence was asked %d times", admission.fenceChecks)
	}
	if _, err := coordinator.beforeWrite(ctx, false); err != nil {
		t.Fatalf("a fenced write after a refused fence check: %v, want it left to its script", err)
	}
}

// A BeginSlot put off is made once, before the attempt's first write, and
// its script's fence stands for the step's check; one that does not begin
// the Slot is every later write's answer (014 ⑩ F5).
func TestAPutOffBeginSlotIsMadeOnceBeforeTheFirstWrite(t *testing.T) {
	admission := &ledgerAdmission{}
	progress := &ledgerProgress{status: execution.ProgressCommitted}
	coordinator := ledgerCoordinator(admission, progress)
	ctx, writes := withSlotWrites(context.Background(), coordinator, shadowRequest(execution.OperationNormal, 60))
	writes.deferBegin(execution.ProgressBeginRequest{})
	if !writes.unbegun() {
		t.Fatal("a put-off BeginSlot reads as made")
	}
	for _, unfenced := range []bool{false, true, true} {
		if _, err := coordinator.beforeWrite(ctx, unfenced); err != nil {
			t.Fatal(err)
		}
	}
	if progress.begins != 1 || writes.unbegun() {
		t.Fatalf("the put-off BeginSlot was made %d times", progress.begins)
	}
	if admission.fenceChecks != 0 {
		t.Fatalf("the step's unfenced writes checked the fence %d times behind the BeginSlot that ran it", admission.fenceChecks)
	}

	refused := &ledgerProgress{status: execution.ProgressStaleOwner}
	coordinator = ledgerCoordinator(&ledgerAdmission{}, refused)
	ctx, writes = withSlotWrites(context.Background(), coordinator, shadowRequest(execution.OperationNormal, 60))
	writes.deferBegin(execution.ProgressBeginRequest{})
	for range 2 {
		_, err := coordinator.beforeWrite(ctx, false)
		var notBegun *execution.SlotBeginRefusedError
		if !errors.As(err, &notBegun) || !errors.Is(err, ownership.ErrStaleFence) {
			t.Fatalf("a write after a refused put-off BeginSlot: %v", err)
		}
	}
	if refused.begins != 1 || writes.notBegun() == nil {
		t.Fatalf("a refused put-off BeginSlot was made %d times, recorded %v", refused.begins, writes.notBegun())
	}
}

// exitProgress answers every BeginSlot committed and keeps whether the
// context it was made on was live.
type exitProgress struct {
	execution.ProgressStore
	begins  int
	liveCtx bool
}

func (progress *exitProgress) BeginSlot(ctx context.Context, _ execution.ProgressBeginRequest) (execution.ProgressBeginResult, error) {
	progress.begins++
	progress.liveCtx = ctx.Err() == nil
	return execution.ProgressBeginResult{Status: execution.ProgressCommitted}, nil
}

// exitCounts is slot_begin_total, by its words.
type exitCounts map[string]int

func (counts exitCounts) RecordSlotBegin(result string) { counts[result]++ }
func (counts exitCounts) RecordSlotNotCommitted(string) {}

// An attempt that still owes its put-off BeginSlot as it leaves without
// writing its Slot down makes it then, on a context of its own - the
// attempt's stopped is one of the ways out - but not when the owner fence
// turned it away, which is counted as a BeginSlot refused at its start and
// returns its own refusal; one whose commit stood in for it makes none
// (014 ⑩ F5).
func TestAnAttemptLeavingUncommittedMakesTheBeginSlotItOwes(t *testing.T) {
	exitWith := func(exitErr error, standIn bool) (*exitProgress, exitCounts, *slotWrites) {
		progress, counts := &exitProgress{}, exitCounts{}
		coordinator := &SlotExecutionCoordinator{ports: Ports{Admission: &ledgerAdmission{}, Progress: progress,
			SlotCounts: counts, Observer: observability.NopObserver{}}}
		ctx, writes := withSlotWrites(context.Background(), coordinator, shadowRequest(execution.OperationNormal, 60))
		writes.deferBegin(execution.ProgressBeginRequest{})
		if standIn {
			writes.commitStandsIn()
		}
		stopped, stop := context.WithCancel(ctx)
		stop()
		writes.beginOnExit(stopped, exitErr, false)
		return progress, counts, writes
	}
	progress, counts, writes := exitWith(context.Canceled, false)
	if progress.begins != 1 || !progress.liveCtx || writes.unbegun() || writes.notBegun() != nil {
		t.Fatalf("a stopped attempt's BeginSlot on its way out: made %d, on a live context %t, owed %t, not begun %v",
			progress.begins, progress.liveCtx, writes.unbegun(), writes.notBegun())
	}
	if counts[execution.SlotBeginResult(execution.ProgressCommitted)] != 1 {
		t.Fatalf("the BeginSlot on the way out counted %v", counts)
	}
	for _, refusal := range []struct {
		err  error
		word string
	}{
		{fmt.Errorf("a write: %w", ownership.ErrStaleFence), execution.SlotBeginResult(execution.ProgressStaleOwner)},
		{fmt.Errorf("a write: %w", ownership.ErrContentScopeMoved), execution.SlotBeginResult(execution.ProgressContentMoved)},
	} {
		progress, counts, writes := exitWith(refusal.err, false)
		notBegun := writes.notBegun()
		if progress.begins != 0 || notBegun == nil || notBegun.err != refusal.err || counts[refusal.word] != 1 {
			t.Fatalf("an attempt the fence turned away (%v): made %d BeginSlots, returns %v, counted %v, want none, its refusal, %s",
				refusal.err, progress.begins, notBegun, counts, refusal.word)
		}
	}
	if progress, counts, _ := exitWith(nil, true); progress.begins != 0 || len(counts) != 0 {
		t.Fatalf("an attempt whose commit stood in for its BeginSlot made %d on its way out, counted %v", progress.begins, counts)
	}
}

// endingAlerts is the open-alert copy holding every alert open, its Levels
// not known, as a judge.
type endingAlerts struct {
	*levelCopy
	fakeOpenAlertJudge
}

// countedEndIndex is the no-data end index, its writes counted.
type countedEndIndex struct {
	execution.NoDataEndIndex
	writes int
}

func (index *countedEndIndex) ApplyNoDataEnds(context.Context, execution.NoDataEndWrite) error {
	index.writes++
	return nil
}

// A no-data end's recovery events are sent behind the fence's check of
// their step: a refused owner sends none and its round returns the refusal,
// where the events' own send would have counted it a send that failed and
// gone on, writing nothing to say so (014 ⑩ F1).
func TestARefusedOwnerSendsNoNoDataEndAndReturnsTheRefusal(t *testing.T) {
	// The plan at a snapshot and the alert's dedupe digest an end's event
	// names; the series' last real point four periods before the judged
	// one, its end due (TestAnAbsentSeriesEndsOnNoDataOnlyOnALiveSource).
	plan := internalCompiledPlanAtSnapshot(t, "301", 1, 60, json.RawMessage(`{"enabled":true,"consecutive_windows":2,"missing_data":"evaluate_as_zero"}`), 7)
	due := execution.DuePlan{Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "301"}, CompiledPlan: plan}
	const now = int64(1_788_000_000)
	fingerprint := strings.Repeat("fa", 16)
	entry := noDataEndEntryFor("a", fingerprint, now-300)
	round := noDataEndPlan{loaded: execution.NoDataEndLoaded{Entries: []execution.NoDataEndEntry{entry}}, complete: true,
		present: map[execution.SeriesIdentityDigest]struct{}{}}
	for _, refuse := range []error{nil, ownership.ErrStaleFence} {
		sink, index, admission := &reportingSink{}, &countedEndIndex{}, &ledgerAdmission{refuse: refuse}
		coordinator := &SlotExecutionCoordinator{ports: Ports{Admission: admission, Events: sink, Observer: observability.NopObserver{},
			OpenAlerts: endingAlerts{levelCopy: &levelCopy{}, fakeOpenAlertJudge: fakeOpenAlertJudge{held: map[string]bool{fingerprint: true}, judged: true}}}}
		request := execution.SlotExecutionRequest{Operation: execution.OperationNormal}
		request.Contract.Slot.EvaluationTime = execution.EvaluationTime(now)
		ctx, _ := withSlotWrites(context.Background(), coordinator, request)
		err := coordinator.applyPlanNoDataEnds(ctx, request, "execution-1", now-60, due, round, nil, index)
		if refuse == nil {
			if err != nil || sink.calls != 1 || index.writes != 1 || admission.fenceChecks != 1 {
				t.Fatalf("an owner's due end: err %v, sends %d, index writes %d, fence checks %d; want it sent and removed behind one check",
					err, sink.calls, index.writes, admission.fenceChecks)
			}
			continue
		}
		if !errors.Is(err, refuse) || sink.calls != 0 || index.writes != 0 {
			t.Fatalf("a refused owner's due end: err %v, sends %d, index writes %d; want the refusal, nothing sent or written",
				err, sink.calls, index.writes)
		}
	}
}

// stagingAfterBegins is the staging store, keeping how many BeginSlots had
// been made as each segment was staged.
type stagingAfterBegins struct {
	*stagingRosterStore
	progress    *ledgerProgress
	beginsAtSet []int
}

func (store *stagingAfterBegins) StageNoData(ctx context.Context, request execution.NoDataStageRequest) (execution.NoDataStageResult, error) {
	store.beginsAtSet = append(store.beginsAtSet, store.progress.begins)
	return store.stagingRosterStore.StageNoData(ctx, request)
}

// A first write staged ahead is a write: an attempt that put its BeginSlot
// off makes it before the first segment is staged, and one whose BeginSlot
// does not begin the Slot stages nothing - its staging stops (014 ⑩ F5).
func TestAStageAheadIsWrittenOnlyBehindTheSlotsBeginSlot(t *testing.T) {
	due := noDataWiredPlan(t)
	evaluation := int64(1_788_000_000)
	_, hosts := rosterGroups(t, execution.NoDataFirstWriteSegmentGroups, evaluation-60)
	for _, status := range []execution.ProgressCommitStatus{execution.ProgressCommitted, execution.ProgressStaleOwner} {
		progress := &ledgerProgress{status: status}
		store := &stagingAfterBegins{stagingRosterStore: &stagingRosterStore{rosterStore: &rosterStore{}}, progress: progress}
		stream := rosterStream(t, due, store.rosterStore, 7)
		stream.coordinator.ports.NoData, stream.coordinator.ports.Progress = store, progress
		ctx, writes := withSlotWrites(context.Background(), stream.coordinator, stream.request)
		writes.deferBegin(execution.ProgressBeginRequest{})
		present := stream.windows.presentSet(due)
		for _, host := range hosts {
			if key, fresh := present.Add(host); fresh {
				stream.stageAhead(ctx, due, key)
			}
		}
		if status == execution.ProgressCommitted {
			if len(store.beginsAtSet) != 1 || store.beginsAtSet[0] != 1 {
				t.Fatalf("a segment staged with %v BeginSlots made before it, want one segment behind the BeginSlot", store.beginsAtSet)
			}
			continue
		}
		if len(store.beginsAtSet) != 0 || stream.stageAheadEnds[stageAheadStopped] != 1 || progress.begins != 1 {
			t.Fatalf("a refused BeginSlot staged %v, its staging ended %v after %d BeginSlots; want nothing staged, stopped",
				store.beginsAtSet, stream.stageAheadEnds, progress.begins)
		}
	}
}
