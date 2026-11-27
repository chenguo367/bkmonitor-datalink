// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package progress

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// countedStore is a store over control whose conflicts and query-free
// completions of Slots applied whole are counted.
func countedStore(t *testing.T, control ControlStore, slots ContinuousSlotResolver) (*Store, *[]string, *[]string) {
	t.Helper()
	conflicts, applied := []string{}, []string{}
	store, err := NewStore(StoreOptions{Prefix: "alarmd", Control: control, Slots: slots, Now: func() time.Time { return time.Unix(1, 0) },
		Conflicts:    func(operation, reason string) { conflicts = append(conflicts, operation+"/"+reason) },
		AppliedWhole: func(kind string) { applied = append(applied, kind) }})
	if err != nil {
		t.Fatal(err)
	}
	return store, &conflicts, &applied
}

func fullCommitAt(slot execution.EvaluationTime) execution.ProgressCommitRequest {
	return execution.ProgressCommitRequest{Identity: execution.ProgressIdentity{QueryGroup: "q"},
		OwnerFence: execution.OwnerFence{QueryGroup: "q", OwnerID: "w", OwnerEpoch: 1, LeaseToken: "l"}, ExpectedNextSlot: slot,
		Completion: execution.SlotCompletion{Contract: progressContractAt(slot), Kind: execution.CompletionFull,
			Primary: &execution.PrimaryInputFact{Completeness: execution.CompletenessFull, DataState: execution.DataStateData},
			Result:  observability.ResultSuccess}}
}

// Each of the four facts a commit is refused by is named on the result and
// counted once under its own word; a cursor not at the commit's Slot also
// says the Slot the record's completed facts derive and the cursor it
// stores. They were one CONFLICT, the same on every refusal.
func TestARefusedCommitNamesTheFactThatRefusedIt(t *testing.T) {
	identity := execution.ProgressIdentity{QueryGroup: "q"}
	refused := func(name string, control *controlFake, slots ContinuousSlotResolver, request execution.ProgressCommitRequest,
		want execution.ProgressCommitResult) {
		t.Helper()
		store, conflicts, _ := countedStore(t, control, slots)
		result, err := store.CommitProgress(context.Background(), request)
		if err != nil || !reflect.DeepEqual(result, want) {
			t.Fatalf("%s: CommitProgress = (%+v, %v), want %+v", name, result, err, want)
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("%s: the refusal does not validate: %v", name, err)
		}
		if wantCounted := []string{"commit/" + string(want.Conflict)}; !reflect.DeepEqual(*conflicts, wantCounted) {
			t.Fatalf("%s: counted %v, want %v", name, *conflicts, wantCounted)
		}
	}
	pending := rangeFixture(t)
	inRange := execution.ScheduleProgress{Identity: identity, NextSlot: 60, UnfinishedRange: &pending}
	refused("range in flight", &controlFake{value: mustEncode(t, inRange)}, fixedSlotResolver{interval: 60}, fullCommitAt(60),
		execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalRangeInFlight})
	// Another attempt moved the cursor on: the two cursors agree, and are
	// past the commit's Slot.
	movedOn := execution.ScheduleProgress{Identity: identity, NextSlot: 180, LastFullSlot: 120}
	refused("cursor moved on", &controlFake{value: mustEncode(t, movedOn)}, fixedSlotResolver{interval: 60}, fullCommitAt(120),
		execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalCursorMoved,
			RecordNext: 180, StoredNext: 180})
	// The grid moved under the record: its completed facts derive another
	// Slot than the one it stores.
	regridded := execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60}
	refused("grid moved", &controlFake{value: mustEncode(t, regridded)}, mappedSlotResolver{60: 90}, fullCommitAt(120),
		execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalCursorMoved,
			RecordNext: 90, StoredNext: 120})
	absent := fullCommitAt(60)
	absent.Projection = progressProjectionAt(60)
	refused("absent", &controlFake{missing: true}, fixedSlotResolver{interval: 60}, absent,
		execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalProgressMissing})
	current := execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60}
	refused("lost compare-and-set", &controlFake{value: mustEncode(t, current), status: ownership.FencedCASConflict},
		fixedSlotResolver{interval: 60}, fullCommitAt(120),
		execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalCASConflict})
}

// A conflict met on the owner's record is read again and written once more
// within the call (fencedWrite); the call is counted once, under the fact
// its last pass was refused by.
func TestACommitWrittenAgainOnAConflictIsCountedOnce(t *testing.T) {
	identity := execution.ProgressIdentity{QueryGroup: "q"}
	fake := &controlFake{value: mustEncode(t, execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60})}
	store, conflicts, _ := countedStore(t, fake, fixedSlotResolver{interval: 60})
	if _, err := store.LoadProgress(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	// The owner's record is the one just read; another writer moves the
	// key on behind it.
	if _, err := store.CommitProgress(context.Background(), fullCommitAt(120)); err != nil {
		t.Fatal(err)
	}
	fake.value = mustEncode(t, execution.ScheduleProgress{Identity: identity, NextSlot: 240, LastFullSlot: 180})
	result, err := store.CommitProgress(context.Background(), fullCommitAt(180))
	want := execution.ProgressCommitResult{Status: execution.ProgressConflict, Conflict: execution.SkipRefusalCursorMoved,
		RecordNext: 240, StoredNext: 240}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("CommitProgress = (%+v, %v), want %+v", result, err, want)
	}
	if wantCounted := []string{"commit/" + string(execution.SkipRefusalCursorMoved)}; !reflect.DeepEqual(*conflicts, wantCounted) {
		t.Fatalf("counted %v, want %v: once a call, from the pass it ended on", *conflicts, wantCounted)
	}
}

// A query-free completion of a Slot an earlier attempt applied whole is
// counted by its kind once its commit applied: how often an attempt ends
// between its apply and its commit. One whose evidence says otherwise, or
// whose commit was refused, is not.
func TestAQueryFreeCompletionOfASlotAppliedWholeIsCountedOnceApplied(t *testing.T) {
	identity := execution.ProgressIdentity{QueryGroup: "q"}
	queryFree := func(kind execution.CompletionKind, evidence execution.ExecutionEvidence) execution.ProgressCommitRequest {
		reason := execution.ReasonCode(contract.ReasonGapSkipped)
		if kind == execution.CompletionSnapshotUnavailable {
			reason = execution.ReasonCode(contract.ReasonSnapshotUnavailable)
		}
		request := fullCommitAt(120)
		request.Completion = execution.SlotCompletion{Contract: progressContractAt(120), Kind: kind,
			Result: observability.ResultDegraded, ReasonCode: reason, Evidence: &evidence}
		return request
	}
	whole := execution.ExecutionEvidence{Kind: execution.EvidenceStateApplied, PlansApplied: 2, PlansTotal: 2}
	part := execution.ExecutionEvidence{Kind: execution.EvidenceStateApplied, PlansApplied: 1, PlansTotal: 2}
	for _, test := range []struct {
		name    string
		status  ownership.FencedCASStatus
		request execution.ProgressCommitRequest
		want    []string
	}{
		{"gap skipped, applied whole", "", queryFree(execution.CompletionGapSkipped, whole), []string{"GAP_SKIPPED"}},
		{"snapshot unavailable, applied whole", "", queryFree(execution.CompletionSnapshotUnavailable, whole),
			[]string{"SNAPSHOT_UNAVAILABLE"}},
		{"gap skipped, applied in part", "", queryFree(execution.CompletionGapSkipped, part), []string{}},
		{"gap skipped, applied whole, refused", ownership.FencedCASConflict, queryFree(execution.CompletionGapSkipped, whole), []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &controlFake{value: mustEncode(t, execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60}),
				status: test.status}
			store, _, applied := countedStore(t, fake, fixedSlotResolver{interval: 60})
			if _, err := store.CommitProgress(context.Background(), test.request); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*applied, test.want) {
				t.Fatalf("counted %v, want %v", *applied, test.want)
			}
		})
	}
}

// Each fact a BeginSlot is refused by is named on its result and counted
// once under begin; a cursor not at the requested Slot also says the Slot
// the record's completed facts derive and the cursor it stores.
func TestARefusedBeginSlotNamesTheFactThatRefusedIt(t *testing.T) {
	identity := execution.ProgressIdentity{QueryGroup: "q"}
	fence := execution.OwnerFence{QueryGroup: "q", OwnerID: "w", OwnerEpoch: 1, LeaseToken: "l"}
	refused := func(name string, control *controlFake, slots ContinuousSlotResolver, projection execution.UnfinishedSlotProjection,
		want execution.ProgressBeginResult) {
		t.Helper()
		store, conflicts, _ := countedStore(t, control, slots)
		result, err := store.BeginSlot(context.Background(), execution.ProgressBeginRequest{Identity: identity, OwnerFence: fence,
			Projection: projection})
		if err != nil || !reflect.DeepEqual(result, want) {
			t.Fatalf("%s: BeginSlot = (%+v, %v), want %+v", name, result, err, want)
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("%s: the refusal does not validate: %v", name, err)
		}
		if wantCounted := []string{"begin/" + string(want.Conflict)}; !reflect.DeepEqual(*conflicts, wantCounted) {
			t.Fatalf("%s: counted %v, want %v", name, *conflicts, wantCounted)
		}
	}
	conflict := func(refusal execution.ProgressSkipRefusal, recordNext, storedNext execution.EvaluationTime) execution.ProgressBeginResult {
		return execution.ProgressBeginResult{Status: execution.ProgressConflict, Conflict: refusal, RecordNext: recordNext, StoredNext: storedNext}
	}
	fixed := fixedSlotResolver{interval: 60}
	other := execution.ScheduleProgress{Identity: execution.ProgressIdentity{QueryGroup: "other"}, NextSlot: 60}
	refused("another Query Group's record", &controlFake{value: mustEncode(t, other)}, fixed, progressProjectionAt(60),
		conflict(execution.ConflictIdentityMismatch, 0, 0))
	pending := rangeFixture(t)
	inRange := execution.ScheduleProgress{Identity: identity, NextSlot: 60, UnfinishedRange: &pending}
	refused("range in flight", &controlFake{value: mustEncode(t, inRange)}, fixed, progressProjectionAt(60),
		conflict(execution.SkipRefusalRangeInFlight, 0, 0))
	unfinished := progressProjectionAt(120)
	begun := execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60, UnfinishedSlot: &unfinished}
	// A later Segment owns a Slot the abandoned one's successor is not.
	refused("abandoned for another Slot", &controlFake{value: mustEncode(t, begun)}, fixed, newerSegmentProjectionAt(240),
		conflict(execution.SkipRefusalCursorMoved, 180, 120))
	refused("another Slot on the same Segment", &controlFake{value: mustEncode(t, begun)}, fixed, progressProjectionAt(180),
		conflict(execution.SkipRefusalCursorMoved, 120, 120))
	newer := newerSegmentProjectionAt(120)
	begunNewer := execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60, UnfinishedSlot: &newer}
	refused("the same Slot through an older Segment", &controlFake{value: mustEncode(t, begunNewer)}, fixed, progressProjectionAt(120),
		conflict(execution.ConflictSegmentOlder, 0, 0))
	idle := execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60}
	refused("a Slot past the cursor", &controlFake{value: mustEncode(t, idle)}, fixed, progressProjectionAt(180),
		conflict(execution.SkipRefusalCursorMoved, 120, 120))
	// The grid moved under the record: off its cursor, its completed facts
	// derive another Slot than the one asked.
	refused("a grid moved under the record", &controlFake{value: mustEncode(t, idle)}, mappedSlotResolver{60: 90}, progressProjectionAt(150),
		conflict(execution.SkipRefusalCursorMoved, 90, 120))
	refused("lost compare-and-set", &controlFake{missing: true, status: ownership.FencedCASConflict}, fixed, progressProjectionAt(60),
		conflict(execution.SkipRefusalCASConflict, 0, 0))
}

// Each fact an expired range is refused by is named on its result and
// counted once under range.
func TestARefusedExpiredRangeNamesTheFactThatRefusedIt(t *testing.T) {
	identity := execution.ProgressIdentity{QueryGroup: "q"}
	p := rangeFixture(t)
	request := execution.ExpiredRangeRequest{Projection: p, OwnerFence: execution.OwnerFence{QueryGroup: "q", OwnerID: "w", OwnerEpoch: 1,
		LeaseToken: "l"}}
	refused := func(name string, control *controlFake, slots ContinuousSlotResolver, commit bool, want execution.ProgressSkipRefusal) {
		t.Helper()
		store, conflicts, _ := countedStore(t, control, slots)
		change := store.BeginRange
		if commit {
			change = store.CommitRange
		}
		result, err := change(context.Background(), request)
		if err != nil || result.Status != execution.ProgressConflict || result.Conflict != want {
			t.Fatalf("%s: = (%+v, %v), want a conflict refused by %s", name, result, err, want)
		}
		if wantCounted := []string{"range/" + string(want)}; !reflect.DeepEqual(*conflicts, wantCounted) {
			t.Fatalf("%s: counted %v, want %v", name, *conflicts, wantCounted)
		}
	}
	fixed := fixedSlotResolver{interval: 60}
	at := func(progress execution.ScheduleProgress) *controlFake {
		return &controlFake{value: mustEncode(t, progress)}
	}
	refused("absent", &controlFake{missing: true}, fixed, false, execution.SkipRefusalProgressMissing)
	refused("another Query Group's record", at(execution.ScheduleProgress{Identity: execution.ProgressIdentity{QueryGroup: "other"},
		NextSlot: 60}), fixed, false, execution.ConflictIdentityMismatch)
	unfinished := progressProjectionAt(60)
	refused("a Slot in flight", at(execution.ScheduleProgress{Identity: identity, NextSlot: 60, UnfinishedSlot: &unfinished}), fixed, false,
		execution.ConflictSlotInFlight)
	refused("the cursor elsewhere", at(execution.ScheduleProgress{Identity: identity, NextSlot: 120, LastFullSlot: 60}), fixed, false,
		execution.SkipRefusalCursorMoved)
	another := distanceRangeFixture(t)
	refused("another range in flight", at(execution.ScheduleProgress{Identity: identity, NextSlot: 60, UnfinishedRange: &another}), fixed,
		false, execution.SkipRefusalRangeInFlight)
	refused("a range never begun", at(execution.ScheduleProgress{Identity: identity, NextSlot: 60}), fixed, true,
		execution.ConflictRangeMissing)
	refused("the grid moved under the proof", at(execution.ScheduleProgress{Identity: identity, NextSlot: 60}),
		mappedSlotResolver{180: 300}, false, execution.ConflictGridMoved)
	lost := at(execution.ScheduleProgress{Identity: identity, NextSlot: 60})
	lost.status = ownership.FencedCASConflict
	refused("lost compare-and-set", lost, fixed, false, execution.SkipRefusalCASConflict)
}
