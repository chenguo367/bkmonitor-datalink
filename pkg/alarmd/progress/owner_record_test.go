// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package progress

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// ownerControl is a control store whose fenced compare-and-set compares as
// the production script does - the stored bytes, or their absence, against
// the request's - and that counts the key's reads. refuse, when set, is the
// status every write answers with.
type ownerControl struct {
	mu      sync.Mutex
	value   []byte
	missing bool
	reads   int
	refuse  ownership.FencedCASStatus
}

func (control *ownerControl) ReadControl(context.Context, execution.QueryGroupIdentity, string) ([]byte, bool, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.reads++
	return append([]byte(nil), control.value...), control.missing, nil
}

func (control *ownerControl) FencedCompareAndSet(_ context.Context, request ownership.FencedCASRequest) (ownership.FencedCASStatus, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.refuse != "" {
		return control.refuse, ownership.FencedCASRefusalError(control.refuse)
	}
	if request.ExpectedMissing != control.missing || !request.ExpectedMissing && !bytes.Equal(request.Expected, control.value) {
		return ownership.FencedCASConflict, nil
	}
	control.value, control.missing = append([]byte(nil), request.Value...), false
	return ownership.FencedCASApplied, nil
}

// writeBehind writes the record behind the owner, as no production writer
// does.
func (control *ownerControl) writeBehind(value []byte) {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.value, control.missing = value, false
}

func (control *ownerControl) readCount() int {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.reads
}

// readCounts is a Reads hook keeping each (caller, source) count.
type readCounts map[[2]string]int

func (counts readCounts) hook(caller, source string) { counts[[2]string{caller, source}]++ }

type ownerFixture struct {
	control *ownerControl
	store   *Store
	reads   readCounts
	now     *time.Time
}

func newOwnerFixture(t *testing.T, current execution.ScheduleProgress, slots ContinuousSlotResolver) ownerFixture {
	t.Helper()
	control := &ownerControl{value: mustEncode(t, current)}
	reads := readCounts{}
	now := time.Unix(1_800_000_000, 0)
	store, err := NewStore(StoreOptions{Prefix: "alarmd", Control: control, Slots: slots,
		Now: func() time.Time { return now }, Reads: reads.hook})
	if err != nil {
		t.Fatal(err)
	}
	return ownerFixture{control: control, store: store, reads: reads, now: &now}
}

var ownerIdentity = execution.ProgressIdentity{QueryGroup: "q"}

func ownerFence(epoch uint64) execution.OwnerFence {
	return execution.OwnerFence{QueryGroup: "q", OwnerID: "worker", OwnerEpoch: epoch, LeaseToken: "lease"}
}

func ownerCommit(slot execution.EvaluationTime, fence execution.OwnerFence) execution.ProgressCommitRequest {
	projection := progressProjectionAt(slot)
	return execution.ProgressCommitRequest{Identity: ownerIdentity, OwnerFence: fence, ExpectedNextSlot: slot, Projection: projection,
		Completion: execution.SlotCompletion{Contract: projection.Contract, Kind: execution.CompletionFull,
			Primary: &execution.PrimaryInputFact{Completeness: execution.CompletenessFull, DataState: execution.DataStateData},
			Result:  observability.ResultSuccess}}
}

// runOwnedSlot is one Slot as the owner runs it: the Next read, BeginSlot
// and CommitProgress, each required to go through.
func (fixture ownerFixture) runOwnedSlot(t *testing.T, slot execution.EvaluationTime, fence execution.OwnerFence) {
	t.Helper()
	ctx := context.Background()
	loaded, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, fence)
	if err != nil || loaded.Progress == nil || loaded.Progress.NextSlot != slot {
		t.Fatalf("Slot %d: LoadProgressFor() = (%+v, %v), want the cursor at it", slot, loaded, err)
	}
	begin, err := fixture.store.BeginSlot(ctx, execution.ProgressBeginRequest{Identity: ownerIdentity, OwnerFence: fence,
		Projection: progressProjectionAt(slot)})
	if err != nil || begin.Status != execution.ProgressCommitted {
		t.Fatalf("Slot %d: BeginSlot() = (%+v, %v), want committed", slot, begin, err)
	}
	commit, err := fixture.store.CommitProgress(ctx, ownerCommit(slot, fence))
	if err != nil || commit.Status != execution.ProgressCommitted {
		t.Fatalf("Slot %d: CommitProgress() = (%+v, %v), want committed", slot, commit, err)
	}
}

func cursorAt(slot execution.EvaluationTime) execution.ScheduleProgress {
	return execution.ScheduleProgress{Identity: ownerIdentity, NextSlot: slot}
}

// Three consecutive Slots of one owner read the key once: the first Next
// read, and every later read - Next, begin and commit - from the record the
// owner wrote. What the Slots wrote is in the store all the same.
func TestTheOwnerReadsItsProgressOnceAcrossThreeSlots(t *testing.T) {
	fixture := newOwnerFixture(t, cursorAt(60), fixedSlotResolver{interval: 60})
	for _, slot := range []execution.EvaluationTime{60, 120, 180} {
		fixture.runOwnedSlot(t, slot, ownerFence(1))
	}
	if reads := fixture.control.readCount(); reads != 1 {
		t.Fatalf("the key was read %d times over three Slots, want once", reads)
	}
	want := readCounts{{ProgressReadNext, ProgressReadRedis}: 1, {ProgressReadNext, ProgressReadCache}: 2,
		{ProgressReadBegin, ProgressReadCache}: 3, {ProgressReadCommit, ProgressReadCache}: 3}
	for key, count := range want {
		if fixture.reads[key] != count {
			t.Errorf("reads %v = %d, want %d (all %v)", key, fixture.reads[key], count, fixture.reads)
		}
	}
	stored, err := decode(fixture.control.value)
	if err != nil || stored.NextSlot != 240 || stored.UnfinishedSlot != nil {
		t.Fatalf("stored record = (%+v, %v), want the cursor past the third Slot", stored, err)
	}
}

// A record written behind the owner makes the owner's first write conflict
// on the bytes it kept; the write reads the key again and goes through in
// the same call, on what was written behind it, and the Slot begins.
func TestABeginOnARecordChangedBehindTheOwnerReadsAgainAndCommits(t *testing.T) {
	fixture := newOwnerFixture(t, cursorAt(60), fixedSlotResolver{interval: 60})
	fixture.runOwnedSlot(t, 60, ownerFence(1))
	behind, err := decode(fixture.control.value)
	if err != nil {
		t.Fatal(err)
	}
	behind.LastDataSlot = 7
	fixture.control.writeBehind(mustEncode(t, behind))
	reads := fixture.control.readCount()

	begin, err := fixture.store.BeginSlot(context.Background(), execution.ProgressBeginRequest{Identity: ownerIdentity,
		OwnerFence: ownerFence(1), Projection: progressProjectionAt(120)})
	if err != nil || begin.Status != execution.ProgressCommitted {
		t.Fatalf("BeginSlot() = (%+v, %v), want committed after reading the record again", begin, err)
	}
	if got := fixture.control.readCount() - reads; got != 1 || fixture.reads[[2]string{ProgressReadBegin, ProgressReadRedis}] != 1 {
		t.Fatalf("the begin read the key %d times (%v), want once, after its conflict", got, fixture.reads)
	}
	stored, err := decode(fixture.control.value)
	if err != nil || stored.LastDataSlot != 7 || stored.UnfinishedSlot == nil {
		t.Fatalf("stored record = (%+v, %v), want the begin written over what was written behind it", stored, err)
	}
	// And the owner goes on from what it just wrote, without another read.
	commit, err := fixture.store.CommitProgress(context.Background(), ownerCommit(120, ownerFence(1)))
	if err != nil || commit.Status != execution.ProgressCommitted || fixture.control.readCount()-reads != 1 {
		t.Fatalf("CommitProgress() = (%+v, %v) after %d reads, want committed from the owner's record", commit, err,
			fixture.control.readCount()-reads)
	}
}

// A cursor set back behind the owner - which no production writer does - is
// seen within the bound: by the eleventh Next read at the latest, or by the
// first after five minutes since the key was last read.
func TestARewindBehindTheOwnerIsSeenWithinTheBound(t *testing.T) {
	ctx := context.Background()
	t.Run("rounds", func(t *testing.T) {
		fixture := newOwnerFixture(t, cursorAt(120), fixedSlotResolver{interval: 60})
		for read := 1; read <= ownerRecordRounds+1; read++ {
			loaded, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, ownerFence(1))
			if err != nil || loaded.Progress == nil {
				t.Fatalf("read %d: LoadProgressFor() = (%+v, %v)", read, loaded, err)
			}
			if read == 1 {
				fixture.control.writeBehind(mustEncode(t, cursorAt(60)))
				continue
			}
			if want := execution.EvaluationTime(120); read == ownerRecordRounds+1 {
				if loaded.Progress.NextSlot != 60 {
					t.Fatalf("read %d: cursor %d, want the rewind to 60 seen", read, loaded.Progress.NextSlot)
				}
			} else if loaded.Progress.NextSlot != want {
				t.Fatalf("read %d: cursor %d, want the owner's record (%d) inside the bound", read, loaded.Progress.NextSlot, want)
			}
		}
		if reads := fixture.control.readCount(); reads != 2 {
			t.Fatalf("the key was read %d times over %d Next reads, want twice", reads, ownerRecordRounds+1)
		}
	})
	t.Run("age", func(t *testing.T) {
		fixture := newOwnerFixture(t, cursorAt(120), fixedSlotResolver{interval: 60})
		if _, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, ownerFence(1)); err != nil {
			t.Fatal(err)
		}
		fixture.control.writeBehind(mustEncode(t, cursorAt(60)))
		*fixture.now = fixture.now.Add(ownerRecordAge - time.Second)
		if loaded, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, ownerFence(1)); err != nil || loaded.Progress.NextSlot != 120 {
			t.Fatalf("inside the age: LoadProgressFor() = (%+v, %v), want the owner's record", loaded, err)
		}
		*fixture.now = fixture.now.Add(time.Second)
		if loaded, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, ownerFence(1)); err != nil || loaded.Progress.NextSlot != 60 {
			t.Fatalf("at the age: LoadProgressFor() = (%+v, %v), want the rewind read from the key", loaded, err)
		}
	})
}

// A timeline the Leader rewrote - a repair that moved the successor of the
// last completed Slot - with the record untouched is run the next round
// without reading the key: the begin works the Slot out from the owner's
// record and the timeline as it is now.
func TestATimelineRepairIsRunTheNextRoundWithoutAProgressRead(t *testing.T) {
	slots := mappedSlotResolver{60: 120}
	current := execution.ScheduleProgress{Identity: ownerIdentity, NextSlot: 120, LastFullSlot: 60, LastCompletionKind: execution.CompletionFull}
	fixture := newOwnerFixture(t, current, slots)
	if _, err := fixture.store.LoadProgressFor(context.Background(), ownerIdentity, ownerFence(1)); err != nil {
		t.Fatal(err)
	}
	slots[60], slots[90] = 90, 150
	begin, err := fixture.store.BeginSlot(context.Background(), execution.ProgressBeginRequest{Identity: ownerIdentity,
		OwnerFence: ownerFence(1), Projection: progressProjectionAt(90)})
	if err != nil || begin.Status != execution.ProgressCommitted {
		t.Fatalf("BeginSlot(repaired successor) = (%+v, %v), want committed", begin, err)
	}
	if reads := fixture.control.readCount(); reads != 1 {
		t.Fatalf("the key was read %d times, want only the first Next read", reads)
	}
	stored, err := decode(fixture.control.value)
	if err != nil || stored.NextSlot != 90 || stored.UnfinishedSlot == nil {
		t.Fatalf("stored record = (%+v, %v), want the repaired Slot begun", stored, err)
	}
}

// The owner's record answers only the fence it was kept under: a new epoch
// reads the key, and so does a call after the Query Group is released
// (Forget) or after a write the fence refused.
func TestTheOwnersRecordAnswersOnlyItsFenceAndGoesWithTheRelease(t *testing.T) {
	ctx := context.Background()
	fixture := newOwnerFixture(t, cursorAt(60), fixedSlotResolver{interval: 60})
	load := func(epoch uint64) {
		t.Helper()
		if _, err := fixture.store.LoadProgressFor(ctx, ownerIdentity, ownerFence(epoch)); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(step string, reads int) {
		t.Helper()
		if got := fixture.control.readCount(); got != reads {
			t.Fatalf("%s: the key was read %d times, want %d", step, got, reads)
		}
	}
	load(1)
	load(1)
	expect("same fence", 1)
	load(2)
	expect("new epoch", 2)
	load(2)
	expect("new epoch again", 2)
	// The old epoch's write starts from the key, not from the new epoch's record.
	if begin, err := fixture.store.BeginSlot(ctx, execution.ProgressBeginRequest{Identity: ownerIdentity, OwnerFence: ownerFence(1),
		Projection: progressProjectionAt(60)}); err != nil || begin.Status != execution.ProgressCommitted {
		t.Fatalf("BeginSlot(old epoch) = (%+v, %v)", begin, err)
	}
	expect("old epoch's begin", 3)
	fixture.store.Forget(ownerIdentity.QueryGroup)
	load(1)
	expect("after release", 4)
	fixture.control.refuse = ownership.FencedCASStaleOwner
	if begin, err := fixture.store.BeginSlot(ctx, execution.ProgressBeginRequest{Identity: ownerIdentity, OwnerFence: ownerFence(1),
		Projection: progressProjectionAt(60)}); err != nil || begin.Status != execution.ProgressStaleOwner {
		t.Fatalf("BeginSlot(refused) = (%+v, %v), want stale owner", begin, err)
	}
	expect("refused begin, from the record", 4)
	load(1)
	expect("after a refused write", 5)
}

// A projection the attempt supersedes is reported once though the write is
// made twice: on the owner's record, which a write behind it made stale, and
// again on the key it then read. Reported from each pass, the redo would
// count one superseded projection as two.
func TestASupersededProjectionIsReportedOnceAcrossARedo(t *testing.T) {
	persisted := progressProjectionAt(120)
	current := execution.ScheduleProgress{Identity: ownerIdentity, NextSlot: 120, LastFullSlot: 60,
		LastCompletionKind: execution.CompletionFull, UnfinishedSlot: &persisted}
	control := &ownerControl{value: mustEncode(t, current)}
	var observations []observability.Observation
	now := time.Unix(1_800_000_000, 0)
	store, err := NewStore(StoreOptions{Prefix: "alarmd", Control: control, Slots: fixedSlotResolver{interval: 60},
		Now: func() time.Time { return now }, Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			observations = append(observations, observation)
		})})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.LoadProgressFor(ctx, ownerIdentity, ownerFence(1)); err != nil {
		t.Fatal(err)
	}
	behind := current
	behind.LastDataSlot = 7
	control.writeBehind(mustEncode(t, behind))
	superseding := progressProjectionAt(120)
	superseding.EarliestQueryDeadlineUnixMilli += 5_000
	begin, err := store.BeginSlot(ctx, execution.ProgressBeginRequest{Identity: ownerIdentity, OwnerFence: ownerFence(1), Projection: superseding})
	if err != nil || begin.Status != execution.ProgressCommitted {
		t.Fatalf("BeginSlot() = (%+v, %v), want committed after reading the record again", begin, err)
	}
	drift := 0
	for _, observation := range observations {
		if observation.ReasonCode == observability.ReasonCode(contract.ReasonConfigDrift) {
			drift++
		}
	}
	if drift != 1 {
		t.Fatalf("%d superseded-projection observations, want one for the one projection superseded", drift)
	}
}
