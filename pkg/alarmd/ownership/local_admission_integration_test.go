// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package ownership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// localAdmissionRig is a lease taken under a worker session on a real store,
// confirmed once by a fence check, and the view entry a Leader would send
// for it: the record that check read, with the instant only a fence check
// knows left out.
type localAdmissionRig struct {
	store     *RedisStore
	authority PublicationAuthority
	holder    *heldWorkerSession
	session   *Session
	worker    WorkerSession
	view      LocalOwnership
	record    AssignmentRecord
	at        time.Time
}

func newLocalAdmissionRig(t *testing.T) *localAdmissionRig {
	t.Helper()
	store := newIntegrationStore(t)
	ctx := context.Background()
	authority, _ := publishTo(t, store, "worker-1")
	// The worker session outlives the lease's TTL, so the lease's own
	// horizon is what Deadline says.
	worker, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	holder := &heldWorkerSession{}
	holder.set(worker, true)
	opened := time.Now()
	session, err := OpenSessionUnder(ctx, store, sessionLeaseQueryGroup, holder.current, opened, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	confirmedAt := opened.Add(time.Millisecond)
	_, record, err := session.ValidateCurrentWithAssignment(ctx, confirmedAt)
	if err != nil {
		t.Fatal(err)
	}
	sent := record
	sent.ObservedAt = time.Time{}
	return &localAdmissionRig{store: store, authority: authority, holder: holder, session: session, worker: worker, record: record,
		view: LocalOwnership{Named: true, Complete: true, Record: sent, HeardWithin: true}, at: confirmedAt.Add(time.Second)}
}

// A lease confirmed by a fence check, and a fresh view naming its holder
// desired at the generation that check read, with nothing pending: the
// attempt is admitted from them with the lease's fence and the view's
// record, and the store is asked nothing - nor is the lease's deadline moved:
// only a fence check that reads the record moves it, so a holder whose view
// is behind a move never extends its own lease past the move's grace (015
// R1, section 2.1).
func TestALeaseConfirmedOnceIsAdmittedFromAFreshView(t *testing.T) {
	rig := newLocalAdmissionRig(t)
	lease, _ := rig.session.Current()
	deadline := rig.session.Deadline()
	fence, record, reason, err := rig.session.ValidateLocally(rig.at, rig.view)
	if err != nil || reason != LocalCheckPass {
		t.Fatalf("ValidateLocally = (%q, %v), want the pass", reason, err)
	}
	if fence != lease.Fence || record != rig.view.Record {
		t.Fatalf("admitted fence %+v record %+v, want the lease's fence %+v and the view's record", fence, record, lease.Fence)
	}
	if record.Validate() != nil {
		t.Fatalf("the record admitted does not validate: %v", record.Validate())
	}
	if !rig.session.Deadline().Equal(deadline) {
		t.Fatalf("a local admission moved the deadline from %v to %v; only a fence check may", deadline, rig.session.Deadline())
	}
	if after, _ := rig.session.Current(); !after.Deadline.Equal(lease.Deadline) {
		t.Fatalf("a local admission moved the lease's horizon from %v to %v; only a fence check may", lease.Deadline, after.Deadline)
	}
}

// Every condition that does not hold sends the attempt to the store, under
// its own word; each cell holds the others, so the word is the cell's.
func TestEachLocalConditionThatDoesNotHoldSendsTheAttemptToTheStore(t *testing.T) {
	for _, cell := range []struct {
		name   string
		change func(*LocalOwnership)
		want   string
	}{
		{"not in the view", func(view *LocalOwnership) { view.Named = false }, LocalCheckNotInView},
		{"a Leader that sends no record fields", func(view *LocalOwnership) { view.Complete = false }, LocalCheckViewFieldsMissing},
		{"a record that does not validate", func(view *LocalOwnership) { view.Record.AssignedAt = time.Time{} }, LocalCheckViewFieldsMissing},
		{"another Query Group's record", func(view *LocalOwnership) { view.Record.QueryGroup = "query-group-other" }, LocalCheckNotInView},
		{"desired elsewhere", func(view *LocalOwnership) { view.Record.DesiredWorkerID = "worker-2" }, LocalCheckNotDesired},
		{"a newer generation", func(view *LocalOwnership) { view.Record.AssignmentGeneration++ }, LocalCheckGenerationChanged},
		{"a content change pending", func(view *LocalOwnership) {
			view.Record.PendingContentScope, view.Record.EffectiveAt = "scope-next", time.Now().Add(time.Minute)
		}, LocalCheckPendingContent},
		{"a content scope the lease was not confirmed with", func(view *LocalOwnership) { view.Record.ContentScope = "scope-moved" }, LocalCheckUnconfirmedRecord},
		{"a timeline the lease was not confirmed with", func(view *LocalOwnership) { view.Record.TimelineRecordRevision++ }, LocalCheckUnconfirmedRecord},
		{"a Leader not heard from", func(view *LocalOwnership) { view.HeardWithin = false }, LocalCheckViewSilent},
	} {
		t.Run(cell.name, func(t *testing.T) {
			rig := newLocalAdmissionRig(t)
			if _, _, reason, err := rig.session.ValidateLocally(rig.at, rig.view); err != nil || reason != LocalCheckPass {
				t.Fatalf("the rig before the change = (%q, %v), want the pass", reason, err)
			}
			view := rig.view
			cell.change(&view)
			fence, record, reason, err := rig.session.ValidateLocally(rig.at, view)
			if err != nil || reason != cell.want {
				t.Fatalf("ValidateLocally = (%q, %v), want %q", reason, err, cell.want)
			}
			if fence.LeaseToken != "" || record.QueryGroup != "" {
				t.Fatalf("a check sent to the store came back with fence %+v record %+v, want neither", fence, record)
			}
		})
	}
}

// A lease no fence check has confirmed yet - the first round after it was
// taken - goes to the store; that check confirms it, and the next is local.
func TestALeaseNoFenceCheckConfirmedGoesToTheStoreFirst(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	_, _ = publishTo(t, store, "worker-1")
	worker, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	holder := &heldWorkerSession{}
	holder.set(worker, true)
	opened := time.Now()
	session, err := OpenSessionUnder(ctx, store, sessionLeaseQueryGroup, holder.current, opened, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.ReadAssignment(ctx, sessionLeaseQueryGroup)
	if err != nil {
		t.Fatal(err)
	}
	record.ObservedAt = time.Time{}
	view := LocalOwnership{Named: true, Complete: true, Record: record, HeardWithin: true}
	if _, _, reason, err := session.ValidateLocally(opened.Add(time.Millisecond), view); err != nil || reason != LocalCheckNeverConfirmed {
		t.Fatalf("before any fence check = (%q, %v), want %q", reason, err, LocalCheckNeverConfirmed)
	}
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, opened.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, reason, err := session.ValidateLocally(opened.Add(3*time.Millisecond), view); err != nil || reason != LocalCheckPass {
		t.Fatalf("after the fence check = (%q, %v), want the pass", reason, err)
	}
}

// A write whose own fence refused the lease sends the attempts after it to
// the store, and a check that began before the refusal does not take that
// back: only one that began after it does.
func TestAFenceRefusalSendsTheAttemptsAfterItToTheStore(t *testing.T) {
	rig := newLocalAdmissionRig(t)
	ctx := context.Background()
	refusedAt := rig.at.Add(time.Second)
	rig.session.NoteFenceRefused(refusedAt)
	if _, _, reason, err := rig.session.ValidateLocally(refusedAt.Add(time.Millisecond), rig.view); err != nil || reason != LocalCheckFenceRefused {
		t.Fatalf("after the refusal = (%q, %v), want %q", reason, err, LocalCheckFenceRefused)
	}
	// Began before the refusal, answered after it.
	if _, _, err := rig.session.ValidateCurrentWithAssignment(ctx, refusedAt.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, reason, _ := rig.session.ValidateLocally(refusedAt.Add(2*time.Millisecond), rig.view); reason != LocalCheckFenceRefused {
		t.Fatalf("after a check that began before the refusal = %q, want still %q", reason, LocalCheckFenceRefused)
	}
	if _, _, err := rig.session.ValidateCurrentWithAssignment(ctx, refusedAt.Add(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, reason, err := rig.session.ValidateLocally(refusedAt.Add(4*time.Millisecond), rig.view); err != nil || reason != LocalCheckPass {
		t.Fatalf("after a check that began after the refusal = (%q, %v), want the pass", reason, err)
	}
}

// A move away and back - A to B to A - is a new generation of the record:
// the holder that was confirmed under the first sees the third as a change
// and goes to the store, whatever its desired worker says.
func TestAMoveAwayAndBackGoesToTheStore(t *testing.T) {
	rig := newLocalAdmissionRig(t)
	ctx := context.Background()
	away, err := rig.store.PublishAssignment(ctx, rig.authority, AssignmentDecision{
		QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: "worker-2", HolderWorkerID: "worker-1",
		ExpectedRecordRevision: rig.record.RecordRevision, PlacementReason: PlacementRebalance, DecidedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	back, err := rig.store.PublishAssignment(ctx, rig.authority, AssignmentDecision{
		QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: "worker-1", HolderWorkerID: "worker-2",
		ExpectedRecordRevision: away.RecordRevision, PlacementReason: PlacementRebalance, DecidedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if back.DesiredWorkerID != "worker-1" || back.AssignmentGeneration == rig.record.AssignmentGeneration {
		t.Fatalf("the record back = %+v, want worker-1 under a generation other than %d", back, rig.record.AssignmentGeneration)
	}
	view := rig.view
	view.Record = back
	view.Record.ObservedAt = time.Time{}
	if _, _, reason, err := rig.session.ValidateLocally(rig.at, view); err != nil || reason != LocalCheckGenerationChanged {
		t.Fatalf("a view of the move back = (%q, %v), want %q", reason, err, LocalCheckGenerationChanged)
	}
}

// A lease its holder renews itself, and one whose worker session is gone:
// the first goes to the store as it always did; the second is refused
// before any store is asked, as the store path refuses it.
func TestALeaseNotUnderASessionOrWithoutOneIsNotAdmittedLocally(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	_, _ = publishTo(t, store, "worker-1")
	own, err := OpenSession(ctx, store, sessionLeaseQueryGroup, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, reason, err := own.ValidateLocally(time.Now(), LocalOwnership{Named: true, Complete: true, HeardWithin: true}); err != nil || reason != LocalCheckOwnLease {
		t.Fatalf("a lease its holder renews = (%q, %v), want %q", reason, err, LocalCheckOwnLease)
	}

	rig := newLocalAdmissionRig(t)
	rig.holder.set(rig.worker, false)
	_, _, reason, err := rig.session.ValidateLocally(rig.at, rig.view)
	if !errors.Is(err, ErrStaleFence) || reason != LocalCheckLeaseRefused {
		t.Fatalf("a lease whose worker session is gone = (%q, %v), want %q with a stale fence", reason, err, LocalCheckLeaseRefused)
	}
	if _, _, storeErr := rig.session.ValidateCurrentWithAssignment(ctx, rig.at); !errors.Is(storeErr, ErrStaleFence) {
		t.Fatalf("the store path for the same lease = %v, want the same refusal", storeErr)
	}
}

// The dual-owner window (015 R1, section 2.1): the Leader moves the Query
// Group from A to B, and A's view never hears of it - A goes on being
// admitted from it, its worker session renewed as its maintainer renews it.
// B takes the lease once the move's grace has ended. A's output authority -
// the deadline a batch is admitted against (execution.LeaseAuthority) - is
// never past the moment B holds the lease: a local admission moves nothing,
// so it ends where A's last fence check put it. And once B holds it, A's
// fence is refused by the store - not desired, or stale - which ends A's
// admission. On a real store,
// the lease's TTL and the sessions' brought down to seconds so the grace
// ends within the case.
func TestAHolderWhoseViewMissedAMoveNeverHoldsPastItsTakeover(t *testing.T) {
	store := newIntegrationStore(t)
	const leaseTTL, sessionTTL = 2 * time.Second, 3 * time.Second
	if err := store.ConfigureLeaseTTL(leaseTTL); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	authority, _ := publishTo(t, store, "worker-1")
	holderA := &heldWorkerSession{}
	workerA, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), sessionTTL)
	if err != nil {
		t.Fatal(err)
	}
	holderA.set(workerA, true)
	session, err := OpenSessionUnder(ctx, store, sessionLeaseQueryGroup, holderA.current, time.Now(), leaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	_, record, err := session.ValidateCurrentWithAssignment(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// The view A holds, from before the move.
	before := record
	before.ObservedAt = time.Time{}
	view := LocalOwnership{Named: true, Complete: true, Record: before, HeardWithin: true}
	workerB, err := store.AcquireWorkerSession(ctx, "worker-2", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	movedAt := time.Now()
	if _, err := store.PublishAssignment(ctx, authority, AssignmentDecision{QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: "worker-2",
		HolderWorkerID: "worker-1", ExpectedRecordRevision: record.RecordRevision, PlacementReason: PlacementRebalance, DecidedAt: movedAt}); err != nil {
		t.Fatal(err)
	}
	var takenAt, latest time.Time
	admitted, taken := 0, false
	giveUp := testwait.Deadline(t)
	for !taken {
		if time.Now().After(giveUp) {
			t.Fatalf("B did not take the lease by the test's wait bound after the move")
		}
		renewed, err := store.RenewWorkerSession(ctx, workerA, time.Now(), sessionTTL)
		if err != nil {
			t.Fatal(err)
		}
		workerA = renewed
		holderA.set(workerA, true)
		if _, _, reason, err := session.ValidateLocally(time.Now(), view); err == nil && reason == LocalCheckPass {
			admitted++
		}
		if deadline := session.Deadline(); deadline.After(latest) {
			latest = deadline
		}
		if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, workerB, time.Now(), time.Minute); err == nil {
			takenAt, taken = time.Now(), true
		} else if !errors.Is(err, ErrLeaseBusy) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // fixed-wait: the pace of A's attempts and B's tries while the grace runs; the case ends when B takes the lease
	}
	if admitted == 0 {
		t.Fatal("A was never admitted from its view while the grace ran; the case shows nothing")
	}
	if latest.After(takenAt) {
		t.Fatalf("A's output authority reached %v, past B taking the lease at %v (moved at %v): A would send past its takeover",
			latest, takenAt, movedAt)
	}
	lease, _ := session.Current()
	if _, err := store.CheckFenceWithAssignment(ctx, lease.Fence); !IsLeaseDecision(err) {
		t.Fatalf("A's fence once B holds the lease = %v, want the store's refusal of it", err)
	}
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, time.Now()); !IsLeaseDecision(err) {
		t.Fatalf("A's next check with the store = %v, want its fence refused", err)
	}
	if session.Alive(time.Now()) {
		t.Fatal("A's session still admits work after the store refused its fence")
	}
	t.Logf("A admitted %d times from its view; its authority ended at %v, B took the lease %v after the move",
		admitted, latest.Sub(movedAt).Round(time.Millisecond), takenAt.Sub(movedAt).Round(time.Millisecond))
}
