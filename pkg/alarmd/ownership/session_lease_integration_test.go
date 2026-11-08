// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.
package ownership

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

const sessionLeaseQueryGroup = execution.QueryGroupIdentity("query-group-session")

// heldWorkerSession is a worker session a test can replace or drop.
type heldWorkerSession struct {
	mu      sync.Mutex
	session WorkerSession
	held    bool
}

func (holder *heldWorkerSession) set(session WorkerSession, held bool) {
	holder.mu.Lock()
	defer holder.mu.Unlock()
	holder.session, holder.held = session, held
}

func (holder *heldWorkerSession) current() (WorkerSession, bool) {
	holder.mu.Lock()
	defer holder.mu.Unlock()
	return holder.session, holder.held
}

func publishTo(t *testing.T, store *RedisStore, worker string) (PublicationAuthority, AssignmentRecord) {
	t.Helper()
	ctx := context.Background()
	authority, err := store.AcquireControlLeader(ctx, "control-1", time.Now(), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: worker, PlacementReason: PlacementRendezvous, DecidedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return authority, record
}

func ownershipFields(t *testing.T, store *RedisStore) map[string]string {
	t.Helper()
	fields, err := store.client.HGetAll(context.Background(), store.ownershipKey(sessionLeaseQueryGroup)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

// A lease taken under a session names the session and the TTL the Leader
// published, and has no deadline of its own; it passes the fence with none,
// and its release takes the session fields with it.
func TestALeaseTakenUnderASessionHasNoDeadlineOfItsOwn(t *testing.T) {
	store := newIntegrationStore(t)
	if err := store.ConfigureLeaseTTL(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	publishTo(t, store, "worker-1")
	session, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// A lapsed deadline left by a lease its holder renewed itself is not
	// carried into the lease taken under the session.
	if err := store.client.HSet(ctx, store.ownershipKey(sessionLeaseQueryGroup), "deadline_ms", "1").Err(); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, session, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.SessionEpoch != session.Epoch || lease.TTL != 20*time.Second {
		t.Fatalf("lease = %+v, want it under epoch %d by the published TTL 20s", lease, session.Epoch)
	}
	fields := ownershipFields(t, store)
	if _, has := fields["deadline_ms"]; has || fields["owner_session_token"] != session.Token || fields["owner_lease_ttl_ms"] != "20000" {
		t.Fatalf("ownership = %v, want the session's fields and no deadline", fields)
	}
	if err := store.CheckFence(ctx, lease.Fence); err != nil {
		t.Fatalf("CheckFence() = %v, want a lease with no deadline to pass", err)
	}
	if err := store.Release(ctx, lease.Fence); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"owner_session_epoch", "owner_session_token", "owner_lease_ttl_ms"} {
		if _, has := ownershipFields(t, store)[field]; has {
			t.Fatalf("%s left after the release", field)
		}
	}
}

// The same worker takes its Query Group back from a session that ended before
// its own, and from the very process that holds it; another worker is kept
// out only while a move's grace protects the holder.
func TestWhoMayTakeALeaseHeldUnderASession(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	authority, _ := publishTo(t, store, "worker-1")
	first, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, first, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, first, time.Now(), time.Minute); err != nil {
		t.Fatalf("the same process taking it back = %v, want it granted", err)
	}
	impostor := first
	impostor.Token = "another-process"
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, impostor, time.Now(), time.Minute); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("another process under the same epoch = %v, want ErrLeaseBusy", err)
	}
	lapseWorkerSession(t, store, "worker-1")
	second, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, second, time.Now(), time.Minute); err != nil {
		t.Fatalf("the worker's next session taking it back = %v, want it granted", err)
	}

	other, err := store.AcquireWorkerSession(ctx, "worker-2", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.ReadAssignment(ctx, sessionLeaseQueryGroup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishAssignment(ctx, authority, AssignmentDecision{QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: "worker-2",
		HolderWorkerID: "worker-1", ExpectedRecordRevision: current.RecordRevision, PlacementReason: PlacementRebalance, DecidedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, other, time.Now(), time.Minute); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("another worker inside the grace = %v, want ErrLeaseBusy", err)
	}
	if err := store.client.HSet(ctx, store.assignmentKey(sessionLeaseQueryGroup), "effective_at_ms", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireUnder(ctx, sessionLeaseQueryGroup, other, time.Now(), time.Minute); err != nil {
		t.Fatalf("another worker once the grace ended = %v, want it granted", err)
	}
}

// A Session under a worker session acts until the earliest of its session's
// deadline, its last confirmation's horizon and a change pending for it; a
// session that is no longer the one it was taken under ends it at once; and
// a horizon that ran out while it was idle leaves it alive, so its next fence
// check is the confirmation that moves it on.
func TestASessionUnderAWorkerSessionActsUntilTheEarliestOfItsBounds(t *testing.T) {
	store := newIntegrationStore(t)
	if err := store.ConfigureLeaseTTL(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	authority, _ := publishTo(t, store, "worker-1")
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
	if deadline := session.Deadline(); deadline.Before(opened.Add(time.Second)) || deadline.After(opened.Add(3*time.Second)) {
		t.Fatalf("Deadline() = %v, want about the TTL from opening", deadline)
	}

	// An answer from a check that began before the one in hand changes nothing.
	later := time.Now().Add(time.Second)
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, later); err != nil {
		t.Fatal(err)
	}
	confirmed := session.Deadline()
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, opened.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !session.Deadline().Equal(confirmed) {
		t.Fatalf("an older confirmation moved the deadline from %v to %v", confirmed, session.Deadline())
	}

	// The worker session caps it.
	capped := worker
	capped.Deadline = later.Add(500 * time.Millisecond)
	holder.set(capped, true)
	if !session.Deadline().Equal(capped.Deadline) {
		t.Fatalf("Deadline() = %v, want the worker session's %v", session.Deadline(), capped.Deadline)
	}
	holder.set(worker, true)

	// A move caps a draining holder at the move's effective time.
	if _, err := store.AcquireWorkerSession(ctx, "worker-2", time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	current, err := store.ReadAssignment(ctx, sessionLeaseQueryGroup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishAssignment(ctx, authority, AssignmentDecision{QueryGroup: sessionLeaseQueryGroup, DesiredWorkerID: "worker-2",
		HolderWorkerID: "worker-1", ExpectedRecordRevision: current.RecordRevision, PlacementReason: PlacementRebalance, DecidedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, ok := func() (string, bool) {
		value, err := store.client.HGet(ctx, store.assignmentKey(sessionLeaseQueryGroup), "effective_at_ms").Result()
		return value, err == nil
	}(); !ok {
		t.Fatal("the move gave the live holder no grace")
	}
	// Taken a while after the move -- its effective time a second away on
	// the server -- a check caps the draining holder there, short of the TTL
	// its confirmation would otherwise give.
	effective := serverNow(t, store).Add(time.Second)
	if err := store.client.HSet(ctx, store.assignmentKey(sessionLeaseQueryGroup), "effective_at_ms", effective.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	checkedAt := time.Now().Add(2 * time.Second)
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, checkedAt); err != nil {
		t.Fatalf("the draining holder's check = %v, want it valid inside the grace", err)
	}
	if deadline := session.Deadline(); !deadline.Before(checkedAt.Add(1500*time.Millisecond)) || deadline.Before(checkedAt.Add(500*time.Millisecond)) {
		t.Fatalf("Deadline() = %v, want it capped about a second after the check at %v, short of its 2s horizon", deadline, checkedAt)
	}

	// A session that is not the one the lease was taken under ends it.
	replaced := worker
	replaced.Epoch++
	holder.set(replaced, true)
	if !session.Deadline().IsZero() || session.Alive(time.Now()) {
		t.Fatalf("under another session: Deadline() = %v Alive = %t, want ended", session.Deadline(), session.Alive(time.Now()))
	}
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, time.Now()); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("a check under another session = %v, want ErrStaleFence", err)
	}
}

// A lease that went quiet past its confirmation's horizon is still alive:
// its next fence check confirms it, and only output stops at the horizon.
func TestAnIdleLeaseUnderASessionIsConfirmedByItsNextCheck(t *testing.T) {
	store := newIntegrationStore(t)
	if err := store.ConfigureLeaseTTL(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	publishTo(t, store, "worker-1")
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
	idle := opened.Add(5 * time.Second)
	if session.Deadline().After(idle) || !session.Alive(idle) {
		t.Fatalf("idle past the horizon: Deadline() = %v Alive = %t, want the horizon passed and the lease alive", session.Deadline(), session.Alive(idle))
	}
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, idle); err != nil {
		t.Fatalf("the next check = %v, want it to confirm the lease", err)
	}
	if !session.Deadline().After(idle) {
		t.Fatalf("Deadline() = %v after the confirmation at %v, want it moved on", session.Deadline(), idle)
	}
}

// A lease held under a session ends with the session as the session's own
// loop decides: a deadline that passed on this process's clock, the session
// still held under the same epoch, is not an end -- the next renewal may
// restate it -- while a session let go or replaced is.
func TestALeaseUnderASessionEndsOnlyWhenTheSessionDoes(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	publishTo(t, store, "worker-1")
	worker, err := store.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	holder := &heldWorkerSession{}
	holder.set(worker, true)
	session, err := OpenSessionUnder(ctx, store, sessionLeaseQueryGroup, holder.current, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stale := worker
	stale.Deadline = time.Now().Add(-time.Second)
	holder.set(stale, true)
	if session.SessionEnded() || session.Alive(time.Now()) {
		t.Fatalf("a session past its local deadline: ended=%t alive=%t, want not ended and not alive", session.SessionEnded(), session.Alive(time.Now()))
	}
	holder.set(worker, false)
	if !session.SessionEnded() {
		t.Fatal("a session let go did not end the lease")
	}
	replaced := worker
	replaced.Epoch++
	holder.set(replaced, true)
	if !session.SessionEnded() {
		t.Fatal("a replaced session did not end the lease")
	}
}

// callerRecordingStore is the store with the job each fence check names.
type callerRecordingStore struct {
	*RedisStore
	callers []string
}

func (store *callerRecordingStore) CheckFenceWithAssignment(ctx context.Context, fence execution.OwnerFence) (AssignmentRecord, error) {
	store.callers = append(store.callers, redisfailure.Caller(ctx))
	return store.RedisStore.CheckFenceWithAssignment(ctx, fence)
}

// A confirmation names its fence check lease_confirmation, whichever way it is
// asked for: the view gate's early renewal and a long Slot's output. A round's
// own check confirms the lease too, and names no job of its own, so the store's
// client counts it as the ownership store's, and the confirmations can be read
// apart from it.
func TestAConfirmationNamesItsFenceCheckAndARoundsCheckDoesNot(t *testing.T) {
	redisStore := newIntegrationStore(t)
	if err := redisStore.ConfigureLeaseTTL(time.Second); err != nil {
		t.Fatal(err)
	}
	store := &callerRecordingStore{RedisStore: redisStore}
	ctx := context.Background()
	publishTo(t, redisStore, "worker-1")
	worker, err := redisStore.AcquireWorkerSession(ctx, "worker-1", time.Now(), time.Minute)
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
	store.callers = nil
	if err := session.Renew(ctx, opened.Add(100*time.Millisecond), time.Minute); err != nil {
		t.Fatalf("the gate's renewal = %v", err)
	}
	if err := session.Confirm(ctx, opened.Add(200*time.Millisecond)); err != nil {
		t.Fatalf("the output's confirmation = %v", err)
	}
	if _, _, err := session.ValidateCurrentWithAssignment(ctx, opened.Add(300*time.Millisecond)); err != nil {
		t.Fatalf("the round's check = %v", err)
	}
	want := []string{redisfailure.CallerLeaseConfirmation, redisfailure.CallerLeaseConfirmation, ""}
	if !reflect.DeepEqual(store.callers, want) {
		t.Fatalf("fence checks named %q, want %q", store.callers, want)
	}
}
