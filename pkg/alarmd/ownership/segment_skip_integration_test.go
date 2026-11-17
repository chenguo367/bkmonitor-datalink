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
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// previousCheckFenceLua is the fence check of the release before the
// segment skip word (dffa8d5e6), after the shared FenceLua: what a worker of
// that release runs before each Slot. Frozen: it is the old binary.
const previousCheckFenceLua = `
local require_assignment = ARGV[1]
local owner_id = ARGV[2]
local epoch = ARGV[3]
local token = ARGV[4]
local content_scope = ARGV[5] or ''
local now_ms = redis_now_ms()
local empty = {'', '', '', '', '', '', '', '', '', ''}
local record_fields = {'desired_worker_id', 'assignment_generation',
  'record_revision', 'control_epoch', 'placement_reason', 'assigned_at_ms',
  'content_scope', 'pending_content_scope', 'effective_at_ms', 'timeline_record_revision'}
local refusal, facts = fence_refusal(KEYS[1], KEYS[2], require_assignment, owner_id, epoch, token, content_scope, now_ms, record_fields)
if refusal then return {refusal, empty[1], empty[2], empty[3], empty[4], empty[5], empty[6], empty[7], empty[8], empty[9], empty[10]} end
local record = {'', '', '', '', '', '', '', '', '', ''}
if require_assignment == '1' then
  for index, name in ipairs(record_fields) do
    if facts[name] then record[index] = facts[name] end
  end
end
return {'VALID', record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], now_ms}
`

// The segment skip word is the epoch of the Leader that set it, written from
// the fence the publish script checked; it travels with the fence check a
// Slot makes; the release before reads the record as it did; a decision
// naming neither setting nor withdrawal leaves it; withdrawal clears it;
// none of these moves the record's revision; and a decision cannot ask for
// both.
func TestTheSegmentSkipWordIsTheSettingLeadersEpochAndTravelsWithTheFence(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)
	authority, err := store.AcquireControlLeader(ctx, "control-1", now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	epoch := authority.Fence.OwnerEpoch
	placed, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-skip", DesiredWorkerID: "worker-1", PlacementReason: PlacementRendezvous, DecidedAt: now,
	})
	if err != nil || placed.SegmentSkip != 0 {
		t.Fatalf("a placement naming no word = (%+v, %v), want none", placed, err)
	}
	set, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-skip", DesiredWorkerID: "worker-1", ExpectedRecordRevision: placed.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(time.Second), SetSegmentSkip: true,
	})
	if err != nil || set.SegmentSkip != epoch || set.RecordRevision != placed.RecordRevision {
		t.Fatalf("setting the word = (%+v, %v), want the Leader's epoch %d with the revision unmoved", set, err, epoch)
	}
	if read, err := store.ReadAssignment(ctx, "qg-skip"); err != nil || read.SegmentSkip != epoch {
		t.Fatalf("ReadAssignment() = (%+v, %v), want the word %d", read, err, epoch)
	}
	lease, err := store.Acquire(ctx, "qg-skip", "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fenced, err := store.CheckFenceWithAssignment(ctx, lease.Fence)
	if err != nil || fenced.SegmentSkip != epoch || fenced.ObservedAt.IsZero() {
		t.Fatalf("the Slot's fence check = (%+v, %v), want the word %d beside the server instant", fenced, err, epoch)
	}

	// The release before reads the record as it always did: its fence check
	// answers VALID with the elements it knows, and it decodes them.
	reply, err := redis.NewScript(FenceLua+previousCheckFenceLua).Run(ctx, store.client,
		[]string{store.assignmentKey("qg-skip"), store.ownershipKey("qg-skip")},
		"1", lease.Fence.OwnerID, lease.Fence.OwnerEpoch, lease.Fence.LeaseToken, "").Result()
	if err != nil {
		t.Fatal(err)
	}
	values, err := scriptValues(reply, 11)
	if err != nil || scriptText(values[0]) != "VALID" || len(values) != 12 {
		t.Fatalf("the previous release's fence check = (%v, %v), want VALID and its twelve elements", reply, err)
	}
	previous, err := assignmentFromReply("qg-skip", values[1:7], values[7:11])
	if err != nil || previous.DesiredWorkerID != "worker-1" || previous.RecordRevision != set.RecordRevision {
		t.Fatalf("the previous release decoded (%+v, %v), want the record", previous, err)
	}

	kept, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-skip", DesiredWorkerID: "worker-1", ExpectedRecordRevision: set.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(2 * time.Second),
	})
	if err != nil || kept.SegmentSkip != epoch {
		t.Fatalf("a decision naming neither = (%+v, %v), want the word %d kept", kept, err, epoch)
	}
	cleared, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-skip", DesiredWorkerID: "worker-1", ExpectedRecordRevision: kept.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(3 * time.Second), WithdrawSegmentSkip: true,
	})
	if err != nil || cleared.SegmentSkip != 0 || cleared.RecordRevision != kept.RecordRevision {
		t.Fatalf("withdrawing the word = (%+v, %v), want none with the revision unmoved", cleared, err)
	}
	if fenced, err := store.CheckFenceWithAssignment(ctx, lease.Fence); err != nil || fenced.SegmentSkip != 0 {
		t.Fatalf("the fence check after the withdrawal = (%+v, %v), want no word", fenced, err)
	}
	if _, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-skip", DesiredWorkerID: "worker-1", ExpectedRecordRevision: cleared.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(4 * time.Second), SetSegmentSkip: true, WithdrawSegmentSkip: true,
	}); err == nil {
		t.Fatal("a decision both setting and withdrawing the word was published")
	}
}

// A word set under one Leader stays on the record through a later Leader
// that does not restate it - as a Leader a rollback put back never does -
// and still names the Leader that set it, not the later one; restated by
// the later Leader, it names that one.
func TestASegmentSkipWordNamesTheLeaderThatSetIt(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)
	first, err := store.AcquireControlLeader(ctx, "control-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	set, err := store.PublishAssignment(ctx, first, AssignmentDecision{
		QueryGroup: "qg-term", DesiredWorkerID: "worker-1", PlacementReason: PlacementRendezvous, DecidedAt: now,
		SetSegmentSkip: true,
	})
	if err != nil || set.SegmentSkip != first.Fence.OwnerEpoch {
		t.Fatalf("the first Leader's word = (%+v, %v), want its epoch %d", set, err, first.Fence.OwnerEpoch)
	}
	elapseOnRedis(t, store, ControlLeaderIdentity, 2*time.Minute)
	next, err := store.AcquireControlLeader(ctx, "control-2", now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if next.Fence.OwnerEpoch == first.Fence.OwnerEpoch {
		t.Fatalf("the next Leader holds epoch %d, the first's", next.Fence.OwnerEpoch)
	}
	unsaid, err := store.PublishAssignment(ctx, next, AssignmentDecision{
		QueryGroup: "qg-term", DesiredWorkerID: "worker-2", ExpectedRecordRevision: set.RecordRevision,
		PlacementReason: PlacementRebalance, DecidedAt: now.Add(2 * time.Minute),
	})
	if err != nil || unsaid.SegmentSkip != first.Fence.OwnerEpoch {
		t.Fatalf("a later Leader's move naming no word = (%+v, %v), want the first Leader's epoch %d left", unsaid, err,
			first.Fence.OwnerEpoch)
	}
	restated, err := store.PublishAssignment(ctx, next, AssignmentDecision{
		QueryGroup: "qg-term", DesiredWorkerID: "worker-2", ExpectedRecordRevision: unsaid.RecordRevision,
		PlacementReason: PlacementRebalance, DecidedAt: now.Add(2*time.Minute + time.Second), SetSegmentSkip: true,
	})
	if err != nil || restated.SegmentSkip != next.Fence.OwnerEpoch {
		t.Fatalf("restated by the later Leader = (%+v, %v), want its epoch %d", restated, err, next.Fence.OwnerEpoch)
	}
}
