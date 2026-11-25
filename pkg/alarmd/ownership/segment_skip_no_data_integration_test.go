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

// segmentSkipCheckFenceLua is the fence check of the release before the
// no-data family's word (014 next9 ⑥), after the shared FenceLua: what a
// worker of that release runs before each Slot, the segment skip word its
// last element. Frozen: it is the old binary.
const segmentSkipCheckFenceLua = `
local require_assignment = ARGV[1]
local owner_id = ARGV[2]
local epoch = ARGV[3]
local token = ARGV[4]
local content_scope = ARGV[5] or ''
local now_ms = redis_now_ms()
local empty = {'', '', '', '', '', '', '', '', '', ''}
local record_fields = {'desired_worker_id', 'assignment_generation',
  'record_revision', 'control_epoch', 'placement_reason', 'assigned_at_ms',
  'content_scope', 'pending_content_scope', 'effective_at_ms', 'timeline_record_revision', 'segment_skip'}
local refusal, facts = fence_refusal(KEYS[1], KEYS[2], require_assignment, owner_id, epoch, token, content_scope, now_ms, record_fields)
if refusal then return {refusal, empty[1], empty[2], empty[3], empty[4], empty[5], empty[6], empty[7], empty[8], empty[9], empty[10]} end
local record = {'', '', '', '', '', '', '', '', '', '', ''}
if require_assignment == '1' then
  for index, name in ipairs(record_fields) do
    if facts[name] then record[index] = facts[name] end
  end
end
return {'VALID', record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], now_ms,
  record[11]}
`

// The no-data family's word is the epoch of the Leader that set it, written
// from the fence the publish script checked, as the segment skip word is;
// it travels with the fence check a Slot makes, after that word; the
// release before reads the record and its segment skip word as it did; a
// decision naming neither setting nor withdrawal leaves it; its withdrawal
// clears it alone; none of these moves the record's revision; and a
// decision cannot ask for both.
func TestTheNoDataSkipWordIsTheSettingLeadersEpochAndTravelsWithTheFence(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)
	authority, err := store.AcquireControlLeader(ctx, "control-1", now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	epoch := authority.Fence.OwnerEpoch
	placed, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-no-data", DesiredWorkerID: "worker-1", PlacementReason: PlacementRendezvous, DecidedAt: now,
	})
	if err != nil || placed.SegmentSkipNoData != 0 {
		t.Fatalf("a placement naming no word = (%+v, %v), want none", placed, err)
	}
	set, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-no-data", DesiredWorkerID: "worker-1", ExpectedRecordRevision: placed.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(time.Second), SetSegmentSkip: true, SetSegmentSkipNoData: true,
	})
	if err != nil || set.SegmentSkip != epoch || set.SegmentSkipNoData != epoch || set.RecordRevision != placed.RecordRevision {
		t.Fatalf("setting both words = (%+v, %v), want the Leader's epoch %d on both with the revision unmoved", set, err, epoch)
	}
	if read, err := store.ReadAssignment(ctx, "qg-no-data"); err != nil || read.SegmentSkipNoData != epoch || read.SegmentSkip != epoch {
		t.Fatalf("ReadAssignment() = (%+v, %v), want both words %d", read, err, epoch)
	}
	lease, err := store.Acquire(ctx, "qg-no-data", "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fenced, err := store.CheckFenceWithAssignment(ctx, lease.Fence)
	if err != nil || fenced.SegmentSkip != epoch || fenced.SegmentSkipNoData != epoch || fenced.ObservedAt.IsZero() {
		t.Fatalf("the Slot's fence check = (%+v, %v), want both words %d beside the server instant", fenced, err, epoch)
	}

	// The release before reads the record and the segment skip word as it
	// always did: its fence check answers VALID with the thirteen elements it
	// knows.
	reply, err := redis.NewScript(FenceLua+segmentSkipCheckFenceLua).Run(ctx, store.client,
		[]string{store.assignmentKey("qg-no-data"), store.ownershipKey("qg-no-data")},
		"1", lease.Fence.OwnerID, lease.Fence.OwnerEpoch, lease.Fence.LeaseToken, "").Result()
	if err != nil {
		t.Fatal(err)
	}
	values, err := scriptValues(reply, 11)
	if err != nil || scriptText(values[0]) != "VALID" || len(values) != 13 || uint64(scriptInt(values[12])) != epoch {
		t.Fatalf("the previous release's fence check = (%v, %v), want VALID, its thirteen elements and the segment skip word", reply, err)
	}

	kept, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-no-data", DesiredWorkerID: "worker-1", ExpectedRecordRevision: set.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(2 * time.Second),
	})
	if err != nil || kept.SegmentSkipNoData != epoch {
		t.Fatalf("a decision naming neither = (%+v, %v), want the word %d kept", kept, err, epoch)
	}
	cleared, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
		QueryGroup: "qg-no-data", DesiredWorkerID: "worker-1", ExpectedRecordRevision: kept.RecordRevision,
		PlacementReason: PlacementRendezvous, DecidedAt: now.Add(3 * time.Second), WithdrawSegmentSkipNoData: true,
	})
	if err != nil || cleared.SegmentSkipNoData != 0 || cleared.SegmentSkip != epoch || cleared.RecordRevision != kept.RecordRevision {
		t.Fatalf("withdrawing the word = (%+v, %v), want it alone gone with the revision unmoved", cleared, err)
	}
	if fenced, err := store.CheckFenceWithAssignment(ctx, lease.Fence); err != nil || fenced.SegmentSkipNoData != 0 || fenced.SegmentSkip != epoch {
		t.Fatalf("the fence check after the withdrawal = (%+v, %v), want the segment skip word alone", fenced, err)
	}
	if err := (AssignmentDecision{QueryGroup: "qg-no-data", DesiredWorkerID: "worker-1", PlacementReason: PlacementRendezvous,
		DecidedAt: now, SetSegmentSkipNoData: true, WithdrawSegmentSkipNoData: true}).Validate(); err == nil {
		t.Fatal("a decision both setting and withdrawing the no-data word was valid")
	}
}
