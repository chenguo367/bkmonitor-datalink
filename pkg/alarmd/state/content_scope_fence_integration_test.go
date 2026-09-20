// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// decision-016: the batched State write runs the same fence text as the
// ownership store, so a writer that declares the content scope it produced
// its writes from is refused -- by name, with no key written -- once the
// Assignment record names another; a writer that declares none keeps the
// fence it always had; and the refusal is not a stale fence, because the
// lease is live and the Query Group is still this worker's.
func TestRedisFencedBatchApplyRefusesAMovedContentScopeByName(t *testing.T) {
	fixture := newRedisBatchFixture(t)
	ctx := context.Background()
	store := fixture.store(t, "scoped", true)
	queryGroup := frozenRef().Slot.QueryGroup
	at := fixture.leased.Add(time.Second)

	// The fixture's record was published without a scope and worker-1 holds
	// a live lease on it. Naming a scope under that lease would be written
	// as pending (the holder is protected until its deadline), and a record
	// whose current scope is still empty admits every declared scope -- so
	// the holder releases first, the leader names the scope with nobody to
	// protect, and worker-1 comes back on a lease admitted to view-a.
	if err := fixture.owners.Release(ctx, fixture.fence); err != nil {
		t.Fatal(err)
	}
	record, err := fixture.owners.ReadAssignment(ctx, queryGroup)
	if err != nil {
		t.Fatal(err)
	}
	named, err := fixture.owners.PublishAssignment(ctx, fixture.authority, ownership.AssignmentDecision{
		QueryGroup: queryGroup, DesiredWorkerID: "worker-1", ExpectedRecordRevision: record.RecordRevision,
		PlacementReason: ownership.PlacementRendezvous, DecidedAt: at, ContentScope: "view-a",
	})
	if err != nil || named.ContentScope != "view-a" || named.ContentChangePending() {
		t.Fatalf("PublishAssignment(view-a) with no holder = (%+v, %v), want view-a written directly", named, err)
	}
	lease, err := fixture.owners.Acquire(ctx, queryGroup, "worker-1", at, time.Minute)
	if err != nil || lease.ContentScope != "view-a" {
		t.Fatalf("Acquire() = (%+v, %v), want a lease admitted to view-a", lease, err)
	}
	fixture.fence = lease.Fence

	mutations := seriesMutations(t, 3, applyVersion(), 0)
	request := execution.StateApplyRequest{Contract: frozenRef(), Retention: testRetention(), Items: mutations}
	keys := make([]string, len(mutations))
	for index, mutation := range mutations {
		keys[index], _ = RuntimeStateKeyV2("scoped", mutation.Identity)
	}
	cases := []struct {
		name  string
		scope string
		moved bool
	}{
		{name: "no scope declared keeps the old fence", scope: "", moved: false},
		{name: "the named scope is admitted", scope: "view-a", moved: false},
		{name: "another scope is refused", scope: "view-b", moved: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			loadInStreamBatches(t, store, preflightItems(mutations))
			result, err := store.ApplyRuntimeFenced(ctx, request, execution.StateApplyFence{Fence: fixture.fence, At: at, ContentScope: test.scope})
			exists := fixture.client.Exists(ctx, keys...).Val()
			if test.moved {
				if !errors.Is(err, ownership.ErrContentScopeMoved) || len(result.Items) != 0 || exists != 0 {
					t.Fatalf("moved-scope apply = (%+v, %v) exists=%d, want ErrContentScopeMoved and no keys", result, err, exists)
				}
				if errors.Is(err, ownership.ErrStaleFence) {
					t.Fatal("a moved content scope was reported as a stale fence")
				}
				if checked := fixture.owners.CheckFence(ctx, fixture.fence, at); checked != nil {
					t.Fatalf("the lease itself is live, CheckFence() = %v", checked)
				}
				return
			}
			if err != nil || exists != int64(len(keys)) {
				t.Fatalf("apply = (%+v, %v) exists=%d", result, err, exists)
			}
			requireAllStatus(t, result, execution.StateApplied)
			if err := fixture.client.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
