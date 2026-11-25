// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// What a round's two control reads took is returned with them, on a real
// Redis: the registry read and the assignment read each report a Duration
// within the call's own wall time and not 0. They feed
// control_read_duration_seconds{read} and the control_reads_spent facts; a
// Duration set by a deferred write on a copy the return already took reads
// 0 on every round, every observation in the histogram's first bucket.
func TestAControlReadReturnsWhatItTook(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	if err := store.RegisterWorker(ctx, WorkerRegistration{
		WorkerID: "worker-1", AssignmentReadiness: WorkerReady, DependencyStatus: DependencyHealthy,
		DeploymentProfile: "standard", CapabilitiesDigest: "capabilities-v1", ExpiresAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	authority, err := store.AcquireControlLeader(ctx, "control-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	queryGroups := []execution.QueryGroupIdentity{"qg-1", "qg-2", "qg-unpublished"}
	for _, queryGroup := range queryGroups[:2] {
		if _, err := store.PublishAssignment(ctx, authority, AssignmentDecision{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", PlacementReason: PlacementRendezvous, DecidedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	began := time.Now()
	ready, registry, err := store.ListReadyWorkers(ctx, now)
	took := time.Since(began)
	if err != nil || len(ready) != 1 || registry.RoundTrips == 0 {
		t.Fatalf("ListReadyWorkers() = (%d workers, %+v, %v), want the one worker and its round trips", len(ready), registry, err)
	}
	if registry.Duration <= 0 || registry.Duration > took {
		t.Fatalf("the registry read took %v by its own account, the call %v: want it measured and within the call", registry.Duration, took)
	}

	began = time.Now()
	records, assignments, err := store.ReadAssignments(ctx, queryGroups)
	took = time.Since(began)
	if err != nil || len(records) != 2 || assignments.Keys != 3 || assignments.RoundTrips == 0 {
		t.Fatalf("ReadAssignments() = (%d records, %+v, %v), want the two published, three keys read", len(records), assignments, err)
	}
	if assignments.Duration <= 0 || assignments.Duration > took {
		t.Fatalf("the assignment read took %v by its own account, the call %v: want it measured and within the call", assignments.Duration, took)
	}
}
