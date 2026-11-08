// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// A Query Group taken under a worker session names both epochs; one whose
// holder renews its own lease names its owner epoch and no session. The
// expected epochs come from outside the lease: the owner epoch a first and a
// second acquire of a Query Group are given, and the session the store
// granted.
func TestAQueryGroupNamesTheLeaseItIsHeldUnder(t *testing.T) {
	ctx := context.Background()
	store := newViewGateTestStore(t)
	now := time.Now()
	authority, err := store.AcquireControlLeader(ctx, "control-1", now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, queryGroup := range []execution.QueryGroupIdentity{"qg-session", "qg-self"} {
		if _, err := store.PublishAssignment(ctx, authority, ownership.AssignmentDecision{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", PlacementReason: ownership.PlacementRendezvous, DecidedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	worker, err := store.AcquireWorkerSession(ctx, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	holder := &workerSessionHolder{}
	holder.set(worker, true)
	session, err := ownership.OpenSessionUnder(ctx, store, "qg-session", holder.current, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	under := &productionPhaseTwoQueryGroup{session: session, queryGroup: "qg-session", now: time.Now}
	if owner, held := under.HeldEpochs(); owner != 1 || held != worker.Epoch {
		t.Fatalf("HeldEpochs() under the session = %d, %d, want 1, %d", owner, held, worker.Epoch)
	}
	if err := session.Release(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := ownership.OpenSessionUnder(ctx, store, "qg-session", holder.current, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if owner, held := (&productionPhaseTwoQueryGroup{session: again}).HeldEpochs(); owner != 2 || held != worker.Epoch {
		t.Fatalf("HeldEpochs() after a second acquire = %d, %d, want 2, %d", owner, held, worker.Epoch)
	}
	self, err := ownership.OpenSession(ctx, store, "qg-self", "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if owner, held := (&productionPhaseTwoQueryGroup{session: self}).HeldEpochs(); owner != 1 || held != 0 {
		t.Fatalf("HeldEpochs() of a self-renewed lease = %d, %d, want 1, 0", owner, held)
	}
}

type epochPhaseTwoQueryGroup struct {
	*fakePhaseTwoQueryGroup
	owner, session uint64
}

func (runner *epochPhaseTwoQueryGroup) HeldEpochs() (uint64, uint64) {
	return runner.owner, runner.session
}

// The two lines a takeover writes on success carry the lease the Query Group
// was taken under, so which lease a takeover took is read on the line that
// names it.
func TestATakeoverLineCarriesTheEpochsOfTheLeaseItTook(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	control := &fakePhaseTwoControl{queryGroups: []execution.QueryGroupIdentity{"query-group-1"}}
	runner := &epochPhaseTwoQueryGroup{fakePhaseTwoQueryGroup: newFakePhaseTwoQueryGroup(), owner: 4, session: 1758400000123}
	owner := &fakePhaseTwoOwnership{
		assigned: []execution.QueryGroupIdentity{"query-group-1"},
		runners:  map[execution.QueryGroupIdentity]phaseTwoQueryGroupRuntime{"query-group-1": runner},
	}
	var mu sync.Mutex
	var observations []observability.Observation
	observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		mu.Lock()
		defer mu.Unlock()
		observations = append(observations, observation)
	})
	bundle, err := newPhaseTwoWorkerBundle(phaseTwoWorkerBundleDependencies{
		Config: cfg, Health: newPhaseTwoApplicationHealth(), Control: control, Ownership: owner,
		Recorder: metric.NewRecorder(metric.BuildInfo{}), Observer: observer, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = bundle.Shutdown(context.Background()) })
	waitSignal(t, runner.leaseStarted, "query-group lease maintenance")

	mu.Lock()
	defer mu.Unlock()
	for _, stage := range []observability.Stage{observability.StageTakeoverCompleted, observability.StageAssignmentAcquired} {
		found := false
		for _, observation := range observations {
			if observation.Component != observability.ComponentOwnership || observation.Stage != stage ||
				observation.Result != observability.ResultSuccess || observation.Trace.QueryGroupKey != "query-group-1" {
				continue
			}
			found = true
			if observation.Trace.OwnerEpoch != 4 || observation.Trace.SessionEpoch != 1758400000123 {
				t.Fatalf("%s epochs = %d, %d, want 4, 1758400000123", stage, observation.Trace.OwnerEpoch, observation.Trace.SessionEpoch)
			}
		}
		if !found {
			t.Fatalf("no successful %s observation in %+v", stage, observations)
		}
	}
}
