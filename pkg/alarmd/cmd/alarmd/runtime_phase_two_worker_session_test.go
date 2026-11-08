// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// The worker holds one session for as long as it runs: it acquires it,
// renews it on the interval, and once the session is gone -- here released
// from outside, as a session that lapsed is -- it does not renew it again but
// acquires a new one under a larger epoch. Released at shutdown, the Control
// Leader reads every session up to that epoch as lapsed. Each attempt is a
// lease_renewed line on the session's identity.
func TestAWorkerHoldsOneSessionAndReplacesOneItLost(t *testing.T) {
	store := newViewGateTestStore(t)
	var mu sync.Mutex
	var lines []observability.Observation
	runtime := &productionPhaseTwoOwnership{dependencies: productionPhaseTwoOwnershipDependencies{
		Store: store, Now: time.Now,
		Observer: observability.ObserverFunc(func(_ context.Context, o observability.Observation) {
			if o.Stage == observability.StageLeaseRenewed {
				mu.Lock()
				lines = append(lines, o)
				mu.Unlock()
			}
		}),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.MaintainWorkerSession(ctx, "worker-1", 50*time.Millisecond, time.Second) }()

	var first ownership.WorkerSession
	testwait.Until(t, "the session acquired", func() bool {
		var held bool
		first, held = runtime.workerSession.current()
		return held
	})
	if err := store.ReleaseWorkerSession(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	var second ownership.WorkerSession
	testwait.Until(t, "a new session after the lost one", func() bool {
		session, held := runtime.workerSession.current()
		second = session
		return held && session.Epoch > first.Epoch
	})
	cancel()
	<-done
	if err := runtime.ReleaseWorkerSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	facts, err := store.ReadWorkerSessions(context.Background(), []string{"worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	if through, ok := facts["worker-1"].LapsedThrough(); !ok || through != second.Epoch {
		t.Fatalf("after shutdown the sessions lapsed through (%d, %t), want through %d", through, ok, second.Epoch)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lines) == 0 {
		t.Fatal("no lease_renewed line for the session")
	}
	for _, line := range lines {
		if line.Trace.QueryGroupKey != string(ownership.WorkerSessionIdentity("worker-1")) {
			t.Fatalf("a session line traced %q, want the session's identity", line.Trace.QueryGroupKey)
		}
	}
}
