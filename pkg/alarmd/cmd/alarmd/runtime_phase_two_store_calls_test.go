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
	"io"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// A call the runtime state store makes failing on a Redis client the bundle
// hooked reaches the replica's fleet tracker as a store call segment; one
// the cost projection makes does not, it not being the deployment's store.
func TestAFailedOwnStoreCallReachesTheFleetTracker(t *testing.T) {
	recorder := metric.NewRecorder(metric.BuildInfo{})
	tracker := fleet.NewTracker(nil, "replica-1", time.Now)
	listenStoreCalls(recorder, tracker)
	hook := recorder.RedisHook("source")
	call := func(caller string) {
		ctx, _ := hook.BeforeProcess(redisfailure.WithCaller(context.Background(), caller), nil)
		cmd := redis.NewStringCmd(ctx, "get", "key")
		cmd.SetErr(context.DeadlineExceeded)
		_ = hook.AfterProcess(ctx, cmd)
	}
	call(redisfailure.CallerCostProjection)
	if run, kept := tracker.StoreCallRun(); kept {
		t.Fatalf("run %+v after a diagnostics job failed, want none", run)
	}
	call(redisfailure.CallerRuntimeState)
	if run, kept := tracker.StoreCallRun(); !kept || run.Reasons[redisfailure.Timeout] != 1 {
		t.Fatalf("run %+v, want the runtime state store's timed-out call", run)
	}
}

// The production bundle hands its recorder's store call failures to its own fleet tracker: a runtime state call failing on
// a hooked client is a run on the tracker the assembled bundle publishes from.
func TestTheProductionBundleHandsStoreCallFailuresToItsFleetTracker(t *testing.T) {
	fixture := startCutoverFixtureWith(t, nil, observability.New(observability.ComponentRuntime, io.Discard), nil)
	if fixture.bundle.fleetTracker == nil {
		t.Fatal("the assembled bundle keeps no fleet tracker")
	}
	hook := fixture.recorder.RedisHook("source")
	ctx, _ := hook.BeforeProcess(redisfailure.WithCaller(context.Background(), redisfailure.CallerRuntimeState), nil)
	cmd := redis.NewStringCmd(ctx, "get", "key")
	cmd.SetErr(context.DeadlineExceeded)
	_ = hook.AfterProcess(ctx, cmd)
	if run, kept := fixture.bundle.fleetTracker.StoreCallRun(); !kept || run.Reasons[redisfailure.Timeout] < 1 {
		t.Fatalf("run %+v (kept %v), want the bundle's tracker to have heard the failed call", run, kept)
	}
}
