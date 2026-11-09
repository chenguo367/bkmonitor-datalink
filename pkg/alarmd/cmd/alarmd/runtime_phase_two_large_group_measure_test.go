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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// largeGroupStrategy is the largest Query Group seen on a 4 GiB replica as a
// legacy strategy: one time-series clause aggregated by the maximum over 60
// seconds by three dimensions, detected every 60 seconds, five anomalies of
// five points to trigger and three windows to recover.
func largeGroupStrategy(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/g1_full_threshold_strategy.json")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	item := document["items"].([]any)[0].(map[string]any)
	query := item["query_configs"].([]any)[0].(map[string]any)
	query["agg_interval"], query["agg_method"] = 60, "MAX"
	query["agg_dimension"] = []string{"bk_target_cloud_id", "bk_target_ip", "device_name"}
	detect := document["detects"].([]any)[0].(map[string]any)
	detect["trigger_config"] = map[string]any{"count": 5, "check_window": 5}
	detect["recovery_config"] = map[string]any{"check_window": 3}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// largeGroupAnswer is its answer for one Slot: series series of one point
// each, the three dimensions at the lengths the group's carry at most - a
// cloud id of three bytes, an address of fourteen, a device name of 32.
func largeGroupAnswer(series int, pointMillis int64) []byte {
	var body bytes.Buffer
	body.WriteString(`{"series":[`)
	for index := 0; index < series; index++ {
		if index > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],`+
			`"group_keys":["bk_target_cloud_id","bk_target_ip","device_name"],"group_values":["%03d","198.51.100.%03d","device-%025d"],`+
			`"values":[[%d,12.5]]}`, index%1000, index%256, index, pointMillis)
	}
	body.WriteString(`],"status":null,"trace_id":"large-group-measure","is_partial":false,"result_table_id":["system.cpu"]}`)
	return body.Bytes()
}

// largeGroupLiveHeap is the live heap in bytes, as the runtime last marked it.
func largeGroupLiveHeap() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// TestMeasureALargeQueryGroupsSlotAgainstTheHeap runs one Slot of the largest
// Query Group seen on a 4 GiB replica - 95,760 series of one point and three
// dimensions - through the production bundle on a real Redis, and holds what
// the Slot reserved, input, output and State apart, to the live heap it
// took: the peak of the live heap over the Slot, the collector set to run at
// every few percent of growth, less what was live before it. It prints both
// and asserts nothing; run on demand with ALARMD_MEASURE_LARGE_GROUP=1. It
// builds against the code before batch 7 as well, which is how the two are
// compared.
func TestMeasureALargeQueryGroupsSlotAgainstTheHeap(t *testing.T) {
	if os.Getenv("ALARMD_MEASURE_LARGE_GROUP") == "" {
		t.Skip("a measurement: set ALARMD_MEASURE_LARGE_GROUP=1")
	}
	const series = 95_760
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	if err := redisClient.Set(ctx, "alarm-config.strategy_ids", `[1001]`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisClient.Set(ctx, "alarm-config.strategy_1001", largeGroupStrategy(t), 0).Err(); err != nil {
		t.Fatal(err)
	}
	// A minute boundary two minutes ahead, so the frozen query deadline the
	// UQ client puts on the request stays ahead of the wall clock.
	base := time.Now().Unix()
	base = base - base%60 + 120
	var clock atomic.Int64
	clock.Store(base)
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	response := largeGroupAnswer(series, (base-1)*1000)
	uqServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(response)
	}))
	defer uqServer.Close()

	cfg := validGoAccessRuntimeConfig()
	cfg.Redis.Address = address
	withCompatibilityOutput(&cfg, address)
	cfg.Redis.StatePrefix = "alarmd-large-group"
	cfg.PhaseTwo.Control.RefreshInterval = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Access.UQEndpoint = uqServer.URL
	cfg.PhaseTwo.Access.MinReadyDelay = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Access.DownstreamExecutionReserve = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Worker.RegistrationTTL = config.Duration(10 * time.Minute)
	cfg.PhaseTwo.Worker.RegistrationRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.ControlLeaderTTL = config.Duration(10 * time.Minute)
	cfg.PhaseTwo.Ownership.ControlLeaderRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.LeaseTTL = config.Duration(10 * time.Minute)
	cfg.PhaseTwo.Ownership.LeaseRenewInterval = config.Duration(time.Minute)
	// The Slot is measured, not refused: every budget holds it, the counts
	// at the most the chunked State apply takes (max_keys_per_batch x 64).
	cfg.PhaseTwo.Coordinator.MaxEvents = 1 << 19
	cfg.PhaseTwo.Coordinator.MaxStateMutations = 1 << 19
	cfg.PhaseTwo.Coordinator.MaxGapMutations = 1 << 19
	cfg.PhaseTwo.Coordinator.MaxRetainedBytes = 8 << 30
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configuration rejected: %v", err)
	}

	events := &recordingPhaseTwoEventSink{}
	var mu sync.Mutex
	var usage *observability.SlotBudgetUsageFacts
	observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		if observation.Stage == observability.StageSlotCompleted && observation.SlotBudgetUsage != nil {
			mu.Lock()
			copied := *observation.SlotBudgetUsage
			usage = &copied
			mu.Unlock()
		}
	})
	bundle, err := openProductionPhaseTwoBundleWithDependencies(
		ctx, cfg, metric.NewRecorder(metric.BuildInfo{}), observability.Discard(observability.ComponentRuntime),
		newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{
			Now: now, HTTPClient: uqServer.Client(), AdditionalObserver: observer,
			PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) { return events, nil }),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bundle.Shutdown(ctx) }()
	settleExecutableView(ctx, bundle, 10*time.Second)

	clock.Store(base + 1)
	previous := debug.SetGCPercent(5)
	runtime.GC()
	before := largeGroupLiveHeap()
	var peak atomic.Uint64
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Millisecond) // fixed-wait: the sampling interval of a measurement; it waits on no condition
		defer ticker.Stop()
		for {
			if live := largeGroupLiveHeap(); live > peak.Load() {
				peak.Store(live)
			}
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	started := time.Now()
	runErr := bundle.runScheduledOnce(ctx)
	took := time.Since(started)
	close(done)
	<-sampled
	debug.SetGCPercent(previous)
	if runErr != nil {
		t.Fatal(runErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if usage == nil {
		t.Fatal("no Slot completed with its budget usage")
	}
	live := peak.Load() - before
	reserved := usage.RetainedInputBytes + usage.RetainedOutputBytes + usage.RetainedStateBytes + usage.RetainedGapBytes
	t.Logf("series=%d took=%s reserved=%d (input %d output %d state %d gap %d; %d a series) live_peak=%d (%d a series) ratio=%.3f "+
		"retained_bytes=%d resident_before=%d", usage.Series, took.Round(time.Millisecond), reserved, usage.RetainedInputBytes,
		usage.RetainedOutputBytes, usage.RetainedStateBytes, usage.RetainedGapBytes, reserved/max(usage.Series, 1),
		live, live/series, float64(reserved)/float64(live), usage.RetainedBytes, before)
}
