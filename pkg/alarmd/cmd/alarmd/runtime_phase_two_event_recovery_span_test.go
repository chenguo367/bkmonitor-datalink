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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A grouped event count recovers on the zeros the query service fills in
// for a group that had an event inside the range asked, and it can only do
// so while the event is still inside that range: the query service returns a
// group only when the range holds one of its events. The trigger reads back
// N + R - 1 points to recover, so the range asked is N + R periods, the
// period accepted the newest of them. Here N = 2 and R = 3: the event's round
// opens the alert, the windows ending on rounds 2, 3 and 4 hold no anomaly,
// and RECOVERY is decided on round 4 - not before, and not never, as it was
// while the range was one period and the group vanished on round 1.
//
// Through the production bundle, with a process restart after the event's
// round, as a release would put one: the open alert's history is carried
// across it on the same store.
func TestAGroupedEventCountRecoversOnTheQueryServicesZeros(t *testing.T) {
	address, client := startPhaseTwoRedis(t)
	ctx := context.Background()
	var document map[string]any
	if err := json.Unmarshal(controlledG4StrategyDocument(t, 7201, "Threshold", "usage", "system.cpu", []string{"host"},
		[]any{[]any{map[string]any{"method": "gte", "threshold": 1}}}), &document); err != nil {
		t.Fatal(err)
	}
	item := document["items"].([]any)[0].(map[string]any)
	query := item["query_configs"].([]any)[0].(map[string]any)
	query["data_source_label"], query["data_type_label"], query["agg_method"] = "custom", "event", "COUNT"
	query["custom_event_name"], query["result_table_id"] = "synthetic-event", "system_event"
	document["detects"] = []any{map[string]any{"level": 1, "priority": 1, "connector": "and",
		"trigger_config": map[string]any{"count": 1, "check_window": 2}, "recovery_config": map[string]any{"check_window": 3}}}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "alarm-config.strategy_ids", "[7201]", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "alarm-config.strategy_7201", body, 0).Err(); err != nil {
		t.Fatal(err)
	}

	const period = int64(60)
	base := controlledG4Base(t)
	var clock atomic.Int64
	clock.Store(base)
	var (
		mu          sync.Mutex
		eventBucket int64 // the bucket the one event falls in: round 0's accepted period
		ranges      [][2]int64
	)
	// The query service as Elasticsearch answers a grouped count: a group
	// with a document in [start, end) comes back with a point for every
	// step, zero where the step holds none; a group with none in the range
	// does not come back at all.
	uq := &http.Client{Transport: controlledRoundTripper(func(request *http.Request) (*http.Response, error) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			return nil, err
		}
		start, err := strconv.ParseInt(fmt.Sprint(payload["start_time"]), 10, 64)
		if err != nil {
			return nil, err
		}
		end, err := strconv.ParseInt(fmt.Sprint(payload["end_time"]), 10, 64)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		if eventBucket == 0 {
			eventBucket = end - period
		}
		ranges = append(ranges, [2]int64{start, end})
		event := eventBucket
		mu.Unlock()
		series := []any{}
		if event >= start && event < end {
			values := []any{}
			for bucket := start; bucket < end; bucket += period {
				count := 0
				if bucket == event {
					count = 1
				}
				values = append(values, []any{bucket * 1000, count})
			}
			series = append(series, map[string]any{"name": "_result0", "columns": []string{"_time", "_value"}, "types": []string{"float", "float"},
				"group_keys": []string{"host"}, "group_values": []string{"synthetic-a"}, "values": values})
		}
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(map[string]any{"series": series, "status": nil, "trace_id": "event-span", "is_partial": false}); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(&buf), Request: request}, nil
	})}

	var observations []observability.Observation
	open := func() *phaseTwoWorkerBundle {
		cfg := controlledG4RuntimeConfig(address, "http://controlled-uq", "alarmd-event-span")
		bundle, err := openProductionPhaseTwoBundleWithDependencies(ctx, cfg, metric.NewRecorder(metric.BuildInfo{}),
			observability.Discard(observability.ComponentRuntime), newPhaseTwoApplicationHealth(),
			func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
				return controlplane.NewLegacyRedisStrategySource(client, prefix)
			}, phaseTwoProductionExternalDependencies{
				Now: func() time.Time { return time.Unix(clock.Load(), 0) }, HTTPClient: uq,
				AdditionalObserver: observability.ObserverFunc(func(_ context.Context, o observability.Observation) {
					mu.Lock()
					defer mu.Unlock()
					observations = append(observations, observability.NormalizeObservation(o))
				}),
				PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) {
					return &recordingPhaseTwoEventSink{}, nil
				}),
			})
		if err != nil {
			t.Fatal(err)
		}
		if err := bundle.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return bundle
	}
	decidedOn := make(map[int][]string)
	run := func(bundle *phaseTwoWorkerBundle, round int) {
		mu.Lock()
		seen := len(observations)
		mu.Unlock()
		clock.Store(base + int64(round)*period + 1)
		if err := runScheduledOnceSettled(ctx, bundle); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		mu.Lock()
		decidedOn[round] = decidedEventKinds(observations[seen:])
		mu.Unlock()
	}

	first := open()
	run(first, 0)
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	second := open()
	t.Cleanup(func() {
		if err := second.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	for round := 1; round <= 4; round++ {
		run(second, round)
	}

	if !containsKind(decidedOn[0], contract.TriggerEventAbnormal) {
		t.Fatalf("round 0 decided %v, want the event's ABNORMAL", decidedOn[0])
	}
	for round := 1; round <= 3; round++ {
		if containsKind(decidedOn[round], contract.TriggerEventRecovery) {
			t.Fatalf("round %d decided %v: a RECOVERY before the R windows after the trigger window passed", round, decidedOn[round])
		}
	}
	if !containsKind(decidedOn[4], contract.TriggerEventRecovery) {
		t.Fatalf("round 4 decided %v, want RECOVERY at t0 + N + R - 1 (all rounds: %v)", decidedOn[4], decidedOn)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, asked := range ranges {
		if asked[1]-asked[0] != 5*period {
			t.Fatalf("the query service was asked for %d s, want N + R = 5 periods of %d s (ranges %v)", asked[1]-asked[0], period, ranges)
		}
	}
}

func containsKind(kinds []string, kind string) bool {
	for _, candidate := range kinds {
		if candidate == kind {
			return true
		}
	}
	return false
}
