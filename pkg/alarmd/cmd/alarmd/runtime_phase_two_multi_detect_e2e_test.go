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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// multiDetectStrategyDocument is the full-flow fixture strategy turned into an
// item detected over two inputs, as the writer projects one: CPU used (alias
// a) over 80 and memory used (alias b) over 70, each its own input, joined
// on the host, no item expression, with its frozen revision.
func multiDetectStrategyDocument(t *testing.T) []byte {
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
	cpu := item["query_configs"].([]any)[0].(map[string]any)
	memory := map[string]any{}
	for key, value := range cpu {
		memory[key] = value
	}
	memory["result_table_id"], memory["metric_field"], memory["alias"] = "system.mem", "pct_used", "b"
	item["query_configs"] = []any{cpu, memory}
	delete(item, "expression")
	item["query_output_config"] = map[string]any{"response_contract": "multi_detect/v1", "common_dimensions": []any{"host"},
		"inputs": []any{
			map[string]any{"ref": "cpu", "display_name": "CPU", "expression": "a", "interval": cpu["agg_interval"], "unit": "percent"},
			map[string]any{"ref": "memory", "display_name": "Memory", "expression": "b", "interval": cpu["agg_interval"], "unit": "percent"},
		}}
	threshold := func(ref string, limit int) map[string]any {
		return map[string]any{"level": 1, "type": "Threshold", "unit_prefix": "", "input_ref": ref,
			"config": []any{[]any{map[string]any{"method": "gte", "threshold": limit}}}}
	}
	item["algorithms"] = []any{threshold("cpu", 80), threshold("memory", 70)}
	// A frozen revision, as the writer stamps every strategy it projects:
	// the item goes out as the standard raw event, which carries a value
	// for each input.
	document["strategy_revision"] = 7
	out, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// An item detected over two inputs runs end to end on the production wiring,
// once the ready worker - this one - declares the capability (015 F1): each
// input read in its own request, a series both hold over both thresholds
// sending its ABNORMAL, and the two series only one input holds - one the
// PRIMARY's, one the other input's alone, a round that input anchors - judged
// undecided, sending nothing, the Slot completing - degraded for them.
func TestAnItemDetectedOverTwoInputsRunsEndToEnd(t *testing.T) {
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	if err := redisClient.Set(ctx, "alarm-config.strategy_ids", `[1001]`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisClient.Set(ctx, "alarm-config.strategy_1001", multiDetectStrategyDocument(t), 0).Err(); err != nil {
		t.Fatal(err)
	}
	base := phaseTwoQueryableSlotBoundary()
	var clock atomic.Int64
	clock.Store(base)
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	// Each input's own answer: host-a over both thresholds, host-c in the
	// CPU's alone, host-b in the memory's alone.
	answers := map[string][]struct {
		host  string
		value int
	}{
		"usage":    {{"host-a", 95}, {"host-c", 95}},
		"pct_used": {{"host-a", 86}, {"host-b", 86}},
	}
	var requests sync.Map
	uqServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		var payload struct {
			QueryList []struct {
				FieldName string `json:"field_name"`
			} `json:"query_list"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil || len(payload.QueryList) != 1 {
			t.Errorf("UQ request %+v, %v: want one query", payload, err)
			http.Error(writer, "bad request", http.StatusBadRequest)
			return
		}
		field := payload.QueryList[0].FieldName
		count, _ := requests.LoadOrStore(field, new(atomic.Int64))
		count.(*atomic.Int64).Add(1)
		series := make([]string, 0, len(answers[field]))
		for _, answer := range answers[field] {
			series = append(series, fmt.Sprintf(`{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["host"],"group_values":[%q],"values":[[%s,%d]]}`,
				answer.host, strconv.FormatInt((base-1)*1000, 10), answer.value))
		}
		_, _ = writer.Write([]byte(`{"series":[` + strings.Join(series, ",") + `],"status":null,"trace_id":"multi-detect-uq","is_partial":false,"result_table_id":["system.cpu"]}`))
	}))
	defer uqServer.Close()

	cfg := validGoAccessRuntimeConfig()
	cfg.Redis.Address = address
	withCompatibilityOutput(&cfg, address)
	cfg.Redis.StatePrefix = "alarmd-multi-detect-e2e"
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

	events := &recordingPhaseTwoEventSink{}
	var observationsMu sync.Mutex
	var observations []observability.Observation
	observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		observationsMu.Lock()
		defer observationsMu.Unlock()
		observations = append(observations, observability.NormalizeObservation(observation))
	})
	recorder := metric.NewRecorder(metric.BuildInfo{})
	bundle, err := openProductionPhaseTwoBundleWithDependencies(
		ctx, cfg, recorder, observability.Discard(observability.ComponentRuntime),
		newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{
			RoleRuntime: openRoleRuntimeForTest(t, cfg),
			Now:         now, HTTPClient: uqServer.Client(), AdditionalObserver: observer,
			PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) {
				return events, nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("open production bundle: %v", err)
	}
	if err := bundle.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = bundle.Shutdown(ctx) }()
	// The ready set the gate admits by is this worker's, read by the
	// ownership round; the source round after it releases the item.
	for round := 0; round < 3 && len(bundle.queryGroups) == 0; round++ {
		if err := bundle.register(ctx, ownership.WorkerReady); err != nil {
			t.Fatal(err)
		}
		if err := bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(bundle.queryGroups) != 1 {
		observationsMu.Lock()
		defer observationsMu.Unlock()
		t.Fatalf("assigned Query Groups %v, want the item's one: the gate did not release it", bundle.queryGroups)
	}
	clock.Store(base + 1)
	if err := runScheduledOnceSettled(t, ctx, bundle); err != nil {
		t.Fatalf("the item's Slot: %v", err)
	}

	for _, field := range []string{"usage", "pct_used"} {
		count, ok := requests.Load(field)
		if !ok || count.(*atomic.Int64).Load() != 1 {
			t.Fatalf("UQ requests for %s: %v, want each input read once in its own request", field, count)
		}
	}
	observationsMu.Lock()
	defer observationsMu.Unlock()
	outcomes := map[string]int{}
	var completed *observability.Observation
	var failures []string
	for index, observation := range observations {
		switch {
		case observation.Component == observability.ComponentEvaluation && observation.Stage == observability.StageEvaluationCompleted:
			for _, fact := range observation.LevelOutcomes {
				outcomes[fact.Outcome+"/"+fact.Reason] += int(fact.Count)
			}
		case observation.Stage == observability.StageSlotCompleted:
			completed = &observations[index]
		}
		if observation.Result != "" && observation.Result != observability.ResultSuccess {
			failures = append(failures, fmt.Sprintf("%s/%s %s %s %v", observation.Component, observation.Stage, observation.Result, observation.ReasonCode, observation.Err))
		}
	}
	written := events.snapshot()
	if len(written) != 1 || written[0].EventKind != "ABNORMAL" || written[0].PlanRef.StrategyID != "1001" {
		t.Fatalf("events %+v, want host-a's one ABNORMAL; Level outcomes %v; not successful: %v", written, outcomes, failures)
	}
	if outcomes["ABNORMAL/"] != 1 || outcomes["UNKNOWN/"+contract.ReasonMultiDetectInputMissing] != 2 {
		t.Fatalf("Level outcomes %v, want host-a ABNORMAL and host-b, host-c undecided for a missing input", outcomes)
	}
	// Completed, and degraded for the series it could not judge, as a Slot
	// with a dependency point missing is: Progress moves on.
	if completed == nil || completed.ExecuteOutcome != "completed" || completed.Result != observability.ResultDegraded ||
		completed.SlotCompletionCause != "LEVEL_OUTCOME_UNKNOWN" || completed.ReasonCode != contract.ReasonMultiDetectInputMissing {
		t.Fatalf("the Slot completed as %+v, want it completed, degraded for the undecided series", completed)
	}
}

// A ready worker that does not declare the capability - one of the release
// before it, mid rollout - holds the item out of the Catalog, named on its
// disposition, while the single-query strategy on the same query is
// accepted as ever; once that worker is no longer ready, the next rounds
// release the item, its document unchanged (015 F1 6.2).
func TestAnOldWorkerHoldsTheItemOutAndNotTheStrategiesBesideIt(t *testing.T) {
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	single, err := os.ReadFile("testdata/g1_full_threshold_strategy.json")
	if err != nil {
		t.Fatal(err)
	}
	var singleDocument map[string]any
	if err := json.Unmarshal(single, &singleDocument); err != nil {
		t.Fatal(err)
	}
	singleDocument["id"] = 1002
	singleItem := singleDocument["items"].([]any)[0].(map[string]any)
	singleItem["id"] = 2002
	single, err = json.Marshal(singleDocument)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{
		"alarm-config.strategy_ids": `[1001,1002]`, "alarm-config.strategy_1001": multiDetectStrategyDocument(t), "alarm-config.strategy_1002": single,
	} {
		if err := redisClient.Set(ctx, key, value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	uqServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"series":[],"status":null,"trace_id":"unused","is_partial":false,"result_table_id":[]}`))
	}))
	defer uqServer.Close()
	cfg := validGoAccessRuntimeConfig()
	cfg.Redis.Address = address
	withCompatibilityOutput(&cfg, address)
	cfg.Redis.StatePrefix = "alarmd-multi-detect-old-worker"
	cfg.PhaseTwo.Control.RefreshInterval = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Access.UQEndpoint = uqServer.URL
	cfg.PhaseTwo.Worker.RegistrationTTL = config.Duration(10 * time.Minute)
	now := time.Now
	bundle, err := openProductionPhaseTwoBundleWithDependencies(
		ctx, cfg, metric.NewRecorder(metric.BuildInfo{}), observability.Discard(observability.ComponentRuntime),
		newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{
			Now: now, HTTPClient: uqServer.Client(),
			PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) {
				return &recordingPhaseTwoEventSink{}, nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("open production bundle: %v", err)
	}
	if err := bundle.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = bundle.Shutdown(ctx) }()
	store, err := ownership.NewRedisStoreWithClient(redisClient, productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "ownership"))
	if err != nil {
		t.Fatal(err)
	}
	// The old worker: this binary's registration without the capability.
	old, err := phaseTwoWorkerRegistration(cfg, ownership.WorkerReady, now(), nil, nil, viewStreamIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	old.WorkerID = "worker-old"
	old.Capabilities = slices.DeleteFunc(slices.Clone(old.Capabilities), func(capability string) bool {
		return capability == ownership.CapabilityMultiDetect
	})
	if err := store.RegisterWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	repository, err := controlplane.NewRedisCatalogRepository(redisClient, productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "catalog"), phaseTwoCatalogRetention(cfg))
	if err != nil {
		t.Fatal(err)
	}
	dispositions := func() map[string]controlplane.ObjectDisposition {
		t.Helper()
		for round := 0; round < 2; round++ {
			if err := bundle.register(ctx, ownership.WorkerReady); err != nil {
				t.Fatal(err)
			}
			if err := bundle.refreshAndReconcile(ctx, true); err != nil {
				t.Fatal(err)
			}
		}
		audit, err := repository.LoadLatestAudit(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plans := map[string]controlplane.ObjectDisposition{}
		for _, disposition := range audit.Dispositions {
			if disposition.Scope == "PLAN" && (disposition.Disposition == controlplane.DispositionAccepted || disposition.Reason == "MULTI_DETECT_NOT_ENABLED") {
				plans[disposition.SourceID] = disposition
			}
		}
		return plans
	}
	held := dispositions()
	if got := held["1001"]; got.Reason != "MULTI_DETECT_NOT_ENABLED" || !strings.Contains(got.Detail, "worker-old") {
		t.Fatalf("the item with an old worker ready: %+v, want it held naming worker-old", got)
	}
	if got := held["1002"]; got.Disposition != controlplane.DispositionAccepted {
		t.Fatalf("the single-query strategy beside it: %+v, want it accepted", got)
	}
	// The old worker drains: no longer ready, it holds nothing.
	old.AssignmentReadiness = ownership.WorkerDraining
	if err := store.RegisterWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	released := dispositions()
	if got := released["1001"]; got.Disposition != controlplane.DispositionAccepted {
		t.Fatalf("the item once every ready worker declares it: %+v, want it accepted", got)
	}
	if got := released["1002"]; got.Disposition != controlplane.DispositionAccepted {
		t.Fatalf("the single-query strategy: %+v, want it still accepted", got)
	}
}
