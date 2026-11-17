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
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

// segmentSkipFull is a Control process and a Worker process of this build
// over one Redis, the Worker reading the Control's view stream, one quiet
// threshold strategy answering a point below its threshold each round.
type segmentSkipFull struct {
	t        *testing.T
	clock    *atomic.Int64
	control  *phaseTwoWorkerBundle
	worker   *phaseTwoWorkerBundle
	recorder *metric.Recorder
	slot     int64
	redis    *redis.Client
	cfg      config.Config
	now      func() time.Time
	uq       *http.Client
}

// restartControl replaces the Control process with one of the same build
// asked as given: the values' ask changed, applied by a restart.
func (full *segmentSkipFull) restartControl(asked bool) {
	full.t.Helper()
	if err := full.control.Shutdown(context.Background()); err != nil {
		full.t.Fatal(err)
	}
	cfg := full.cfg
	cfg.PhaseTwo.Control.SegmentSkip = asked
	full.control = openSplitControlForFullPipeline(full.t, cfg, full.now, observability.NopObserver{}, full.uq)
}

func openSegmentSkipFull(t *testing.T, asked bool) *segmentSkipFull {
	t.Helper()
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	strategy, err := os.ReadFile("testdata/g1_full_threshold_strategy.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := redisClient.Set(ctx, "alarm-config.strategy_ids", `[1001]`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisClient.Set(ctx, "alarm-config.strategy_1001", strategy, 0).Err(); err != nil {
		t.Fatal(err)
	}
	full := &segmentSkipFull{t: t, clock: &atomic.Int64{}, slot: phaseTwoQueryableSlotBoundary()}
	full.clock.Store(full.slot)
	now := func() time.Time { return time.Unix(full.clock.Load(), 0) }
	// The Slot's one point, a second before it and below the threshold:
	// steady and normal round after round.
	uq := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		at := strconv.FormatInt((full.clock.Load()-2)*1000, 10)
		_, _ = writer.Write([]byte(`{"series":[{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],` +
			`"group_keys":["host"],"group_values":["127.0.0.1"],"values":[[` + at + `,5]]}],"status":null,"trace_id":"segment-skip-full",` +
			`"is_partial":false,"result_table_id":["system.cpu"]}`))
	}))
	t.Cleanup(uq.Close)
	cfg := validGoAccessRuntimeConfig()
	cfg.Redis.Address = address
	withCompatibilityOutput(&cfg, address)
	cfg.Redis.StatePrefix = "alarmd-segment-skip-full"
	cfg.PhaseTwo.Control.RefreshInterval = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Control.SegmentSkip = asked
	cfg.PhaseTwo.Access.UQEndpoint = uq.URL
	cfg.PhaseTwo.Access.MinReadyDelay = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Access.DownstreamExecutionReserve = config.Duration(time.Millisecond)
	// The rounds move the clock five minutes each and nothing renews on it:
	// the registrations, the leases and the Leader's outlive every round.
	cfg.PhaseTwo.Worker.RegistrationTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Worker.RegistrationRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.ControlLeaderTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Ownership.ControlLeaderRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.LeaseTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Ownership.LeaseRenewInterval = config.Duration(time.Minute)
	full.redis, full.cfg, full.now, full.uq = redisClient, cfg, now, uq.Client()
	full.control = openSplitControlForFullPipeline(t, cfg, now, observability.NopObserver{}, uq.Client())
	cfg.Roles = roles.Set{roles.Worker}
	cfg.HTTP.Listen = reserveBundleAddress()
	full.recorder = metric.NewRecorder(metric.BuildInfo{})
	full.worker, err = openProductionPhaseTwoBundleWithDependencies(ctx, cfg, full.recorder,
		observability.Discard(observability.ComponentRuntime), newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{RoleRuntime: openRoleRuntimeForTest(t, cfg), Now: now, HTTPClient: uq.Client(),
			PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) {
				return &recordingPhaseTwoEventSink{}, nil
			})})
	if err != nil {
		t.Fatal(err)
	}
	if err := full.worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := full.worker.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return full
}

// round runs the next Slot: the Leader's round, the Worker's, its Slot.
func (full *segmentSkipFull) round() {
	full.t.Helper()
	ctx := context.Background()
	full.clock.Store(full.slot + 1)
	if err := full.control.refreshAndReconcile(ctx, false); err != nil {
		full.t.Fatal(err)
	}
	if err := full.worker.refreshAndReconcile(ctx, false); err != nil {
		full.t.Fatal(err)
	}
	if err := runScheduledOnceSettled(ctx, full.worker); err != nil {
		full.t.Fatalf("the Slot at %d: %v", full.slot, err)
	}
	full.slot += 300
}

// skipped is the members the Worker's rounds left unwritten so far.
func (full *segmentSkipFull) skipped() float64 {
	return counterValue(full.t, full.recorder, "alarmd_state_series_not_written_total", map[string]string{"reason": "segment_member_skipped"})
}

// ignored is the Worker's Slots that did not follow their word, by why.
func (full *segmentSkipFull) ignored(reason string) float64 {
	return counterValue(full.t, full.recorder, "alarmd_state_segment_skip_flag_ignored_total", map[string]string{"reason": reason})
}

// The segment skip through the production processes: a Control asked for
// the skip, over a fleet of this build, sets the word; the Worker installs
// the Control's view from its stream, follows the word its Slot's fence
// reads - the view's epoch the Leader's - and leaves its quiet series' State
// write out. Without the ask the same rounds write every State.
func TestTheProductionProcessesLeaveAStableSeriesWriteOutUnderTheLeadersWord(t *testing.T) {
	for _, asked := range []bool{true, false} {
		full := openSegmentSkipFull(t, asked)
		for round := 0; round < 8 && full.skipped() == 0; round++ {
			full.round()
		}
		client := full.worker.dependencies.ViewClient
		if client == nil {
			t.Fatal("the Worker has no view stream client")
		}
		testwait.Until(t, "the Worker installs the Control's view", func() bool {
			_, installed := client.Installed()
			return installed
		})
		view, _ := client.Installed()
		if view.Version.ControlEpoch == view.Version.Revision {
			t.Fatalf("the view's epoch and revision are both %d: a reading of the wrong one would pass", view.Version.Revision)
		}
		if asked && full.skipped() == 0 {
			t.Fatalf("asked, no round left the quiet series' write out (ignored: epoch_mismatch %v, no_view %v)",
				full.ignored("epoch_mismatch"), full.ignored("no_view"))
		}
		if !asked && full.skipped() != 0 {
			t.Fatalf("not asked, %v writes left out", full.skipped())
		}
	}
}

// A Query Group draining after its strategy was retired runs its Slots up to
// its boundary, and the control load hands it to the Leader's round with
// the rest: so when the values' ask goes away - the Control restarted
// without it - its word is taken back in the new Leader's first round, and
// the Worker's next Slot of it, dispatched with none, writes every State:
// no member left, no word ignored.
func TestADrainingQueryGroupsNextSlotWritesWholeOnceTheAskGoesAway(t *testing.T) {
	ctx := context.Background()
	full := openSegmentSkipFull(t, true)
	for round := 0; round < 8 && full.skipped() == 0; round++ {
		full.round()
	}
	if full.skipped() == 0 || len(full.worker.queryGroups) != 1 {
		t.Fatalf("before the retirement: %v left, Query Groups %v", full.skipped(), full.worker.queryGroups)
	}
	retired := full.worker.queryGroups[0]
	word := func() uint64 {
		t.Helper()
		record, err := ownershipOf(t, full.control).dependencies.Store.ReadAssignment(ctx, retired)
		if err != nil {
			t.Fatal(err)
		}
		return record.SegmentSkip
	}
	ran := func() execution.EvaluationTime {
		t.Helper()
		loaded, err := ownershipOf(t, full.worker).dependencies.Progress.LoadProgress(ctx, execution.ProgressIdentity{QueryGroup: retired})
		if err != nil || loaded.Progress == nil {
			t.Fatalf("the retired Query Group's Progress = (%+v, %v)", loaded, err)
		}
		return loaded.Progress.LastFullSlot
	}
	if word() == 0 {
		t.Fatal("the Query Group skipped without a word on its record")
	}
	// The strategy's query changed - its own aggregation - so the Query
	// Group it ran in retires and drains to its boundary, the strategy going
	// on in a new one: two control rounds observe and confirm it. It retires
	// with its next Slot due and not yet run, which it drains.
	full.clock.Store(full.slot + 1)
	payload, err := full.redis.Get(ctx, "alarm-config.strategy_1001").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var changed map[string]any
	if err := json.Unmarshal(payload, &changed); err != nil {
		t.Fatal(err)
	}
	item := changed["items"].([]any)[0].(map[string]any)
	item["query_md5"] = "segment-skip-draining-query-md5"
	item["query_configs"].([]any)[0].(map[string]any)["agg_method"] = "max"
	if payload, err = json.Marshal(changed); err != nil {
		t.Fatal(err)
	}
	if err := full.redis.Set(ctx, "alarm-config.strategy_1001", payload, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := full.control.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	activation, err := full.control.dependencies.Control.(*productionPhaseTwoControl).dependencies.Repository.LoadActivationHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var boundary execution.EvaluationTime
	for _, draining := range activation.Draining {
		if draining.QueryGroup == retired {
			boundary = draining.RetiredBoundary
		}
	}
	if boundary <= execution.EvaluationTime(full.slot) {
		t.Fatalf("the activation's draining %+v, want the retired Query Group, its Slot %d before its boundary", activation.Draining, full.slot)
	}
	loaded, err := full.control.dependencies.Control.LoadActive(ctx)
	if err != nil || !slices.Contains(loaded.QueryGroups, retired) {
		t.Fatalf("the control load = (%v, %v), want the draining Query Group among the round's", loaded.QueryGroups, err)
	}
	if word() == 0 {
		t.Fatal("the asking Leader took the word back from the draining Query Group")
	}
	// The ask goes away: the Control restarted without it.
	full.restartControl(false)
	skipped, before := full.skipped(), ran()
	ignored := full.ignored("epoch_mismatch") + full.ignored("no_view")
	full.round()
	if word() != 0 {
		t.Fatalf("the new Leader's round left the draining Query Group's word %d", word())
	}
	if ran() <= before || full.skipped() != skipped || full.ignored("epoch_mismatch")+full.ignored("no_view") != ignored {
		t.Fatalf("its next Slot: ran %v, left %v, ignored a word %v times; want it run with no word, every State written",
			ran() > before, full.skipped()-skipped, full.ignored("epoch_mismatch")+full.ignored("no_view")-ignored)
	}
}

func ownershipOf(t *testing.T, bundle *phaseTwoWorkerBundle) *productionPhaseTwoOwnership {
	t.Helper()
	production, ok := bundle.dependencies.Ownership.(*productionPhaseTwoOwnership)
	if !ok {
		t.Fatalf("ownership %T", bundle.dependencies.Ownership)
	}
	return production
}
