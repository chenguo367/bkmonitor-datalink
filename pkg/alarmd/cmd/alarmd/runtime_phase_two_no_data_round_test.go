// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
)

// recorded is what the sink has taken so far.
func (sink *recordingPhaseTwoEventSink) recorded() []contract.TriggerEventV1 {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]contract.TriggerEventV1(nil), sink.events...)
}

// noDataContinuous is how many consecutive absent periods the fixture's
// strategy requires before it says anything.
const noDataContinuous = 2

// A strategy with no data reports it, and stops reporting it when the data
// comes back -- under one identity, through the real bundle, against Redis.
//
// This is the question every other no-data test leaves open. The pieces each
// have their own: the group's identity is hashed correctly, the event carries
// the right fields, the tag reaches both protocols. None of them answers
// whether an alert raised this way can ever be closed, and that is the only
// thing that decides if the capability is usable: an alert that opens and
// never closes is worse than one that never opens.
//
// It closes by identity, not by a message. Nothing sends a "clear"; the group
// stops producing anomalies, and the other side pairs the silence with the
// alert through the dimensions md5. So the assertion is that the record the
// recovering round produces is the same object as the one the alerting round
// produced -- same identity digest, and the same anomaly_id up to the period.
// Two objects that differ by anything leave the alert open for ever, and
// nothing anywhere reports it.
func TestNoDataOpensAnAlertAndTheReturningDataClosesIt(t *testing.T) {
	fixture := startNoDataFixture(t)
	ctx := context.Background()

	// Rounds with nothing to read. Each one judges the whole-item group absent.
	var abnormal *contract.TriggerEventV1
	for round := int64(1); round <= noDataContinuous+2 && abnormal == nil; round++ {
		fixture.runSlot(ctx, round)
		for index := range fixture.events.recorded() {
			event := fixture.events.recorded()[index]
			if event.EventKind == contract.TriggerEventAbnormal && noDataTagged(event) {
				abnormal = &event
				break
			}
		}
	}
	if abnormal == nil {
		t.Fatalf("no no-data anomaly after %d absent rounds with continuous=%d; a strategy that never "+
			"reports the silence is a capability that does nothing", noDataContinuous+2, noDataContinuous)
	}
	if abnormal.PrimaryLevelID != noDataConfiguredLevelID {
		t.Fatalf("the anomaly is at level %d, want the configured no-data level %d: that number is the "+
			"last segment of the anomaly_id and decides which alert it pairs with",
			abnormal.PrimaryLevelID, noDataConfiguredLevelID)
	}

	// The data comes back. The same group is now present, and the round that
	// sees it must produce a record under the same identity -- that is what
	// the other side pairs with the open alert.
	fixture.dataReturns()
	var recovered *contract.TriggerEventV1
	for round := int64(1); round <= 3 && recovered == nil; round++ {
		fixture.runSlot(ctx, int64(len(fixture.events.recorded()))+int64(round)+noDataContinuous+2)
		for index := range fixture.events.recorded() {
			event := fixture.events.recorded()[index]
			if event.EventKind != contract.TriggerEventAbnormal && noDataTagged(event) {
				recovered = &event
				break
			}
		}
	}
	if recovered == nil {
		// This is the property the test exists for. A skip here would read as
		// "not exercised" on exactly the day the recovery path stopped working.
		t.Fatal("the returning data produced no closing record for the no-data group: the alert " +
			"the absence opened would stay open for ever")
	}
	if recovered.RecordRef.DimensionIdentityDigest != abnormal.RecordRef.DimensionIdentityDigest {
		t.Fatalf("the closing record is identity %q and the alerting record was %q. Nothing sends a "+
			"clear: the alert closes because the other side pairs the two by identity, so two digests "+
			"leave it open for ever",
			recovered.RecordRef.DimensionIdentityDigest, abnormal.RecordRef.DimensionIdentityDigest)
	}
}

func noDataTagged(event contract.TriggerEventV1) bool {
	_, tagged := event.RecordRef.Dimensions[contract.NoDataDimensionTag]
	return tagged
}

const noDataConfiguredLevelID = uint32(1)

type noDataFixture struct {
	t        *testing.T
	base     int64
	clock    *atomic.Int64
	hasData  *atomic.Bool
	runner   phaseTwoQueryGroupRuntime
	events   *recordingPhaseTwoEventSink
	interval int64
}

func (fixture *noDataFixture) dataReturns() { fixture.hasData.Store(true) }

// runSlot moves the clock one period on and runs whatever is due.
func (fixture *noDataFixture) runSlot(ctx context.Context, round int64) {
	fixture.t.Helper()
	fixture.clock.Store((fixture.base + round*fixture.interval + fixture.interval/2) * 1000)
	for attempt := 0; attempt < 4; attempt++ {
		_, attempted, err := fixture.runner.RunOne(ctx)
		if err != nil {
			fixture.t.Fatalf("round %d RunOne error = %v", round, err)
		}
		if !attempted {
			return
		}
	}
}

func startNoDataFixture(t *testing.T) *noDataFixture {
	t.Helper()
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	installNoDataStrategy(t, ctx, redisClient)

	const interval = int64(60)
	base := time.Now().Unix()
	base += interval - base%interval
	fixture := &noDataFixture{t: t, base: base, clock: &atomic.Int64{}, hasData: &atomic.Bool{}, interval: interval}
	fixture.clock.Store(base * 1000)
	now := func() time.Time { return time.UnixMilli(fixture.clock.Load()) }

	uqServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			EndTime string `json:"end_time"`
		}
		_ = json.NewDecoder(request.Body).Decode(&payload)
		end, err := strconv.ParseInt(payload.EndTime, 10, 64)
		if err != nil || end <= 0 {
			end = fixture.clock.Load() / 1000
		}
		if end > 1_000_000_000_000 {
			end /= 1000
		}
		series := ""
		if fixture.hasData.Load() {
			series = `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],` +
				`"group_keys":["host"],"group_values":["127.0.0.1"],"values":[[` +
				strconv.FormatInt((end-1)*1000, 10) + `,5]]}`
		}
		_, _ = writer.Write([]byte(`{"series":[` + series + `],"status":null,"trace_id":"no-data-round",` +
			`"is_partial":false,"result_table_id":["system.cpu"]}`))
	}))
	t.Cleanup(uqServer.Close)

	cfg := validGoAccessRuntimeConfig()
	cfg.Redis.Address = address
	cfg.Redis.StatePrefix = "alarmd-no-data-round"
	withCompatibilityOutput(&cfg, address)
	cfg.PhaseTwo.Control.RefreshInterval = config.Duration(time.Millisecond)
	cfg.PhaseTwo.Access.UQEndpoint = uqServer.URL
	cfg.PhaseTwo.Worker.RegistrationTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Worker.RegistrationRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.ControlLeaderTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Ownership.ControlLeaderRenewInterval = config.Duration(time.Minute)
	cfg.PhaseTwo.Ownership.LeaseTTL = config.Duration(6 * time.Hour)
	cfg.PhaseTwo.Ownership.LeaseRenewInterval = config.Duration(time.Minute)

	events := &recordingPhaseTwoEventSink{}
	bundle, err := openProductionPhaseTwoBundleWithDependencies(
		ctx, cfg, metric.NewRecorder(metric.BuildInfo{}), observability.Discard(observability.ComponentRuntime),
		newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{
			Now: now, HTTPClient: uqServer.Client(),
			OpenEvents: func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) { return events, nil },
		},
	)
	if err != nil {
		t.Fatalf("openProductionPhaseTwoBundleWithDependencies() error = %v", err)
	}
	if err := bundle.Start(ctx); err != nil {
		t.Fatalf("phase-two production Start() error = %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := bundle.Shutdown(context.Background()); shutdownErr != nil {
			t.Errorf("phase-two production Shutdown() error = %v", shutdownErr)
		}
	})
	if len(bundle.queryGroups) != 1 {
		t.Fatalf("Query Groups = %v, want one", bundle.queryGroups)
	}
	queryGroup := bundle.queryGroups[0]
	fixture.runner = bundle.runners[queryGroup].runner
	fixture.events = events

	// The Plan must actually carry no-data detection, or every round below
	// would pass by judging nothing at all.
	production := bundle.dependencies.Ownership.(*productionPhaseTwoOwnership)
	schedule, err := production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, queryGroup)
	if err != nil || len(schedule.Plans) != 1 {
		t.Fatalf("initial schedule=%+v error=%v", schedule, err)
	}
	return fixture
}

func installNoDataStrategy(t *testing.T, ctx context.Context, redisClient *redis.Client) {
	t.Helper()
	raw, err := os.ReadFile("testdata/g1_full_threshold_strategy.json")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["update_time"] = 1725000000
	item := document["items"].([]any)[0].(map[string]any)
	item["query_configs"].([]any)[0].(map[string]any)["agg_interval"] = 60
	// The whole-item group: no aggregation dimensions, which is the group the
	// backend reports when an item has no data at all. It needs no roster
	// history, so the first absent round already has something to judge.
	item["no_data_config"] = map[string]any{
		"is_enabled": true, "continuous": noDataContinuous,
		"level": noDataConfiguredLevelID, "agg_dimension": []any{},
	}
	for _, detect := range document["detects"].([]any) {
		detect.(map[string]any)["trigger_config"].(map[string]any)["check_window"] = 1
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{
		"alarm-config.strategy_ids":  `[1001]`,
		"alarm-config.strategy_1001": encoded,
	} {
		if err := redisClient.Set(ctx, key, value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
