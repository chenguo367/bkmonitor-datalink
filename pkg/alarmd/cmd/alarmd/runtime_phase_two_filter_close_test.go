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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/filterclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// stubControlPlane answers the link's management routes: a search finds
// nothing, a filtered close matches the alerts given, or every route
// answers 501 (storage that cannot search). It records each request's body.
type stubControlPlane struct {
	mu          sync.Mutex
	unsupported bool
	// busy answers every request 429.
	busy    bool
	matches []string
	bodies  map[string][]string
}

func (stub *stubControlPlane) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.bodies[r.URL.Path] = append(stub.bodies[r.URL.Path], string(body))
	if stub.unsupported {
		http.Error(w, "alert search requires Elasticsearch storage", http.StatusNotImplemented)
		return
	}
	if stub.busy {
		http.Error(w, "alert search busy", http.StatusTooManyRequests)
		return
	}
	switch r.URL.Path {
	case "/api/v1/alerts/search":
		_, _ = w.Write([]byte(`{"items":[],"next":""}`))
	case "/api/v1/alerts/close":
		var request struct {
			DryRun bool `json:"dry_run"`
		}
		_ = json.Unmarshal(body, &request)
		items := []map[string]any{}
		for _, id := range stub.matches {
			items = append(items, map[string]any{"alert_id": id, "status": 200})
		}
		if !request.DryRun {
			stub.matches = nil
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "next": "", "dry_run": request.DryRun})
	default:
		http.NotFound(w, r)
	}
}

func (stub *stubControlPlane) requests(path string) []string {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return append([]string(nil), stub.bodies[path]...)
}

// newStubFilterRoute is the production closer, built from configuration as
// the bundle builds it, against the stub and a real runtime Redis.
func newStubFilterRoute(t *testing.T, stub *stubControlPlane) *filterRoute {
	t.Helper()
	server := redistest.Start(t)
	if server == nil {
		t.Skip("no redis-server")
	}
	plane := httptest.NewServer(stub)
	t.Cleanup(plane.Close)
	var cfg config.Config
	cfg.Redis.StatePrefix = "alarmd-test"
	cfg.PhaseTwo.Linkd.ControlPlane = config.LinkdControlPlaneConfig{URL: plane.URL, JWTSecret: "example-secret"}
	closer, err := newFilterCloser(cfg, server.Client(), time.Now, nil)
	if err != nil || closer == nil {
		t.Fatalf("closer %v (%v)", closer, err)
	}
	return newFilterRoute(cfg, closer, nil)
}

// While the link serves its search, a strategy's edit close goes through it
// and reads no listing or record from the Console; a link that answers 501
// leaves the strategy to the Console path in the same round.
func TestTheEditCloseTakesTheLinksSearchWhileItIsInForce(t *testing.T) {
	stub := &stubControlPlane{bodies: map[string][]string{}}
	fixture := newEditFixture()
	fixture.loop.filter = newStubFilterRoute(t, stub)
	plan := editPlan()
	plan.Levels = plan.Levels[:2]
	fixture.plans.facts["10"] = plan
	fixture.threshold("old", 8, "info", "ip", "device")
	fixture.step()
	searches := stub.requests("/api/v1/alerts/search")
	if len(searches) == 0 || len(fixture.link.listings) != 0 || fixture.link.reads != 0 {
		t.Fatalf("searches %d, Console listings %v, records %d", len(searches), fixture.link.listings, fixture.link.reads)
	}
	for _, field := range []string{`"bk_tenant_id":"system"`, `"event_source_id":"built_in_bk"`, `"labels.strategy_id":"10"`, `"labels.strategy_version":"9"`} {
		if !strings.Contains(searches[0], field) {
			t.Fatalf("search %s does not carry %s", searches[0], field)
		}
	}
	stub.unsupported = true
	fixture.loop.after = absentalerts.Key{}
	fixture.step()
	if len(fixture.link.listings) != 1 || fixture.closed()["old"].Detail != "level_removed" {
		t.Fatalf("after a 501: listings %v, closes %+v", fixture.link.listings, fixture.closed())
	}
	if fixture.loop.filter.inForce() {
		t.Fatal("the route is still in force after a 501")
	}
}

// absentTestConsole counts what the absent close asks the Console.
type absentTestConsole struct{ reconciles int }

func (console *absentTestConsole) Roster(context.Context, string) (openalerts.RosterPage, error) {
	return openalerts.RosterPage{}, nil
}

func (console *absentTestConsole) Reconcile(context.Context, openalerts.StrategyKey) (openalerts.Reconciliation, error) {
	console.reconciles++
	return openalerts.Reconciliation{}, nil
}

func (console *absentTestConsole) AlertRecord(context.Context, string, string) (openalerts.AlertRecord, error) {
	return openalerts.AlertRecord{}, nil
}

// An absent strategy's alerts close through the link's filter with the
// roster's count as the expectation, without a Console listing; past the
// expectation nothing closes.
func TestAnAbsentStrategyClosesThroughTheLinksFilter(t *testing.T) {
	stub := &stubControlPlane{bodies: map[string][]string{}, matches: []string{"alert-1", "alert-2"}}
	bundle := &phaseTwoWorkerBundle{dependencies: phaseTwoWorkerBundleDependencies{Now: time.Now}}
	console := &absentTestConsole{}
	loop := newAbsentStrategyClose(bundle, nil, console, &absentTestWriter{})
	loop.filter = newStubFilterRoute(t, stub)
	key := absentalerts.Key{TenantID: "system", StrategyID: "10"}
	loop.lastRoster = map[absentalerts.Key]int{key: 2}
	execution := loop.closeStrategy(context.Background(), absentalerts.Absent{Key: key}, time.Now())
	closes := stub.requests("/api/v1/alerts/close")
	if execution.word != filterclose.OutcomeJobDone || console.reconciles != 0 || len(closes) != 2 ||
		!strings.Contains(closes[1], `"operation_source":"strategy_change"`) {
		t.Fatalf("word %s, Console reconciles %d, requests %v", execution.word, console.reconciles, closes)
	}
	stub.matches = make([]string, 40)
	for i := range stub.matches {
		stub.matches[i] = "alert-" + strings.Repeat("x", i+1)
	}
	if execution := loop.closeStrategy(context.Background(), absentalerts.Absent{Key: key}, time.Now()); execution.word != filterclose.OutcomeExpectExceeded {
		t.Fatalf("word %s, want expect_exceeded past the roster's two", execution.word)
	}
}

// Without the control plane's URL and secret there is no closer and the
// decisions stay on the existing paths; the route is out of force.
func TestWithoutTheControlPlaneThereIsNoFilterRoute(t *testing.T) {
	var cfg config.Config
	cfg.Redis.StatePrefix = "alarmd-test"
	cfg.PhaseTwo.Linkd.ControlPlane.URL = "http://192.0.2.10:8090"
	closer, err := newFilterCloser(cfg, nil, time.Now, nil)
	if err != nil || closer != nil || newFilterRoute(cfg, closer, nil).inForce() {
		t.Fatalf("closer %v (%v)", closer, err)
	}
}

// Closes decided series by series - an inactive strategy's listed alerts,
// a target out of scope - go through the link's filter per strategy and
// decision, by fingerprint, while the route is in force; a link that
// cannot serves them through the producer; a busy link is a failed send,
// decided again next step, and the producer is not used.
func TestSeriesClosesGoThroughTheLinksFilterWhileInForce(t *testing.T) {
	stub := &stubControlPlane{bodies: map[string][]string{}}
	route := newStubFilterRoute(t, stub)
	fallback := &maintenanceTestWriter{}
	writer := withFilterClose(route, fallback)
	batch := []linkdoutput.CloseRequest{
		{TenantID: "system", StrategyID: 10, StrategyRevision: 3, Fingerprint: "f1", AlertInstanceID: "a1"},
		{TenantID: "system", StrategyID: 10, StrategyRevision: 3, Fingerprint: "f2", AlertInstanceID: "a2"},
		{TenantID: "system", StrategyID: 11, StrategyRevision: 4, Fingerprint: "f3", Reason: linkdoutput.CloseReasonTargetOutOfScope},
	}
	if err := writer.WriteCloseBatch(context.Background(), batch); err != nil || len(fallback.batches) != 0 {
		t.Fatalf("err %v, producer batches %d", err, len(fallback.batches))
	}
	dryRuns := stub.requests("/api/v1/alerts/close")
	if len(dryRuns) != 2 || !strings.Contains(dryRuns[0], `"terms":{"fingerprint":["f1","f2"]}`) || !strings.Contains(dryRuns[0], `"labels.strategy_id":"10"`) ||
		!strings.Contains(dryRuns[1], `"terms":{"fingerprint":["f3"]}`) || !strings.Contains(dryRuns[1], `"labels.strategy_id":"11"`) {
		t.Fatalf("requests %v", dryRuns)
	}
	stub.busy = true
	if err := writer.WriteCloseBatch(context.Background(), batch); err == nil || len(fallback.batches) != 0 {
		t.Fatalf("a busy link: err %v, producer batches %d", err, len(fallback.batches))
	}
	stub.busy, stub.unsupported = false, true
	if err := writer.WriteCloseBatch(context.Background(), batch); err != nil || len(fallback.batches) != 1 || len(fallback.batches[0]) != 3 {
		t.Fatalf("after a 501: err %v, producer batches %v", err, fallback.batches)
	}
	asked := len(stub.requests("/api/v1/alerts/close"))
	if err := writer.WriteCloseBatch(context.Background(), batch); err != nil || len(fallback.batches) != 2 ||
		len(stub.requests("/api/v1/alerts/close")) != asked {
		t.Fatalf("out of force: err %v, producer batches %d, the link asked again", err, len(fallback.batches))
	}
}

// Closed through the link's filter, a close ends the alert id active now:
// the record of reopens is not consulted (L40) by the scope close's guard
// or the inactive close's hold.
func TestTheFilterRouteDoesNotConsultTheReopenRecord(t *testing.T) {
	route := newStubFilterRoute(t, &stubControlPlane{bodies: map[string][]string{}})
	// A record whose Redis is gone holds every close (recovery_unknown)
	// wherever it is consulted.
	server := redistest.Start(t)
	recoveries, err := openalerts.NewRecoveries(server.Client(), "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	server.Stop()
	if word := recoveryGuard(nil, recoveries, nil)(context.Background(), openalerts.StrategyKey{TenantID: "system", StrategyID: "10"}, "f", false); word == "" {
		t.Fatal("the fixture's record holds nothing; the test exercises nothing")
	}
	if word := recoveryGuard(nil, recoveries, route)(context.Background(), openalerts.StrategyKey{TenantID: "system", StrategyID: "10"}, "f", false); word != "" {
		t.Fatalf("guard held %s", word)
	}
	m := &effectiveMaintenance{recoveries: recoveries, filter: route}
	batch := []linkdoutput.CloseRequest{{Fingerprint: "f"}}
	if kept := m.holdReopened(context.Background(), "qg", controlplane.MaintenancePlan{}, openalerts.StrategyKey{}, batch, false); len(kept) != 1 {
		t.Fatalf("kept %v", kept)
	}
}
