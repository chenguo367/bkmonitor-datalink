// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func overviewHealth() HealthResponse {
	expected := 36
	used, limit := uint64(180<<20), uint64(1<<30)
	ready := true
	success, failure := 5.0, 60.0
	return HealthResponse{
		Health: HealthHealthy, Expected: &expected,
		Builds:         []BuildGroup{{Build: BuildFacts{Version: "0.4.10", Commit: "abc1234"}, Replicas: []string{"replica-a"}}},
		SourceStanding: &SourceStanding{Listed: 56, Accepted: 53, Executing: 36},
		Schedule:       &ScheduleCensus{Completed1h: 7200, OnTime1h: 7200, Late: 0, Overdue: 0},
		PerReplica: []ReplicaView{{Replica: "replica-a", Owned: 36, UptimeSeconds: 600,
			Build:     &BuildFacts{Version: "0.4.10", Commit: "abc1234"},
			Readiness: &ReadinessFacts{Ready: true},
			Capacity:  &Capacity{PermitsHeld: 1, PermitBudget: 16, MemoryUsed: used, MemoryLimit: limit, ThrottledKnown: true}}},
		Dependencies: []Endpoint{
			{Role: "query_backend", Address: "http://198.51.100.7:10205", Configured: true, Ready: &ready, LastSuccessAgeSeconds: &success, LastFailureAgeSeconds: &failure},
			{Role: "state_redis", Address: "192.0.2.10:6379", Configured: true, Ready: &ready, LastSuccessAgeSeconds: &success},
			{Role: "output_kafka", Address: "203.0.113.5:9092", Configured: false},
		},
	}
}

func flowStage(overview Overview, stage string) OverviewStage {
	for _, row := range overview.Flow {
		if row.Stage == stage {
			return row
		}
	}
	return OverviewStage{}
}

// The verdict is the health route's, said once in a sentence, with the
// stage a degradation is about; unknown is its own state.
func TestTheOverviewVerdictIsTheHealthVerdictInOneLine(t *testing.T) {
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	running := map[StateWord]int{StateDetecting: 50, StateDataAbsent: 3}
	ok := overviewOf(overviewHealth(), running, true, at)
	if ok.Verdict.State != OverviewOK || !strings.Contains(ok.Verdict.Line, "53") || !strings.Contains(ok.Verdict.Line, "50") {
		t.Fatalf("healthy verdict = %+v, want ok naming 53 usable and 50 detecting", ok.Verdict)
	}
	degraded := overviewHealth()
	degraded.Health = HealthDegraded
	degraded.Degradations = []Degradation{{Kind: DegradationOpenAlertSetStale, Replica: "replica-a"}}
	if got := overviewOf(degraded, running, true, at).Verdict; got.State != OverviewAttention || got.Stage != "output" {
		t.Fatalf("degraded verdict = %+v, want attention at the output stage", got)
	}
	unknown := overviewHealth()
	unknown.Health = HealthUnknown
	if got := overviewOf(unknown, running, true, at).Verdict; got.State != OverviewUnknown || got.Line == "" {
		t.Fatalf("unknown verdict = %+v", got)
	}
}

// Each dependency hangs on its stage, by state and never by address, and a
// number the replicas do not report is unmeasured, never zero.
func TestTheOverviewFlowHangsDependenciesOnTheirStageAndSaysWhatIsUnmeasured(t *testing.T) {
	overview := overviewOf(overviewHealth(), nil, false, time.Now())
	pull := flowStage(overview, "pull")
	if len(pull.Dependencies) != 1 || pull.Dependencies[0].Name != "query_backend" || pull.Dependencies[0].State != OverviewOK {
		t.Fatalf("pull dependencies = %+v, want the query backend, ok", pull.Dependencies)
	}
	if state := flowStage(overview, "state"); len(state.Dependencies) != 1 || state.Dependencies[0].Name != "state_redis" {
		t.Fatalf("state dependencies = %+v", state.Dependencies)
	}
	if output := flowStage(overview, "output"); len(output.Dependencies) != 1 || output.Dependencies[0].State != OverviewNotConfigured {
		t.Fatalf("output dependencies = %+v, want the unconfigured Kafka said so", output.Dependencies)
	}
	if pull.Volume.Measured || pull.Failures.Measured || pull.LatencyP95MS.Measured {
		t.Fatalf("pull numbers = %+v, want unmeasured until the replicas report them", pull)
	}
	evaluate := flowStage(overview, "evaluate")
	if !evaluate.Volume.Measured || evaluate.Volume.Value != 2 || !evaluate.Failures.Measured || evaluate.Failures.Value != 0 {
		t.Fatalf("evaluate = %+v, want 7200 an hour as 2 a second and no late detections", evaluate)
	}
	encoded, err := json.Marshal(overview)
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"198.51.100.7", "192.0.2.10", "203.0.113.5"} {
		if strings.Contains(string(encoded), address) {
			t.Fatalf("the overview carries an address: %s", encoded)
		}
	}
	if overview.Capability.Funnel.Listed != 56 || overview.Capability.Funnel.Accepted != 53 || overview.Capability.Funnel.Detecting.Measured {
		t.Fatalf("funnel = %+v, want 56 listed, 53 accepted and detecting unmeasured without strategy counts", overview.Capability.Funnel)
	}
}

// Resources per replica as shares of their limits, with what is not known
// said so.
func TestTheOverviewResourcesAreSharesOfTheirLimits(t *testing.T) {
	overview := overviewOf(overviewHealth(), nil, false, time.Now())
	if len(overview.Resources.Replicas) != 1 {
		t.Fatalf("replicas = %+v", overview.Resources.Replicas)
	}
	replica := overview.Resources.Replicas[0]
	if !replica.MemoryRatio.Measured || replica.MemoryRatio.Value < 0.17 || replica.MemoryRatio.Value > 0.18 ||
		replica.QueryPermits.Held != 1 || replica.QueryPermits.Limit != 16 || replica.CPURatio.Measured || replica.LiveHeapBytes.Measured {
		t.Fatalf("replica resources = %+v", replica)
	}
	if overview.Resources.Scale.Strategies.Value != 53 || overview.Resources.Scale.MergedQueries.Value != 36 || overview.Resources.Scale.Hosts.Measured {
		t.Fatalf("scale = %+v", overview.Resources.Scale)
	}
	if overview.Runtime.Replicas[0].Version != "0.4.10" || !overview.Runtime.Replicas[0].Ready || overview.Runtime.Replicas[0].State != OverviewOK {
		t.Fatalf("runtime = %+v", overview.Runtime.Replicas[0])
	}
}

// The answer stays within its bound at the largest fleet.
func TestTheOverviewStaysWithinItsBoundAtSixtyFourReplicas(t *testing.T) {
	health := overviewHealth()
	base := health.PerReplica[0]
	health.PerReplica = nil
	for i := 0; i < 64; i++ {
		replica := base
		replica.Replica = fmt.Sprintf("alarmd-worker-%02d-0123456789abcdef", i)
		health.PerReplica = append(health.PerReplica, replica)
	}
	encoded, err := json.Marshal(overviewOf(health, map[StateWord]int{StateDetecting: 1}, true, time.Now()))
	if err != nil || len(encoded) > OverviewMaxBytes {
		t.Fatalf("overview is %d bytes (%v), want at most %d", len(encoded), err, OverviewMaxBytes)
	}
}

// The overview is computed at most once per freshness window, however many
// callers ask at once, and again after the window.
func TestTheOverviewIsComputedOncePerWindow(t *testing.T) {
	clock := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	var computed atomic.Int32
	cache := newOverviewCache(now, func() Overview {
		computed.Add(1)
		time.Sleep(10 * time.Millisecond)
		return Overview{GeneratedAt: now()}
	})
	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() { defer wait.Done(); cache.get() }()
	}
	wait.Wait()
	if computed.Load() != 1 {
		t.Fatalf("computed %d times for 100 concurrent callers, want once", computed.Load())
	}
	mu.Lock()
	clock = clock.Add(OverviewMaxAge - time.Second)
	mu.Unlock()
	if cache.get(); computed.Load() != 1 {
		t.Fatalf("recomputed within the window")
	}
	mu.Lock()
	clock = clock.Add(2 * time.Second)
	mu.Unlock()
	if got := cache.get(); computed.Load() != 2 || got.MaxAgeSeconds != int(OverviewMaxAge/time.Second) {
		t.Fatalf("after the window: computed %d, max age %d", computed.Load(), got.MaxAgeSeconds)
	}
}

// The capability matrix is the Leader's published per-strategy count, one
// row per area; the reasons list keeps the five largest and counts the
// rest, each with what it means. Without the count it is unmeasured.
func TestTheOverviewCapabilityIsThePublishedStrategyCount(t *testing.T) {
	health := overviewHealth()
	if got := overviewOf(health, nil, false, time.Now()).Capability; got.Measured || len(got.Matrix) != 0 {
		t.Fatalf("without a published count: %+v, want unmeasured", got)
	}
	cells := []CapabilityCell{}
	for _, area := range CapabilityAreas {
		for _, outcome := range CapabilityOutcomes {
			count := 0
			switch {
			case outcome == CapabilitySupported:
				count = 50
			case area == CapabilitySource && outcome == CapabilityNotYetSupported:
				count = 6
			}
			cells = append(cells, CapabilityCell{Area: area, Outcome: outcome, Strategies: count})
		}
	}
	reasons := []CapabilityReason{}
	for i := 0; i < 7; i++ {
		reasons = append(reasons, CapabilityReason{Code: fmt.Sprintf("QUERY_SOURCE_NOT_MIGRATED_%d", i), Area: CapabilitySource,
			Outcome: CapabilityNotYetSupported, Strategies: 7 - i, SampleIDs: []string{"79"}})
	}
	reasons[0].Code = "QUERY_SOURCE_NOT_MIGRATED"
	health.Source = &SourceFacts{Capability: &SourceCapability{Cells: cells, Reasons: reasons,
		IgnoredByDesign: []CapabilityReason{{Code: "PRIORITY_IGNORED", Strategies: 2}}}}
	got := overviewOf(health, nil, false, time.Now()).Capability
	if !got.Measured || len(got.Matrix) != len(CapabilityAreas) || got.Matrix[0].Area != CapabilitySource ||
		got.Matrix[0].Supported != 50 || got.Matrix[0].NotYetSupported != 6 {
		t.Fatalf("matrix = %+v", got.Matrix)
	}
	if len(got.Reasons) != OverviewReasonRows || got.Reasons[0].Code != "QUERY_SOURCE_NOT_MIGRATED" || got.Reasons[0].What == "" ||
		got.ReasonsOmitted.Kinds != 2 || got.ReasonsOmitted.Strategies != 2+1 {
		t.Fatalf("reasons = %+v omitted %+v", got.Reasons, got.ReasonsOmitted)
	}
	if len(got.IgnoredByDesign) != 1 || got.IgnoredByDesign[0].Strategies != 2 {
		t.Fatalf("ignored = %+v", got.IgnoredByDesign)
	}
}
