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
	"fmt"
	"sync"
	"time"
)

// The overview: whether this deployment detects what it can, on time and
// whole, in a few conclusions and numbers for a page that shows nothing
// else. It is a projection of the health route's answer, cached for
// OverviewMaxAge, and carries no address, key, Query Group or Slot: detail
// is for the CLI.

// OverviewMaxAge is how long one computation answers; OverviewMaxBytes
// bounds the answer at the largest fleet.
const (
	OverviewMaxAge   = 15 * time.Second
	OverviewMaxBytes = 256 << 10
	// overviewComputeTimeout bounds one computation, which no caller's
	// context does: they all wait on it.
	overviewComputeTimeout = 10 * time.Second
)

// The states a conclusion, a stage or a dependency is in, closed.
const (
	OverviewOK            = "ok"
	OverviewAttention     = "attention"
	OverviewUnknown       = "unknown"
	OverviewNotConfigured = "not_configured"
)

// OverviewNumber is a number with its unit, or the fact that it is not
// measured: never a zero standing in for "not known".
type OverviewNumber struct {
	Value    float64 `json:"value"`
	Unit     string  `json:"unit,omitempty"`
	Measured bool    `json:"measured"`
}

func overviewMeasured(value float64, unit string) OverviewNumber {
	return OverviewNumber{Value: value, Unit: unit, Measured: true}
}

func overviewUnmeasured(unit string) OverviewNumber { return OverviewNumber{Unit: unit} }

type Overview struct {
	GeneratedAt   time.Time          `json:"generated_at"`
	MaxAgeSeconds int                `json:"max_age_seconds"`
	Build         OverviewBuild      `json:"build"`
	Verdict       OverviewVerdict    `json:"verdict"`
	Capability    OverviewCapability `json:"capability"`
	Flow          []OverviewStage    `json:"flow"`
	Resources     OverviewResources  `json:"resources"`
	Runtime       OverviewRuntime    `json:"runtime"`
}

type OverviewBuild struct {
	Versions []OverviewVersion `json:"versions"`
	// Rolling is more than one build answering at once.
	Rolling bool `json:"rolling"`
}

type OverviewVersion struct {
	Version  string `json:"version"`
	Commit   string `json:"commit"`
	Replicas int    `json:"replicas"`
}

type OverviewVerdict struct {
	State string `json:"state"`
	Line  string `json:"line"`
	// Stage is the part of the flow a degradation is about: pull,
	// evaluate, state, output or resources; empty when none is named.
	Stage string `json:"stage,omitempty"`
}

// OverviewReasonRows is how many refusal reasons the overview names; the
// rest are counted.
const OverviewReasonRows = 5

// OverviewCapability is the strategy funnel and the capability matrix: the
// Leader's published count of strategies per area and outcome
// (SourceFacts.Capability), unmeasured on a Leader that did not publish it.
type OverviewCapability struct {
	Funnel          OverviewFunnel          `json:"funnel"`
	Measured        bool                    `json:"measured"`
	Matrix          []OverviewCapabilityRow `json:"matrix"`
	Reasons         []OverviewReason        `json:"reasons"`
	ReasonsOmitted  OverviewOmitted         `json:"reasons_omitted"`
	IgnoredByDesign []OverviewIgnoredReason `json:"ignored_by_design"`
}

type OverviewCapabilityRow struct {
	Area                string `json:"area"`
	Supported           int    `json:"supported"`
	Partial             int    `json:"partial"`
	UnsupportedByDesign int    `json:"unsupported_by_design"`
	NotYetSupported     int    `json:"not_yet_supported"`
}

// OverviewReason is one refusal reason with what it means, in the words the
// page shows for it elsewhere.
type OverviewReason struct {
	Code       string   `json:"code"`
	Area       string   `json:"area"`
	Outcome    string   `json:"outcome"`
	What       string   `json:"what"`
	Strategies int      `json:"strategies"`
	SampleIDs  []string `json:"sample_ids"`
}

type OverviewOmitted struct {
	Kinds      int `json:"kinds"`
	Strategies int `json:"strategies"`
}

type OverviewIgnoredReason struct {
	Code       string `json:"code"`
	Strategies int    `json:"strategies"`
}

// OverviewFunnel is the strategy cache's count, how many of them became
// Plans, and how many are detecting -- the last from the replicas' strategy
// states, unmeasured when they did not report them.
type OverviewFunnel struct {
	Listed    int            `json:"listed"`
	Accepted  int            `json:"accepted"`
	Detecting OverviewNumber `json:"detecting"`
}

// OverviewStage is one part of the flow -- pull, evaluate, state, output --
// with its volume, failures and latency, and the dependencies it needs.
type OverviewStage struct {
	Stage        string               `json:"stage"`
	State        string               `json:"state"`
	Volume       OverviewNumber       `json:"volume"`
	Failures     OverviewNumber       `json:"failures"`
	LatencyP95MS OverviewNumber       `json:"latency_p95_ms"`
	Dependencies []OverviewDependency `json:"dependencies"`
}

type OverviewDependency struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type OverviewResources struct {
	Scale    OverviewScale             `json:"scale"`
	Replicas []OverviewReplicaResource `json:"replicas"`
	// Constraint is the load judgment's bottleneck, as the health route
	// decides it.
	Constraint string `json:"constraint"`
}

type OverviewScale struct {
	Strategies    OverviewNumber `json:"strategies"`
	MergedQueries OverviewNumber `json:"merged_queries"`
	Series        OverviewNumber `json:"series"`
	Hosts         OverviewNumber `json:"hosts"`
}

type OverviewReplicaResource struct {
	Name                string          `json:"name"`
	CPURatio            OverviewNumber  `json:"cpu_ratio"`
	MemoryRatio         OverviewNumber  `json:"memory_ratio"`
	LiveHeapBytes       OverviewNumber  `json:"live_heap_bytes"`
	QueryPermits        OverviewPermits `json:"query_permits"`
	CPUThrottledSeconds OverviewNumber  `json:"cpu_throttled_seconds"`
}

type OverviewPermits struct {
	Held  int `json:"held"`
	Limit int `json:"limit"`
}

type OverviewRuntime struct {
	Replicas  []OverviewReplica `json:"replicas"`
	Attention []OverviewNotice  `json:"attention"`
}

type OverviewReplica struct {
	Name          string  `json:"name"`
	Version       string  `json:"version"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	Ready         bool    `json:"ready"`
	MergedQueries int     `json:"merged_queries"`
	State         string  `json:"state"`
}

type OverviewNotice struct {
	Kind string `json:"kind"`
	Line string `json:"line"`
}

// overviewStages is the flow in order, and overviewStageOf the stage each
// dependency role hangs on: the query backend and the host cache feed the
// pull, the strategy and target sources the evaluation, the runtime store
// the state, and Kafka and the alert link the output.
var overviewStages = []string{"pull", "evaluate", "state", "output"}

var overviewStageOf = map[string]string{
	"query_backend": "pull", "cmdb_cache": "pull",
	"strategy_cache": "evaluate", "dynamic_config": "evaluate", "target_group": "evaluate",
	"state_redis":  "state",
	"output_kafka": "output", "compat_output_redis": "output", "linkd_console": "output", "open_alert_set": "output",
}

// overviewDegradationStage is the stage each degradation is about; one not
// listed is the evaluation's, which every other one ends up holding back.
var overviewDegradationStage = map[DegradationKind]string{
	DegradationOpenAlertSetStale:       "output",
	DegradationOpenAlertSetUnconfirmed: "output",
}

// overviewOf projects the health route's answer, and the replicas'
// strategy states when they reported them, onto the overview.
func overviewOf(health HealthResponse, running map[StateWord]int, runningKnown bool, at time.Time) Overview {
	overview := Overview{GeneratedAt: at.UTC(), MaxAgeSeconds: int(OverviewMaxAge / time.Second),
		Flow: make([]OverviewStage, 0, len(overviewStages))}
	for _, group := range health.Builds {
		overview.Build.Versions = append(overview.Build.Versions, OverviewVersion{Version: group.Build.Version, Commit: group.Build.Commit, Replicas: len(group.Replicas)})
	}
	overview.Build.Rolling = len(overview.Build.Versions) > 1
	if overview.Build.Versions == nil {
		overview.Build.Versions = []OverviewVersion{}
	}

	funnel := OverviewFunnel{Detecting: overviewUnmeasured("strategies")}
	if standing := health.SourceStanding; standing != nil {
		funnel.Listed, funnel.Accepted = standing.Listed, standing.Accepted
	}
	if runningKnown {
		funnel.Detecting = overviewMeasured(float64(running[StateDetecting]), "strategies")
	}
	overview.Capability = overviewCapabilityOf(health.Source)
	overview.Capability.Funnel = funnel
	overview.Verdict = overviewVerdictOf(health, funnel)

	degradedReplicas := map[string]bool{}
	for _, degradation := range health.Degradations {
		degradedReplicas[degradation.Replica] = true
	}
	for _, stage := range overviewStages {
		row := OverviewStage{Stage: stage, State: OverviewOK, Volume: overviewUnmeasured("per_second"), Failures: overviewUnmeasured("count"),
			LatencyP95MS: overviewUnmeasured("ms"), Dependencies: []OverviewDependency{}}
		for _, endpoint := range health.Dependencies {
			if overviewStageOf[endpoint.Role] != stage {
				continue
			}
			dependency := OverviewDependency{Name: endpoint.Role, State: endpointState(endpoint)}
			row.Dependencies = append(row.Dependencies, dependency)
			if dependency.State == OverviewAttention {
				row.State = OverviewAttention
			}
		}
		if stage == "evaluate" && health.Schedule != nil {
			row.Volume = overviewMeasured(float64(health.Schedule.Completed1h)/3600, "per_second")
			row.Failures = overviewMeasured(float64(health.Schedule.Late+health.Schedule.Overdue), "count")
			if health.Schedule.Overdue > 0 {
				row.State = OverviewAttention
			}
		}
		if overview.Verdict.Stage == stage {
			row.State = OverviewAttention
		}
		overview.Flow = append(overview.Flow, row)
	}

	overview.Resources = OverviewResources{Constraint: string(health.Load.Bottleneck.Resource), Replicas: []OverviewReplicaResource{},
		Scale: OverviewScale{Strategies: overviewUnmeasured("strategies"), MergedQueries: overviewUnmeasured("merged_queries"), Series: overviewUnmeasured("series"), Hosts: overviewUnmeasured("hosts")}}
	if health.SourceStanding != nil {
		overview.Resources.Scale.Strategies = overviewMeasured(float64(health.SourceStanding.Accepted), "strategies")
	}
	if health.Expected != nil {
		overview.Resources.Scale.MergedQueries = overviewMeasured(float64(*health.Expected), "merged_queries")
	}
	overview.Runtime = OverviewRuntime{Replicas: []OverviewReplica{}, Attention: []OverviewNotice{}}
	for _, replica := range health.PerReplica {
		resource := OverviewReplicaResource{Name: replica.Replica, CPURatio: overviewUnmeasured("ratio"), MemoryRatio: overviewUnmeasured("ratio"),
			LiveHeapBytes: overviewUnmeasured("bytes"), CPUThrottledSeconds: overviewUnmeasured("seconds")}
		if capacity := replica.Capacity; capacity != nil {
			resource.QueryPermits = OverviewPermits{Held: capacity.PermitsHeld, Limit: capacity.PermitBudget}
			if capacity.MemoryLimit > 0 {
				resource.MemoryRatio = overviewMeasured(float64(capacity.MemoryUsed)/float64(capacity.MemoryLimit), "ratio")
			}
			if capacity.ThrottledKnown {
				resource.CPUThrottledSeconds = overviewMeasured(capacity.ThrottledSeconds, "seconds")
			}
		}
		overview.Resources.Replicas = append(overview.Resources.Replicas, resource)
		row := OverviewReplica{Name: replica.Replica, UptimeSeconds: replica.UptimeSeconds, MergedQueries: replica.Owned, State: OverviewOK}
		if replica.Build != nil {
			row.Version = replica.Build.Version
		}
		if replica.Readiness != nil {
			row.Ready = replica.Readiness.Ready
		}
		if degradedReplicas[replica.Replica] || !row.Ready {
			row.State = OverviewAttention
		}
		overview.Runtime.Replicas = append(overview.Runtime.Replicas, row)
	}
	if activation := health.Activation; activation != nil && activation.BehindBeyondBound {
		overview.Runtime.Attention = append(overview.Runtime.Attention, OverviewNotice{Kind: "config_behind",
			Line: fmt.Sprintf("已发布的策略配置还没在检测里生效：连续 %d 轮没有应用成功", activation.ConsecutiveFailures)})
	}
	return overview
}

func overviewVerdictOf(health HealthResponse, funnel OverviewFunnel) OverviewVerdict {
	switch health.Health {
	case HealthHealthy:
		line := fmt.Sprintf("%d 条可用策略都在检测范围内", funnel.Accepted)
		if funnel.Detecting.Measured {
			line = fmt.Sprintf("%d 条可用策略中 %d 条在检测，其余在等数据或按配置不检测", funnel.Accepted, int(funnel.Detecting.Value))
		}
		return OverviewVerdict{State: OverviewOK, Line: line}
	case HealthDegraded:
		verdict := OverviewVerdict{State: OverviewAttention, Line: "检测需要关注：alarmd 自己有问题要处理", Stage: "evaluate"}
		if len(health.Degradations) > 0 {
			first := health.Degradations[0]
			if stage, named := overviewDegradationStage[first.Kind]; named {
				verdict.Stage = stage
			}
			verdict.Line = fmt.Sprintf("检测需要关注：%d 项运行状态超出设计界", len(health.Degradations))
		}
		return verdict
	default:
		return OverviewVerdict{State: OverviewUnknown, Line: "有策略现在说不出状态：刚启动或刚滚动时在预热，或者有副本没报告"}
	}
}

// endpointState is a dependency's state on the overview: not configured,
// failing (its latest word is a failure), working, or not yet known.
func endpointState(endpoint Endpoint) string {
	switch {
	case !endpoint.Configured:
		return OverviewNotConfigured
	case endpoint.LastFailureAgeSeconds != nil && (endpoint.LastSuccessAgeSeconds == nil || *endpoint.LastFailureAgeSeconds < *endpoint.LastSuccessAgeSeconds):
		return OverviewAttention
	case endpoint.LastSuccessAgeSeconds != nil || (endpoint.Ready != nil && *endpoint.Ready):
		return OverviewOK
	default:
		return OverviewUnknown
	}
}

// overviewCache answers from one computation for OverviewMaxAge; callers
// arriving while it is computed wait for it rather than computing again.
type overviewCache struct {
	now     func() time.Time
	compute func() Overview

	mu       sync.Mutex
	value    Overview
	at       time.Time
	inFlight *sync.WaitGroup
}

func newOverviewCache(now func() time.Time, compute func() Overview) *overviewCache {
	return &overviewCache{now: now, compute: compute}
}

func (cache *overviewCache) get() Overview {
	cache.mu.Lock()
	for {
		if !cache.at.IsZero() && cache.now().Sub(cache.at) < OverviewMaxAge {
			value := cache.value
			cache.mu.Unlock()
			return value
		}
		if cache.inFlight == nil {
			break
		}
		wait := cache.inFlight
		cache.mu.Unlock()
		wait.Wait()
		cache.mu.Lock()
	}
	wait := &sync.WaitGroup{}
	wait.Add(1)
	cache.inFlight = wait
	cache.mu.Unlock()
	var value Overview
	computed := false
	// Released however compute ends: a computation that panics caches
	// nothing, its waiters compute again, and the panic goes on to its own
	// caller -- left set, every later caller would wait on it forever,
	// each holding a request slot of the channel.
	defer func() {
		cache.mu.Lock()
		cache.inFlight = nil
		if computed {
			cache.value, cache.at = value, cache.now()
		}
		cache.mu.Unlock()
		wait.Done()
	}()
	value = cache.compute()
	value.MaxAgeSeconds = int(OverviewMaxAge / time.Second)
	computed = true
	return value
}

func overviewCapabilityOf(source *SourceFacts) OverviewCapability {
	capability := OverviewCapability{Matrix: []OverviewCapabilityRow{}, Reasons: []OverviewReason{}, IgnoredByDesign: []OverviewIgnoredReason{}}
	if source == nil || source.Capability == nil {
		return capability
	}
	capability.Measured = true
	capability.Matrix = make([]OverviewCapabilityRow, len(CapabilityAreas))
	rows := make(map[string]*OverviewCapabilityRow, len(CapabilityAreas))
	for index, area := range CapabilityAreas {
		capability.Matrix[index].Area = area
		rows[area] = &capability.Matrix[index]
	}
	for _, cell := range source.Capability.Cells {
		row := rows[cell.Area]
		if row == nil {
			continue
		}
		switch cell.Outcome {
		case CapabilitySupported:
			row.Supported = cell.Strategies
		case CapabilityPartial:
			row.Partial = cell.Strategies
		case CapabilityUnsupportedByDesign:
			row.UnsupportedByDesign = cell.Strategies
		case CapabilityNotYetSupported:
			row.NotYetSupported = cell.Strategies
		}
	}
	for index, reason := range source.Capability.Reasons {
		if index >= OverviewReasonRows {
			capability.ReasonsOmitted.Kinds++
			capability.ReasonsOmitted.Strategies += reason.Strategies
			continue
		}
		capability.Reasons = append(capability.Reasons, OverviewReason{Code: reason.Code, Area: reason.Area, Outcome: reason.Outcome,
			What: WithheldWordsOf(reason.Code).What, Strategies: reason.Strategies, SampleIDs: reason.SampleIDs})
	}
	for _, reason := range source.Capability.IgnoredByDesign {
		capability.IgnoredByDesign = append(capability.IgnoredByDesign, OverviewIgnoredReason{Code: reason.Code, Strategies: reason.Strategies})
	}
	return capability
}
