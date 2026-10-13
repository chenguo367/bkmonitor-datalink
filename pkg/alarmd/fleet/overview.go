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
	// AtLeast is a quantile past a histogram's last bound: the value is that
	// bound, and the truth is at least it.
	AtLeast bool `json:"at_least,omitempty"`
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
	// FlowCoverage is how many replicas the flow's numbers are from.
	FlowCoverage OverviewCoverage  `json:"flow_coverage"`
	Resources    OverviewResources `json:"resources"`
	Runtime      OverviewRuntime   `json:"runtime"`
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
	Stage        string         `json:"stage"`
	State        string         `json:"state"`
	Volume       OverviewNumber `json:"volume"`
	Failures     OverviewNumber `json:"failures"`
	LatencyP95MS OverviewNumber `json:"latency_p95_ms"`
	// FailureCauses splits the pull stage's failures by cause, per minute.
	FailureCauses []OverviewCauseCount `json:"failure_causes,omitempty"`
	// Basis names the counters the numbers are rates of, so they can be
	// read against the same counters' charts.
	Basis        string               `json:"basis"`
	Dependencies []OverviewDependency `json:"dependencies"`
}

type OverviewCauseCount struct {
	Cause string  `json:"cause"`
	Count float64 `json:"count"`
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
	Name          string   `json:"name"`
	Roles         []string `json:"roles,omitempty"`
	Version       string   `json:"version"`
	UptimeSeconds float64  `json:"uptime_seconds"`
	Ready         bool     `json:"ready"`
	MergedQueries int      `json:"merged_queries"`
	State         string   `json:"state"`
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

// overviewStageBasis is the counters each stage's numbers are rates of,
// over the last two computations.
var overviewStageBasis = map[string]string{
	"pull": "worker_work_total{work_kind=\"physical_query\"}、access_query_failure_total、access_query_duration_seconds，取最近两次计算的差",
	"evaluate": "progress_completed_total（不含 GAP_SKIPPED、SNAPSHOT_UNAVAILABLE）、slot_decision_lag_seconds，取最近两次计算的差；" +
		"失败是此刻调度普查里迟到和超期的检测",
	"state":  "worker_work_total{work_kind=\"state_apply\"}、state_admission_total（非 success）与 state_version_conflict_total、redis_command_duration_seconds{client=\"runtime\"}（运行态 Redis 往返），取最近两次计算的差",
	"output": "output_events_by_kind_total、output_events_rejected_all_total，取最近两次计算的差；发送耗时没有度量",
}

// overviewReading is one replica's counters as one computation read them,
// with when they were taken and the process they belong to.
type overviewReading struct {
	takenAt, startedAt time.Time
	flow               FlowFacts
}

// overviewFlows turns the replicas' counters into the stages' numbers: the
// difference from the previous computation over the time between the two
// readings of each replica. A replica whose reading moved, in the same
// process, with no counter gone back, contributes that difference. One
// whose reading did not -- a missed publish, a restart -- contributes the
// rate it had the computation before, once, and is counted as carried;
// after that it contributes nothing, and the flow covers fewer replicas
// than publish (OverviewCoverage), which holds the verdict.
type overviewFlows struct {
	readings map[string]overviewReading
	// last is each replica's latest fresh contribution and whether it has
	// already been carried once.
	last map[string]carriedContribution
}

type carriedContribution struct {
	contribution flowContribution
	carried      bool
}

func newOverviewFlows() *overviewFlows {
	return &overviewFlows{readings: map[string]overviewReading{}, last: map[string]carriedContribution{}}
}

// flowContribution is one replica's rates and histogram differences between
// two of its readings.
type flowContribution struct {
	queries, detections, stateWrites, stateFailures, emitted, rejected float64
	failures                                                           map[string]float64
	queryBuckets, lagBuckets, redisBuckets                             []uint64
	queryCount, lagCount, redisCount                                   uint64
}

type flowSum struct {
	contributed bool
	flowContribution
}

// OverviewCoverage is how many replicas the flow's numbers are from, of the
// replicas that publish the counters: Carried of Counted are a missed
// publish's last rate.
type OverviewCoverage struct {
	Counted  int `json:"counted"`
	Carried  int `json:"carried,omitempty"`
	Expected int `json:"expected"`
	// Warming are the replicas seen for the first time by this process -- its
	// first computation, a scale-out, a rollout's new pods -- which have no
	// earlier reading to be a difference from. They are not expected yet and
	// do not hold the verdict; from their second computation they are.
	Warming int `json:"warming,omitempty"`
}

func (flows *overviewFlows) apply(overview *Overview, health HealthResponse, at time.Time) {
	sum := flowSum{flowContribution: flowContribution{failures: map[string]float64{}, queryBuckets: make([]uint64, len(FlowQueryBounds)),
		lagBuckets: make([]uint64, len(FlowDecisionBounds)), redisBuckets: make([]uint64, len(FlowRedisBounds))}}
	next := make(map[string]overviewReading, len(health.PerReplica))
	last := make(map[string]carriedContribution, len(health.PerReplica))
	coverage := OverviewCoverage{}
	var hosts uint64
	hostsKnown := false
	for index, replica := range health.PerReplica {
		if replica.Flow == nil {
			continue
		}
		reading := overviewReading{takenAt: at.Add(-time.Duration(replica.AgeSeconds * float64(time.Second))), startedAt: replica.StartedAt, flow: *replica.Flow}
		next[replica.Replica] = reading
		if replica.Flow.LiveHeapBytes != nil && index < len(overview.Resources.Replicas) {
			overview.Resources.Replicas[index].LiveHeapBytes = overviewMeasured(float64(*replica.Flow.LiveHeapBytes), "bytes")
		}
		if index < len(overview.Runtime.Replicas) {
			overview.Runtime.Replicas[index].Roles = replica.Flow.Roles
		}
		if replica.Flow.Hosts > hosts {
			hosts, hostsKnown = replica.Flow.Hosts, true
		}
		// A replica this process has not read before has nothing to be a
		// difference from: it is warming, not missing (OverviewCoverage).
		previous, found := flows.readings[replica.Replica]
		if !found {
			coverage.Warming++
			continue
		}
		coverage.Expected++
		if contribution, fresh := contributionOf(previous, reading); fresh {
			sum.add(contribution)
			coverage.Counted++
			last[replica.Replica] = carriedContribution{contribution: contribution}
			continue
		}
		if moved := reading.takenAt.After(previous.takenAt); !moved {
			// The reading did not move: keep the older one, so the next
			// reading that does is measured from it.
			next[replica.Replica] = previous
		}
		if kept, had := flows.last[replica.Replica]; had && !kept.carried {
			sum.add(kept.contribution)
			coverage.Counted++
			coverage.Carried++
			last[replica.Replica] = carriedContribution{contribution: kept.contribution, carried: true}
		}
	}
	flows.readings, flows.last = next, last
	overview.FlowCoverage = coverage
	if coverage.Counted < coverage.Expected && overview.Verdict.State == OverviewOK {
		overview.Verdict = OverviewVerdict{State: OverviewAttention, Stage: "",
			Line: fmt.Sprintf("数据流只覆盖 %d/%d 个副本：有副本没有按时发布计数，下面各段的量少算了它的份", coverage.Counted, coverage.Expected)}
	}
	if hostsKnown {
		overview.Resources.Scale.Hosts = overviewMeasured(float64(hosts), "hosts")
	}
	for index := range overview.Flow {
		stage := &overview.Flow[index]
		stage.Basis = overviewStageBasis[stage.Stage]
		if !sum.contributed {
			continue
		}
		switch stage.Stage {
		case "pull":
			stage.Volume = overviewMeasured(sum.queries, "per_second")
			total := 0.0
			for _, cause := range overviewQueryFailureOrder {
				if count := sum.failures[cause]; count > 0 {
					stage.FailureCauses = append(stage.FailureCauses, OverviewCauseCount{Cause: cause, Count: count})
					total += count
				}
			}
			stage.Failures = overviewMeasured(total, "per_minute")
			stage.LatencyP95MS = overviewQuantile(FlowQueryBounds, sum.queryBuckets, sum.queryCount)
		case "evaluate":
			stage.Volume = overviewMeasured(sum.detections, "per_second")
			stage.LatencyP95MS = overviewQuantile(FlowDecisionBounds, sum.lagBuckets, sum.lagCount)
		case "state":
			stage.Volume = overviewMeasured(sum.stateWrites, "per_second")
			stage.Failures = overviewMeasured(sum.stateFailures, "per_minute")
			stage.LatencyP95MS = overviewQuantile(FlowRedisBounds, sum.redisBuckets, sum.redisCount)
		case "output":
			stage.Volume = overviewMeasured(sum.emitted, "per_second")
			stage.Failures = overviewMeasured(sum.rejected, "per_minute")
		}
		if stage.Failures.Measured && stage.Failures.Value > 0 && stage.State == OverviewOK {
			stage.State = OverviewAttention
		}
	}
}

// overviewQueryFailureOrder is the order the pull stage lists causes in.
var overviewQueryFailureOrder = []string{"timeout", "refused", "not_found", "transport", "decode", "other"}

// contributionOf is a replica's rates between two readings, and whether
// they are a difference of anything: the same process, a reading that
// moved, no counter gone back.
func contributionOf(previous, current overviewReading) (flowContribution, bool) {
	elapsed := current.takenAt.Sub(previous.takenAt).Seconds()
	was, now := previous.flow, current.flow
	if !previous.startedAt.Equal(current.startedAt) || elapsed <= 0 || now.Queries < was.Queries || now.Detections < was.Detections ||
		now.StateWrites < was.StateWrites || now.EventsEmitted < was.EventsEmitted || now.QueryDuration.Count < was.QueryDuration.Count {
		return flowContribution{}, false
	}
	contribution := flowContribution{failures: map[string]float64{}, queryBuckets: make([]uint64, len(FlowQueryBounds)),
		lagBuckets: make([]uint64, len(FlowDecisionBounds)), redisBuckets: make([]uint64, len(FlowRedisBounds))}
	contribution.queries = float64(now.Queries-was.Queries) / elapsed
	contribution.detections = float64(now.Detections-was.Detections) / elapsed
	contribution.stateWrites = float64(now.StateWrites-was.StateWrites) / elapsed
	contribution.emitted = float64(now.EventsEmitted-was.EventsEmitted) / elapsed
	contribution.stateFailures = float64(subtract(now.StateFailures, was.StateFailures)) / elapsed * 60
	contribution.rejected = float64(subtract(now.EventsRejected, was.EventsRejected)) / elapsed * 60
	for cause, count := range now.QueryFailures {
		contribution.failures[cause] = float64(subtract(count, was.QueryFailures[cause])) / elapsed * 60
	}
	contribution.queryCount = addBuckets(contribution.queryBuckets, now.QueryDuration, was.QueryDuration)
	contribution.lagCount = addBuckets(contribution.lagBuckets, now.DecisionLag, was.DecisionLag)
	contribution.redisCount = addBuckets(contribution.redisBuckets, now.RuntimeRedis, was.RuntimeRedis)
	return contribution, true
}

func (sum *flowSum) add(contribution flowContribution) {
	sum.contributed = true
	sum.queries += contribution.queries
	sum.detections += contribution.detections
	sum.stateWrites += contribution.stateWrites
	sum.emitted += contribution.emitted
	sum.stateFailures += contribution.stateFailures
	sum.rejected += contribution.rejected
	for cause, count := range contribution.failures {
		sum.failures[cause] += count
	}
	for index := range sum.queryBuckets {
		sum.queryBuckets[index] += contribution.queryBuckets[index]
	}
	for index := range sum.lagBuckets {
		sum.lagBuckets[index] += contribution.lagBuckets[index]
	}
	for index := range sum.redisBuckets {
		sum.redisBuckets[index] += contribution.redisBuckets[index]
	}
	sum.queryCount += contribution.queryCount
	sum.lagCount += contribution.lagCount
	sum.redisCount += contribution.redisCount
}

func subtract(now, was uint64) uint64 {
	if now < was {
		return 0
	}
	return now - was
}

// addBuckets adds one replica's bucket differences into the sum and returns
// its count's difference.
func addBuckets(into []uint64, now, was FlowHistogram) uint64 {
	for index := range into {
		var current, previous uint64
		if index < len(now.Buckets) {
			current = now.Buckets[index]
		}
		if index < len(was.Buckets) {
			previous = was.Buckets[index]
		}
		into[index] += subtract(current, previous)
	}
	return subtract(now.Count, was.Count)
}

// overviewQuantile is the 95th percentile in milliseconds from cumulative
// bucket counts, interpolated within its bucket; past the last bound it is
// that bound, said as at least. No observations is no number.
func overviewQuantile(bounds []float64, buckets []uint64, count uint64) OverviewNumber {
	if count == 0 {
		return overviewUnmeasured("ms")
	}
	target := 0.95 * float64(count)
	lower, below := 0.0, uint64(0)
	for index, bound := range bounds {
		if float64(buckets[index]) >= target {
			inBucket := buckets[index] - below
			fraction := 1.0
			if inBucket > 0 {
				fraction = (target - float64(below)) / float64(inBucket)
			}
			return overviewMeasured((lower+(bound-lower)*fraction)*1000, "ms")
		}
		lower, below = bound, buckets[index]
	}
	number := overviewMeasured(bounds[len(bounds)-1]*1000, "ms")
	number.AtLeast = true
	return number
}
