// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/detect"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/evaluation"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/trigger"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A SimpleRingRatio Plan - PRIMARY plus its "previous" ALGORITHM_DEPENDENCY,
// two physical queries of one logical query - over a Query Group past half
// its share is read in slices with reads ahead replayed, through the real
// access.Source over a fake provider and the real evaluator, under a runner
// context never cancelled. Every round, every series the provider holds is
// evaluated once by the ring-ratio algorithm with its history input, and
// its State written once, replayed slices included.
func TestRingRatioSlicesReadAheadAndReplayedEvaluateEverySeries(t *testing.T) {
	const hosts, padding, rounds = 24, 40 << 10, 3
	rig := newRingRatioRig(t, hosts, padding)
	runner, stop := context.WithCancel(context.Background())
	defer stop()
	replays := uint64(0)
	for round := 1; round <= rounds; round++ {
		// Each round evaluates afresh: no State kept from the round before.
		rig.state.mu.Lock()
		rig.state.saved = map[execution.StateKeyIdentity]execution.StateMutation{}
		rig.state.mu.Unlock()
		firstKey := len(rig.ports.appliedKeys)
		rig.observed.reset()
		firstRead := rig.provider.readCount()

		result, err := rig.coordinator.Execute(runner, rig.request)
		usage := result.Usage
		t.Logf("round %d: completed=%t err=%v slices=%d readAhead(replayed)=%d otherLeaf=%d yielded=%d failed=%d discarded=%d",
			round, result.Completed, err, usage.InputSlices, usage.InputSlicesReadAhead, usage.InputSlicesReadAheadOtherLeaf,
			usage.InputSlicesReadAheadYielded, usage.InputSlicesReadAheadFailed, usage.InputSlicesDiscarded)
		reads := rig.provider.readsFrom(firstRead)
		for _, read := range reads {
			t.Logf("round %d read: slice=%s primary=%t delivered=%d err=%v", round, read.slice, read.primary, len(read.delivered), read.err)
		}
		if err != nil || !result.Completed {
			t.Fatalf("round %d: result=%+v err=%v, want the Slot completed", round, result, err)
		}
		if usage.InputSlices < 2 {
			t.Fatalf("round %d read in %d slices, want the Query Group read in slices", round, usage.InputSlices)
		}
		replays += usage.InputSlicesReadAhead

		// What the provider delivered: every host, on its primary and its
		// dependency, in a read that completed.
		deliveredPrimary, deliveredHistory := map[string]bool{}, map[string]bool{}
		for _, read := range reads {
			if read.err != nil {
				continue
			}
			for _, digest := range read.delivered {
				if read.primary {
					deliveredPrimary[digest] = true
				} else {
					deliveredHistory[digest] = true
				}
			}
		}
		if len(deliveredPrimary) != hosts || len(deliveredHistory) != hosts {
			t.Fatalf("round %d: provider delivered %d primary and %d history series in completed reads, want %d each",
				round, len(deliveredPrimary), len(deliveredHistory), hosts)
		}

		evaluated, history := rig.observed.ringRatio()
		writes := map[string]int{}
		for _, key := range rig.ports.appliedKeys[firstKey:] {
			writes[string(key.SeriesIdentityDigest)]++
		}
		var missing []string
		for digest := range deliveredPrimary {
			if evaluated[digest] != 1 || history[digest] != 1 || writes[digest] != 1 {
				missing = append(missing, fmt.Sprintf("%s(%s): evaluated=%d history=%d writes=%d",
					rig.provider.hostOf[digest], digest[:8], evaluated[digest], history[digest], writes[digest]))
			}
		}
		sort.Strings(missing)
		if len(missing) != 0 || len(evaluated) != hosts {
			t.Fatalf("round %d (replays %d): %d of %d series evaluated by SimpleRingRatio, %d with history; off: %s",
				round, usage.InputSlicesReadAhead, len(evaluated), hosts, len(history), strings.Join(missing, "; "))
		}
	}
	if replays == 0 {
		t.Fatalf("no read ahead was replayed in %d rounds: the setup does not reach the replay path", rounds)
	}
	if runner.Err() != nil {
		t.Fatal("the runner's context ended during the test")
	}
}

// ringRatioRig is the coordinator over a real access.Source.
type ringRatioRig struct {
	ports       *recordingPorts
	state       *persistedRetryState
	provider    *ringRatioProvider
	observed    *ringRatioObservations
	coordinator *worker.SlotExecutionCoordinator
	request     execution.SlotExecutionRequest
}

func newRingRatioRig(t *testing.T, hosts, padding int) *ringRatioRig {
	t.Helper()
	return newRingRatioRigObserved(t, hosts, padding, nil)
}

// newRingRatioRigObserved is newRingRatioRig with the access source telling
// observer what it sees.
func newRingRatioRigObserved(t *testing.T, hosts, padding int, observer observability.Observer) *ringRatioRig {
	t.Helper()
	facts := ringRatioQueryFacts(t)
	ref := execution.LogicalQueryRef(facts.QueryRevision)
	compiled, requirements := ringRatioCompiledPlan(t, string(ref))
	identity := planIdentity()
	evaluationTime := frozenContract().Slot.EvaluationTime
	due := execution.DuePlan{Identity: identity, CompiledPlan: compiled, StateGeneration: "state-v1", StateApplyEpoch: 1,
		ScheduleSpec:     execution.ScheduleSpec{EvaluationIntervalSeconds: 60, Timezone: "UTC"},
		ScheduleRevision: "plan-schedule-v1", CompletionDeadlineUnixMilli: int64(evaluationTime)*1000 + 600_000}
	consumer := execution.ConsumerRef{Plan: identity, LevelID: 5, HasLevel: true}
	for index := range requirements {
		requirements[index].Consumers = []execution.DataRequirementConsumer{{Consumer: consumer,
			ConsumerDeadlineUnixMilli: due.CompletionDeadlineUnixMilli, DownstreamExecutionReserveMilliSec: 5_000}}
	}
	digest, err := execution.DeriveDuePlanSetDigest([]execution.DuePlan{due}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	contractRef := frozenContract()
	contractRef.QueryRevision = facts.QueryRevision
	contractRef.DuePlanSetDigest = digest
	frozen := access.FrozenPlan{DuePlans: []execution.DuePlan{due}, Requirements: requirements,
		QueryFacts: map[execution.LogicalQueryRef]execution.QueryPlanFacts{ref: facts}}

	provider := newRingRatioProvider(t, hosts, padding, int64(evaluationTime))
	readAt := time.Unix(int64(evaluationTime)+120, 0)
	source, err := access.NewSource(ringRatioFrozen{plan: frozen}, provider, ringRatioPermits{},
		access.Config{MinReadyDelay: time.Second, Now: func() time.Time { return readAt }, Observer: observer})
	if err != nil {
		t.Fatal(err)
	}

	detector, err := detect.NewEvaluator(detect.NewDefaultRegistry())
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := evaluation.New(detector, evaluation.Limits{MaxLevels: 16,
		Trigger: trigger.EvaluationLimitsV2{MaxLevels: 16, MaxTriggerWindowSize: 16,
			MaxRecoveryConsecutiveWindows: 16, MaxRequiredHistoryPoints: 32,
			MaxLevelResultsPerEvent: 16, MaxEvidenceBytesPerEvent: 1 << 20, MaxComputeCost: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true}
	state := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	observed := &ringRatioObservations{}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports,
		Activation: ports, Query: source, Sequencer: ports, Evaluator: evaluator, Admission: ports,
		GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: ports, State: state, Progress: ports, Observer: observed},
		worker.ProvisionalBudget{MaxSeries: 400, MaxRetainedBytes: slicedPool, MaxStateMutations: 400, MaxEvents: 400, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	request := windowedRequest()
	request.Contract = contractRef
	request.DuePlanTargets = execution.FrozenDuePlanTargets{DuePlanSetDigest: digest, Plans: []execution.PlanKey{due.Key()}}
	return &ringRatioRig{ports: ports, state: state, provider: provider, observed: observed, coordinator: coordinator, request: request}
}

// ringRatioQueryFacts is a structured UQ metric query grouped by host, the
// shape SliceableDimensions accepts, normalized as the compiled Plan reads.
func ringRatioQueryFacts(t *testing.T) execution.QueryPlanFacts {
	t.Helper()
	facts, err := execution.BuildQueryPlanFacts(execution.QueryPlanFacts{Provider: execution.ProviderUQ, ProviderRouteRef: "uq-main",
		TenantID: "tenant", BusinessID: "2", SpaceScope: "bkcc__2", QueryList: []execution.QueryClause{{DataSource: "bkmonitor",
			TableID: "system.cpu", FieldName: "usage", ReferenceName: "a", Driver: "influxdb", TimeField: "time",
			Dimensions:      []string{"host"},
			TimeAggregation: execution.QueryFunction{Method: "avg", Position: 0, Window: "60s"}}},
		MetricMerge: "a", StepMillis: 60_000, AlignmentMillis: 60_000, Timezone: "UTC",
		Normalization: execution.DatasetNormalizationSpec{DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64),
			NormalizationDigest: strings.Repeat("2", 64), IdentityFields: []string{"host"}, SourceTimeField: "time", ReceivedTimeField: "received_time"},
			SourceTimeUnit: execution.TimeUnitMillisecond, CanonicalSourceTimeUnit: execution.TimeUnitSecond,
			SeriesIdentityMode: execution.SeriesIdentityUQGroupKeysValuesV1, GroupKeyRule: execution.GroupKeyStripTableSuffixV1,
			ValueSelectionMode: execution.ValueSelectionResultOrFirstReferenceV1, CanonicalValueField: "value",
			ReceivedTimeMode: execution.ReceivedTimeProviderReceivedAt, Version: "uq-threshold-normalization-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	return facts
}

// ringRatioCompiledPlan is workerG4CompiledPlan's SimpleRingRatio with both
// requirements on ref, the one logical query the Plan's queries are built
// from, as the strategy compiler binds a history dependency.
func ringRatioCompiledPlan(t *testing.T, ref string) (*strategy.CompiledPlan, []execution.DataRequirement) {
	t.Helper()
	return ringRatioCompiledPlanRecovering(t, ref, `{"enabled":true,"consecutive_windows":1}`)
}

// ringRatioCompiledPlanRecovering is ringRatioCompiledPlan with the Level's
// recovery plan given.
func ringRatioCompiledPlanRecovering(t *testing.T, ref, recovery string) (*strategy.CompiledPlan, []execution.DataRequirement) {
	t.Helper()
	return ringRatioCompiledPlanWith(t, ref, recovery, 0, nil)
}

// ringRatioCompiledPlanWith is ringRatioCompiledPlan with the Level's
// recovery plan, the strategy's snapshot revision - above zero, the Plan is
// on the consumer's standard protocol - and its no-data setting given.
func ringRatioCompiledPlanWith(t *testing.T, ref, recovery string, snapshotRevision int64,
	noData *contract.NoDataConfigV1) (*strategy.CompiledPlan, []execution.DataRequirement) {
	t.Helper()
	projection := strategy.AlgorithmInputProjection{ValueFields: []string{"value"}, DimensionFields: []string{"host"}, IdentityFields: []string{"host"}}
	requirement := func(name string, role strategy.AlgorithmInputRole, start, end int64, points []strategy.AlgorithmNamedInputPoint,
		readiness strategy.AlgorithmReadinessClass) strategy.AlgorithmInputRequirement {
		offsets := make([]int64, 0, len(points))
		named := make([]execution.NamedInputPoint, 0, len(points))
		for _, point := range points {
			offsets = append(offsets, point.OffsetSeconds)
			named = append(named, execution.NamedInputPoint{Name: point.Name, OffsetSeconds: point.OffsetSeconds})
		}
		template, err := execution.BuildDataRequirementTemplate(execution.DataRequirementTemplate{
			DatasetName: execution.DatasetName(name), Role: execution.InputRole(role), ConsumerLevelID: 5,
			LogicalQueryRef: execution.LogicalQueryRef(ref),
			RelativeWindow:  execution.RelativeQueryWindow{StartOffsetSeconds: start, EndOffsetSeconds: end, HalfOpen: true},
			StepMillis:      60_000, AlignmentMillis: 60_000, ResultWindowPolicy: execution.ResultWindowExactHalfOpen,
			ReadinessClass: execution.ReadinessClass(readiness),
			InputProjection: execution.InputProjection{ValueFields: projection.ValueFields,
				DimensionFields: projection.DimensionFields, IdentityFields: projection.IdentityFields},
			PointOffsetsSeconds: offsets, NamedPoints: named,
		})
		if err != nil {
			t.Fatal(err)
		}
		return strategy.AlgorithmInputRequirement{RequirementID: string(template.RequirementID), DatasetName: name, Role: role,
			ConsumerLevelID: 5, LogicalQueryRef: ref,
			RelativeWindow: strategy.AlgorithmRelativeWindow{StartOffsetSeconds: start, EndOffsetSeconds: end, HalfOpen: true},
			StepMillis:     60_000, AlignmentMillis: 60_000, ReadinessClass: readiness, InputProjection: projection,
			PointOffsetsSeconds: offsets, NamedPoints: points}
	}
	inputs := []strategy.AlgorithmInputRequirement{
		requirement("primary", strategy.AlgorithmInputPrimary, -60, 0, nil, strategy.AlgorithmReadinessEager),
		requirement("previous", strategy.AlgorithmInputDependency, -120, -60,
			[]strategy.AlgorithmNamedInputPoint{{Name: "previous", OffsetSeconds: 60}}, strategy.AlgorithmReadinessFinalizedRequired),
	}
	payload, err := json.Marshal(map[string]any{"floor": 50, "ceil": nil, "input_projection": projection, "requirements": inputs})
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := strategy.NewCompiler(strategy.NewDefaultAlgorithmCompilerRegistry(), strategy.Limits{
		MaxPlanBytes: 64 << 10, MaxLevelsPerPlan: 16, MaxAlgorithmsPerLevel: 8, MaxGroupsPerAlgorithm: 16,
		MaxConditionsPerAlgorithm: 64, MaxASTNodesPerLevel: 256, MaxTriggerWindowSize: 4096,
		MaxRecoveryConsecutiveWindows: 4096, MaxRequiredHistoryPoints: 4096, MaxTriggerComputeCost: 1 << 20,
		MaxCompiledPlanBytes: 64 << 10, MaxCacheEntries: 64, MaxCacheBytes: 4 << 20,
		NegativeCacheTTL: time.Minute, BudgetRevision: "worker-ring-ratio-slice-test-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	strategyRef := contract.StrategyRefV2{TenantID: "tenant", StrategyID: "7", Revision: "strategy-v1", SnapshotRevision: snapshotRevision}
	inputProjection := contract.InputProjectionV2{ValueFields: []string{"value"}, DimensionFields: []string{"host"},
		BusinessIdentityField: "bk_biz_id", MultiValueAlignment: "SINGLE_VALUE", DataUnit: "percent",
		MissingValuePolicy: contract.MissingValuePolicyRequired}
	plan := contract.EvaluationPlanV2{PlanID: "7", StrategyRef: strategyRef, InputProjection: inputProjection, NoData: noData,
		StrategyIR: contract.StrategyIRV2{Schema: contract.Schema{Name: contract.StrategyIRSchemaV2, Major: 2},
			StrategyRef: strategyRef, InputProjection: inputProjection,
			ExecutionSemantics: contract.ExecutionSemanticsV2{EvaluationScope: contract.EvaluationScopeSeries,
				QueryWindow: 300, AggregationInterval: 60, EvaluationInterval: 60, LatenessTolerance: 120},
			Levels: []contract.LevelIRV2{{Definition: contract.LevelDefinitionV2{LevelID: 5, Priority: 1},
				Connector: contract.LevelConnectorAND,
				DetectPlan: contract.DetectPlanV2{Algorithms: []contract.AlgorithmIRV2{{Type: strategy.DetectorKindSimpleRingRatio,
					Version: 1, Config: payload}}},
				TriggerPlan: contract.TypedPlanV1{Type: "N_OF_M", Version: 1,
					Config: json.RawMessage(`{"window_size":1,"required_anomalies":1,"step_seconds":60}`)},
				RecoveryPlan: contract.TypedPlanV1{Type: "CONTINUOUS_TRIGGER_MISS", Version: 1,
					Config: json.RawMessage(recovery)},
			}}}}
	result, err := compiler.Compile(context.Background(), strategy.CompileRequest{Plan: plan,
		DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64),
			NormalizationDigest: strings.Repeat("2", 64), IdentityFields: []string{"host"},
			SourceTimeField: "time", ReceivedTimeField: "received_time"},
		StateSemantics: strategy.StateSemantics{StateSchemaVersion: "window-state-v1", CodecSemanticsVersion: "window-codec-v1",
			IdentitySchemaDigest: strings.Repeat("3", 64), SourceTimeSemanticsVersion: "source-time-v1",
			HistoryCellSemanticsVersion: "history-cell-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled, ok := result.Plan()
	if !ok || compiled.Levels().Len() == 0 {
		t.Fatalf("compile ring ratio: terminal=%+v levels=%+v", result.PlanTerminal(), result.LevelTerminals())
	}
	algorithmRequirements := compiled.Levels().At(0).Algorithms().At(0).InputRequirements()
	requirements := make([]execution.DataRequirement, 0, len(algorithmRequirements))
	for _, requirement := range algorithmRequirements {
		points := make([]execution.NamedInputPoint, len(requirement.NamedPoints))
		for index, point := range requirement.NamedPoints {
			points[index] = execution.NamedInputPoint{Name: point.Name, OffsetSeconds: point.OffsetSeconds}
		}
		template, err := execution.BuildDataRequirementTemplate(execution.DataRequirementTemplate{
			DatasetName: execution.DatasetName(requirement.DatasetName), Role: execution.InputRole(requirement.Role),
			ConsumerLevelID: requirement.ConsumerLevelID, LogicalQueryRef: execution.LogicalQueryRef(requirement.LogicalQueryRef),
			RelativeWindow: execution.RelativeQueryWindow{StartOffsetSeconds: requirement.RelativeWindow.StartOffsetSeconds,
				EndOffsetSeconds: requirement.RelativeWindow.EndOffsetSeconds, HalfOpen: requirement.RelativeWindow.HalfOpen},
			StepMillis: requirement.StepMillis, AlignmentMillis: requirement.AlignmentMillis,
			ResultWindowPolicy: execution.ResultWindowExactHalfOpen, ReadinessClass: execution.ReadinessClass(requirement.ReadinessClass),
			InputProjection: execution.InputProjection{ValueFields: requirement.InputProjection.ValueFields,
				DimensionFields: requirement.InputProjection.DimensionFields, IdentityFields: requirement.InputProjection.IdentityFields},
			PointOffsetsSeconds: requirement.PointOffsetsSeconds, NamedPoints: points,
		})
		if err != nil || string(template.RequirementID) != requirement.RequirementID {
			t.Fatalf("materialize %q: template=%+v error=%v", requirement.DatasetName, template, err)
		}
		requirements = append(requirements, template.Bind(execution.DataRequirementConsumer{}))
	}
	return compiled, requirements
}

// thresholdCompiledPlan is a threshold Plan of one Level, 5, raised at 100
// and up in any of its last three positions, its primary requirement on ref:
// one logical query, one physical query a Slot. Three positions, so it keeps
// a history its stable segments cover - a window of one keeps none - and no
// recovery: a quiet series decided RECOVERY round after round sends its
// event each time, and a series with events is no member.
func thresholdCompiledPlan(t *testing.T, ref string) (*strategy.CompiledPlan, []execution.DataRequirement) {
	t.Helper()
	compiler, err := strategy.NewCompiler(strategy.NewDefaultAlgorithmCompilerRegistry(), strategy.Limits{
		MaxPlanBytes: 64 << 10, MaxLevelsPerPlan: 16, MaxAlgorithmsPerLevel: 8, MaxGroupsPerAlgorithm: 16,
		MaxConditionsPerAlgorithm: 64, MaxASTNodesPerLevel: 256, MaxTriggerWindowSize: 4096,
		MaxRecoveryConsecutiveWindows: 4096, MaxRequiredHistoryPoints: 4096, MaxTriggerComputeCost: 1 << 20,
		MaxCompiledPlanBytes: 64 << 10, MaxCacheEntries: 64, MaxCacheBytes: 4 << 20,
		NegativeCacheTTL: time.Minute, BudgetRevision: "worker-threshold-shadow-test-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	strategyRef := contract.StrategyRefV2{TenantID: "tenant", StrategyID: "7", Revision: "strategy-v1"}
	inputProjection := contract.InputProjectionV2{ValueFields: []string{"value"}, DimensionFields: []string{"host"},
		BusinessIdentityField: "bk_biz_id", MultiValueAlignment: "SINGLE_VALUE", DataUnit: "percent",
		MissingValuePolicy: contract.MissingValuePolicyRequired}
	threshold := json.RawMessage(`{"value_field":"value","data_unit":"percent","threshold_unit_prefix":"","precision":{"decimal_places":6,"rounding":"HALF_EVEN"},"groups":[{"conditions":[{"operator":"GTE","threshold_decimal":"100"}]}]}`)
	plan := contract.EvaluationPlanV2{PlanID: "7", StrategyRef: strategyRef, InputProjection: inputProjection,
		StrategyIR: contract.StrategyIRV2{Schema: contract.Schema{Name: contract.StrategyIRSchemaV2, Major: 2},
			StrategyRef: strategyRef, InputProjection: inputProjection,
			ExecutionSemantics: contract.ExecutionSemanticsV2{EvaluationScope: contract.EvaluationScopeSeries,
				QueryWindow: 300, AggregationInterval: 60, EvaluationInterval: 60, LatenessTolerance: 120},
			Levels: []contract.LevelIRV2{{Definition: contract.LevelDefinitionV2{LevelID: 5, Priority: 1},
				Connector:  contract.LevelConnectorAND,
				DetectPlan: contract.DetectPlanV2{Algorithms: []contract.AlgorithmIRV2{{Type: strategy.DetectorKindThreshold, Version: 1, Config: threshold}}},
				TriggerPlan: contract.TypedPlanV1{Type: "N_OF_M", Version: 1,
					Config: json.RawMessage(`{"window_size":3,"required_anomalies":1,"step_seconds":60}`)},
				RecoveryPlan: contract.TypedPlanV1{Type: "CONTINUOUS_TRIGGER_MISS", Version: 1,
					Config: json.RawMessage(shadowRecoveryOff)},
			}}}}
	result, err := compiler.Compile(context.Background(), strategy.CompileRequest{Plan: plan,
		DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64),
			NormalizationDigest: strings.Repeat("2", 64), IdentityFields: []string{"host"},
			SourceTimeField: "time", ReceivedTimeField: "received_time"},
		StateSemantics: strategy.StateSemantics{StateSchemaVersion: "window-state-v1", CodecSemanticsVersion: "window-codec-v1",
			IdentitySchemaDigest: strings.Repeat("3", 64), SourceTimeSemanticsVersion: "source-time-v1",
			HistoryCellSemanticsVersion: "history-cell-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled, ok := result.Plan()
	if !ok || compiled.Levels().Len() == 0 {
		t.Fatalf("compile threshold: terminal=%+v levels=%+v", result.PlanTerminal(), result.LevelTerminals())
	}
	template, err := execution.BuildDataRequirementTemplate(execution.DataRequirementTemplate{
		DatasetName: "primary", Role: execution.InputRole(strategy.AlgorithmInputPrimary), ConsumerLevelID: 5,
		LogicalQueryRef: execution.LogicalQueryRef(ref),
		RelativeWindow:  execution.RelativeQueryWindow{StartOffsetSeconds: -60, EndOffsetSeconds: 0, HalfOpen: true},
		StepMillis:      60_000, AlignmentMillis: 60_000, ResultWindowPolicy: execution.ResultWindowExactHalfOpen,
		ReadinessClass: execution.ReadinessClass(strategy.AlgorithmReadinessEager),
		InputProjection: execution.InputProjection{ValueFields: []string{"value"}, DimensionFields: []string{"host"},
			IdentityFields: []string{"host"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return compiled, []execution.DataRequirement{template.Bind(execution.DataRequirementConsumer{})}
}

type ringRatioFrozen struct{ plan access.FrozenPlan }

func (frozen ringRatioFrozen) ResolveFrozenPlan(context.Context, execution.FrozenExecutionContractRef) (access.FrozenPlan, error) {
	return frozen.plan, nil
}

type ringRatioPermit struct{}

func (ringRatioPermit) RecoveryPermit() *execution.RecoveryPermit { return nil }
func (ringRatioPermit) Release()                                  {}

type ringRatioPermits struct{}

func (ringRatioPermits) AcquireQueryPermit(context.Context, execution.SlotIdentity, execution.Operation, time.Time) (access.QueryPermit, error) {
	return ringRatioPermit{}, nil
}

func (permits ringRatioPermits) Release() {}

func (permits ringRatioPermits) AcquireRecoveryChannels(context.Context, execution.SlotIdentity, execution.Operation, time.Time, int, func()) (access.RecoveryChannels, error) {
	return permits, nil
}

// ringRatioRead is one physical query the provider answered.
type ringRatioRead struct {
	slice     string
	primary   bool
	delivered []string
	err       error
}

// ringRatioProvider is the query service: it answers each physical query
// with the hosts the attempt's slice holds, the PRIMARY window's points
// padded on a dimension outside the identity, the history window's not.
type ringRatioProvider struct {
	hosts      []string
	padding    string
	evaluation int64
	hostOf     map[string]string
	mu         sync.Mutex
	reads      []ringRatioRead
}

func newRingRatioProvider(t *testing.T, hosts, padding int, evaluation int64) *ringRatioProvider {
	t.Helper()
	provider := &ringRatioProvider{padding: strings.Repeat("p", padding), evaluation: evaluation, hostOf: map[string]string{}}
	for index := 0; index < hosts; index++ {
		host := fmt.Sprintf("host-%02d", index)
		provider.hosts = append(provider.hosts, host)
		digest, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", ringRatioFields(host))
		if err != nil {
			t.Fatal(err)
		}
		provider.hostOf[digest] = host
	}
	return provider
}

func ringRatioFields(host string) []contract.DimensionFieldV2 {
	return []contract.DimensionFieldV2{{Name: "host", Value: json.RawMessage(strconv.Quote(host))}}
}

func (provider *ringRatioProvider) readCount() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return len(provider.reads)
}

func (provider *ringRatioProvider) readsFrom(from int) []ringRatioRead {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return append([]ringRatioRead(nil), provider.reads[from:]...)
}

func (provider *ringRatioProvider) Execute(ctx context.Context, attempt execution.QueryAttempt, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	window := attempt.Spec.LogicalWindow
	read := ringRatioRead{slice: attempt.Slice.Digest(), primary: window.End > provider.evaluation-60}
	defer func() {
		provider.mu.Lock()
		provider.reads = append(provider.reads, read)
		provider.mu.Unlock()
	}()
	ref := execution.ProviderResultRef("ring-result-" + string(attempt.Spec.Digest))
	sourceTime, value := window.Start, json.RawMessage(`60`)
	if !read.primary {
		value = json.RawMessage(`100`)
	}
	var delivery execution.SeriesDelivery
	for _, host := range provider.hosts {
		in, err := attempt.Slice.Matches(map[string]string{"host": host})
		if err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		if !in {
			continue
		}
		if err := ctx.Err(); err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		fields := ringRatioFields(host)
		dimension, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", fields)
		if err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		recordID, err := contract.DeriveRecordIDV2(dimension, sourceTime)
		if err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		dimensions := map[string]json.RawMessage{"host": json.RawMessage(strconv.Quote(host))}
		if read.primary {
			dimensions["padding"] = json.RawMessage(strconv.Quote(provider.padding))
		}
		records := []contract.CanonicalRecordV2{{RecordID: recordID, SourceTime: sourceTime, BusinessID: "2",
			DimensionIdentity: contract.DimensionIdentityV2{Fields: fields, Digest: dimension},
			Values:            map[string]json.RawMessage{"value": value}, Dimensions: dimensions, ReceivedTime: provider.evaluation}}
		digest, err := contract.DeriveCanonicalDigestV2("ring-ratio-delivery", records)
		if err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		batch := execution.SeriesDelivery{PhysicalQuery: attempt.Spec.Digest, QueryRevision: attempt.Spec.PlanFacts.QueryRevision,
			Series: 1, Records: 1, Digest: digest}
		if err := sink.ConsumeProviderSeries(ctx, execution.ProviderSeriesBatch{PhysicalQuery: attempt.Spec.Digest,
			CompletionRef: ref, Dataset: execution.NewDataset(records), Delivery: batch}); err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		if delivery, err = execution.AccumulateSeriesDelivery(delivery, batch); err != nil {
			read.err = err
			return execution.ProviderCompletion{}, err
		}
		read.delivered = append(read.delivered, dimension)
	}
	completion := execution.ProviderCompletion{Ref: ref, PhysicalQuery: attempt.Spec.Digest, Completeness: execution.CompletenessFull,
		DataState: execution.DataStateData, Delivery: delivery,
		RouteFacts: execution.ProviderRouteFacts{ProviderRouteRef: attempt.Spec.PlanFacts.ProviderRouteRef},
		Stats:      execution.ProviderStats{Series: uint64(len(read.delivered)), Records: uint64(len(read.delivered))}}
	if len(read.delivered) == 0 {
		completion.DataState, completion.Delivery = execution.DataStateEmpty, execution.SeriesDelivery{}
	}
	return completion, nil
}

// ringRatioObservations keeps, per series, the SimpleRingRatio evaluations
// and history inputs the coordinator reported.
type ringRatioObservations struct {
	mu        sync.Mutex
	evaluated map[string]int
	history   map[string]int
}

func (observed *ringRatioObservations) reset() {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	observed.evaluated, observed.history = map[string]int{}, map[string]int{}
}

func (observed *ringRatioObservations) Observe(_ context.Context, observation observability.Observation) {
	observation = observability.NormalizeObservation(observation)
	if observation.Component != observability.ComponentEvaluation || observation.Stage != observability.StageEvaluationCompleted {
		return
	}
	series := observation.Trace.DimensionIdentityDigest
	observed.mu.Lock()
	defer observed.mu.Unlock()
	if observed.evaluated == nil {
		observed.evaluated, observed.history = map[string]int{}, map[string]int{}
	}
	for _, fact := range observation.AlgorithmEvaluations {
		if fact.SourceAlgorithmFamily == observability.AlgorithmFamilySimpleRingRatio {
			observed.evaluated[series]++
		}
	}
	for _, fact := range observation.AlgorithmInputs {
		if fact.SourceAlgorithmFamily == observability.AlgorithmFamilySimpleRingRatio &&
			fact.InputName == observability.AlgorithmInputNameHistory && fact.Result == observability.AlgorithmInputResultAvailable {
			observed.history[series]++
		}
	}
}

func (observed *ringRatioObservations) ringRatio() (map[string]int, map[string]int) {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	evaluated, history := map[string]int{}, map[string]int{}
	for key, value := range observed.evaluated {
		evaluated[key] = value
	}
	for key, value := range observed.history {
		history[key] = value
	}
	return evaluated, history
}
