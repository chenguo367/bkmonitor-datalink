// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package detect

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// thresholdOver is a Threshold config judging the value over limit, as the
// control plane writes it.
func thresholdOver(limit string) map[string]any {
	return map[string]any{"value_field": "value", "data_unit": "none", "threshold_unit_prefix": "",
		"precision": map[string]any{"decimal_places": 6, "rounding": "HALF_EVEN"},
		"groups":    []map[string]any{{"conditions": []map[string]any{{"operator": "GT", "threshold_decimal": limit}}}}}
}

// compileMultiDetectLevel compiles one level of an item detected over two
// inputs, each read through its own MultiDetectThreshold: CPU, the PRIMARY,
// over 90, and memory, the named input "input:memory", over 80, joined by
// connector. With kinds the first algorithm is of the kind given instead.
func compileMultiDetectLevel(t *testing.T, connector string, kinds ...string) (*strategy.CompiledPlan, *strategy.Terminal) {
	t.Helper()
	if len(kinds) == 0 {
		kinds = []string{strategy.DetectorKindMultiDetectThreshold, strategy.DetectorKindMultiDetectThreshold}
	}
	projection := strategy.AlgorithmInputProjection{ValueFields: []string{"value"}, IdentityFields: []string{"host"}}
	primary := namedAlgorithmRequirement(t, "primary", strategy.AlgorithmInputPrimary, -60, 0, nil, projection)
	memory := namedAlgorithmRequirement(t, "input:memory", strategy.AlgorithmInputMultiDetect, -60, 0, nil, projection)
	algorithms := make([]contract.AlgorithmIRV2, 0, len(kinds))
	for index, kind := range kinds {
		var config any = thresholdOver("90")
		if kind == strategy.DetectorKindMultiDetectThreshold {
			cpu, _ := json.Marshal(thresholdOver("90"))
			config = map[string]any{"dataset_name": "primary", "input": map[string]any{"ref": "cpu", "display_name": "CPU"}, "threshold": json.RawMessage(cpu),
				"input_projection": projection, "requirements": []strategy.AlgorithmInputRequirement{primary}}
			if index > 0 {
				threshold, _ := json.Marshal(thresholdOver("80"))
				config = map[string]any{"dataset_name": "input:memory", "input": map[string]any{"ref": "memory", "display_name": "Memory"}, "threshold": json.RawMessage(threshold),
					"input_projection": projection, "requirements": []strategy.AlgorithmInputRequirement{primary, memory}}
			}
		}
		payload, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		algorithms = append(algorithms, contract.AlgorithmIRV2{Type: kind, Version: 1, Config: payload})
	}
	ref := contract.StrategyRefV2{TenantID: "tenant", StrategyID: "7", Revision: "revision-v1"}
	inputProjection := contract.InputProjectionV2{ValueFields: projection.ValueFields, BusinessIdentityField: "bk_biz_id",
		MultiValueAlignment: "SINGLE_VALUE", DataUnit: "none", MissingValuePolicy: contract.MissingValuePolicyRequired}
	level := contract.LevelIRV2{Definition: contract.LevelDefinitionV2{LevelID: 5, Priority: 1}, Connector: connector,
		DetectPlan:   contract.DetectPlanV2{Algorithms: algorithms},
		TriggerPlan:  contract.TypedPlanV1{Type: "N_OF_M", Version: 1, Config: json.RawMessage(`{"window_size":1,"required_anomalies":1,"step_seconds":60}`)},
		RecoveryPlan: contract.TypedPlanV1{Type: "CONTINUOUS_TRIGGER_MISS", Version: 1, Config: json.RawMessage(`{"enabled":true,"consecutive_windows":1}`)}}
	plan := contract.EvaluationPlanV2{PlanID: "7", StrategyRef: ref, InputProjection: inputProjection,
		StrategyIR: contract.StrategyIRV2{Schema: contract.Schema{Name: contract.StrategyIRSchemaV2, Major: 2}, StrategyRef: ref,
			InputProjection: inputProjection, ExecutionSemantics: contract.ExecutionSemanticsV2{EvaluationScope: contract.EvaluationScopeSeries,
				QueryWindow: 300, AggregationInterval: 60, EvaluationInterval: 60, LatenessTolerance: 120}, Levels: []contract.LevelIRV2{level}}}
	compiler, err := strategy.NewCompiler(strategy.NewDefaultAlgorithmCompilerRegistry(), strategy.Limits{MaxPlanBytes: 1 << 20,
		MaxLevelsPerPlan: 16, MaxAlgorithmsPerLevel: 8, MaxGroupsPerAlgorithm: 16, MaxConditionsPerAlgorithm: 64,
		MaxASTNodesPerLevel: 256, MaxTriggerWindowSize: 16, MaxRecoveryConsecutiveWindows: 16, MaxRequiredHistoryPoints: 32,
		MaxTriggerComputeCost: 1 << 20, MaxCompiledPlanBytes: 1 << 20, MaxCacheEntries: 16, MaxCacheBytes: 1 << 20,
		NegativeCacheTTL: time.Minute, BudgetRevision: "multi-detect-test"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiler.Compile(context.Background(), strategy.CompileRequest{Plan: plan,
		DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64), NormalizationDigest: strings.Repeat("2", 64),
			IdentityFields: projection.IdentityFields, SourceTimeField: "time", ReceivedTimeField: "received_time"},
		StateSemantics: strategy.StateSemantics{StateSchemaVersion: "window-state-v1", CodecSemanticsVersion: "window-codec-v1",
			IdentitySchemaDigest: strings.Repeat("3", 64), SourceTimeSemanticsVersion: "source-time-v1", HistoryCellSemanticsVersion: "history-cell-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	// A level refused is a terminal of its own, whether or not the plan
	// compiled around it.
	if terminals := result.LevelTerminals(); len(terminals) > 0 {
		return nil, &terminals[0]
	}
	compiled, ok := result.Plan()
	if !ok {
		return nil, result.PlanTerminal()
	}
	return compiled, nil
}

// The inputs of an item detected over several are joined strictly: the
// level is anomalous only when every input is, normal when every input is
// read and one is not, and undecided when any input has no point - a CPU
// under its threshold beside a memory that was not read included.
func TestTheInputsOfAMultiDetectLevelAreJoinedStrictly(t *testing.T) {
	plan, terminal := compileMultiDetectLevel(t, contract.LevelConnectorStrictAND)
	if terminal != nil {
		t.Fatalf("the level was refused: %+v", *terminal)
	}
	evaluator, err := NewEvaluator(NewDefaultRegistry())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := evaluator.PreparePlan(plan)
	if err != nil {
		t.Fatalf("PreparePlan() error = %v", err)
	}
	requirements := plan.Levels().At(0).Algorithms().At(1).InputRequirements()
	for _, test := range []struct {
		name, cpu, memory, want, reason string
	}{
		{name: "cpu not read, memory over", memory: "86", want: FactResultUnavailable, reason: contract.ReasonMultiDetectInputMissing},
		{name: "both over", cpu: "95", memory: "86", want: FactResultAnomalous},
		{name: "memory under", cpu: "95", memory: "50", want: FactResultNormal},
		{name: "both under", cpu: "50", memory: "50", want: FactResultNormal},
		{name: "memory not read", cpu: "95", want: FactResultUnavailable, reason: contract.ReasonMultiDetectInputMissing},
		{name: "cpu under, memory not read", cpu: "50", want: FactResultUnavailable, reason: contract.ReasonMultiDetectInputMissing},
	} {
		records := map[string][]contract.CanonicalRecordV2{}
		if test.cpu != "" {
			records["primary"] = []contract.CanonicalRecordV2{namedRecord(t, 600, test.cpu, nil)}
		}
		if test.memory != "" {
			records["input:memory"] = []contract.CanonicalRecordV2{namedRecord(t, 600, test.memory, nil)}
		}
		bindings, primary := namedBindings(t, requirements, records)
		if test.cpu == "" {
			// The round is judged at the memory input's record: the series
			// and the time are the same whichever input it came from.
			view := bindings[1].View
			primary, _ = view.Record(0)
		}
		input := execution.SeriesEvaluationInputRequest{
			Consumer:       execution.ConsumerRef{Plan: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}, LevelID: 5, HasLevel: true},
			SeriesIdentity: execution.SeriesIdentityDigest(primary.DimensionIdentity().Digest), Inputs: bindings,
		}
		for _, requirement := range requirements {
			input.RequirementIDs = append(input.RequirementIDs, execution.RequirementID(requirement.RequirementID))
		}
		facts, _, _, err := evaluator.EvaluatePreparedSeriesRecord(context.Background(), prepared, []execution.SeriesEvaluationInputRequest{input}, primary)
		if err != nil {
			t.Fatalf("%s: EvaluatePreparedSeriesRecord() error = %v", test.name, err)
		}
		if len(facts) != 1 || facts[0].Result != test.want || facts[0].ReasonCode != test.reason {
			t.Errorf("%s: facts = %+v, want %s %s", test.name, facts, test.want, test.reason)
		}
	}
}

// The strict join is the multi-input level's alone: a level of one input -
// its algorithms joined by AND as they always were - still reads a miss
// beside an unknown as a miss, and only a level with a MultiDetectThreshold
// may be strict, or one with one anything but strict.
func TestOnlyAMultiDetectLevelIsJoinedStrictly(t *testing.T) {
	if matched, determined := combineAlgorithmTruth(contract.LevelConnectorAND, []algorithmTruth{algorithmTruthFalse, algorithmTruthUnknown}); matched || !determined {
		t.Fatalf("AND of a miss and an unknown = %t/%t, want a decided miss as before", matched, determined)
	}
	if matched, determined := combineAlgorithmTruth(contract.LevelConnectorStrictAND, []algorithmTruth{algorithmTruthFalse, algorithmTruthUnknown}); matched || determined {
		t.Fatalf("STRICT_AND of a miss and an unknown = %t/%t, want undecided", matched, determined)
	}
	if matched, determined := combineAlgorithmTruth(contract.LevelConnectorStrictAND, []algorithmTruth{algorithmTruthTrue, algorithmTruthTrue}); !matched || !determined {
		t.Fatalf("STRICT_AND of two matches = %t/%t, want a decided match", matched, determined)
	}
	if _, terminal := compileMultiDetectLevel(t, contract.LevelConnectorAND); terminal == nil || terminal.ReasonCode != contract.ReasonLevelInvalid {
		t.Fatalf("a multi-input level joined by AND compiled (%+v), want LEVEL_INVALID", terminal)
	}
	if _, terminal := compileMultiDetectLevel(t, contract.LevelConnectorStrictAND, strategy.DetectorKindThreshold); terminal == nil ||
		terminal.ReasonCode != contract.ReasonLevelInvalid {
		t.Fatalf("a strict level of one Threshold compiled (%+v), want LEVEL_INVALID", terminal)
	}
	// A plain Threshold beside a MultiDetectThreshold: on a round the other
	// input anchors it would judge that input's value as the PRIMARY's.
	if _, terminal := compileMultiDetectLevel(t, contract.LevelConnectorStrictAND, strategy.DetectorKindThreshold,
		strategy.DetectorKindMultiDetectThreshold); terminal == nil || terminal.ReasonCode != contract.ReasonLevelInvalid {
		t.Fatalf("a strict level of a Threshold and a MultiDetectThreshold compiled (%+v), want LEVEL_INVALID", terminal)
	}
}
