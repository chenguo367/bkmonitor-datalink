// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package strategy

import (
	"context"
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// multiDetectThresholdConfigV1 is a MultiDetectThreshold as a Plan writes it:
// the named input it reads, the Threshold config its point is judged by -
// in the input's own data unit - and the inputs the algorithm reads, the
// PRIMARY first and the input second.
type multiDetectThresholdConfigV1 struct {
	DatasetName     string                      `json:"dataset_name"`
	Input           MultiDetectInput            `json:"input"`
	Threshold       json.RawMessage             `json:"threshold"`
	InputProjection AlgorithmInputProjection    `json:"input_projection"`
	Requirements    []AlgorithmInputRequirement `json:"requirements"`
}

type multiDetectThresholdAlgorithmCompiler struct{}

func (multiDetectThresholdAlgorithmCompiler) Capability() AlgorithmCapability {
	return AlgorithmCapability{
		Kind: DetectorKindMultiDetectThreshold, Version: 1, EvaluationScope: contract.EvaluationScopeSeries,
		InputShape: "NAMED_SERIES", RequiredHistoryKind: algorithmHistoryPlanLocalInput,
		StateSchemaVersion: "multi-detect-threshold-stateless-v1", Deterministic: true,
		FixedComputeCost: 1, CostPerRecord: 1, Gate: MultiDetectCapability,
	}
}

// Compile compiles the input's threshold as a Threshold is compiled, against
// the input's own data unit, and checks the inputs it reads: the PRIMARY,
// and the named input - read over the PRIMARY's own window, step and
// alignment, so that the input's point at the PRIMARY's source time is its
// point of the same round.
func (multiDetectThresholdAlgorithmCompiler) Compile(ctx context.Context, compileContext AlgorithmCompileContext, raw contract.AlgorithmIRV2) (AlgorithmCompileResult, error) {
	var config multiDetectThresholdConfigV1
	if err := decodeStrict(raw.Config, &config); err != nil {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: %v", err)
	}
	if config.DatasetName == "" {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: the named input it reads is required")
	}
	// The input it judges, by its reference and shown name; any input but
	// the first, the PRIMARY, is read from the dataset its reference names.
	if !multiDetectInputRef(config.Input.Ref) || config.Input.DisplayName == "" ||
		config.DatasetName != "primary" && config.DatasetName != "input:"+config.Input.Ref {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: input %+v does not name the input it reads from %q", config.Input, config.DatasetName)
	}
	if err := config.InputProjection.validate(); err != nil {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: %v", err)
	}
	want := AlgorithmInputProjection{
		ValueFields:     append([]string(nil), compileContext.Projection.ValueFields...),
		DimensionFields: append([]string(nil), compileContext.Projection.DimensionFields...),
		IdentityFields:  append([]string(nil), compileContext.IdentityFields...),
	}
	if !equalAlgorithmProjection(config.InputProjection, want) {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: input projection does not match plan and identity contracts")
	}
	requirements, err := canonicalAlgorithmRequirements(config.Requirements)
	if err != nil {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: %v", err)
	}
	// The first input is the PRIMARY itself, read as one; every other is
	// the PRIMARY and an input of its own query over the PRIMARY's window.
	primaryOnly := config.DatasetName == "primary" && len(requirements) == 1 && requirements[0].Role == AlgorithmInputPrimary &&
		requirements[0].DatasetName == "primary" && len(requirements[0].PointOffsetsSeconds) == 0
	peer := config.DatasetName != "primary" && len(requirements) == 2 && requirements[0].Role == AlgorithmInputPrimary &&
		requirements[0].DatasetName == "primary" && requirements[1].Role == AlgorithmInputMultiDetect &&
		requirements[1].DatasetName == config.DatasetName && requirements[1].RelativeWindow == requirements[0].RelativeWindow &&
		requirements[1].StepMillis == requirements[0].StepMillis && requirements[1].AlignmentMillis == requirements[0].AlignmentMillis &&
		len(requirements[1].PointOffsetsSeconds) == 0 && requirements[1].LogicalQueryRef != requirements[0].LogicalQueryRef
	if !primaryOnly && !peer {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: the PRIMARY alone, or the PRIMARY and one input of its own query read over the PRIMARY's window, are required")
	}
	for _, requirement := range requirements {
		if !equalAlgorithmProjection(requirement.InputProjection, config.InputProjection) {
			return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: input requirement projection mismatch")
		}
	}
	// The threshold is the input's own, in its own data unit: compiled as a
	// Threshold of a Plan whose value is in that unit.
	var unit struct {
		DataUnit string `json:"data_unit"`
	}
	if err := json.Unmarshal(config.Threshold, &unit); err != nil {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: threshold: %v", err)
	}
	inner := compileContext
	inner.Projection.DataUnit = unit.DataUnit
	thresholdIR := contract.AlgorithmIRV2{Type: DetectorKindThreshold, Version: 1, Config: config.Threshold}
	threshold, err := thresholdAlgorithmCompiler{}.Compile(ctx, inner, thresholdIR)
	if err != nil {
		return AlgorithmCompileResult{}, err
	}
	// Validated as a Threshold of the level is - which is what lets its
	// predicate be evaluated at all.
	if err := validateAlgorithmCompileResult(thresholdIR, thresholdAlgorithmCompiler{}.Capability(), inner.Projection, &threshold); err != nil {
		return AlgorithmCompileResult{}, err
	}
	if threshold.Config.Threshold == nil || threshold.Config.SourceProvenance != nil || len(threshold.InputRequirements) != 0 {
		return AlgorithmCompileResult{}, configErrorf("MultiDetectThreshold config: the threshold is a plain one")
	}
	return g4CompileResult(
		compiledAlgorithmConfig{MultiDetectThreshold: &MultiDetectThresholdConfig{DatasetName: config.DatasetName, Input: config.Input,
			Threshold: threshold.Config.Threshold, detector: threshold.Detector, normalizer: threshold.Normalizer}},
		config.InputProjection, requirements, "multi-detect-threshold-compiler-v1", "exact-same-time-point-v1", threshold.ASTNodes,
	), nil
}

// multiDetectInputRef is whether ref is an input reference as the item's
// query_output_config writes one: an identifier of at most 64 bytes.
func multiDetectInputRef(ref string) bool {
	if ref == "" || len(ref) > 64 {
		return false
	}
	for index := 0; index < len(ref); index++ {
		c := ref[index]
		letter := c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if !letter && (index == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return true
}
