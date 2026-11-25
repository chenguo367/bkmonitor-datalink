// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// compileFailureDisposition is the disposition a query compile's refusal
// names, false for an error that names none.
func compileFailureDisposition(sourceID string, err error) (ObjectDisposition, bool) {
	var compileFailure *QueryPlanCompileError
	if !errors.As(err, &compileFailure) || compileFailure.Disposition == "" || compileFailure.Reason == "" {
		return ObjectDisposition{}, false
	}
	disposition := ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: compileFailure.Disposition, Reason: compileFailure.Reason}
	if compileFailure.FieldPath != "" {
		disposition.FieldPath = "items[0]." + compileFailure.FieldPath
	}
	if compileFailure.Err != nil {
		disposition.Detail = dispositionDetail(compileFailure.Err.Error())
	}
	return disposition, true
}

// algorithmInputRefRefusal refuses an item that is not detected over several
// inputs and yet binds an algorithm to one (input_ref): the writer meant a
// multi-input item and wrote another contract, and detecting the algorithm
// against the item's one expression would judge it against the wrong value.
func algorithmInputRefRefusal(sourceID string, item legacyItem) *ObjectDisposition {
	for index, algorithm := range item.Algorithms {
		if algorithm.InputRef != "" {
			return &ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: DispositionConfigRejected,
				Reason: "ALGORITHM_INPUT_REF_UNEXPECTED", FieldPath: fmt.Sprintf("items[0].algorithms[%d].input_ref", index),
				Detail: dispositionDetail(fmt.Sprintf("input_ref %q on an item whose query_output_config is not multi_detect/v1", algorithm.InputRef))}
		}
	}
	return nil
}

// compileMultiDetectItem compiles and checks an item detected over several
// inputs: its inputs' queries (MultiDetectQueryCompiler), then its
// algorithms against them - one level, one Threshold for each input, every
// input bound - no no-data detection, and time series only. A refusal is
// returned with the error that names it.
func compileMultiDetectItem(ctx context.Context, planner PrimaryQueryCompiler, sourceID string, item legacyItem,
	querySource PrimaryQuerySource,
) (MultiDetect, *ObjectDisposition, error) {
	refuse := func(disposition ObjectDisposition) (MultiDetect, *ObjectDisposition, error) {
		return MultiDetect{}, &disposition, errors.New(disposition.Reason + ": " + disposition.Detail)
	}
	compiler, ok := planner.(MultiDetectQueryCompiler)
	if !ok {
		return MultiDetect{}, nil, errors.New("MULTI_DETECT_QUERY_COMPILER_UNAVAILABLE")
	}
	detected, err := compiler.CompileMultiDetect(ctx, querySource)
	if err != nil {
		if disposition, named := compileFailureDisposition(sourceID, err); named {
			if disposition.FieldPath == "" {
				disposition.FieldPath = "items[0].query_output_config"
			}
			return MultiDetect{}, &disposition, fmt.Errorf("QUERY_PLAN_INVALID: %w", err)
		}
		return MultiDetect{}, nil, fmt.Errorf("QUERY_PLAN_INVALID: %w", err)
	}
	if refusal := multiDetectAlgorithmRefusal(sourceID, item, detected); refusal != nil {
		return refuse(*refusal)
	}
	for index, label := range itemDataTypes(item) {
		// An event or log count is read from earlier than its window (its
		// PRIMARY's provider lead), and its quiet steps are zeros it infers:
		// another input's window and points would not be the PRIMARY's.
		if label != "time_series" {
			return refuse(ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: DispositionConfigRejected,
				Reason: "MULTI_DETECT_DATA_TYPE_UNSUPPORTED", FieldPath: fmt.Sprintf("items[0].query_configs[%d].data_type_label", index),
				Detail: dispositionDetail(fmt.Sprintf("data type %q: an item detected over several inputs reads time series only", label))})
		}
	}
	if enabled, err := multiDetectNoDataEnabled(item.NoDataConfig); err != nil || enabled {
		detail := "no-data detection is not offered for an item detected over several inputs"
		if err != nil {
			detail = err.Error()
		}
		return refuse(ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: DispositionConfigRejected,
			Reason: "MULTI_DETECT_NO_DATA_UNSUPPORTED", FieldPath: "items[0].no_data_config", Detail: dispositionDetail(detail)})
	}
	return detected, nil, nil
}

// multiDetectAlgorithmRefusal checks an item's algorithms against its inputs:
// one level, Threshold algorithms only, each naming an input, and every
// input named by one at least.
func multiDetectAlgorithmRefusal(sourceID string, item legacyItem, detected MultiDetect) *ObjectDisposition {
	rejected := func(reason, field, detail string) *ObjectDisposition {
		return &ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: DispositionConfigRejected,
			Reason: reason, FieldPath: field, Detail: dispositionDetail(detail)}
	}
	inputs := make(map[string]bool, len(detected.Inputs))
	for _, input := range detected.Inputs {
		inputs[input.Ref] = false
	}
	levels := make(map[uint32]struct{}, 1)
	for index, algorithm := range item.Algorithms {
		field := fmt.Sprintf("items[0].algorithms[%d]", index)
		levels[algorithm.Level] = struct{}{}
		if len(levels) > 1 {
			return rejected("MULTI_DETECT_LEVELS_UNSUPPORTED", field+".level",
				"an item detected over several inputs has one level")
		}
		if algorithm.Type != strategy.DetectorKindThreshold {
			return rejected("MULTI_DETECT_ALGORITHM_UNSUPPORTED", field+".type",
				fmt.Sprintf("algorithm %q: an item detected over several inputs detects by thresholds only", algorithm.Type))
		}
		bound, known := inputs[algorithm.InputRef]
		if !known {
			return rejected("MULTI_DETECT_ALGORITHM_UNBOUND", field+".input_ref",
				fmt.Sprintf("input_ref %q names no input", algorithm.InputRef))
		}
		if bound {
			// One Threshold an input: its groups say or and its conditions
			// and, and the level's join is the inputs' alone.
			return rejected("MULTI_DETECT_ALGORITHM_UNSUPPORTED", field+".input_ref",
				fmt.Sprintf("input %q has more than one algorithm: write its conditions in one Threshold", algorithm.InputRef))
		}
		inputs[algorithm.InputRef] = true
	}
	for _, input := range detected.Inputs {
		if !inputs[input.Ref] {
			return rejected("MULTI_DETECT_ALGORITHM_UNBOUND", "items[0].algorithms",
				fmt.Sprintf("input %q has no algorithm", input.Ref))
		}
	}
	return nil
}

// multiDetectNoDataEnabled is whether an item's no_data_config turns no-data
// detection on, read as frozenNoDataConfig reads it.
func multiDetectNoDataEnabled(raw json.RawMessage) (bool, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return false, nil
	}
	var config legacyNoDataConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return false, fmt.Errorf("no_data_config: %w", err)
	}
	return legacyNoDataEnabled(config.IsEnabled)
}

// multiDetectInputOf is the input an algorithm binds by its input_ref, the
// item's algorithms already checked against its inputs
// (multiDetectAlgorithmRefusal).
func multiDetectInputOf(inputs []MultiDetectInput, ref string) MultiDetectInput {
	for _, input := range inputs {
		if input.Ref == ref {
			return input
		}
	}
	return MultiDetectInput{}
}

// btoi is 1 for true.
func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

// multiDetectThresholdConfig is the MultiDetectThreshold config of an input:
// the Threshold the input's point is judged by, in the input's unit, and
// what it reads - the PRIMARY alone for the first input, the PRIMARY and the
// input for every other, each a requirement the Plan's inputs hold
// (buildRequirements).
func multiDetectThresholdConfig(
	raw legacyAlgorithm,
	unit string,
	levelID uint32,
	projection contract.InputProjectionV2,
	identityFields []string,
	interval int64,
	step int64,
	inputs *compiledPlanInputs,
	peer MultiDetectInput,
) (json.RawMessage, error) {
	threshold, err := thresholdConfig(raw, unit)
	if err != nil {
		return nil, err
	}
	inputProjection := execution.InputProjection{
		ValueFields:     append([]string(nil), projection.ValueFields...),
		DimensionFields: append([]string(nil), projection.DimensionFields...),
		IdentityFields:  append([]string(nil), identityFields...),
	}
	requirements, err := inputs.buildRequirements(strategy.DetectorKindMultiDetectThreshold, levelID, interval, step, inputProjection)
	if err != nil {
		return nil, err
	}
	dataset := multiDetectDatasetName(peer.Ref)
	if len(inputs.multiDetectAll) > 0 && peer.Ref == inputs.multiDetectAll[0].Ref {
		dataset = "primary"
	}
	algorithmProjection := strategy.AlgorithmInputProjection{
		ValueFields:     append([]string(nil), inputProjection.ValueFields...),
		DimensionFields: append([]string(nil), inputProjection.DimensionFields...),
		IdentityFields:  append([]string(nil), inputProjection.IdentityFields...),
	}
	read := make([]strategy.AlgorithmInputRequirement, 0, 2)
	for _, requirement := range requirements {
		if requirement.Role != execution.InputRolePrimary && requirement.DatasetName != dataset {
			continue
		}
		read = append(read, strategy.AlgorithmInputRequirement{
			RequirementID: string(requirement.RequirementID), DatasetName: string(requirement.DatasetName),
			Role: strategy.AlgorithmInputRole(requirement.Role), ConsumerLevelID: requirement.ConsumerLevelID,
			LogicalQueryRef: string(requirement.LogicalQueryRef),
			RelativeWindow: strategy.AlgorithmRelativeWindow{StartOffsetSeconds: requirement.RelativeWindow.StartOffsetSeconds,
				EndOffsetSeconds: requirement.RelativeWindow.EndOffsetSeconds, HalfOpen: requirement.RelativeWindow.HalfOpen},
			StepMillis: requirement.StepMillis, AlignmentMillis: requirement.AlignmentMillis,
			ReadinessClass:  strategy.AlgorithmReadinessClass(requirement.ReadinessClass),
			InputProjection: algorithmProjection,
		})
	}
	if want := 2 - btoi(dataset == "primary"); len(read) != want {
		return nil, fmt.Errorf("alarmd controlplane: input %q is not among the Plan's requirements", peer.Ref)
	}
	// The input the algorithm judges, as its events name it: its reference
	// and the name a reader is shown.
	input := struct {
		Ref         string `json:"ref"`
		DisplayName string `json:"display_name"`
	}{peer.Ref, peer.DisplayName}
	return json.Marshal(struct {
		DatasetName     string                               `json:"dataset_name"`
		Input           any                                  `json:"input"`
		Threshold       json.RawMessage                      `json:"threshold"`
		InputProjection strategy.AlgorithmInputProjection    `json:"input_projection"`
		Requirements    []strategy.AlgorithmInputRequirement `json:"requirements"`
	}{string(dataset), input, threshold, algorithmProjection, read})
}

// multiDetectCompiledDetail says what a multi-input item compiled to: its
// inputs, and each level's join and algorithms in order. A withheld item's
// disposition carries it, beside the workers that held it
// (multiDetectWithheld).
func multiDetectCompiledDetail(detected MultiDetect, plan contract.EvaluationPlanV2) string {
	refs := make([]string, 0, len(detected.Inputs))
	for _, input := range detected.Inputs {
		refs = append(refs, input.Ref)
	}
	levels := make([]string, 0, len(plan.StrategyIR.Levels))
	for _, level := range plan.StrategyIR.Levels {
		kinds := make([]string, 0, len(level.DetectPlan.Algorithms))
		for _, algorithm := range level.DetectPlan.Algorithms {
			kinds = append(kinds, algorithm.Type)
		}
		levels = append(levels, fmt.Sprintf("level %d %s %s", level.Definition.LevelID, level.Connector, strings.Join(kinds, "+")))
	}
	return fmt.Sprintf("inputs %s compiled: %s", strings.Join(refs, ","), strings.Join(levels, "; "))
}

// MultiDetectAdmission is whether a round's Catalog may hold the Plans of
// items detected over several inputs: only while every ready worker declares
// the capability (ownership.CapabilityMultiDetect), since a worker without it
// refuses such a Plan's requirements and with them its Query Group's Slot,
// the single-query Plans in it too (015 F1 6.2). Unaware names the ready
// workers that do not declare it, sorted; Known says the ready set was read
// at all. The zero value is no ready set known, and admits nothing.
type MultiDetectAdmission struct {
	Admitted bool
	Known    bool
	Unaware  []string
}

// key is the admission as the round key carries it: a round whose sources
// did not move and whose admission did rebuilds, and one whose held item
// names other workers says so.
func (admission MultiDetectAdmission) key() string {
	switch {
	case admission.Admitted:
		return "multi-detect:admitted"
	case !admission.Known:
		return "multi-detect:unknown"
	default:
		return "multi-detect:held:" + strings.Join(admission.Unaware, ",")
	}
}

// multiDetectHeldWorkersListed bounds the workers a held item's detail names;
// the count carries the rest.
const multiDetectHeldWorkersListed = 5

// multiDetectWithheld is the disposition of an item detected over several
// inputs the round does not admit: a reversible capability refusal, alerts
// left as they are, naming what it compiled to and which workers held it.
func multiDetectWithheld(sourceID, compiled string, admission MultiDetectAdmission) ObjectDisposition {
	held := "no ready worker is known yet"
	switch {
	case admission.Known && len(admission.Unaware) == 0:
		held = "no worker is ready"
	case admission.Known:
		listed := admission.Unaware
		if len(listed) > multiDetectHeldWorkersListed {
			listed = listed[:multiDetectHeldWorkersListed]
		}
		held = fmt.Sprintf("%d of the ready workers do not declare %s: %s", len(admission.Unaware), multiDetectCapabilityWord, strings.Join(listed, ","))
		if len(admission.Unaware) > len(listed) {
			held += fmt.Sprintf(" and %d more", len(admission.Unaware)-len(listed))
		}
	}
	return ObjectDisposition{SourceID: sourceID, Scope: "PLAN", Disposition: DispositionUnsupported, Reason: "MULTI_DETECT_NOT_ENABLED",
		FieldPath: "items[0].query_output_config", Detail: dispositionDetail(compiled + "; held: " + held)}
}

// multiDetectCapabilityWord is the capability a held item's detail names:
// the algorithm's registry gate, which the workers declare by the same word.
const multiDetectCapabilityWord = strategy.MultiDetectCapability
