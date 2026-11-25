// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The values a round's inputs hold of the series: CPU over 90 and memory over
// 80 is a hit, both under is a miss, and CPU with no memory is a round the
// item detected over the two cannot decide.
var (
	roundHit       = map[string]string{"primary": "95", "input:memory": "86"}
	roundMiss      = map[string]string{"primary": "50", "input:memory": "50"}
	roundUndecided = map[string]string{"primary": "50"}
)

// undecidedPlan compiles one Level judged on one position, one anomaly firing
// it, recovered after consecutive quiet windows: with peers, the item detected
// over CPU over 90 and each peer over 80, each its own threshold, joined
// strictly; with none, CPU over 90 alone, as any single query is.
func undecidedPlan(t *testing.T, consecutive uint32, peers ...string) *strategy.CompiledPlan {
	t.Helper()
	return undecidedPlanWindow(t, 1, consecutive, peers...)
}

// undecidedPlanWindow is undecidedPlan judged on window positions, one
// anomaly among them firing it.
func undecidedPlanWindow(t *testing.T, window, consecutive uint32, peers ...string) *strategy.CompiledPlan {
	t.Helper()
	projection := strategy.AlgorithmInputProjection{ValueFields: []string{"value"}, IdentityFields: []string{"host"}}
	primary := g4Requirement(t, "primary", strategy.AlgorithmInputPrimary, -60, 0, nil, projection)
	// An input's threshold is in percent, a unit of a scale, the first step
	// of which its judged values are in: a factor of one, the values as read.
	threshold := func(limit, unit string) map[string]any {
		return map[string]any{"value_field": "value", "data_unit": unit, "threshold_unit_prefix": "",
			"precision": map[string]any{"decimal_places": 6, "rounding": "HALF_EVEN"},
			"groups":    []map[string]any{{"conditions": []map[string]any{{"operator": "GT", "threshold_decimal": limit}}}}}
	}
	algorithm := func(kind string, config map[string]any) contract.AlgorithmIRV2 {
		payload, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		return contract.AlgorithmIRV2{Type: kind, Version: 1, Config: payload}
	}
	connector := contract.LevelConnectorAND
	var algorithms []contract.AlgorithmIRV2
	if len(peers) > 0 {
		connector = contract.LevelConnectorStrictAND
		cpu, _ := json.Marshal(threshold("90", "percent"))
		algorithms = append(algorithms, algorithm(strategy.DetectorKindMultiDetectThreshold, map[string]any{"dataset_name": "primary",
			"input": map[string]any{"ref": "cpu", "display_name": "CPU"}, "threshold": json.RawMessage(cpu), "input_projection": projection, "requirements": []strategy.AlgorithmInputRequirement{primary}}))
		for _, peer := range peers {
			name := "input:" + peer
			requirement := g4Requirement(t, name, strategy.AlgorithmInputMultiDetect, -60, 0, nil, projection)
			used, _ := json.Marshal(threshold("80", "percent"))
			algorithms = append(algorithms, algorithm(strategy.DetectorKindMultiDetectThreshold, map[string]any{"dataset_name": name,
				"input": map[string]any{"ref": peer, "display_name": peer}, "threshold": json.RawMessage(used), "input_projection": projection, "requirements": []strategy.AlgorithmInputRequirement{primary, requirement}}))
		}
	} else {
		// A single query's Threshold reads the Slot's PRIMARY record, as every
		// threshold strategy of one query does.
		algorithms = []contract.AlgorithmIRV2{algorithm(strategy.DetectorKindThreshold, threshold("90", "none"))}
	}
	compiler, err := strategy.NewCompiler(strategy.NewDefaultAlgorithmCompilerRegistry(), strategy.Limits{MaxPlanBytes: 1 << 20,
		MaxLevelsPerPlan: 16, MaxAlgorithmsPerLevel: 8, MaxGroupsPerAlgorithm: 16, MaxConditionsPerAlgorithm: 64,
		MaxASTNodesPerLevel: 256, MaxTriggerWindowSize: 16, MaxRecoveryConsecutiveWindows: 16, MaxRequiredHistoryPoints: 32,
		MaxTriggerComputeCost: 1 << 20, MaxCompiledPlanBytes: 1 << 20, MaxCacheEntries: 16, MaxCacheBytes: 1 << 20,
		NegativeCacheTTL: time.Minute, BudgetRevision: "multi-detect-undecided-test"})
	if err != nil {
		t.Fatal(err)
	}
	ref := contract.StrategyRefV2{TenantID: "tenant", StrategyID: "7", Revision: "r1"}
	inputProjection := contract.InputProjectionV2{ValueFields: projection.ValueFields, BusinessIdentityField: "bk_biz_id",
		MultiValueAlignment: "SINGLE_VALUE", DataUnit: "none", MissingValuePolicy: contract.MissingValuePolicyRequired}
	level := contract.LevelIRV2{Definition: contract.LevelDefinitionV2{LevelID: 5, Priority: 1}, Connector: connector,
		DetectPlan: contract.DetectPlanV2{Algorithms: algorithms},
		TriggerPlan: contract.TypedPlanV1{Type: "N_OF_M", Version: 1,
			Config: json.RawMessage(fmt.Sprintf(`{"window_size":%d,"required_anomalies":1,"step_seconds":60}`, window))},
		RecoveryPlan: contract.TypedPlanV1{Type: "CONTINUOUS_TRIGGER_MISS", Version: 1,
			Config: json.RawMessage(fmt.Sprintf(`{"enabled":true,"consecutive_windows":%d}`, consecutive))}}
	plan := contract.EvaluationPlanV2{PlanID: "7", StrategyRef: ref, InputProjection: inputProjection,
		StrategyIR: contract.StrategyIRV2{Schema: contract.Schema{Name: contract.StrategyIRSchemaV2, Major: 2}, StrategyRef: ref,
			InputProjection: inputProjection, ExecutionSemantics: contract.ExecutionSemanticsV2{EvaluationScope: contract.EvaluationScopeSeries,
				QueryWindow: 300, AggregationInterval: 60, EvaluationInterval: 60, LatenessTolerance: 120}, Levels: []contract.LevelIRV2{level}}}
	result, err := compiler.Compile(context.Background(), strategy.CompileRequest{Plan: plan,
		DatasetContract: contract.DatasetContractV2{SchemaDigest: strings.Repeat("1", 64), NormalizationDigest: strings.Repeat("2", 64),
			IdentityFields: projection.IdentityFields, SourceTimeField: "time", ReceivedTimeField: "received_time"},
		StateSemantics: strategy.StateSemantics{StateSchemaVersion: "s", CodecSemanticsVersion: "c", IdentitySchemaDigest: strings.Repeat("3", 64),
			SourceTimeSemanticsVersion: "t", HistoryCellSemanticsVersion: "h"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled, ok := result.Plan()
	if !ok || len(result.LevelTerminals()) > 0 {
		t.Fatalf("compile terminal = %+v/%+v", result.PlanTerminal(), result.LevelTerminals())
	}
	return compiled
}

// undecidedRounds runs one series through rounds a minute apart, each round
// reading what the one before wrote, as the worker does between Slots.
type undecidedRounds struct {
	t         *testing.T
	plan      *strategy.CompiledPlan
	evaluator *Evaluator
	view      *execution.RuntimeStateView
	// heldEnd and heldInside are the recovery windows the last round's walk
	// held, as the evaluator reports them (WithMultiDetectRecoveryHeld).
	heldEnd, heldInside uint32
}

func newUndecidedRounds(t *testing.T, plan *strategy.CompiledPlan) *undecidedRounds {
	run := &undecidedRounds{t: t, plan: plan, evaluator: newEvaluator(t)}
	run.evaluator.WithMultiDetectRecoveryHeld(func(end, inside uint32) { run.heldEnd, run.heldInside = run.heldEnd+end, run.heldInside+inside })
	return run
}

// round evaluates the series in the Slot at, its inputs holding the values
// given at the minute the Slot reads - an input not named holds nothing of the
// series - keeps the State the round wrote for the next, and returns the
// Level's outcome.
func (r *undecidedRounds) round(at int64, values map[string]string) execution.LevelOutcome {
	t := r.t
	t.Helper()
	source := at - 60
	records := map[string][]contract.CanonicalRecordV2{}
	for name, value := range values {
		records[name] = []contract.CanonicalRecordV2{g4Record(source, value, nil)}
	}
	// One query's round reads its PRIMARY record through the Slot's own
	// binding; the item over several binds every input it reads.
	primary := records["primary"]
	if len(primary) == 0 {
		primary = []contract.CanonicalRecordV2{g4Record(source, "0", nil)}
	}
	request := requestFixtureAt(t, r.plan, at, primary, nil)
	if r.view != nil {
		request.State.Items[0] = *r.view
	}
	if r.plan.Levels().At(0).Connector() == contract.LevelConnectorStrictAND {
		request.Inputs = []execution.SeriesEvaluationInputRequest{undecidedInput(t, request, records)}
	}
	r.heldEnd, r.heldInside = 0, 0
	result, err := r.evaluator.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatalf("round at %d: %v", at, err)
	}
	if err := result.Validate(request); err != nil {
		t.Fatalf("round at %d: result contract: %v", at, err)
	}
	plan := result.Plans[0]
	if len(plan.LevelOutcomes) != 1 {
		t.Fatalf("round at %d: outcomes %+v, want the one Level's", at, plan.LevelOutcomes)
	}
	if len(plan.StateResults) == 1 {
		view := carriedView(plan.StateResults[0].Mutation)
		r.view = &view
	}
	return plan.LevelOutcomes[0]
}

// pointAt is what the State holds at the source time: the Level's fact there,
// or "" for no point.
func (r *undecidedRounds) pointAt(source int64) execution.LevelFactResult {
	if r.view == nil {
		return ""
	}
	for _, point := range r.view.History {
		if point.SourceTime == source && len(point.Levels) == 1 {
			return point.Levels[0].Result
		}
	}
	return ""
}

// carriedView is the State a mutation leaves, as the next Slot loads it: the
// history merged, each Level's guard with its origin, the status its Levels'
// completeness makes.
func carriedView(mutation execution.StateMutation) execution.RuntimeStateView {
	status := execution.StateFoundReady
	for _, level := range mutation.Levels {
		switch {
		case level.HistoryCompleteness == execution.HistoryGapped:
			status = execution.StateFoundGapped
		case level.HistoryCompleteness == execution.HistoryWarming && status == execution.StateFoundReady:
			status = execution.StateFoundWarming
		}
	}
	view := stateViewFromMutation(mutation, status)
	for index, level := range mutation.Levels {
		view.Levels[index].GapReasonOriginSlot = level.GapReasonOriginSlot
		view.Levels[index].LastRealPointTime = level.LastRealPointTime
	}
	return view
}

// undecidedInput binds every input the Level's algorithms read, once each, as
// the worker binds one series: a dataset of that series' rows, empty where the
// input holds none of it.
func undecidedInput(t *testing.T, request execution.EvaluationRequest, records map[string][]contract.CanonicalRecordV2) execution.SeriesEvaluationInputRequest {
	t.Helper()
	consumer := execution.ConsumerRef{Plan: request.Header.DuePlans[0].Identity, LevelID: 5, HasLevel: true}
	input := execution.SeriesEvaluationInputRequest{Contract: request.Header.Contract, Consumer: consumer,
		SeriesIdentity: execution.SeriesIdentityDigest(request.State.Items[0].Identity.SeriesIdentityDigest)}
	algorithms := request.Header.DuePlans[0].CompiledPlan.Levels().At(0).Algorithms()
	bound := map[string]bool{}
	for index := 0; index < algorithms.Len(); index++ {
		for _, requirement := range algorithms.At(index).InputRequirements() {
			if bound[requirement.RequirementID] {
				continue
			}
			bound[requirement.RequirementID] = true
			dataset := execution.NewDataset(records[requirement.DatasetName])
			ordinals := make([]uint32, dataset.Len())
			for row := range ordinals {
				ordinals[row] = uint32(row)
			}
			view, err := execution.NewDatasetView(dataset, ordinals)
			if err != nil {
				t.Fatal(err)
			}
			state := execution.DataStateData
			if dataset.Len() == 0 {
				state = execution.DataStateEmpty
			}
			input.RequirementIDs = append(input.RequirementIDs, execution.RequirementID(requirement.RequirementID))
			input.Inputs = append(input.Inputs, execution.NamedInputBinding{Consumer: consumer, RequirementID: execution.RequirementID(requirement.RequirementID),
				DatasetName: execution.DatasetName(requirement.DatasetName), Role: execution.InputRole(requirement.Role), Dataset: dataset, View: view,
				Completeness: execution.CompletenessFull, DataState: state, Disposition: execution.AccessAvailable})
		}
	}
	return input
}

// firedAt is the Slot of the round that fires the alert: late enough that the
// longest window here, eleven positions, ends after the epoch.
const firedAt = 6000

// recoveredAt runs the rounds after a round that fires the alert, one a
// minute, a nil round being one no input holds the series in (no round at
// all), and returns which of them - counted from 1 - first recovered, 0 for
// none. Nothing here holds the alert open, so a later round may recover
// again; only the first is the alert's.
func recoveredAt(t *testing.T, plan *strategy.CompiledPlan, rounds []map[string]string) int {
	t.Helper()
	run := newUndecidedRounds(t, plan)
	if outcome := run.round(firedAt, roundHit); outcome.Outcome != execution.LevelOutcomeAbnormal {
		t.Fatalf("the first round %+v, want it to fire", outcome)
	}
	for index, values := range rounds {
		if values == nil {
			continue
		}
		if outcome := run.round(firedAt+int64(index+1)*60, values); outcome.Outcome == execution.LevelOutcomeRecovery {
			return index + 1
		}
	}
	return 0
}

// sequence is n rounds of values, each in turn.
func sequence(n int, values ...map[string]string) []map[string]string {
	rounds := make([]map[string]string, n)
	for index := range rounds {
		rounds[index] = values[index%len(values)]
	}
	return rounds
}

// asHoles is the rounds of the single-query control: a round the item over two
// inputs could not decide is, for one query, a round the series was not read
// in - a hole.
func asHoles(rounds []map[string]string) []map[string]string {
	holes := make([]map[string]string, len(rounds))
	for index, values := range rounds {
		if len(values) == len(roundUndecided) && values["input:memory"] == "" && values["primary"] != "" {
			continue
		}
		holes[index] = values
	}
	return holes
}

// Undecided rounds in a row after the alert fired delay its recovery by
// themselves (015 F1 D4): each is written as a point the Level could not use -
// the second under the guard the first raised too - and neither counts as a
// quiet minute, so the recovery comes check_window decided misses after them,
// not check_window rounds after the alert. The same rounds for one query are
// holes, which a read has covered and which count as quiet, as they always
// did: that series recovers on its first miss.
func TestUndecidedRoundsInARowDelayTheRecoveryByThemselves(t *testing.T) {
	plan := undecidedPlan(t, 3, "memory")
	run := newUndecidedRounds(t, plan)
	run.round(firedAt, roundHit)
	for _, at := range []int64{firedAt + 60, firedAt + 120} {
		if outcome := run.round(at, roundUndecided); outcome.Outcome != execution.LevelOutcomeUnknown {
			t.Fatalf("the round at %d: %+v, want it undecided", at, outcome)
		}
		if got := run.pointAt(at - 60); got != execution.LevelFactUnavailable {
			t.Fatalf("the undecided round at %d holds %q, want a point the Level could not use", at, got)
		}
	}
	rounds := append(sequence(2, roundUndecided), sequence(3, roundMiss)...)
	if multi := recoveredAt(t, plan, rounds); multi != 5 {
		t.Fatalf("the item over two inputs recovered at round %d, want 5: two undecided, then three misses", multi)
	}
	if single := recoveredAt(t, undecidedPlan(t, 3), asHoles(rounds)); single != 3 {
		t.Fatalf("one query recovered at round %d, want 3: the two holes count as quiet", single)
	}
}

// Undecided rounds coming between misses delay the recovery only by
// themselves: it comes on the check_window-th decided miss, the walk stepping
// over each undecided round rather than starting again on it. One query, the
// same rounds holes, recovers check_window rounds after the alert.
func TestUndecidedRoundsBetweenMissesDelayTheRecoveryOnlyByThemselves(t *testing.T) {
	const check = 11
	plan := undecidedPlan(t, check, "memory")
	// Every other round undecided: the walk steps back 2*check-1 positions to
	// count check misses, which the Level retains here. Retaining fewer, it
	// waits for the data to improve (the next test).
	if retained := plan.Levels().At(0).StateRequirement().RetentionPoints; retained < 2*check-1 {
		t.Fatalf("the Level retains %d positions, the walk needs %d", retained, 2*check-1)
	}
	rounds := sequence(2*check-1, roundMiss, roundUndecided)
	if multi := recoveredAt(t, plan, rounds); multi != 2*check-1 {
		t.Fatalf("the item over two inputs recovered at round %d, want %d: its %dth decided miss", multi, 2*check-1, check)
	}
	if single := recoveredAt(t, undecidedPlan(t, check), asHoles(rounds)); single != check {
		t.Fatalf("one query recovered at round %d, want %d", single, check)
	}
}

// Past what the Level retains, undecided rounds hold the recovery until the
// data improves - never a recovery on rounds nobody could judge: every other
// round undecided and three misses needed of three retained positions, no
// recovery comes while they alternate, and three misses in a row recover.
func TestUndecidedRoundsPastTheRetainedWindowHoldTheRecoveryUntilTheDataImproves(t *testing.T) {
	rounds := append(sequence(8, roundMiss, roundUndecided), sequence(3, roundMiss)...)
	if multi := recoveredAt(t, undecidedPlan(t, 3, "memory"), rounds); multi != 11 {
		t.Fatalf("recovered at round %d, want 11: the third of the misses in a row", multi)
	}
}

// A series no input holds is not evaluated, as a series of one query that
// stops appearing is not: its rounds are holes, not undecided rounds, and it
// recovers on the round it is back with a miss, the same round the one query
// does (015 F1 section 4: all inputs absent follow the single query's path).
func TestASeriesNoInputHoldsRecoversAsASingleQueryDoes(t *testing.T) {
	rounds := append([]map[string]string{nil, nil}, roundMiss)
	multi := recoveredAt(t, undecidedPlan(t, 3, "memory"), rounds)
	single := recoveredAt(t, undecidedPlan(t, 3), rounds)
	if multi != 3 || single != 3 {
		t.Fatalf("recovered at round %d over two inputs and %d over one, want both at 3", multi, single)
	}
}

// degradedRound evaluates one round of an item detected over several inputs,
// the inputs holding the values given and each binding then as edit leaves it,
// requires the result contract to take the round, and returns the Level's
// outcome with the reason of the Level guard the worker proposes for the
// round's inputs ("" for none): the evaluation advances markers already
// stored, and the worker proposes a round's own (RoundGuardReasonForLevel).
func degradedRound(t *testing.T, plan *strategy.CompiledPlan, values map[string]string, edit func(*execution.NamedInputBinding)) (execution.LevelOutcome, execution.ReasonCode, []observability.MultiDetectUndecidedReason) {
	t.Helper()
	const at = 660
	records := map[string][]contract.CanonicalRecordV2{}
	for name, value := range values {
		records[name] = []contract.CanonicalRecordV2{g4Record(at-60, value, nil)}
	}
	request := requestFixtureAt(t, plan, at, []contract.CanonicalRecordV2{g4Record(at-60, "0", nil)}, nil)
	input := undecidedInput(t, request, records)
	for index := range input.Inputs {
		edit(&input.Inputs[index])
	}
	request.Inputs = []execution.SeriesEvaluationInputRequest{input}
	var undecided []observability.MultiDetectUndecidedReason
	evaluator := newEvaluator(t).WithMultiDetectUndecided(func(reason observability.MultiDetectUndecidedReason) { undecided = append(undecided, reason) })
	result, err := evaluator.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(request); err != nil {
		t.Fatalf("the round could not finish: %v", err)
	}
	outcomes := result.Plans[0].LevelOutcomes
	if len(outcomes) != 1 || outcomes[0].Outcome != execution.LevelOutcomeUnknown {
		t.Fatalf("outcomes %+v, want the Level undecided", outcomes)
	}
	marker, _ := execution.RoundGuardReasonForLevel(input.Inputs, request.Header.DuePlans[0].Identity, 5)
	return outcomes[0], marker, undecided
}

// unavailable is a binding that did not complete, for reason.
func unavailable(binding *execution.NamedInputBinding, reason string) {
	binding.Dataset, binding.View = nil, nil
	binding.Completeness, binding.DataState = execution.CompletenessUnavailable, execution.DataStateUnknown
	binding.Disposition, binding.ReasonCode, binding.ImpactScope = execution.AccessUnavailable, execution.ReasonCode(reason), execution.ImpactPlan
}

// An undecided round of an item detected over several inputs says what the
// guard it raises says (015 F1): two inputs not read
// for different reasons fold as the guard folds them - QUERY_UNAVAILABLE over
// QUERY_TIMEOUT - not the first in algorithm order; and an input back PARTIAL
// at a time only it holds - the PRIMARY lacking the point, which alone would
// be MULTI_DETECT_INPUT_MISSING - says QUERY_PARTIAL, the guard's. A round
// with no input incomplete raises no guard and names the missing point.
func TestAnUndecidedRoundSaysWhatTheGuardItRaisesSays(t *testing.T) {
	plan := undecidedPlan(t, 3, "memory", "disk")
	outcome, marker, undecided := degradedRound(t, plan, map[string]string{"primary": "95"}, func(binding *execution.NamedInputBinding) {
		switch binding.DatasetName {
		case "input:memory":
			unavailable(binding, contract.ReasonQueryTimeout)
		case "input:disk":
			unavailable(binding, contract.ReasonQueryUnavailable)
		}
	})
	if marker != execution.ReasonCode(contract.ReasonQueryUnavailable) || outcome.ReasonCode != marker {
		t.Fatalf("two inputs not read: outcome %s, guard %s, want both %s", outcome.ReasonCode, marker, contract.ReasonQueryUnavailable)
	}
	wantUndecided(t, "two inputs not read", undecided, observability.MultiDetectUndecidedInputError)

	outcome, marker, undecided = degradedRound(t, undecidedPlan(t, 3, "memory"), map[string]string{"input:memory": "86"}, func(binding *execution.NamedInputBinding) {
		if binding.DatasetName == "input:memory" {
			binding.Completeness, binding.Disposition = execution.CompletenessPartial, execution.AccessDegraded
			binding.ReasonCode = execution.ReasonCode(contract.ReasonQueryPartial)
		}
	})
	if marker != execution.ReasonCode(contract.ReasonQueryPartial) || outcome.ReasonCode != marker {
		t.Fatalf("an input back PARTIAL: outcome %s, guard %s, want both %s", outcome.ReasonCode, marker, contract.ReasonQueryPartial)
	}
	// PARTIAL is the round's reason, the PRIMARY's missing point beside it.
	wantUndecided(t, "an input back PARTIAL", undecided, observability.MultiDetectUndecidedInputPartial)

	outcome, marker, undecided = degradedRound(t, undecidedPlan(t, 3, "memory"), roundUndecided, func(*execution.NamedInputBinding) {})
	if marker != "" || outcome.ReasonCode != execution.ReasonCode(contract.ReasonMultiDetectInputMissing) {
		t.Fatalf("an input holding nothing of the series: outcome %s, guard %q, want %s and no guard", outcome.ReasonCode, marker, contract.ReasonMultiDetectInputMissing)
	}
	wantUndecided(t, "an input holding nothing of the series", undecided, observability.MultiDetectUndecidedInputMissing)

	// A series only the other input holds: the PRIMARY binding is the FULL
	// empty one the worker gives it, and the round, undecided, still writes
	// its point - the Plan carries data through its other input.
	outcome, marker, undecided = degradedRound(t, undecidedPlan(t, 3, "memory"), map[string]string{"input:memory": "86"}, func(*execution.NamedInputBinding) {})
	if marker != "" || outcome.ReasonCode != execution.ReasonCode(contract.ReasonMultiDetectInputMissing) {
		t.Fatalf("a series only the other input holds: outcome %s, guard %q, want %s and no guard", outcome.ReasonCode, marker, contract.ReasonMultiDetectInputMissing)
	}
	wantUndecided(t, "a series only the other input holds", undecided, observability.MultiDetectUndecidedInputMissing)
}

// wantUndecided is the round heard undecided once, for reason.
func wantUndecided(t *testing.T, round string, got []observability.MultiDetectUndecidedReason, reason observability.MultiDetectUndecidedReason) {
	t.Helper()
	if len(got) != 1 || got[0] != reason {
		t.Fatalf("%s: heard undecided %v, want once for %s", round, got, reason)
	}
}

// The windows undecided rounds held are counted where they were held: a
// window ending on one at its end, a window ending on a decided round that
// holds one within. Two undecided rounds before a miss, on one position, hold
// the two windows ending on them; on two positions, an undecided round between
// two misses is held within by the second miss's window and at its end by its
// own, the walk stopping on the window that still holds the alert's anomaly;
// one query, the same rounds holes, holds nothing.
func TestTheRecoveryWindowsUndecidedRoundsHoldAreCountedWhereTheyWereHeld(t *testing.T) {
	for _, test := range []struct {
		name               string
		plan               *strategy.CompiledPlan
		rounds             []map[string]string
		wantEnd, wantInner uint32
	}{
		{name: "one position", plan: undecidedPlan(t, 3, "memory"), rounds: []map[string]string{roundUndecided, roundUndecided, roundMiss}, wantEnd: 2},
		{name: "two positions", plan: undecidedPlanWindow(t, 2, 2, "memory"), rounds: []map[string]string{roundMiss, roundUndecided, roundMiss}, wantEnd: 1, wantInner: 1},
		{name: "one query", plan: undecidedPlan(t, 3), rounds: []map[string]string{nil, nil, roundMiss}},
	} {
		run := newUndecidedRounds(t, test.plan)
		run.round(firedAt, roundHit)
		for index, values := range test.rounds {
			if values != nil {
				run.round(firedAt+int64(index+1)*60, values)
			}
		}
		if run.heldEnd != test.wantEnd || run.heldInside != test.wantInner {
			t.Errorf("%s: held %d at the end and %d within, want %d and %d", test.name,
				run.heldEnd, run.heldInside, test.wantEnd, test.wantInner)
		}
	}
}

// The events of an item detected over several inputs say each input (015 F1
// section 7): the detection snapshot is version 2, each input by its
// reference - read, its value as judged and the unit it is in, whether it met
// its threshold, its shown name - on the ABNORMAL and on the RECOVERY alike,
// and each algorithm a Threshold naming the input it judges; the event's
// values are each input's own, by reference, and none under "value".
func TestTheEventsOfAnItemDetectedOverSeveralInputsSayEachInput(t *testing.T) {
	plan := undecidedPlan(t, 1, "memory")
	run := newUndecidedRounds(t, plan)
	eventAt := func(at int64, values map[string]string) contract.TriggerEventV1 {
		t.Helper()
		source := at - 60
		records := map[string][]contract.CanonicalRecordV2{}
		for name, value := range values {
			records[name] = []contract.CanonicalRecordV2{g4Record(source, value, nil)}
		}
		request := requestFixtureAt(t, plan, at, records["primary"], nil)
		if run.view != nil {
			request.State.Items[0] = *run.view
		}
		request.Inputs = []execution.SeriesEvaluationInputRequest{undecidedInput(t, request, records)}
		result, err := run.evaluator.Evaluate(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Validate(request); err != nil {
			t.Fatalf("round at %d: %v", at, err)
		}
		states := result.Plans[0].StateResults
		if len(states) != 1 || len(states[0].Events) != 1 {
			t.Fatalf("round at %d: %+v, want one event", at, result.Plans[0])
		}
		view := carriedView(states[0].Mutation)
		run.view = &view
		return states[0].Events[0]
	}
	type input struct {
		State       string      `json:"state"`
		Unit        string      `json:"unit"`
		DisplayName string      `json:"display_name"`
		Value       json.Number `json:"value"`
		Hit         bool        `json:"hit"`
	}
	for _, test := range []struct {
		at     int64
		values map[string]string
		kind   string
		hits   map[string]bool
	}{
		{at: firedAt, values: roundHit, kind: contract.TriggerEventAbnormal, hits: map[string]bool{"cpu": true, "memory": true}},
		{at: firedAt + 60, values: map[string]string{"primary": "95", "input:memory": "50"}, kind: contract.TriggerEventRecovery,
			hits: map[string]bool{"cpu": true, "memory": false}},
	} {
		event := eventAt(test.at, test.values)
		if event.EventKind != test.kind {
			t.Fatalf("round at %d sent %s, want %s", test.at, event.EventKind, test.kind)
		}
		if _, found := event.Observed.Values["value"]; found || string(event.Observed.Values["cpu"]) != test.values["primary"] ||
			string(event.Observed.Values["memory"]) != test.values["input:memory"] {
			t.Fatalf("%s values %v, want each input's own by reference and no value", test.kind, event.Observed.Values)
		}
		var snapshot struct {
			Version int              `json:"version"`
			Inputs  map[string]input `json:"inputs"`
			Levels  []struct {
				Algorithms []struct {
					Type     string `json:"type"`
					InputRef string `json:"input_ref"`
				} `json:"algorithms"`
			} `json:"levels"`
		}
		if err := json.Unmarshal(event.DetectionSnapshot, &snapshot); err != nil {
			t.Fatalf("%s snapshot %s: %v", test.kind, event.DetectionSnapshot, err)
		}
		if snapshot.Version != contract.DetectionSnapshotVersionMultiDetect || len(snapshot.Inputs) != 2 {
			t.Fatalf("%s snapshot %s, want version 2 with both inputs", test.kind, event.DetectionSnapshot)
		}
		for ref, name := range map[string]string{"cpu": "CPU", "memory": "memory"} {
			got := snapshot.Inputs[ref]
			want := test.values[map[string]string{"cpu": "primary", "memory": "input:memory"}[ref]]
			// The value as judged: normalized to the threshold's precision.
			judged, _ := got.Value.Float64()
			read, _ := strconv.ParseFloat(want, 64)
			// In the platform's unit id a reader formats by, not the
			// normalizer's own scale (%).
			if got.State != "SUCCESS" || judged != read || got.Unit != "percent" || got.Hit != test.hits[ref] || got.DisplayName != name {
				t.Fatalf("%s input %s = %+v, want read at %s, in percent, hit %t, shown as %s", test.kind, ref, got, want, test.hits[ref], name)
			}
		}
		if len(snapshot.Levels) != 1 || len(snapshot.Levels[0].Algorithms) != 2 ||
			snapshot.Levels[0].Algorithms[0].Type != "Threshold" || snapshot.Levels[0].Algorithms[0].InputRef != "cpu" ||
			snapshot.Levels[0].Algorithms[1].InputRef != "memory" {
			t.Fatalf("%s levels %+v, want one of a Threshold for each input, in order", test.kind, snapshot.Levels)
		}
	}
}
