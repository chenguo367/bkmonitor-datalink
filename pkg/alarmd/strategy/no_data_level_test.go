// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package strategy

import (
	"context"
	"math/big"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

func noDataPlan(t *testing.T, config *contract.NoDataConfigV1) *CompiledPlan {
	t.Helper()
	plan := validPlan()
	plan.NoData = config
	result, err := newTestCompiler(t).Compile(context.Background(), validRequest(plan))
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	compiled, ok := result.Plan()
	if !ok {
		t.Fatalf("Compile() plan terminal = %+v, level terminals = %+v", result.PlanTerminal(), result.LevelTerminals())
	}
	return compiled
}

// Adding no-data detection to a strategy must not move anything the runtime
// state is keyed by.
//
// This is the reason the no-data level is compiled beside Levels rather than
// into StrategyIR.Levels. The plan fingerprints and the state compatibility
// hash are derived from the declared levels; a level added there would change
// both, and a changed state compatibility hash re-keys the runtime state of
// every Plan in the deployment on the rollout that introduced it. Every window,
// every trigger history, every open alert's state would start again from empty
// - for every strategy, not only the ones that gained no-data - and the symptom
// would be a fleet-wide quiet period that looks like the feature working.
func TestNoDataDetectionDoesNotMoveThePlanFingerprints(t *testing.T) {
	without := noDataPlan(t, nil)
	with := noDataPlan(t, &contract.NoDataConfigV1{Continuous: 3, Level: 2})

	if without.NoDataLevel() != nil {
		t.Fatal("a plan without a no_data section compiled a no-data level")
	}
	if with.NoDataLevel() == nil {
		t.Fatal("a plan with a no_data section compiled no no-data level")
	}
	if with.Fingerprints() != without.Fingerprints() {
		t.Fatalf("plan fingerprints moved: %+v with no-data, %+v without", with.Fingerprints(), without.Fingerprints())
	}
	if with.PlanRef().StateCompatibilityHash != without.PlanRef().StateCompatibilityHash {
		t.Fatalf("state compatibility hash moved: %q with no-data, %q without; "+
			"this would re-key every Plan's runtime state in the deployment",
			with.PlanRef().StateCompatibilityHash, without.PlanRef().StateCompatibilityHash)
	}
	if len(with.Levels()) != len(without.Levels()) {
		t.Fatalf("Levels() = %d with no-data and %d without; the no-data level is not a declared level",
			len(with.Levels()), len(without.Levels()))
	}
}

// Every number in the no-data level comes from the configuration. The window
// and the required count are both Continuous because the backend sets
// check_window_size and trigger_count from that one field, and the step is the
// Plan's evaluation interval because the trigger compiler requires it to be.
func TestNoDataLevelTriggerIsDerivedFromTheConfiguration(t *testing.T) {
	for _, continuous := range []uint32{1, 3, 10} {
		compiled := noDataPlan(t, &contract.NoDataConfigV1{Continuous: continuous, Level: 2})
		level := compiled.NoDataLevel()
		trigger := level.Trigger()
		if trigger.WindowSize != continuous || trigger.RequiredAnomalies != continuous {
			t.Fatalf("continuous %d gave window %d and required %d, want both %d",
				continuous, trigger.WindowSize, trigger.RequiredAnomalies, continuous)
		}
		if trigger.StepSeconds != validPlan().StrategyIR.ExecutionSemantics.EvaluationInterval {
			t.Fatalf("step = %d, want the Plan's evaluation interval %d",
				trigger.StepSeconds, validPlan().StrategyIR.ExecutionSemantics.EvaluationInterval)
		}
		// One window of misses closes the alert: a round that judged and found
		// the group present is the whole evidence of recovery.
		if recovery := level.Recovery(); !recovery.Enabled || recovery.ConsecutiveWindows != 1 {
			t.Fatalf("recovery = %+v, want enabled with one window", recovery)
		}
	}
}

// The level ID is the configured severity, which shares its number space with
// the declared levels. That is not a collision: the synthetic series have their
// own identity, so their runtime state is a different key, and the level number
// is what the event carries as its severity.
func TestNoDataLevelCarriesTheConfiguredSeverity(t *testing.T) {
	for _, severity := range []uint32{1, 2, 3} {
		compiled := noDataPlan(t, &contract.NoDataConfigV1{Continuous: 2, Level: severity})
		if got := compiled.NoDataLevel().Definition().LevelID; got != severity {
			t.Fatalf("no-data level ID = %d, want the configured severity %d", got, severity)
		}
	}
}

// The detector reads the synthetic value and says nothing about the item's own
// measurements. A detector declaring one of the item's real value fields would
// be readable, fingerprintable and wrong in a way nothing downstream reports.
func TestNoDataLevelDetectorReadsTheSyntheticValue(t *testing.T) {
	compiled := noDataPlan(t, &contract.NoDataConfigV1{Continuous: 2, Level: 2, AggDimension: []string{"host"}})
	detectors := compiled.NoDataLevel().Detectors()
	if len(detectors) != 1 {
		t.Fatalf("detectors = %d, want one threshold", len(detectors))
	}
	if got := detectors[0].ValueRef(); got != NoDataValueField {
		t.Fatalf("detector reads %q, want %q", got, NoDataValueField)
	}
	if got := detectors[0].Kind(); got != DetectorKindThreshold {
		t.Fatalf("detector kind = %q, want %q", got, DetectorKindThreshold)
	}

	// What it decides, not how it is configured. Asserting the operator and the
	// threshold separately leaves the pair unchecked: a threshold of zero with
	// the same GTE reads as a perfectly ordinary configuration and makes every
	// present group an anomaly, so a no-data alert would open on the first round
	// and never recover, for every strategy that detects no-data. A mutation run
	// found exactly that, because nothing here had asked the predicate anything.
	predicate := detectors[0].Predicate()
	for _, decision := range []struct {
		value  int64
		anomal bool
		what   string
	}{
		{value: 1, anomal: true, what: "an absent group"},
		{value: 0, anomal: false, what: "a present group"},
	} {
		normalized, ok := normalizeRational(big.NewRat(decision.value, 1), 1)
		if !ok {
			t.Fatalf("fixture: %d did not normalize", decision.value)
		}
		evaluation, err := predicate.Evaluate(normalized)
		if err != nil {
			t.Fatal(err)
		}
		if evaluation.Matched() != decision.anomal {
			t.Fatalf("%s (value %d) matched = %t, want %t",
				decision.what, decision.value, evaluation.Matched(), decision.anomal)
		}
	}

	// And the values reach the predicate unchanged: an absence answer has no
	// unit, so a normalizer that scaled it would be converting a count of
	// nothing into a different count of nothing.
	normalizer, ok := compiled.normalizers[detectors[0].normalizerRef]
	if !ok {
		t.Fatal("the no-data detector's normalizer is not registered on the Plan")
	}
	if normalizer.sourceMultiplier != 1 {
		t.Fatalf("normalizer multiplier = %d, want 1: the synthetic value is unitless",
			normalizer.sourceMultiplier)
	}

	// The projection describes the series that actually arrive: the configured
	// dimensions plus the tag that keeps them apart from the item's real series.
	projection := NoDataProjection(&contract.NoDataConfigV1{Continuous: 2, Level: 2, AggDimension: []string{"host"}})
	if len(projection.ValueFields) != 1 || projection.ValueFields[0] != NoDataValueField {
		t.Fatalf("projection value fields = %v, want only %q", projection.ValueFields, NoDataValueField)
	}
	// Sorted, because the algorithm contract checks a projection states its
	// dimensions as a set. The tag sorts before "host", which is a fact about
	// the names and not about the order they were written in.
	if len(projection.DimensionFields) != 2 ||
		projection.DimensionFields[0] != contract.NoDataDimensionTag || projection.DimensionFields[1] != "host" {
		t.Fatalf("projection dimensions = %v, want the configured ones and the tag in sorted order",
			projection.DimensionFields)
	}
}

// An item whose no_data section is not valid is refused at the level rather
// than compiled into something that detects the wrong thing. Continuous has no
// default anywhere - supplying one would turn an item that reports nothing into
// one that alerts after that many periods.
func TestNoDataLevelRefusesAnInvalidConfiguration(t *testing.T) {
	semantics := validPlan().StrategyIR.ExecutionSemantics
	for name, config := range map[string]*contract.NoDataConfigV1{
		"no config":     nil,
		"no continuous": {Level: 2},
		"level zero":    {Continuous: 3},
		"level above 3": {Continuous: 3, Level: 4},
		"tag as a dimension": {
			Continuous: 3, Level: 2, AggDimension: []string{contract.NoDataDimensionTag},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildNoDataLevelIR(config, semantics); err == nil {
				t.Fatal("BuildNoDataLevelIR() accepted an invalid no_data configuration")
			}
		})
	}
}
