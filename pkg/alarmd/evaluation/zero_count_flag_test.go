// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/trigger"
)

// A zero is no observation for the plan's own primary when it reads event
// counts, or log counts with no grouping; a grouped log count, a time series,
// and another plan's primary leave it an observation.
func TestAZeroCountIsNoObservationOnlyForTheseSources(t *testing.T) {
	plan := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	other := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "8"}
	primary := func(consumer execution.PlanIdentity, events, unfilled bool) execution.DataRequirement {
		return execution.DataRequirement{Role: execution.InputRolePrimary, EventCounts: events, UnfilledCounts: unfilled,
			Consumers: []execution.DataRequirementConsumer{{Consumer: execution.ConsumerRef{Plan: consumer}}}}
	}
	ungrouped, grouped := zeroCountRecord(t, map[string]json.RawMessage{}), zeroCountRecord(t, map[string]json.RawMessage{"host": json.RawMessage(`"host-a"`)})
	for _, test := range []struct {
		name         string
		requirements []execution.DataRequirement
		record       execution.RecordView
		want         bool
	}{
		{"event count", []execution.DataRequirement{primary(plan, true, false)}, grouped, true},
		{"ungrouped log count", []execution.DataRequirement{primary(plan, false, true)}, ungrouped, true},
		{"grouped log count", []execution.DataRequirement{primary(plan, false, true)}, grouped, false},
		{"time series", []execution.DataRequirement{primary(plan, false, false)}, ungrouped, false},
		{"another plan's event count", []execution.DataRequirement{primary(other, true, false), primary(plan, false, false)}, ungrouped, false},
		{"an event count dependency", []execution.DataRequirement{{Role: execution.InputRoleAlgorithmDependency, EventCounts: true,
			Consumers: []execution.DataRequirementConsumer{{Consumer: execution.ConsumerRef{Plan: plan}}}}}, ungrouped, false},
	} {
		if got := zeroCountIsNoObservation(test.requirements, plan, test.record); got != test.want {
			t.Errorf("%s: zeroCountIsNoObservation = %t, want %t", test.name, got, test.want)
		}
	}
}

func zeroCountRecord(t *testing.T, dimensions map[string]json.RawMessage) execution.RecordView {
	t.Helper()
	dataset := execution.NewDataset([]contract.CanonicalRecordV2{{
		RecordID: strings.Repeat("b", 64), SourceTime: 1_788_000_000, BusinessID: "2",
		DimensionIdentity: contract.DimensionIdentityV2{Digest: strings.Repeat("c", 64)},
		Values:            map[string]json.RawMessage{"value": json.RawMessage(`0`)}, Dimensions: dimensions,
		ReceivedTime: 1_788_000_000,
	}})
	view, err := execution.NewDatasetView(dataset, []uint32{0})
	if err != nil {
		t.Fatal(err)
	}
	record, ok := view.Record(0)
	if !ok {
		t.Fatal("no record")
	}
	return record
}

// A Level outcome is withheld on a zero when its decision window says so;
// one withheld for nothing and one with no window are not.
func TestWithheldOnZeroCountReadsTheDecisionWindow(t *testing.T) {
	window := func(withheld string) *contract.DecisionWindowV1 {
		return &contract.DecisionWindowV1{Recovery: contract.RecoveryWindowEvidenceV1{Withheld: withheld}}
	}
	for _, test := range []struct {
		outcome trigger.LevelOutcomeV2
		want    bool
	}{
		{trigger.LevelOutcomeV2{DecisionWindow: window(contract.RecoveryWithheldZeroCount)}, true},
		{trigger.LevelOutcomeV2{DecisionWindow: window("")}, false},
		{trigger.LevelOutcomeV2{}, false},
	} {
		if got := withheldOnZeroCount(test.outcome); got != test.want {
			t.Errorf("withheldOnZeroCount(%+v) = %t, want %t", test.outcome.DecisionWindow, got, test.want)
		}
	}
}

// Through the evaluator: a Plan whose primary reads event counts, whose two
// Levels triggered on the round before, reads a zero. Each Level met its
// recovery window and stays NORMAL, counted as withheld, and no envelope
// goes; the result contract accepts it. Without the word the same zero
// recovers both and counts nothing.
func TestTheEvaluatorWithholdsAndCountsARecoveryOnAZeroEventCount(t *testing.T) {
	for _, arm := range []struct {
		name         string
		eventCounts  bool
		wantOutcome  execution.LevelOutcomeKind
		wantWithheld uint64
		wantEvents   int
	}{
		{"event count", true, execution.LevelOutcomeNormal, 2, 0},
		{"time series", false, execution.LevelOutcomeRecovery, 0, 1},
	} {
		t.Run(arm.name, func(t *testing.T) {
			plan := compiledTwoLevels(t)
			history := []execution.StateHistoryPoint{{RecordID: strings.Repeat("a", 64), SourceTime: 40, Levels: []execution.StateLevelFact{
				{LevelID: 5, DetectFingerprint: plan.Levels().At(0).Fingerprints().Detect, Result: execution.LevelFactAnomalous},
				{LevelID: 6, DetectFingerprint: plan.Levels().At(1).Fingerprints().Detect, Result: execution.LevelFactAnomalous},
			}}}
			req := requestFixtureTwoLevels(t, plan, json.RawMessage(`0`), history)
			req.Header.Requirements[0].EventCounts = arm.eventCounts
			result, err := newEvaluator(t).Evaluate(context.Background(), req)
			if err != nil {
				t.Fatalf("Evaluate()=%v", err)
			}
			plan0 := result.Plans[0]
			for _, outcome := range plan0.LevelOutcomes {
				if outcome.Outcome != arm.wantOutcome {
					t.Fatalf("outcomes=%+v want every Level %s", plan0.LevelOutcomes, arm.wantOutcome)
				}
			}
			var withheld uint64
			for _, outcome := range plan0.LevelOutcomes {
				if outcome.RecoveryWithheld {
					withheld++
				}
			}
			if withheld != arm.wantWithheld {
				t.Fatalf("withheld=%d want %d", withheld, arm.wantWithheld)
			}
			if len(plan0.StateResults) != 1 || len(plan0.StateResults[0].Events) != arm.wantEvents {
				t.Fatalf("state results=%+v want one with %d envelopes", plan0.StateResults, arm.wantEvents)
			}
			if err := result.Validate(req); err != nil {
				t.Fatalf("the result contract refused the evaluator's own result: %v", err)
			}
		})
	}
}
