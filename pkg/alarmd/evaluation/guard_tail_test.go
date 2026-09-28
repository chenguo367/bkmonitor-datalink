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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// An UNKNOWN under a standing guard is marked as the guard's alone only when
// this round proposes no guard of its own for the Level.
//
// The side that needs the mark is a dependency that answered FULL with no
// rows. PlanInputsWhole counts it whole - it answered, and on a round with no
// series that is all a warmup asks - but for a series that needed it the round
// proposes its own QUERY_EMPTY guard, and the outcome carries that reason.
// Such an UNKNOWN is this round's, not the tail of an earlier gap, so its
// Slot must not read GAP_GUARD_WARMING; only the evaluator's mark says so,
// since the inputs alone look whole.
func TestAnUnknownIsMarkedAsTheGuardsAloneOnlyWhenTheRoundProposesNoGuardOfItsOwn(t *testing.T) {
	plan := compiledG4Plan(t, strategy.DetectorKindSimpleRingRatio, map[string]any{"floor": 20, "ceil": nil},
		strategy.AlgorithmInputProjection{ValueFields: []string{"value"}, IdentityFields: []string{"host"}})
	guard := execution.ReasonCode(contract.ReasonQueryUnavailable)
	for _, testCase := range []struct {
		name     string
		previous []contract.CanonicalRecordV2
		reason   execution.ReasonCode
		tail     bool
	}{
		// The dependency answered with rows, none at the offset this record
		// needs: nothing incomplete, so the round proposes no guard, and the
		// UNKNOWN the missing point makes carries the standing guard's reason.
		{name: "dependency answered without the point", previous: []contract.CanonicalRecordV2{g4OffsetMissRecord(39, `100`)},
			reason: guard, tail: true},
		{name: "dependency answered empty", previous: []contract.CanonicalRecordV2{},
			reason: execution.ReasonCode(contract.ReasonQueryEmpty), tail: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			record := g4Record(99, `80`, nil)
			request := requestFixtureForPlan(t, plan, []contract.CanonicalRecordV2{record}, nil)
			request.State.Items[0].Status = execution.StateFoundWarming
			request.State.Items[0].Levels[0].HistoryCompleteness = execution.HistoryWarming
			request.State.Items[0].Levels[0].GapReasonCode = guard
			input := g4Input(t, request, map[string][]contract.CanonicalRecordV2{"primary": {record}, "previous": testCase.previous})
			request.Inputs = []execution.SeriesEvaluationInputRequest{input}
			if !execution.PlanInputsWhole(input.Inputs, request.Header.DuePlans[0].Identity) {
				t.Fatal("fixture: every input must answer FULL and available, so only the mark can tell the sides apart")
			}

			result, err := newEvaluator(t).Evaluate(context.Background(), request)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			outcomes := result.Plans[0].LevelOutcomes
			if len(outcomes) != 1 || outcomes[0].Outcome != execution.LevelOutcomeUnknown || outcomes[0].ReasonCode != testCase.reason {
				t.Fatalf("outcomes = %+v, want one UNKNOWN %s", outcomes, testCase.reason)
			}
			if outcomes[0].GuardTail != testCase.tail {
				t.Fatalf("GuardTail = %t, want %t", outcomes[0].GuardTail, testCase.tail)
			}
			if got := execution.NewSlotInputWholeness(input.Inputs).UnknownIsGuardTail(outcomes[0]); got != testCase.tail {
				t.Fatalf("UnknownIsGuardTail() = %t, want %t", got, testCase.tail)
			}
		})
	}
}
