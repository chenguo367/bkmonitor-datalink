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
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// supplementOf is what the executor reports for a supplement of an earlier
// Slot: the Slot completion stage, the supplement's operation, and the
// execute outcome every supplement carries, since it completes no Slot.
func supplementOf(slot int64, operation observability.Operation) observability.Observation {
	return observability.Observation{Component: observability.ComponentScheduler, Stage: observability.StageSlotCompleted,
		Operation: operation, ExecuteOutcome: "incomplete", Result: observability.ResultSuccess, ReasonCode: observability.ReasonNone,
		Trace: observability.TraceFields{StrategyID: "4101", BusinessID: "7", EvaluationTime: slot}}
}

// An object whose rounds end degraded, supplemented between them, keeps the
// line its rounds put it on and its latest round: a supplement is not a
// round. The same observation with any other operation is a round that
// reached execution and did not finish, which is what put supplemented
// objects on DEFECT -- the second case shows the fixture reaches it.
func TestASupplementIsNotARoundOfTheObject(t *testing.T) {
	for name, tc := range map[string]struct {
		operation observability.Operation
		defect    bool
	}{
		"supplement":                       {observability.OperationSupplement, false},
		"the same outcome of a real round": {observability.OperationNormal, true},
	} {
		tracker := newTracker(t, &clock{at: now})
		ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg"})
		const period, first = int64(60), int64(60_000)
		last := int64(0)
		for index := int64(0); index < int64(DefaultDegradedRounds)+2; index++ {
			last = first + index*period
			round(ctx, tracker, last, "COMPLETED_WITH_UNAVAILABLE", "LEVEL_OUTCOME_UNKNOWN", "HISTORY_GAPPED", primary("FULL", "DATA"), nil)
			tracker.Observe(ctx, supplementOf(last-2*period, tc.operation))
		}
		rows := tracker.Anomalies()
		if len(rows) != 1 {
			t.Fatalf("%s: rows %+v, want the object's one row", name, rows)
		}
		onDefect := rows[0].ReasonCode == "incomplete" || rows[0].Finding.Check == CheckDefect
		if onDefect != tc.defect {
			t.Errorf("%s: reason %s under %s, want DEFECT from the outcome %v", name, rows[0].ReasonCode, rows[0].Finding.Check, tc.defect)
		}
		if state := tracker.groups["qg"]; !tc.defect && state.lastRoundSlot != last {
			t.Errorf("%s: latest round %d, want the latest round's Slot %d, not the supplemented one", name, state.lastRoundSlot, last)
		}
	}
}
