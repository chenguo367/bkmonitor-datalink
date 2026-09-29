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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A row degraded by rounds that did not answer whole says where the latest
// round's cause was found - the strategy, the Level, the query - beside the
// cause and its reason; the run's end takes it off with them.
func TestADegradedRowNamesWhereItsCauseWasFound(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	for round := 0; round < DefaultDegradedRounds; round++ {
		tracker.Observe(context.Background(), observability.Observation{
			ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE", ProgressCompletionCause: "PRIMARY_INPUT_UNAVAILABLE",
			ProgressCompletionReason: "QUERY_UNAVAILABLE",
			ProgressCompletionScope: &observability.CompletionScopeFacts{StrategyID: "4101", BusinessID: "2", LevelID: 1, HasLevel: true,
				PhysicalQuery: "physical-query-1"},
			Trace: observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: int64(100 + 60*round)},
		})
		at.at = at.at.Add(time.Minute)
	}
	rows := tracker.Anomalies()
	if len(rows) != 1 || rows[0].Cause != "PRIMARY_INPUT_UNAVAILABLE" || rows[0].CauseScope == nil ||
		rows[0].CauseScope.StrategyID != "4101" || rows[0].CauseScope.LevelID == nil || *rows[0].CauseScope.LevelID != 1 ||
		rows[0].CauseScope.Query != "physical-query-1" {
		t.Fatalf("rows = %+v, want the cause with its strategy, Level and query", rows)
	}
	tracker.Observe(context.Background(), observability.Observation{ProgressCompletionKind: "FULL_COMPLETED",
		Trace: observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: int64(100 + 60*DefaultDegradedRounds)}})
	if rows := tracker.Anomalies(); len(rows) != 0 {
		t.Fatalf("rows after a whole round = %+v, want none", rows)
	}
	// A later run whose cause named no place does not show the last one's.
	for round := 1; round <= DefaultDegradedRounds; round++ {
		at.at = at.at.Add(time.Minute)
		tracker.Observe(context.Background(), observability.Observation{
			ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE", ProgressCompletionCause: "CONFIG_DRIFT",
			Trace: observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: int64(100 + 60*(DefaultDegradedRounds+round))},
		})
	}
	if rows := tracker.Anomalies(); len(rows) != 1 || rows[0].CauseScope != nil {
		t.Fatalf("rows = %+v, want the new run without the earlier run's place", rows)
	}
}
