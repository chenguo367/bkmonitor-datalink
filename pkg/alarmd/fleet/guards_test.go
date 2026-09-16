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
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// guardProgress is the observation the worker emits for every held gap
// scope on every round that reads it: the scope, its status and reason, and
// where its count stands, with the Plan on the trace and the object on the
// context.
func guardProgress(ctx context.Context, tracker *Tracker, strategy, scope, status, reason string, required, observed uint32) {
	tracker.Observe(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageGapGuardProgress, Result: observability.ResultSuccess,
		Direction:   observability.DirectionInternal,
		Trace:       observability.TraceFields{StrategyID: strategy, BusinessID: "2"},
		GapProgress: &observability.GapProgressFacts{Scope: scope, Status: status, Reason: reason, Required: required, Observed: observed},
	})
}

// anyColumn is the object's row wherever its column put it: a warming
// object with nothing else wrong is undecidable, one that has failed since
// is an anomaly, and these tests are about the row, not the column.
func anyColumn(tracker *Tracker) []Anomaly {
	rows := tracker.Anomalies()
	rows = append(rows, tracker.Undecidable()...)
	rows = append(rows, tracker.ByDesign()...)
	return rows
}

// degradedRound completes a round whose windows are held short by a gap
// guard: the Level reports the guard's trigger as its reason, and the
// counts stay live -- the shape that sits on the undecided-window line
// while the guard holds.
func degradedRound(ctx context.Context, tracker *Tracker, slot int64) {
	tracker.Observe(ctx, observability.Observation{ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE", ProgressCompletionCause: "LEVEL_OUTCOME_UNKNOWN", ProgressCompletionReason: "GAP_SKIPPED",
		HistoryCoverage: &observability.HistoryCoverageFacts{Levels: 2, Short: 2, WorstValid: 1, WorstRequired: 9, Guarded: 2},
		Trace:           observability.TraceFields{EvaluationTime: slot}})
}

// The held scopes ride on the row as the round reported them: a warming
// scope with its k/N, a gapped one with no count to show and how long it
// has held without moving; the worst first, gapped before warming, and no
// more than the bound with the total beside them.
func TestHeldGapScopesRideOnTheRowWorstFirst(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-guarded"})
	for round := 0; round < DefaultDegradedRounds; round++ {
		slot := int64(1000 + 60*round)
		guardProgress(ctx, tracker, "s-1", "plan", "WARMING", "GAP_SKIPPED", 9, uint32(1+round))
		guardProgress(ctx, tracker, "s-1", "3", "GAPPED", "QUERY_UNAVAILABLE", 9, 0)
		guardProgress(ctx, tracker, "s-2", "plan", "WARMING", "CONFIG_DRIFT", 5, 4)
		guardProgress(ctx, tracker, "s-2", "7", "WARMING", "CONFIG_DRIFT", 5, 2)
		guardProgress(ctx, tracker, "s-3", "plan", "WARMING", "GAP_SKIPPED", 4, 3)
		degradedRound(ctx, tracker, slot)
		at.at = at.at.Add(time.Minute)
	}
	rows := anyColumn(tracker)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want the one object", rows)
	}
	row := rows[0]
	if row.GuardsTotal != 5 || len(row.Guards) != MaxGuardsPerRow {
		t.Fatalf("guards = %d shown of %d, want %d of 5", len(row.Guards), row.GuardsTotal, MaxGuardsPerRow)
	}
	gapped := row.Guards[0]
	if gapped.Status != "GAPPED" || gapped.Scope != "3" || gapped.Reason != "QUERY_UNAVAILABLE" || gapped.Progress != GapProgressNone || gapped.UnchangedRounds != 2 || gapped.Rounds != 3 {
		t.Fatalf("first guard = %+v, want the gapped scope first, at no progress for two rounds after the first", gapped)
	}
	warming := row.Guards[1]
	if warming.Plan.StrategyID != "s-1" || warming.Scope != "plan" || warming.Observed != 3 || warming.Required != 9 || warming.Progress != GapProgressPartial || warming.UnchangedRounds != 0 {
		t.Fatalf("second guard = %+v, want the least advanced warming scope, moving", warming)
	}
	if row.Guards[2].Plan.StrategyID != "s-2" || row.Guards[2].Scope != "7" || row.Guards[3].Plan.StrategyID != "s-3" {
		t.Fatalf("guards = %+v, want the rest by how far their count has got", row.Guards)
	}
	if !gapped.FirstAt.Equal(now) || !gapped.LastAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("guard clocks = %v..%v, want first and latest report", gapped.FirstAt, gapped.LastAt)
	}
}

// A scope the round did not report was released: it leaves the row at that
// round's completion, and the ones still reported stay.
func TestAScopeTheRoundDidNotReportIsReleased(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-guarded"})
	for round := 0; round < DefaultDegradedRounds; round++ {
		guardProgress(ctx, tracker, "s-1", "plan", "WARMING", "GAP_SKIPPED", 9, uint32(1+round))
		guardProgress(ctx, tracker, "s-1", "3", "WARMING", "GAP_SKIPPED", 9, uint32(7+round))
		degradedRound(ctx, tracker, int64(1000+60*round))
		at.at = at.at.Add(time.Minute)
	}
	if rows := anyColumn(tracker); len(rows) != 1 || rows[0].GuardsTotal != 2 {
		t.Fatalf("rows = %+v, want both scopes held", rows)
	}
	// Level 3 filled and its guard released: the next round reports only
	// the plan scope.
	guardProgress(ctx, tracker, "s-1", "plan", "WARMING", "GAP_SKIPPED", 9, 4)
	degradedRound(ctx, tracker, 1180)
	rows := anyColumn(tracker)
	if len(rows) != 1 || rows[0].GuardsTotal != 1 || rows[0].Guards[0].Scope != "plan" || rows[0].Guards[0].Observed != 4 || rows[0].Guards[0].Rounds != 4 {
		t.Fatalf("rows = %+v / %+v, want only the plan scope, still counted from its first report", rows, rows[0].Guards)
	}
	// Reported again before the round completes: still on the row.
	guardProgress(ctx, tracker, "s-1", "plan", "WARMING", "GAP_SKIPPED", 9, 5)
	if rows := anyColumn(tracker); len(rows) != 1 || rows[0].GuardsTotal != 1 || rows[0].Guards[0].Observed != 5 {
		t.Fatalf("rows = %+v, want the scope updated mid-round", rows)
	}
}

// The window counts and the guard describe the last round that completed.
// An object whose last completion was warming and whose latest round failed
// is read as a failed round -- its own code if named, unclassified if not
// -- and goes back to the window line when a round completes again.
func TestAFailedRoundOutranksTheLastCompletionsWindow(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: "qg-window"})
	for round := 0; round < DefaultDegradedRounds; round++ {
		degradedRound(ctx, tracker, int64(1000+60*round))
		at.at = at.at.Add(time.Minute)
	}
	rows := anyColumn(tracker)
	Attribute(rows, at.at)
	if len(rows) != 1 || rows[0].Finding.Check != CheckWindowUndecided {
		t.Fatalf("rows = %+v, want the object on the window line while its rounds complete degraded", rows)
	}
	tracker.Observe(ctx, observability.Observation{ExecuteOutcome: "error", ReasonCode: "internal_unknown",
		Err:   errors.New("alarmd worker: commit: redis: connection pool timeout"),
		Trace: observability.TraceFields{EvaluationTime: 1180}})
	rows = anyColumn(tracker)
	Attribute(rows, at.at)
	if len(rows) != 1 || rows[0].Finding.Check != CheckDefect || rows[0].Blocked == nil || rows[0].Blocked.Dependency != DependencyRedis {
		t.Fatalf("rows after a failed round = %+v / %+v, want the failed round read by its own facts, not the window", rows[0].Finding, rows[0].Blocked)
	}
	at.at = at.at.Add(time.Minute)
	degradedRound(ctx, tracker, 1180)
	rows = anyColumn(tracker)
	Attribute(rows, at.at)
	if len(rows) != 1 || rows[0].Finding.Check != CheckWindowUndecided {
		t.Fatalf("rows after the round completed again = %+v, want the window line again", rows[0].Finding)
	}
}
