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
)

func readEarlyFacts(since time.Time) ReadEarlyFacts {
	return ReadEarlyFacts{StepSeconds: 60, CurrentDelaySeconds: 60, SuggestedDelaySeconds: 180, Since: since,
		Samples: []ReadEarlySample{{EvaluationTime: since.Unix(), CompletionAgeSeconds: 90, Rung: "x1.5",
			ChangedAgeSeconds: 90, Buckets: []int64{since.Unix() - 60}}}}
}

// An object the lookback reports as read early is a row under the
// strategies this process has seen evaluate on it, with the time_delay it
// runs under and the one that would have read it complete; one this process
// never saw evaluate has no strategy to be named under and is left out.
func TestAnObjectReadEarlyIsARowUnderItsStrategies(t *testing.T) {
	now := time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)
	tracker := newTracker(t, &clock{at: now})
	tracker.Observe(context.Background(), slotCompleted("qg-late", 0, 0, now))
	rows := rowsOfKind(tracker.ReadEarly(map[string]ReadEarlyFacts{"qg-late": readEarlyFacts(now), "qg-unseen": readEarlyFacts(now)}),
		KindReadBeforeComplete)
	if len(rows) != 1 {
		t.Fatalf("rows %+v, want the object seen evaluating only", rows)
	}
	row := rows["qg-late"]
	if row.ReadEarly == nil || row.ReadEarly.SuggestedDelaySeconds != 180 || row.ReadEarly.CurrentDelaySeconds != 60 ||
		len(row.Strategies) != 1 || row.Strategies[0].StrategyID != "4101" || !row.Since.Equal(now) {
		t.Fatalf("row %+v facts %+v", row, row.ReadEarly)
	}
}

// End to end from the replica's snapshot: the object lands on
// READ_BEFORE_COMPLETE as the strategy's to act on while its rounds
// complete, the words are "the results cannot be taken as they stand" and
// "the strategy's owner changes it", and a diagnosis of the strategy reads
// it there; the health response names the object with the suggested
// time_delay.
func TestAReadEarlyObjectReachesItsLineTheDiagnosisAndTheHealthResponse(t *testing.T) {
	now := time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)
	tracker := newTracker(t, &clock{at: now})
	tracker.Observe(context.Background(), slotCompleted("qg-late", 0, 0, now))
	snapshot := Snapshot{Replica: "pod-a", TakenAt: now, Owned: 1, Determined: 1, OwnedObjects: []string{"qg-late"},
		ReadEarly: tracker.ReadEarly(map[string]ReadEarlyFacts{"qg-late": readEarlyFacts(now)})}
	view := Aggregate(Expectation{Known: true, QueryGroups: 1, IDs: []string{"qg-late"}}, []Snapshot{snapshot}, []string{"pod-a"}, now, time.Minute)
	Decide(&view, now, 0)
	if len(view.ReadEarly) != 1 {
		t.Fatalf("view carries %d rows, want the snapshot's one", len(view.ReadEarly))
	}
	finding := view.ReadEarly[0].Finding
	if finding.Check != CheckReadBeforeComplete || finding.Owner != OwnerStrategy || finding.Result != ResultCompleted {
		t.Fatalf("finding %+v, want the strategy's line with its rounds completed", finding)
	}
	var line *CheckReport
	for _, report := range ReportChecks(nil, nil, &view, now) {
		if report.Code == CheckReadBeforeComplete {
			line = &report
			break
		}
	}
	if line == nil || line.Current != 1 {
		t.Fatalf("line %+v, want READ_BEFORE_COMPLETE with the one object", line)
	}
	if pair := checkWords[CheckReadBeforeComplete]; pair.State != StateResultUntrusted || pair.Action != ActionStrategyEdit {
		t.Fatalf("words %+v, want the results not taken as they stand, for the strategy's owner", pair)
	}
	facts := StrategyLookupFacts{Available: true, Found: true, Publication: StrategyPublication{SnapshotRevision: "s1", Epoch: 7},
		Plans:        []StrategyPlanRef{{Tenant: "default", Business: "2", QueryGroup: "qg-late", SnapshotRevision: "s1", QueryRevision: "q", ScheduleRevision: "r"}},
		Dispositions: []StrategyDisposition{{Scope: "PLAN", Disposition: "ACCEPTED"}}}
	row := diagnoseStrategy("4101", facts, newDiagnosisContext(&view, "pod-a", now))
	if row.Verdict != StateResultUntrusted || row.Action != ActionStrategyEdit || row.Check != CheckReadBeforeComplete {
		t.Fatalf("diagnosis %s/%s/%s, want RESULT_UNTRUSTED/STRATEGY_EDIT under READ_BEFORE_COMPLETE", row.Verdict, row.Action, row.Check)
	}

	snapshots := healthySnapshots()
	snapshots[0].ReadEarly = snapshot.ReadEarly
	handler := handlerWith(t, snapshots, Expectation{QueryGroups: 949, Known: true}, []string{"pod-a", "pod-b"})
	body := requestJSON(t, handler, "/api/health")
	list, ok := body["read_early"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("health read_early = %v, want the one object", body["read_early"])
	}
	entry, _ := list[0].(map[string]any)
	if entry["query_group"] != "qg-late" || entry["suggested_time_delay_seconds"] != float64(180) || entry["current_time_delay_seconds"] != float64(60) {
		t.Fatalf("health entry %v", entry)
	}
}
