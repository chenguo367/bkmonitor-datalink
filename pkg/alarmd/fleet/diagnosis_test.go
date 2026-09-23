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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// diagnosisFacts is the standing fixture's strategies, plus the ids a
// diagnosis must still answer: one the catalog never heard of.
func diagnosisFacts() map[string]StrategyLookupFacts {
	pub := StrategyPublication{SnapshotRevision: "s1", Epoch: 7}
	return map[string]StrategyLookupFacts{
		"4101": {Available: true, Found: true, Publication: pub, Plans: plans(planA, planB),
			Dispositions: []StrategyDisposition{{Scope: "PLAN", Disposition: "ACCEPTED"}}},
		"4102": {Available: true, Found: true, Publication: pub, Dispositions: []StrategyDisposition{
			{Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"}}},
		"4103": {Available: true, Found: true, Publication: pub, Plans: plans(planA), Dispositions: []StrategyDisposition{
			{Scope: "LEVEL", LevelID: 2, Disposition: "CONFIG_REJECTED", Reason: "LEVEL_INVALID", FieldPath: "items[0].algorithms[1].config"},
			{Scope: "LEVEL", LevelID: 1, Disposition: "ACCEPTED"}}},
		"4109": {Available: true, Found: true, Publication: pub, Dispositions: []StrategyDisposition{
			{Scope: "PLAN", Disposition: "SOURCE_INCOMPLETE", Reason: "EFFECTIVE_TIME_SNAPSHOT_UNAVAILABLE", FieldPath: "effective_time_snapshot"}}},
	}
}

type diagnosisRig struct {
	handler       http.Handler
	universe      []string
	universeErr   error
	universeReads int
	clock         time.Time
}

func newDiagnosisRig(t *testing.T, facts map[string]StrategyLookupFacts, progress ProgressReader) *diagnosisRig {
	t.Helper()
	rig := &diagnosisRig{clock: now}
	snapshots := healthySnapshots()
	snapshots[0].Owned, snapshots[0].Determined = 2, 2
	snapshots[0].OwnedObjects = []string{"qg-4101-a", "qg-other"}
	snapshots[1].Owned, snapshots[1].Determined = 1, 1
	snapshots[1].OwnedObjects = []string{"qg-4101-b"}
	row := anomaly("qg-4101-b")
	row.Replica = "pod-b"
	snapshots[1].Anomalies = []Anomaly{row}
	snapshots[1].TotalAnomalies = 1
	service := mustService(t, stubExpectations{expectation: Expectation{QueryGroups: 3, Known: true, IDs: []string{"qg-4101-a", "qg-4101-b", "qg-other"}}},
		stubRegistry{replicas: replicas()}, stubSnapshots{snapshots: snapshots})
	lookup := func(id string) StrategyLookupFacts {
		if f, ok := facts[id]; ok {
			return f
		}
		return StrategyLookupFacts{Available: true, Publication: StrategyPublication{SnapshotRevision: "s1", Epoch: 7}}
	}
	if facts == nil {
		lookup = func(string) StrategyLookupFacts { return StrategyLookupFacts{} }
	}
	universe := func(context.Context) ([]string, error) {
		rig.universeReads++
		return append([]string(nil), rig.universe...), rig.universeErr
	}
	rig.handler = WithDiagnosis(http.NotFoundHandler(), service, lookup, nil, universe, progress, "pod-a",
		func() time.Time { return rig.clock }, 0)
	return rig
}

func (rig *diagnosisRig) page(t *testing.T, cursor string, limit int) DiagnosisResponse {
	t.Helper()
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	w := httptest.NewRecorder()
	rig.handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/diagnose?"+q.Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var body DiagnosisResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &raw)
	if page, _ := raw["page"].(map[string]any); page != nil {
		body.Page.RowsWritten = int(page["rows"].(float64))
	}
	return body
}

// Every id of the source's set gets exactly one row with a word from the
// closed list, the withheld ones with whose they are to fix, and the page
// proves its own count: the ids the universe has in its range equal the
// rows it wrote. An id the catalog does not list is UNKNOWN with a reason,
// never counted as detecting.
func TestTheDiagnosisGivesEveryListedStrategyOneRowFromTheExistingWords(t *testing.T) {
	rig := newDiagnosisRig(t, diagnosisFacts(), nil)
	rig.universe = []string{"4109", "4101", "4102", "4103", "4105", "4101"}
	body := rig.page(t, "", 0)
	if body.Universe.Status != "ok" || body.Universe.Count != 5 || body.Universe.Digest == "" {
		t.Fatalf("universe = %+v, want five distinct ids", body.Universe)
	}
	if !body.Page.Holds || body.Page.IDsExpected != 5 || body.Page.RowsWritten != 5 || len(body.Strategies) != 5 || body.NextCursor != "" {
		t.Fatalf("page = %+v rows %d next %q, want one holding page of five", body.Page, len(body.Strategies), body.NextCursor)
	}
	want := map[string]struct {
		verdict     StateWord
		action      ActionWord
		reason      string
		attribution string
	}{
		"4101": {StateDefect, ActionServiceFix, "", ""},
		"4102": {StateNotDetecting, ActionServiceFix, "ALGORITHM_NOT_MIGRATED", "alarmd"},
		"4103": {StateDetecting, ActionNone, "", "strategy"},
		"4105": {DiagnosisUnknown, "", UnknownNotYetPublished, ""},
		"4109": {StateNotDetecting, ActionCacheWriterFill, "EFFECTIVE_TIME_SNAPSHOT_UNAVAILABLE", "writer"},
	}
	order := []string{}
	sum := 0
	for _, row := range body.Strategies {
		order = append(order, row.StrategyID)
		w := want[row.StrategyID]
		if row.Verdict != w.verdict || row.Action != w.action {
			t.Errorf("%s = %s/%s, want %s/%s", row.StrategyID, row.Verdict, row.Action, w.verdict, w.action)
		}
		if w.reason != "" && row.Reason != w.reason {
			t.Errorf("%s reason = %q, want %q", row.StrategyID, row.Reason, w.reason)
		}
		if w.attribution != "" && (len(row.Dispositions) == 0 || row.Dispositions[0].Attribution != w.attribution) {
			t.Errorf("%s dispositions = %+v, want attribution %s", row.StrategyID, row.Dispositions, w.attribution)
		}
	}
	for _, n := range body.Page.ByVerdict {
		sum += n
	}
	if strings.Join(order, ",") != "4101,4102,4103,4105,4109" || sum != 5 {
		t.Errorf("order %v sum %d, want numeric order and the verdicts summing to the universe", order, sum)
	}
}

// Paging covers the universe exactly once and reads it, and the fleet's
// snapshots, once for the whole diagnosis: later pages reuse the first
// page's read.
func TestDiagnosisPagesCoverTheUniverseOnceAndReadItOnce(t *testing.T) {
	rig := newDiagnosisRig(t, diagnosisFacts(), nil)
	for i := 1; i <= 7; i++ {
		rig.universe = append(rig.universe, strconv.Itoa(5000+i))
	}
	seen := map[string]int{}
	cursor, pages, total := "", 0, 0
	for {
		body := rig.page(t, cursor, 3)
		pages++
		if !body.Page.Holds || body.UniverseChanged != nil || body.SnapshotReread {
			t.Fatalf("page %d = %+v changed %v reread %v", pages, body.Page, body.UniverseChanged, body.SnapshotReread)
		}
		for _, row := range body.Strategies {
			seen[row.StrategyID]++
		}
		total += body.Page.RowsWritten
		if body.NextCursor == "" {
			break
		}
		cursor = body.NextCursor
	}
	if pages != 3 || total != 7 || len(seen) != 7 || rig.universeReads != 1 {
		t.Fatalf("pages %d total %d distinct %d reads %d, want 3 pages over 7 ids and one read", pages, total, len(seen), rig.universeReads)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s written %d times", id, n)
		}
	}
}

// With no population there is no diagnosis: the universe is unreadable by
// name, no rows, and the page does not hold -- never an empty list that
// reads as nothing wrong. The published catalog is not put in its place.
func TestAnUnreadableUniverseIsAFailedDiagnosisNotAnEmptyOne(t *testing.T) {
	rig := newDiagnosisRig(t, diagnosisFacts(), nil)
	rig.universeErr = errors.New("SOURCE_INCOMPLETE: active set key absent")
	body := rig.page(t, "", 0)
	if body.Universe.Status != "unreadable" || !strings.Contains(body.Universe.Reason, "SOURCE_INCOMPLETE") {
		t.Fatalf("universe = %+v, want unreadable with the reason", body.Universe)
	}
	if body.Page.Holds || len(body.Strategies) != 0 || body.NextCursor != "" {
		t.Fatalf("page = %+v rows %d next %q, want no rows and not holding", body.Page, len(body.Strategies), body.NextCursor)
	}
}

// A page asked after the first page's read has expired reads again, and
// says so; when the source's set moved in between, the page names both
// digests so the CLI reruns rather than stitching two populations.
func TestALaterPageThatRereadsSaysSoAndNamesAChangedUniverse(t *testing.T) {
	rig := newDiagnosisRig(t, diagnosisFacts(), nil)
	rig.universe = []string{"4101", "4102", "4103", "4109"}
	first := rig.page(t, "", 2)
	rig.clock = rig.clock.Add(DiagnosisCacheTTL + time.Second)
	rig.universe = []string{"4101", "4102", "4109"}
	second := rig.page(t, first.NextCursor, 2)
	if !second.SnapshotReread || second.UniverseChanged == nil || second.UniverseChanged.FromDigest != first.Universe.Digest ||
		second.UniverseChanged.ToDigest == first.Universe.Digest || rig.universeReads != 2 {
		t.Fatalf("second = reread %v changed %+v reads %d", second.SnapshotReread, second.UniverseChanged, rig.universeReads)
	}
	if second.Diagnosis != first.Diagnosis {
		t.Errorf("diagnosis id changed %s -> %s", first.Diagnosis, second.Diagnosis)
	}
}

// Progress is read once per page for the page's objects: a Plan found gets
// its slots, a Plan not found and a failed read are each unknown by name,
// and neither changes a verdict.
func TestDiagnosisProgressFillsSlotsAndNamesWhatItCouldNotRead(t *testing.T) {
	var asked [][]string
	rig := newDiagnosisRig(t, diagnosisFacts(), func(groups []string) (map[string]ProgressFacts, error) {
		asked = append(asked, groups)
		return map[string]ProgressFacts{"qg-4101-a": {LastFullSlot: 1790150400, NextSlot: 1790150460}}, nil
	})
	rig.universe = []string{"4101", "4103"}
	body := rig.page(t, "", 0)
	if body.Progress != "read" || len(asked) != 1 || strings.Join(asked[0], ",") != "qg-4101-a,qg-4101-b" {
		t.Fatalf("progress %q asked %v, want one read of the page's two objects", body.Progress, asked)
	}
	row := body.Strategies[0]
	if row.Plans[0].LastFullSlot == nil || *row.Plans[0].NextSlot != 1790150460 || row.Plans[1].LastFullSlot != nil {
		t.Errorf("plans = %+v, want a's slots and b's absent", row.Plans)
	}
	if !hasPart(row, "qg-4101-b progress", UnknownObjectNotObserved) || row.Verdict != StateDefect {
		t.Errorf("row = %+v, want b's progress unknown and the verdict unchanged", row)
	}

	failing := newDiagnosisRig(t, diagnosisFacts(), func([]string) (map[string]ProgressFacts, error) { return nil, errors.New("down") })
	failing.universe = []string{"4103"}
	body = failing.page(t, "", 0)
	if body.Progress != "unavailable" || !hasPart(body.Strategies[0], "qg-4101-a progress", "PROGRESS_UNREADABLE") ||
		body.Strategies[0].Verdict != StateDetecting {
		t.Errorf("failed read = %q %+v", body.Progress, body.Strategies[0])
	}
}

// With no catalog to answer from, every row is UNKNOWN by name and the
// universe is still counted, so the equation still holds.
func TestWithoutACatalogEveryRowIsUnknownAndTheUniverseIsStillCounted(t *testing.T) {
	rig := newDiagnosisRig(t, nil, nil)
	rig.universe = []string{"1", "2", "3"}
	body := rig.page(t, "", 0)
	if !body.Page.Holds || body.Page.ByVerdict[DiagnosisUnknown] != 3 || body.Universe.Count != 3 {
		t.Fatalf("page = %+v universe %+v", body.Page, body.Universe)
	}
	for _, row := range body.Strategies {
		if row.Reason != UnknownLookupUnavailable {
			t.Errorf("%s reason %q", row.StrategyID, row.Reason)
		}
	}
}

// A page is cut on bytes before a row, never in one: the rows after the
// cut open the next page, and nothing is lost or repeated.
func TestADiagnosisPageIsCutOnBytesWithoutLosingARow(t *testing.T) {
	universe, _ := NormalizeUniverse([]string{"1", "2", "3", "4", "5"})
	big := strings.Repeat("x", DiagnosisPageBytes/2)
	row := func(id string) DiagnosisRow {
		return DiagnosisRow{StrategyID: id, Verdict: StateDetecting, Plans: []DiagnosisPlan{{StrategyPlanRef: StrategyPlanRef{QueryGroup: big}}}}
	}
	var got []string
	after := ""
	for pages := 0; pages < 10; pages++ {
		page := buildDiagnosisPage(universe, after, 0, row)
		if !page.Holds {
			t.Fatalf("page %+v does not hold", page)
		}
		if page.RowsWritten > 1 && !page.TruncatedByBytes {
			t.Fatalf("page of %d half-budget rows not cut", page.RowsWritten)
		}
		for _, r := range page.Rows {
			got = append(got, r.StrategyID)
		}
		if page.Last == "" {
			break
		}
		after = page.Last
	}
	if strings.Join(got, ",") != "1,2,3,4,5" {
		t.Fatalf("rows = %v", got)
	}
}

func hasPart(row DiagnosisRow, what, reason string) bool {
	for _, part := range row.UnknownParts {
		if part.What == what && part.Reason == reason {
			return true
		}
	}
	return false
}
