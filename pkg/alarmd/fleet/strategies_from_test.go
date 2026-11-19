// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	model "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// pooledRow is a demoted row of the degraded pool, as the tracker lists one
// no round named a strategy of.
func pooledRow(queryGroup string, epoch uint64, at time.Time) Anomaly {
	return Anomaly{QueryGroup: queryGroup, Kind: KindQueryCooldown, Since: at.Add(-time.Hour), CooldownOwnerEpoch: epoch,
		QueryCooldown: &observability.QueryCooldownFacts{Event: "restored", Source: "restored", Until: at.Add(time.Hour)}}
}

func plansOf(strategies ...int) []model.FrozenPlanSchedule {
	plans := make([]model.FrozenPlanSchedule, 0, len(strategies))
	for _, strategy := range strategies {
		plans = append(plans, model.FrozenPlanSchedule{Identity: model.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: fmt.Sprint(strategy)}})
	}
	return plans
}

// A row read from its schedule names each strategy once, its list cut at a
// row's bound and its count over every Plan; the impact and the check
// report count all of them, and say how many the list does not name.
func TestARowReadFromItsScheduleCountsEveryPlansStrategy(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	row := pooledRow("qg-large", 3, at)
	strategies := make([]int, 0, 41)
	for strategy := 100; strategy < 140; strategy++ {
		strategies = append(strategies, strategy)
	}
	FillScheduleStrategies(&row, plansOf(append(strategies, 100)...))
	if len(row.Strategies) != 32 || row.StrategiesTotal != 40 || row.StrategiesFrom != StrategiesFromSchedule ||
		row.Strategies[0] != (StrategyRef{StrategyID: "100", BusinessID: "2"}) {
		t.Fatalf("row lists %d of %d from %q (%v), want 32 of the 40 strategies of its Plans, each once", len(row.Strategies),
			row.StrategiesTotal, row.StrategiesFrom, row.Strategies[:min(2, len(row.Strategies))])
	}
	view := View{Demoted: []Anomaly{row}, DemotedTotal: 1}
	tally := ImpactTallyOf(view, at)
	encoded, err := json.Marshal(tally)
	if err != nil {
		t.Fatal(err)
	}
	var read ImpactTally
	if err := json.Unmarshal(encoded, &read); err != nil {
		t.Fatal(err)
	}
	for _, impact := range []Impact{tally.Impact(), read.Impact()} {
		if impact.Demoted.Strategies != 40 || impact.Demoted.StrategiesUnlisted != 8 || impact.Blind.Strategies != 40 || impact.NoStrategies != 0 {
			t.Fatalf("demoted %+v, blind %+v, no strategies %d: want all 40 counted, 8 of them unlisted", impact.Demoted, impact.Blind, impact.NoStrategies)
		}
	}
	// Under a check folded by strategy, as attribute decides a row's finding.
	row.Finding = Finding{Check: CheckQuiet, Group: groupKeyOf(row, CheckQuiet)}
	for _, report := range ReportChecks([][]Anomaly{{row}}, nil, nil, at) {
		if report.Code != CheckQuiet {
			continue
		}
		if report.Strategies != 40 || len(report.Groups) != 1 || report.Groups[0].Strategies != 40 {
			t.Fatalf("the check counts %d strategies, its group %+v; want all 40", report.Strategies, report.Groups)
		}
		return
	}
	t.Fatal("no QUIET report")
}

// A pooled row whose schedule was not read names no strategy and says why:
// counted among the rows without strategies by its reason, and folded under
// its reason's word, apart from the rows nothing could name a strategy for.
// A tally naming a reason a row cannot have is refused.
func TestAnUnreadPooledRowIsCountedByWhyAndGroupedByItsWord(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	var rows []Anomaly
	for index, reason := range StrategiesUnfilled {
		row := pooledRow(fmt.Sprintf("qg-%d", index), 3, at)
		MarkStrategiesUnfilled(&row, reason)
		rows = append(rows, row)
	}
	bare := Anomaly{QueryGroup: "qg-bare", Kind: KindQueryCooldown, Since: at.Add(-time.Hour)}
	rows = append(rows, bare)
	impact := ImpactTallyOf(View{Demoted: rows, DemotedTotal: len(rows)}, at).Impact()
	if impact.NoStrategies != len(rows) {
		t.Fatalf("no strategies %d, want every row", impact.NoStrategies)
	}
	for _, reason := range StrategiesUnfilled {
		if impact.NoStrategiesBy.Of(reason) != 1 {
			t.Fatalf("no strategies by %+v, want one %s", impact.NoStrategiesBy, reason)
		}
	}
	if encoded, _ := json.Marshal(impact.NoStrategiesBy); string(encoded) != `{"schedule_unread":1,"schedule_revision_moved":1,"schedule_revision_unknown":1,"schedule_pending":1}` {
		t.Fatalf("no strategies by writes %s, each reason under its own name", encoded)
	}
	words := ProductWords().StrategiesFrom
	want := map[string]bool{groupNoStrategy: true}
	for _, reason := range StrategiesUnfilled {
		want["("+words[reason]+")"] = true
	}
	for index := range rows {
		rows[index].Finding = Finding{Check: CheckQuiet, Group: groupKeyOf(rows[index], CheckQuiet)}
	}
	got := map[string]bool{}
	for _, report := range ReportChecks([][]Anomaly{rows}, nil, nil, at) {
		for _, group := range report.Groups {
			got[group.Key] = true
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("groups %v, want each reason under its word and the bare row under %s", got, groupNoStrategy)
	}
	if err := json.Unmarshal([]byte(`{"parts":{},"no_strategies":1,"no_strategies_by":{"guessed":1}}`), &ImpactTally{}); err == nil ||
		!strings.Contains(err.Error(), "guessed") {
		t.Fatalf("a tally naming an unknown reason read as %v", err)
	}
}

// Of a handover's two owners listing one pooled Query Group, the newer
// owner's row is kept whole - its strategies and where they came from -
// whichever replica read its schedule first.
func TestAHandoverKeepsTheNewerOwnersPooledRowWhole(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	older, newer := pooledRow("qg-1", 7, at), pooledRow("qg-1", 9, at)
	FillScheduleStrategies(&older, plansOf(11, 12))
	MarkStrategiesUnfilled(&newer, StrategiesFromSchedulePending)
	for _, first := range [][2]Anomaly{{older, newer}, {newer, older}} {
		view := Aggregate(Expectation{}, []Snapshot{
			{Replica: "r1", TakenAt: at, Owned: 1, Demoted: []Anomaly{first[0]}, TotalDemoted: 1},
			{Replica: "r2", TakenAt: at, Owned: 1, Demoted: []Anomaly{first[1]}, TotalDemoted: 1},
		}, []string{"r1", "r2"}, at, 0)
		if len(view.Demoted) != 1 || view.Demoted[0].CooldownOwnerEpoch != 9 || view.Demoted[0].StrategiesFrom != StrategiesFromSchedulePending ||
			len(view.Demoted[0].Strategies) != 0 {
			t.Fatalf("kept %+v, want the newer owner's row, still pending", view.Demoted)
		}
	}
}

// The words a page shows for where a row's strategies came from are its
// values, each with a word.
func TestTheStrategiesFromWordsAreItsValues(t *testing.T) {
	words := ProductWords().StrategiesFrom
	if len(words) != len(StrategiesFromValues()) {
		t.Fatalf("%d words for %d values", len(words), len(StrategiesFromValues()))
	}
	for _, value := range StrategiesFromValues() {
		if words[value] == "" {
			t.Errorf("%s has no word", value)
		}
	}
}
