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
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func pausedObject(queryGroup string, entered, until int64, failures uint32, reason string, targets *observability.RequestTargetsFacts) Anomaly {
	return Anomaly{QueryGroup: queryGroup, PoolReason: reason, RequestTargets: targets,
		QueryCooldown: &observability.QueryCooldownFacts{Event: "entered", EnteredAt: time.Unix(entered, 0), Until: time.Unix(until, 0), Failures: failures}}
}

func askedFor(digest string, tables ...string) *observability.RequestTargetsFacts {
	facts := &observability.RequestTargetsFacts{Count: len(tables), Digest: digest}
	for _, table := range tables {
		facts.Targets = append(facts.Targets, observability.RequestTarget{TableID: table, FieldName: "usage", SpaceUID: "bkcc__2"})
	}
	return facts
}

func pausedStrategy(id string, paused ...Anomaly) DiagnosisRow {
	return DiagnosisRow{StrategyID: id, Verdict: StateStrategyInvalid, Action: ActionStrategyEdit, Check: CheckQueryTargetMissing,
		Plans: []DiagnosisPlan{boardPlan("qg-"+id, "r1")}, paused: paused}
}

// A paused strategy's row says since when (its earliest entry), until when
// every paused object has had its retry (the latest end), the most failures,
// how many objects are paused, and the reason and request targets of the one
// paused first; beside it, how many paused strategies asked for the same
// targets. A strategy whose targets are not known is counted with none, one
// that is not paused carries nothing. The paused strategies sort first, the
// most shared targets before the others, and select alone; read short of an
// instance, every count is a lower bound.
func TestAPausedStrategysRowSaysWhatItWaitsOnAndWhoElseAskedTheSame(t *testing.T) {
	shared, alone := askedFor("aaaaaaaaaaaaaaaa", "system.mem"), askedFor("bbbbbbbbbbbbbbbb", "system.disk")
	rows := map[string]DiagnosisRow{
		"10": pausedStrategy("10", pausedObject("qg-10a", 9_000, 10_500, 16, "QUERY_TARGET_MISSING", alone),
			pausedObject("qg-10b", 8_000, 10_100, 20, "QUERY_TARGET_MISSING", shared)),
		"11": pausedStrategy("11", pausedObject("qg-11", 8_500, 10_200, 3, "QUERY_TARGET_MISSING", shared)),
		"12": pausedStrategy("12", pausedObject("qg-12", 8_600, 10_200, 3, "QUERY_TIMEOUT", alone)),
		"13": pausedStrategy("13", pausedObject("qg-13", 8_700, 10_200, 3, "A_WORD_NOBODY_WROTE", nil)),
		"14": {StrategyID: "14", Verdict: StateStrategyInvalid, Action: ActionStrategyEdit, Check: CheckQueryTargetMissing,
			Plans: []DiagnosisPlan{boardPlan("qg-14", "r1")}},
	}
	universe := []string{"14", "13", "12", "11", "10"}
	board := func(reading pauseReading) *boardIndex {
		return buildBoard("c1", time.Unix(10_000, 0), "d", universe, func(id string) DiagnosisRow { return rows[id] }, nil, nil, nil, nil, reading)
	}
	index := board(pauseReading{coverage: InstanceCoverage{Counted: 2, Expected: 2}})
	byID := boardRows(index)
	pause := byID["10"].QueryPause
	until := time.Unix(10_500, 0)
	if pause == nil || !pause.Since.Equal(time.Unix(8_000, 0)) || pause.Until == nil || !pause.Until.Equal(until) || pause.Failures != 20 ||
		pause.Objects != (BoardCount{Value: 2}) || pause.Reason != "QUERY_TARGET_MISSING" ||
		!reflect.DeepEqual(pause.RequestTargets, shared.Targets) || pause.RequestTargetCount != 1 {
		t.Fatalf("strategy 10's pause = %+v, want since its first object's entry, until the latest end, 20 failures, two objects, the first's targets", pause)
	}
	for id, want := range map[string]*BoardCount{"10": {Value: 2}, "11": {Value: 2}, "12": {Value: 1}, "13": nil} {
		if got := byID[id].SameTargetStrategies; !reflect.DeepEqual(got, want) {
			t.Errorf("strategy %s: same target strategies %+v, want %+v", id, got, want)
		}
	}
	if byID["13"].QueryPause == nil || byID["13"].QueryPause.Reason != QueryPauseReasonOther || byID["13"].QueryPause.RequestTargets != nil {
		t.Errorf("a pause under a reason outside the list = %+v, want other and no targets", byID["13"].QueryPause)
	}
	if byID["14"].QueryPause != nil || byID["14"].SameTargetStrategies != nil {
		t.Errorf("a strategy not paused carries %+v", byID["14"].QueryPause)
	}
	var order []string
	for _, row := range index.rows {
		order = append(order, row.StrategyID)
	}
	if !reflect.DeepEqual(order, []string{"10", "11", "12", "13", "14"}) {
		t.Errorf("order %v, want the most shared targets first, the unknown last of the paused, the strategy not paused after them", order)
	}
	if index.facets.Paused != 4 {
		t.Errorf("paused facet = %d, want 4", index.facets.Paused)
	}
	if page := index.page(BoardFilter{Paused: true}, "", 50, DiagnosisSummaryFreshFor); page.Matched != 4 || page.Coverage != (InstanceCoverage{Counted: 2, Expected: 2}) {
		t.Errorf("paused selection matched %d with coverage %+v, want 4 of a whole reading", page.Matched, page.Coverage)
	}
	short := boardRows(board(pauseReading{coverage: InstanceCoverage{Counted: 1, Expected: 2}}))
	if !short["10"].QueryPause.Objects.AtLeast || !short["10"].SameTargetStrategies.AtLeast {
		t.Errorf("read short of an instance: %+v / %+v, want lower bounds", short["10"].QueryPause.Objects, short["10"].SameTargetStrategies)
	}
}

// Two objects that entered the pool in the same second - a pool restored at
// once - are named in one order whatever order the replicas answered in: the
// row's targets and its key do not trade places from one computation to the
// next.
func TestAPausedRowNamesTheSameObjectHoweverTheAnswersWereOrdered(t *testing.T) {
	// The object identities sort the other way from the target keys, so
	// it is the key that decides, as it must before the identity does.
	first := pausedObject("qg-a", 9_000, 10_000, 1, "QUERY_TARGET_MISSING", askedFor("bbbbbbbbbbbbbbbb", "system.disk"))
	second := pausedObject("qg-b", 9_000, 10_000, 1, "QUERY_TARGET_MISSING", askedFor("aaaaaaaaaaaaaaaa", "system.mem"))
	one, keyOne := queryPauseOf(pausedStrategy("1", first, second), false)
	other, keyOther := queryPauseOf(pausedStrategy("1", second, first), false)
	if !reflect.DeepEqual(one, other) || keyOne != keyOther || one.RequestTargets[0].TableID != "system.mem" {
		t.Fatalf("one order names %+v (%s), the other %+v (%s); want the same, the smaller key's", one, keyOne, other, keyOther)
	}
	objects := queryPauseObjectsOf(pausedStrategy("1", first, second))
	if len(objects) != 2 || objects[0].RequestTargets[0].TableID != "system.mem" || objects[1].RequestTargets[0].TableID != "system.disk" {
		t.Fatalf("drawer objects %+v, want both, in the same order", objects)
	}
}

// A Query Group two replicas both list in the pool - a handover's old owner
// and its new one for a moment - is one row, the newer owner's, and counted
// once in the total.
func TestAHandoversTwoOwnersListAPausedObjectOnce(t *testing.T) {
	older, newer := pausedObject("qg-1", 9_000, 10_000, 3, "QUERY_TARGET_MISSING", nil), pausedObject("qg-1", 9_000, 10_100, 4, "QUERY_TARGET_MISSING", nil)
	older.CooldownOwnerEpoch, newer.CooldownOwnerEpoch = 7, 9
	alone := pausedObject("qg-2", 9_000, 10_000, 1, "QUERY_TIMEOUT", nil)
	// The newer owner's row read first and read last: which replica answers
	// first is not which owner is newer.
	for _, first := range []Anomaly{newer, older} {
		second := older
		if first.CooldownOwnerEpoch == older.CooldownOwnerEpoch {
			second = newer
		}
		view := Aggregate(Expectation{}, []Snapshot{
			{Replica: "r1", TakenAt: now, Owned: 1, Demoted: []Anomaly{first}, TotalDemoted: 1},
			{Replica: "r2", TakenAt: now, Owned: 2, Demoted: []Anomaly{second, alone}, TotalDemoted: 2},
		}, []string{"r1", "r2"}, now, 0)
		if len(view.Demoted) != 2 || view.DemotedTotal != 2 {
			t.Fatalf("epoch %d first: pooled rows %d, total %d; want two objects, each once",
				first.CooldownOwnerEpoch, len(view.Demoted), view.DemotedTotal)
		}
		for _, row := range view.Demoted {
			if row.QueryGroup == "qg-1" && (row.CooldownOwnerEpoch != 9 || row.QueryCooldown.Failures != 4) {
				t.Fatalf("epoch %d first: the handed-over object's row is %+v, want the newer owner's", first.CooldownOwnerEpoch, row)
			}
		}
		if reading := pauseReadingOf(&view); reading.coverage != (InstanceCoverage{Counted: 2, Expected: 2}) || reading.atLeast() {
			t.Fatalf("epoch %d first: reading %+v, want both instances read whole", first.CooldownOwnerEpoch, reading)
		}
	}
}

// The pool's line reaches the object's row with what its failed query asked
// for and the epoch of the owner that said so, the pool's reason beside it;
// leaving the pool takes all three away.
func TestThePooledRowCarriesWhatItAskedForAndWhoseWordItIs(t *testing.T) {
	pool := newPoolRows(t)
	asked := askedFor("cccccccccccccccc", "system.cpu_summary")
	line := func(event string, result observability.Result) {
		pool.tick()
		trace := pool.trace(0)
		trace.OwnerEpoch = 12
		pool.tracker.Observe(context.Background(), observability.Observation{
			Component: observability.ComponentScheduler, Stage: observability.StageQueryCooldown, Result: result,
			ReasonCode: "QUERY_TARGET_MISSING", Trace: trace, RequestTargets: asked,
			QueryCooldown: &observability.QueryCooldownFacts{Event: event, EnteredAt: pool.at.at, Until: pool.at.at.Add(time.Hour)},
		})
	}
	line("entered", observability.ResultDegraded)
	row := pool.row()
	if row == nil || row.RequestTargets != asked || row.CooldownOwnerEpoch != 12 || row.PoolReason != "QUERY_TARGET_MISSING" {
		t.Fatalf("pooled row = %+v, want the targets, the owner's epoch and the reason", row)
	}
	line("recovered", observability.ResultResumed)
	if row := pool.row(); row != nil && (row.RequestTargets != nil || row.CooldownOwnerEpoch != 0 || row.PoolReason != "") {
		t.Fatalf("the row out of the pool keeps %+v", row)
	}
}

// A replica's paused objects count by pool reason, every reason present and
// one outside the list as other, and the earliest entry; the strategies they
// hold are the board's to count, so neither the health route nor the
// overview names a number of them. A part from a build before them leaves the
// deployment's counts lower bounds.
func TestThePausedObjectsCountByReasonAndTheEarliestEntry(t *testing.T) {
	strategy := func(ids ...string) []StrategyRef {
		var refs []StrategyRef
		for _, id := range ids {
			refs = append(refs, StrategyRef{StrategyID: id, BusinessID: "2"})
		}
		return refs
	}
	row := func(queryGroup, reason string, entered int64, strategies ...string) Anomaly {
		object := pausedObject(queryGroup, entered, 10_000, 1, reason, nil)
		object.Strategies = strategy(strategies...)
		return object
	}
	one := []Anomaly{row("qg-1", "QUERY_TARGET_MISSING", 9_000, "1"), row("qg-2", "QUERY_TARGET_MISSING", 8_000, "2"),
		row("qg-3", "A_WORD_NOBODY_WROTE", 9_500, "1")}
	two := []Anomaly{row("qg-4", "QUERY_TIMEOUT", 9_200, "1")}
	var merged *QueryPausePart
	mergeQueryPause(&merged, queryPausePartOf(View{Demoted: one, DemotedTotal: len(one)}))
	mergeQueryPause(&merged, queryPausePartOf(View{Demoted: two, DemotedTotal: len(two)}))
	want := newPauseReasonCounts()
	want["QUERY_TARGET_MISSING"], want["QUERY_TIMEOUT"], want[QueryPauseReasonOther] = 2, 1, 1
	if merged.Objects != 4 || !reflect.DeepEqual(merged.ByReason, want) ||
		!merged.LongestEnteredAt.Equal(time.Unix(8_000, 0)) || merged.Cut {
		t.Fatalf("merged = %+v, want four objects by reason and the earliest entry", merged)
	}
	view := &View{Replicas: []string{"r1", "r2"}, expectedReplicas: 2}
	health := healthQueryCooldownOf(view, merged)
	if health.Objects != 4 || health.AtLeast || health.LongestEnteredAt == nil {
		t.Fatalf("health = %+v, want the merged counts, exact", health)
	}
	line := overviewQueryPauseOf(HealthResponse{QueryCooldown: health})
	if line == nil || line.Objects != (BoardCount{Value: 4}) || !line.LongestSince.Equal(time.Unix(8_000, 0)) {
		t.Fatalf("overview line = %+v, want four objects, since the earliest entry", line)
	}
	encoded, _ := json.Marshal(map[string]any{"health": health, "overview": line})
	if strings.Contains(string(encoded), "strateg") {
		t.Fatalf("the health route or the overview names a number of strategies: %s", encoded)
	}
	mergeQueryPause(&merged, nil)
	if health := healthQueryCooldownOf(view, merged); !health.AtLeast {
		t.Fatalf("with a part from a build before it, health = %+v, want lower bounds", health)
	}
	if overviewQueryPauseOf(HealthResponse{}) != nil {
		t.Fatal("an overview with nothing from the health route says a pause")
	}
}

// paused=true selects the paused strategies; false or none, every strategy;
// any other word is refused by name.
func TestTheBoardSelectsPausedStrategiesByName(t *testing.T) {
	for raw, want := range map[string]bool{"true": true, "false": false, "": false} {
		filter, _, refusal := parseBoardQuery(url.Values{"paused": {raw}})
		if refusal != nil || filter.Paused != want {
			t.Errorf("paused=%q: %+v %v, want %t", raw, filter, refusal, want)
		}
	}
	if _, _, refusal := parseBoardQuery(url.Values{"paused": {"yes"}}); refusal == nil || refusal["facet"] != "paused" {
		t.Errorf("paused=yes refused as %v, want the facet named", refusal)
	}
	if (BoardFilter{Paused: true}).key() == (BoardFilter{}).key() {
		t.Error("a paused selection keys as every strategy, so its cursor would continue the other")
	}
}
