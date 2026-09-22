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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A strategy is one line, whatever its objects are under: the most severe
// check by the table's order decides the words, the onset is the earliest
// object's, the last good time the latest, and the objects are counted
// once each. A strategy on two rows under two checks was two lines in two
// places before; here it is one.
func TestAStrategyIsOneLineOverEveryObjectThatRunsIt(t *testing.T) {
	strategy := []StrategyRef{{StrategyID: "4101", BusinessID: "7"}}
	other := []StrategyRef{{StrategyID: "4102", BusinessID: "7"}}
	rows := []Anomaly{
		// Under the undecided window, holes the data's, seen an hour ago.
		{QueryGroup: "qg-window", Finding: Finding{Check: CheckWindowUndecided}, Since: now.Add(-time.Hour), SinceFrom: SinceBusinessState,
			LastHealthyAt: now.Add(-2 * time.Hour), ReasonLastAt: now, Coverage: shortWindows(6, 9, VerdictDataAbsentWhenQueried), Strategies: strategy},
		// The same strategy's other object, under a dependency that is
		// down -- earlier in the table, so it decides the line -- since two
		// hours, healthy more recently.
		{QueryGroup: "qg-redis", Finding: Finding{Check: CheckDependencyDown}, Since: now.Add(-2 * time.Hour), SinceFrom: SinceBusinessState,
			LastHealthyAt: now.Add(-90 * time.Minute), ReasonLastAt: now, Strategies: strategy},
		// A second strategy sharing the first object.
		{QueryGroup: "qg-window", Finding: Finding{Check: CheckWindowUndecided}, Since: now.Add(-time.Hour), SinceFrom: SinceBusinessState,
			ReasonLastAt: now, Coverage: shortWindows(6, 9, VerdictDataAbsentWhenQueried), Strategies: other},
	}
	view := &View{Anomalies: rows}
	lines := StrategyLines(view, now)
	if len(lines) != 2 {
		t.Fatalf("lines = %+v, want one per strategy", lines)
	}
	first := lines[0]
	if first.StrategyID != "4101" || first.Objects != 2 || first.DecidingObject != "qg-redis" {
		t.Fatalf("first line = %+v, want strategy 4101 over 2 objects decided by the dependency row", first)
	}
	if first.Standing.Check != CheckDependencyDown || first.Standing.State != StateDependencyUnanswered || first.Standing.Action != ActionServiceFix {
		t.Fatalf("first standing = %+v, want the dependency's words: the more severe check decides", first.Standing)
	}
	if first.Since == nil || !first.Since.Equal(now.Add(-2*time.Hour)) || first.LastGoodAt == nil || !first.LastGoodAt.Equal(now.Add(-90*time.Minute)) {
		t.Fatalf("first clocks = since %v / last good %v, want the earliest onset and the latest healthy completion", first.Since, first.LastGoodAt)
	}
	if !strings.HasPrefix(first.Line, "策略 4101 · 2 个对象 · 依赖没应答 · 本服务处理") {
		t.Fatalf("first line reads %q", first.Line)
	}
	second := lines[1]
	if second.StrategyID != "4102" || second.Objects != 1 || second.Standing.Action != ActionDataCheck ||
		!strings.Contains(second.Line, "数据没到 · 数据负责人查 · 最差窗口 6/9，缺的 3 分钟查询都正常返回、序列不在结果里") {
		t.Fatalf("second line = %+v", second)
	}
	// The filter keeps only the words asked for.
	if kept := FilterStrategyLines(lines, "", ActionDataCheck); len(kept) != 1 || kept[0].StrategyID != "4102" {
		t.Fatalf("filtered by DATA_CHECK = %+v", kept)
	}
	if kept := FilterStrategyLines(lines, StateDefect, ""); len(kept) != 0 {
		t.Fatalf("filtered by DEFECT = %+v, want none", kept)
	}
}

// The list endpoint: one line per strategy with the vocabulary beside it,
// filtered by words from the closed lists and refused for words outside
// them, bounded by the limit and saying so.
func TestTheStrategyListIsServedWithItsWords(t *testing.T) {
	handler := standingHandler(t, func(string) StrategyLookupFacts { return StrategyLookupFacts{} }, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/strategies?limit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/strategies = %d %s", response.Code, response.Body.String())
	}
	var body StrategyListResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Words.State) != len(StateWords) || len(body.Words.Action) != len(ActionWords) {
		t.Fatalf("words = %+v, want the whole vocabulary on the response", body.Words)
	}
	if body.Listed != 1 || body.Total < 1 {
		t.Fatalf("listed/total = %d/%d, want the one line the fixture's row makes", body.Listed, body.Total)
	}
	if body.Strategies[0].StrategyID != "8930" || body.Strategies[0].Line == "" {
		t.Fatalf("line = %+v", body.Strategies[0])
	}
	for _, bad := range []string{"/api/strategies?state=MOSTLY_FINE", "/api/strategies?action=SHRUG", "/api/strategies?limit=0"} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, bad, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400: a word outside the list is refused, not ignored", bad, response.Code)
		}
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/strategies?action=NONE", nil))
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Total != 0 || body.Action != ActionNone {
		t.Fatalf("filtered by NONE = %s", response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/strategies", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d", response.Code)
	}
}
