// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func namedWindow() HistoryWindowFact {
	return HistoryWindowFact{Series: "abc", Level: 5, Valid: 6, Required: 9, End: 540,
		Missing: []int64{120, 300}, MissingTotal: 2, Unusable: []int64{420}, UnusableTotal: 1, Guarded: true, GuardReason: "CONFIG_DRIFT"}
}

// A named window has to be one of the round's short windows and add up to
// its own shortfall; a list that does not comes from somewhere else and
// takes the counts beside it down with it, the way every other pair here
// does. Each defect is tried on its own so a fixture that trips two rules
// cannot pass on the wrong one.
func TestNamedWindowsThatCannotDescribeTheRoundAreDropped(t *testing.T) {
	t.Parallel()
	sound := &HistoryCoverageFacts{Levels: 3, Short: 1, WorstValid: 6, WorstRequired: 9, Windows: []HistoryWindowFact{namedWindow()}}
	if got := normalizeHistoryCoverageFacts(sound); got == nil || len(got.Windows) != 1 || got.Windows[0].Missing[0] != 120 {
		t.Fatalf("a sound named window was dropped or rewritten: %+v", got)
	}
	for name, mutate := range map[string]func(*HistoryWindowFact, *HistoryCoverageFacts){
		"more windows named than short": func(_ *HistoryWindowFact, facts *HistoryCoverageFacts) { facts.Short = 0 },
		"a full window named":           func(w *HistoryWindowFact, _ *HistoryCoverageFacts) { w.Valid = 9 },
		"no series":                     func(w *HistoryWindowFact, _ *HistoryCoverageFacts) { w.Series = "" },
		"holes not adding up":           func(w *HistoryWindowFact, _ *HistoryCoverageFacts) { w.MissingTotal = 5 },
		"more listed than counted": func(w *HistoryWindowFact, _ *HistoryCoverageFacts) {
			w.Unusable = []int64{420, 480}
			w.MissingTotal = 1
		},
		"a guard reason with no guard": func(w *HistoryWindowFact, _ *HistoryCoverageFacts) { w.Guarded = false },
		"over the listing bound": func(w *HistoryWindowFact, _ *HistoryCoverageFacts) {
			w.Missing = make([]int64, MaxHistoryWindowHoles+1)
			w.MissingTotal = MaxHistoryWindowHoles + 1
			w.UnusableTotal, w.Unusable = 0, nil
			w.Valid = w.Required - w.MissingTotal
			if w.Required <= w.MissingTotal {
				w.Required = w.MissingTotal + 1
				w.Valid = 1
			}
		},
	} {
		facts := &HistoryCoverageFacts{Levels: 3, Short: 1, WorstValid: 6, WorstRequired: 9, Windows: []HistoryWindowFact{namedWindow()}}
		mutate(&facts.Windows[0], facts)
		if got := normalizeHistoryCoverageFacts(facts); got != nil {
			t.Fatalf("%s: facts survived as %+v, want dropped", name, got)
		}
	}
	// More named windows than the bound is the evaluator's bound broken.
	crowded := &HistoryCoverageFacts{Levels: 20, Short: 20, WorstValid: 6, WorstRequired: 9}
	for i := 0; i <= MaxHistoryWindows; i++ {
		crowded.Windows = append(crowded.Windows, namedWindow())
	}
	if got := normalizeHistoryCoverageFacts(crowded); got != nil {
		t.Fatalf("%d named windows survived, want dropped over the bound of %d", len(crowded.Windows), MaxHistoryWindows)
	}
}

// The completion line carries the primary's answer and the worst named
// window under their own keys, so a search for the strategy reads which
// minutes its window lacks and whether the query that round answered whole.
func TestTheCompletionLineNamesTheWorstWindowAndThePrimaryAnswer(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	withheldObserver(t, &output).Observe(context.Background(), Observation{
		Component: ComponentProgress, Stage: StageProgressCommitted, Result: ResultSuccess,
		Trace:           TraceFields{StrategyID: "4101", BusinessID: "7", QueryGroupKey: "qg-window", EvaluationTime: 600},
		HistoryCoverage: &HistoryCoverageFacts{Levels: 3, Short: 2, WorstValid: 6, WorstRequired: 9, Windows: []HistoryWindowFact{namedWindow(), namedWindow()}},
		PrimaryInput:    &PrimaryInputFacts{Completeness: "FULL", DataState: "EMPTY"},
	})
	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode completion log: %v; log=%s", err, output.String())
	}
	for field, value := range map[string]any{
		"history_worst_series":   "abc",
		"history_worst_end":      float64(540),
		"history_worst_missing":  "120,300",
		"history_worst_unusable": "420",
		"history_worst_guard":    "CONFIG_DRIFT",
		"history_windows_named":  float64(2),
		"primary_completeness":   "FULL",
		// EMPTY rendered: it is the reading that says every series was
		// absent this minute, not a value to leave out.
		"primary_data_state": "EMPTY",
	} {
		got, present := event[field]
		if !present {
			t.Fatalf("the line has no %q; event=%#v", field, event)
		}
		if got != value {
			t.Fatalf("event[%q] = %#v, want %#v", field, got, value)
		}
	}
	// A primary answer outside the contract's words is not carried.
	if got := normalizePrimaryInputFacts(&PrimaryInputFacts{Completeness: "MOSTLY"}); got != nil {
		t.Fatalf("an unknown completeness survived: %+v", got)
	}
	if got := normalizePrimaryInputFacts(&PrimaryInputFacts{Completeness: "PARTIAL", DataState: "SOME"}); got != nil {
		t.Fatalf("an unknown data state survived: %+v", got)
	}
	if !(&PrimaryInputFacts{Completeness: "FULL", DataState: "DATA"}).PrimaryAnsweredWhole() ||
		(&PrimaryInputFacts{Completeness: "FULL", DataState: "EMPTY"}).PrimaryAnsweredWhole() ||
		(&PrimaryInputFacts{Completeness: "PARTIAL", DataState: "DATA"}).PrimaryAnsweredWhole() {
		t.Fatal("answered-whole is FULL with DATA and nothing else")
	}
}
