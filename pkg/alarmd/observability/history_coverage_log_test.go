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

// A completion that carries the window coverage renders it: the five numbers
// that describe the worst window on every such line, the optional counts only
// when they say something. Asserted on the keys the log carries: these facts
// rode the observation for months and reached only the object page, because
// nothing rendered them, and a reader of the logs could not tell that from a
// build that does not report coverage.
func TestACompletionLineRendersTheWindowCoverageItCarries(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	withheldObserver(t, &output).Observe(context.Background(), Observation{
		Component: ComponentProgress, Stage: StageProgressCommitted, Result: ResultDegraded,
		ReasonCode: "HISTORY_GAPPED",
		Trace:      TraceFields{QueryGroupKey: "qg-window", StrategyID: "4101", EvaluationTime: 600},
		HistoryCoverage: &HistoryCoverageFacts{
			Levels: 249, Short: 249, Empty: 0, WorstValid: 1466, WorstRequired: 1469, Guarded: 249,
			Abnormal: 9, AbnormalOnIncomplete: 9,
		},
	})
	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode completion log: %v; log=%s", err, output.String())
	}
	want := map[string]any{
		"history_levels":                 float64(249),
		"history_short":                  float64(249),
		"history_empty":                  float64(0),
		"history_worst_valid":            float64(1466),
		"history_worst_required":         float64(1469),
		"history_guarded":                float64(249),
		"history_abnormal":               float64(9),
		"history_abnormal_on_incomplete": float64(9),
	}
	for field, value := range want {
		got, present := event[field]
		if !present {
			t.Fatalf("the line has no %q; event=%#v", field, event)
		}
		if got != value {
			t.Fatalf("event[%q] = %#v, want %#v; event=%#v", field, got, value, event)
		}
	}
	// The counts that said nothing are not on the line, so a reader does not
	// learn to ignore a field that is always there and always zero.
	for _, field := range []string{"history_fresh", "history_short_fresh", "history_unusable", "history_unusable_reason"} {
		if _, present := event[field]; present {
			t.Fatalf("the line carries %q with nothing to say; event=%#v", field, event)
		}
	}

	// A complete window: levels, short and empty are still there, saying so.
	// Levels > 0 with short 0 is "every window was complete", and it has to
	// be a line that says it rather than a line missing the fields. The worst
	// pair belongs to a short window and there is none, so it is not on the
	// line rather than on it as 0/0.
	output.Reset()
	withheldObserver(t, &output).Observe(context.Background(), Observation{
		Component: ComponentProgress, Stage: StageProgressCommitted, Result: ResultSuccess,
		Trace:           TraceFields{QueryGroupKey: "qg-full", StrategyID: "4102", EvaluationTime: 660},
		HistoryCoverage: &HistoryCoverageFacts{Levels: 3, Short: 0, WorstValid: 5, WorstRequired: 5},
	})
	event = map[string]any{}
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode completion log: %v; log=%s", err, output.String())
	}
	if event["history_levels"] != float64(3) || event["history_short"] != float64(0) || event["history_empty"] != float64(0) {
		t.Fatalf("a complete window's line = %#v, want levels 3, short 0, empty 0 on it", event)
	}
	for _, field := range []string{"history_worst_valid", "history_worst_required", "history_guarded"} {
		if _, present := event[field]; present {
			t.Fatalf("a complete, unguarded window's line carries %q; event=%#v", field, event)
		}
	}
}
