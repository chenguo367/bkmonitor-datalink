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
	"strconv"
	"strings"
	"testing"
	"time"
)

// withheldObserver is a logging observer over a buffer, with a limiter tight
// enough that anything it governs is suppressed after one line.
func withheldObserver(t *testing.T, output *bytes.Buffer) Observer {
	t.Helper()
	limiter, err := NewWindowLogLimiter(WindowLogLimiterConfig{Window: time.Hour, MaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewBoundedLogPolicy(limiter)
	if err != nil {
		t.Fatal(err)
	}
	return NewLoggingObserver(New("alarmd", output), policy)
}

// The line says which strategy, what happened to it, and why.
//
// Those are the three an operator arrives with. The strategy comes from the
// trace, which already carries it for every other stage, and the pair comes
// from the facts because ReasonCode would fold every one of these to _other.
func TestAWithheldLineNamesTheStrategyTheDispositionAndTheReason(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	withheldObserver(t, &output).Observe(context.Background(), Observation{
		Component: ComponentControlPlane, Stage: StageSourceWithheld, Result: ResultSuccess,
		Trace:          TraceFields{StrategyID: "31415", TerminalScope: "LEVEL", LevelID: "2"},
		SourceWithheld: &SourceWithheldFacts{Disposition: "CONFIG_REJECTED", Reason: "NO_DATA_CONFIG_INVALID"},
	})

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode withheld observation log: %v; log=%s", err, output.String())
	}
	want := map[string]any{
		"strategy_id":          "31415",
		"withheld_disposition": "CONFIG_REJECTED",
		"withheld_reason":      "NO_DATA_CONFIG_INVALID",
		"level_id":             "2",
		"terminal_scope":       "LEVEL",
	}
	for field, value := range want {
		if event[field] != value {
			t.Fatalf("event[%q] = %#v, want %#v; event=%#v", field, event[field], value, event)
		}
	}
	// Nothing was cut, so the line does not carry a cut. A withheld_dropped: 0
	// on every line would be read as a field that is always there and always
	// zero, and the one line that says 58,000 would look like the same field.
	if _, present := event["withheld_dropped"]; present {
		t.Fatalf("a line that cut nothing reports withheld_dropped; event=%#v", event)
	}
}

// The cut is a field, on the line that reports it.
func TestAWithheldLineReportsWhatTheBudgetCut(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	withheldObserver(t, &output).Observe(context.Background(), Observation{
		Component: ComponentControlPlane, Stage: StageSourceWithheld, Result: ResultSuccess,
		Trace:          TraceFields{StrategyID: "1"},
		SourceWithheld: &SourceWithheldFacts{Disposition: "CONFIG_REJECTED", Reason: "PLAN_INVALID", Dropped: 58000},
	})

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode withheld observation log: %v; log=%s", err, output.String())
	}
	if event["withheld_dropped"] != float64(58000) {
		t.Fatalf("withheld_dropped = %#v, want 58000; event=%#v", event["withheld_dropped"], event)
	}
}

// Every line of a round is written, however many there are.
//
// The repeated-line budget is per (reason, query group), and a withheld source
// has no query group - it never became a Plan - so every line of a round would
// share one bucket and all but the first would be merged into a suppressed
// count. The half that vanished is the half someone is looking for: these
// lines exist because a strategy that is not running cannot be asked about
// any other way here. What bounds the volume is upstream, in the report being
// a difference with a line budget.
func TestARoundsWithheldLinesAreNotMergedIntoASuppressedCount(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	observer := withheldObserver(t, &output)
	const lines = 25
	for index := 0; index < lines; index++ {
		observer.Observe(context.Background(), Observation{
			Component: ComponentControlPlane, Stage: StageSourceWithheld, Result: ResultSuccess,
			Trace: TraceFields{StrategyID: strconv.Itoa(index)},
			// The same reason on every line, which is the real shape: a round
			// that rejects a whole release's worth of strategies rejects most
			// of them for the same reason.
			SourceWithheld: &SourceWithheldFacts{Disposition: "CONFIG_REJECTED", Reason: "PLAN_INVALID"},
		})
	}

	written := strings.Count(strings.TrimSpace(output.String()), "\n") + 1
	if written != lines {
		t.Fatalf("lines written = %d, want all %d: the limiter is per (reason, query group) and these have "+
			"no query group, so a governed stage would collapse the round into one line", written, lines)
	}
	// And each one names its own strategy, rather than the last one naming a
	// count of the ones before it.
	for index := 0; index < lines; index++ {
		if !strings.Contains(output.String(), `"strategy_id":"`+strconv.Itoa(index)+`"`) {
			t.Fatalf("strategy %d has no line of its own; log=%s", index, output.String())
		}
	}
}
