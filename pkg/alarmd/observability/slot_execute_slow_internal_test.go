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
	"strings"
	"testing"
	"time"
)

// A Slot attempt is slow at half its period, no sooner than 5 s and no later
// than 30 s, and at 30 s where its period is not known: on each of the three
// bounds, a millisecond under it says nothing and the bound itself says a
// slot_execute_slow line, degraded with SLOT_EXECUTE_SLOW and the threshold
// it was judged by.
func TestASlotAttemptIsSlowAtHalfItsPeriodWithinFiveAndThirtySeconds(t *testing.T) {
	for _, bound := range []struct {
		name      string
		period    int64
		threshold time.Duration
	}{
		{name: "half a twenty-second period", period: 20, threshold: 10 * time.Second},
		{name: "the floor under half a six-second period", period: 6, threshold: 5 * time.Second},
		{name: "the ceiling over half a two-minute period", period: 120, threshold: 30 * time.Second},
		{name: "the ceiling for a period not known", period: 0, threshold: 30 * time.Second},
	} {
		for _, elapsed := range []time.Duration{bound.threshold - time.Millisecond, bound.threshold} {
			var said []Observation
			observer := ObserverFunc(func(_ context.Context, observation Observation) { said = append(said, observation) })
			slow := ObserveSlotExecution(context.Background(), observer, OperationNormal, TraceFields{QueryGroupKey: "qg"}, elapsed,
				SlotExecuteSlowFacts{PeriodSeconds: bound.period})
			if want := elapsed >= bound.threshold; slow != want || len(said) != map[bool]int{false: 0, true: 1}[want] {
				t.Fatalf("%s: an attempt of %s said %v (%d lines), want %v", bound.name, elapsed, slow, len(said), want)
			}
			if !slow {
				continue
			}
			line := said[0]
			if line.Component != ComponentScheduler || line.Stage != StageSlotExecuteSlow || line.Result != Result(ResultDegraded) ||
				line.ReasonCode != ReasonSlotExecuteSlow || line.Duration != elapsed || line.SlotExecuteSlow == nil ||
				line.SlotExecuteSlow.ThresholdMillis != bound.threshold.Milliseconds() || line.SlotExecuteSlow.PeriodSeconds != bound.period {
				t.Fatalf("%s: said %+v (facts %+v), want a degraded SLOT_EXECUTE_SLOW line of %s against %s", bound.name, line,
					line.SlotExecuteSlow, elapsed, bound.threshold)
			}
		}
	}
	if ObserveSlotExecution(context.Background(), nil, OperationNormal, TraceFields{}, time.Hour, SlotExecuteSlowFacts{}) {
		t.Fatal("a slow attempt with no observer says it was said")
	}
}

// A slow attempt is written at WARN with where its time went, at most one
// line a Query Group an hour: three of one Query Group within the hour are
// one line, another Query Group's has its own, and the first of the next
// hour carries the two merged into it.
func TestASlowSlotAttemptIsWrittenOnceAQueryGroupAnHour(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter, err := newScopedLogLimiter(ScopedLogLimiterConfig{Window: time.Minute, MaxEvents: 16, MaxScopes: 1024}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewScopedBoundedLogPolicy(limiter)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	logging := NewLoggingObserver(New("alarmd", &output), policy)
	facts := SlotExecuteSlowFacts{PeriodSeconds: 60, Timing: SlotTimingFacts{Slot: 41000, Input: 22000, Apply: 9000},
		QueryWaitMillis: 18000, QueryBodyMillis: 4000, QueryWaitMillisMax: 9000, QueryBodyMillisMax: 2000, InputSlices: 3,
		InputSliceTailMillis: 1200, InputSliceTailMillisMax: 700, ExecutionSlotWaitMillis: 2500, QueryPermitWaitMillis: 6000,
		QueryPermitWaitMillisMax: 4000}
	slow := func(queryGroup string, operation Operation) {
		t.Helper()
		if !ObserveSlotExecution(context.Background(), logging, operation, TraceFields{QueryGroupKey: queryGroup, EvaluationTime: 1_700_000_040},
			41*time.Second, facts) {
			t.Fatal("fixture: a 41 s attempt of a minute's period is not slow")
		}
	}
	slow("qg-a", OperationNormal)
	slow("qg-a", OperationRetry)
	slow("qg-a", OperationReplay)
	slow("qg-b", OperationNormal)
	now = now.Add(time.Hour)
	slow("qg-a", OperationNormal)

	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %v: %q", err, raw)
		}
		lines = append(lines, line)
	}
	if len(lines) != 3 || lines[0]["query_group_key"] != "qg-a" || lines[1]["query_group_key"] != "qg-b" ||
		lines[2]["query_group_key"] != "qg-a" || lines[0]["suppressed_logs"] != float64(0) || lines[2]["suppressed_logs"] != float64(2) {
		t.Fatalf("lines %v, want qg-a's first, qg-b's, and qg-a's of the next hour carrying its two merged", lines)
	}
	want := map[string]any{"stage": string(StageSlotExecuteSlow), "result": string(ResultDegraded),
		"reason_code": string(ReasonSlotExecuteSlow), "level": "WARN", "operation": string(OperationNormal),
		"duration_ms": float64(41000), "period_seconds": float64(60),
		"threshold_ms": float64(30000), "execution_slot_wait_ms": float64(2500), "query_permit_wait_ms": float64(6000),
		"query_permit_wait_ms_max": float64(4000), "query_wait_millis": float64(18000), "query_body_millis": float64(4000),
		"query_wait_millis_max": float64(9000), "query_body_millis_max": float64(2000), "input_slices": float64(3),
		"input_slice_tail_millis": float64(1200), "input_slice_tail_millis_max": float64(700), "slot_millis": float64(41000),
		"input_millis": float64(22000), "apply_millis": float64(9000), "evaluation_time": float64(1_700_000_040)}
	for key, value := range want {
		if lines[0][key] != value {
			t.Errorf("the line's %s is %v, want %v: %v", key, lines[0][key], value, lines[0])
		}
	}
}
