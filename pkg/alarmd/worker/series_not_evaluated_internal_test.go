// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// seriesLines is the series_not_evaluated lines written to output.
func seriesLines(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	scanner.Buffer(make([]byte, 0, 1<<16), 1<<20)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("log line %q: %v", scanner.Text(), err)
		}
		if line["stage"] == observability.StageSeriesNotEvaluated {
			lines = append(lines, line)
		}
	}
	return lines
}

// A completed Slot that bound series to a Plan and took some of them up
// writes one WARN line naming its Query Group, the Plan, its family and the
// series lost, outside the sample: under a limiter that admits one line an
// hour per bucket, the Query Group's first is written at once. Its Slots
// after it write none for ten minutes; the first after says what was held
// back - the Slots and the series they lost. Another Query Group writes its
// own the while, and one released writes its first at once again. A Slot
// that took up every series writes none.
func TestASlotThatLostSeriesNamesThePlanOnOneWarnLine(t *testing.T) {
	var output bytes.Buffer
	limiter, err := observability.NewScopedLogLimiter(observability.ScopedLogLimiterConfig{Window: time.Hour, MaxEvents: 1, MaxScopes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := observability.NewScopedBoundedLogPolicy(limiter)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_788_000_000, 0)
	coordinator := &SlotExecutionCoordinator{ports: Ports{Observer: observability.NewLoggingObserver(observability.New("alarmd", &output), policy),
		Now: func() time.Time { return now }}}
	plan := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	bound := 5
	stream := &streamedExecution{
		slotStream: &slotStream{
			header: execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{{Identity: plan}}},
		},
		planSeriesCounts: map[execution.PlanIdentity]*int{plan: &bound},
	}
	for range 3 {
		stream.noteTakenUp(plan)
	}
	report := func(group execution.QueryGroupIdentity) {
		request := execution.SlotExecutionRequest{Operation: execution.OperationNormal,
			Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: group, EvaluationTime: execution.EvaluationTime(now.Unix())}}}
		coordinator.reportSeriesNotEvaluated(context.Background(), request, stream, stream.seriesNotEvaluated())
	}
	lineOf := func(t *testing.T, want int) map[string]any {
		t.Helper()
		lines := seriesLines(t, &output)
		if len(lines) != want {
			t.Fatalf("%d series_not_evaluated lines, want %d:\n%s", len(lines), want, output.String())
		}
		return lines[want-1]
	}

	report("query-group")
	line := lineOf(t, 1)
	if line["level"] != "WARN" || line["query_group_key"] != "query-group" ||
		line["reason_code"] != string(observability.ReasonSeriesNotEvaluated) || line["series_not_evaluated_suppressed_slots"] != nil {
		t.Fatalf("line %v, want WARN naming the Query Group and SERIES_NOT_EVALUATED, nothing held back", line)
	}
	plans, _ := line["series_not_evaluated_plans"].([]any)
	named, _ := plans[0].(map[string]any)
	if len(plans) != 1 || named["strategy_id"] != "7" || named["tenant_id"] != "tenant" || named["business_id"] != "2" ||
		named["family"] != observability.SeriesNotEvaluatedNoFamily || named["missing"] != float64(2) {
		t.Fatalf("plans %v, want strategy 7 losing 2 series under %s", plans, observability.SeriesNotEvaluatedNoFamily)
	}
	for minute := 1; minute <= 3; minute++ {
		now = now.Add(time.Minute)
		report("query-group")
	}
	lineOf(t, 1)
	report("another")
	if other := lineOf(t, 2); other["query_group_key"] != "another" {
		t.Fatalf("line %v, want another Query Group's own", other)
	}
	now = now.Add(seriesNotEvaluatedLineEvery)
	report("query-group")
	line = lineOf(t, 3)
	if line["query_group_key"] != "query-group" || line["series_not_evaluated_suppressed_slots"] != float64(3) ||
		line["series_not_evaluated_suppressed_series"] != float64(6) {
		t.Fatalf("line %v, want the three Slots and six series held back since the first", line)
	}
	ForgetQueryGroup(coordinator, "query-group")
	report("query-group")
	if line := lineOf(t, 4); line["series_not_evaluated_suppressed_slots"] != nil {
		t.Fatalf("line %v, want a released Query Group's first again, nothing held back", line)
	}

	output.Reset()
	for range 2 {
		stream.noteTakenUp(plan)
	}
	now = now.Add(seriesNotEvaluatedLineEvery)
	report("query-group")
	if lines := seriesLines(t, &output); len(lines) != 0 {
		t.Fatalf("%d series_not_evaluated lines for a Slot that took up every series, want none:\n%s", len(lines), output.String())
	}
}
