// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
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
	"reflect"
	"strings"
	"testing"
	"time"
)

// A Slot's plan scope is rendered on its line as arrays and counts, the lists
// kept to PlanScopeSample and the counts never below what they were given;
// a line without one carries none of its keys.
func TestThePlanScopeRendersAsArraysAndBoundedCounts(t *testing.T) {
	render := func(observation Observation) map[string]any {
		t.Helper()
		var output bytes.Buffer
		limiter, err := NewScopedLogLimiter(ScopedLogLimiterConfig{Window: time.Hour, MaxEvents: 100, MaxScopes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		policy, err := NewScopedBoundedLogPolicy(limiter)
		if err != nil {
			t.Fatal(err)
		}
		NewLoggingObserver(New("alarmd", &output), policy).Observe(context.Background(), observation)
		var line map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(output.String())), &line); err != nil {
			t.Fatalf("line %q: %v", output.String(), err)
		}
		return line
	}
	line := render(Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultDegraded, ReasonCode: "GAP_SKIPPED",
		PlanScope: &PlanScopeFacts{StrategyIDs: []string{"1", "2", "3", "4", "5", "6", "7"}, BusinessIDs: []string{"2"}, BusinessCount: 1}})
	if !reflect.DeepEqual(line["strategy_ids"], []any{"1", "2", "3", "4", "5"}) || line["strategy_count"] != float64(7) ||
		!reflect.DeepEqual(line["business_ids"], []any{"2"}) || line["business_count"] != float64(1) {
		t.Fatalf("plan scope rendered as %v %v %v %v; want five strategies of seven, one business",
			line["strategy_ids"], line["strategy_count"], line["business_ids"], line["business_count"])
	}
	bare := render(Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultDegraded, ReasonCode: "GAP_SKIPPED"})
	for _, key := range []string{"strategy_ids", "strategy_count", "business_ids", "business_count", "skip_gap_strategies"} {
		if _, present := bare[key]; present {
			t.Fatalf("a line without a plan scope carries %s: %v", key, bare)
		}
	}
}
