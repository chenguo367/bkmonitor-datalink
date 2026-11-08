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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The series a replica's Query Groups bind are the Plans' latest counts over
// the Query Groups it holds: a Query Group it does not hold is not counted,
// and one with no evaluation line counts nothing.
func TestTheSeriesHeldAreThePlansCountsOverTheQueryGroupsHeld(t *testing.T) {
	tracker := newTracker(t, &clock{at: now})
	evaluated := func(queryGroup, strategy string, matched int) {
		ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: queryGroup})
		tracker.Observe(ctx, observability.Observation{
			Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
			Result:            observability.ResultSuccess,
			Trace:             observability.TraceFields{StrategyID: strategy, BusinessID: "2", EvaluationTime: 600},
			PlanSeriesMatched: &matched,
		})
	}
	evaluated("qg-a", "s-1", 3)
	evaluated("qg-a", "s-2", 5)
	evaluated("qg-b", "s-3", 7)
	for _, tc := range []struct {
		held []string
		want uint64
	}{
		{held: []string{"qg-a"}, want: 8},
		{held: []string{"qg-a", "qg-b"}, want: 15},
		{held: []string{"qg-b", "qg-none"}, want: 7},
		{held: nil, want: 0},
	} {
		held := map[string]struct{}{}
		for _, queryGroup := range tc.held {
			held[queryGroup] = struct{}{}
		}
		if got := tracker.SeriesMatchedIn(held); got != tc.want {
			t.Fatalf("series held over %v = %d, want %d", tc.held, got, tc.want)
		}
	}
}
