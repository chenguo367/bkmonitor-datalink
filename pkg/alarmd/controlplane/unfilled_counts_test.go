// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package controlplane

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Which queries answer counts their provider does not zero-fill: the two log
// count sources, and a log time series aggregated by count or a count-like
// distinct. A log time series aggregated by avg, sum or max is not, and
// neither is an event count (its range is widened instead) or a metric.
func TestUnfilledCountsAreLogCountsAndCountedLogSeries(t *testing.T) {
	clause := func(over string, functions ...string) execution.QueryClause {
		c := execution.QueryClause{TimeAggregation: execution.QueryFunction{Method: over}}
		for _, method := range functions {
			c.Functions = append(c.Functions, execution.QueryFunction{Method: method})
		}
		return c
	}
	query := func(source string, clauses ...execution.QueryClause) execution.QueryPlanFacts {
		return execution.QueryPlanFacts{SourceSemantics: []string{source}, QueryList: clauses}
	}
	for name, test := range map[string]struct {
		query execution.QueryPlanFacts
		want  bool
	}{
		"log search log":           {query("bk_log_search/log", clause("count_over_time", "sum")), true},
		"monitor log":              {query("bk_monitor/log", clause("count_over_time", "sum")), true},
		"log series count":         {query("bk_log_search/time_series", clause("count_over_time", "sum")), true},
		"log series distinct":      {query("bk_log_search/time_series", clause("distinct_over_time", "distinct")), true},
		"log series count no time": {query("bk_log_search/time_series", clause("", "count")), true},
		"log series avg":           {query("bk_log_search/time_series", clause("avg_over_time", "mean")), false},
		"log series sum":           {query("bk_log_search/time_series", clause("sum_over_time", "sum")), false},
		"log series max":           {query("bk_log_search/time_series", clause("max_over_time", "max")), false},
		"log series mixed":         {query("bk_log_search/time_series", clause("count_over_time", "sum"), clause("avg_over_time", "mean")), false},
		"event count":              {query("custom/event", clause("count_over_time", "sum")), false},
		"metric count":             {query("bk_monitor/time_series", clause("count_over_time", "sum")), false},
		"two sources": {execution.QueryPlanFacts{SourceSemantics: []string{"bk_log_search/log", "bk_monitor/log"},
			QueryList: []execution.QueryClause{clause("count_over_time", "sum")}}, false},
	} {
		if got := readsUnfilledCounts(test.query); got != test.want {
			t.Errorf("%s: unfilled = %t, want %t", name, got, test.want)
		}
	}
}

// The primary requirement carries the word, for a log count and only for one.
func TestThePrimaryRequirementSaysALogCountIsUnfilled(t *testing.T) {
	runtime := &RedisCatalogRuntime{downstreamReserve: time.Second}
	due := []execution.DuePlan{{Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "101"},
		ScheduleSpec: execution.ScheduleSpec{EvaluationIntervalSeconds: 60}}}
	for source, want := range map[string]bool{"bk_log_search/log": true, "bk_monitor/time_series": false} {
		group := QueryGroup{QueryPlan: execution.QueryPlanFacts{SourceSemantics: []string{source}, StepMillis: 60000, AlignmentMillis: 60000,
			QueryList: []execution.QueryClause{{TimeAggregation: execution.QueryFunction{Method: "count_over_time"}}}}}
		requirements, err := runtime.primaryRequirements(group, due, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(requirements) != 1 || requirements[0].UnfilledCounts != want {
			t.Fatalf("%s: requirements %+v, want one with UnfilledCounts %t", source, requirements, want)
		}
	}
}
