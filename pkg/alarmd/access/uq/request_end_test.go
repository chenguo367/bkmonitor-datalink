// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// rangeQueryService answers /query/ts as unify-query evaluates a range query
// (v3.9.4910, v3.9.4892): the request's start aligned in its timezone to a
// multiple of the step (function.TimeOffset), then every grid point from it
// up to and including the end, each valued from its own time. It keeps the
// end each request asked for.
func rangeQueryService(t *testing.T) (*Client, func() []int64) {
	t.Helper()
	var mu sync.Mutex
	var ends []int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		start, _ := strconv.ParseInt(fmt.Sprint(body["start_time"]), 10, 64)
		end, _ := strconv.ParseInt(fmt.Sprint(body["end_time"]), 10, 64)
		step, _ := time.ParseDuration(fmt.Sprint(body["step"]))
		location, err := time.LoadLocation(fmt.Sprint(body["timezone"]))
		if err != nil {
			location = time.UTC
		}
		mu.Lock()
		ends = append(ends, end)
		mu.Unlock()
		_, offset := time.Unix(start, 0).In(location).Zone()
		seconds := int64(step.Seconds())
		aligned := int64(math.Floor(float64(start+int64(offset))/float64(seconds)))*seconds - int64(offset)
		var times []int64
		for at := aligned; at <= end; at += seconds {
			times = append(times, at)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(valuedPointsBody(times...)))
	}))
	t.Cleanup(server.Close)
	client, err := NewClientWithLimits(server.URL, "alarmd-shadow", server.Client(), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return client, func() []int64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]int64(nil), ends...)
	}
}

// valuedPointsBody is one series with a point at each time, valued by it.
func valuedPointsBody(times ...int64) string {
	values := ""
	for index, at := range times {
		if index > 0 {
			values += ","
		}
		values += fmt.Sprintf("[%d,%d]", at*1000, at%1000)
	}
	return `{"series":[{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],` +
		`"group_values":["127.0.0.1"],"values":[` + values + `]}],"is_partial":false}`
}

// windowedAttempt is validAttempt's metric time series over the window
// [end-span, end), at step and in timezone, its clauses' data source as given.
func windowedAttempt(t *testing.T, end, span, step int64, timezone, source string) execution.QueryAttempt {
	t.Helper()
	attempt := validAttempt(t)
	facts := attempt.Spec.PlanFacts
	facts.QueryRevision, facts.Timezone = "", timezone
	facts.StepMillis, facts.AlignmentMillis = step*1000, step*1000
	facts.QueryList = append([]execution.QueryClause(nil), facts.QueryList...)
	facts.QueryList[0].DataSource = source
	facts.QueryList[0].TimeAggregation.Window = fmt.Sprintf("%ds", step)
	rebuilt, err := execution.BuildQueryPlanFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	window := execution.QueryWindow{Start: end - span, End: end}
	spec, err := execution.BuildPhysicalQuerySpec(execution.PhysicalQuerySpec{PlanFacts: rebuilt,
		LogicalWindow: window, ProviderRange: window, AcceptedRange: window, RequiredColumns: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt.Spec = spec
	return attempt
}

// executed runs the attempt against client: the times it took in and the
// values at them, and what it let go.
func executed(t *testing.T, client *Client, attempt execution.QueryAttempt) ([]int64, map[int64]float64, observability.PointDrops) {
	t.Helper()
	sink := &collectingSink{}
	completion, err := client.Execute(context.Background(), attempt, sink)
	if err != nil || completion.Completeness != execution.CompletenessFull {
		t.Fatalf("completion %+v (%v), want full", completion, err)
	}
	var times []int64
	values := map[int64]float64{}
	for _, batch := range sink.batches {
		for index := 0; index < batch.Dataset.Len(); index++ {
			record, _ := batch.Dataset.Record(index)
			times = append(times, record.SourceTime())
			if value, ok := record.Value("value"); ok {
				values[record.SourceTime()], _ = strconv.ParseFloat(string(value), 64)
			}
		}
	}
	return times, values, completion.Stats.PointsDropped
}

// A one-minute window of a metric time series - by any of the names its
// source goes by - asks the provider to end a second before the window: the
// point at the window's end, the next round's, is not asked for and none is
// let go; every point of the window still comes back, its last one with its
// value. In UTC and in a timezone hours off it alike.
func TestAOneMinuteMetricWindowAsksASecondBeforeItsEndAndTakesEveryPointOfIt(t *testing.T) {
	const end, span = int64(1_700_124_000), int64(300)
	for _, source := range []string{"", "bkmonitor", "bk_monitor"} {
		for _, timezone := range []string{"UTC", "Asia/Shanghai"} {
			client, ends := rangeQueryService(t)
			var heard []string
			client.WithRequestEndObserver(func(outcome string) { heard = append(heard, outcome) })
			times, values, dropped := executed(t, client, windowedAttempt(t, end, span, 60, timezone, source))
			if asked := ends(); len(asked) != 1 || asked[0] != end-1 {
				t.Fatalf("%q %s: asked to end at %v, want a second before the window's end %d", source, timezone, asked, end)
			}
			if dropped.Total() != 0 {
				t.Fatalf("%q %s: let go %+v, want nothing", source, timezone, dropped)
			}
			want := []int64{end - 300, end - 240, end - 180, end - 120, end - 60}
			if fmt.Sprint(times) != fmt.Sprint(want) || values[end-60] != float64((end-60)%1000) {
				t.Fatalf("%q %s: took %v (last valued %v), want %v with the last valued %d", source, timezone, times, values[end-60], want,
					(end-60)%1000)
			}
			if len(heard) != 1 || heard[0] != observability.QueryRequestEndTrimmed {
				t.Fatalf("%q %s: heard %v, want the request trimmed", source, timezone, heard)
			}
		}
	}
}

// A day's step in a timezone half an hour off whole hours puts its grid on
// local midnight. Whether the window ends on a Unix day - no grid point - or
// on the local midnight, asking a second before its end takes the same
// points: every local midnight in the window, nothing let go.
func TestADaysStepOnLocalMidnightTakesEveryMidnightOfItsWindow(t *testing.T) {
	const day = int64(86400)
	unixDay := int64(1_700_092_800)          // 00:00 UTC
	localMidnight := unixDay - 5*3600 - 1800 // 00:00 at +05:30
	for _, end := range []int64{unixDay, localMidnight} {
		client, ends := rangeQueryService(t)
		times, _, dropped := executed(t, client, windowedAttempt(t, end, 3*day, day, "Asia/Kolkata", ""))
		var want []int64
		for midnight := localMidnight - 3*day; midnight < end; midnight += day {
			if midnight >= end-3*day {
				want = append(want, midnight)
			}
		}
		if asked := ends(); len(asked) != 1 || asked[0] != end-1 {
			t.Fatalf("a window ending at %d asked to end at %v, want a second before", end, asked)
		}
		if fmt.Sprint(times) != fmt.Sprint(want) || dropped.OutsideRangeAfter != 0 {
			t.Fatalf("a window ending at %d took %v, let go %+v; want the local midnights %v and nothing after", end, times, dropped, want)
		}
	}
}

// A query not aligned - asked a step apart from its bucket, or off the grid -
// is held back: asked as its window ends, until the first readings.
func TestAnUnalignedQueryAsksAsItsWindowEnds(t *testing.T) {
	for name, attempt := range map[string]execution.QueryAttempt{"stepped": steppedAttempt(t), "unaligned": unalignedAttempt(t)} {
		end, outcome := requestEnd(attempt.Spec)
		if end != attempt.Spec.ProviderRange.End || outcome != observability.QueryRequestEndNotTimeAlign {
			t.Fatalf("%s: end %d (%s), want the window's %d, not_time_align", name, end, outcome, attempt.Spec.ProviderRange.End)
		}
	}
}

// A step that is not whole seconds has a grid whose points need not be
// whole seconds: asked as the window ends.
func TestAStepNotOfWholeSecondsAsksAsItsWindowEnds(t *testing.T) {
	attempt := windowedAttempt(t, 1_700_124_000, 300, 60, "UTC", "")
	attempt.Spec.PlanFacts.StepMillis = 1_500
	if end, outcome := requestEnd(attempt.Spec); end != 1_700_124_000 || outcome != observability.QueryRequestEndStepNotWholeSeconds {
		t.Fatalf("a 1.5 s step: end %d (%s), want the window's, step_not_whole_seconds", end, outcome)
	}
}

// A clause of logs, events, APM or bk_data - which the provider buckets
// [t, t+step) and filters by the range's end, Lte(end) - keeps its request's
// end: ending it a step earlier would cut the window's last bucket short. One
// such clause keeps the whole request's; so does a PromQL query, and a metric
// clause carrying a query string, a filter only a search store reads.
func TestASourceThatBucketsByTheRangesEndAsksAsItsWindowEnds(t *testing.T) {
	const end = int64(1_700_124_000)
	for _, source := range []string{"bklog", "bkapm", "bkdata", "custom"} {
		client, ends := rangeQueryService(t)
		var heard []string
		client.WithRequestEndObserver(func(outcome string) { heard = append(heard, outcome) })
		executed(t, client, windowedAttempt(t, end, 300, 60, "UTC", source))
		if asked := ends(); len(asked) != 1 || asked[0] != end {
			t.Fatalf("%s: asked to end at %v, want the window's own %d", source, asked, end)
		}
		if len(heard) != 1 || heard[0] != observability.QueryRequestEndSourceNotEligible {
			t.Fatalf("%s: heard %v, want source_not_eligible", source, heard)
		}
	}
	mixed := windowedAttempt(t, end, 300, 60, "UTC", "")
	facts := mixed.Spec.PlanFacts
	facts.QueryRevision = ""
	facts.QueryList = append(facts.QueryList, facts.QueryList[0])
	facts.QueryList[1].ReferenceName, facts.QueryList[1].DataSource = "b", "bklog"
	facts.MetricMerge = "a + b"
	rebuilt, err := execution.BuildQueryPlanFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	mixed.Spec.PlanFacts = rebuilt
	if got, outcome := requestEnd(mixed.Spec); got != end || outcome != observability.QueryRequestEndSourceNotEligible {
		t.Fatalf("a metric clause beside a log one: end %d (%s), want the window's %d", got, outcome, end)
	}
	filtered := windowedAttempt(t, end, 300, 60, "UTC", "bkmonitor")
	filteredFacts := filtered.Spec.PlanFacts
	filteredFacts.QueryRevision = ""
	filteredFacts.QueryList = append([]execution.QueryClause(nil), filteredFacts.QueryList...)
	filteredFacts.QueryList[0].QueryString = "*"
	if filtered.Spec.PlanFacts, err = execution.BuildQueryPlanFacts(filteredFacts); err != nil {
		t.Fatal(err)
	}
	if got, outcome := requestEnd(filtered.Spec); got != end || outcome != observability.QueryRequestEndSourceNotEligible {
		t.Fatalf("a metric clause with a query string: end %d (%s), want the window's %d", got, outcome, end)
	}
	promQL := mixed.Spec
	promQL.PlanFacts.PromQL = &execution.PromQLQuery{Expression: "sum(up)"}
	if got, outcome := requestEnd(promQL); got != end || outcome != observability.QueryRequestEndSourceNotEligible {
		t.Fatalf("a PromQL query: end %d (%s), want the window's %d", got, outcome, end)
	}
}
