// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package fleet

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type rangeCall struct {
	space, promQL string
	window, step  time.Duration
}

type stubRange struct {
	mu     sync.Mutex
	answer func(space, promQL string) (SeriesRange, error)
	calls  []rangeCall
}

func (stub *stubRange) Range(ctx context.Context, space, promQL string, start, end time.Time, step time.Duration) (SeriesRange, error) {
	stub.mu.Lock()
	stub.calls = append(stub.calls, rangeCall{space: space, promQL: promQL, window: end.Sub(start), step: step})
	stub.mu.Unlock()
	if stub.answer == nil {
		return SeriesRange{}, nil
	}
	answer, err := stub.answer(space, promQL)
	// As a real provider does: a context gone by the time it answers fails.
	if ctx.Err() != nil {
		return SeriesRange{}, ctx.Err()
	}
	return answer, err
}

func resolvedSpaces(context.Context) map[string]TrendSpace {
	return map[string]TrendSpace{
		TrendSpaceSelf:      {Space: "bkcc__2", State: TrendSpaceResolved},
		TrendSpaceContainer: {Space: "bkcc__3", State: TrendSpaceResolved},
	}
}

var trendTestScope = TrendScope{Namespace: "alarmd-ns", Container: "alarmd"}

func onePoint(value float64) []SeriesLine {
	return []SeriesLine{{Points: []SeriesPoint{{AtUnixMilli: 1789006260000, Value: value}}}}
}

func trendByKey(answer TrendsAnswer, key string) Trend {
	for _, trend := range answer.Trends {
		if trend.Key == key {
			return trend
		}
	}
	return Trend{}
}

// Each chart reads its own space with the expressions scoped by this
// deployment's namespace and container and the window's rate; a by-pod line
// is one line per pod, named with it; and the expression travels with each
// line.
func TestTrendsReadEachChartInItsSpaceScopedToThisDeployment(t *testing.T) {
	provider := &stubRange{answer: func(space, promQL string) (SeriesRange, error) {
		if strings.Contains(promQL, "container_cpu_usage_seconds_total") {
			return SeriesRange{Lines: []SeriesLine{
				{Labels: map[string]string{"pod": "alarmd-b"}, Points: []SeriesPoint{{AtUnixMilli: 1, Value: 0.05}}},
				{Labels: map[string]string{"pod": "alarmd-a"}, Points: []SeriesPoint{{AtUnixMilli: 1, Value: 0.04}}},
			}}, nil
		}
		return SeriesRange{Lines: onePoint(1)}, nil
	}}
	window, _ := lookupSeriesWindow("1h")
	answer := collectTrends(context.Background(), provider, resolvedSpaces(context.Background()), trendTestScope, window, time.Unix(1789006380, 0))
	if len(answer.Trends) != len(trendCatalog) || answer.RateWindow != "5m" || answer.Spaces[TrendSpaceSelf].Space != "bkcc__2" {
		t.Fatalf("answer = %+v", answer)
	}
	for _, call := range provider.calls {
		container := strings.Contains(call.promQL, "container_") || strings.Contains(call.promQL, "kube_pod_")
		if container && (call.space != "bkcc__3" || !strings.Contains(call.promQL, `namespace="alarmd-ns",container="alarmd"`)) {
			t.Errorf("container expression %s in %s, want bkcc__3 scoped to this deployment", call.promQL, call.space)
		}
		if !container && (call.space != "bkcc__2" || !strings.Contains(call.promQL, "bkmonitor_alarmd_")) {
			t.Errorf("self expression %s in %s, want bkcc__2 on alarmd's own metrics", call.promQL, call.space)
		}
		if strings.Contains(call.promQL, "rate(") && !strings.Contains(call.promQL, "[5m]") {
			t.Errorf("expression %s, want the 1h window's 5m rate", call.promQL)
		}
		if call.window != time.Hour || call.step != time.Minute {
			t.Errorf("window %v step %v, want 1h by 1m", call.window, call.step)
		}
	}
	cpu := trendByKey(answer, "cpu_cores")
	if len(cpu.Lines) < 2 || cpu.Lines[0].Pod != "alarmd-a" || cpu.Lines[0].Name != "使用 · alarmd-a" || cpu.Lines[1].Pod != "alarmd-b" ||
		!strings.Contains(cpu.Lines[0].PromQL, "container_cpu_usage_seconds_total") {
		t.Fatalf("cpu lines = %+v, want one per pod in pod order, named and with the expression", cpu.Lines)
	}
	failures := trendByKey(answer, "failures_per_minute")
	if len(failures.Lines) != 4 || failures.Lines[1].Name != "状态·准入未成功" || !strings.Contains(failures.Lines[1].PromQL, `result!="success"`) {
		t.Fatalf("failure lines = %+v, want the four stages", failures.Lines)
	}
}

// A container limit is read from kube-state-metrics in either generation's
// names: the resource-labelled family, else the per-resource one.
func TestAContainerLimitReadsEitherKubeStateMetricsGeneration(t *testing.T) {
	cpu := containerLimit("cpu", "kube_pod_container_resource_limits_cpu_cores")(trendTestScope, "5m")
	if !strings.Contains(cpu, `kube_pod_container_resource_limits{namespace="alarmd-ns",container="alarmd",resource="cpu"}`) ||
		!strings.Contains(cpu, ` or `) || !strings.Contains(cpu, `kube_pod_container_resource_limits_cpu_cores{namespace="alarmd-ns",container="alarmd"}`) {
		t.Fatalf("cpu limit = %s", cpu)
	}
}

// A chart whose space is unresolved says so with the reason and reads
// nothing; one read with nothing in the window says it has no series; one
// the provider refused or failed is reported on that chart alone.
func TestEachChartSaysWhyItHasNoLines(t *testing.T) {
	spaces := map[string]TrendSpace{
		TrendSpaceSelf:      {Space: "bkcc__2", State: TrendSpaceResolved},
		TrendSpaceContainer: {State: TrendSpaceUnresolved, Reason: "no candidate space has samples"},
	}
	provider := &stubRange{answer: func(_, promQL string) (SeriesRange, error) {
		switch {
		case strings.Contains(promQL, "slot_decision_lag"):
			return SeriesRange{}, errors.New("provider is down")
		case strings.Contains(promQL, "worker_work_total"):
			return SeriesRange{Code: "SPACE_TABLE_ID_FIELD_IS_NOT_EXISTS", Message: "field is empty"}, nil
		case strings.Contains(promQL, "access_query_failure_total"):
			return SeriesRange{Lines: onePoint(0)}, nil
		}
		return SeriesRange{}, nil
	}}
	window, _ := lookupSeriesWindow("")
	answer := collectTrends(context.Background(), provider, spaces, trendTestScope, window, time.Unix(1789006380, 0))
	if lag := trendByKey(answer, "decision_lag_p95"); lag.Unavailable != "PROVIDER_ERROR" || lag.Detail != "provider is down" || len(lag.Lines) != 0 {
		t.Errorf("lag = %+v, want the provider's failure", lag)
	}
	if queries := trendByKey(answer, "queries_per_second"); queries.Unavailable != "SPACE_TABLE_ID_FIELD_IS_NOT_EXISTS" || queries.NoSeries {
		t.Errorf("queries = %+v, want the refusal, not silence", queries)
	}
	// One stage has a point: the chart has series, the empty stages included.
	if failures := trendByKey(answer, "failures_per_minute"); failures.Unavailable != "" || failures.NoSeries || len(failures.Lines) != 4 {
		t.Errorf("failures = %+v, want read with its four lines", failures)
	}
	for _, key := range []string{"cpu_cores", "memory_bytes"} {
		if trend := trendByKey(answer, key); trend.Unavailable != TrendSpaceUnresolved || trend.Detail != "no candidate space has samples" {
			t.Errorf("%s = %+v, want space_unresolved with the reason", key, trend)
		}
	}
	for _, call := range provider.calls {
		if strings.Contains(call.promQL, "container_") {
			t.Errorf("a container expression was sent with its space unresolved: %s", call.promQL)
		}
	}
	quiet := collectTrends(context.Background(), &stubRange{}, resolvedSpaces(context.Background()), trendTestScope, window, time.Unix(1789006380, 0))
	if trend := trendByKey(quiet, "queries_per_second"); !trend.NoSeries || trend.Unavailable != "" || trend.Detail != "" {
		t.Errorf("an empty window = %+v, want no_series", trend)
	}
	// A resolved container space holds the namespace's container data (that
	// is what resolved it); none for alarmd's container is said as seen, with
	// the node, and the collector not covering it as the likely cause.
	scope := trendTestScope
	scope.Node = "node-7"
	quiet = collectTrends(context.Background(), &stubRange{}, resolvedSpaces(context.Background()), scope, window, time.Unix(1789006380, 0))
	for _, key := range []string{"cpu_cores", "memory_bytes"} {
		trend := trendByKey(quiet, key)
		if !trend.NoSeries || trend.Unavailable != "" || trend.Detail != `space bkcc__3 has container data for namespace "alarmd-ns" but no series for container "alarmd" on node node-7; most likely the collector does not cover that node` {
			t.Errorf("%s with no alarmd series = %+v", key, trend)
		}
	}
	// Without a known node the sentence names none.
	unknown := collectTrends(context.Background(), &stubRange{}, resolvedSpaces(context.Background()), trendTestScope, window, time.Unix(1789006380, 0))
	if cpu := trendByKey(unknown, "cpu_cores"); cpu.Detail != `space bkcc__3 has container data for namespace "alarmd-ns" but no series for container "alarmd"; most likely the collector does not cover the node this replica runs on` {
		t.Errorf("cpu with no known node = %+v", cpu)
	}
	// A read takes the node from the deployment's Node at read time.
	trends := &Trends{Range: &stubRange{}, Spaces: resolvedSpaces, Scope: trendTestScope, Node: func(context.Context) string { return "node-9" }}
	if memory := trendByKey(trends.read(context.Background(), window, time.Unix(1789006380, 0)), "memory_bytes"); !strings.Contains(memory.Detail, "on node node-9;") {
		t.Errorf("a read's memory chart = %+v, want the node it was told", memory)
	}
}

// The window is chosen from a fixed list, and the rate spans four steps and
// never less than five minutes, so it always covers several scrapes.
func TestTheWindowComesFromAFixedListWithItsRate(t *testing.T) {
	for key, rate := range map[string]string{"1h": "5m", "6h": "20m", "24h": "40m"} {
		window, ok := lookupSeriesWindow(key)
		if !ok || window.rate() != rate {
			t.Errorf("%s: rate %s, want %s", key, window.rate(), rate)
		}
	}
	trends := &Trends{Range: &stubRange{}, Spaces: resolvedSpaces, Scope: trendTestScope}
	status, body := get(t, trendsHandler(t, trends), "/api/series?window=6h")
	if status != http.StatusOK || body["window"] != "6h" || body["rate_window"] != "20m" {
		t.Fatalf("status %d body %v", status, body)
	}
	if status, _ := get(t, trendsHandler(t, trends), "/api/series?window=90d"); status != http.StatusBadRequest {
		t.Fatalf("an unlisted window returned %d", status)
	}
}

// One read of a window answers for TrendsMaxAge: a burst of page refreshes
// costs the query backend one query per line.
func TestOneReadOfAWindowAnswersForItsMaxAge(t *testing.T) {
	provider := &stubRange{}
	trends := &Trends{Range: provider, Spaces: resolvedSpaces, Scope: trendTestScope}
	window, _ := lookupSeriesWindow("1h")
	at := time.Unix(1789006380, 0)
	trends.read(context.Background(), window, at)
	once := len(provider.calls)
	trends.read(context.Background(), window, at.Add(TrendsMaxAge-time.Second))
	if len(provider.calls) != once {
		t.Fatalf("a read within the max age queried again: %d calls, want %d", len(provider.calls), once)
	}
	trends.read(context.Background(), window, at.Add(TrendsMaxAge))
	if len(provider.calls) != 2*once {
		t.Fatalf("a read past the max age: %d calls, want %d", len(provider.calls), 2*once)
	}
}

func trendsHandler(t *testing.T, trends *Trends) http.Handler {
	t.Helper()
	service := mustService(t, stubExpectations{expectation: Expectation{QueryGroups: 949, Known: true}},
		stubRegistry{replicas: replicas()}, stubSnapshots{snapshots: healthySnapshots()})
	handler, err := NewHandler(service, nil, func() time.Time { return now }, 0, trends, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

// Without a query client there is nothing to read the trends with, and the
// route is absent rather than mounted and empty.
func TestWithoutTrendsTheSeriesRouteIsAbsent(t *testing.T) {
	response := httptest.NewRecorder()
	trendsHandler(t, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/series", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the route to be absent", response.Code)
	}
}

// A chart is read whole or not at all: when a later line fails, the lines
// read before it are not shown as if they were the chart.
func TestAChartWithOneFailingLineShowsNoneOfItsLines(t *testing.T) {
	provider := &stubRange{answer: func(_, promQL string) (SeriesRange, error) {
		if strings.Contains(promQL, "state_version_conflict_total") {
			return SeriesRange{}, errors.New("provider is down")
		}
		return SeriesRange{Lines: onePoint(1)}, nil
	}}
	window, _ := lookupSeriesWindow("1h")
	answer := collectTrends(context.Background(), provider, resolvedSpaces(context.Background()), trendTestScope, window, time.Unix(1789006380, 0))
	if failures := trendByKey(answer, "failures_per_minute"); failures.Unavailable != "PROVIDER_ERROR" || len(failures.Lines) != 0 {
		t.Fatalf("failures = %+v, want unavailable with no lines", failures)
	}
}

// Callers arriving together after the max age share one read: the query
// backend sees each line once, not once per caller.
func TestConcurrentReadsOfAWindowShareOneRead(t *testing.T) {
	release := make(chan struct{})
	provider := &stubRange{answer: func(string, string) (SeriesRange, error) {
		<-release
		return SeriesRange{Lines: onePoint(1)}, nil
	}}
	trends := &Trends{Range: provider, Spaces: resolvedSpaces, Scope: trendTestScope}
	window, _ := lookupSeriesWindow("1h")
	at := time.Unix(1789006380, 0)
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			trends.read(context.Background(), window, at)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	group.Wait()
	lines := 0
	for _, definition := range trendCatalog {
		lines += len(definition.Lines)
	}
	if len(provider.calls) != lines {
		t.Fatalf("%d calls for four callers, want one read's %d", len(provider.calls), lines)
	}
}

// A caller that leaves while the shared read runs gets an empty answer at
// once, and the read goes on for the others on its own context.
func TestACallerLeavingDoesNotCancelTheSharedRead(t *testing.T) {
	release := make(chan struct{})
	provider := &stubRange{answer: func(string, string) (SeriesRange, error) {
		<-release
		return SeriesRange{Lines: onePoint(1)}, nil
	}}
	trends := &Trends{Range: provider, Spaces: resolvedSpaces, Scope: trendTestScope}
	window, _ := lookupSeriesWindow("1h")
	at := time.Unix(1789006380, 0)
	first := make(chan TrendsAnswer, 1)
	go func() { first <- trends.read(context.Background(), window, at) }()
	time.Sleep(20 * time.Millisecond)
	leaving, cancel := context.WithCancel(context.Background())
	cancel()
	if left := trends.read(leaving, window, at); len(left.Trends) != 0 {
		t.Fatalf("the leaving caller got %d charts, want an empty answer", len(left.Trends))
	}
	close(release)
	if answer := <-first; len(answer.Trends) != len(trendCatalog) || answer.Trends[1].NoSeries {
		t.Fatalf("the shared read = %+v, want every chart read", answer)
	}
}

// The caller that started the shared read may leave too: the read runs on
// its own context, and the callers still waiting get every chart.
func TestTheStartingCallerLeavingDoesNotCancelTheSharedRead(t *testing.T) {
	release := make(chan struct{})
	provider := &stubRange{answer: func(string, string) (SeriesRange, error) {
		<-release
		return SeriesRange{Lines: onePoint(1)}, nil
	}}
	trends := &Trends{Range: provider, Spaces: resolvedSpaces, Scope: trendTestScope}
	window, _ := lookupSeriesWindow("1h")
	at := time.Unix(1789006380, 0)
	starting, cancel := context.WithCancel(context.Background())
	go trends.read(starting, window, at)
	time.Sleep(20 * time.Millisecond)
	waiting := make(chan TrendsAnswer, 1)
	go func() { waiting <- trends.read(context.Background(), window, at) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(release)
	answer := <-waiting
	for _, trend := range answer.Trends {
		if trend.Unavailable != "" {
			t.Fatalf("chart %s = %+v, want read: the starting caller's leaving cancelled the shared read", trend.Key, trend)
		}
	}
}
