// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// The trends: this deployment's own numbers over time, read back from the
// platform's collection of them -- its ServiceMonitor's metrics and the
// cluster's container metrics -- through the query backend the evaluation
// already uses. The expressions are fixed here; a caller picks a window
// from a fixed list and nothing else.

// SeriesPoint is one sample of one line.
type SeriesPoint struct {
	AtUnixMilli int64   `json:"at"`
	Value       float64 `json:"value"`
}

// SeriesLine is one series of an answer, with the labels the expression
// grouped it by.
type SeriesLine struct {
	Labels map[string]string `json:"labels,omitempty"`
	Points []SeriesPoint     `json:"points"`
}

// SeriesRange is what a provider returns for one expression.
type SeriesRange struct {
	Lines []SeriesLine
	// Code is the provider's own classification when it answered without
	// samples. Empty means the window really was empty.
	Code    string
	Message string
	Partial bool
}

// RangeProvider answers one fixed expression in one space over a window.
type RangeProvider interface {
	Range(ctx context.Context, space, promQL string, start, end time.Time, step time.Duration) (SeriesRange, error)
}

// The two spaces a trend reads from: where the platform files this
// deployment's own metrics (its ServiceMonitor's), and where it files the
// cluster's container metrics.
const (
	TrendSpaceSelf      = "self"
	TrendSpaceContainer = "container"
)

// TrendSpace is one space as the deployment derived it. State is resolved
// with Space set, or unresolved with Reason saying why and Candidates the
// spaces tried.
type TrendSpace struct {
	Space      string   `json:"space,omitempty"`
	State      string   `json:"state"`
	Reason     string   `json:"reason,omitempty"`
	Candidates []string `json:"candidates,omitempty"`
}

// The space states, closed.
const (
	TrendSpaceResolved   = "resolved"
	TrendSpaceUnresolved = "space_unresolved"
)

// TrendScope is what the container expressions select by: the namespace
// this deployment's pods run in and its container's name, both its own.
type TrendScope struct {
	Namespace string
	Container string
	// Node is the node this replica runs on, when known: a container chart
	// with no series names it, since the collector not covering it is why.
	Node string
}

// TrendLine is one line of a chart: its name and its expression. ByPod
// lines are one series per pod, named with the pod.
type TrendLine struct {
	Name   string
	PromQL func(scope TrendScope, rate string) string
	ByPod  bool
	// Fallback, when set, is asked when PromQL is refused or has no series:
	// the same quantity under the names another exporter generation uses.
	// They are asked apart, since the query backend refuses a whole
	// expression that names a field the space does not have.
	Fallback func(scope TrendScope, rate string) string
}

// TrendDefinition is one chart: what it shows, in which unit, from which
// space, and what a reader concludes from it.
type TrendDefinition struct {
	Key   string
	Title string
	Unit  string
	Space string
	Help  string
	Lines []TrendLine
}

const metricPrefix = "alarmd_"

// SelfSpaceProbe is the expression whose samples say a space holds alarmd's
// own metrics.
const SelfSpaceProbe = "count(" + metricPrefix + "build_info)"

// trendCatalog is fixed at build time. The alarmd counters here are each
// process's own work -- a physical query, a decision, an admission is
// counted once, by the process that did it -- so the replicas add up.
var trendCatalog = []TrendDefinition{
	{
		Key: "decision_lag_p95", Title: "判定延迟 P95", Unit: "seconds", Space: TrendSpaceSelf,
		Help: "slot_decision_lag_seconds 的 P95：从数据可读（检测时刻加读取等待）到判定完成确认；与总览判定段的 P95 同一口径。直方图最后一个桶是 300 秒，超过时画 300。",
		Lines: []TrendLine{{Name: "P95", PromQL: func(_ TrendScope, rate string) string {
			return fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(%sslot_decision_lag_seconds_bucket[%s])))`, metricPrefix, rate)
		}}},
	},
	{
		Key: "queries_per_second", Title: "每秒查询数", Unit: "query/s", Space: TrendSpaceSelf,
		Help: "worker_work_total{work_kind=\"physical_query\"} 的速率：合并后实际发往统一查询的物理查询，全部副本合计。",
		Lines: []TrendLine{{Name: "物理查询", PromQL: func(_ TrendScope, rate string) string {
			return fmt.Sprintf(`sum(rate(%sworker_work_total{work_kind="physical_query"}[%s]))`, metricPrefix, rate)
		}}},
	},
	{
		Key: "failures_per_minute", Title: "各段每分钟失败数", Unit: "failure/min", Space: TrendSpaceSelf,
		Help: "拉取：access_query_failure_total；状态：state_admission_total 中非 success 的准入与 state_version_conflict_total；输出：output_events_rejected_all_total。判定段的失败是调度普查里此刻迟到和超期的检测，不是计数，不在图上。",
		Lines: []TrendLine{
			{Name: "拉取", PromQL: perMinute("access_query_failure_total", "")},
			{Name: "状态·准入未成功", PromQL: perMinute("state_admission_total", `result!="success"`)},
			{Name: "状态·版本冲突", PromQL: perMinute("state_version_conflict_total", "")},
			{Name: "输出", PromQL: perMinute("output_events_rejected_all_total", "")},
		},
	},
	{
		Key: "cpu_cores", Title: "CPU 与上限", Unit: "cores", Space: TrendSpaceContainer,
		Help: "容器监控的 container_cpu_usage_seconds_total 速率，按副本；上限取 kube-state-metrics 的容器 CPU limit。持续贴近上限说明在被限流。",
		Lines: []TrendLine{
			{Name: "使用", ByPod: true, PromQL: func(scope TrendScope, rate string) string {
				return fmt.Sprintf(`sum by (pod) (rate(container_cpu_usage_seconds_total{%s}[%s]))`, scope.selector(), rate)
			}},
			containerLimit("cpu", "kube_pod_container_resource_limits_cpu_cores"),
		},
	},
	{
		Key: "memory_bytes", Title: "内存工作集与上限", Unit: "bytes", Space: TrendSpaceContainer,
		Help: "容器监控的 container_memory_working_set_bytes，按副本；上限取 kube-state-metrics 的容器 memory limit。工作集到上限会被 OOM 杀掉。",
		Lines: []TrendLine{
			{Name: "工作集", ByPod: true, PromQL: func(scope TrendScope, _ string) string {
				return fmt.Sprintf(`max by (pod) (container_memory_working_set_bytes{%s})`, scope.selector())
			}},
			containerLimit("memory", "kube_pod_container_resource_limits_memory_bytes"),
		},
	},
}

func perMinute(name, matcher string) func(TrendScope, string) string {
	return func(_ TrendScope, rate string) string {
		selector := metricPrefix + name
		if matcher != "" {
			selector += "{" + matcher + "}"
		}
		return fmt.Sprintf(`60 * sum(rate(%s[%s]))`, selector, rate)
	}
}

// containerLimit is the limit line: a container's limit as kube-state-metrics
// publishes it, the resource-labelled family of version 2, else the
// per-resource family of version 1, whichever this space has.
func containerLimit(resource, legacy string) TrendLine {
	return TrendLine{Name: "上限", ByPod: true,
		PromQL: func(scope TrendScope, _ string) string {
			return fmt.Sprintf(`max by (pod) (kube_pod_container_resource_limits{%s,resource=%q})`, scope.selector(), resource)
		},
		Fallback: func(scope TrendScope, _ string) string {
			return fmt.Sprintf(`max by (pod) (%s{%s})`, legacy, scope.selector())
		}}
}

func (scope TrendScope) selector() string {
	return fmt.Sprintf(`namespace=%q,container=%q`, scope.Namespace, scope.Container)
}

// SeriesWindow is a bounded choice of range and resolution, and the rate
// window the expressions use: four steps and never under five minutes, so
// a rate always spans several scrapes whatever the collection's interval.
type SeriesWindow struct {
	Key      string
	Duration time.Duration
	Step     time.Duration
}

var seriesWindows = []SeriesWindow{
	{Key: "1h", Duration: time.Hour, Step: time.Minute},
	{Key: "6h", Duration: 6 * time.Hour, Step: 5 * time.Minute},
	{Key: "24h", Duration: 24 * time.Hour, Step: 10 * time.Minute},
}

// DefaultSeriesWindow is what a reader gets without choosing.
const DefaultSeriesWindow = "1h"

func (window SeriesWindow) rate() string {
	rate := 4 * window.Step
	if rate < 5*time.Minute {
		rate = 5 * time.Minute
	}
	return fmt.Sprintf("%dm", int(rate/time.Minute))
}

func lookupSeriesWindow(key string) (SeriesWindow, bool) {
	if key == "" {
		key = DefaultSeriesWindow
	}
	for _, window := range seriesWindows {
		if window.Key == key {
			return window, true
		}
	}
	return SeriesWindow{}, false
}

// TrendLineAnswer is one line as a reader receives it: its name (with the
// pod for a by-pod line), the expression, and its samples.
type TrendLineAnswer struct {
	Name   string        `json:"name"`
	Pod    string        `json:"pod,omitempty"`
	PromQL string        `json:"promql"`
	Points []SeriesPoint `json:"points"`
	// Unavailable says this line could not be read, with Detail why; the
	// chart's other lines are drawn.
	Unavailable string `json:"unavailable,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

// Trend is one chart as a reader receives it. Unavailable says the chart
// could not be read -- its space is unresolved, or the provider declined or
// failed -- with Detail why; NoSeries that it was read and the collection
// has nothing for it in the window, which is its own answer: the metrics
// are not being collected.
type Trend struct {
	Key         string            `json:"key"`
	Title       string            `json:"title"`
	Unit        string            `json:"unit"`
	Space       string            `json:"space"`
	Help        string            `json:"help"`
	Lines       []TrendLineAnswer `json:"lines"`
	Unavailable string            `json:"unavailable,omitempty"`
	Detail      string            `json:"detail,omitempty"`
	NoSeries    bool              `json:"no_series,omitempty"`
	Partial     bool              `json:"partial,omitempty"`
}

// TrendsAnswer is the whole read: the window, when it was read, the two
// spaces as derived, and the charts.
type TrendsAnswer struct {
	Window       string                `json:"window"`
	WindowChoice []string              `json:"window_choices"`
	StartUnixMs  int64                 `json:"start"`
	EndUnixMs    int64                 `json:"end"`
	StepSeconds  int                   `json:"step_seconds"`
	RateWindow   string                `json:"rate_window"`
	ReadAt       time.Time             `json:"read_at"`
	Spaces       map[string]TrendSpace `json:"spaces"`
	Trends       []Trend               `json:"trends"`
}

// Trends reads the catalog: through Range, in the spaces Spaces derives,
// scoped by Scope. One answer per window serves for TrendsMaxAge.
type Trends struct {
	Range  RangeProvider
	Spaces func(ctx context.Context) map[string]TrendSpace
	Scope  TrendScope
	// Node says the node this replica runs on, or "" when it cannot.
	Node func(ctx context.Context) string

	mu       sync.Mutex
	answers  map[string]TrendsAnswer
	inFlight map[string]*trendsFlight
}

// trendsFlight is one window's read in progress; callers arriving while it
// runs wait for it rather than reading again.
type trendsFlight struct {
	done   chan struct{}
	answer TrendsAnswer
}

// trendsComputeTimeout bounds one shared read: it runs on its own context,
// since the callers it answers share it and any one of them may leave.
const trendsComputeTimeout = 45 * time.Second

// TrendsMaxAge is how long one read of a window answers: a page refresh
// costs the query backend one query per line at most this often.
const TrendsMaxAge = 30 * time.Second

func (trends *Trends) read(ctx context.Context, window SeriesWindow, at time.Time) TrendsAnswer {
	trends.mu.Lock()
	if cached, found := trends.answers[window.Key]; found && at.Sub(cached.ReadAt) < TrendsMaxAge {
		trends.mu.Unlock()
		return cached
	}
	if flight, running := trends.inFlight[window.Key]; running {
		trends.mu.Unlock()
		select {
		case <-flight.done:
			return flight.answer
		case <-ctx.Done():
			return TrendsAnswer{Window: window.Key, WindowChoice: seriesWindowKeys(), ReadAt: at.UTC(), Trends: []Trend{}}
		}
	}
	if trends.inFlight == nil {
		trends.inFlight, trends.answers = map[string]*trendsFlight{}, map[string]TrendsAnswer{}
	}
	flight := &trendsFlight{done: make(chan struct{})}
	trends.inFlight[window.Key] = flight
	trends.mu.Unlock()
	defer func() {
		trends.mu.Lock()
		trends.answers[window.Key] = flight.answer
		delete(trends.inFlight, window.Key)
		trends.mu.Unlock()
		close(flight.done)
	}()
	shared, cancel := context.WithTimeout(context.Background(), trendsComputeTimeout)
	defer cancel()
	scope := trends.Scope
	if trends.Node != nil {
		scope.Node = trends.Node(shared)
	}
	flight.answer = collectTrends(shared, trends.Range, trends.Spaces(shared), scope, window, at)
	return flight.answer
}

// collectTrends runs the catalog over one window. One failing chart is
// reported on that chart and nowhere else.
func collectTrends(ctx context.Context, provider RangeProvider, spaces map[string]TrendSpace, scope TrendScope,
	window SeriesWindow, at time.Time,
) TrendsAnswer {
	start := at.Add(-window.Duration)
	answer := TrendsAnswer{Window: window.Key, WindowChoice: seriesWindowKeys(), StartUnixMs: start.UnixMilli(), EndUnixMs: at.UnixMilli(),
		StepSeconds: int(window.Step / time.Second), RateWindow: window.rate(), ReadAt: at.UTC(), Spaces: spaces,
		Trends: make([]Trend, 0, len(trendCatalog))}
	for _, definition := range trendCatalog {
		trend := Trend{Key: definition.Key, Title: definition.Title, Unit: definition.Unit, Space: definition.Space, Help: definition.Help,
			Lines: []TrendLineAnswer{}}
		space := spaces[definition.Space]
		if space.State != TrendSpaceResolved {
			trend.Unavailable, trend.Detail = TrendSpaceUnresolved, space.Reason
			answer.Trends = append(answer.Trends, trend)
			continue
		}
		points, failed := 0, 0
		var firstCode, firstDetail string
		for _, line := range definition.Lines {
			answers, partial, code, detail := readLine(ctx, provider, space.Space, line, scope, window, start, at)
			if code != "" {
				failed++
				if firstCode == "" {
					firstCode, firstDetail = code, detail
				}
				trend.Lines = append(trend.Lines, TrendLineAnswer{Name: line.Name, PromQL: line.PromQL(scope, window.rate()),
					Points: []SeriesPoint{}, Unavailable: code, Detail: detail})
				continue
			}
			trend.Partial = trend.Partial || partial
			for _, kept := range answers {
				points += len(kept.Points)
			}
			trend.Lines = append(trend.Lines, answers...)
		}
		// One line that cannot be read is said on that line; the chart is
		// unavailable only when none of its lines could be read.
		if failed == len(definition.Lines) {
			trend.Unavailable, trend.Detail = firstCode, firstDetail
			trend.Lines = []TrendLineAnswer{}
		} else if points == 0 {
			trend.NoSeries = true
			// The container space resolved on the namespace's container data,
			// so what is seen is that data present and none for this
			// deployment's container; the collector not reaching its node is
			// the likely cause, not an observed one (a replica younger than
			// the window, or a relabel rule, look the same).
			if definition.Space == TrendSpaceContainer {
				where, cause := "", "the collector does not cover the node this replica runs on"
				if scope.Node != "" {
					where, cause = " on node "+scope.Node, "the collector does not cover that node"
				}
				trend.Detail = fmt.Sprintf("space %s has container data for namespace %q but no series for container %q%s; most likely %s",
					space.Space, scope.Namespace, scope.Container, where, cause)
			}
		}
		answer.Trends = append(answer.Trends, trend)
	}
	return answer
}

// readLine reads one line: its expression, then its fallback when that is
// refused or has no series. The first with series is the line; one that was
// read and is empty is an empty line; when every expression was refused or
// failed, the code and words of the last say why.
func readLine(ctx context.Context, provider RangeProvider, space string, line TrendLine, scope TrendScope, window SeriesWindow,
	start, at time.Time,
) (answers []TrendLineAnswer, partial bool, code, detail string) {
	expressions := []string{line.PromQL(scope, window.rate())}
	if line.Fallback != nil {
		expressions = append(expressions, line.Fallback(scope, window.rate()))
	}
	var empty []TrendLineAnswer
	for _, promQL := range expressions {
		result, err := provider.Range(ctx, space, promQL, start, at, window.Step)
		switch {
		case err != nil:
			code, detail = "PROVIDER_ERROR", err.Error()
			continue
		case result.Code != "":
			code, detail = result.Code, result.Message
			continue
		}
		read := linesOf(line, promQL, result.Lines)
		for _, kept := range read {
			if len(kept.Points) > 0 {
				return read, result.Partial, "", ""
			}
		}
		if empty == nil {
			empty, partial = read, result.Partial
		}
	}
	if empty != nil {
		return empty, partial, "", ""
	}
	return nil, false, code, detail
}

// linesOf names a line's series: by pod, one line per pod in pod order;
// otherwise the one line, every series' samples together.
func linesOf(line TrendLine, promQL string, series []SeriesLine) []TrendLineAnswer {
	if !line.ByPod {
		kept := TrendLineAnswer{Name: line.Name, PromQL: promQL, Points: []SeriesPoint{}}
		for _, one := range series {
			kept.Points = append(kept.Points, one.Points...)
		}
		return []TrendLineAnswer{kept}
	}
	answers := make([]TrendLineAnswer, 0, len(series))
	for _, one := range series {
		pod := one.Labels["pod"]
		answers = append(answers, TrendLineAnswer{Name: strings.TrimSpace(line.Name + " · " + pod), Pod: pod, PromQL: promQL,
			Points: append([]SeriesPoint{}, one.Points...)})
	}
	sort.Slice(answers, func(i, j int) bool { return answers[i].Pod < answers[j].Pod })
	return answers
}

func seriesWindowKeys() []string {
	keys := make([]string, 0, len(seriesWindows))
	for _, window := range seriesWindows {
		keys = append(keys, window.Key)
	}
	return keys
}
