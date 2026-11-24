// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// The flow's own counts: what the overview's four stages read, kept as the
// same counters Prometheus scrapes, so the overview's rate over two of its
// computations and a chart over the scrape are one number.

// QueryDurationBuckets reach the 30-second query cut, with a bucket at 20
// so the slow groups between 17 and 30 seconds are told apart.
var QueryDurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30}

// QueryAnswerBuckets reach past the provider's 30 s backend deadline to a
// minute: a one-minute round's query window runs to about 45 s.
var QueryAnswerBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 25, 30, 35, 40, 45, 60}

// QueryAnswerSeriesBuckets is the bounds of a physical query's answered
// series: 0 first, so an answer with no series is told apart from one with a
// single series, then powers of 4 from 1.
var QueryAnswerSeriesBuckets = append([]float64{0}, prometheus.ExponentialBuckets(1, 4, 11)...)

// DecisionLagBuckets reach five minutes: a held round lags tens of seconds
// and a long-period Slot minutes. They are dense between 10 and 60 seconds
// because that is where decisions land: one deployment's 5220 decisions all
// fell in (10, 30], mean 15.8 s, and with only 10 and 30 there the P95 came
// out as 10 + 20 x 0.95 = 29 s by construction, whatever the lag was.
var DecisionLagBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 15, 20, 25, 30, 40, 50, 60, 120, 300}

// queryFailureReasons is access.QueryFailures, held here because this
// package does not import access; a test in the wiring holds the two
// equal.
var queryFailureReasons = []string{"timeout", "refused", "not_found", "transport", "decode", "other", "sample_limit"}

type flowMetrics struct {
	physicalQueries prometheus.Counter
	stateApplied    prometheus.Counter
	queryDuration   prometheus.Histogram
	// The answers of the same queries, by how each ended (outcome): what
	// came back and how long it took.
	answerBytes, answerSeries, answerRecords, answerSeconds *prometheus.HistogramVec
	// answerPhaseSeconds splits an answered query's seconds by phase.
	answerPhaseSeconds *prometheus.HistogramVec
	// sharedFrameWait is what the two sides of shared-schema answers' frames
	// waited for each other, by the side waited on.
	sharedFrameWait *prometheus.CounterVec
	queryFailures   *prometheus.CounterVec
	failures        map[string]prometheus.Counter
	// pointsDropped is the points the answers' normalization let go, by
	// reason, each reason built at zero.
	pointsDropped *prometheus.CounterVec
	// requestEnd is the requests sent, by how their end was asked.
	requestEnd        *prometheus.CounterVec
	decisionLag       prometheus.Histogram
	outputRejectedAll prometheus.Counter
	// backend is the query backend's last answer and last failure, the
	// dependency entry's health for the role (QueryBackendHealth).
	backend *queryBackendBook
}

// QueryBackendHealth is when a physical query last answered and last failed,
// and the failure's closed word (access_query_failure_total's reason).
type QueryBackendHealth struct {
	LastSuccessAt time.Time
	LastFailureAt time.Time
	LastFailure   string
}

type queryBackendBook struct {
	mu     sync.Mutex
	health QueryBackendHealth
	known  bool
}

func newFlowMetrics(work *prometheus.CounterVec) flowMetrics {
	m := flowMetrics{
		physicalQueries: work.WithLabelValues("physical_query"),
		stateApplied:    work.WithLabelValues("state_apply"),
		queryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Subsystem: metricSubsystem, Name: "access_query_duration_seconds",
			Help: "Each physical query a Slot runs, every attempt including retries, by the time its provider took from " +
				"request to answer or failure. Counted where worker_work_total{work_kind=\"physical_query\"} is, so the " +
				"two agree; diagnostic reads are not in it. A query cut at 30 seconds lands in +Inf.",
			Buckets: QueryDurationBuckets,
		}),
		queryFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "access_query_failure_total",
			Help: "Physical queries that did not answer, by what their last failed attempt says: timeout, refused (the " +
				"backend answered no), not_found (its status names something that does not exist), transport, decode " +
				"(the answer broke the wire contract), other, sample_limit (the backend refused the query as reading more " +
				"samples than it serves in one query, VM_SAMPLE_LIMIT; which Query Group it was is on the fleet's page). " +
				"Closed set.",
		}, []string{"reason"}),
		decisionLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Subsystem: metricSubsystem, Name: "slot_decision_lag_seconds",
			Help: "Seconds from a Slot's data being readable (its evaluation time plus the read hold) to its completion " +
				"being acknowledged, for every period, in normal rounds: a normal Slot, its retry, and a replay short of " +
				"time - sent before its deadline for a read that would not fit, the normal round in its place. Other " +
				"replays, probes and supplements are late by what they are and are not in it; skipped and snapshot-unavailable completions " +
				"are not decisions and are not in it either.",
			Buckets: DecisionLagBuckets,
		}),
		outputRejectedAll: prometheus.NewCounter(prometheus.CounterOpts{
			Subsystem: metricSubsystem, Name: "output_events_rejected_all_total",
			Help: "Events refused on their way to the wire, all rules and strategies: output_events_rejected_total " +
				"without its labels, counted at the same place.",
		}),
	}
	answer := func(name, help string, buckets []float64) *prometheus.HistogramVec {
		vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{Subsystem: metricSubsystem, Name: name, Help: help,
			Buckets: buckets}, []string{"outcome"})
		for _, outcome := range observability.PhysicalQueryOutcomes {
			vec.WithLabelValues(outcome)
		}
		return vec
	}
	const outcomes = " By outcome: ok, timeout_alarmd_deadline (alarmd's own deadline: the round's, its transport, " +
		"its body read or its delivery), upstream_504 (the provider answered that its backend ran out of time, " +
		"504 or 408), unavailable (no usable answer for another reason) and rejected (the provider refused the " +
		"query). Counted where access_query_duration_seconds is."
	m.answerBytes = answer("access_query_answer_bytes", "Bytes of each physical query's answer as read."+outcomes,
		prometheus.ExponentialBuckets(1<<10, 4, 12))
	m.answerSeries = answer("access_query_answer_series", "Series in each physical query's answer."+outcomes,
		QueryAnswerSeriesBuckets)
	m.answerRecords = answer("access_query_answer_records", "Records (points) in each physical query's answer."+outcomes,
		prometheus.ExponentialBuckets(1, 4, 14))
	m.answerSeconds = answer("access_query_answer_seconds", "Seconds each physical query's provider took, to 60 s: "+
		"a query read past 30 s is in it, as its round's window on a one-minute period runs to about 45."+outcomes,
		QueryAnswerBuckets)
	m.answerPhaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{Subsystem: metricSubsystem,
		Name: "access_query_answer_phase_seconds",
		Help: "Seconds of each physical query whose response's headers came back, by its outcome (as answer_seconds) and " +
			"phase: wait, until the headers - the query service writes them once the query is computed; body, after them - " +
			"the answer carried, read and decoded (a shared-schema answer's encoding past its first frame among it). Within " +
			"one outcome a query's two phases add up to its answer_seconds; a query no response came back to is in neither.",
		Buckets: QueryAnswerBuckets}, []string{"outcome", "phase"})
	for _, outcome := range observability.PhysicalQueryOutcomes {
		for _, phase := range QueryAnswerPhases {
			m.answerPhaseSeconds.WithLabelValues(outcome, phase)
		}
	}
	m.sharedFrameWait = prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem,
		Name: "access_shared_frame_wait_seconds_total",
		Help: "Seconds the two sides of shared-schema answers' frames waited for each other, over the answers decoded to " +
			"their end, by the side waited on: consumer, the frame reader with a frame read and parsed waiting for the Slot " +
			"to take it - the Slot handing series on is the slower, and a faster decode leaves its time as it is; reader, " +
			"the Slot waiting for the reader's next frame - the answer's reading and decoding is on the Slot's path. Read " +
			"against access_query_answer_phase_seconds{phase=\"body\"}. An answer that failed is in neither."}, []string{"side"})
	for _, side := range SharedFrameWaitSides {
		m.sharedFrameWait.WithLabelValues(side)
	}
	m.pointsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: "access_query_points_dropped_total",
		Help: "Points the normalization of a query's answer let go rather than evaluate, by reason: outside_range_before and " +
			"outside_range_after, a point before the range the query accepts or at or after its end; row_missing_columns, a " +
			"trends range row without the time or value column. Not zero where the provider looks back further than asked: " +
			"read its baseline, and each Slot's own on its completion line (points_dropped)."}, []string{"reason"})
	for _, reason := range observability.PointDropReasons {
		m.pointsDropped.WithLabelValues(reason)
	}
	m.requestEnd = prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: "access_query_request_end_total",
		Help: "Query requests sent, by how their end was asked: trimmed, a second before the window's end - the provider " +
			"takes its range's end in, and the point there is the next round's, let go as outside_range_after; every grid " +
			"point being a whole second, no other point is left out. Asked at the window's end: step_not_whole_seconds, a " +
			"step whose grid need not be whole seconds; not_time_align, a query not aligned to the provider's grid; " +
			"source_not_eligible, a clause not a metric time series read from VictoriaMetrics (logs, events, APM, bk_data, " +
			"PromQL - the control process's trends ranges among them), whose last bucket a range ending earlier would cut " +
			"short. A trimmed request leaves the point at the end out; a provider that rounds a long range's end up to " +
			"its step may still answer it, let go as outside_range_after as before."}, []string{"outcome"})
	for _, outcome := range observability.QueryRequestEndOutcomes {
		m.requestEnd.WithLabelValues(outcome)
	}
	m.backend = &queryBackendBook{}
	m.failures = make(map[string]prometheus.Counter, len(queryFailureReasons))
	for _, reason := range queryFailureReasons {
		m.failures[reason] = m.queryFailures.WithLabelValues(reason)
	}
	return m
}

// RecordPhysicalQuery counts one physical query with the time its provider
// took and how it failed, "" when it answered, and notes it as the query
// backend's latest word.
func (r *Recorder) RecordPhysicalQuery(elapsed time.Duration, failure string) {
	if r == nil {
		return
	}
	flow := r.phaseTwo.flow
	flow.physicalQueries.Inc()
	flow.queryDuration.Observe(elapsed.Seconds())
	if failure == "" {
		flow.backend.note(time.Now(), "")
		return
	}
	counter, known := flow.failures[failure]
	if !known {
		counter, failure = flow.failures["other"], "other"
	}
	counter.Inc()
	flow.backend.note(time.Now(), failure)
}

// RecordQueryRequestEnd counts one request sent by how its end was asked; an
// outcome outside the closed list is not one this build gives and makes no
// cell.
func (r *Recorder) RecordQueryRequestEnd(outcome string) {
	if r == nil {
		return
	}
	for _, known := range observability.QueryRequestEndOutcomes {
		if outcome == known {
			r.phaseTwo.flow.requestEnd.WithLabelValues(outcome).Inc()
			return
		}
	}
}

// RecordQueryPointsDropped counts the points an answer's normalization let
// go, by reason.
func (r *Recorder) RecordQueryPointsDropped(dropped observability.PointDrops) {
	if r == nil {
		return
	}
	for _, reason := range observability.PointDropReasons {
		if count := dropped.Of(reason); count > 0 {
			r.phaseTwo.flow.pointsDropped.WithLabelValues(reason).Add(float64(count))
		}
	}
}

// RecordPhysicalQueryAnswer counts one physical query's answer in the
// per-query histograms, by how it ended.
func (r *Recorder) RecordPhysicalQueryAnswer(answer observability.PhysicalQueryAnswer) {
	if r == nil {
		return
	}
	outcome := answer.Outcome
	if !slices.Contains(observability.PhysicalQueryOutcomes, outcome) {
		outcome = observability.PhysicalQueryUnavailable
	}
	flow := r.phaseTwo.flow
	flow.answerBytes.WithLabelValues(outcome).Observe(float64(answer.Bytes))
	flow.answerSeries.WithLabelValues(outcome).Observe(float64(answer.Series))
	flow.answerRecords.WithLabelValues(outcome).Observe(float64(answer.Records))
	flow.answerSeconds.WithLabelValues(outcome).Observe(answer.Elapsed.Seconds())
	if answer.Answered {
		flow.answerPhaseSeconds.WithLabelValues(outcome, QueryAnswerPhaseWait).Observe(answer.Wait.Seconds())
		flow.answerPhaseSeconds.WithLabelValues(outcome, QueryAnswerPhaseBody).Observe(answer.Body.Seconds())
	}
	if answer.FrameWaitConsumer > 0 {
		flow.sharedFrameWait.WithLabelValues(SharedFrameWaitConsumer).Add(answer.FrameWaitConsumer.Seconds())
	}
	if answer.FrameWaitReader > 0 {
		flow.sharedFrameWait.WithLabelValues(SharedFrameWaitReader).Add(answer.FrameWaitReader.Seconds())
	}
}

// The sides of a shared-schema answer's frames, by the one waited on:
// consumer, the Slot taking a parsed frame; reader, the frame reader's next.
const (
	SharedFrameWaitConsumer = "consumer"
	SharedFrameWaitReader   = "reader"
)

// SharedFrameWaitSides lists them.
var SharedFrameWaitSides = []string{SharedFrameWaitConsumer, SharedFrameWaitReader}

// The phases of an answered query's seconds, closed: wait, before the
// answer's first byte; body, after it.
const (
	QueryAnswerPhaseWait = "wait"
	QueryAnswerPhaseBody = "body"
)

// QueryAnswerPhases lists them.
var QueryAnswerPhases = []string{QueryAnswerPhaseWait, QueryAnswerPhaseBody}

func (book *queryBackendBook) note(at time.Time, failure string) {
	if book == nil {
		return
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	book.known = true
	if failure == "" {
		book.health.LastSuccessAt = at
		return
	}
	book.health.LastFailureAt, book.health.LastFailure = at, failure
}

// QueryBackendHealth reads the query backend's latest answer and failure;
// false before this process has run a physical query.
func (r *Recorder) QueryBackendHealth() (QueryBackendHealth, bool) {
	if r == nil || r.phaseTwo.flow.backend == nil {
		return QueryBackendHealth{}, false
	}
	book := r.phaseTwo.flow.backend
	book.mu.Lock()
	defer book.mu.Unlock()
	return book.health, book.known
}

// SetRuntimeRedisClient names the Redis hook label of the client that
// serves runtime state, which the flow's runtime round trips are read under.
func (r *Recorder) SetRuntimeRedisClient(label string) {
	if r != nil && label != "" {
		r.runtimeRedis.Store(label)
	}
}

func (r *Recorder) runtimeRedisClient() string {
	if label, ok := r.runtimeRedis.Load().(string); ok {
		return label
	}
	return "runtime"
}

// RecordDecisionLag counts one decision's lag from readable data to its
// acknowledged completion.
func (r *Recorder) RecordDecisionLag(lag time.Duration) {
	if r == nil {
		return
	}
	r.phaseTwo.flow.decisionLag.Observe(lag.Seconds())
}

// FlowHistogram is a histogram's cumulative bucket counts, in the order of
// its bounds, and its total count; the observations past the last bound
// are Count minus the last bucket.
type FlowHistogram struct {
	Buckets []uint64 `json:"buckets"`
	Count   uint64   `json:"count"`
}

// FlowCounters is the current value of every counter the overview's flow
// reads, copied from the metrics themselves.
type FlowCounters struct {
	Queries        uint64            `json:"queries"`
	QueryFailures  map[string]uint64 `json:"query_failures"`
	QueryDuration  FlowHistogram     `json:"query_duration"`
	Detections     uint64            `json:"detections"`
	DecisionLag    FlowHistogram     `json:"decision_lag"`
	StateWrites    uint64            `json:"state_writes"`
	StateFailures  uint64            `json:"state_failures"`
	SeriesRejected uint64            `json:"series_rejected"`
	RuntimeRedis   FlowHistogram     `json:"runtime_redis"`
	EventsEmitted  uint64            `json:"events_emitted"`
	EventsRejected uint64            `json:"events_rejected"`
	// LiveHeapBytes is the heap collector's last sample, never a fresh one:
	// reading it forces no collection. Zero with LiveHeapKnown false before
	// the first sample. LiveHeapReserved is the retained-byte pool's
	// reservation at that sample's collection (heap_inuse_reserved_bytes),
	// the only reservation the live heap may be read against.
	LiveHeapBytes    uint64      `json:"live_heap_bytes"`
	LiveHeapKnown    bool        `json:"live_heap_known"`
	LiveHeapReserved HeapReading `json:"live_heap_reserved"`
	Hosts            uint64      `json:"hosts"`
}

// notDecisions are the completion kinds that book a Slot without deciding
// it; they are left out of the detection count as of the lag.
var notDecisions = []string{"GAP_SKIPPED", "SNAPSHOT_UNAVAILABLE"}

// FlowCounters reads the flow's counters now, from these few metrics only.
func (r *Recorder) FlowCounters() FlowCounters {
	flow := FlowCounters{QueryFailures: map[string]uint64{}}
	if r == nil {
		return flow
	}
	m := &r.phaseTwo
	flow.Queries = counterOf(m.flow.physicalQueries)
	for reason, counter := range m.flow.failures {
		if value := counterOf(counter); value > 0 {
			flow.QueryFailures[reason] = value
		}
	}
	flow.QueryDuration = histogramOf(QueryDurationBuckets, m.flow.queryDuration)
	flow.DecisionLag = histogramOf(DecisionLagBuckets, m.flow.decisionLag)
	eachSample(m.workflow.progress, func(sample *dto.Metric) {
		if !slices.Contains(notDecisions, labelOf(sample, "kind")) {
			flow.Detections += uint64(sample.GetCounter().GetValue())
		}
	})
	flow.StateWrites = counterOf(m.flow.stateApplied)
	eachSample(m.stateAdmissions, func(sample *dto.Metric) {
		if labelOf(sample, "result") != "success" {
			flow.StateFailures += uint64(sample.GetCounter().GetValue())
		}
	})
	eachSample(m.stateVersionConflict, func(sample *dto.Metric) { flow.StateFailures += uint64(sample.GetCounter().GetValue()) })
	// One admission decision per series per Plan; rejected is never recorded under host_identity, which
	// only adds a second record for an admitted series.
	eachSample(m.seriesAdmission, func(sample *dto.Metric) {
		if labelOf(sample, "result") == "rejected" {
			flow.SeriesRejected += uint64(sample.GetCounter().GetValue())
		}
	})
	flow.RuntimeRedis = FlowHistogram{Buckets: make([]uint64, len(redisDurationBuckets))}
	runtimeClient := r.runtimeRedisClient()
	eachSample(m.redisCalls.duration, func(sample *dto.Metric) {
		if labelOf(sample, "client") != runtimeClient {
			return
		}
		histogram := sample.GetHistogram()
		flow.RuntimeRedis.Count += histogram.GetSampleCount()
		for index, bucket := range histogram.GetBucket() {
			if index < len(flow.RuntimeRedis.Buckets) {
				flow.RuntimeRedis.Buckets[index] += bucket.GetCumulativeCount()
			}
		}
	})
	eachSample(m.outputEventsByKind, func(sample *dto.Metric) { flow.EventsEmitted += uint64(sample.GetCounter().GetValue()) })
	flow.EventsRejected = counterOf(m.flow.outputRejectedAll)
	if r.heap != nil {
		var reading HeapReading
		reading, flow.LiveHeapKnown = r.heap.cachedLive()
		flow.LiveHeapBytes, flow.LiveHeapReserved = reading.Live, reading
	}
	flow.Hosts = uint64(gaugeOf(m.cmdbIndexHosts))
	return flow
}

func counterOf(counter prometheus.Counter) uint64 {
	var sample dto.Metric
	if counter.Write(&sample) != nil {
		return 0
	}
	return uint64(sample.GetCounter().GetValue())
}

func gaugeOf(gauge prometheus.Gauge) float64 {
	var sample dto.Metric
	if gauge == nil || gauge.Write(&sample) != nil {
		return 0
	}
	return sample.GetGauge().GetValue()
}

func histogramOf(bounds []float64, histogram prometheus.Histogram) FlowHistogram {
	out := FlowHistogram{Buckets: make([]uint64, len(bounds))}
	var sample dto.Metric
	if histogram.Write(&sample) != nil {
		return out
	}
	out.Count = sample.GetHistogram().GetSampleCount()
	for index, bucket := range sample.GetHistogram().GetBucket() {
		if index < len(out.Buckets) {
			out.Buckets[index] = bucket.GetCumulativeCount()
		}
	}
	return out
}

// eachSample collects one collector's series and hands each to visit.
func eachSample(collector prometheus.Collector, visit func(*dto.Metric)) {
	samples := make(chan prometheus.Metric, 64)
	go func() {
		collector.Collect(samples)
		close(samples)
	}()
	for metric := range samples {
		var sample dto.Metric
		if metric.Write(&sample) == nil {
			visit(&sample)
		}
	}
}

func labelOf(sample *dto.Metric, name string) string {
	for _, pair := range sample.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
