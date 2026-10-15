// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package uq

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TopFailuresPages bounds one read: at most this many raw Span pages of
// MaxTraceRows each are scanned, and a larger failure set is answered as a
// sample.
const TopFailuresPages = 5

// TopFailuresGroups is how many groups an answer lists.
const TopFailuresGroups = 20

// TraceRetention is how far back a window may start: the trace store keeps
// spans this long.
const TraceRetention = 7 * 24 * time.Hour

// traceTotalCap is the backend's cap on a reported total; a total at it is
// a lower bound.
const traceTotalCap = 10000

// topFailuresMessageBytes bounds the raw message a group carries; the
// channel scrubs and bounds it again for its reader.
const topFailuresMessageBytes = 1024

var topFailuresWindows = map[string]time.Duration{"1h": time.Hour, "6h": 6 * time.Hour}

// TopFailuresRequest names a registered trace target and the window to read
// its failing server spans over: the hour or six hours before End (RFC 3339
// in UTC, default now), optionally one service, optionally counting 4xx.
type TopFailuresRequest struct {
	Egress     string `json:"egress"`
	Target     string `json:"target"`
	Window     string `json:"window,omitempty"`
	End        string `json:"end,omitempty"`
	Service    string `json:"service,omitempty"`
	Include4xx bool   `json:"include_4xx,omitempty"`
	// Now is the read's clock; zero is the wall clock.
	Now time.Time `json:"-"`
}

// FailureGroup is one service, route, HTTP status and business code with how
// often it failed in the spans scanned.
type FailureGroup struct {
	Service        string    `json:"service"`
	Route          string    `json:"route"`
	HTTPStatus     string    `json:"http_status,omitempty"`
	BusinessCode   string    `json:"business_code,omitempty"`
	Count          int       `json:"count"`
	FirstSeen      time.Time `json:"first_seen"`
	LastSeen       time.Time `json:"last_seen"`
	P50Ms          float64   `json:"p50_ms"`
	P95Ms          float64   `json:"p95_ms"`
	SampleTraceIDs []string  `json:"sample_trace_ids"`
	Message        string    `json:"message,omitempty"`
}

// TopFailures is the answer: the window, the native query, how many failing
// spans were scanned of how many there are, and the groups. Sampled says the
// groups and their percentiles come from the scanned part only; TotalAtLeast
// that the total is the backend's cap and there may be more.
type TopFailures struct {
	Target       TraceTarget    `json:"target"`
	Window       string         `json:"window"`
	Start        time.Time      `json:"start"`
	End          time.Time      `json:"end"`
	QueryString  string         `json:"query_string"`
	Scanned      int            `json:"scanned"`
	Total        int64          `json:"total"`
	TotalAtLeast bool           `json:"total_at_least,omitempty"`
	Sampled      bool           `json:"sampled,omitempty"`
	Pages        int            `json:"pages"`
	Groups       []FailureGroup `json:"groups"`
	Limitations  []string       `json:"limitations"`
}

var topFailureColumns = []any{"trace_id", "span_name", "start_time", "elapsed_time", "resource.service.name", "attributes.http.route",
	"attributes.http.status_code", "attributes.http.response.status_code", "attributes.http.response.code", "attributes.http.response.message", "events"}

// TopFailures reads a target's failing server spans and groups them. The
// failure predicate is part of the native query, so every row read is a
// failure and the total counts failures; the counting is done here because
// the query backend does not aggregate this store.
func (p *Provider) TopFailures(ctx context.Context, request TopFailuresRequest) (TopFailures, error) {
	answer := TopFailures{Groups: []FailureGroup{}, Limitations: []string{}}
	window := request.Window
	if window == "" {
		window = "1h"
	}
	length, ok := topFailuresWindows[window]
	if !ok {
		return answer, fail("invalid_input", "window must be 1h or 6h.")
	}
	now := request.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	end := now
	if request.End != "" {
		if !strings.HasSuffix(request.End, "Z") {
			return answer, fail("invalid_input", "end must be an RFC 3339 time in UTC, ending in Z.")
		}
		parsed, err := time.Parse(time.RFC3339, request.End)
		if err != nil {
			return answer, fail("invalid_input", "end must be an RFC 3339 time in UTC, ending in Z.")
		}
		end = parsed.UTC()
	}
	start := end.Add(-length)
	if end.After(now) {
		return answer, fail("invalid_input", "end is in the future.")
	}
	if start.Before(now.Add(-TraceRetention)) {
		return answer, fail("invalid_input", "the window starts before the trace retention of 7 days; move end later or shorten the window.")
	}
	egress, ok := p.egresses[request.Egress]
	if !ok {
		return answer, fail("uq_egress_unknown", "UQ egress is not registered in this deployment.")
	}
	var target *TraceTarget
	for i := range egress.TraceTargets {
		if egress.TraceTargets[i].Name == request.Target {
			target = &egress.TraceTargets[i]
		}
	}
	if target == nil {
		return answer, fail("uq_trace_target_unknown", "Trace target is not registered in this deployment.")
	}
	answer.Target, answer.Window, answer.Start, answer.End = *target, window, start, end
	answer.QueryString = failurePredicate(request)
	groups := map[string]*failureAccumulator{}
	for page := 0; page < TopFailuresPages; page++ {
		query := map[string]any{"data_source": "bkapm", "table_id": target.TableID, "time_field": "start_time",
			"query_string": answer.QueryString, "keep_columns": topFailureColumns}
		if target.Shared || strings.HasPrefix(target.TableID, "apm_global.shared") {
			query["conditions"] = map[string]any{"field_list": []any{
				map[string]any{"field_name": "bk_biz_id", "op": "eq", "value": []any{strconv.FormatInt(target.BkBizID, 10)}},
				map[string]any{"field_name": "app_name", "op": "eq", "value": []any{target.AppName}},
			}, "condition_list": []any{"and"}}
		}
		body := map[string]any{"query_list": []any{query}, "start_time": strconv.FormatInt(start.Unix(), 10), "end_time": strconv.FormatInt(end.Unix(), 10),
			"from": page * MaxTraceRows, "limit": MaxTraceRows}
		receipt, err := p.Trace(ctx, TraceRequest{Egress: request.Egress, Target: request.Target, Body: body})
		answer.Pages++
		if err != nil {
			return answer, err
		}
		var response struct {
			List  []map[string]json.RawMessage `json:"list"`
			Total int64                        `json:"total"`
		}
		if json.Unmarshal(receipt.Native, &response) != nil {
			return answer, fail("uq_trace_response_invalid", "Raw Span response could not be read.")
		}
		answer.Total = response.Total
		for _, row := range response.List {
			addFailure(groups, row)
		}
		answer.Scanned += len(response.List)
		if len(response.List) < MaxTraceRows || int64(answer.Scanned) >= response.Total {
			break
		}
	}
	answer.TotalAtLeast = answer.Total >= traceTotalCap
	answer.Sampled = int64(answer.Scanned) < answer.Total
	if answer.Sampled {
		answer.Limitations = append(answer.Limitations, fmt.Sprintf("sampled: %d of the failing spans were scanned; groups, counts and percentiles are over the scanned part only", answer.Scanned))
	}
	if answer.TotalAtLeast {
		answer.Limitations = append(answer.Limitations, fmt.Sprintf("the backend caps the total at %d; there are at least that many failing spans", traceTotalCap))
	}
	answer.Groups = rankFailures(groups)
	return answer, nil
}

// failurePredicate is the native query_string that selects failing server
// spans: marked failed, or an HTTP status at or above 500 (400 with 4xx).
func failurePredicate(request TopFailuresRequest) string {
	floor := 500
	if request.Include4xx {
		floor = 400
	}
	predicate := fmt.Sprintf("kind:2 AND (status.code:2 OR attributes.http.status_code:>=%d OR attributes.http.response.status_code:>=%d)", floor, floor)
	if request.Service != "" {
		predicate += fmt.Sprintf(" AND resource.service.name:%q", request.Service)
	}
	return predicate
}

type failureAccumulator struct {
	group    FailureGroup
	elapsed  []float64
	seenMins bool
}

func addFailure(groups map[string]*failureAccumulator, row map[string]json.RawMessage) {
	route := rawText(row["attributes.http.route"])
	if route == "" {
		route = rawText(row["span_name"])
	}
	status := rawText(row["attributes.http.status_code"])
	if status == "" {
		status = rawText(row["attributes.http.response.status_code"])
	}
	group := FailureGroup{Service: rawText(row["resource.service.name"]), Route: route, HTTPStatus: status, BusinessCode: rawText(row["attributes.http.response.code"])}
	key := strings.Join([]string{group.Service, group.Route, group.HTTPStatus, group.BusinessCode}, "\x00")
	accumulator, ok := groups[key]
	if !ok {
		group.SampleTraceIDs = []string{}
		accumulator = &failureAccumulator{group: group}
		groups[key] = accumulator
	}
	g := &accumulator.group
	g.Count++
	if startUs, ok := rawInt(row["start_time"]); ok {
		at := time.UnixMicro(startUs).UTC()
		if !accumulator.seenMins || at.Before(g.FirstSeen) {
			g.FirstSeen = at
		}
		if !accumulator.seenMins || at.After(g.LastSeen) {
			g.LastSeen = at
		}
		accumulator.seenMins = true
	}
	if elapsedUs, ok := rawInt(row["elapsed_time"]); ok {
		accumulator.elapsed = append(accumulator.elapsed, float64(elapsedUs)/1000)
	}
	if trace := rawText(row["trace_id"]); trace != "" && len(g.SampleTraceIDs) < 3 {
		g.SampleTraceIDs = append(g.SampleTraceIDs, trace)
	}
	if g.Message == "" {
		g.Message = failureMessage(row)
	}
}

// failureMessage is the exception event's message, else the response's.
func failureMessage(row map[string]json.RawMessage) string {
	var events []struct {
		Name       string         `json:"name"`
		Attributes map[string]any `json:"attributes"`
	}
	message := ""
	if json.Unmarshal(row["events"], &events) == nil {
		for _, event := range events {
			if text, ok := event.Attributes["exception.message"].(string); ok && text != "" {
				message = text
				break
			}
		}
	}
	if message == "" {
		message = rawText(row["attributes.http.response.message"])
	}
	if len(message) > topFailuresMessageBytes {
		message = strings.ToValidUTF8(message[:topFailuresMessageBytes], "")
	}
	return message
}

func rankFailures(groups map[string]*failureAccumulator) []FailureGroup {
	ranked := make([]FailureGroup, 0, len(groups))
	for _, accumulator := range groups {
		sort.Float64s(accumulator.elapsed)
		accumulator.group.P50Ms = percentile(accumulator.elapsed, 0.50)
		accumulator.group.P95Ms = percentile(accumulator.elapsed, 0.95)
		ranked = append(ranked, accumulator.group)
	}
	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		if !a.LastSeen.Equal(b.LastSeen) {
			return a.LastSeen.After(b.LastSeen)
		}
		return groupKey(a) < groupKey(b)
	})
	if len(ranked) > TopFailuresGroups {
		ranked = ranked[:TopFailuresGroups]
	}
	return ranked
}

func groupKey(g FailureGroup) string {
	return strings.Join([]string{g.Service, g.Route, g.HTTPStatus, g.BusinessCode}, "\x00")
}

// percentile is the nearest-rank percentile of sorted values.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	return sorted[rank]
}

// rawText is a JSON string or number as text; anything else is "".
func rawText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return ""
}

func rawInt(raw json.RawMessage) (int64, bool) {
	value, err := strconv.ParseInt(rawText(raw), 10, 64)
	return value, err == nil
}
