// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FrontReads is what the front surface may read, the admin server's page:
// alarmd's own data and nothing of the cluster's, no raw store read, no
// re-query and no write. The value says whether a read is charged to the
// front read budget. overview.get answers from its own cache and runtime.get
// from process memory without Redis: both are bounded without it, and a
// burst of list reads must not refuse the page its own cheapest reads.
var FrontReads = map[string]bool{
	"overview.get":         false,
	"fleet.get":            true,
	"strategy.list":        true,
	"strategy.get":         true,
	"object.list":          true,
	"object.get":           true,
	"diagnose.environment": true,
	"runtime.get":          false,
}

// FrontReadShare is the share of this process's CPU limit the charged front
// reads may spend each minute, charged by their elapsed time since Go has no
// per-request CPU. Elapsed time overstates CPU about twice (a measured list
// read is about 120 ms of CPU in 230 ms), so the reads use about 1.3% of the
// limit: below the 1.7-point CPU spread measured between two identical
// replicas (20.19% against 21.91%), so detection cannot tell them from noise.
// The reads share the runtime Redis pool with detection; this bound, not a
// separate pool, is what keeps them apart. It is not exact: a read is
// charged after it ends, so reads already running when the minute's budget
// runs out finish over it -- at most the evidence pool's concurrency times
// one read's elapsed time -- and a read that straddles a minute is not
// charged to either.
const FrontReadShare = 0.025

// frontBudget is the charged front reads' spend of elapsed time in one
// clock minute; a new minute starts over.
type frontBudget struct {
	mu     sync.Mutex
	minute int64
	spent  time.Duration
}

// frontReadAllowance is a minute's budget for a CPU limit of cores.
func frontReadAllowance(cores int) time.Duration {
	return time.Duration(FrontReadShare * float64(cores) * float64(time.Minute))
}

// open says whether the minute's budget has any left, and when it refills
// if not.
func (b *frontBudget) open(now time.Time, allowance time.Duration) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	minute := now.Unix() / 60
	if b.minute != minute {
		b.minute, b.spent = minute, 0
	}
	if b.spent >= allowance {
		return false, time.Unix((minute+1)*60, 0).Sub(now)
	}
	return true, 0
}

// charge spends elapsed from the minute the read started in.
func (b *frontBudget) charge(started time.Time, elapsed time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.minute == started.Unix()/60 {
		b.spent += elapsed
	}
}

// FrontReadHandler serves the front surface's read: an invoke of one of
// FrontReads, authorized by the administrator key through authorize. It
// writes no audit record: a page view is not audited.
func (c *Channel) FrontReadHandler(authorize func(*http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { c.serveFront(w, r, authorize) })
}

func (c *Channel) serveFront(w http.ResponseWriter, r *http.Request, authorize func(*http.Request) error) {
	meta := c.localMeta("")
	fail := func(status int, code, message string) {
		c.write(w, status, Response{Status: "error", Summary: message, Error: &Failure{Code: code, Message: message}, Meta: meta})
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(RequestTimeout))
	_ = controller.SetWriteDeadline(time.Now().Add(RequestTimeout + TransportMargin))
	admitted := false
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		_ = controller.Flush()
		_ = controller.SetReadDeadline(time.Time{})
		_ = controller.SetWriteDeadline(time.Time{})
		if admitted {
			<-c.httpSlots
		}
	}()
	select {
	case c.httpSlots <- struct{}{}:
		admitted = true
	default:
		_ = controller.SetReadDeadline(time.Now())
		fail(429, "request_budget_exceeded", "OB channel request slots are busy.")
		return
	}
	if r.Method != http.MethodPost {
		fail(405, "method_not_allowed", "Use POST for the front read.")
		return
	}
	if authorize == nil || authorize(r) != nil {
		fail(403, "admin_unauthorized", "Deployment administrator authorization is required.")
		return
	}
	var req request
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, EnvelopeMaxBytes))
	if err != nil {
		fail(400, "invalid_input", "Request exceeds the transport byte limit or could not be read.")
		return
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		fail(400, "invalid_input", "Request must be one JSON envelope within the request byte limit.")
		return
	}
	if req.Version != Version {
		fail(400, "unsupported_channel_version", "This server supports "+Version)
		return
	}
	if req.Mode != "invoke" {
		fail(400, "invalid_mode", "The front read takes invoke only.")
		return
	}
	charged, permitted := FrontReads[req.Operation]
	if !permitted {
		fail(403, "op_not_permitted_for_front", "The front surface reads only alarmd's own data; use the CLI for this operation.")
		return
	}
	op, ok := c.ops[req.Operation]
	if !ok {
		fail(404, "unknown_operation", "Operation is not registered on this replica.")
		return
	}
	if req.Revision != "" && req.Revision != c.revision {
		fail(409, "catalog_changed", "Expected catalog revision does not match.")
		return
	}
	req.Revision = c.revision
	if len(body) > op.RequestMaxBytes {
		fail(400, "invalid_input", "Request exceeds this operation's described byte limit.")
		return
	}
	params, target, err := invocationParams(op, req.Params)
	if err != nil {
		fail(400, "invalid_input", err.Error())
		return
	}
	if target.Explicit() {
		fail(400, "front_target_not_allowed", "The front read answers for the deployment; it takes no replica target.")
		return
	}
	if c.options.RouteControlDefaults && op.DefaultControlLeader {
		target.ControlLeader = true
	}
	if target.Explicit() && c.options.Route == nil {
		fail(503, "target_routing_unavailable", "Targeted evidence routing is not configured.")
		return
	}
	if !target.Explicit() {
		if availability := available(op); !availability.Available {
			fail(503, "operation_unavailable", availability.Reason)
			return
		}
	}
	if charged {
		if open, refill := c.front.open(c.options.Now(), frontReadAllowance(c.frontCores())); !open {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(refill.Seconds()))))
			fail(429, "front_rate_limited", "This replica's front read budget for the minute is spent; retry when it refills.")
			return
		}
	}
	pool := c.executionSlots(op)
	select {
	case pool <- struct{}{}:
		defer func() { <-pool }()
	default:
		fail(429, "request_budget_exceeded", "OB evidence readers are busy; retry this read later.")
		return
	}
	_ = controller.SetReadDeadline(time.Time{})
	_ = controller.SetWriteDeadline(time.Now().Add(op.ExecutionTimeout + TransportMargin))
	execCtx, cancel := context.WithTimeout(r.Context(), op.ExecutionTimeout)
	defer cancel()
	execCtx = WithCaller(execCtx, Caller{RequestID: meta.RequestID})
	started, startedAt := time.Now(), c.options.Now()
	var out Response
	if target.Explicit() {
		out = c.options.Route(execCtx, Invocation{EnvironmentID: c.options.EnvironmentID, Version: req.Version, Revision: req.Revision,
			OperationContractRevision: c.OperationContractRevision(op.ID), Operation: req.Operation, RequestID: meta.RequestID, Params: params, Target: target})
		out.Meta.Revision = c.revision
	} else {
		out = c.runHere(execCtx, op, req, params, meta)
	}
	if charged {
		c.front.charge(startedAt, time.Since(started))
	}
	code := 200
	if out.Status == "error" {
		code = 502
	}
	c.write(w, code, out)
}

// frontCores is the CPU limit the front read budget is a share of: the
// container's, as GOMAXPROCS is set from the cgroup quota at start.
func (c *Channel) frontCores() int {
	if c.options.FrontCores != nil {
		return c.options.FrontCores()
	}
	return runtime.GOMAXPROCS(0)
}
