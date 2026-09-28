// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package metric

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// callerCounts reads one caller family as client/caller, and /reason when
// it has one.
func callerCounts(t *testing.T, r *Recorder, family string) map[string]float64 {
	t.Helper()
	counts := map[string]float64{}
	for _, m := range gatherFamily(t, r, family) {
		labels := map[string]string{}
		for _, label := range m.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		key := labels["client"] + "/" + labels["caller"]
		if reason, ok := labels["reason"]; ok {
			key += "/" + reason
		}
		counts[key] = m.GetCounter().GetValue()
	}
	return counts
}

const (
	callerOperationFamily = "bkmonitor_alarmd_redis_caller_operation_total"
	callerReasonFamily    = "bkmonitor_alarmd_redis_caller_failure_reason_total"
)

// The diagnostics client's jobs each have their cells from startup, so a
// zero is a count of that job's failures and not a series never registered.
func TestTheDiagnosticsClientsJobsHaveTheirCellsFromStartup(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	operations, reasons := callerCounts(t, r, callerOperationFamily), callerCounts(t, r, callerReasonFamily)
	if len(operations) != len(redisfailure.Callers) || len(reasons) != len(redisfailure.Callers)*len(redisfailure.Reasons) {
		t.Fatalf("%d operation and %d reason cells before any call, want every job and every reason", len(operations), len(reasons))
	}
	for _, caller := range redisfailure.Callers {
		if _, ok := operations["diagnostics/"+caller]; !ok {
			t.Fatalf("no operation cell for %s: %v", caller, operations)
		}
	}
}

// An operation, and its failure, count against the job its context names -
// one call and one pipeline alike - beside the client's own counts, which
// stay as they were. A call that names no job counts against none, and a
// name outside the closed set counts as other.
func TestARedisFailureCountsAgainstTheJobThatMadeTheCall(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	hook := r.RedisHook("diagnostics")
	call := func(ctx context.Context, name string, err error) {
		ctx, _ = hook.BeforeProcess(ctx, nil)
		_ = hook.AfterProcess(ctx, failedCommand(name, err))
	}
	refresh := redisfailure.WithCaller(context.Background(), redisfailure.CallerDirectoryRefresh)
	call(refresh, "getrange", context.DeadlineExceeded)
	call(refresh, "getrange", nil)
	call(context.Background(), "getrange", context.DeadlineExceeded)
	call(redisfailure.WithCaller(context.Background(), "directroy_refresh"), "getrange", context.DeadlineExceeded)

	write := redisfailure.WithCaller(context.Background(), redisfailure.CallerDiagnosticWrite)
	ctx, _ := hook.BeforeProcessPipeline(write, nil)
	_ = hook.AfterProcessPipeline(ctx, []redis.Cmder{failedCommand("lpush", context.DeadlineExceeded), failedCommand("ltrim", context.DeadlineExceeded)})

	operations, reasons := callerCounts(t, r, callerOperationFamily), callerCounts(t, r, callerReasonFamily)
	for cell, want := range map[string]float64{
		"diagnostics/directory_refresh": 2, "diagnostics/other": 1, "diagnostics/diagnostic_write": 1, "diagnostics/diagnostic_read": 0,
	} {
		if operations[cell] != want {
			t.Errorf("operations %s = %v, want %v; all %v", cell, operations[cell], want, operations)
		}
	}
	for cell, want := range map[string]float64{
		"diagnostics/directory_refresh/timeout": 1, "diagnostics/other/timeout": 1, "diagnostics/diagnostic_write/timeout": 1,
		"diagnostics/directory_read/timeout": 0,
	} {
		if reasons[cell] != want {
			t.Errorf("reasons %s = %v, want %v", cell, reasons[cell], want)
		}
	}
	if got := failureReasonCounts(t, r)["diagnostics/timeout"]; got != 4 {
		t.Errorf("the client's own timeouts = %v, want all 4, named job or not", got)
	}
	for _, name := range []string{"getrange", "lrange", "lpush", "ltrim"} {
		if boundedRedisCommand(name) != name {
			t.Errorf("%s reads as %q, want its own name", name, boundedRedisCommand(name))
		}
	}
}

// A call made on its caller's spent deadline fails at once, and go-redis,
// asking each Sentinel with that context, gives up in the words of a Sentinel
// outage; such a call is named by the deadline - timeout, or canceled for a
// cancelled caller - whatever the error said. So is one that fails with no
// Sentinel answering after the deadline passed during it. A Sentinel that does
// not answer while the caller still has time is sentinel_unreachable, and a
// call that failed in its own words during its time keeps them.
func TestAFailureOnTheCallersSpentDeadlineIsNamedByTheDeadline(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	hook := r.RedisHook("diagnostics")
	refresh := redisfailure.WithCaller(context.Background(), redisfailure.CallerDirectoryRefresh)
	sentinels := errors.New("redis: all sentinels specified in configuration are unreachable")
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	call := func(ctx context.Context, during func(), err error) {
		ctx, _ = hook.BeforeProcess(ctx, nil)
		during()
		_ = hook.AfterProcess(ctx, failedCommand("getrange", err))
	}
	nothing := func() {}

	spent, cancelSpent := context.WithDeadline(refresh, time.Now().Add(-time.Second))
	defer cancelSpent()
	call(spent, nothing, sentinels)
	call(spent, nothing, errors.New("redis: connection pool timeout"))
	cancelled, cancel := context.WithCancel(refresh)
	cancel()
	call(cancelled, nothing, sentinels)
	call(refresh, nothing, sentinels)
	expiring, cancelExpiring := context.WithTimeout(refresh, 20*time.Millisecond)
	defer cancelExpiring()
	call(expiring, func() { <-expiring.Done() }, sentinels)
	during, cancelDuring := context.WithCancel(refresh)
	call(during, cancelDuring, refused)
	pipeline, _ := hook.BeforeProcessPipeline(spent, nil)
	_ = hook.AfterProcessPipeline(pipeline, []redis.Cmder{failedCommand("getrange", sentinels)})

	reasons := callerCounts(t, r, callerReasonFamily)
	for cell, want := range map[string]float64{
		"diagnostics/directory_refresh/timeout": 4, "diagnostics/directory_refresh/canceled": 1,
		"diagnostics/directory_refresh/sentinel_unreachable": 1, "diagnostics/directory_refresh/connection_refused": 1,
		"diagnostics/directory_refresh/pool_timeout": 0,
	} {
		if reasons[cell] != want {
			t.Errorf("reasons %s = %v, want %v", cell, reasons[cell], want)
		}
	}
	if got := failureReasonCounts(t, r); got["diagnostics/timeout"] != 4 || got["diagnostics/sentinel_unreachable"] != 1 {
		t.Errorf("the client's own reasons = %v, want the same naming", got)
	}
	// Each call's round trip is still timed from when it was issued.
	var timed uint64
	for _, m := range gatherFamily(t, r, "bkmonitor_alarmd_redis_command_duration_seconds") {
		labels := map[string]string{}
		for _, label := range m.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		if labels["client"] == "diagnostics" && labels["command"] == "getrange" {
			timed += m.GetHistogram().GetSampleCount()
		}
	}
	if timed != 7 {
		t.Errorf("timed round trips = %d, want the 7 calls", timed)
	}
}
