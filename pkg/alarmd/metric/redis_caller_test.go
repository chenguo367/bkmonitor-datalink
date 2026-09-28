// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package metric

import (
	"context"
	"testing"

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
