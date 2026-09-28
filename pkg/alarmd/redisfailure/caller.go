// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package redisfailure

import "context"

// Callers, closed: the jobs that share one Redis client and whose failures
// have to be told apart. A client's failures by reason say how often and why,
// and not whose - on the diagnostics client, which several observation jobs
// share on one small pool, a timeout could be a directory refresh that read a
// stale snapshot, a diagnostic record that was never written, or a cost
// projection that published nothing, and those lose different things.
const (
	// CallerDirectoryRefresh is the strategy directory's periodic read of the
	// published catalog.
	CallerDirectoryRefresh = "directory_refresh"
	// CallerDirectoryRead is a point read the directory makes for an answer:
	// one Plan's effective content or output.
	CallerDirectoryRead = "directory_read"
	// CallerDiagnosticWrite is the diagnostic store writing an observation
	// window's records and series samples.
	CallerDiagnosticWrite = "diagnostic_write"
	// CallerDiagnosticRead is the diagnostic store reading them back.
	CallerDiagnosticRead = "diagnostic_read"
	// CallerCostProjection is the cost refresh: the replica registry it pages,
	// the other replicas' projections it loads, and its own it publishes.
	CallerCostProjection = "cost_projection"
)

// Callers is every caller name, for callers that must enumerate them.
var Callers = []string{CallerDirectoryRefresh, CallerDirectoryRead, CallerDiagnosticWrite, CallerDiagnosticRead, CallerCostProjection}

type callerKey struct{}

// WithCaller names the job the Redis calls made with ctx belong to.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// Caller is the job ctx names, "" when it names none. A name outside Callers
// reads as "other", so a caller's typo cannot mint a series.
func Caller(ctx context.Context) string {
	caller, _ := ctx.Value(callerKey{}).(string)
	if caller == "" {
		return ""
	}
	for _, known := range Callers {
		if caller == known {
			return caller
		}
	}
	return "other"
}
