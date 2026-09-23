// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// linkdConsoleEndpoint is the Console's entry as the configuration names it.
// The address is the configured URL, which the configuration refuses to
// carry credentials or a query in; the Basic Auth pair is never here.
//
// An unconfigured Console carries its state from the start rather than
// waiting for anything to fill it: "not configured" is the reading.
func linkdConsoleEndpoint(cfg config.Config) fleet.Endpoint {
	address := cfg.PhaseTwo.Linkd.ConsoleURL
	entry := fleet.Endpoint{Role: fleet.EndpointLinkdConsole, Kind: "http", Address: address, Configured: address != ""}
	if !entry.Configured {
		entry.Console = linkdConsoleFacts(nil, time.Time{})
	}
	return entry
}

// withLinkdConsole fills the Console's entry with what this replica has seen
// of it. A nil console is a deployment that configured none, whose entry
// already says so.
func withLinkdConsole(endpoints func() []fleet.Endpoint, console *openalerts.HTTPReconciler,
	discovery *fleet.LinkdDiscoveryFacts, now func() time.Time) func() []fleet.Endpoint {
	if console == nil {
		return endpoints
	}
	return func() []fleet.Endpoint {
		entries := endpoints()
		for index := range entries {
			if entries[index].Role == fleet.EndpointLinkdConsole {
				facts := linkdConsoleFacts(console, now())
				facts.Discovery = discovery
				entries[index].Console = facts
			}
		}
		return entries
	}
}

// linkdConsoleFacts decides the Console's state from its call record. The
// order is the order a reader acts in: nothing to call, a call that fails,
// a link that answers and says it is behind, nothing called yet, and only
// then reachable. The link's own word comes from the same function the
// close's link_unhealthy refusal reads, under the same bound.
func linkdConsoleFacts(console *openalerts.HTTPReconciler, at time.Time) *fleet.LinkdConsoleFacts {
	facts := &fleet.LinkdConsoleFacts{MaxLinkHealthAgeSeconds: int(absentCloseMaxLinkHealthAge / time.Second),
		Calls: make([]fleet.ConsoleCallFacts, 0, len(openalerts.ConsoleOps))}
	var record openalerts.ConsoleRecord
	if console != nil {
		record = console.Record()
	}
	called := false
	for _, op := range openalerts.ConsoleOps {
		call := record.Calls[op]
		row := fleet.ConsoleCallFacts{Op: op, Calls: call.Calls, Failures: call.Failures, Failing: call.LatestFailed}
		if !call.LastSuccessAt.IsZero() {
			age := at.Sub(call.LastSuccessAt).Seconds()
			row.LastSuccessAgeSeconds = &age
		}
		if !call.LastFailureAt.IsZero() {
			age := at.Sub(call.LastFailureAt).Seconds()
			row.LastFailureAgeSeconds, row.LastFailure = &age, call.LastFailure
		}
		facts.Calls = append(facts.Calls, row)
		called = called || call.Calls > 0
		if row.Failing && facts.Reason == "" {
			facts.Reason = op
		}
	}
	if console != nil {
		if target, resolvedAt := console.Target(); !resolvedAt.IsZero() {
			age := at.Sub(resolvedAt).Seconds()
			facts.Target, facts.TargetAgeSeconds = linkdTargetFacts(target), &age
		}
	}
	linkWord := ""
	if !record.LinkReadAt.IsZero() {
		age := at.Sub(record.LinkReadAt).Seconds()
		pending := record.Link.PendingCount
		facts.LinkReadAgeSeconds, facts.LinkPending, facts.LinkError = &age, &pending, record.Link.Error
		if !record.Link.LastSuccess.IsZero() {
			health := at.Sub(record.Link.LastSuccess).Seconds()
			facts.LinkHealthAgeSeconds = &health
		}
		linkWord = absentalerts.LinkHealthWord(record.Link.LastSuccess, record.Link.Error, at, absentCloseMaxLinkHealthAge)
	}
	switch {
	case console == nil:
		facts.State = fleet.LinkdConsoleNotConfigured
	case facts.Reason != "":
		facts.State = fleet.LinkdConsoleUnreadable
	case linkWord != "" && linkWord != absentalerts.LinkHealthy:
		facts.State, facts.Reason = fleet.LinkdConsoleLinkUnhealthy, linkWord
	case !called:
		facts.State = fleet.LinkdConsoleNotCalled
	default:
		facts.State = fleet.LinkdConsoleReachable
	}
	return facts
}

// linkdConsoleReading is the metric's view of the same facts the endpoint
// entry carries, decided by the same function.
func linkdConsoleReading(console *openalerts.HTTPReconciler, now func() time.Time) func() metric.LinkdConsoleReading {
	return func() metric.LinkdConsoleReading {
		facts := linkdConsoleFacts(console, now())
		reading := metric.LinkdConsoleReading{State: facts.State, Calls: make(map[string]metric.LinkdConsoleCalls, len(facts.Calls))}
		for _, call := range facts.Calls {
			reading.Calls[call.Op] = metric.LinkdConsoleCalls{Calls: call.Calls, Failures: call.Failures}
		}
		return reading
	}
}
