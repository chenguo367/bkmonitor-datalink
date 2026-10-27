// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/filterclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// The control leader closes the alerts of its edit and absent decisions
// through the link's search and close by filter (package filterclose)
// whenever the link's control plane and its secret are named and the link
// serves the routes. A link that answers 501, 404 or 405 - storage that
// cannot search, or a link older than the routes - leaves the decision on
// the existing path for that round, counted, and is asked again after
// filterclose's recheck. There is no switch.
//
// The leader's absent close loop runs the filter's work in its rounds: the
// absent decisions, then the edit close, then the retries of kept items.

// filterCloseHTTPTimeout bounds one request: the link's own budget for a
// filtered close is ten seconds (alert-close.md), plus the transfer.
const filterCloseHTTPTimeout = 15 * time.Second

// filterClosePendingPerRound is how many kept items a round retries.
const filterClosePendingPerRound = 500

// newFilterCloser is the closer for a deployment that names the link's
// control plane and its secret; nil otherwise.
func newFilterCloser(cfg config.Config, runtime redis.UniversalClient, now func() time.Time, recorder *metric.Recorder) (*filterclose.Closer, error) {
	plane := cfg.PhaseTwo.Linkd.ControlPlane
	if !plane.Configured() || runtime == nil {
		recorder.SetLinkdFilterCloseSource(filterclose.Outcomes, (*filterclose.Closer)(nil).Stats)
		return nil, nil
	}
	client, err := linkdmanage.New(linkdmanage.Options{BaseURL: plane.URL, Secret: plane.JWTSecret, Username: plane.Username(),
		Client: &http.Client{Timeout: filterCloseHTTPTimeout}, Now: now})
	if err != nil {
		return nil, err
	}
	commands, err := filterclose.NewCommands(runtime, cfg.Redis.StatePrefix)
	if err != nil {
		return nil, err
	}
	pending, err := filterclose.NewPending(runtime, cfg.Redis.StatePrefix, filterclose.MaxPending)
	if err != nil {
		return nil, err
	}
	closer, err := filterclose.New(filterclose.Options{Link: client, Commands: commands, Pending: pending, Now: now})
	if err != nil {
		return nil, err
	}
	recorder.SetLinkdFilterCloseSource(filterclose.Outcomes, closer.Stats)
	return closer, nil
}

// filterRoute is the closer and how the leader fills in what a decision
// needs beside it: the deployment's event source, and an expectation for a
// strategy's alerts from a source other than the search.
type filterRoute struct {
	closer *filterclose.Closer
	source string
	// counts is alarmd's own count of each strategy's open alerts
	// (openalerts.Cache.OpenCounts); nil where no copy is kept.
	counts func() (map[openalerts.StrategyKey]int, []openalerts.StrategyKey)
}

func newFilterRoute(cfg config.Config, closer *filterclose.Closer, cache *openalerts.Cache) *filterRoute {
	if closer == nil {
		return nil
	}
	route := &filterRoute{closer: closer, source: cfg.PhaseTwo.Linkd.ControlPlane.Source()}
	if cache != nil {
		route.counts = cache.OpenCounts
	}
	return route
}

// inForce is whether a decision takes the filter route this round.
func (route *filterRoute) inForce() bool { return route != nil && route.closer.InForce() }

func (route *filterRoute) scope(key absentalerts.Key) filterclose.Scope {
	return filterclose.Scope{TenantID: key.TenantID, EventSourceID: route.source, StrategyID: key.StrategyID}
}

// expect is the expectation for a strategy's alerts: alarmd's own count of
// them, else the Console roster's last count, else the fixed ceiling.
func (route *filterRoute) expect(key absentalerts.Key, roster map[absentalerts.Key]int) filterclose.Expectation {
	if route.counts != nil {
		counts, _ := route.counts()
		if n, ok := counts[openalerts.StrategyKey{TenantID: key.TenantID, StrategyID: key.StrategyID}]; ok {
			return filterclose.Expectation{Count: n, Source: filterclose.ExpectOwnRecords}
		}
	}
	if n, ok := roster[key]; ok {
		return filterclose.Expectation{Count: n, Source: filterclose.ExpectRoster}
	}
	return filterclose.Expectation{Count: filterclose.FixedCeiling, Source: filterclose.ExpectFixedCeiling}
}

// retryPending retries the kept items, on the leader, while the route is in
// force.
func (route *filterRoute) retryPending(ctx context.Context) {
	if !route.inForce() {
		return
	}
	round, cancel := context.WithTimeout(ctx, absentCloseInterval/4)
	defer cancel()
	route.closer.RetryPending(round, filterClosePendingPerRound)
}

// forget drops the term's memory when the term ends.
func (route *filterRoute) forget() {
	if route != nil {
		route.closer.Forget()
	}
}

// filterCloseWriter sends closes decided series by series - an inactive
// strategy's listed alerts, a target out of scope - through the link's
// filter while the route is in force: per strategy and decision, the
// strategy's active alerts of those fingerprints, each closed by its alert
// id, so a close never ends an alert that reopened since it was decided.
// Through the fallback writer (the Kafka close events) otherwise, and for
// a group the link answers it cannot serve. A group the link did not settle
// this time - busy, failed, refused - is an error, which every caller
// reads as a send that failed and decides again next step.
type filterCloseWriter struct {
	route    *filterRoute
	fallback closeWriter
}

func withFilterClose(route *filterRoute, fallback closeWriter) closeWriter {
	if route == nil {
		return fallback
	}
	return filterCloseWriter{route: route, fallback: fallback}
}

type filterCloseGroup struct {
	tenantID, strategyID, decision string
	revision                       int64
}

func (writer filterCloseWriter) WriteCloseBatch(ctx context.Context, batch []linkdoutput.CloseRequest) error {
	if !writer.route.inForce() {
		return writer.fallback.WriteCloseBatch(ctx, batch)
	}
	var order []filterCloseGroup
	groups := map[filterCloseGroup][]linkdoutput.CloseRequest{}
	for _, request := range batch {
		decision := request.Reason
		if decision == "" {
			decision = linkdoutput.CloseReasonInactive
		}
		if request.Detail != "" {
			decision += "/" + request.Detail
		}
		group := filterCloseGroup{tenantID: request.TenantID, strategyID: strconv.FormatInt(request.StrategyID, 10), decision: decision,
			revision: request.StrategyRevision}
		if _, seen := groups[group]; !seen {
			order = append(order, group)
		}
		groups[group] = append(groups[group], request)
	}
	var rest []linkdoutput.CloseRequest
	for _, group := range order {
		requests := groups[group]
		scope := writer.route.scope(absentalerts.Key{TenantID: group.tenantID, StrategyID: group.strategyID})
		for start := 0; start < len(requests); start += linkdmanage.MaxLimit {
			chunk := requests[start:min(len(requests), start+linkdmanage.MaxLimit)]
			fingerprints := make([]string, 0, len(chunk))
			for _, request := range chunk {
				fingerprints = append(fingerprints, request.Fingerprint)
			}
			switch word := writer.route.closer.Fingerprints(ctx, scope, fingerprints, group.decision, group.revision); word {
			case filterclose.OutcomeJobDone, filterclose.OutcomeNothingMatched:
			case filterclose.OutcomeLinkUnsupported:
				rest = append(rest, chunk...)
			default:
				return fmt.Errorf("alarmd: the link's filtered close of strategy %s did not settle: %s", group.strategyID, word)
			}
		}
	}
	if len(rest) > 0 {
		return writer.fallback.WriteCloseBatch(ctx, rest)
	}
	return nil
}
