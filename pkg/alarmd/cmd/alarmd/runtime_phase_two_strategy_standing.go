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
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// strategyLookupSource answers the fleet's strategy standing from the
// reconciler's catalog memory, field for field. The control plane's words
// for a disposition are the fleet's already: the first screen counts by
// them.
func strategyLookupSource(reconciler *controlplane.SourceReconciler) fleet.StrategyLookupFunc {
	if reconciler == nil {
		return nil
	}
	return func(strategyID string) fleet.StrategyLookupFacts {
		return strategyLookupFactsOf(reconciler.LookupStrategy(strategyID))
	}
}

func strategyLookupFactsOf(lookup controlplane.StrategyLookup) fleet.StrategyLookupFacts {
	facts := fleet.StrategyLookupFacts{
		Available: lookup.Available, Found: lookup.Found, Retained: lookup.Retained,
		Publication: fleet.StrategyPublication{SnapshotRevision: string(lookup.Publication.SnapshotRevision), Epoch: lookup.Publication.PublicationEpoch},
	}
	for _, plan := range lookup.Plans {
		facts.Plans = append(facts.Plans, fleet.StrategyPlanRef{
			Tenant: plan.Plan.TenantID, Business: plan.Plan.BusinessID, QueryGroup: string(plan.QueryGroup),
			ObjectDigest: string(plan.ObjectDigest), SnapshotRevision: string(plan.SnapshotRevision),
			QueryRevision: string(plan.QueryRevision), ScheduleRevision: string(plan.ScheduleRevision),
		})
	}
	for _, disposition := range lookup.Dispositions {
		facts.Dispositions = append(facts.Dispositions, fleet.StrategyDisposition{
			Scope: disposition.Scope, LevelID: disposition.LevelID, Disposition: string(disposition.Disposition),
			Reason: disposition.Reason, FieldPath: disposition.FieldPath,
		})
	}
	return facts
}

// leaderDiscovery is the one question the forwarder asks: who leads, and
// where. The view stream's discovery answers it from the ownership store,
// and the endpoint it names is the Leader's HTTP listener -- the stream
// shares it.
type leaderDiscovery interface {
	Leader(ctx context.Context) (viewstream.LeaderEndpoint, string, error)
}

// strategyStandingForwardTimeout bounds the one hop. The Leader answers from
// memory; a hop that takes longer is a Leader that is not answering, and
// the reader is told that rather than kept waiting.
const strategyStandingForwardTimeout = 2 * time.Second

// leaderForwarder hands a request to the Leader's HTTP listener once. It
// marks the request so the Leader answers or refuses it and never hands it
// on; it copies the Leader's status and body back as they are. No Leader
// -- no lease, a lease holder without a registration, a registration
// without an endpoint -- is the view stream's word for which, and the
// caller carries it in the refusal.
func leaderForwarder(discovery leaderDiscovery, replica string, client *http.Client) fleet.LeaderForward {
	if discovery == nil {
		return nil
	}
	if client == nil {
		client = &http.Client{Timeout: strategyStandingForwardTimeout}
	}
	return func(response http.ResponseWriter, request *http.Request) (bool, string) {
		leader, miss, err := discovery.Leader(request.Context())
		if err != nil {
			return false, viewstream.MissDiscoveryFailed
		}
		if miss != "" {
			return false, miss
		}
		if _, _, splitErr := net.SplitHostPort(leader.Endpoint); splitErr != nil {
			return false, viewstream.MissLeaderNoEndpoint
		}
		ctx, cancel := context.WithTimeout(request.Context(), strategyStandingForwardTimeout)
		defer cancel()
		forwarded, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+leader.Endpoint+request.URL.RequestURI(), nil)
		if err != nil {
			return false, "FORWARD_FAILED"
		}
		forwarded.Header.Set(fleet.ForwardedHeader(), replica)
		reply, err := client.Do(forwarded)
		if err != nil {
			return false, "FORWARD_FAILED"
		}
		defer reply.Body.Close()
		if contentType := reply.Header.Get("Content-Type"); contentType != "" {
			response.Header().Set("Content-Type", contentType)
		}
		response.Header().Set("X-Alarmd-Answered-By", leader.WorkerID)
		response.WriteHeader(reply.StatusCode)
		_, _ = io.Copy(response, reply.Body)
		return true, ""
	}
}

// strategyStandingReplica is the name a replica answers under: its Worker
// id, as every other fact of the fleet names it.
func strategyStandingReplica(workerID string) string {
	return strings.TrimSpace(workerID)
}
