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
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A replica's snapshot carries the pages its rounds wrote whole because
// renewals did not keep them alive, as the worker's fallback line reported
// them to the tracker the replica's observations pass through, and omits
// them before there is one.
func TestTheSnapshotCarriesTheFallbackWrites(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Unix(20_000, 0) }
	tracker := fleet.NewTracker(nil, "replica-1", now)
	publisher := &fleetPublisher{tracker: tracker, replica: "replica-1", now: now,
		owned: func() []execution.QueryGroupIdentity { return nil }}
	if encoded, _ := json.Marshal(publisher.snapshot(ctx)); strings.Contains(string(encoded), "fallback_write") {
		t.Fatalf("a replica that wrote no page whole: %s", encoded)
	}
	facts := observability.SegmentFallbackFacts{TenantID: "system", BusinessID: "2", StrategyID: "7", Generation: "g-1", Page: 2,
		Result: observability.SegmentRenewalFallbackWrite, SinceRenewalMillis: 31_000, RenewalTTLMillis: 60_000, TTLNowMillis: 60_000,
		IntervalSeconds: 60, LastMarkEvaluationTime: 19_880}
	observability.ObserveSegmentFallback(ctx, tracker, observability.TraceFields{QueryGroupKey: "qg-1", EvaluationTime: 19_940},
		observability.OperationNormal, facts)
	snapshot := publisher.snapshot(ctx)
	want := []fleet.FallbackWriteFacts{{At: now(), QueryGroup: "qg-1", EvaluationTime: 19_940, SegmentFallbackFacts: facts}}
	if !reflect.DeepEqual(snapshot.FallbackWrites, want) || snapshot.FallbackWritesEvicted != 0 {
		t.Fatalf("snapshot carries %+v (%d pushed out), want %+v", snapshot.FallbackWrites, snapshot.FallbackWritesEvicted, want)
	}
}
