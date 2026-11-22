// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A page left unrenewed past half its lifetime by the worker's clock while
// its Plan's rounds go on is a fallback_write, and the replica keeps it for
// its snapshot: through the production wiring - Execute on the real State
// store over redis-server, the coordinator's observations handed on to the
// fleet tracker every observation of a replica passes through - the
// tracker's list, which the replica snapshot carries as fallback_writes,
// names the Plan's page with its line's facts, in the Slot it was found in.
// Before, while the page was renewed in time, the list is empty.
func TestAPageUnrenewedPastHalfItsLifetimeIsKeptForTheReplicasSnapshot(t *testing.T) {
	const hosts = 4
	rig := newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 0, nil, 16<<20)
	tracker := fleet.NewTracker(nil, "replica-1", time.Now)
	rig.observed.next = tracker
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 3; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: err=%v", at, err)
		}
	}
	if rows, evicted := tracker.FallbackWrites(); rows != nil || evicted != 0 {
		t.Fatalf("pages renewed in time, and the replica keeps %+v (%d pushed out)", rows, evicted)
	}
	mark := rig.storedMark(at)
	ttl := mark.Renewed[0].TTL
	// Past half the page's lifetime by the worker's clock, the Plan's rounds
	// one interval apart: renewals did not keep the page alive.
	rig.wallAhead.Store(int64(time.Duration(ttl/2)*time.Millisecond + time.Minute))
	last := at
	at += 60
	if completed, err := rig.run(at, nil, nil, false); !completed {
		t.Fatalf("the Slot the page fell back in: err=%v", err)
	}
	if counted := rig.observed.renewal(observability.SegmentRenewalFallbackWrite); counted != 1 {
		t.Fatalf("%d pages counted fallback_write, want the Plan's one page", counted)
	}
	rows, evicted := tracker.FallbackWrites()
	if len(rows) != 1 || evicted != 0 {
		t.Fatalf("the replica keeps %+v (%d pushed out), want the Plan's one page", rows, evicted)
	}
	row, plan := rows[0], planIdentity()
	if row.QueryGroup != string(frozenContract().Slot.QueryGroup) || row.EvaluationTime != int64(at) ||
		row.TenantID != plan.TenantID || row.BusinessID != plan.BusinessID || row.StrategyID != plan.StrategyID ||
		row.Generation != "state-v1" || row.Page != 0 || row.Result != observability.SegmentRenewalFallbackWrite ||
		row.SinceRenewalMillis < ttl/2 || row.RenewalTTLMillis != ttl || row.IntervalSeconds != 60 ||
		row.LastMarkEvaluationTime != int64(last) || row.Suppressed != 0 || row.At.IsZero() {
		t.Fatalf("the replica keeps %+v, want the Plan's page 0 of state-v1 in the Slot at %d, past half its %d ms, its mark last written at %d",
			row, at, ttl, last)
	}
}
