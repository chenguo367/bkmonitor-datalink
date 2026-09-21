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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

type mapView map[execution.QueryGroupIdentity]viewstream.Entry

func (view mapView) Entry(queryGroup execution.QueryGroupIdentity) (viewstream.Entry, bool) {
	entry, ok := view[queryGroup]
	return entry, ok
}

// Each of the three checks fails on its own and names itself; only all three
// holding yields the timeline revision to hint with. The record's side is
// the lease, never the view: an entry the renewal does not vouch for is not
// executed from the view whatever the entry says.
func TestTheViewGateFailsEachCheckOnItsOwnAndPassesOnlyAllThree(t *testing.T) {
	gate := newViewExecutionGate()
	entry := viewstream.Entry{QueryGroup: "qg-1",
		Content:    &viewstream.Content{ObjectDigest: "obj-a"},
		Assignment: viewstream.Assignment{DesiredWorkerID: "w1", Revision: 3, ContentScope: "obj-a", TimelineRecordRevision: 12}}
	gate.attach(mapView{"qg-1": entry, "qg-draining": {QueryGroup: "qg-draining", Assignment: entry.Assignment}})
	good := ownership.Lease{ContentScope: "obj-a", TimelineRecordRevision: 12}
	cases := []struct {
		name    string
		group   execution.QueryGroupIdentity
		lease   ownership.Lease
		held    bool
		want    viewGateOutcome
		hinting uint64
	}{
		{"all three hold", "qg-1", good, true, viewGateExecutable, 12},
		{"no lease", "qg-1", good, false, viewGateNoLease, 0},
		{"not in the view", "qg-2", good, true, viewGateNotInView, 0},
		{"in the view without content", "qg-draining", good, true, viewGateNotInView, 0},
		{"renewal names another scope", "qg-1", ownership.Lease{ContentScope: "obj-old", TimelineRecordRevision: 12}, true, viewGateScopeMismatch, 0},
		{"renewal names the scope as pending", "qg-1", ownership.Lease{ContentScope: "obj-old", PendingContentScope: "obj-a", TimelineRecordRevision: 12}, true, viewGateExecutable, 12},
		{"record has not said the timeline", "qg-1", ownership.Lease{ContentScope: "obj-a"}, true, viewGateTimelineUnsaid, 0},
		{"record says another timeline", "qg-1", ownership.Lease{ContentScope: "obj-a", TimelineRecordRevision: 11}, true, viewGateTimelineMismatch, 0},
	}
	for _, testCase := range cases {
		hint, outcome := gate.judge(testCase.group, testCase.lease, testCase.held)
		if outcome != testCase.want || hint != testCase.hinting {
			t.Errorf("%s: outcome %s hint %d, want %s hint %d", testCase.name, outcome, hint, testCase.want, testCase.hinting)
		}
	}
	// The view has not said the timeline either: not executable, whatever
	// the record says, because the third check compares two numbers.
	unsaid := entry
	unsaid.Assignment.TimelineRecordRevision = 0
	gate.attach(mapView{"qg-1": unsaid})
	if hint, outcome := gate.judge("qg-1", good, true); outcome != viewGateTimelineUnsaid || hint != 0 {
		t.Errorf("view without a timeline revision: outcome %s hint %d, want %s", outcome, hint, viewGateTimelineUnsaid)
	}
}

type hintCapturingCatalog struct {
	productionPhaseTwoSlotCatalog
	hints []uint64
}

func (catalog *hintCapturingCatalog) NextSlotAfter(ctx context.Context, _ execution.QueryGroupIdentity, at execution.EvaluationTime) (execution.EvaluationTime, error) {
	catalog.hints = append(catalog.hints, controlplane.TimelineRevisionHint(ctx))
	return at + 60, nil
}

// The gated catalog carries the hint on a read the gate lets through and
// none on one it does not, decides per read, counts the latest outcome per
// Query Group for the receipt, and forgets a Query Group let go.
func TestTheGatedCatalogHintsOnlyWhenTheGateLetsTheReadThrough(t *testing.T) {
	gate := newViewExecutionGate()
	entry := viewstream.Entry{QueryGroup: "qg-1", Content: &viewstream.Content{ObjectDigest: "obj-a"},
		Assignment: viewstream.Assignment{DesiredWorkerID: "w1", Revision: 3, ContentScope: "obj-a", TimelineRecordRevision: 12}}
	gate.attach(mapView{"qg-1": entry})
	store := newViewGateTestStore(t)
	session := openViewGateTestSession(t, store, "qg-1", 12, "obj-a")
	next := &hintCapturingCatalog{}
	catalog := &viewGatedCatalog{next: next, gate: gate, queryGroup: "qg-1", session: session}
	if _, err := catalog.NextSlotAfter(context.Background(), "qg-1", 60); err != nil {
		t.Fatal(err)
	}
	if len(next.hints) != 1 || next.hints[0] != 12 {
		t.Fatalf("a read the gate let through carried hints %v, want [12]", next.hints)
	}
	if gate.SwitchedQueryGroups() != 1 || gate.Counts()[string(viewGateExecutable)] != 1 {
		t.Fatalf("after an executable read the gate counts %d switched, %v", gate.SwitchedQueryGroups(), gate.Counts())
	}
	// The view moves on to a timeline the record has not confirmed: the next
	// read carries no hint and the count falls, with no new version needed.
	moved := entry
	moved.Assignment.TimelineRecordRevision = 13
	gate.attach(mapView{"qg-1": moved})
	if _, err := catalog.NextSlotAfter(context.Background(), "qg-1", 120); err != nil {
		t.Fatal(err)
	}
	if len(next.hints) != 2 || next.hints[1] != 0 {
		t.Fatalf("a read the gate refused carried hints %v, want the second 0", next.hints)
	}
	if gate.SwitchedQueryGroups() != 0 || gate.Counts()[string(viewGateTimelineMismatch)] != 1 {
		t.Fatalf("after a refused read the gate counts %d switched, %v", gate.SwitchedQueryGroups(), gate.Counts())
	}
	gate.forget("qg-1")
	if counts := gate.Counts(); counts[string(viewGateTimelineMismatch)] != 0 || gate.SwitchedQueryGroups() != 0 {
		t.Fatalf("a forgotten Query Group still counts: %v", counts)
	}
}

func newViewGateTestStore(t *testing.T) *ownership.RedisStore {
	t.Helper()
	_, client := startPhaseTwoRedis(t)
	store, err := ownership.NewRedisStoreWithClient(client, "alarmd:test:gate:ownership")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// openViewGateTestSession places the Query Group on worker-1 with the given
// content scope and timeline revision, then opens worker-1's session on it,
// so the session's lease carries both from the record.
func openViewGateTestSession(t *testing.T, store *ownership.RedisStore, queryGroup execution.QueryGroupIdentity, timeline uint64, scope string) *ownership.Session {
	t.Helper()
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)
	authority, err := store.AcquireControlLeader(ctx, "control-1", now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishAssignment(ctx, authority, ownership.AssignmentDecision{
		QueryGroup: queryGroup, DesiredWorkerID: "worker-1", PlacementReason: ownership.PlacementRendezvous, DecidedAt: now,
		ContentScope: scope, TimelineRecordRevision: timeline,
	}); err != nil {
		t.Fatal(err)
	}
	session, err := ownership.OpenSession(ctx, store, queryGroup, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Release(ctx) })
	return session
}
