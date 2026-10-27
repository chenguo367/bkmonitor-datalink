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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/editclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

func testRecoveries(t *testing.T) *openalerts.Recoveries {
	t.Helper()
	recoveries, err := openalerts.NewRecoveries(redistest.Start(t).Client(), "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	return recoveries
}

// sinkEvent is an event of the kind given the sink acknowledges for a
// series of strategy in tenant.
func sinkEvent(tenant, strategy, fingerprint, kind string) contract.TriggerEventV1 {
	return contract.TriggerEventV1{TenantID: tenant, PlanRef: contract.RuntimePlanRefV1{StrategyID: strategy}, DedupeMD5: fingerprint,
		EventKind: kind, StrategyRef: &contract.StrategySnapshotRef{}}
}

// noteReopen has the sink acknowledge a RECOVERY of fingerprint at
// recoveredAt and an ABNORMAL of it at reopenedAt: the series recovered and
// fired again.
func noteReopen(recoveries *openalerts.Recoveries, recoveredAt, reopenedAt time.Time, tenant, strategy, fingerprint string) {
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent(tenant, strategy, fingerprint, contract.TriggerEventRecovery)}, recoveredAt)
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent(tenant, strategy, fingerprint, contract.TriggerEventAbnormal)}, reopenedAt)
}

// An edit removed the Level an alert stands at while its series is healthy
// at the Levels kept: the series says RECOVERY at a kept Level every cycle,
// which passes the open-alert gate (the set answers membership, not
// severity) and is an orphan at the link. Those recoveries are no reopen,
// and the edit close closes the alert in its first round however short the
// cycle - before, with a cycle within the clock skew, every round held it.
// The sink and the control leader each have their own handle on the
// record, as in production.
func TestALevelRemovedClosesAnAlertWhoseSeriesRecoversEveryCycle(t *testing.T) {
	const cycle = 20 * time.Second
	fixture := newEditFixture()
	server := redistest.Start(t)
	sink, err := openalerts.NewRecoveries(server.Client(), "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	if fixture.loop.recoveries, err = openalerts.NewRecoveries(server.Client(), "alarmd-test"); err != nil {
		t.Fatal(err)
	}
	now := fixture.bundle.dependencies.Now()
	fixture.bundle.dependencies.Now = func() time.Time { return now }
	plan := editPlan()
	plan.Levels = []contract.LevelDefinitionV2{{LevelID: 1}, {LevelID: 3}}
	fixture.plans.facts["10"] = plan
	fixture.threshold("at-warning", 8, "warning", "ip", "device")
	fingerprint := fixture.link.records["at-warning"].Fingerprint
	const rounds = 6
	for round := 1; round <= rounds && len(fixture.closed()) == 0; round++ {
		noteReopens(sink, []contract.TriggerEventV1{sinkEvent("system", "10", fingerprint, contract.TriggerEventRecovery)}, now)
		fixture.step()
		now = now.Add(cycle)
	}
	fixture.wantClosed(t, map[string]string{"at-warning": "level_removed"})
	if stats := fixture.loop.Stats(); stats[editclose.OutcomeReopenedSinceRead] != 0 || stats[editclose.OutcomeCloseSent] != 1 {
		t.Fatalf("stats %+v, want the alert closed in its first round and never held", stats)
	}
}

// The edit close's per-alert path (L40): an alert whose series recovered
// after the listing began and fired again may have ended, and the
// fingerprint's active alert be the new one, which the close would end. It
// is held back and counted. An ABNORMAL with no recovery before it, a
// recovery with no ABNORMAL after it, and a reopen whose recovery came long
// before the listing are closed as before.
func TestTheEditCloseHoldsAnAlertReopenedSinceTheListing(t *testing.T) {
	fixture := newEditFixture()
	recoveries := testRecoveries(t)
	fixture.loop.recoveries = recoveries
	plan := editPlan()
	plan.DimensionFields = []string{"ip"}
	fixture.plans.facts["10"] = plan
	for _, id := range []string{"reopened", "abnormal-only", "recovered-only", "reopened-long-ago"} {
		fixture.threshold(id, 8, "warning", "ip", "device")
	}
	fingerprint := func(id string) string { return fixture.link.records[id].Fingerprint }
	now := fixture.bundle.dependencies.Now()
	noteReopen(recoveries, now, now.Add(time.Second), "system", "10", fingerprint("reopened"))
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("system", "10", fingerprint("abnormal-only"), contract.TriggerEventAbnormal)}, now)
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("system", "10", fingerprint("recovered-only"), contract.TriggerEventRecovery)}, now)
	noteReopen(recoveries, now.Add(-time.Hour/2), now.Add(-time.Hour/2+time.Minute), "system", "10", fingerprint("reopened-long-ago"))
	fixture.step()
	fixture.wantClosed(t, map[string]string{"abnormal-only": "dimensions_changed", "recovered-only": "dimensions_changed",
		"reopened-long-ago": "dimensions_changed"})
	if stats := fixture.loop.Stats(); stats[editclose.OutcomeReopenedSinceRead] != 1 || stats[editclose.OutcomeCloseSent] != 3 {
		t.Fatalf("stats %+v, want one held as reopened_since_read and three sent", stats)
	}
}

// The inactive close's listing (L40): a listed alert reopened after the
// calibration began is held back and counted, the others close; with the
// record unreadable every close is held, as recovery_unknown.
func TestTheInactiveCloseHoldsAListedAlertReopenedSinceTheRead(t *testing.T) {
	overtaken := maintenanceAlert("native", "critical")
	other := openalerts.Alert{AlertID: "other", EventSourceID: "native", Severity: "critical", Fingerprint: strings.Repeat("c", 32)}
	f := newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(8, 0), maintenanceTestIndex{trusted: true, alerts: []openalerts.Alert{overtaken, other}})
	server := redistest.Start(t)
	recoveries, _ := openalerts.NewRecoveries(server.Client(), "alarmd-test")
	f.m.recoveries = recoveries
	noteReopen(recoveries, maintenanceTime(8, 0), maintenanceTime(8, 0).Add(time.Second), "tenant-a", "123", overtaken.Fingerprint)
	f.m.step(context.Background())
	sent := sentCloses(f.writer)
	if len(sent) != 1 || sent[0].Fingerprint != other.Fingerprint || f.m.counts[closeOutcomeReopenedSinceRead] != 1 {
		t.Fatalf("sent %+v, counts %v; want the overtaken alert held and the other closed", sent, f.m.counts)
	}
	server.Stop()
	f.advance(2 * time.Minute)
	f.m.step(context.Background())
	if len(sentCloses(f.writer)) != 1 || f.m.counts[closeOutcomeRecoveryUnknown] == 0 {
		t.Fatalf("an unreadable record sent %d, counts %v", len(sentCloses(f.writer)), f.m.counts)
	}
}

// The inactive close's index set stands in from a read whose start is
// recorded: a member reopened after the read began is held.
func TestTheIndexSetStandInHoldsAMemberReopenedSinceTheRead(t *testing.T) {
	f := newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(8, 0), maintenanceTestIndex{trusted: true,
		reconcileErr: errors.New("reconcile: link answered 503"), members: []string{ourMember, foreignMember}})
	recoveries := testRecoveries(t)
	f.m.recoveries = recoveries
	noteReopen(recoveries, maintenanceTime(8, 0), maintenanceTime(8, 0).Add(time.Second), "tenant-a", "123", ourMember)
	f.m.step(context.Background())
	sent := sentCloses(f.writer)
	if len(sent) != 1 || sent[0].Fingerprint != foreignMember || f.m.counts[closeOutcomeReopenedSinceRead] != 1 {
		t.Fatalf("sent %+v, counts %v; want our member held and the other closed", sent, f.m.counts)
	}
	if _, index := f.m.cache.EvidenceStarted(openalerts.StrategyKey{TenantID: "tenant-a", StrategyID: "123"}); index.IsZero() || index.After(maintenanceTime(8, 0)) {
		t.Fatalf("the set's read start is %v, want one no later than the read", index)
	}
}

// A close from the index set is judged from the set's read, not from the
// listing - which here never landed: a member reopened before the set's
// read began, past the skew, closes.
func TestTheIndexSetStandInClosesAMemberReopenedBeforeItsRead(t *testing.T) {
	f := newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(8, 0), maintenanceTestIndex{trusted: true,
		reconcileErr: errors.New("reconcile: link answered 503"), members: []string{ourMember, foreignMember}})
	recoveries := testRecoveries(t)
	f.m.recoveries = recoveries
	noteReopen(recoveries, maintenanceTime(7, 50), maintenanceTime(7, 51), "tenant-a", "123", ourMember)
	f.m.step(context.Background())
	if sent := sentCloses(f.writer); len(sent) != 2 || f.m.counts[closeOutcomeReopenedSinceRead] != 0 {
		t.Fatalf("sent %+v, counts %v; want both members closed", sent, f.m.counts)
	}
}

// The target scope close asks the same record before each close, from the
// read its close was decided from: the listing for a listed alert, the set
// for one addressed from it. A recovery alone holds nothing.
func TestTheScopeCloseGuardAsksTheRecoveryRecord(t *testing.T) {
	f := newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(8, 0), maintenanceTestIndex{trusted: true,
		reconcileErr: errors.New("reconcile: link answered 503"), members: []string{ourMember}})
	recoveries := testRecoveries(t)
	guard := recoveryGuard(f.m.cache, recoveries, nil)
	key := openalerts.StrategyKey{TenantID: "tenant-a", StrategyID: "123"}
	ctx := testwait.Context(t)
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("tenant-a", "123", ourMember, contract.TriggerEventRecovery)}, maintenanceTime(8, 0))
	if word := guard(ctx, key, ourMember, true); word != "" {
		t.Fatalf("a member that only recovered held as %q", word)
	}
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("tenant-a", "123", ourMember, contract.TriggerEventAbnormal)}, maintenanceTime(8, 0).Add(time.Second))
	if word := guard(ctx, key, ourMember, true); word != openalerts.HeldReopenedSinceRead {
		t.Fatalf("a member reopened since the set's read admitted as %q", word)
	}
}

// Only what reaches the link is recorded: a RECOVERY then an ABNORMAL of a
// fingerprint, not an ABNORMAL alone, a compatibility envelope or an event
// without a fingerprint.
func TestTheSinkRecordsOnlyItsReopens(t *testing.T) {
	recoveries := testRecoveries(t)
	legacy := func(kind string) contract.TriggerEventV1 {
		event := sinkEvent("tenant-a", "7", strings.Repeat("e", 32), kind)
		event.LegacyOutput = &contract.LegacyEventContext{}
		return event
	}
	at := time.Unix(1_800_000_000, 0)
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("tenant-a", "7", strings.Repeat("a", 32), contract.TriggerEventRecovery),
		legacy(contract.TriggerEventRecovery), sinkEvent("tenant-a", "7", "", contract.TriggerEventRecovery)}, at)
	noteReopens(recoveries, []contract.TriggerEventV1{sinkEvent("tenant-a", "7", strings.Repeat("a", 32), contract.TriggerEventAbnormal),
		sinkEvent("tenant-a", "7", strings.Repeat("c", 32), contract.TriggerEventAbnormal), legacy(contract.TriggerEventAbnormal),
		sinkEvent("tenant-a", "7", "", contract.TriggerEventAbnormal)}, at.Add(time.Second))
	held := recoveries.Held(testwait.Context(t), "tenant-a", "7", []string{strings.Repeat("a", 32), strings.Repeat("c", 32), strings.Repeat("e", 32)}, at)
	if len(held) != 1 || held[strings.Repeat("a", 32)] != openalerts.HeldReopenedSinceRead {
		t.Fatalf("held %v, want only the reopen recorded", held)
	}
	noteReopens(nil, []contract.TriggerEventV1{sinkEvent("tenant-a", "7", strings.Repeat("a", 32), contract.TriggerEventRecovery)}, at)
}
