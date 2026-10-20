// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package access

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scopeclose"
)

// overlayFuller stands for CMDB placing a record's host by its agent at
// 192.0.2.172 while the record reports another address.
type overlayFuller struct{}

func (overlayFuller) Name() string { return "host_overlay" }

func (overlayFuller) Fill(dimensions map[string]json.RawMessage, facts *admission.Facts) {
	if _, named := dimensions["bk_agent_id"]; named {
		facts.HostOverlay = map[string]json.RawMessage{"bk_target_ip": json.RawMessage(`"192.0.2.172"`),
			"bk_target_cloud_id": json.RawMessage(`"0"`), "bk_host_id": json.RawMessage(`"720002"`)}
	}
}

// heldAlerts is the consumer's open alert set as the close reads it: every
// fingerprint held, each listed as an alert of this deployment's source.
type heldAlerts map[string]struct{}

func (heldAlerts) Trusted() bool { return true }
func (set heldAlerts) MemberCount(openalerts.StrategyKey) (int, bool) {
	return len(set), true
}
func (set heldAlerts) Holds(_ openalerts.StrategyKey, fingerprint string) (bool, bool) {
	_, held := set[fingerprint]
	return held, true
}
func (set heldAlerts) ActiveAlerts(openalerts.StrategyKey) []openalerts.Alert {
	alerts := make([]openalerts.Alert, 0, len(set))
	for fingerprint := range set {
		alerts = append(alerts, openalerts.Alert{AlertID: "alert-" + fingerprint, EventSourceID: "own", Fingerprint: fingerprint})
	}
	return alerts
}
func (heldAlerts) OwnEventSourceID() string { return "own" }

type recordedCloses struct{ sent []linkdoutput.CloseRequest }

func (w *recordedCloses) WriteCloseBatch(_ context.Context, batch []linkdoutput.CloseRequest) error {
	w.sent = append(w.sent, batch...)
	return nil
}

// forwardingSink hands the access path's observations to a real close, as
// the production sink does.
type forwardingSink struct{ closer *scopeclose.Closer }

func (sink forwardingSink) Screen(plan execution.PlanIdentity) string {
	return sink.closer.Screen(openalerts.StrategyKey{TenantID: plan.TenantID, StrategyID: plan.StrategyID})
}
func (sink forwardingSink) Observe(drop ScopeDrop) {
	sink.closer.Observe(scopeclose.Drop{TenantID: drop.Plan.TenantID, BusinessID: drop.Plan.BusinessID,
		StrategyID: drop.Plan.StrategyID, Fingerprint: drop.Fingerprint, StrategyRevision: drop.StrategyRevision,
		GlobalBusiness: drop.GlobalBusiness, Round: drop.Round, IntervalSeconds: drop.IntervalSeconds,
		ReadOffset: drop.ReadOffset, CloseReason: drop.CloseReason})
}
func (forwardingSink) Count(ScopeDropReporter, string, int) {}

// A superseded series is offered to the close on every Slot that admits it,
// not only on the first: an offer the close refuses at its entry bound is
// counted as memory_full and only delays the close. Once room frees up, the
// series' next Slots offer the old fingerprint again and it is closed exactly
// once, with its own reason.
func TestARefusedSupersededOfferIsMadeAgainOnTheNextSlot(t *testing.T) {
	output := planOutput{strategyID: "42", revision: 3,
		identity: &contract.MonitorOutputIdentity{DimensionFields: []string{"bk_agent_id", "bk_target_cloud_id", "bk_target_ip"}}}
	record := map[string]json.RawMessage{"bk_agent_id": json.RawMessage(`"agent-live"`),
		"bk_target_cloud_id": json.RawMessage(`"0"`), "bk_target_ip": json.RawMessage(`"198.51.100.9"`)}
	busyA, busyB := "0000000000000000000000000000000a", "0000000000000000000000000000000b"
	now := time.Unix(1700000000, 0)
	closer := scopeclose.New(scopeclose.Options{Now: func() time.Time { return now }, MaxEntries: 2, Batch: 8})
	writer := &recordedCloses{}
	chain := admission.NewChain([]admission.Fuller{admission.IdentityFuller{}, overlayFuller{}}, nil)

	var plan execution.PlanIdentity
	slot := func(round int64) {
		t.Helper()
		var adapter *seriesAdapter
		adapter, plan = scopeDropAdapter(t, chain, admission.PlanContext{}, nil, output)
		adapter.scopeSink, adapter.round = forwardingSink{closer: closer}, round
		deliverSeries(t, adapter, record)
		closer.Step(context.Background())
		now = now.Add(time.Minute)
	}
	_, plan = scopeDropAdapter(t, chain, admission.PlanContext{}, nil, output)
	old := output.fingerprint(plan.BusinessID, record)
	if old == "" {
		t.Fatal("the fixture record has no fingerprint")
	}
	closer.Bind(heldAlerts{old: {}, busyA: {}, busyB: {}}, writer)
	busy := func(fingerprint string, round int64) scopeclose.Drop {
		return scopeclose.Drop{TenantID: plan.TenantID, BusinessID: plan.BusinessID, StrategyID: plan.StrategyID,
			Fingerprint: fingerprint, StrategyRevision: 3, Round: round}
	}

	// Two other alerts fill the close's entries: the series' offer is refused.
	closer.Observe(busy(busyA, 1700000000))
	closer.Observe(busy(busyB, 1700000000))
	slot(1700000000)
	if got := closer.Stats()[scopeclose.OutcomeMemoryFull]; got != 1 {
		t.Fatalf("memory_full = %d after the full close refused the offer, want 1", got)
	}
	// The two close and leave: room again.
	closer.Observe(busy(busyA, 1700000060))
	closer.Observe(busy(busyB, 1700000060))
	slot(1700000060)
	slot(1700000120)
	slot(1700000180)
	var superseded []linkdoutput.CloseRequest
	for _, request := range writer.sent {
		if request.Fingerprint == old {
			superseded = append(superseded, request)
		}
	}
	if len(superseded) != 1 || superseded[0].Reason != linkdoutput.CloseReasonSupersededIdentity || superseded[0].AlertInstanceID != "alert-"+old {
		t.Fatalf("the superseded alert got closes %+v (all sent %+v), want exactly one with reason %s",
			superseded, writer.sent, linkdoutput.CloseReasonSupersededIdentity)
	}
	if got := closer.Stats()[scopeclose.OutcomeSupersededCloseSent]; got != 1 {
		t.Fatalf("superseded_identity_close_sent = %d, want 1", got)
	}
}
