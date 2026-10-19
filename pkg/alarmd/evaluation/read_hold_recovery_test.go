// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package evaluation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The settled boundary of the recovery walk is the Slot's own: its
// evaluation time less the read hold its frozen contract carries. A series
// with state and a point three minutes before this one, under a three minute
// window and one recovery window, recovers when the Slot was read with no
// hold - the window's two holes are settled minutes with no anomaly - and
// does not when the contract froze the Slot with a ten minute hold, inside
// which those holes may still fill.
//
// The earlier point is older than this Level retains, so the window holds
// only this record: the series is known to be past its first position from
// its state alone, which is what a sparse series' recovery depends on once
// retention has dropped every point before its oldest window.
func TestTheContractsReadHoldSetsTheSettledBoundary(t *testing.T) {
	plan := compiledWindow(t, 3, 1)
	fingerprint := plan.Levels().At(0).Fingerprints().Detect
	const at = int64(6000)
	records := []contract.CanonicalRecordV2{{RecordID: strings.Repeat("b", 64), SourceTime: at, BusinessID: "2",
		DimensionIdentity: contract.DimensionIdentityV2{Digest: strings.Repeat("c", 64)},
		Values:            map[string]json.RawMessage{"value": json.RawMessage(`10`)}, Dimensions: map[string]json.RawMessage{}, ReceivedTime: at}}
	history := []execution.StateHistoryPoint{{RecordID: strings.Repeat("d", 64), SourceTime: at - 3*60,
		Levels: []execution.StateLevelFact{{LevelID: 5, DetectFingerprint: fingerprint, Result: execution.LevelFactNormal}}}}
	outcome := func(holdMillis int64) execution.LevelOutcomeKind {
		t.Helper()
		request := atSlot(t, plan, requestFixtureForPlan(t, plan, records, history), at)
		request.Header.Contract.ReadHoldMillis = holdMillis
		request.Inputs[0].Contract = request.Header.Contract
		result, err := newEvaluator(t).Evaluate(context.Background(), request)
		if err != nil {
			t.Fatalf("Evaluate(hold %d) = %v", holdMillis, err)
		}
		return result.Plans[0].LevelOutcomes[0].Outcome
	}
	if got := outcome(0); got != execution.LevelOutcomeRecovery {
		t.Fatalf("with no read hold the outcome is %s, want RECOVERY: settled holes did not count as non-anomalous", got)
	}
	if got := outcome(int64(10 * time.Minute / time.Millisecond)); got == execution.LevelOutcomeRecovery {
		t.Fatal("inside a ten minute read hold the walk recovered: the contract's hold did not reach the trigger")
	}
}

// atSlot moves the fixture's Slot to evaluation time at, with the facts and
// window that go with it.
func atSlot(t *testing.T, plan *strategy.CompiledPlan, request execution.EvaluationRequest, at int64) execution.EvaluationRequest {
	t.Helper()
	request.Header.Contract.Slot.EvaluationTime = execution.EvaluationTime(at)
	request.Header.Contract.ScheduleSegmentStart = execution.EvaluationTime(at)
	provider := strategy.NewStaticScheduleProvider(strategy.TimezoneResolverFunc(func(context.Context, string, string, string) (*time.Location, error) {
		return time.UTC, nil
	}))
	facts, err := provider.Resolve(context.Background(), []strategy.EffectiveTimeRequest{{TenantID: "tenant", BusinessID: "2",
		EvaluationTime: at, Requirement: plan.Levels().At(0).EffectiveTimeRequirement()}})
	if err != nil {
		t.Fatal(err)
	}
	request.Header.EffectiveTimeFacts[0].Fact = facts[0]
	request.Inputs[0].Contract = request.Header.Contract
	request.Inputs[0].Inputs[0].QueryWindow = execution.QueryWindow{Start: at - 60, End: at}
	return request
}
