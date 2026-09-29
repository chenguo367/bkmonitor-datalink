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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

func noRecheck(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	return execution.ProviderCompletion{}, nil
}

// Nothing configures the lookback: with no observation capacity - the
// container's memory is not known - it is not running and says why; with
// room, it runs on its share.
func TestTheLookbackRunsWhereverItHasAShare(t *testing.T) {
	owner := &lookbackOwnership{}
	engine, standing, err := buildLookback(config.ObservationCapacity{}, noRecheck, nil, owner, time.Now)
	if err != nil || engine != nil || standing != (lookbackStanding{Reason: lookbackNoObservationShare}) {
		t.Fatalf("no capacity: %v %+v %v", engine, standing, err)
	}
	engine, standing, err = buildLookback(config.ObservationCapacity{LookbackBytes: 1 << 20}, noRecheck, nil, owner, time.Now)
	if err != nil || engine == nil || standing != (lookbackStanding{Running: true}) || engine.Stats().MemoryBytes != 1<<20 {
		t.Fatalf("with a share: %v %+v %v", engine, standing, err)
	}
	// Every reason the scheduler refuses with is a series from the start.
	refusals := engine.Stats().PermitRefusals
	for _, reason := range append([]string{lookback.RefusedOther}, scheduler.LookbackRefusals...) {
		if count, present := refusals[reason]; !present || count != 0 {
			t.Fatalf("refusal %q is not counted from the start: %v", reason, refusals)
		}
	}
}

// The lookback's permit is the scheduler's: a refusal carries its reason,
// and a granted permit yields the moment a formal query has to wait.
func TestTheLookbackPermitYieldsToAWaitingFormalQuery(t *testing.T) {
	flights, err := scheduler.NewFlightCoordinatorWithRecovery(scheduler.RecoveryLimits{
		ProcessQueryPermits: 2, RecoveryQueryPermits: 1,
		ReadyQueueCapacity: 8, RecoveryQueueCapacity: 8,
		MaxQueuedItemsPerQG: 2,
		MaxReplaySlots:      3, MaxReplayAge: 10 * time.Minute,
		RetryMinDelay: time.Second, RetryMaxDelay: 8 * time.Second,
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	permit := lookbackPermit(flights)
	release, yield, refused := permit()
	if release == nil || yield == nil || refused != "" {
		t.Fatalf("an idle pool gave the lookback %v %v %q", release != nil, yield != nil, refused)
	}
	if _, _, again := permit(); again != scheduler.LookbackRefusedLimit {
		t.Fatalf("a second lookback permit of two = %q, want refused at the limit", again)
	}
	ctx := context.Background()
	formal, err := flights.AcquireQueryPermit(ctx, execution.SlotIdentity{QueryGroup: "a", EvaluationTime: 60},
		execution.OperationNormal, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer formal.Release()
	waiting := make(chan *scheduler.QueryPermit, 1)
	go func() {
		granted, err := flights.AcquireQueryPermit(ctx, execution.SlotIdentity{QueryGroup: "b", EvaluationTime: 60},
			execution.OperationNormal, time.Now().Add(time.Minute))
		if err != nil {
			t.Error(err)
		}
		waiting <- granted
	}()
	select {
	case <-yield:
	case <-time.After(5 * time.Second):
		t.Fatal("a formal query waits and the lookback permit was not asked to yield")
	}
	release()
	select {
	case granted := <-waiting:
		granted.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("the permit the lookback gave back did not reach the waiting query")
	}
}

func sampledLogQuery(t *testing.T, queryGroup execution.QueryGroupIdentity) lookback.Query {
	t.Helper()
	return lookback.Query{Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: queryGroup, EvaluationTime: 1_700_000_060}},
		Spec: execution.PhysicalQuerySpec{Digest: "physical", PlanFacts: execution.QueryPlanFacts{SourceSemantics: []string{lookback.SourceLogSearch},
			QueryList:     []execution.QueryClause{{TimeAggregation: execution.QueryFunction{Method: "count_over_time"}}},
			Normalization: execution.DatasetNormalizationSpec{CanonicalValueField: "value"}}},
		Operation: execution.OperationNormal, AttemptNo: 1}
}

// The bundle is what the lookback asks about ownership: nothing is owned
// before it is bound, a Query Group with a Runner is, and the moment its
// Runner is removed its waiting samples are dropped as owner_lost -- the
// call is in removeRunnerLocked, the one way the Runner set shrinks.
func TestAQueryGroupTheBundleStopsOwningLeavesTheLookback(t *testing.T) {
	owner := &lookbackOwnership{}
	engine, err := lookback.New(lookback.Options{SampleOneIn: 1, MemoryBytes: 1 << 20, Recheck: noRecheck, Owns: owner.owns,
		Permit: func() (func(), <-chan struct{}, string) { return nil, nil, "headroom" }})
	if err != nil {
		t.Fatal(err)
	}
	const queryGroup = execution.QueryGroupIdentity("qg-1")
	if owner.owns(queryGroup) {
		t.Fatal("owned before the bundle was bound")
	}
	bundle := &phaseTwoWorkerBundle{dependencies: phaseTwoWorkerBundleDependencies{Lookback: engine},
		runners: map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{}}
	owner.bind(bundle)
	engine.Begin(sampledLogQuery(t, queryGroup)).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if stats := engine.Stats(); stats.Samples[lookback.SourceLogSearch][lookback.OutcomeOwnerLost] != 1 || stats.Pending != 0 {
		t.Fatalf("a read of a Query Group with no Runner was kept: %+v", stats.Samples[lookback.SourceLogSearch])
	}
	bundle.mu.Lock()
	bundle.setRunnerLocked(queryGroup, &phaseTwoQueryGroupLifecycle{})
	bundle.mu.Unlock()
	engine.Begin(sampledLogQuery(t, queryGroup)).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	if stats := engine.Stats(); stats.Pending != 1 {
		t.Fatalf("a read of an owned Query Group was not kept: %+v", stats.Samples[lookback.SourceLogSearch])
	}
	bundle.mu.Lock()
	bundle.removeRunnerLocked(queryGroup)
	bundle.mu.Unlock()
	stats := engine.Stats()
	if stats.Pending != 0 || stats.PendingBytes != 0 || stats.Rechecks[lookback.SourceLogSearch]["t90"][lookback.RecheckOwnerLost] != 1 {
		t.Fatalf("after the Runner left: pending %d bytes %d rechecks %v", stats.Pending, stats.PendingBytes, stats.Rechecks[lookback.SourceLogSearch]["t90"])
	}
	// A bundle without the lookback removes Runners as before.
	plain := &phaseTwoWorkerBundle{runners: map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{queryGroup: {}}}
	plain.mu.Lock()
	plain.removeRunnerLocked(queryGroup)
	plain.mu.Unlock()
}

// lookback.get says whether the answering process runs the lookback and
// why not, and carries its counts when it does.
func TestLookbackGetSaysWhetherItRunsAndCarriesTheCounts(t *testing.T) {
	read := func(op obchannel.Operation) cliLookbackReading {
		t.Helper()
		out := op.Run(context.Background(), obchannel.Params{})
		reading, ok := out.Value.(cliLookbackReading)
		if !ok || !out.Complete || op.ID != "lookback.get" || !op.Targetable {
			t.Fatalf("lookback.get = %+v", out)
		}
		return reading
	}
	if off := read(cliLookbackOperation(nil, lookbackStanding{Reason: lookbackNoObservationShare})); off.Running ||
		off.Reason != lookbackNoObservationShare || off.Stats != nil {
		t.Fatalf("off = %+v", off)
	}
	engine, err := lookback.New(lookback.Options{MemoryBytes: 4096, Recheck: noRecheck,
		Owns: func(execution.QueryGroupIdentity) bool { return true }, Permit: func() (func(), <-chan struct{}, string) { return nil, nil, "headroom" }})
	if err != nil {
		t.Fatal(err)
	}
	on := read(cliLookbackOperation(engine, lookbackStanding{Running: true}))
	if !on.Running || on.Stats == nil || on.Stats.MemoryBytes != 4096 || len(on.Stats.Samples[lookback.SourceLogSearch]) != len(lookback.SampleOutcomes) {
		t.Fatalf("on = %+v", on)
	}
}
