// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

type periodCensus struct {
	peaks []observability.CostRetainedPeak
}

func (census periodCensus) RetainedPeaks() []observability.CostRetainedPeak { return census.peaks }
func (periodCensus) Retain([]string)                                        {}
func (periodCensus) Overflow() uint64                                       { return 0 }

// The heartbeat's load carries the coordinator's high-water, the window it
// covers, its share, and the peaks of the owned Query Groups whose period is
// longer than the window or not yet known - a Worker an hour old has seen
// its minutely Query Groups run and maybe not its daily one.
func TestTheWorkerLoadCarriesTheRetainedHighWaterAndWhatItMayNotHaveSeen(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	cfg.PhaseTwo.Coordinator.MaxRetainedBytes = 1 << 30
	costs := newWorkerCostSource(periodCensus{peaks: []observability.CostRetainedPeak{
		{QueryGroupKey: "minutely", RetainedBytesPeak: 10, Interval: time.Minute},
		{QueryGroupKey: "hourly", RetainedBytesPeak: 20, Interval: time.Hour},
		{QueryGroupKey: "daily", RetainedBytesPeak: 40, Interval: 24 * time.Hour},
		{QueryGroupKey: "unknown", RetainedBytesPeak: 80},
	}}, nil, nil)
	costs.boundByOwned(func() []execution.QueryGroupIdentity {
		return []execution.QueryGroupIdentity{"minutely", "hourly", "daily", "unknown"}
	})
	costs.Costs()
	bundle := &phaseTwoWorkerBundle{dependencies: phaseTwoWorkerBundleDependencies{Config: cfg}, workerCosts: costs,
		capacity: func() *fleet.Capacity { return &fleet.Capacity{PermitBudget: 4} }}
	if load := bundle.loadFacts(); load == nil || load.RetainedHigh != nil {
		t.Fatalf("load without a coordinator = %+v, want no high-water rather than a zero", load)
	}
	window := 40 * time.Minute
	bundle.retained = func() (uint64, time.Duration, uint64) { return 300 << 20, window, 672 << 20 }
	load := bundle.loadFacts()
	if load == nil || load.RetainedHigh == nil || load.RetainedHigh.Bytes != 300<<20 || load.RetainedHigh.WindowSeconds != 2400 ||
		load.RetainedHigh.LongPeriodBytes != 20+40+80 || load.RetainedShareBytes != 672<<20 {
		t.Fatalf("load forty minutes in = %+v (high %+v), want the hourly, daily and unknown peaks added", load, load.RetainedHigh)
	}
	window = time.Hour
	if load := bundle.loadFacts(); load.RetainedHigh.LongPeriodBytes != 40+80 {
		t.Fatalf("an hour in the long-period peaks = %d, want the daily and the unknown", load.RetainedHigh.LongPeriodBytes)
	}
	bundle.retained = func() (uint64, time.Duration, uint64) { return 0, time.Hour, 672 << 20 }
	if load := bundle.loadFacts(); load.RetainedHigh == nil || load.RetainedHigh.Bytes != 0 {
		t.Fatalf("a quiet hour = %+v, want a reading of zero", load.RetainedHigh)
	}
}

// The Leader judges a fleet that reports high-waters by occupancy, holds
// byte moves for its term's first hour, then moves - and the round after,
// the high-waters not yet refreshed, the moved Query Groups are counted on
// the Workers they went to and off the one they left, so nothing moves
// again for a high-water that still holds what has left.
func TestProductionLeaderJudgesOccupancyAndAccountsWhatMoved(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const pool = uint64(1 << 30)
	peak := pool / 100 * 35
	highs := map[string]uint64{"worker-1": 3 * peak}
	worker := func(id string) ownership.WorkerRegistration {
		return ownership.WorkerRegistration{
			WorkerID: id, AssignmentReadiness: ownership.WorkerReady, DependencyStatus: ownership.DependencyHealthy,
			DeploymentProfile: "shadow", CapabilitiesDigest: "capabilities", ExpiresAt: now.Add(3 * time.Hour),
			Load: &ownership.WorkerLoad{RetainedPoolBytes: pool, RetainedHigh: &ownership.RetainedHighReading{Bytes: highs[id], WindowSeconds: 3600}},
		}
	}
	groups := []execution.QueryGroupIdentity{"query-group-1", "query-group-2", "query-group-3"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{})},
		workers:                    []ownership.WorkerRegistration{worker("worker-1"), worker("worker-2"), worker("worker-3")},
		assignments:                map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
		holderKind:                 ownership.HolderUnderSession,
	}
	for _, queryGroup := range groups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now.Add(-3 * time.Hour),
		}
	}
	limits := validGoAccessRuntimeConfig().PhaseTwo.Scheduler.RecoveryLimits()
	flights, err := scheduler.NewFlightCoordinatorWithRecovery(limits, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	eligibility, err := scheduler.NewStaticWorkerEligibility(ownership.WorkerCompatibility{DeploymentProfile: "shadow", CapabilitiesDigest: "capabilities"})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := scheduler.NewReconciler(scheduler.NewRouter(eligibility), store)
	if err != nil {
		t.Fatal(err)
	}
	clock := now
	costs := scheduler.NewCostLedger(func() time.Time { return clock })
	var observations []observability.Observation
	production, err := newProductionPhaseTwoOwnership(productionPhaseTwoOwnershipDependencies{
		Store: store, WorkerID: "worker-1", Catalog: unavailableSlotCatalog{},
		Progress: unavailableScheduleProgress{}, Executor: rejectingSlotExecutor{}, Now: func() time.Time { return clock },
		ControlLeaderTTL: 3 * time.Hour, Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			observations = append(observations, observation)
		}), Reconcile: reconciler, Flights: flights, RecoveryLimits: limits, PostRecoveryTerminalDelay: time.Minute,
		QueryDeadlineReserve: 5 * time.Second, SnapshotRetention: time.Hour, PublicationDelayAllowance: time.Minute,
		SettlingWait: 30 * time.Second, LeaseTTL: 30 * time.Second, ReconcileInterval: 5 * time.Second, ContentScopes: noContentScopes,
		Costs: costs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if leader, err := production.TryAcquireControlLeader(context.Background(), now, 3*time.Hour); err != nil || !leader {
		t.Fatalf("TryAcquireControlLeader() leader=%v error=%v", leader, err)
	}
	costLedgerSink{ledger: costs}.RecordCosts("worker-1", []viewstream.QueryGroupCost{
		{QueryGroup: "query-group-1", RetainedBytesPeak: peak}, {QueryGroup: "query-group-2", RetainedBytesPeak: peak}, {QueryGroup: "query-group-3", RetainedBytesPeak: peak},
	})
	round := func(at time.Time) *observability.RebalanceFacts {
		t.Helper()
		clock = at
		store.workers = []ownership.WorkerRegistration{worker("worker-1"), worker("worker-2"), worker("worker-3")}
		if err := production.PublishAssignments(context.Background(), groups, at); err != nil {
			t.Fatalf("PublishAssignments(%v) error = %v", at, err)
		}
		var found *observability.RebalanceFacts
		for _, observation := range observations {
			if observation.Stage == observability.StageRebalancePlanned && observation.Result == observability.ResultSuccess {
				found = observation.Rebalance
			}
		}
		if found == nil || found.Bytes == nil {
			t.Fatalf("no rebalance facts at %v", at)
		}
		return found
	}

	facts := round(now)
	if facts.Bytes.Judgement != scheduler.ByteJudgementOccupancy || !facts.Bytes.Held || facts.Bytes.PlannedMoves != 0 ||
		len(facts.Bytes.Overloaded) != 1 {
		t.Fatalf("the term's first round: %+v, want worker-1 over by occupancy and no byte move held for the hour", facts.Bytes)
	}
	published := production.LastRebalance()
	if published == nil || published.Bytes == nil || published.Bytes.Judgement != scheduler.ByteJudgementOccupancy || !published.Bytes.Held ||
		len(published.Bytes.Sums) != 3 || published.Bytes.Sums[0].Occupancy == nil || published.Bytes.Sums[0].Occupancy.HighBytes != 3*peak ||
		published.Bytes.Sums[0].PeakSumBytes != 3*peak {
		t.Fatalf("fleet byte facts = %+v, want occupancy with worker-1's parts beside its sum of peaks", published)
	}

	// An hour into the term: the byte move and a count move go out.
	facts = round(now.Add(time.Hour + time.Minute))
	if facts.Bytes.Held || facts.Bytes.PublishedMoves != 1 || facts.PublishedMoves != 1 {
		t.Fatalf("an hour in: bytes %+v, counts published %d; want one byte move and one count move", facts.Bytes, facts.PublishedMoves)
	}
	// The high-waters have not moved; the account has. worker-1 holds one
	// peak, each of the others one: nothing is over, nothing moves.
	facts = round(now.Add(time.Hour + 2*time.Minute))
	if len(facts.Bytes.Overloaded) != 0 || facts.Bytes.PlannedMoves != 0 || facts.PlannedMoves != 0 {
		t.Fatalf("the round after: bytes %+v, count moves %d; want worker-1 relieved by the account and nothing moved", facts.Bytes, facts.PlannedMoves)
	}
	sums := production.LastRebalance().Bytes.Sums
	for _, sum := range sums {
		if sum.Occupancy == nil {
			t.Fatalf("sums = %+v, want the occupancy parts", sums)
		}
		switch sum.WorkerID {
		case "worker-1":
			if sum.Occupancy.Leaving != 2 || sum.Occupancy.Bytes != peak {
				t.Fatalf("worker-1 = %+v, want its high-water less the two that left", sum.Occupancy)
			}
		default:
			if sum.Occupancy.Arriving != 1 || sum.Occupancy.Bytes != peak {
				t.Fatalf("%s = %+v, want the one that landed", sum.WorkerID, sum.Occupancy)
			}
		}
	}
}

// Under occupancy the fleet facts keep each Worker's sum of peaks beside
// the occupancy it was judged by; under the sum of peaks the sum is the
// judged number and no occupancy is shown.
func TestTheByteFactsCarryTheOccupancyBesideTheSumOfPeaks(t *testing.T) {
	plan := scheduler.BytePlan{
		Judgement: scheduler.ByteJudgementOccupancy,
		Sum:       map[string]uint64{"worker-1": 300},
		PeakSum:   map[string]uint64{"worker-1": 1900},
		Parts: map[string]scheduler.OccupancyParts{"worker-1": {HighBytes: 250, WindowSeconds: 3600, LongPeriodBytes: 20, ArrivingBytes: 40, Arriving: 1,
			Waiting: 1, WaitingSample: []execution.QueryGroupIdentity{"quiet"}, LeavingBytes: 10, Leaving: 1}},
		OverShare: []execution.QueryGroupIdentity{"big"},
		TermAge:   90 * time.Minute,
		// Set, not at their reassuring zero values: a lost assignment reads
		// as "no Worker left out, not all over, not held".
		NotReporting: []string{"worker-2"}, AllOver: true, Held: true,
	}
	facts := fleetByteConstraintFacts(plan, rebalanceOutcome{})
	want := fleet.ByteSumSample{WorkerID: "worker-1", PeakSumBytes: 1900, Occupancy: &fleet.ByteOccupancy{Bytes: 300, HighBytes: 250, WindowSeconds: 3600,
		LongPeriodBytes: 20, ArrivingBytes: 40, Arriving: 1, Waiting: 1, WaitingSample: []string{"quiet"}, LeavingBytes: 10, Leaving: 1}}
	if len(facts.Sums) != 1 || !reflect.DeepEqual(facts.Sums[0], want) {
		t.Fatalf("sums = %+v, want %+v", facts.Sums, want)
	}
	// Every other field, the whole of it: what the fleet snapshot says of
	// the judgement and why, the all-over mode and the term's first hour.
	rest := *facts
	rest.Sums = nil
	wantRest := fleet.ByteConstraintFacts{SharePercent: scheduler.ByteConstraintPercent, Judgement: scheduler.ByteJudgementOccupancy,
		NotReporting: []string{"worker-2"}, AllOver: true, Held: true, TermAgeSeconds: 5400, OverShare: []string{"big"}}
	if !reflect.DeepEqual(rest, wantRest) {
		t.Fatalf("fleet byte facts = %+v, want %+v", rest, wantRest)
	}
	// The log line carries the same, the over-share Query Groups counted.
	line := byteConstraintFacts(plan, rebalanceOutcome{})
	wantLine := observability.ByteConstraintFacts{SharePercent: scheduler.ByteConstraintPercent, Judgement: scheduler.ByteJudgementOccupancy,
		NotReporting: []string{"worker-2"}, AllOver: true, Held: true, OverShare: 1}
	if !reflect.DeepEqual(*line, wantLine) {
		t.Fatalf("byte facts on the line = %+v, want %+v", *line, wantLine)
	}
	plan.Judgement = scheduler.ByteJudgementPeakSum
	if sums := fleetByteConstraintFacts(plan, rebalanceOutcome{}).Sums; sums[0].PeakSumBytes != 300 || sums[0].Occupancy != nil {
		t.Fatalf("under the sum of peaks: sums = %+v, want the judged sum and no occupancy", sums)
	}
}

// The count correction the Leader runs after its byte moves judges the
// destination with them on it: worker-2's line holds one of the large Query
// Groups, the byte move puts one there, and the count correction that
// follows in the same round gives it a small one rather than a second
// large one.
func TestProductionLeaderCountsTheByteMovesBeforeTheCountCorrection(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const pool = uint64(1 << 30)
	peak := pool / 100 * 35
	pools := map[string]uint64{"worker-1": pool, "worker-2": pool / 2}
	highs := map[string]uint64{"worker-1": 3 * peak}
	worker := func(id string) ownership.WorkerRegistration {
		return ownership.WorkerRegistration{
			WorkerID: id, AssignmentReadiness: ownership.WorkerReady, DependencyStatus: ownership.DependencyHealthy,
			DeploymentProfile: "shadow", CapabilitiesDigest: "capabilities", ExpiresAt: now.Add(3 * time.Hour),
			Load: &ownership.WorkerLoad{RetainedPoolBytes: pools[id], RetainedHigh: &ownership.RetainedHighReading{Bytes: highs[id], WindowSeconds: 3600}},
		}
	}
	groups := []execution.QueryGroupIdentity{"query-group-1", "query-group-2", "query-group-3", "small-1", "small-2", "small-3", "small-4"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{})},
		workers:                    []ownership.WorkerRegistration{worker("worker-1"), worker("worker-2")},
		assignments:                map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
		holderKind:                 ownership.HolderUnderSession,
	}
	costs := []viewstream.QueryGroupCost{}
	for _, queryGroup := range groups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now.Add(-3 * time.Hour),
		}
		size := uint64(1 << 20)
		if queryGroup[0] == 'q' {
			size = peak
		}
		costs = append(costs, viewstream.QueryGroupCost{QueryGroup: queryGroup, RetainedBytesPeak: size})
	}
	limits := validGoAccessRuntimeConfig().PhaseTwo.Scheduler.RecoveryLimits()
	flights, err := scheduler.NewFlightCoordinatorWithRecovery(limits, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	eligibility, err := scheduler.NewStaticWorkerEligibility(ownership.WorkerCompatibility{DeploymentProfile: "shadow", CapabilitiesDigest: "capabilities"})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := scheduler.NewReconciler(scheduler.NewRouter(eligibility), store)
	if err != nil {
		t.Fatal(err)
	}
	clock := now
	ledger := scheduler.NewCostLedger(func() time.Time { return clock })
	production, err := newProductionPhaseTwoOwnership(productionPhaseTwoOwnershipDependencies{
		Store: store, WorkerID: "worker-1", Catalog: unavailableSlotCatalog{},
		Progress: unavailableScheduleProgress{}, Executor: rejectingSlotExecutor{}, Now: func() time.Time { return clock },
		ControlLeaderTTL: 3 * time.Hour, Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {}),
		Reconcile: reconciler, Flights: flights, RecoveryLimits: limits, PostRecoveryTerminalDelay: time.Minute,
		QueryDeadlineReserve: 5 * time.Second, SnapshotRetention: time.Hour, PublicationDelayAllowance: time.Minute,
		SettlingWait: 30 * time.Second, LeaseTTL: 30 * time.Second, ReconcileInterval: 5 * time.Second, ContentScopes: noContentScopes,
		Costs: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if leader, err := production.TryAcquireControlLeader(context.Background(), now, 3*time.Hour); err != nil || !leader {
		t.Fatalf("TryAcquireControlLeader() leader=%v error=%v", leader, err)
	}
	costLedgerSink{ledger: ledger}.RecordCosts("worker-1", costs)
	for _, at := range []time.Time{now, now.Add(time.Hour + time.Minute)} {
		clock = at
		if err := production.PublishAssignments(context.Background(), groups, at); err != nil {
			t.Fatalf("PublishAssignments(%v) error = %v", at, err)
		}
	}
	onTwo := map[execution.QueryGroupIdentity]bool{}
	for queryGroup, record := range store.assignments {
		if record.DesiredWorkerID == "worker-2" {
			onTwo[queryGroup] = true
		}
	}
	large := 0
	for queryGroup := range onTwo {
		if queryGroup[0] == 'q' {
			large++
		}
	}
	if len(onTwo) != 2 || large != 1 {
		t.Fatalf("worker-2 holds %v, want one large Query Group by the byte move and a small one by the count correction", onTwo)
	}
}
