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
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

func segmentSkipWorker(id string, aware bool, now time.Time) ownership.WorkerRegistration {
	registration := ownership.WorkerRegistration{
		WorkerID: id, AssignmentReadiness: ownership.WorkerReady, DependencyStatus: ownership.DependencyHealthy,
		DeploymentProfile: "shadow", CapabilitiesDigest: "capabilities", ExpiresAt: now.Add(time.Hour),
		Capabilities: []string{ownership.CapabilityContentScope},
	}
	if aware {
		registration.Capabilities = append(registration.Capabilities, ownership.CapabilitySegmentSkip)
	}
	return registration
}

// segmentSkipLeader is a production Leader over store, its view stream a
// real server, asking for the segment skip word when asked is set.
func segmentSkipLeader(t *testing.T, store *rebalanceOwnershipStore, asked bool, now time.Time,
	observations *[]observability.Observation) (*productionPhaseTwoOwnership, *viewstream.Server) {
	t.Helper()
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
	server, err := viewstream.NewServer(viewStreamAdmission{registry: nil, now: time.Now}, nil, viewstream.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	production, err := newProductionPhaseTwoOwnership(productionPhaseTwoOwnershipDependencies{
		Store: store, WorkerID: "worker-1", Catalog: unavailableSlotCatalog{},
		Progress: unavailableScheduleProgress{}, Executor: rejectingSlotExecutor{}, Now: func() time.Time { return now },
		ControlLeaderTTL: time.Hour, Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			*observations = append(*observations, observation)
		}), Reconcile: reconciler, Flights: flights, RecoveryLimits: limits, PostRecoveryTerminalDelay: time.Minute,
		QueryDeadlineReserve: 5 * time.Second, SnapshotRetention: time.Hour, PublicationDelayAllowance: time.Minute,
		SettlingWait: 30 * time.Second, LeaseTTL: 30 * time.Second, ReconcileInterval: 5 * time.Second, ContentScopes: noContentScopes,
		Costs: scheduler.NewCostLedger(func() time.Time { return now }), ViewStream: server, SegmentSkip: asked,
	})
	if err != nil {
		t.Fatal(err)
	}
	if leader, err := production.TryAcquireControlLeader(context.Background(), now, time.Hour); err != nil || !leader {
		t.Fatalf("TryAcquireControlLeader() leader=%v error=%v", leader, err)
	}
	return production, server
}

func lastSegmentSkip(t *testing.T, observations []observability.Observation) observability.SegmentSkipFacts {
	t.Helper()
	var found *observability.SegmentSkipFacts
	for _, observation := range observations {
		if observation.Stage == observability.StageRebalancePlanned && observation.Rebalance != nil {
			found = observation.Rebalance.SegmentSkip
		}
	}
	if found == nil {
		t.Fatal("the round's facts carry no segment skip word")
	}
	return *found
}

func segmentSkipWords(store *rebalanceOwnershipStore, groups []execution.QueryGroupIdentity) []uint64 {
	words := make([]uint64, 0, len(groups))
	for _, queryGroup := range groups {
		words = append(words, store.assignments[queryGroup].SegmentSkip)
	}
	return words
}

// The Leader sets the segment skip word on the records only while the values
// ask for it and every ready worker declares the contract; asked and held, the
// round names the replicas that do not, on the log line and the snapshot. Set,
// the word is the epoch of the view this Leader publishes - the one number a
// worker compares it with. A replica that does not declare the contract
// joining takes it back, as does the values' ask going away.
func TestTheLeaderSetsTheSegmentSkipWordOnlyForAFleetThatDeclaresIt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	groups := []execution.QueryGroupIdentity{"query-group-1", "query-group-2"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{}), leaderEpoch: 9},
		workers: []ownership.WorkerRegistration{segmentSkipWorker("worker-1", true, now), segmentSkipWorker("worker-2", false, now),
			segmentSkipWorker("worker-3", true, now)},
		assignments: map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
	}
	for _, queryGroup := range groups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now,
		}
	}
	var observations []observability.Observation
	production, server := segmentSkipLeader(t, store, true, now, &observations)

	// Mid-roll: one ready worker does not declare the contract.
	if err := production.PublishAssignments(context.Background(), groups, now); err != nil {
		t.Fatal(err)
	}
	if facts := lastSegmentSkip(t, observations); !facts.Asked || facts.Held != ownership.SegmentSkipHeld || facts.Ready != 3 ||
		!reflect.DeepEqual(facts.Unaware, []string{"worker-2"}) || facts.Set != 0 {
		t.Fatalf("round 1 = %+v, want the word asked for, held for worker-2, nothing set", facts)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{0, 0}) {
		t.Fatalf("round 1 words %v, want none", words)
	}
	if published := production.LastRebalance(); published == nil || published.SegmentSkip == nil || published.SegmentSkip.Held != ownership.SegmentSkipHeld {
		t.Fatalf("fleet facts = %+v, want the hold on the snapshot", published)
	}

	// The roll completes: the word is set, the epoch of the view this
	// Leader publishes.
	store.workers[1] = segmentSkipWorker("worker-2", true, now)
	at := now.Add(40 * time.Second)
	if err := production.PublishAssignments(context.Background(), groups, at); err != nil {
		t.Fatal(err)
	}
	viewEpoch := server.Stats().ControlEpoch
	if viewEpoch == 0 {
		t.Fatal("the Leader publishes no view")
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{viewEpoch, viewEpoch}) {
		t.Fatalf("round 2 words %v, want the view's epoch %d on both", words, viewEpoch)
	}
	if facts := lastSegmentSkip(t, observations); facts.Held != "" || facts.Set != 2 || facts.Withdrawn != 0 || facts.Failed != 0 {
		t.Fatalf("round 2 = %+v, want both set", facts)
	}
	// A round with nothing to restate writes nothing.
	before := store.published
	if err := production.PublishAssignments(context.Background(), groups, at.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if facts := lastSegmentSkip(t, observations); facts.Set != 0 || store.published != before {
		t.Fatalf("round 3 = %+v with %d publications, want nothing restated", facts, store.published-before)
	}

	// A replica of a build without the contract joins: the word is taken
	// back from every record.
	store.workers = append(store.workers, segmentSkipWorker("worker-4", false, now))
	if err := production.PublishAssignments(context.Background(), groups, at.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{0, 0}) {
		t.Fatalf("after worker-4 joined the words are %v, want none", words)
	}
	if facts := lastSegmentSkip(t, observations); facts.Held != ownership.SegmentSkipHeld || facts.Withdrawn != 2 {
		t.Fatalf("after worker-4 joined = %+v, want held and both withdrawn", facts)
	}
}

// Without the values' ask the Leader sets nothing and takes back a word an
// earlier Leader left, whatever the fleet declares.
func TestWithoutTheAskTheLeaderTakesTheSegmentSkipWordBack(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	groups := []execution.QueryGroupIdentity{"query-group-1"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{}), leaderEpoch: 9},
		workers:                    []ownership.WorkerRegistration{segmentSkipWorker("worker-1", true, now)},
		assignments: map[execution.QueryGroupIdentity]ownership.AssignmentRecord{"query-group-1": {
			QueryGroup: "query-group-1", DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now, SegmentSkip: 8,
		}},
	}
	var observations []observability.Observation
	production, _ := segmentSkipLeader(t, store, false, now, &observations)
	if err := production.PublishAssignments(context.Background(), groups, now); err != nil {
		t.Fatal(err)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{0}) {
		t.Fatalf("words %v, want the earlier Leader's taken back", words)
	}
	if facts := lastSegmentSkip(t, observations); facts.Asked || facts.Held != "" || facts.Withdrawn != 1 || facts.Ready != 0 || len(facts.Unaware) != 0 {
		t.Fatalf("facts %+v, want not asked, one withdrawn, no gate", facts)
	}
	// Once nothing is left to take back, a round without the ask carries
	// no word at all: every worker not declaring it is not news.
	observations = nil
	if err := production.PublishAssignments(context.Background(), groups, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, observation := range observations {
		if observation.Stage == observability.StageRebalancePlanned && observation.Rebalance != nil && observation.Rebalance.SegmentSkip != nil {
			t.Fatalf("a steady round without the ask carries %+v", observation.Rebalance.SegmentSkip)
		}
	}
	if published := production.LastRebalance(); published == nil || published.SegmentSkip != nil {
		t.Fatalf("the snapshot carries %+v, want no word", published)
	}
}

// A new Leader - the same build re-elected - finds the records naming the
// Leader before it, which its workers no longer follow, and restates them
// with its own epoch in its first round.
func TestANewLeaderRestatesTheSegmentSkipWordWithItsOwnEpoch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	groups := []execution.QueryGroupIdentity{"query-group-1", "query-group-2"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{}), leaderEpoch: 9},
		workers:                    []ownership.WorkerRegistration{segmentSkipWorker("worker-1", true, now)},
		assignments:                map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
	}
	for _, queryGroup := range groups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now,
		}
	}
	var observations []observability.Observation
	first, _ := segmentSkipLeader(t, store, true, now, &observations)
	if err := first.PublishAssignments(context.Background(), groups, now); err != nil {
		t.Fatal(err)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{9, 9}) {
		t.Fatalf("the first Leader's words %v, want its epoch", words)
	}
	store.leaderEpoch = 10
	next, server := segmentSkipLeader(t, store, true, now.Add(time.Minute), &observations)
	if epoch := server.Stats().ControlEpoch; epoch != 10 {
		t.Fatalf("the next Leader's view is of epoch %d, want 10", epoch)
	}
	if err := next.PublishAssignments(context.Background(), groups, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{10, 10}) {
		t.Fatalf("after the next Leader's round the words are %v, want its epoch 10", words)
	}
	if facts := lastSegmentSkip(t, observations); facts.Set != 2 {
		t.Fatalf("the next Leader's round = %+v, want both restated", facts)
	}
}

// A Query Group draining after its strategy was retired keeps running its
// Slots until its boundary, and is among the Query Groups the control load
// hands the Leader's round; so when the ask goes away its word is taken back
// in the next round with every other, and no draining Query Group goes on
// skipping under a Leader whose epoch still matches.
func TestTurningTheSegmentSkipOffTakesTheWordBackFromADrainingQueryGroup(t *testing.T) {
	publication := controlplane.SnapshotPublicationRef{SnapshotRevision: "snapshot-new", PublicationEpoch: 2}
	boundary := execution.EvaluationTime(90)
	retired := execution.QueryGroupIdentity("query-group-old")
	repository := &fakeProductionCatalogRepository{
		activation: controlplane.ActivationState{RecordRevision: 2, Current: publication,
			Draining: []controlplane.DrainingQueryGroup{{QueryGroup: retired, RetiredBoundary: boundary}}},
		snapshot: controlplane.PublishedSnapshot{Publication: publication,
			QueryGroups: []controlplane.QueryGroup{{Identity: "query-group-new"}}},
	}
	schedules := &fakeScheduleProjection{
		initial: map[execution.QueryGroupIdentity]execution.FrozenQueryGroupSchedule{
			retired: schedulerScheduleForProductionControl(t, retired, 60, 60, &boundary),
		},
		retired: map[execution.QueryGroupIdentity]execution.EvaluationTime{retired: boundary},
	}
	progress := &fakeProductionProgressReader{byGroup: map[execution.QueryGroupIdentity]execution.ProgressLoadResult{
		retired: {Status: execution.ProgressFound, Progress: &execution.ScheduleProgress{
			Identity: execution.ProgressIdentity{QueryGroup: retired}, NextSlot: 60,
		}},
	}}
	control, err := newProductionPhaseTwoControl(productionPhaseTwoControlDependencies{
		Source: fakeStrategySource{}, Planner: fakePrimaryQueryCompiler{}, Reconciler: &fakeSourceReconciler{},
		Activator: &fakeInitialScheduleActivator{}, Repository: repository, Schedules: schedules, Progress: progress,
		Observer: observability.NopObserver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := control.LoadActive(context.Background())
	if err != nil || !reflect.DeepEqual(loaded.QueryGroups, []execution.QueryGroupIdentity{"query-group-new", retired}) {
		t.Fatalf("the control load = (%v, %v), want the draining Query Group among the round's", loaded.QueryGroups, err)
	}

	now := time.Unix(1_700_000_000, 0)
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{}), leaderEpoch: 9},
		workers:                    []ownership.WorkerRegistration{segmentSkipWorker("worker-1", true, now)},
		assignments:                map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
	}
	for _, queryGroup := range loaded.QueryGroups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now, SegmentSkip: 9,
		}
	}
	var observations []observability.Observation
	production, _ := segmentSkipLeader(t, store, false, now, &observations)
	if err := production.PublishAssignments(context.Background(), loaded.QueryGroups, now); err != nil {
		t.Fatal(err)
	}
	if words := segmentSkipWords(store, loaded.QueryGroups); !reflect.DeepEqual(words, []uint64{0, 0}) {
		t.Fatalf("after the ask went away the words are %v, want both taken back, the draining one's too", words)
	}
}

// This build declares the segment skip contract in its registration, beside
// the content and split contracts: a Leader asked for the skip and reading a
// fleet of this build alone admits it; one replica of the build before
// holds it, named.
func TestThisBuildDeclaresTheSegmentSkipContract(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	at := time.Unix(1_700_000_000, 0)
	registration, err := phaseTwoWorkerRegistration(cfg, ownership.WorkerReady, at, nil, nil, viewStreamIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	if !registration.Declares(ownership.CapabilitySegmentSkip) || !registration.Declares(ownership.CapabilityShardAware) ||
		!registration.Declares(ownership.CapabilityContentScope) {
		t.Fatalf("registration declares %v, want the three contracts", registration.Capabilities)
	}
	second := registration
	second.WorkerID = "worker-2"
	if gate := ownership.SegmentSkipAdmission([]ownership.WorkerRegistration{registration, second}); !gate.Admitted || len(gate.Unaware) != 0 {
		t.Fatalf("a fleet of this build: %+v, want the skip admitted", gate)
	}
	before := segmentSkipWorker("worker-3", false, at)
	if gate := ownership.SegmentSkipAdmission([]ownership.WorkerRegistration{registration, before}); gate.Admitted ||
		len(gate.Unaware) != 1 || gate.Unaware[0] != "worker-3" {
		t.Fatalf("beside a replica of the build before: %+v, want it held, the replica named", gate)
	}
}
