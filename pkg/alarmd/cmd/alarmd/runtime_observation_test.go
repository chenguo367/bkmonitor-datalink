package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// The cost roster is what each owned Query Group executes: the Segment its
// Slots are frozen from, with that Segment's Plans. An event carrying its
// Slot's revisions is counted against the group; one carrying the latest
// publication's revisions - which a roster read from the directory's
// PUBLISHED rows would have held - is not the group's. A Query Group whose
// owner is not accepting, or whose identity is not in memory, is left out and
// the roster says it is incomplete.
func TestTheCostRosterIsWhatEachOwnedGroupExecutes(t *testing.T) {
	running := controlplane.ExecutionIdentity{SnapshotRevision: "snapshot-running", QueryRevision: "query-running",
		ScheduleRevision: "schedule-running", Plans: []execution.PlanIdentity{{TenantID: "t", BusinessID: "b", StrategyID: "11440"}}}
	identity := func(qg execution.QueryGroupIdentity, revision uint64, at execution.EvaluationTime) (controlplane.ExecutionIdentity, bool) {
		// The idle group has an identity in memory too: only its lease not
		// accepting keeps it out.
		if (qg == "ours" && revision == 3 || qg == "idle" && revision == 4) && at == 600 {
			return running, true
		}
		return controlplane.ExecutionIdentity{}, false
	}
	owned := []ownedLease{{queryGroup: "ours", revision: 3, accepting: true}, {queryGroup: "idle", revision: 4},
		{queryGroup: "cold", revision: 5, accepting: true}}
	groups, complete := executionCostGroups(owned, identity, 600)
	if complete || len(groups) != 1 {
		t.Fatalf("roster = %+v complete=%v, want only the group that answered, incomplete", groups, complete)
	}
	g := groups[0]
	if g.QueryGroupKey != "ours" || g.SnapshotRevision != "snapshot-running" || g.QueryRevision != "query-running" ||
		g.ScheduleRevision != "schedule-running" || len(g.Members) != 1 || g.Members[0].StrategyID != "11440" {
		t.Fatalf("roster group = %+v, want the running Segment's revisions and its Plan", g)
	}
	if groups, complete = executionCostGroups([]ownedLease{owned[0], owned[2]}, identity, 600); complete || len(groups) != 1 {
		t.Fatalf("an accepting group with no identity in memory: roster %+v complete=%v, want it left out and incomplete", groups, complete)
	}
	if groups, complete = executionCostGroups(owned[:1], identity, 600); !complete || len(groups) != 1 {
		t.Fatalf("every owned group answered: roster %+v complete=%v, want complete", groups, complete)
	}
	if groups, complete = executionCostGroups(owned[:1], nil, 600); complete || len(groups) != 0 {
		t.Fatalf("no identity source: roster %+v complete=%v, want empty and incomplete", groups, complete)
	}

	now := time.Unix(600, 0)
	cost := observability.NewCostSummary(observability.CostSummaryOptions{ProcessID: "p", Window: time.Minute, GroupCapacity: 4,
		PlanCapacity: 8, MetadataBytes: 4096, TopN: 2, Now: func() time.Time { return now }})
	groups, complete = executionCostGroups(owned[:1], identity, 600)
	cost.Reconcile(groups, complete)
	slot := func(snapshot string) observability.Observation {
		return observability.Observation{Stage: observability.StageSlotCompleted, Result: observability.ResultSuccess, Duration: time.Millisecond,
			Trace: observability.TraceFields{QueryGroupKey: "ours", SnapshotRevision: snapshot, QueryRevision: "query-running",
				ScheduleRevision: "schedule-running", EvaluationTime: 590}}
	}
	cost.Observe(context.Background(), slot("snapshot-running"))
	cost.Observe(context.Background(), slot("snapshot-latest"))
	cost.Publish(now)
	coverage := cost.Snapshot().Coverage
	if !coverage.CatalogComplete || coverage.TrackedGroups != 1 || coverage.TrackedPlans != 1 || coverage.ObservedGroups != 1 ||
		coverage.UntrackedObservations != 1 {
		t.Fatalf("coverage = %+v, want the running Slot counted against its group and the latest publication's not", coverage)
	}
}

// A refresh builds the roster at its own clock's second from the owned leases
// and reconciles it into the summary, whether or not this replica keeps a
// strategy directory: the roster no longer reads one.
func TestARefreshReconcilesTheExecutingRosterAtItsOwnTime(t *testing.T) {
	now := time.Unix(600, 0)
	cost := observability.NewCostSummary(observability.CostSummaryOptions{ProcessID: "p", Window: time.Minute, GroupCapacity: 4,
		PlanCapacity: 8, MetadataBytes: 4096, TopN: 2, Now: func() time.Time { return now }})
	refresh := &observationRefresh{cost: cost, now: func() time.Time { return now }, interval: time.Minute,
		owned: func() []ownedLease { return []ownedLease{{queryGroup: "ours", revision: 3, accepting: true}} },
		identity: func(qg execution.QueryGroupIdentity, revision uint64, at execution.EvaluationTime) (controlplane.ExecutionIdentity, bool) {
			if qg != "ours" || revision != 3 || at != execution.EvaluationTime(now.Unix()) {
				return controlplane.ExecutionIdentity{}, false
			}
			return controlplane.ExecutionIdentity{SnapshotRevision: "s", QueryRevision: "q", ScheduleRevision: "r",
				Plans: []execution.PlanIdentity{{TenantID: "t", BusinessID: "b", StrategyID: "1"}}}, true
		}}
	refresh.publish(context.Background())
	if coverage := cost.Snapshot().Coverage; !coverage.CatalogComplete || coverage.TrackedGroups != 1 || coverage.TrackedPlans != 1 {
		t.Fatalf("coverage after a refresh = %+v, want the owned group tracked at the refresh's second", coverage)
	}
}

// The owned leases are read from each Runner's lease in memory: its timeline
// revision and whether its owner accepts on it. A Runner with no lease to
// read is owned and not accepting, so the roster leaves it out rather than
// charging it under a revision it may not run.
func TestOwnedLeasesAreEachRunnersLeaseFromMemory(t *testing.T) {
	bundle := &phaseTwoWorkerBundle{runners: map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{
		"leased":   {runner: &maintenanceTestRunner{scope: "obj", revision: 7}},
		"unleased": {runner: &fakePhaseTwoQueryGroup{}},
	}}
	got := map[execution.QueryGroupIdentity]ownedLease{}
	for _, lease := range bundle.ownedLeases() {
		got[lease.queryGroup] = lease
	}
	if len(got) != 2 || got["leased"] != (ownedLease{queryGroup: "leased", revision: 7, accepting: true}) ||
		got["unleased"] != (ownedLease{queryGroup: "unleased"}) {
		t.Fatalf("owned leases = %+v, want the leased Runner at revision 7 accepting and the other not accepting", got)
	}
}

func TestObservationRegistryAndCostPaginationCannotStarveReplicas(t *testing.T) {
	client := windowRedis(t)
	ctx := context.Background()
	at := time.Now()
	registry, err := ownership.NewRedisStoreWithClient(client, "test:ob:registry")
	if err != nil {
		t.Fatal(err)
	}
	limits := fleet.CostProjectionLimits{PublishBytes: 16 << 10, ReadBytes: 64 << 10, ReadCommands: 1, Timeout: time.Second, FreshFor: time.Minute, TTL: 2 * time.Minute}
	projection, err := fleet.NewCostProjectionStore(client, "test:ob:cost", limits)
	if err != nil {
		t.Fatal(err)
	}
	cost := observability.CostSnapshot{Enabled: true, ProcessID: "process", Scope: "process_observed_candidates", GeneratedAt: at}
	for _, id := range []string{"a", "b", "c", "d"} {
		if err = registry.RegisterWorker(ctx, ownership.WorkerRegistration{WorkerID: id, AssignmentReadiness: ownership.WorkerReady, DependencyStatus: ownership.DependencyHealthy, DeploymentProfile: "profile", CapabilitiesDigest: "capabilities", ExpiresAt: at.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err = projection.Publish(ctx, id, at, cost); err != nil {
			t.Fatal(err)
		}
	}
	r := &observationCostRefresh{store: projection, registry: registry, reader: client, cache: fleet.NewCostCandidatesCache(func() time.Time { return at }, time.Minute), limits: limits, registryLimits: ownership.ObservationRegistryLimits{Bytes: 8 << 10, Commands: 4, Rows: 2, Timeout: time.Second}, replica: "a", interval: time.Second}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		at = at.Add(time.Second)
		r.publish(ctx, at, cost)
		for _, snapshot := range r.observation.Projection.Snapshots {
			seen[snapshot.Replica] = true
		}
		if r.observation.Projection.Complete {
			t.Fatal("partial projection claimed full fleet")
		}
	}
	if len(seen) != 4 {
		t.Fatalf("independent cursors starved replicas: %v", seen)
	}
}

func TestObservationCapacityCannotBlockExecutionAtSmallOrLargeResources(t *testing.T) {
	for _, resources := range []config.CapacityInputs{{}, {CPUBudget: 1, MemoryLimitBytes: 2 << 20, MemorySource: "cgroup"}, {CPUBudget: 1, MemoryLimitBytes: 1 << 30, MemorySource: "cgroup"}, {CPUBudget: 64, MemoryLimitBytes: 64 << 30, MemorySource: "cgroup"}} {
		capacity := config.DeriveObservationCapacity(resources, config.PhaseTwoObservationConfig{MemoryPercent: 3})
		limits, enabled := observationSampleLimits(capacity)
		if enabled {
			if _, err := observability.NewSeriesSampler(limits); err != nil {
				t.Fatalf("resource-derived sample rejected %+v: %v", resources, err)
			}
		}
		o := observationCostOptions(capacity, "process", time.Now)
		if got := observability.CostSummaryCapacityBytes(o); got > int64(capacity.CostBytes/2) {
			t.Fatalf("collector reservation %d > half budget %d", got, capacity.CostBytes/2)
		}
		if resources.MemoryLimitBytes == 1<<30 {
			t.Logf("1GiB/1CPU: capacity=%+v directory_entry_reservation=%d cost_groups=%d cost_plans=%d cost_top_n=%d cost_reservation=%d sample=%+v projection=%+v", capacity, controlplane.DirectoryEntryReservationBytes(), o.GroupCapacity, o.PlanCapacity, o.TopN, observability.CostSummaryCapacityBytes(o), limits, observationProjectionLimits(capacity, 30*time.Second))
		}
		if resources.MemoryLimitBytes <= 2<<20 && enabled {
			t.Fatalf("enabled without one complete buffer %+v", limits)
		}
	}
}

func TestObservationRedisHasIndependentPoolAndNoHiddenRetries(t *testing.T) {
	o := observationRedisOptions(config.RedisConnectionConfig{PoolSize: 100, ReadTimeout: config.Duration(5 * time.Second), WriteTimeout: config.Duration(5 * time.Second)})
	if o.MaxRetries != -1 || o.PoolSize != phaseTwoDiagnosticsPoolSize || o.ReadTimeout > time.Second || o.WriteTimeout > time.Second || o.PoolTimeout > time.Second {
		t.Fatalf("diagnostics can consume unbounded attempts or execution pool %+v", o)
	}
}

// The cost summary is sized to the most groups that fit its half of the
// budget: those fit, and one group more does not. Halving the budget until it
// fit left up to half of it unused, so a reservation a few hundred bytes a
// group larger could halve the groups a replica tracks; at a 5% share of a
// 4 GiB container it did, from 3276 to 1638.
func TestTheCostSummaryTracksTheMostGroupsItsBudgetFits(t *testing.T) {
	for _, percent := range []int{1, 3, 5, 12, 25} {
		for _, limit := range []uint64{1 << 30, 4 << 30, 16 << 30} {
			capacity := config.DeriveObservationCapacity(config.CapacityInputs{MemorySource: "pod_limit", MemoryLimitBytes: limit, CPUBudget: 2}, config.PhaseTwoObservationConfig{MemoryPercent: percent})
			collector := int64(capacity.CostBytes / 2)
			o := observationCostOptions(capacity, "process", time.Now)
			if o.GroupCapacity == 0 {
				t.Fatalf("%d%% of %d MiB: no groups tracked", percent, limit>>20)
			}
			if got := observability.CostSummaryCapacityBytes(o); got > collector {
				t.Fatalf("%d%% of %d MiB: %d groups reserve %d, over the collector's %d", percent, limit>>20, o.GroupCapacity, got, collector)
			}
			next := observationCostOptionsFor(capacity, o.GroupCapacity+1, "process", time.Now)
			if got := observability.CostSummaryCapacityBytes(next); got <= collector {
				t.Fatalf("%d%% of %d MiB: %d groups tracked, but %d reserve %d and fit the collector's %d", percent, limit>>20, o.GroupCapacity, next.GroupCapacity, got, collector)
			}
		}
	}
}

// TopN is derived from the rankings the summary publishes -- two scopes
// times its dimensions -- not from a count of the dimensions it once had:
// at a budget where the literal for six dimensions gave one row more than
// the eight the summary has, the derived TopN follows the list.
func TestCostTopNFollowsTheSummarysDimensionCount(t *testing.T) {
	rankings := 2 * len(observability.CostDimensions())
	// A CostBytes chosen so that CostBytes/16 is exactly 20 rows of the true
	// ranking count: fewer rows under any larger ranking count, more under
	// the old literal of twelve rankings.
	costBytes := 16 * rankings * 4096 * 20
	capacity := config.ObservationCapacity{CostBytes: costBytes, DirectoryCommands: 64, SampleRecordsPerMinute: 60, SampleBytesPerMinute: 1 << 20, SampleBufferBytes: 1 << 20}
	o := observationCostOptions(capacity, "process-a", time.Now)
	if o.TopN != 20 {
		t.Fatalf("TopN=%d at a budget of exactly 20 rows per ranking (%d rankings), want 20", o.TopN, rankings)
	}
	smaller := capacity
	smaller.CostBytes = costBytes - 16*rankings*4096
	if o := observationCostOptions(smaller, "process-a", time.Now); o.TopN != 19 {
		t.Fatalf("TopN=%d one ranking-row short of 20, want 19: the derivation does not follow the dimension count", o.TopN)
	}
}
