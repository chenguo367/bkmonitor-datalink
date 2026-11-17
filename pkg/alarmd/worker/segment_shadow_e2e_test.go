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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/detect"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/evaluation"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/trigger"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// shadowRig runs one Plan's Slots round after round through the coordinator's
// own entry, on the real evaluator and a real State store over redis-server,
// the Plan's provider told each round which hosts answer and with what.
type shadowRig struct {
	t     *testing.T
	store *lateLandingStore
	// redis reads the store's keys with no cache: what Redis holds;
	// client is a plain client of the same server.
	redis  *state.ExecutionStore
	client *redis.Client
	// wallAhead is how far the worker's wall clock runs ahead of the real.
	wallAhead   atomic.Int64
	ports       *recordingPorts
	provider    *shadowProvider
	frozen      *shadowFrozen
	coordinator *worker.SlotExecutionCoordinator
	base        execution.SlotExecutionRequest
	readAt      time.Time
	readMu      sync.Mutex
	observed    *shadowObserved
}

// shadowFrozen is the Plan as each round's Slot froze it.
type shadowFrozen struct {
	mu   sync.Mutex
	plan access.FrozenPlan
}

func (frozen *shadowFrozen) ResolveFrozenPlan(context.Context, execution.FrozenExecutionContractRef) (access.FrozenPlan, error) {
	frozen.mu.Lock()
	defer frozen.mu.Unlock()
	return frozen.plan, nil
}

// shadowObserved sums the segment shadow's counts the coordinator reported.
type shadowObserved struct {
	mu       sync.Mutex
	outcomes map[observability.SegmentShadowOutcome]int64
	members  map[observability.SegmentShadowEvent]int64
	// applied is the writes the store found already applied, by site.
	applied map[observability.StateAlreadyAppliedSite]int64
	// classes is the series written, by kind and class (the census).
	classes map[string]int64
	// renewals is the page renewals, by result.
	renewals map[string]int64
}

// renewal is the page renewals counted under result.
func (observed *shadowObserved) renewal(result string) int64 {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	return observed.renewals[result]
}

func (observed *shadowObserved) Observe(_ context.Context, observation observability.Observation) {
	if applied := observation.StateAlreadyApplied; applied != nil {
		observed.mu.Lock()
		for key, count := range applied.Counts {
			if observed.applied == nil {
				observed.applied = map[observability.StateAlreadyAppliedSite]int64{}
			}
			observed.applied[key.Site] += count
		}
		observed.mu.Unlock()
	}
	if census := observation.FrozenStateRenewal; census != nil {
		observed.mu.Lock()
		for kind, classes := range census.SeriesClasses {
			for class, count := range classes {
				if observed.classes == nil {
					observed.classes = map[string]int64{}
				}
				observed.classes[fmt.Sprint(kind, "/", class)] += int64(count)
			}
		}
		observed.mu.Unlock()
	}
	facts := observation.SegmentShadow
	if facts == nil {
		return
	}
	observed.mu.Lock()
	defer observed.mu.Unlock()
	for key, count := range facts.Outcomes {
		observed.outcomes[key.Outcome] += count
	}
	for key, count := range facts.Members {
		observed.members[key.Event] += count
	}
	for result, count := range facts.Renewals {
		if observed.renewals == nil {
			observed.renewals = map[string]int64{}
		}
		observed.renewals[result] += count
	}
}

func (observed *shadowObserved) snapshot() (map[observability.SegmentShadowOutcome]int64, map[observability.SegmentShadowEvent]int64) {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	outcomes, members := map[observability.SegmentShadowOutcome]int64{}, map[observability.SegmentShadowEvent]int64{}
	for outcome, count := range observed.outcomes {
		outcomes[outcome] = count
	}
	for event, count := range observed.members {
		members[event] = count
	}
	return outcomes, members
}

// shadowProvider answers each physical query with the round's hosts: the
// PRIMARY window at the round's value for the host, the history window at
// the steady value, PARTIAL when the round says so.
type shadowProvider struct {
	mu    sync.Mutex
	hosts []string
	// padding, on a dimension outside the identity, makes the PRIMARY
	// window's points large enough that a Slot is read in slices and
	// applied window by window.
	padding    string
	evaluation int64
	absent     map[string]bool
	value      map[string]string
	partial    bool
}

func (provider *shadowProvider) round(evaluation int64, absent []string, value map[string]string, partial bool) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.evaluation, provider.absent, provider.value, provider.partial = evaluation, map[string]bool{}, value, partial
	for _, host := range absent {
		provider.absent[host] = true
	}
}

func (provider *shadowProvider) Execute(ctx context.Context, attempt execution.QueryAttempt, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	provider.mu.Lock()
	evaluation, absent, value, partial := provider.evaluation, provider.absent, provider.value, provider.partial
	provider.mu.Unlock()
	window := attempt.Spec.LogicalWindow
	primary := window.End > evaluation-60
	ref := execution.ProviderResultRef("shadow-result-" + string(attempt.Spec.Digest))
	var delivery execution.SeriesDelivery
	delivered := 0
	for _, host := range provider.hosts {
		if absent[host] {
			continue
		}
		in, err := attempt.Slice.Matches(map[string]string{"host": host})
		if err != nil || !in {
			if err != nil {
				return execution.ProviderCompletion{}, err
			}
			continue
		}
		raw := "60"
		if primary && value[host] != "" {
			raw = value[host]
		}
		fields := ringRatioFields(host)
		dimension, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", fields)
		if err != nil {
			return execution.ProviderCompletion{}, err
		}
		recordID, err := contract.DeriveRecordIDV2(dimension, window.Start)
		if err != nil {
			return execution.ProviderCompletion{}, err
		}
		records := []contract.CanonicalRecordV2{{RecordID: recordID, SourceTime: window.Start, BusinessID: "2",
			DimensionIdentity: contract.DimensionIdentityV2{Fields: fields, Digest: dimension},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(raw)},
			Dimensions:        shadowDimensions(host, primary, provider.padding), ReceivedTime: evaluation}}
		digest, err := contract.DeriveCanonicalDigestV2("shadow-delivery", records)
		if err != nil {
			return execution.ProviderCompletion{}, err
		}
		batch := execution.SeriesDelivery{PhysicalQuery: attempt.Spec.Digest, QueryRevision: attempt.Spec.PlanFacts.QueryRevision,
			Series: 1, Records: 1, Digest: digest}
		if err := sink.ConsumeProviderSeries(ctx, execution.ProviderSeriesBatch{PhysicalQuery: attempt.Spec.Digest,
			CompletionRef: ref, Dataset: execution.NewDataset(records), Delivery: batch}); err != nil {
			return execution.ProviderCompletion{}, err
		}
		if delivery, err = execution.AccumulateSeriesDelivery(delivery, batch); err != nil {
			return execution.ProviderCompletion{}, err
		}
		delivered++
	}
	completeness := execution.CompletenessFull
	if partial && primary {
		completeness = execution.CompletenessPartial
	}
	completion := execution.ProviderCompletion{Ref: ref, PhysicalQuery: attempt.Spec.Digest, Completeness: completeness,
		DataState: execution.DataStateData, Delivery: delivery,
		RouteFacts: execution.ProviderRouteFacts{ProviderRouteRef: attempt.Spec.PlanFacts.ProviderRouteRef},
		Stats:      execution.ProviderStats{Series: uint64(delivered), Records: uint64(delivered)}}
	if delivered == 0 {
		completion.DataState, completion.Delivery = execution.DataStateEmpty, execution.SeriesDelivery{}
	}
	return completion, nil
}

func shadowDimensions(host string, primary bool, padding string) map[string]json.RawMessage {
	dimensions := map[string]json.RawMessage{"host": json.RawMessage(strconv.Quote(host))}
	if primary && padding != "" {
		dimensions["padding"] = json.RawMessage(strconv.Quote(padding))
	}
	return dimensions
}

// shadowRecoveryOff is the Level's recovery switched off: the fixture's Plan
// is off the consumer's protocol and carries no compatibility output, so a
// RECOVERY would be built for every quiet series every round - production
// holds it at the open-alert gate or sends it without a message - and an
// event sent makes every member an exception.
const shadowRecoveryOff = `{"enabled":false,"consecutive_windows":1}`

func newShadowRig(t *testing.T, hosts, padding int, recovery string) *shadowRig {
	t.Helper()
	return newShadowRigWith(t, hosts, padding, recovery, 0, nil, 0)
}

// newShadowRigWith is the rig with a Plan of its own snapshot revision and
// no-data detection, and a store whose State cache holds cacheBudget bytes,
// none at zero.
func newShadowRigWith(t *testing.T, hosts, padding int, recovery string, snapshotRevision int64,
	noData *contract.NoDataConfigV1, cacheBudget int64) *shadowRig {
	t.Helper()
	facts := ringRatioQueryFacts(t)
	ref := execution.LogicalQueryRef(facts.QueryRevision)
	compiled, requirements := ringRatioCompiledPlanWith(t, string(ref), recovery, snapshotRevision, noData)
	provider := &shadowProvider{padding: strings.Repeat("p", padding)}
	for index := 0; index < hosts; index++ {
		provider.hosts = append(provider.hosts, fmt.Sprintf("host-%02d", index))
	}
	rig := &shadowRig{t: t, provider: provider, frozen: &shadowFrozen{}, base: windowedRequest(),
		observed: &shadowObserved{outcomes: map[observability.SegmentShadowOutcome]int64{}, members: map[observability.SegmentShadowEvent]int64{}}}
	rig.frozen.plan = access.FrozenPlan{QueryFacts: map[execution.LogicalQueryRef]execution.QueryPlanFacts{ref: facts}}
	rig.base.Contract.QueryRevision = facts.QueryRevision
	rig.frozen.plan.Requirements = requirements
	rig.frozen.plan.DuePlans = []execution.DuePlan{{Identity: planIdentity(), CompiledPlan: compiled, StateGeneration: "state-v1",
		StateApplyEpoch: 1, ScheduleSpec: execution.ScheduleSpec{EvaluationIntervalSeconds: 60, Timezone: "UTC"},
		ScheduleRevision: "plan-schedule-v1"}}
	source, err := access.NewSource(rig.frozen, provider, ringRatioPermits{},
		access.Config{MinReadyDelay: time.Second, Now: func() time.Time {
			rig.readMu.Lock()
			defer rig.readMu.Unlock()
			return rig.readAt
		}})
	if err != nil {
		t.Fatal(err)
	}
	detector, err := detect.NewEvaluator(detect.NewDefaultRegistry())
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := evaluation.New(detector, evaluation.Limits{MaxLevels: 16,
		Trigger: trigger.EvaluationLimitsV2{MaxLevels: 16, MaxTriggerWindowSize: 16,
			MaxRecoveryConsecutiveWindows: 16, MaxRequiredHistoryPoints: 32,
			MaxLevelResultsPerEvent: 16, MaxEvidenceBytesPerEvent: 1 << 20, MaxComputeCost: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	address := startG3ARedis(t)
	real, backend := openG3AStateStore(t, address, "alarmd-shadow")
	rig.client = redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = rig.client.Close() })
	t.Cleanup(func() { _ = backend.Close() })
	if cacheBudget > 0 {
		router, err := state.NewFixedRouter("shadow-cached", backend)
		if err != nil {
			t.Fatal(err)
		}
		// The lifetimes in production's relation: the replay age, the
		// restart margin and the shortest lifetime all ten minutes.
		real, err = state.NewExecutionStore(state.ExecutionStoreOptions{Prefix: "alarmd-shadow", Router: router,
			MaxValueBytes: 1 << 20, MaxItemsPerCall: 16, MinTTL: 10 * time.Minute, MaxTTL: time.Hour,
			RestartMargin: 10 * time.Minute, StateCacheBudgetBytes: cacheBudget, StateCacheReplayAge: 10 * time.Minute,
			Now: time.Now})
		if err != nil {
			t.Fatal(err)
		}
	}
	store := &lateLandingStore{ExecutionStore: real}
	rig.store = store
	direct, err := state.NewFixedRouter("shadow-direct", backend)
	if err != nil {
		t.Fatal(err)
	}
	rig.redis, err = state.NewExecutionStore(state.ExecutionStoreOptions{Prefix: "alarmd-shadow", Router: direct,
		MaxValueBytes: 1 << 20, MaxItemsPerCall: 16, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	trace := make([]string, 0)
	// No gap marker: the fake's default holds every round WARMING, which no
	// Plan of a live Query Group stays.
	rig.ports = &recordingPorts{trace: &trace, ready: true, gapMissing: true}
	rig.coordinator, err = worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: rig.ports, Finalization: rig.ports,
		Activation: rig.ports, Query: source, Sequencer: rig.ports, Evaluator: evaluator, Admission: rig.ports,
		GapGuard: rig.ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: rig.ports, State: store, Progress: rig.ports, Observer: rig.observed,
		// The worker's wall clock, which renewals are judged and timed by,
		// run ahead of the real one by wallAhead.
		Now: func() time.Time { return time.Now().Add(time.Duration(rig.wallAhead.Load())) }},
		worker.ProvisionalBudget{MaxSeries: 400, MaxRetainedBytes: slicedPool, MaxStateMutations: 400, MaxEvents: 400, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

// storedMark is the Plan's mark as the store holds it in Redis.
func (rig *shadowRig) storedMark(at execution.EvaluationTime) segment.Mark {
	rig.t.Helper()
	contract := rig.base.Contract
	contract.Slot.EvaluationTime = at
	read, err := rig.redis.ReadSegments(context.Background(), segment.ReadRequest{Contract: contract,
		Plans: []segment.ReadPlan{{Key: segment.PlanKey{Plan: planIdentity(), Generation: "state-v1"}}}})
	if err != nil {
		rig.t.Fatal(err)
	}
	_, mark, stored, err := read.Plans[0].Book()
	if err != nil || !stored {
		rig.t.Fatalf("the Plan's mark: stored %v, %v", stored, err)
	}
	return mark
}

// storedMembers is the Plan's book as the store holds it in Redis after the
// Slot at at: its open and closed members.
func (rig *shadowRig) storedMembers(at execution.EvaluationTime) (open, closed int) {
	rig.t.Helper()
	contract := rig.base.Contract
	contract.Slot.EvaluationTime = at
	read, err := rig.redis.ReadSegments(context.Background(), segment.ReadRequest{Contract: contract,
		Plans: []segment.ReadPlan{{Key: segment.PlanKey{Plan: planIdentity(), Generation: "state-v1"}}}})
	if err != nil {
		rig.t.Fatal(err)
	}
	book, _, stored, err := read.Plans[0].Book()
	if err != nil || !stored {
		rig.t.Fatalf("the Plan's book: stored %v, %v", stored, err)
	}
	book.Each(func(entry segment.Entry) {
		if entry.Open() {
			open++
		} else {
			closed++
		}
	})
	return open, closed
}

// run executes the Slot at evaluation, the hosts given absent and the values
// given for the round's PRIMARY points, and reports whether it completed.
func (rig *shadowRig) run(evaluation execution.EvaluationTime, absent []string, value map[string]string, partial bool) (bool, error) {
	result, err := rig.execute(evaluation, absent, value, partial)
	return err == nil && result.Completed, err
}

func (rig *shadowRig) execute(evaluation execution.EvaluationTime, absent []string, value map[string]string, partial bool) (execution.SlotExecutionResult, error) {
	rig.t.Helper()
	rig.provider.round(int64(evaluation), absent, value, partial)
	rig.readMu.Lock()
	rig.readAt = time.Unix(int64(evaluation)+120, 0)
	rig.readMu.Unlock()
	due := rig.frozen.plan.DuePlans[0]
	due.CompletionDeadlineUnixMilli = int64(evaluation)*1000 + 600_000
	requirements := append([]execution.DataRequirement(nil), rig.frozen.plan.Requirements...)
	consumer := execution.ConsumerRef{Plan: due.Identity, LevelID: 5, HasLevel: true}
	for index := range requirements {
		requirements[index].Consumers = []execution.DataRequirementConsumer{{Consumer: consumer,
			ConsumerDeadlineUnixMilli: due.CompletionDeadlineUnixMilli, DownstreamExecutionReserveMilliSec: 5_000}}
	}
	digest, err := execution.DeriveDuePlanSetDigest([]execution.DuePlan{due}, requirements)
	if err != nil {
		rig.t.Fatal(err)
	}
	rig.frozen.mu.Lock()
	rig.frozen.plan.DuePlans, rig.frozen.plan.Requirements = []execution.DuePlan{due}, requirements
	rig.frozen.mu.Unlock()
	request := rig.base
	request.Contract.Slot.EvaluationTime, request.Contract.DuePlanSetDigest = evaluation, digest
	request.ExpectedNextSlot = evaluation
	request.DuePlanTargets = execution.FrozenDuePlanTargets{DuePlanSetDigest: digest, Plans: []execution.PlanKey{due.Key()}}
	return rig.coordinator.Execute(context.Background(), request)
}

// lateLandingStore is the State store with a write that lands late: armed,
// it answers its next write as a timeout and keeps it; landing, before the
// next write it lets the kept one reach Redis - the earlier attempt's write
// its client gave up on, still queued on another connection - and then
// writes as asked.
type lateLandingStore struct {
	*state.ExecutionStore
	mu      sync.Mutex
	armed   bool
	landing bool
	kept    []func(context.Context) error
}

func (store *lateLandingStore) late(ctx context.Context, write func(context.Context) error) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.armed {
		store.armed = false
		store.kept = append(store.kept, write)
		return true, errors.New("i/o timeout: the write's outcome is unknown")
	}
	if store.landing {
		store.landing = false
		for _, kept := range store.kept {
			if err := kept(ctx); err != nil {
				return true, err
			}
		}
		store.kept = nil
	}
	return false, nil
}

func (store *lateLandingStore) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	if kept, err := store.late(ctx, func(ctx context.Context) error {
		_, err := store.ExecutionStore.ApplyRuntime(ctx, request)
		return err
	}); kept {
		return execution.StateApplyResult{}, err
	}
	return store.ExecutionStore.ApplyRuntime(ctx, request)
}

func (store *lateLandingStore) ApplyRuntimeFenced(ctx context.Context, request execution.StateApplyRequest,
	fence execution.StateApplyFence) (execution.StateApplyResult, error) {
	if kept, err := store.late(ctx, func(ctx context.Context) error {
		_, err := store.ExecutionStore.ApplyRuntimeFenced(ctx, request, fence)
		return err
	}); kept {
		return execution.StateApplyResult{}, err
	}
	return store.ExecutionStore.ApplyRuntimeFenced(ctx, request, fence)
}

// The shadow, run where production runs it: Slot after Slot through the
// coordinator's Execute, the real evaluator deciding and a real State store
// over redis-server keeping what each round wrote. Steady hosts join and
// match; a host whose value drops is an exception; one absent for a round
// departs and comes back an exception; one gone for good departs and expires;
// a skipped interval, a PARTIAL round and a retried Slot leave nothing read
// otherwise than written. Over all of it no field of a made read differs
// from the write, and every membership event happens: the hooks - observe
// after the write, retried at an applied series, the fold after Progress -
// are on the path, or the counts would be zeros that read as a pass.
func TestTheSegmentShadowThroughExecuteMatchesEveryWriteItWouldHaveLeft(t *testing.T) {
	rig := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	at := frozenContract().Slot.EvaluationTime
	step := func(absent []string, value map[string]string, partial bool) {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, absent, value, partial); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	for round := 0; round < 6; round++ {
		step(nil, nil, false)
	}
	step(nil, map[string]string{"host-01": "10"}, false) // a drop past the floor: anomalous, an event
	step(nil, nil, false)
	step([]string{"host-02"}, nil, false) // away one round
	step(nil, nil, false)
	for round := 0; round < 4; round++ {
		step(nil, nil, false)
	}
	at += 60 // an interval skipped
	step(nil, nil, false)
	for round := 0; round < 3; round++ {
		step(nil, nil, false)
	}
	step(nil, nil, true) // a PARTIAL round
	for round := 0; round < 3; round++ {
		step(nil, nil, false)
	}
	// A Slot whose Progress fails after its State is written, then its retry.
	before := rig.storedMark(at).Position
	at += 60
	rig.ports.failStage = "progress_commit"
	if completed, _ := rig.run(at, nil, nil, false); completed {
		t.Fatal("the Slot completed with its Progress failing")
	}
	// Written before the Progress that failed: the round's mark is its own,
	// one interval on.
	if mark := rig.storedMark(at); mark.Position != before+60 {
		t.Fatalf("after a failed Progress the stored mark is at %d, want the round's own, %d", mark.Position, before+60)
	}
	rig.ports.failStage = ""
	if completed, err := rig.run(at, nil, nil, false); !completed {
		t.Fatalf("the retry: err=%v", err)
	}
	for round := 0; round < 3; round++ {
		step(nil, nil, false)
	}
	// host-03 gone for good: it departs, and expires past its reach.
	for round := 0; round < 40; round++ {
		step([]string{"host-03"}, nil, false)
	}
	outcomes, members := rig.observed.snapshot()
	t.Logf("outcomes %v members %v", outcomes, members)
	if outcomes[observability.SegmentShadowMatch] == 0 || exceptionsIn(outcomes) == 0 {
		t.Fatalf("outcomes %v, want matches and exceptions", outcomes)
	}
	for outcome, count := range outcomes {
		if outcome != observability.SegmentShadowMatch && !observability.SegmentShadowException(outcome) && count != 0 {
			t.Errorf("%s = %d: a made read differs from the write", outcome, count)
		}
	}
	for _, event := range []observability.SegmentShadowEvent{observability.SegmentShadowJoined, observability.SegmentShadowDeparted,
		observability.SegmentShadowExpired, observability.SegmentShadowReset} {
		if members[event] == 0 {
			t.Errorf("no %s: the round's membership did not do what it was run to", event)
		}
	}
	// The book lives in the store: written every round before Progress, read
	// once a Slot before its State - from Redis the first time, from the
	// State cache under the same fence after, a retry's among them - and
	// never read back other than it was written.
	stats := rig.store.SegmentStats()
	t.Logf("segment reads %v writes %v drift %d cache plans %d pages %d bytes %d", stats.Reads, stats.Writes, stats.Drift,
		stats.Plans, stats.Pages, stats.Bytes)
	for _, result := range segment.WriteResults {
		if result != segment.WriteApplied && stats.Writes[result] != 0 {
			t.Errorf("%d segment writes %s", stats.Writes[result], result)
		}
	}
	if stats.Writes[segment.WriteApplied] < 60 {
		t.Errorf("%d segment writes applied over more than 60 Slots, want one a round", stats.Writes[segment.WriteApplied])
	}
	for _, reason := range segment.ReadReasons {
		if want := uint64(map[bool]int{true: 1}[reason == segment.ReadCold]); stats.Reads[[2]string{segment.SourceRedis, reason}] != want {
			t.Errorf("%d segment reads from Redis %s, want %d", stats.Reads[[2]string{segment.SourceRedis, reason}], reason, want)
		}
	}
	if stats.Reads[[2]string{segment.SourceCache, segment.ReadHit}] < 60 || stats.Drift != 0 || stats.Plans != 1 {
		t.Errorf("segment reads %v drift %d plans %d: want every Slot after the first served from the cache", stats.Reads,
			stats.Drift, stats.Plans)
	}
	if open, _ := rig.storedMembers(at); open == 0 {
		t.Error("the book in Redis has no open member")
	}
}

// The same through a Slot read in slices and applied window by window: its
// steady hosts match round after round, and a retry after a failed Progress
// - its series resumed before evaluation, counted by the window's own
// bookkeeping - starts the Plan's shadow over rather than closing them.
func TestTheSegmentShadowThroughAWindowedSlotStartsOverOnARetry(t *testing.T) {
	rig := newShadowRig(t, 24, 40<<10, shadowRecoveryOff)
	at := frozenContract().Slot.EvaluationTime
	windowed := 0
	step := func() {
		t.Helper()
		at += 60
		result, err := rig.execute(at, nil, nil, false)
		if err != nil || !result.Completed {
			t.Fatalf("Slot at %d: result=%+v err=%v", at, result, err)
		}
		if result.Usage.InputSlices >= 2 {
			windowed++
		}
	}
	for round := 0; round < 4; round++ {
		step()
	}
	at += 60
	rig.ports.failStage = "progress_commit"
	if result, _ := rig.execute(at, nil, nil, false); result.Completed {
		t.Fatal("the Slot completed with its Progress failing")
	}
	rig.ports.failStage = ""
	if result, err := rig.execute(at, nil, nil, false); err != nil || !result.Completed {
		t.Fatalf("the retry: result=%+v err=%v", result, err)
	}
	for round := 0; round < 3; round++ {
		step()
	}
	outcomes, members := rig.observed.snapshot()
	t.Logf("outcomes %v members %v windowed %d", outcomes, members, windowed)
	if windowed == 0 {
		t.Fatal("no Slot was read in slices: the case is not the windowed one")
	}
	if outcomes[observability.SegmentShadowMatch] == 0 || members[observability.SegmentShadowReset] == 0 {
		t.Fatalf("outcomes %v members %v, want matches and the retry's reset", outcomes, members)
	}
	if members[observability.SegmentShadowDeparted] != 0 {
		t.Fatalf("%d departed: the retry's resumed series were read as absent", members[observability.SegmentShadowDeparted])
	}
	for outcome, count := range outcomes {
		if outcome != observability.SegmentShadowMatch && !observability.SegmentShadowException(outcome) && count != 0 {
			t.Errorf("%s = %d: a made read differs from the write", outcome, count)
		}
	}
}

// A member whose round sent events is no member the skip would leave: a
// steady host joins, then its value drops - anomalous, an event sent - and
// it is an exception by its events, never a match, through the real path
// from the evaluator's events to the shadow; and a quiet series that sends
// events every round - the fixture's recovery on - joins no page at all.
func TestAMemberWhoseRoundSentEventsIsAnExceptionNeverAMatch(t *testing.T) {
	rig := newShadowRig(t, 4, 0, shadowRecoveryOff)
	at := frozenContract().Slot.EvaluationTime
	step := func(value map[string]string) {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, nil, value, false); !completed {
			t.Fatalf("Slot at %d: err=%v", at, err)
		}
	}
	for round := 0; round < 5; round++ {
		step(nil)
	}
	before, _ := rig.observed.snapshot()
	sent := len(rig.ports.acknowledged)
	step(map[string]string{"host-00": "10"}) // a drop past the floor of 50%
	after, _ := rig.observed.snapshot()
	t.Logf("before %v after %v events %d -> %d", before, after, sent, len(rig.ports.acknowledged))
	if len(rig.ports.acknowledged) == sent {
		t.Fatal("the drop sent no event: the case is not the one with events")
	}
	if after[observability.SegmentShadowExceptionEvents]-before[observability.SegmentShadowExceptionEvents] != 1 ||
		after[observability.SegmentShadowMatch]-before[observability.SegmentShadowMatch] != 3 {
		t.Fatalf("the drop's round: %v -> %v, want the dropping member an exception by its events and the other three matched", before, after)
	}

	recovering := newShadowRig(t, 4, 0, `{"enabled":true,"consecutive_windows":1}`)
	for round := 1; round <= 5; round++ {
		if completed, err := recovering.run(frozenContract().Slot.EvaluationTime+execution.EvaluationTime(60*round), nil, nil, false); !completed {
			t.Fatalf("round %d: err=%v", round, err)
		}
	}
	if outcomes, members := recovering.observed.snapshot(); len(outcomes) != 0 || members[observability.SegmentShadowJoined] != 0 {
		t.Fatalf("quiet series sending events every round: outcomes %v members %v, want none joined or compared", outcomes, members)
	}
}

func exceptionsIn(outcomes map[observability.SegmentShadowOutcome]int64) int64 {
	var count int64
	for outcome, n := range outcomes {
		if observability.SegmentShadowException(outcome) {
			count += n
		}
	}
	return count
}

// A retry whose earlier attempt's write landed late - its client gave up, the
// write still queued, reaching Redis after the retry read the State and
// before it writes - meets its own statement at the store, which takes it
// as applied: the round's members are written and compared as any round's,
// none closed as absent and the Plan not started over.
func TestARetryMeetingItsEarlierAttemptsLateWriteKeepsTheRoundsMembers(t *testing.T) {
	rig := newShadowRig(t, 4, 0, shadowRecoveryOff)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 4; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: err=%v", at, err)
		}
	}
	at += 60
	rig.store.mu.Lock()
	rig.store.armed = true
	rig.store.mu.Unlock()
	if completed, _ := rig.run(at, nil, nil, false); completed {
		t.Fatal("the Slot completed with its write timing out")
	}
	before, beforeMembers := rig.observed.snapshot()
	rig.store.mu.Lock()
	rig.store.landing = true
	rig.store.mu.Unlock()
	if completed, err := rig.run(at, nil, nil, false); !completed {
		t.Fatalf("the retry: err=%v", err)
	}
	after, afterMembers := rig.observed.snapshot()
	rig.observed.mu.Lock()
	applied := rig.observed.applied
	rig.observed.mu.Unlock()
	t.Logf("before %v %v after %v %v applied %v", before, beforeMembers, after, afterMembers, applied)
	var landed int64
	for _, count := range applied {
		landed += count
	}
	if landed == 0 {
		t.Fatal("the store found nothing already applied: the late write did not land before the retry's")
	}
	if afterMembers[observability.SegmentShadowReset] != beforeMembers[observability.SegmentShadowReset] ||
		afterMembers[observability.SegmentShadowDeparted] != beforeMembers[observability.SegmentShadowDeparted] {
		t.Fatalf("members %v -> %v: the late write started the Plan over or closed its members", beforeMembers, afterMembers)
	}
	if after[observability.SegmentShadowMatch]-before[observability.SegmentShadowMatch] != 4 {
		t.Fatalf("outcomes %v -> %v, want the four members matched on the retry", before, after)
	}
	for outcome, count := range after {
		if outcome != observability.SegmentShadowMatch && !observability.SegmentShadowException(outcome) && count != 0 {
			t.Errorf("%s = %d: a made read differs from the write", outcome, count)
		}
	}
}

// A no-data series is no member of a page (014 B3 3.7): the Plan's no-data
// detection says a NORMAL for its present whole-item group every round -
// steady, normal and quiet, its RECOVERY held at the open-alert gate on the
// consumer's standard protocol - and it stays off the shadow's books while
// the Plan's real series join and match beside it.
func TestANoDataSeriesStaysOffTheShadowsBooksBesideThePlansRealSeries(t *testing.T) {
	const hosts = 3
	rig := newShadowRigWith(t, hosts, 0, `{"enabled":true,"consecutive_windows":1}`, 1,
		&contract.NoDataConfigV1{Continuous: 1, Level: 2}, 0)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 4; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: err=%v", at, err)
		}
	}
	rig.observed.mu.Lock()
	classes := rig.observed.classes
	rig.observed.mu.Unlock()
	outcomes, members := rig.observed.snapshot()
	open, closed := rig.storedMembers(at)
	t.Logf("classes %v outcomes %v members %v stored open %d closed %d", classes, outcomes, members, open, closed)
	if classes[fmt.Sprint(observability.StateSeriesNoData, "/", observability.StateSeriesClassStableNormal)] == 0 {
		t.Fatal("no no-data series was written steady and normal: the case does not have the series it guards against")
	}
	if open != hosts || members[observability.SegmentShadowJoined] != hosts {
		t.Fatalf("stored %d open members, joined %d: want the %d real hosts on the page and no no-data series", open,
			members[observability.SegmentShadowJoined], hosts)
	}
	if outcomes[observability.SegmentShadowMatch] == 0 {
		t.Fatalf("outcomes %v: the real series beside the no-data one did not match", outcomes)
	}
}

// A page due by the worker's wall clock is renewed through the production
// wiring: Execute on the real store, its lifetimes in production's relation,
// the worker's clock past the page's quarter lifetime. Every member key the
// renewal names is one the State write path wrote - none is found gone -
// so the names the renewal builds are the State's. In the second stage the
// round writes those keys whole as well, so their lifetime cannot tell the
// renewal reached them; that it found every one of them can.
func TestAPageDueByTheWorkersClockIsRenewedOnTheKeysTheStateWrote(t *testing.T) {
	const hosts = 4
	rig := newShadowRigWith(t, hosts, 0, shadowRecoveryOff, 0, nil, 16<<20)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 3; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: err=%v", at, err)
		}
	}
	mark := rig.storedMark(at)
	if open, _ := rig.storedMembers(at); open != hosts {
		t.Fatalf("%d open members before the page falls due, want %d", open, hosts)
	}
	if renewed := rig.observed.renewal(observability.SegmentRenewalRenewed); renewed != 0 {
		t.Fatalf("%d renewals before the page fell due", renewed)
	}
	if keys := rig.client.Keys(context.Background(), "alarmd-shadow:runtime3:*").Val(); len(keys) < hosts {
		t.Fatalf("%d State keys in Redis, want the %d members' at least", len(keys), hosts)
	}
	// Past the page's quarter lifetime by the worker's clock: its new page's
	// time is no later than when it was made.
	ahead := time.Duration(mark.Renewed[0].TTL/4)*time.Millisecond + time.Minute
	rig.wallAhead.Store(int64(ahead))
	at += 60
	if completed, err := rig.run(at, nil, nil, false); !completed {
		t.Fatalf("the Slot the page fell due in: err=%v", err)
	}
	renewed, missing, failed := rig.observed.renewal(observability.SegmentRenewalRenewed),
		rig.observed.renewal(observability.SegmentRenewalMissing), rig.observed.renewal(observability.SegmentRenewalFailed)
	if renewed != 1 || missing != 0 || failed != 0 {
		t.Fatalf("renewals renewed %d, missing %d, failed %d: want the page renewed with every member key found", renewed, missing, failed)
	}
	if after := rig.storedMark(at); after.Renewed[0].At < time.Now().Add(ahead-time.Minute).UnixMilli() {
		t.Fatalf("the page's renewal time %d did not move to the worker's clock", after.Renewed[0].At)
	}
}
