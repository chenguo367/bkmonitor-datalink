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
	"reflect"
	"sort"
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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
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
	wallAhead atomic.Int64
	// skipWord, when set, is the segment skip word each Slot is dispatched
	// with and the epoch of the view the worker holds: the Slots skip.
	skipWord atomic.Uint64
	// attempt, when set, is the attempt number each Slot is dispatched as.
	attempt     atomic.Uint32
	ports       *recordingPorts
	provider    *shadowProvider
	frozen      *shadowFrozen
	coordinator *worker.SlotExecutionCoordinator
	source      *access.Source
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
	// notWritten is the admitted mutations not written, by reason.
	notWritten map[observability.StateNotWrittenReason]int64
	// notSkipped is the matches written all the same, by gate; rounds the
	// Plans' rounds, by skip mode.
	notSkipped map[string]int64
	rounds     map[string]int64
	// next, when set, is handed every observation after it is summed: the
	// observer the replica's observations go on to (the fleet tracker).
	next observability.Observer
}

// renewal is the page renewals counted under result.
func (observed *shadowObserved) renewal(result string) int64 {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	return observed.renewals[result]
}

func (observed *shadowObserved) Observe(ctx context.Context, observation observability.Observation) {
	if observed.next != nil {
		defer observed.next.Observe(ctx, observation)
	}
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
		for reason, count := range census.NotWritten {
			if observed.notWritten == nil {
				observed.notWritten = map[observability.StateNotWrittenReason]int64{}
			}
			observed.notWritten[reason] += count
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
	for key, count := range facts.NotSkipped {
		if observed.notSkipped == nil {
			observed.notSkipped = map[string]int64{}
		}
		observed.notSkipped[key.Gate] += count
	}
	for key, count := range facts.Rounds {
		if observed.rounds == nil {
			observed.rounds = map[string]int64{}
		}
		observed.rounds[key.Mode] += count
	}
}

// matchLedger is the rig's matches so far, the members it left unwritten,
// the matches it wrote by gate, and its Plans' rounds by skip mode.
func (observed *shadowObserved) matchLedger() (matches, left int64, gates, rounds map[string]int64) {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	gates, rounds = map[string]int64{}, map[string]int64{}
	for gate, count := range observed.notSkipped {
		gates[gate] = count
	}
	for mode, count := range observed.rounds {
		rounds[mode] = count
	}
	return observed.outcomes[observability.SegmentShadowMatch],
		observed.notWritten[observability.StateNotWrittenSegmentMemberSkipped], gates, rounds
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
	return newShadowRigPlanned(t, hosts, padding, cacheBudget, facts, compiled, requirements)
}

// newThresholdShadowRig is the rig over a threshold Plan, the Level read on
// the primary query alone: one physical query a Slot, as a lookback reads a
// Slot again directed only of such Plans. Every host reads 60, under its
// threshold of 100: normal, its members stable.
func newThresholdShadowRig(t *testing.T, hosts int, cacheBudget int64) *shadowRig {
	t.Helper()
	facts := ringRatioQueryFacts(t)
	ref := execution.LogicalQueryRef(facts.QueryRevision)
	compiled, requirements := thresholdCompiledPlan(t, string(ref))
	return newShadowRigPlanned(t, hosts, 0, cacheBudget, facts, compiled, requirements)
}

// newShadowRigPlanned is the rig over the compiled Plan and its requirements
// given, on the query facts given.
func newShadowRigPlanned(t *testing.T, hosts, padding int, cacheBudget int64, facts execution.QueryPlanFacts,
	compiled *strategy.CompiledPlan, requirements []execution.DataRequirement) *shadowRig {
	t.Helper()
	ref := execution.LogicalQueryRef(facts.QueryRevision)
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
	rig.source = source
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
			MaxValueBytes: 1 << 20, MaxItemsPerCall: max(16, hosts), MinTTL: 10 * time.Minute, MaxTTL: time.Hour,
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
		MaxValueBytes: 1 << 20, MaxItemsPerCall: max(16, hosts), MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute})
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
		Now: func() time.Time { return time.Now().Add(time.Duration(rig.wallAhead.Load())) },
		InstalledControlEpoch: func() (uint64, bool) {
			word := rig.skipWord.Load()
			return word, word != 0
		}},
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
	rig.frozen.mu.Lock()
	due := rig.frozen.plan.DuePlans[0]
	requirements := append([]execution.DataRequirement(nil), rig.frozen.plan.Requirements...)
	rig.frozen.mu.Unlock()
	due.CompletionDeadlineUnixMilli = int64(evaluation)*1000 + 600_000
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
	request.SegmentSkip = rig.skipWord.Load()
	if attempt := rig.attempt.Load(); attempt > 0 {
		request.AttemptNo = attempt
	}
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
	// written is every mutation a fenced write carried, by series, the last;
	// items how many they were in all.
	written map[execution.StateKeyIdentity]execution.StateMutation
	items   int
	// failSegments is how many of the next segment writes fail, as of
	// unknown result, nothing sent; failReads the next segment reads.
	failSegments int
	failReads    int
}

func (store *lateLandingStore) ReadSegments(ctx context.Context, request segment.ReadRequest) (segment.ReadResult, error) {
	store.mu.Lock()
	if store.failReads > 0 {
		store.failReads--
		store.mu.Unlock()
		return segment.ReadResult{}, errors.New("dial tcp: connection refused")
	}
	store.mu.Unlock()
	return store.ExecutionStore.ReadSegments(ctx, request)
}

func (store *lateLandingStore) WriteSegments(ctx context.Context, request segment.WriteRequest) (segment.WriteResult, error) {
	store.mu.Lock()
	if store.failSegments > 0 {
		store.failSegments--
		store.mu.Unlock()
		return segment.WriteResult{}, errors.New("i/o timeout: the segments' write's outcome is unknown")
	}
	store.mu.Unlock()
	return store.ExecutionStore.WriteSegments(ctx, request)
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
	store.mu.Lock()
	if store.written == nil {
		store.written = map[execution.StateKeyIdentity]execution.StateMutation{}
	}
	for _, mutation := range request.Items {
		store.written[mutation.Identity] = mutation
	}
	store.items += len(request.Items)
	store.mu.Unlock()
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
	// Every member's State, read through Execute, was read through the
	// segments the Slot read (014 B3 3.2 rule 3): within - its own points
	// reach the segment's end, every State being written - and never
	// extended, nor refused.
	for _, extension := range segment.Extensions {
		if got := stats.Extends[extension]; (extension == segment.Within) != (got > 0) {
			t.Errorf("%d member reads %s through Execute, want within alone", got, extension)
		}
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

// stateRecords is every State record the rig's Redis holds, by key.
func (rig *shadowRig) stateRecords() map[string]string {
	rig.t.Helper()
	ctx := context.Background()
	keys, err := rig.client.Keys(ctx, "alarmd-shadow:runtime*").Result()
	if err != nil {
		rig.t.Fatal(err)
	}
	records := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := rig.client.Get(ctx, key).Result()
		if err != nil {
			rig.t.Fatal(err)
		}
		records[key] = value
	}
	return records
}

// A Slot that follows its segment skip word leaves its steady members' State
// unwritten and reads them through the segment: through Execute, round
// after round beside a rig that writes every State, it sends the same
// events, counts what it left as segment_member_skipped, reads the members
// it left extended - never refused - and asks no coverage after its fold;
// and once the word goes away, its first round, written whole from those
// reads, leaves Redis holding byte for byte what the rig that never skipped
// holds.
func TestASlotThatSkipsReadsAndWritesAsARigThatWritesEveryRound(t *testing.T) {
	skipping := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	writing := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	skipping.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	foldedBefore := worker.SegmentCoverageAfterFold()
	// sent is what a rig's round sent: each event by its series, kind,
	// Level and semantic digest - what the round decided, not how many. The
	// rig's Slot is one Plan of six series, whose round sends its events in
	// one batch: the last batch is the round's.
	sent := func(rig *shadowRig, before int) []string {
		var keys []string
		if rig.ports.eventCount > before {
			for _, event := range rig.ports.lastEvents {
				keys = append(keys, fmt.Sprint(event.RecordRef.DimensionIdentityDigest, "/", event.EventKind, "/",
					event.PrimaryLevelID, "/", event.EvaluationTime, "/", event.EventSemanticDigest))
			}
		}
		sort.Strings(keys)
		return keys
	}
	step := func(absent []string, value map[string]string) {
		t.Helper()
		at += 60
		decided := map[string][]string{}
		for name, rig := range map[string]*shadowRig{"skipping": skipping, "writing": writing} {
			before := rig.ports.eventCount
			if completed, err := rig.run(at, absent, value, false); !completed {
				t.Fatalf("%s Slot at %d: completed=%t err=%v", name, at, completed, err)
			}
			decided[name] = sent(rig, before)
		}
		if !reflect.DeepEqual(decided["skipping"], decided["writing"]) {
			t.Fatalf("at %d the skipping rig sent %v, the writing one %v", at, decided["skipping"], decided["writing"])
		}
	}
	for round := 0; round < 8; round++ {
		step(nil, nil)
	}
	step(nil, map[string]string{"host-01": "10"}) // anomalous: an event, written whole
	step(nil, nil)
	step([]string{"host-02"}, nil) // away one round
	for round := 0; round < 6; round++ {
		step(nil, nil)
	}
	if writing.ports.eventCount == 0 {
		t.Fatal("no round sent an event: the comparison of what they decided compared nothing")
	}
	skipping.observed.mu.Lock()
	skipped := skipping.observed.notWritten[observability.StateNotWrittenSegmentMemberSkipped]
	skipping.observed.mu.Unlock()
	if skipped == 0 {
		t.Fatal("the skipping rig left no member unwritten")
	}
	writing.observed.mu.Lock()
	neverSkipped := writing.observed.notWritten[observability.StateNotWrittenSegmentMemberSkipped]
	writing.observed.mu.Unlock()
	if neverSkipped != 0 {
		t.Fatalf("the rig without a word left %d members unwritten", neverSkipped)
	}
	extends := skipping.store.SegmentStats().Extends
	if extends[segment.Extended] == 0 {
		t.Fatalf("extensions %v: the skipping rig read none of the members it left through their segments", extends)
	}
	for _, extension := range segment.Extensions {
		if extension != segment.Extended && extension != segment.Within && extends[extension] != 0 {
			t.Errorf("%d member reads %s", extends[extension], extension)
		}
	}
	if after := worker.SegmentCoverageAfterFold(); after != foldedBefore {
		t.Fatalf("%d coverages asked after a fold", after-foldedBefore)
	}
	if !skipping.storedMark(at).Skipped || writing.storedMark(at).Skipped {
		t.Fatalf("marks say skipped %v and %v, want the skipping rig's round alone", skipping.storedMark(at).Skipped,
			writing.storedMark(at).Skipped)
	}
	// The word goes away: the round writes every State, the left members
	// from their extended reads.
	skipping.skipWord.Store(0)
	step(nil, nil)
	if skipping.storedMark(at).Skipped {
		t.Fatal("a round that wrote every State says it left one")
	}
	skipping.store.mu.Lock()
	writing.store.mu.Lock()
	for identity, want := range writing.store.written {
		got, written := skipping.store.written[identity]
		if !written {
			t.Errorf("%s never written by the skipping rig", identity.SeriesIdentityDigest)
			continue
		}
		// What the write stores: the history it read - the points the
		// segment made for the rounds left, as written ones - and the
		// round's own.
		if !reflect.DeepEqual(got.BaseHistory, want.BaseHistory) || !reflect.DeepEqual(got.Points, want.Points) {
			t.Errorf("%s last written history %+v + %+v\nwant %+v + %+v", identity.SeriesIdentityDigest, got.BaseHistory, got.Points,
				want.BaseHistory, want.Points)
		}
		if !reflect.DeepEqual(got.Levels, want.Levels) || !reflect.DeepEqual(got.SeriesGuard, want.SeriesGuard) {
			t.Errorf("%s last written Levels %+v guard %+v\nwant %+v guard %+v", identity.SeriesIdentityDigest, got.Levels, got.SeriesGuard,
				want.Levels, want.SeriesGuard)
		}
	}
	if len(writing.store.written) == 0 || len(skipping.store.written) != len(writing.store.written) {
		t.Errorf("%d series written, want %d", len(skipping.store.written), len(writing.store.written))
	}
	writing.store.mu.Unlock()
	skipping.store.mu.Unlock()
	// The records' own revision counts their writes, fewer where rounds were
	// left: the same keys are there, not the same bytes.
	got, want := skipping.stateRecords(), writing.stateRecords()
	if len(want) == 0 || len(got) != len(want) {
		t.Fatalf("%d State records, want %d", len(got), len(want))
	}
	for key := range want {
		if _, there := got[key]; !there {
			t.Errorf("%s missing from the skipping rig", key)
		}
	}
}

// A member departing in the first round after a Slot stopped skipping may
// have lived on its pages' renewals alone: its key is renewed to the full
// lifetime with the mark, because the round before left members unwritten
// (its mark says so), though this round wrote every State - so it comes
// back, within that lifetime, to the window it left. A rig that never
// skipped renews no departing key, as in the second stage.
func TestAMemberDepartingTheRoundAfterTheSkipStoppedIsRenewedWhole(t *testing.T) {
	skipping := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	writing := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	skipping.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	rigs := map[string]*shadowRig{"skipping": skipping, "writing": writing}
	step := func(absent []string) {
		t.Helper()
		at += 60
		for name, rig := range rigs {
			if completed, err := rig.run(at, absent, nil, false); !completed {
				t.Fatalf("%s Slot at %d: completed=%t err=%v", name, at, completed, err)
			}
		}
	}
	for round := 0; round < 8; round++ {
		step(nil)
	}
	if !skipping.storedMark(at).Skipped {
		t.Fatal("the skipping rig's last round left no member")
	}
	skipping.skipWord.Store(0)
	// Every State key's life cut to a minute, as a key that lived on its
	// pages' renewals could be left.
	ctx := context.Background()
	for _, rig := range rigs {
		for key := range rig.stateRecords() {
			if err := rig.client.PExpire(ctx, key, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	step([]string{"host-02"})
	short := func(rig *shadowRig) int {
		count := 0
		for key := range rig.stateRecords() {
			if life := rig.client.PTTL(ctx, key).Val(); life > 0 && life <= time.Minute {
				count++
			}
		}
		return count
	}
	if got := short(skipping); got != 0 {
		t.Fatalf("%d keys of the skipping rig still live a minute or less after the round, want the departing member's renewed", got)
	}
	if got := short(writing); got != 1 {
		t.Fatalf("%d keys of the rig that never skipped live a minute or less, want the departing member's alone, not renewed", got)
	}
}

// commits is how many Progress commits the rig's Slots made.
func (rig *shadowRig) commits() int {
	commits := 0
	for _, step := range *rig.ports.trace {
		if step == "progress_commit" {
			commits++
		}
	}
	return commits
}

// A round that leaves members unwritten leaves no witness behind: the
// witnesses its preflight kept for them, never taken by a write, are let go
// with the rest as the Slot's attempt ends, and the next Slot starts with
// none of the last one's.
func TestARoundThatLeavesMembersUnwrittenLeavesNoWitnessBehind(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 8 && rig.skippedSoFar() == 0; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	if rig.skippedSoFar() == 0 {
		t.Fatal("no round left a member unwritten")
	}
	if stats := rig.store.RuntimeWitnessStats(); stats.Items != 0 || stats.Bytes != 0 {
		t.Fatalf("after a round that left members unwritten the store holds %+v witnesses, want none", stats)
	}
}

// skippedSoFar is the members the rig's rounds left unwritten so far.
func (rig *shadowRig) skippedSoFar() int64 {
	rig.observed.mu.Lock()
	defer rig.observed.mu.Unlock()
	return rig.observed.notWritten[observability.StateNotWrittenSegmentMemberSkipped]
}

// A round that left members unwritten waits for its segments (014 B3 §9.4):
// their write not landing - of unknown result here - fails the Slot,
// SEGMENT_WRITE_FAILED and retryable, its Progress not committed. The next
// attempt of the Slot, a normal one, leaves no member: its own segment
// write failing too, it is counted and the Slot commits, every State
// written. The Slot after finds its members off the mark's grid and writes
// them whole; they join again and later rounds leave them. A rig that never
// skips only counts a failed segment write. Through it all both rigs send the same events, and
// once the word goes away they hold the same State.
func TestASegmentWriteFailingAfterARoundLeftMembersFailsTheSlotAndTheNextAttemptWritesWhole(t *testing.T) {
	skipping := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	writing := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	skipping.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	step := func() {
		t.Helper()
		at += 60
		for name, rig := range map[string]*shadowRig{"skipping": skipping, "writing": writing} {
			if completed, err := rig.run(at, nil, nil, false); !completed {
				t.Fatalf("%s Slot at %d: completed=%t err=%v", name, at, completed, err)
			}
		}
	}
	for round := 0; round < 8; round++ {
		step()
	}
	at += 60
	// The round leaves members, and its segments' write fails.
	skipping.store.mu.Lock()
	skipping.store.failSegments = 1
	skipping.store.mu.Unlock()
	commits, left := skipping.commits(), skipping.skippedSoFar()
	result, err := skipping.execute(at, nil, nil, false)
	if err != nil || result.Completed || result.Result != observability.ResultRetrying ||
		string(result.ReasonCode) != contract.ReasonSegmentWriteFailed {
		t.Fatalf("a round whose segments did not land after it left members = (%+v, %v), want retrying with SEGMENT_WRITE_FAILED", result, err)
	}
	if skipping.commits() != commits || skipping.skippedSoFar() == left {
		t.Fatalf("committed %d, left %d members: want no commit after a round that left some", skipping.commits()-commits,
			skipping.skippedSoFar()-left)
	}
	// The next attempt leaves none; its own segment write failing is counted
	// and the Slot commits.
	skipping.store.mu.Lock()
	skipping.store.failSegments, skipping.store.items = 1, 0
	skipping.store.mu.Unlock()
	left = skipping.skippedSoFar()
	if completed, err := skipping.run(at, nil, nil, false); !completed {
		t.Fatalf("the next attempt: completed=%t err=%v, want committed", completed, err)
	}
	skipping.store.mu.Lock()
	items := skipping.store.items
	skipping.store.mu.Unlock()
	if skipping.skippedSoFar() != left || items != 6 || skipping.commits() != commits+1 {
		t.Fatalf("the next attempt left %d members, wrote %d States, committed %d: want none left, all six written, one commit",
			skipping.skippedSoFar()-left, items, skipping.commits()-commits)
	}
	// A rig that never skips only counts its failed segment write.
	writing.store.mu.Lock()
	writing.store.failSegments = 1
	writing.store.mu.Unlock()
	if completed, err := writing.run(at, nil, nil, false); !completed {
		t.Fatalf("a round that left no member with its segments' write failing: completed=%t err=%v", completed, err)
	}
	// The Slot after finds the mark a round behind the State the attempt
	// wrote: its members are off the mark's grid, written whole and taken off
	// their pages. They join again, and the rounds after leave them.
	left = skipping.skippedSoFar()
	before, _ := skipping.observed.snapshot()
	step()
	after, _ := skipping.observed.snapshot()
	if skipping.skippedSoFar() != left || after[observability.SegmentShadowExceptionOffGrid]-before[observability.SegmentShadowExceptionOffGrid] != 6 {
		t.Fatalf("the Slot after left %d members, %d off the grid: want none left, all six off the mark's grid", skipping.skippedSoFar()-left,
			after[observability.SegmentShadowExceptionOffGrid]-before[observability.SegmentShadowExceptionOffGrid])
	}
	for round := 0; round < 3; round++ {
		step()
	}
	if skipping.skippedSoFar() == left {
		t.Fatal("no round after the members joined again left one")
	}
	skipping.skipWord.Store(0)
	step()
	skipping.store.mu.Lock()
	writing.store.mu.Lock()
	defer writing.store.mu.Unlock()
	defer skipping.store.mu.Unlock()
	if len(writing.store.written) == 0 || len(skipping.store.written) != len(writing.store.written) {
		t.Fatalf("%d series written, want %d", len(skipping.store.written), len(writing.store.written))
	}
	for identity, want := range writing.store.written {
		got := skipping.store.written[identity]
		if !reflect.DeepEqual(got.BaseHistory, want.BaseHistory) || !reflect.DeepEqual(got.Points, want.Points) ||
			!reflect.DeepEqual(got.Levels, want.Levels) {
			t.Errorf("%s last written %+v + %+v %+v\nwant %+v + %+v %+v", identity.SeriesIdentityDigest, got.BaseHistory, got.Points,
				got.Levels, want.BaseHistory, want.Points, want.Levels)
		}
	}
}

// A Slot dispatched with its word whose segments do not read fails by
// SEGMENT_READ_FAILED, through Execute, before anything is written; its
// retry reads them as stored and commits, every State written; the rounds
// after skip again, and once the word goes away the State is what a rig
// that never skipped holds.
func TestASlotWhoseSegmentsDidNotReadFailsByNameAndItsRetryCompletes(t *testing.T) {
	skipping := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	writing := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	skipping.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	step := func() {
		t.Helper()
		at += 60
		for name, rig := range map[string]*shadowRig{"skipping": skipping, "writing": writing} {
			if completed, err := rig.run(at, nil, nil, false); !completed {
				t.Fatalf("%s Slot at %d: completed=%t err=%v", name, at, completed, err)
			}
		}
	}
	for round := 0; round < 8; round++ {
		step()
	}
	at += 60
	skipping.store.mu.Lock()
	skipping.store.failReads, skipping.store.items = 1, 0
	skipping.store.mu.Unlock()
	commits := skipping.commits()
	_, err := skipping.execute(at, nil, nil, false)
	if word, named := worker.SegmentReadFailedReason(err); !named || word != contract.ReasonSegmentReadFailed {
		t.Fatalf("a Slot whose segments did not read = %v, want SEGMENT_READ_FAILED", err)
	}
	skipping.store.mu.Lock()
	items := skipping.store.items
	skipping.store.failReads = 1
	skipping.store.mu.Unlock()
	if items != 0 || skipping.commits() != commits {
		t.Fatalf("the failed attempt wrote %d States and committed %d", items, skipping.commits()-commits)
	}
	skipping.attempt.Store(2)
	left := skipping.skippedSoFar()
	if completed, err := skipping.run(at, nil, nil, false); !completed {
		t.Fatalf("the retry: completed=%t err=%v, want committed", completed, err)
	}
	skipping.attempt.Store(0)
	skipping.store.mu.Lock()
	items = skipping.store.items
	skipping.store.mu.Unlock()
	if items != 6 || skipping.skippedSoFar() != left {
		t.Fatalf("the retry wrote %d States and left %d members: want all six written", items, skipping.skippedSoFar()-left)
	}
	if completed, err := writing.run(at, nil, nil, false); !completed {
		t.Fatalf("writing Slot at %d: completed=%t err=%v", at, completed, err)
	}
	for round := 0; round < 4; round++ {
		step()
	}
	if skipping.skippedSoFar() == left {
		t.Fatal("no round after the retry left a member")
	}
	skipping.skipWord.Store(0)
	step()
	skipping.store.mu.Lock()
	writing.store.mu.Lock()
	defer writing.store.mu.Unlock()
	defer skipping.store.mu.Unlock()
	if len(writing.store.written) == 0 || len(skipping.store.written) != len(writing.store.written) {
		t.Fatalf("%d series written, want %d", len(skipping.store.written), len(writing.store.written))
	}
	for identity, want := range writing.store.written {
		got := skipping.store.written[identity]
		if !reflect.DeepEqual(got.BaseHistory, want.BaseHistory) || !reflect.DeepEqual(got.Points, want.Points) ||
			!reflect.DeepEqual(got.Levels, want.Levels) {
			t.Errorf("%s last written %+v + %+v %+v\nwant %+v + %+v %+v", identity.SeriesIdentityDigest, got.BaseHistory, got.Points,
				got.Levels, want.BaseHistory, want.Points, want.Levels)
		}
	}
}

// Every member the shadow compared a match is either left unwritten or
// counted under the gate that kept it written - the gates and the members
// left add up to the matches after every round - through Execute, in rounds
// without the skip word, with it and without it again: without, every match
// is the word's; with, the round leaves them and no gate is read; other,
// which no round reaches, stays zero throughout. Every round is counted once
// under the mode it ran in.
func TestEveryMatchIsLeftOrCountedUnderTheGateThatKeptItWritten(t *testing.T) {
	rig := newShadowRigWith(t, 6, 0, shadowRecoveryOff, 0, nil, 16<<20)
	at := frozenContract().Slot.EvaluationTime
	rounds := 0
	step := func() {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
		rounds++
		matches, left, gates, modes := rig.observed.matchLedger()
		sum := left
		for _, count := range gates {
			sum += count
		}
		if sum != matches {
			t.Fatalf("at %d: %d matches, %d left and gates %v adding to %d", at, matches, left, gates, sum)
		}
		if gates[observability.SegmentNotSkippedOther] != 0 {
			t.Fatalf("at %d: %d matches counted other", at, gates[observability.SegmentNotSkippedOther])
		}
		total := int64(0)
		for _, count := range modes {
			total += count
		}
		if total != int64(rounds) {
			t.Fatalf("at %d: rounds by mode %v, want %d in all", at, modes, rounds)
		}
	}
	// Without the word: every round's matches are written, as the word's.
	for round := 0; round < 6; round++ {
		step()
	}
	matches, left, gates, modes := rig.observed.matchLedger()
	if matches == 0 || left != 0 || gates[observability.SegmentNotSkippedSlotSkipOff] != matches || len(gates) != 1 {
		t.Fatalf("without the word: %d matches, %d left, gates %v; want every match under slot_skip_off", matches, left, gates)
	}
	if modes[segment.SkipModeOff] != 6 {
		t.Fatalf("rounds %v, want six off", modes)
	}
	// With it: the rounds leave their matches; none is counted under a gate.
	rig.skipWord.Store(7)
	for round := 0; round < 8; round++ {
		step()
	}
	matchesWith, leftWith, gatesWith, modesWith := rig.observed.matchLedger()
	if leftWith == 0 || leftWith != matchesWith-matches {
		t.Fatalf("with the word: %d more matches, %d left; want every new match left", matchesWith-matches, leftWith)
	}
	if !reflect.DeepEqual(gatesWith, gates) {
		t.Fatalf("with the word the gates moved from %v to %v", gates, gatesWith)
	}
	if modesWith[segment.SkipModeOff] != 6 || modesWith[segment.SkipModeSkip]+modesWith[segment.SkipModeSampled] != 8 {
		t.Fatalf("rounds %v, want six off and eight skipping", modesWith)
	}
	// Without it again: the word's matches again.
	rig.skipWord.Store(0)
	for round := 0; round < 2; round++ {
		step()
	}
	matchesAfter, leftAfter, gatesAfter, _ := rig.observed.matchLedger()
	if leftAfter != leftWith || gatesAfter[observability.SegmentNotSkippedSlotSkipOff]-gates[observability.SegmentNotSkippedSlotSkipOff] != matchesAfter-matchesWith {
		t.Fatalf("without the word again: %d more matches, %d more left, slot_skip_off %d more", matchesAfter-matchesWith,
			leftAfter-leftWith, gatesAfter[observability.SegmentNotSkippedSlotSkipOff]-gates[observability.SegmentNotSkippedSlotSkipOff])
	}
}
