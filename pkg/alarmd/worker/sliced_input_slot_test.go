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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The cases here run a Slot whose input does not fit half its share through
// the coordinator's one Execute path (decision-020 batch 8, 7.3-7.6), against
// a query source that reads a slice as the store does: a series is delivered
// when its value on the sliced dimension satisfies the request's conditions
// (InputSlice.Matches), the same function the conditions are rendered from.
// What a case expects is read from the source's own record of what it
// delivered, and from the stores' record of what was written, never from the
// slicer's own counters alone.

// slicedPool is the retained-byte pool of the sliced fixtures: a share of
// three quarters of it, 768 KiB, and a slice limit of at most half that.
const slicedPool = 1 << 20

const slicedShare = slicedPool / 4 * 3

// slicedSeries is one series of the sliced fixture: its value on the
// dimension the Slot is sliced on, and the padding, in a dimension outside
// its identity, that makes its input as large as the case needs.
type slicedSeries struct {
	host    string
	padding int
}

// digest is the series' identity, a function of its sliced value alone.
func (series slicedSeries) digest() string {
	sum := sha256.Sum256([]byte("sliced-series/" + series.host))
	return hex.EncodeToString(sum[:])
}

// slicedHosts is count series named host-00, host-01, ..., each padded to
// padding bytes: their last characters spread over the ten digits, as a
// numbered fleet's do.
func slicedHosts(count, padding int) []slicedSeries {
	series := make([]slicedSeries, count)
	for index := range series {
		series[index] = slicedSeries{host: fmt.Sprintf("host-%02d", index), padding: padding}
	}
	return series
}

// sliceableFacts is the frozen facts of a structured metric query grouped by
// host, every function aggregating by it: what SliceableDimensions accepts.
func sliceableFacts() *execution.QueryPlanFacts {
	return &execution.QueryPlanFacts{Provider: execution.ProviderUQ, QueryList: []execution.QueryClause{{
		TableID: "system.cpu_summary", Dimensions: []string{"host"},
		Functions: []execution.QueryFunction{{Method: "mean", Dimensions: []string{"host"}}},
	}}}
}

// slicedCall is what one query of the Slot asked and was answered.
type slicedCall struct {
	slice     execution.InputSlice
	delivered []string
	padding   int
	completed bool
	err       error
}

// slicedSource is the query source over a fixed set of series. It honours
// the request's slice unless ignoreSlice, which is a store that drops the
// conditions and answers the whole Query Group on every read. completeness
// answers each read that reaches its completion, given which one it is
// (from one) and, for a read that delivered series, which of those it is
// (from one; zero for a read that delivered none); FULL when nil. It is also
// asked before a read streams, with held -1: a read it answers UNAVAILABLE
// then streams nothing, as a query whose answer did not arrive delivered
// nothing. partial is filled with the series every read that did not
// complete delivered.
type slicedSource struct {
	t           *testing.T
	ports       *recordingPorts
	series      []slicedSeries
	facts       *execution.QueryPlanFacts
	ignoreSlice bool
	// plans and requirements replace the fixture's one Plan when set.
	plans        []execution.DuePlan
	requirements []execution.DataRequirement
	completeness func(read, held int) execution.Completeness
	calls        []slicedCall
	partial      map[string]bool
	// incomplete is set once any read was answered short of FULL.
	incomplete bool
}

func (source *slicedSource) Execute(ctx context.Context, request execution.QueryExecutionRequest, consumer execution.QueryExecutionConsumer) (execution.QueryExecutionCompletion, error) {
	source.ports.record("query")
	call := slicedCall{slice: request.Slice}
	completion, err := source.answer(ctx, request, consumer, &call)
	call.completed, call.err = err == nil, err
	source.calls = append(source.calls, call)
	return completion, err
}

func (source *slicedSource) answer(ctx context.Context, request execution.QueryExecutionRequest, consumer execution.QueryExecutionConsumer, call *slicedCall) (execution.QueryExecutionCompletion, error) {
	input := validInternalExecution()
	plans, requirements := input.DuePlans, input.Requirements
	if source.plans != nil {
		plans, requirements = source.plans, source.requirements
	}
	queryRevision := execution.QueryRevision(requirements[0].LogicalQueryRef)
	header := execution.InternalExecutionHeader{
		ExecutionID: "execution-1", Contract: request.Contract, DuePlans: plans, Requirements: requirements,
		RequiredPhysicalQueries: []execution.PlannedPhysicalQueryRef{{Digest: "physical-query-1", QueryRevision: queryRevision}},
		DeadlineUnixMilli:       1_788_000_030_000,
	}
	for _, series := range source.series {
		header.EffectiveTimeFacts = append(header.EffectiveTimeFacts, execution.BoundEffectiveTimeFact{
			Consumer:       execution.ConsumerRef{Plan: planIdentity(), LevelID: 5, HasLevel: true},
			SeriesIdentity: execution.SeriesIdentityDigest(series.digest()), Fact: effectiveTimeFactForTest(plans[0].CompiledPlan),
		})
	}
	if source.facts != nil {
		header.QueryFacts = map[execution.LogicalQueryRef]execution.QueryPlanFacts{requirements[0].LogicalQueryRef: *source.facts}
	}
	if err := consumer.Begin(ctx, header); err != nil {
		return execution.QueryExecutionCompletion{}, err
	}
	reads, heldBefore := 1, 0
	for _, earlier := range source.calls {
		if earlier.completed {
			reads++
			if len(earlier.delivered) != 0 {
				heldBefore++
			}
		}
	}
	unavailable := source.completeness != nil && source.completeness(reads, -1) == execution.CompletenessUnavailable
	var delivery execution.SeriesDelivery
	for _, series := range source.series {
		if unavailable {
			break
		}
		if !source.ignoreSlice {
			in, err := request.Slice.Matches(map[string]string{"host": series.host})
			if err != nil {
				return execution.QueryExecutionCompletion{}, err
			}
			if !in {
				continue
			}
		}
		host, _ := json.Marshal(series.host)
		dataset := execution.NewDataset([]contract.CanonicalRecordV2{{
			RecordID: derivedTestRecordID(), SourceTime: 1_788_000_000, BusinessID: "2",
			DimensionIdentity: contract.DimensionIdentityV2{Digest: series.digest()},
			Values:            map[string]json.RawMessage{"value": json.RawMessage(`50.1`)},
			Dimensions: map[string]json.RawMessage{"host": host,
				"padding": json.RawMessage(`"` + strings.Repeat("p", series.padding) + `"`)},
			ReceivedTime: 1_788_000_000,
		}})
		view, _ := execution.NewDatasetView(dataset, []uint32{0})
		binding := input.Inputs[0]
		binding.Dataset, binding.View = dataset, view
		batch := execution.SeriesDelivery{PhysicalQuery: "physical-query-1", QueryRevision: queryRevision,
			Series: 1, Records: 1, Digest: "delivery-" + series.digest()[:16]}
		accumulated, err := execution.AccumulateSeriesDelivery(delivery, batch)
		if err != nil {
			return execution.QueryExecutionCompletion{}, err
		}
		delivery = accumulated
		call.delivered = append(call.delivered, series.digest())
		call.padding += series.padding
		if err := consumer.ConsumeSeries(ctx, execution.SeriesExecutionBatch{
			PhysicalQuery: "physical-query-1", QueryRevision: queryRevision, CompletionRef: "provider-result-1",
			Dataset: dataset, Inputs: []execution.NamedInputBinding{binding}, Delivery: batch,
		}); err != nil {
			return execution.QueryExecutionCompletion{}, err
		}
	}
	completeness := execution.CompletenessFull
	if unavailable {
		completeness = execution.CompletenessUnavailable
	} else if source.completeness != nil {
		held := 0
		if len(call.delivered) != 0 {
			held = heldBefore + 1
		}
		completeness = source.completeness(reads, held)
	}
	physical := execution.PhysicalQueryCompletion{Ref: "provider-result-1", PhysicalQuery: "physical-query-1",
		QueryRevision: queryRevision, Completeness: completeness, DataState: execution.DataStateData, Delivery: delivery}
	if len(call.delivered) == 0 {
		physical.DataState = execution.DataStateEmpty
	}
	if completeness == execution.CompletenessUnavailable {
		physical.DataState = execution.DataStateUnknown
	}
	if completeness != execution.CompletenessFull {
		source.incomplete = true
		if source.partial == nil {
			source.partial = make(map[string]bool)
		}
		for _, series := range call.delivered {
			source.partial[series] = true
		}
	}
	if completeness == execution.CompletenessPartial {
		physical.PartialEvidence = &execution.PartialEvidence{Kind: execution.PartialEvidenceOmissionStable, Version: 1,
			EvidenceDigest: strings.Repeat("d", 64), OmissionOnly: true, ReturnedRecordsStable: true}
	}
	completion := execution.QueryExecutionCompletion{AllRequiredCompleted: true,
		PhysicalQueries: []execution.PhysicalQueryCompletion{physical}}
	completion.CompletionBindings = accessShapedCompletionBindings(source.t, header, completion.PhysicalQueries)
	return completion, nil
}

// landed is the reads that reached their completion, in order: the slices
// the Slot landed.
func (source *slicedSource) landed() []slicedCall {
	var landed []slicedCall
	for _, call := range source.calls {
		if call.completed {
			landed = append(landed, call)
		}
	}
	return landed
}

// slicedFixture is the coordinator over the sliced source, with State kept
// across attempts.
type slicedFixture struct {
	ports        *recordingPorts
	state        *persistedRetryState
	source       *slicedSource
	coordinator  *worker.SlotExecutionCoordinator
	observations *[]observability.Observation
}

// newSlicedFixture builds the fixture with a 1 MiB pool; maxStates is the
// State writes one window takes, so a slice can itself be written in
// windows.
func newSlicedFixture(t *testing.T, series []slicedSeries, facts *execution.QueryPlanFacts, maxStates uint64) *slicedFixture {
	t.Helper()
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace, ready: true}
	state := &persistedRetryState{recordingPorts: ports, saved: map[execution.StateKeyIdentity]execution.StateMutation{}}
	source := &slicedSource{t: t, ports: ports, series: series, facts: facts}
	observations := make([]observability.Observation, 0)
	fixture := &slicedFixture{ports: ports, state: state, source: source, observations: &observations}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: ports, State: state, GapGuard: ports, Events: ports}, maxStates)
	return fixture
}

// build makes the coordinator with the fixture's ports, the ones a case may
// replace taken from replaced, the fixture's evaluator when it names none.
func (fixture *slicedFixture) build(t *testing.T, replaced worker.Ports, maxStates uint64) *worker.SlotExecutionCoordinator {
	t.Helper()
	ports, observations := fixture.ports, fixture.observations
	if replaced.Evaluator == nil {
		replaced.Evaluator = ports
	}
	coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports, Finalization: ports,
		Activation: replaced.Activation, Query: fixture.source, Sequencer: ports, Evaluator: replaced.Evaluator, Admission: ports,
		GapGuard: replaced.GapGuard, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: replaced.Events, State: replaced.State, Progress: ports, ExecutionEvidence: newMemoryEvidenceStore(),
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			*observations = append(*observations, observability.NormalizeObservation(observation))
		})},
		worker.ProvisionalBudget{MaxSeries: 200, MaxRetainedBytes: slicedPool, MaxStateMutations: maxStates, MaxEvents: 200, MaxGapMutations: 10})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

// countDigests is how many times each series appears in lists.
func countDigests(lists ...[]string) map[string]int {
	counts := make(map[string]int)
	for _, list := range lists {
		for _, digest := range list {
			counts[digest]++
		}
	}
	return counts
}

// stateWrites is how many times each series' State was sent to the store.
func (fixture *slicedFixture) stateWrites() map[string]int {
	counts := make(map[string]int)
	for _, key := range fixture.ports.appliedKeys {
		counts[string(key.SeriesIdentityDigest)]++
	}
	return counts
}

// noDataCensuses is how many no-data censuses the Slot reported: one a Slot,
// whatever its no-data Plans decided.
func (fixture *slicedFixture) noDataCensuses() int {
	count := 0
	for _, observation := range *fixture.observations {
		if observation.Stage == observability.StageNoDataDecided && observation.NoDataCensus != nil {
			count++
		}
	}
	return count
}

// A Slot of 24 series of 40 KiB, about 1 MiB of input, is past its 768 KiB
// share as a whole: before slicing it was refused. Read in slices, each one
// within half the share, it completes: every series is read in exactly one
// slice that landed, evaluated once and its State written once; every slice
// that landed held less than the limit (its padding alone is a lower bound
// of its input); the completion row counts the slices landed and the ones
// split; and what needs every series - the completion-only Plan, the no-data
// census, Progress - happens once, after the last slice was read.
func TestASlotPastHalfItsShareIsReadInSlicesEachSeriesWrittenOnce(t *testing.T) {
	series := slicedHosts(24, 40<<10)
	fixture := newSlicedFixture(t, series, sliceableFacts(), 200)
	plans, requirements, request := withCompletionOnlyPlan(t)
	fixture.source.plans, fixture.source.requirements = plans, requirements

	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot completed in slices", result, err)
	}
	landed := fixture.source.landed()
	if len(landed) < 2 {
		t.Fatalf("landed %d reads, want the Slot read in more than one slice", len(landed))
	}
	reads := make([][]string, 0, len(landed))
	for _, call := range landed {
		reads = append(reads, call.delivered)
		if call.padding > slicedShare/2 {
			t.Fatalf("a landed slice held %d bytes of padding alone, past the slice limit %d", call.padding, slicedShare/2)
		}
		if call.slice.IsZero() {
			t.Fatal("a landed read was the whole Query Group, which does not fit")
		}
	}
	events := countDigests(fixture.ports.eventBatches...)
	writes := fixture.stateWrites()
	for _, one := range series {
		digest := one.digest()
		if read := countDigests(reads...)[digest]; read != 1 {
			t.Fatalf("series %s was read in %d landed slices, want exactly one", one.host, read)
		}
		if events[digest] != 1 || writes[digest] != 1 {
			t.Fatalf("series %s: %d events and %d State writes, want one of each", one.host, events[digest], writes[digest])
		}
	}
	if len(writes) != len(series) || len(fixture.state.saved) != len(series) {
		t.Fatalf("State written for %d keys (%d saved), want the %d series", len(writes), len(fixture.state.saved), len(series))
	}
	discarded := len(fixture.source.calls) - len(landed)
	usage := result.Usage
	if usage.InputSlices != uint64(len(landed)) || usage.InputSlicesDiscarded != uint64(discarded) {
		t.Fatalf("usage counts %d slices and %d discarded, want %d landed and %d split", usage.InputSlices,
			usage.InputSlicesDiscarded, len(landed), discarded)
	}
	for _, call := range fixture.source.calls {
		if !call.completed && !strings.Contains(fmt.Sprint(call.err), "passed its limit") {
			t.Fatalf("a read ended by %v, want only slices let go at their limit", call.err)
		}
	}
	if usage.RetainedBytes > slicedShare {
		t.Fatalf("the Slot held %d bytes at its peak, past its share %d", usage.RetainedBytes, slicedShare)
	}
	if censuses := fixture.noDataCensuses(); censuses != 1 {
		t.Fatalf("no-data census reported %d times, want once for the Slot", censuses)
	}
	completionOnly := 0
	for _, observation := range *fixture.observations {
		if observation.Stage == observability.StageEvaluationCompleted && observation.Trace.BusinessID == "3" {
			completionOnly++
		}
	}
	if completionOnly != 1 {
		t.Fatalf("the Plan without series was decided %d times, want once", completionOnly)
	}
	trace := *fixture.ports.trace
	lastQuery := nthStage(t, trace, "query", countStage(trace, "query"))
	if countStage(trace, "progress_commit") != 1 || nthStage(t, trace, "progress_commit", 1) < lastQuery {
		t.Fatalf("Progress committed %d times, want once after the last slice's read: %v", countStage(trace, "progress_commit"), trace)
	}
}

// A store that drops the slice's conditions answers the whole Query Group to
// every slice: no split can make a slice fit, and the Slot is refused by the
// value-distribution word within the K_max bound instead of looping.
func TestASourceThatIgnoresTheSliceEndsValueConcentratedWithinKMax(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	fixture.source.ignoreSlice = true
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	if err == nil || result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot refused", result, err)
	}
	if reason, ok := worker.InputSliceRefusal(err); !ok || reason != contract.ReasonInputSliceValueConcentrated {
		t.Fatalf("refusal = %q, %v (err %v), want %s", reason, ok, err, contract.ReasonInputSliceValueConcentrated)
	}
	if reads := len(fixture.source.calls); reads > 2*inputslice.MaxLeaves-1 {
		t.Fatalf("%d reads before the refusal, past the 2*K_max-1 bound", reads)
	}
	if len(fixture.source.landed()) != 0 || fixture.ports.eventCount != 0 || len(fixture.state.saved) != 0 {
		t.Fatal("a Slot refused for its values landed a slice")
	}
}

// A Slot with nothing to slice on - no query facts, or a PromQL query - is
// read whole and judged by its share alone, as before slicing: past half its
// share and within it, it completes in one read; past its share it is
// refused QG_BUDGET_SHARE_EXCEEDED, never split.
func TestAnUnsliceableSlotIsReadWholeAndJudgedByItsShare(t *testing.T) {
	promql := sliceableFacts()
	promql.PromQL = &execution.PromQLQuery{Expression: "avg by (host) (cpu_usage)"}
	for _, test := range []struct {
		name  string
		facts *execution.QueryPlanFacts
	}{{name: "no_facts"}, {name: "promql", facts: promql}} {
		t.Run(test.name+"/within_share", func(t *testing.T) {
			fixture := newSlicedFixture(t, slicedHosts(12, 40<<10), test.facts, 200)
			result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
			if err != nil || !result.Completed {
				t.Fatalf("Execute() result=%+v err=%v, want the Slot read whole and completed", result, err)
			}
			if calls := fixture.source.calls; len(calls) != 1 || !calls[0].slice.IsZero() || len(calls[0].delivered) != 12 {
				t.Fatalf("%d reads, want one whole read of the 12 series", len(calls))
			}
			if result.Usage.InputSlicesDiscarded != 0 || len(fixture.state.saved) != 12 {
				t.Fatalf("usage %+v, %d saved: want no split and every series written", result.Usage, len(fixture.state.saved))
			}
		})
		t.Run(test.name+"/past_share", func(t *testing.T) {
			fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), test.facts, 200)
			result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
			if err == nil || result.Completed {
				t.Fatalf("Execute() result=%+v err=%v, want the Slot refused by its share", result, err)
			}
			if _, sliced := worker.InputSliceRefusal(err); sliced || len(fixture.source.calls) != 1 {
				t.Fatalf("err %v after %d reads, want one read refused by the share, not a slicing refusal", err, len(fixture.source.calls))
			}
			assertCapacityRejectionReason(t, fixture.observations, observability.CapacityBudgetRetainedBytes, contract.ReasonQGBudgetShareExceeded)
		})
	}
}

// One series larger than the slice limit cannot be helped by a split: the
// Slot is refused by its share where the slice passes its limit, not split
// until K_max and named for its values. A sliceable Slot of small series
// beside it is not (the positive case above).
func TestASeriesLargerThanTheSliceLimitIsRefusedByTheShare(t *testing.T) {
	series := append(slicedHosts(4, 8<<10), slicedSeries{host: "host-huge", padding: 500 << 10})
	fixture := newSlicedFixture(t, series, sliceableFacts(), 200)
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	if err == nil || result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot refused by its share", result, err)
	}
	if reason, sliced := worker.InputSliceRefusal(err); sliced {
		t.Fatalf("refused as %s, want the share's word: a split does not mend one series", reason)
	}
	if len(fixture.source.calls) != 1 {
		t.Fatalf("%d reads, want the first read refused without a split", len(fixture.source.calls))
	}
	assertCapacityRejectionReason(t, fixture.observations, observability.CapacityBudgetRetainedBytes, contract.ReasonQGBudgetShareExceeded)
	if fixture.ports.eventCount != 0 || len(fixture.state.saved) != 0 {
		t.Fatal("a refused Slot wrote events or State")
	}
}

// withCompletionOnlyPlan is the fixture's due Plans with a second one, of
// business 3, that reads the same query and is handed no series: it is
// decided from its completion alone, which only the last slice can do.
func withCompletionOnlyPlan(t *testing.T) ([]execution.DuePlan, []execution.DataRequirement, execution.SlotExecutionRequest) {
	t.Helper()
	plans, requirements := baseDuePlanAndRequirements()
	second := plans[0]
	second.Identity.BusinessID = "3"
	plans = append(plans, second)
	requirements[0].Consumers = append(requirements[0].Consumers, execution.DataRequirementConsumer{
		Consumer:                  execution.ConsumerRef{Plan: second.Identity, LevelID: 5, HasLevel: true},
		ConsumerDeadlineUnixMilli: second.CompletionDeadlineUnixMilli, DownstreamExecutionReserveMilliSec: 5_000,
	})
	digest, err := execution.DeriveDuePlanSetDigest(plans, requirements)
	if err != nil {
		t.Fatal(err)
	}
	request := windowedRequest()
	request.Contract.DuePlanSetDigest = digest
	request.DuePlanTargets = execution.FrozenDuePlanTargets{DuePlanSetDigest: digest, Plans: []execution.PlanKey{plans[0].Key(), plans[1].Key()}}
	return plans, requirements, request
}

// markerStore is one Plan's gap marker kept by the rules the execution
// store applies them by (state.ExecutionStore.ApplyGap): a statement names
// the revision it expects and moves it one on; a second statement at the
// version of the marker is the same one again (ALREADY_APPLIED), or one that
// only extends it - more scopes, or more FULL Slots, at the same reason -
// through execution.ExtendSameSlotGap, the store's own function, and
// otherwise a CONFLICT. The marker a case reads is what those rules left.
type markerStore struct {
	t       *testing.T
	ports   *recordingPorts
	found   bool
	marker  storedMarker
	version execution.ApplyVersion
	digest  execution.MutationDigest
	sched   execution.PlanScheduleRevision
	shrunk  []string
	results []execution.GapGuardApplyStatus
}

// storedMarker is the marker's revision and scopes; revision zero when there
// is none.
type storedMarker struct {
	MarkerRevision uint64
	Scopes         []execution.GapScopeState
}

// gapped is whether the marker holds an open scope: a series written under
// it is guarded.
func (marker storedMarker) gapped() bool {
	for _, scope := range marker.Scopes {
		if scope.Status == execution.GapStatusGapped {
			return true
		}
	}
	return false
}

// covers is whether marker holds every gapped scope earlier did, with no
// fewer FULL Slots required: a marker that grew or stayed, never one that
// shrank.
func (marker storedMarker) covers(earlier storedMarker) bool {
	if marker.MarkerRevision < earlier.MarkerRevision {
		return false
	}
	for _, before := range earlier.Scopes {
		if before.Status != execution.GapStatusGapped {
			continue
		}
		held := false
		for _, scope := range marker.Scopes {
			held = held || scope.Scope == before.Scope && scope.Status == execution.GapStatusGapped &&
				scope.RequiredFullSlots >= before.RequiredFullSlots
		}
		if !held {
			return false
		}
	}
	return true
}

func (store *markerStore) LoadGaps(_ context.Context, request execution.GapLoadRequest) (execution.GapLoadResult, error) {
	store.ports.record("gap_load")
	result := execution.GapLoadResult{Items: make([]execution.GapGuardSnapshot, len(request.Items))}
	for index, item := range request.Items {
		snapshot := execution.GapGuardSnapshot{Identity: item.Identity, Status: execution.GapMissing}
		if store.found {
			snapshot = execution.GapGuardSnapshot{Identity: item.Identity, Status: execution.GapFound,
				MarkerRevision: store.marker.MarkerRevision, PersistedApplyVersion: store.version,
				PersistedMutationDigest: store.digest, LastScheduleRevision: store.sched,
				Scopes: append([]execution.GapScopeState(nil), store.marker.Scopes...)}
			if len(store.marker.Scopes) == 0 {
				snapshot.Status = execution.GapClearedTombstone
			}
		}
		result.Items[index] = snapshot
	}
	return result, nil
}

func (store *markerStore) LoadGapsInto(ctx context.Context, request execution.GapLoadRequest, accept func(execution.GapGuardSnapshot) error) error {
	result, err := store.LoadGaps(ctx, request)
	if err != nil {
		return err
	}
	for _, item := range result.Items {
		if err := accept(item); err != nil {
			return err
		}
	}
	return nil
}

func (store *markerStore) ApplyGap(_ context.Context, request execution.GapGuardApplyRequest) (execution.GapGuardApplyResult, error) {
	store.ports.record("gap_apply")
	store.ports.gapMutations = append(store.ports.gapMutations, request.Items...)
	result := execution.GapGuardApplyResult{Items: make([]execution.GapGuardApplyItemResult, len(request.Items))}
	for index, mutation := range request.Items {
		status := store.apply(mutation)
		store.results = append(store.results, status)
		result.Items[index] = execution.GapGuardApplyItemResult{Identity: mutation.Identity, Status: status}
	}
	return result, nil
}

func (store *markerStore) apply(mutation execution.PlanGapMutation) execution.GapGuardApplyStatus {
	before := store.marker
	var next []execution.GapScopeState
	if store.found && execution.CompareApplyVersion(store.version, mutation.ApplyVersion) == execution.ApplyVersionEqual {
		if store.digest == mutation.MutationDigest {
			return execution.GapGuardAlreadyApplied
		}
		extended, ok := execution.ExtendSameSlotGap(store.marker.Scopes, mutation.Scopes)
		if !ok || store.sched != mutation.ScheduleRevision {
			return execution.GapGuardConflict
		}
		next = extended
	} else {
		for _, scope := range mutation.Scopes {
			switch scope.Kind {
			case execution.GapOpen, execution.GapStrengthen:
				next = append(next, execution.GapScopeState{Scope: scope.Scope, Status: execution.GapStatusGapped,
					ReasonCode: scope.ReasonCode, RequiredFullSlots: scope.RequiredFullSlots})
			case execution.GapWarmup:
				next = append(next, execution.GapScopeState{Scope: scope.Scope, Status: execution.GapStatusWarming,
					ReasonCode: scope.ReasonCode, RequiredFullSlots: scope.RequiredFullSlots})
			}
		}
	}
	if store.marker.MarkerRevision != mutation.ExpectedMarkerRevision {
		return execution.GapGuardConflict
	}
	store.found = true
	store.marker = storedMarker{MarkerRevision: mutation.ExpectedMarkerRevision + 1, Scopes: next}
	store.version, store.digest, store.sched = mutation.ApplyVersion, mutation.MutationDigest, mutation.ScheduleRevision
	if !store.marker.covers(before) {
		store.shrunk = append(store.shrunk, fmt.Sprintf("%+v after %+v", store.marker, before))
	}
	return execution.GapGuardApplied
}

// coveredEvents is the event sink that, before it takes a batch, checks the
// invariant of 7.5: once a slice's PRIMARY came back PARTIAL, every event
// that goes out goes out under a gapped marker already in the store.
// failAfterPartialAt fails the n-th (from one) such batch, as a crash halfway
// through a slice's events.
type coveredEvents struct {
	*recordingPorts
	source             *slicedSource
	gaps               *markerStore
	uncovered          []string
	afterPartial       int
	failAfterPartialAt int
}

func (events *coveredEvents) WriteBatch(ctx context.Context, batch []contract.TriggerEventV1) error {
	if events.source.incomplete {
		if !events.gaps.marker.gapped() {
			events.uncovered = append(events.uncovered, fmt.Sprint(len(events.eventBatches)))
		}
		events.afterPartial++
		if events.afterPartial == events.failAfterPartialAt {
			events.record("event_ack")
			return &retryableOutputTestError{err: errors.New("injected crash halfway through a slice's events")}
		}
	}
	return events.recordingPorts.WriteBatch(ctx, batch)
}

// partialEvaluator is the fixture's evaluator answering a series whose
// PRIMARY came back PARTIAL as it answers a degraded Slot - a provable
// abnormal outcome, its event, a warming State and the Plan's open - for
// that series: the evaluator sees the series' own inputs, not the Slot.
type partialEvaluator struct {
	*recordingPorts
}

func (evaluator *partialEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	partial := false
	for _, input := range request.Inputs {
		for _, binding := range input.Inputs {
			partial = partial || binding.Role == execution.InputRolePrimary && binding.Completeness == execution.CompletenessPartial
		}
	}
	if !partial {
		return evaluator.recordingPorts.Evaluate(ctx, request)
	}
	evaluator.degraded = true
	defer func() { evaluator.degraded = false }()
	result, err := evaluator.recordingPorts.Evaluate(ctx, request)
	if err != nil {
		return result, err
	}
	mutation := partialStateMutationForTest()
	mutation.Identity.SeriesIdentityDigest = request.State.Items[0].Identity.SeriesIdentityDigest
	mutation.ExpectedBlobRevision = request.State.Items[0].BlobRevision
	mutation.MutationDigest = ""
	if mutation, err = execution.BuildStateMutation(mutation); err != nil {
		return result, err
	}
	result.Plans[0].StateResults[0].Mutation = mutation
	return result, nil
}

// B3: a slice whose PRIMARY came back PARTIAL after a first slice that was
// all FULL opens the Plan's marker in its own head, before anything of it
// and before every later slice's events: the first slice's events went out
// with no marker, which was right for them, and every event after the
// PARTIAL answer is written under the marker, which only grows. (A series
// whose PRIMARY is PARTIAL writes no event of its own: it is answered from
// its bindings, not evaluated.) A crash halfway through a later slice's
// events, written in windows of three, leaves the marker over the events it
// wrote; the retry keeps it as it was or larger, writes the rest under it,
// and completes with every series whose PRIMARY completed written once.
func TestALaterPartialSliceOpensTheMarkerBeforeItsEventsAndARetryKeepsIt(t *testing.T) {
	series := slicedHosts(24, 40<<10)
	fixture := newSlicedFixture(t, series, sliceableFacts(), 3)
	fixture.ports.gapMissing = true
	gaps := &markerStore{t: t, ports: fixture.ports}
	events := &coveredEvents{recordingPorts: fixture.ports, source: fixture.source, gaps: gaps, failAfterPartialAt: 2}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: gaps, Events: events,
		Evaluator: &partialEvaluator{recordingPorts: fixture.ports}}, 3)
	fixture.source.completeness = func(_, held int) execution.Completeness {
		if held == 2 {
			return execution.CompletenessPartial
		}
		return execution.CompletenessFull
	}
	request := windowedRequest()

	first, err := fixture.coordinator.Execute(context.Background(), request)
	if err == nil && first.Completed {
		t.Fatalf("Execute() result=%+v, want the crash halfway through a slice's events to stop it", first)
	}
	landed := fixture.source.landed()
	if len(landed) < 2 || len(fixture.source.partial) == 0 {
		t.Fatalf("landed %d slices, %d series PARTIAL: want the second slice read PARTIAL", len(landed), len(fixture.source.partial))
	}
	for _, digest := range landed[0].delivered {
		if fixture.source.partial[digest] {
			t.Fatal("the first slice was read PARTIAL")
		}
	}
	// The first slice's events went out before any marker: it was FULL.
	if marker := gaps.marker; !marker.gapped() {
		t.Fatalf("after the PARTIAL slice's head the store holds %+v, want its gapped marker", marker)
	}
	crashed := gaps.marker
	if events.afterPartial != 2 {
		t.Fatalf("%d event batches after the PARTIAL answer, want the crash in the second", events.afterPartial)
	}

	fixture.source.calls, events.failAfterPartialAt = nil, 0
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("retry result=%+v err=%v, want it completed", result, err)
	}
	if marker := gaps.marker; !marker.covers(crashed) {
		t.Fatalf("the retry left %+v, smaller than the %+v the crash left", marker, crashed)
	}
	if len(events.uncovered) != 0 || len(gaps.shrunk) != 0 {
		t.Fatalf("event batches %v went out with no marker over them; the marker shrank %v", events.uncovered, gaps.shrunk)
	}
	for _, status := range gaps.results {
		if status == execution.GapGuardConflict {
			t.Fatalf("a marker statement was refused as a conflict: %v", gaps.results)
		}
	}
	writes := fixture.stateWrites()
	for _, one := range series {
		if want := map[bool]int{false: 1, true: 0}[fixture.source.partial[one.digest()]]; writes[one.digest()] != want {
			t.Fatalf("series %s written %d times across the attempts, want %d", one.host, writes[one.digest()], want)
		}
	}
}

// slicedFirstSeen are the ports of a Plan new to its activation, forced to
// warm up and without a marker, over a State store that holds a record from
// an earlier Slot for the series in prior and what this Slot wrote.
type slicedFirstSeen struct {
	*persistedRetryState
	prior  map[execution.SeriesIdentityDigest]bool
	older  execution.ApplyVersion
	levels []execution.RuntimeLevelStateView
}

func (ports *slicedFirstSeen) LoadActivations(ctx context.Context, request execution.PlanActivationRequest) (execution.PlanActivationResult, error) {
	result, err := ports.recordingPorts.LoadActivations(ctx, request)
	for index := range result.Facts {
		result.Facts[index].Selected.ForceWarming, result.Facts[index].Selected.Arrived = true, true
	}
	return result, err
}

func (ports *slicedFirstSeen) LoadRuntime(ctx context.Context, request execution.StatePreflightRequest) (execution.StatePreflightResult, error) {
	result, err := ports.persistedRetryState.LoadRuntime(ctx, request)
	if err != nil {
		return result, err
	}
	for index, item := range request.Items {
		if _, written := ports.saved[item.Identity]; written || !ports.prior[item.Identity.SeriesIdentityDigest] {
			continue
		}
		result.Items[index] = execution.RuntimeStateView{Identity: item.Identity, Status: execution.StateFoundReady, BlobRevision: 1,
			PersistedApplyVersion: ports.older, PersistedMutationDigest: "earlier-slot", Levels: ports.levels}
	}
	return result, nil
}

// newSlicedFirstSeenFixture is the sliced fixture with the Plan new to its
// activation and the series in prior holding State from the Slot before.
func newSlicedFirstSeenFixture(t *testing.T, series []slicedSeries, prior []string) (*slicedFixture, *slicedFirstSeen) {
	t.Helper()
	fixture := newSlicedFixture(t, series, sliceableFacts(), 200)
	fixture.ports.gapMissing = true
	refs, err := execution.DeriveRuntimeLevelContractRefs(compiledPlanForTest(nil))
	if err != nil {
		t.Fatal(err)
	}
	older := frozenApplyVersion()
	older.EvaluationTime -= 60
	ports := &slicedFirstSeen{persistedRetryState: fixture.state, prior: map[execution.SeriesIdentityDigest]bool{}, older: older}
	for _, ref := range refs {
		ports.levels = append(ports.levels, execution.RuntimeLevelStateView{LevelID: ref.LevelID,
			LevelStateCompatibility: ref.LevelStateCompatibility, WarmupRequirementRef: ref.WarmupRequirementRef,
			HistoryCompleteness: execution.HistoryFull, LastProcessedEventTime: int64(older.EvaluationTime)})
	}
	for _, digest := range prior {
		ports.prior[execution.SeriesIdentityDigest(digest)] = true
	}
	gaps := &markerStore{t: t, ports: fixture.ports}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: ports, State: ports, GapGuard: gaps, Events: fixture.ports}, 200)
	return fixture, ports
}

// satisfiedMarkers is how many first-seen markers - a clear of the Plan-wide
// scope - the Slot sent.
func satisfiedMarkers(mutations []execution.PlanGapMutation) int {
	count := 0
	for _, mutation := range mutations {
		if len(mutation.Scopes) == 1 && mutation.Scopes[0].Scope == (execution.GapScope{}) && mutation.Scopes[0].Kind == execution.GapClear {
			count++
		}
	}
	return count
}

// First-seen is judged slice by slice and only ever moves toward seen
// (7.5): a Plan new to its activation, without a marker, finds nothing from
// before in its first two slices and writes them as first-seen, without a
// warm-up; its third slice finds one series that ran before, and from there
// the Plan warms up - nothing more of it is written, and the tail writes no
// satisfied marker - while the completion row names the two slices and
// their series that went out as first-seen. Which series a slice holds is
// learned from a first run of the same input with nothing from before.
func TestFirstSeenIsJudgedPerSliceAndAPlanFoundSeenWarmsUpFromThatSlice(t *testing.T) {
	series := slicedHosts(24, 40<<10)
	learn, _ := newSlicedFirstSeenFixture(t, series, nil)
	if result, err := learn.coordinator.Execute(context.Background(), windowedRequest()); err != nil || !result.Completed {
		t.Fatalf("learning run result=%+v err=%v", result, err)
	}
	// The slices that hold series, in the order they are read: a slice may
	// hold none, and writes nothing as first-seen or otherwise.
	var slices []slicedCall
	seenAt := 0
	for index, slice := range learn.source.landed() {
		if len(slice.delivered) != 0 {
			slices = append(slices, slice)
			if len(slices) == 3 {
				seenAt = index
			}
		}
	}
	if len(slices) < 3 {
		t.Fatalf("the input lands in %d slices that hold series, want at least three", len(slices))
	}
	if satisfiedMarkers(learn.ports.gapMutations) != 1 || learn.ports.eventCount != len(series) {
		t.Fatalf("a Plan never seen sent %d satisfied markers and %d events, want one and every series'", satisfiedMarkers(learn.ports.gapMutations), learn.ports.eventCount)
	}

	// The Plan found to have run is warmed up as any forced Plan is: it
	// writes nothing more this attempt, its WARMING marker is written by the
	// protection path, and the attempt is retried to converge.
	fixture, _ := newSlicedFirstSeenFixture(t, series, []string{slices[2].delivered[0]})
	request := windowedRequest()
	first, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || first.Completed || first.ReasonCode != execution.ReasonCode(contract.ReasonPlanReactivated) {
		t.Fatalf("Execute() result=%+v err=%v, want the Plan warmed up under %s", first, err, contract.ReasonPlanReactivated)
	}
	written := fixture.stateWrites()
	for index, slice := range fixture.source.landed() {
		for _, digest := range slice.delivered {
			if index < seenAt && written[digest] != 1 || index >= seenAt && written[digest] != 0 {
				t.Fatalf("slice %d series written %d times, want the slices before the one found seen written once and none after", index+1, written[digest])
			}
		}
	}
	if markers := satisfiedMarkers(fixture.ports.gapMutations); markers != 0 {
		t.Fatalf("a Plan found to have run sent %d satisfied markers", markers)
	}

	fixture.source.calls = nil
	mutations := len(fixture.ports.gapMutations)
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("retry result=%+v err=%v", result, err)
	}
	if markers := satisfiedMarkers(fixture.ports.gapMutations[mutations:]); markers != 0 {
		t.Fatal("the retry judged the Plan unseen again and sent its satisfied marker")
	}
	for index, slice := range fixture.source.landed() {
		for _, digest := range slice.delivered {
			if index >= seenAt && fixture.stateWrites()[digest] != 0 {
				t.Fatal("the retry wrote a series of the Plan it had found seen")
			}
		}
	}
	unseenSeries := uint64(len(slices[0].delivered) + len(slices[1].delivered))
	if first.Usage.FirstSeenUnseenSlices != 2 || first.Usage.FirstSeenUnseenSeries != unseenSeries {
		t.Fatalf("completion row counts %d slices and %d series written as first-seen, want 2 and %d",
			first.Usage.FirstSeenUnseenSlices, first.Usage.FirstSeenUnseenSeries, unseenSeries)
	}
}

// K_0 (7.4): a replica that read a Query Group's last Slot in slices starts
// its next one already sliced, from that reading, rather than reading the
// whole group first and letting it go: the next Slot's first read is a
// slice, none is split, and there are at least as many as its input over
// the slice limit. The first Slot, with no reading, began with the whole
// group, which did not fit. The starting slices divide a fixed alphabet,
// so the series here end in characters spread over it, as the alphabet
// assumes; values that all end in digits fall in one of its classes, and
// the adaptive split then recovers them at the cost of reads let go.
func TestTheNextSlotOfAQueryGroupStartsWithTheSlicesItsLastReadNeeded(t *testing.T) {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	series := make([]slicedSeries, 24)
	for index := range series {
		series[index] = slicedSeries{host: "node-" + string(alphabet[index*len(alphabet)/len(series)]), padding: 40 << 10}
	}
	fixture := newSlicedFixture(t, series, sliceableFacts(), 200)
	request := windowedRequest()
	first, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !first.Completed {
		t.Fatalf("first Slot result=%+v err=%v", first, err)
	}
	if calls := fixture.source.calls; !calls[0].slice.IsZero() || calls[0].completed || first.Usage.InputSlicesDiscarded == 0 {
		t.Fatalf("first Slot began with %+v, want the whole group read and let go", calls[0].slice)
	}

	// The next read of the Query Group on this replica. What K_0 is counted
	// from is the group's last reading here, not the Slot, and the fixture's
	// evaluator writes at its one Slot: the next read is that Slot's request
	// again, over a State store that starts from nothing, which is not this
	// case's subject.
	fixture.source.calls = nil
	fixture.state.saved = map[execution.StateKeyIdentity]execution.StateMutation{}
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("next Slot result=%+v err=%v", result, err)
	}
	calls := fixture.source.calls
	padding := 0
	for _, one := range series {
		padding += one.padding
	}
	least := (padding + slicedShare/2 - 1) / (slicedShare / 2)
	if calls[0].slice.IsZero() || result.Usage.InputSlicesDiscarded != 0 || len(calls) != len(fixture.source.landed()) {
		t.Fatalf("next Slot read %d times, %d discarded, first read whole=%v: want it sliced from its first read",
			len(calls), result.Usage.InputSlicesDiscarded, calls[0].slice.IsZero())
	}
	if len(calls) < least || result.Usage.InputSlices != uint64(len(calls)) {
		t.Fatalf("next Slot read %d slices (row says %d), want at least %d for its %d bytes of padding",
			len(calls), result.Usage.InputSlices, least, padding)
	}
	reads := make([][]string, 0, len(calls))
	for _, call := range calls {
		reads = append(reads, call.delivered)
	}
	for _, one := range series {
		if countDigests(reads...)[one.digest()] != 1 {
			t.Fatalf("series %s read %d times in the next Slot", one.host, countDigests(reads...)[one.digest()])
		}
	}
}

// A later slice that answers the Plan's scope worse than the slice that
// opened its marker - UNAVAILABLE after PARTIAL - is already guarded by it:
// its head writes nothing more, since the store keeps a scope's reason
// within its Slot and refuses a second statement that changes it as a
// CONFLICT, which would fail the attempt for a marker that already covers
// every event. The Slot completes in one attempt with the marker as opened,
// every event after the PARTIAL answer under it.
func TestALaterSliceAnsweringWorseKeepsTheMarkerItFindsWithoutAConflict(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 3)
	fixture.ports.gapMissing = true
	gaps := &markerStore{t: t, ports: fixture.ports}
	events := &coveredEvents{recordingPorts: fixture.ports, source: fixture.source, gaps: gaps}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: gaps, Events: events,
		Evaluator: &partialEvaluator{recordingPorts: fixture.ports}}, 3)
	partialRead := 0
	fixture.source.completeness = func(read, held int) execution.Completeness {
		switch {
		case held == 2:
			partialRead = read
			return execution.CompletenessPartial
		case held == -1 && partialRead != 0 && read == partialRead+2:
			return execution.CompletenessUnavailable
		}
		return execution.CompletenessFull
	}
	result, err := fixture.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v, want the Slot completed in one attempt", result, err)
	}
	if partialRead == 0 || result.CompletionKind != execution.CompletionUnavailable {
		t.Fatalf("PARTIAL at read %d, completion %s: want a PARTIAL slice and an UNAVAILABLE one", partialRead, result.CompletionKind)
	}
	for _, status := range gaps.results {
		if status == execution.GapGuardConflict {
			t.Fatalf("a marker statement was refused as a conflict: %v", gaps.results)
		}
	}
	if !gaps.marker.gapped() || len(events.uncovered) != 0 || len(gaps.shrunk) != 0 {
		t.Fatalf("marker %+v; event batches %v went out with no marker over them; the marker shrank %v", gaps.marker, events.uncovered, gaps.shrunk)
	}
}

// A slice that held none of the Plan's series can still come back PARTIAL:
// the series it left out are in no other slice. Its answer guards the Plan
// as a PARTIAL answer of the whole read would - the Plan's marker is opened
// before every later event, and the Slot completes with a partial gap, not
// FULL - rather than being dropped because no series carried it.
func TestAnEmptySliceAnsweredPartialStillGuardsThePlan(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(24, 40<<10), sliceableFacts(), 200)
	fixture.ports.gapMissing = true
	gaps := &markerStore{t: t, ports: fixture.ports}
	events := &coveredEvents{recordingPorts: fixture.ports, source: fixture.source, gaps: gaps}
	fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: gaps, Events: events}, 200)
	partialRead := 0
	fixture.source.completeness = func(read, held int) execution.Completeness {
		if held == 0 && read > 1 && (partialRead == 0 || partialRead == read) {
			partialRead = read
			return execution.CompletenessPartial
		}
		return execution.CompletenessFull
	}
	result, err := fixture.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v err=%v", result, err)
	}
	if partialRead == 0 {
		t.Fatal("no slice after the first held no series: the case did not run")
	}
	if !gaps.marker.gapped() || result.CompletionKind == execution.CompletionFull {
		t.Fatalf("marker %+v, completion %s: want the Plan guarded by the empty PARTIAL slice", gaps.marker, result.CompletionKind)
	}
	if events.afterPartial == 0 || len(events.uncovered) != 0 {
		t.Fatalf("%d event batches after the PARTIAL answer, %v of them with no marker over them: want every one under it",
			events.afterPartial, events.uncovered)
	}
	if primary := fixture.ports.lastProgress.Completion.Primary; primary == nil || primary.Completeness != execution.CompletenessPartial {
		t.Fatalf("the completion's PRIMARY fact is %+v, want the round PARTIAL", primary)
	}
}
