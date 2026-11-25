// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/detect"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/evaluation"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/trigger"
)

// The no-data rounds' synthetic series read a batch ahead (014 next9 ⑦), on
// the store production wires for State and no-data memory alike, over a
// redis-server of the test's own, and the real evaluator: two Plans
// detecting no-data, each remembering groups none of which reports this
// round, so each group is said as a synthetic series - more of them than a
// batch, and a batch holding the last of one Plan's and the first of the
// next's.

// readAheadGroups is how many groups each Plan remembers.
const readAheadGroups = 10

// readAheadKey is what a series' State read and its evaluation both name.
type readAheadKey struct {
	plan   execution.PlanIdentity
	series execution.SeriesIdentityDigest
}

// readAheadState is the store, its State reads told as they begin. A batch
// is known by its series: the series in the order the rounds say them, as
// one read of them all asked for them (readInOne), cut into batches of the
// size given - a batch's read can begin after the next one's, each on a
// goroutine of its own, so the order the reads begin in does not number
// them. It keeps what each read asked for, closes a channel as each batch's
// read begins, and counts the reads in flight. hold names a batch whose read,
// once begun, waits for release - or for its context to end, and slow more
// after it, as a round trip already sent does - before it reads. With
// overlap, a batch's read, once begun, waits - up to a bound - for the batch
// before it to be evaluated (evaluating, closed by the evaluator), and says
// whether it was (overlapped): a read made in turn, before that evaluation,
// never sees it.
type readAheadState struct {
	*state.ExecutionStore
	mu         sync.Mutex
	batchOf    map[readAheadKey]int
	asked      [][]readAheadKey
	began      []chan struct{}
	begun      map[int]bool
	evaluating []chan struct{}
	evaluated  map[int]bool
	overlap    bool
	overlapped map[int]bool
	inFlight   atomic.Int64
	hold       int
	release    chan struct{}
	slow       time.Duration
}

// newReadAheadState knows the batches of order cut every batchLimit series.
func newReadAheadState(store *state.ExecutionStore, order []readAheadKey, batchLimit int) *readAheadState {
	batches := (len(order) + batchLimit - 1) / batchLimit
	read := &readAheadState{ExecutionStore: store, batchOf: map[readAheadKey]int{}, began: make([]chan struct{}, batches),
		begun: map[int]bool{}, evaluating: make([]chan struct{}, batches), evaluated: map[int]bool{},
		overlapped: map[int]bool{}, hold: -1, release: make(chan struct{})}
	for index, key := range order {
		read.batchOf[key] = index / batchLimit
	}
	for index := range read.began {
		read.began[index], read.evaluating[index] = make(chan struct{}), make(chan struct{})
	}
	return read
}

// evaluatingBatch notes that the batch's first series is being evaluated.
func (store *readAheadState) evaluatingBatch(batch int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if batch < 0 || batch >= len(store.evaluating) || store.evaluated[batch] {
		return
	}
	store.evaluated[batch] = true
	close(store.evaluating[batch])
}

// overlappedBatch is whether the batch's read was in flight while the batch
// before it was evaluated.
func (store *readAheadState) overlappedBatch(batch int) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.overlapped[batch]
}

func (store *readAheadState) LoadRuntime(ctx context.Context, request execution.StatePreflightRequest) (execution.StatePreflightResult, error) {
	store.inFlight.Add(1)
	defer store.inFlight.Add(-1)
	keys := make([]readAheadKey, len(request.Items))
	for index, item := range request.Items {
		keys[index] = readAheadKey{plan: item.Identity.Plan, series: item.Identity.SeriesIdentityDigest}
	}
	store.mu.Lock()
	store.asked = append(store.asked, keys)
	batch, known := -1, false
	if len(keys) > 0 {
		batch, known = store.batchOf[keys[0]]
	}
	first := known && !store.begun[batch]
	if first {
		store.begun[batch] = true
	}
	store.mu.Unlock()
	if first {
		close(store.began[batch])
	}
	if first && store.overlap && batch >= 1 {
		select {
		case <-store.evaluating[batch-1]:
			store.mu.Lock()
			store.overlapped[batch] = true
			store.mu.Unlock()
		case <-time.After(2 * time.Second): // fixed-wait: the bound of a wait for an evaluation a read made in turn comes before
		}
	}
	if known && batch == store.hold {
		select {
		case <-store.release:
		case <-ctx.Done():
			time.Sleep(store.slow) // fixed-wait: a read whose round trip was sent comes back after its context ended
		}
	}
	return store.ExecutionStore.LoadRuntime(ctx, request)
}

// batchFor is the batch the series is read in, -1 for one of no batch.
func (store *readAheadState) batchFor(key readAheadKey) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	batch, known := store.batchOf[key]
	if !known {
		return -1
	}
	return batch
}

// reads is what every read asked for, in the order the reads began.
func (store *readAheadState) reads() [][]readAheadKey {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([][]readAheadKey(nil), store.asked...)
}

// readAheadEvaluator is the real evaluator, told the batch each series it
// evaluates was read in. For every batch before the last of batches it says
// whether the next batch's read had begun when one of its series was first
// evaluated, waiting for that read up to a bound. fail, when set, may answer
// a batch's series with an error instead.
type readAheadEvaluator struct {
	execution.Evaluator
	state   *readAheadState
	batches int
	mu      sync.Mutex
	ahead   map[int]bool
	fail    func(ctx context.Context, batch int) error
}

func (evaluator *readAheadEvaluator) Evaluate(ctx context.Context, request execution.EvaluationRequest) (execution.EvaluationResult, error) {
	key := readAheadKey{plan: request.Inputs[0].Consumer.Plan, series: request.Inputs[0].SeriesIdentity}
	batch := evaluator.state.batchFor(key)
	evaluator.state.evaluatingBatch(batch)
	if batch >= 0 && batch+1 < evaluator.batches {
		ahead := true
		select {
		case <-evaluator.state.began[batch+1]:
		case <-time.After(2 * time.Second): // fixed-wait: the bound of a wait for a read that, read in turn, begins only after this evaluation
			ahead = false
		}
		evaluator.mu.Lock()
		if _, seen := evaluator.ahead[batch]; !seen {
			evaluator.ahead[batch] = ahead
		}
		evaluator.mu.Unlock()
	}
	if evaluator.fail != nil {
		if err := evaluator.fail(ctx, batch); err != nil {
			return execution.EvaluationResult{}, err
		}
	}
	return evaluator.Evaluator.Evaluate(ctx, request)
}

// readAheadRig is two Plans' no-data rounds, one Slot after another, over a
// store of their own.
type readAheadRig struct {
	t         *testing.T
	store     *state.ExecutionStore
	dues      []execution.DuePlan
	evaluator execution.Evaluator
	contract  execution.FrozenExecutionContractRef
	// segment is how many names the rounds read back at a time; zero, the
	// platform's.
	segment int
}

func readAheadPlan(t *testing.T, strategyID string) execution.DuePlan {
	t.Helper()
	return execution.DuePlan{
		Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: strategyID},
		CompiledPlan: noDataPreflightPlan(t, strategyID, &contract.NoDataConfigV1{
			Continuous: 1, Level: 2, AggDimension: []string{"bk_target_ip", "bk_target_cloud_id"},
		}),
		StateGeneration: "state-v1", StateApplyEpoch: 1, ScheduleRevision: "plan-schedule-v1",
	}
}

// readAheadGroupKey is the key of the Plan's group'th group.
func readAheadGroupKey(plan, group int) string {
	return fmt.Sprintf("bk_target_cloud_id=0,bk_target_ip=192.0.2.%d,%s=true", plan*readAheadGroups+group+1,
		contract.NoDataDimensionTag)
}

// newReadAheadRig is the rig with each Plan's groups remembered as the round
// before wrote them - seen three periods ago, absent since two - and extra
// beside the first Plan's.
func newReadAheadRig(t *testing.T, extra ...string) *readAheadRig {
	t.Helper()
	ctx := context.Background()
	store, _ := redisNoDataStore(t)
	dues := []execution.DuePlan{readAheadPlan(t, "7"), readAheadPlan(t, "8")}
	contractRef := noDataPreflightContract(t, dues)
	at := int64(contractRef.Slot.EvaluationTime)
	for plan, due := range dues {
		var groups []execution.NoDataGroupMemory
		keys := make([]string, 0, readAheadGroups+len(extra))
		for group := 0; group < readAheadGroups; group++ {
			keys = append(keys, readAheadGroupKey(plan, group))
		}
		if plan == 0 {
			keys = append(keys, extra...)
		}
		for _, key := range keys {
			groups = append(groups, execution.NoDataGroupMemory{GroupKey: key, LastSeen: at - 180, FirstAbsent: at - 120})
		}
		mutation, err := execution.BuildPlanNoDataMutation(execution.PlanNoDataMemoryUpdate{
			DerivedFrom: execution.NoDataRepresentationNone, Identity: due.NoDataIdentity(),
			ApplyVersion:     execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: execution.EvaluationTime(at - 60), SlotDigest: "previous"},
			ScheduleRevision: due.ScheduleRevision, RosterVersion: "HISTORY/1", PresentAsOf: at - 120, Memory: groups,
		})
		if err != nil {
			t.Fatal(err)
		}
		retention, err := generationRetentionOf(due)
		if err != nil {
			t.Fatal(err)
		}
		written, err := store.ApplyNoData(ctx, execution.NoDataApplyRequest{Contract: contractRef, Retention: retention,
			Items: []execution.PlanNoDataMutation{mutation}})
		if err != nil || written.Items[0].Status != execution.NoDataApplied {
			t.Fatalf("the round before's write of Plan %s = (%+v, %v)", due.Identity.StrategyID, written, err)
		}
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
	return &readAheadRig{t: t, store: store, dues: dues, evaluator: evaluator, contract: contractRef}
}

// round runs the Slot's no-data rounds in batches of batchLimit, its State
// read through stateStore and its series evaluated by evaluator.
func (rig *readAheadRig) round(ctx context.Context, batchLimit int, stateStore execution.StateStore,
	evaluator execution.Evaluator) (*streamedExecution, error) {
	rig.t.Helper()
	stream := &streamedExecution{
		coordinator: &SlotExecutionCoordinator{
			ports: Ports{NoData: rig.store, Hosts: SharedHostBusiness, State: stateStore, Evaluator: evaluator,
				Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {})},
			budget: ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxGapMutations: 10,
				MaxStateMutations: 100, MaxEvents: 100},
			noDataSegment: rig.segment,
		},
		header: execution.InternalExecutionHeader{ExecutionID: "no-data-read-ahead", Contract: rig.contract, DuePlans: rig.dues},
	}
	stream.request = execution.SlotExecutionRequest{Contract: rig.contract, Operation: execution.OperationNormal}
	stream.effective = mustPrepareAlwaysEffectiveTimeFacts(rig.t, stream.header)
	for _, due := range rig.dues {
		stream.gaps.Items = append(stream.gaps.Items, execution.GapGuardSnapshot{Identity: due.GapIdentity(), Status: execution.GapMissing})
		stream.bindings = append(stream.bindings, execution.NamedInputBinding{Consumer: execution.ConsumerRef{Plan: due.Identity},
			Completeness: execution.CompletenessFull})
	}
	if err := stream.loadNoDataMemory(ctx); err != nil {
		rig.t.Fatal(err)
	}
	return stream, stream.evaluateNoData(ctx, nil, batchLimit)
}

// commit writes what the round evaluated and remembered, as the Slot's
// apply does, and moves the rig to the next Slot.
func (rig *readAheadRig) commit(ctx context.Context, stream *streamedExecution) {
	rig.t.Helper()
	byPlan := map[execution.PlanIdentity][]execution.StateMutation{}
	for _, plan := range stream.evaluated.Plans {
		for _, result := range plan.StateResults {
			byPlan[plan.Plan] = append(byPlan[plan.Plan], result.Mutation)
		}
	}
	for _, due := range rig.dues {
		if len(byPlan[due.Identity]) == 0 {
			continue
		}
		view, err := execution.PlanViewFor(due, execution.SeriesKindNoData)
		if err != nil {
			rig.t.Fatal(err)
		}
		retention, err := execution.DeriveStateRetentionRequirement(view.CompiledPlan)
		if err != nil {
			rig.t.Fatal(err)
		}
		if _, err := rig.store.ApplyRuntime(ctx, execution.StateApplyRequest{Contract: rig.contract,
			Items: byPlan[due.Identity], Retention: retention}); err != nil {
			rig.t.Fatal(err)
		}
	}
	if err := stream.coordinator.applyNoDataMemory(ctx, stream.request, stream.header.DuePlans, stream.noDataMutations); err != nil {
		rig.t.Fatal(err)
	}
	stream.releaseProvisional()
	rig.contract.Slot.EvaluationTime += 60
}

// readAheadOutput is what a round evaluated - every Plan's results, State
// and events, in the order they were merged - and what it remembered, as
// bytes.
func readAheadOutput(t *testing.T, stream *streamedExecution) ([]byte, []byte) {
	t.Helper()
	evaluated, err := json.Marshal(stream.evaluated.Plans)
	if err != nil {
		t.Fatal(err)
	}
	remembered, err := json.Marshal(stream.noDataMutations)
	if err != nil {
		t.Fatal(err)
	}
	return evaluated, remembered
}

// readInOne runs the rig's round with every series in one batch - its State
// read whole before any series is evaluated - and returns its output and the
// series it read, in the order the rounds said them.
func readInOne(ctx context.Context, rig *readAheadRig) ([]byte, []byte, []readAheadKey, *streamedExecution) {
	rig.t.Helper()
	whole := newReadAheadState(rig.store, nil, 1)
	stream, err := rig.round(ctx, 1<<10, whole, rig.evaluator)
	if err != nil {
		rig.t.Fatal(err)
	}
	reads := whole.reads()
	if len(reads) != 1 {
		rig.t.Fatalf("fixture: the round read in one batch made %d reads", len(reads))
	}
	evaluated, remembered := readAheadOutput(rig.t, stream)
	return evaluated, remembered, reads[0], stream
}

// readAheadReads checks that each read asked for one batch of order, whole,
// and every batch was read: the batches are order cut every batchLimit
// series. It says whether a batch held series of more than one Plan.
func readAheadReads(t *testing.T, state *readAheadState, order []readAheadKey, batchLimit int) bool {
	t.Helper()
	reads := state.reads()
	if batches := (len(order) + batchLimit - 1) / batchLimit; len(reads) != batches {
		t.Fatalf("%d reads, want %d batches of %d series", len(reads), batches, len(order))
	}
	both := false
	for _, keys := range reads {
		batch := state.batchFor(keys[0])
		want := order[batch*batchLimit : min((batch+1)*batchLimit, len(order))]
		if fmt.Sprint(keys) != fmt.Sprint(want) {
			t.Fatalf("a read asked for %v, want batch %d, %v", keys, batch, want)
		}
		plans := map[execution.PlanIdentity]bool{}
		for _, key := range keys {
			plans[key.plan] = true
		}
		both = both || len(plans) > 1
	}
	return both
}

// Two Plans' synthetic series, across several batches and a batch holding
// both Plans', are read a batch ahead: each batch's State read begins
// before the batch before it is evaluated, and is in flight while it is. What the rounds evaluate - State
// and events - and remember is, byte for byte, what the same rounds read in
// one batch, read whole before any series is evaluated, give; the second
// round reads the State the first wrote (014 next9 ⑦ case 1).
func TestNoDataSeriesAreReadABatchAheadAndEvaluateAsReadInOne(t *testing.T) {
	const batchLimit = 4
	ctx := context.Background()
	reference, readAhead := newReadAheadRig(t), newReadAheadRig(t)
	for round := 1; round <= 2; round++ {
		wantEvaluated, wantRemembered, order, stream := readInOne(ctx, reference)
		reference.commit(ctx, stream)
		if len(order) < 2*readAheadGroups {
			t.Fatalf("fixture: round %d said %d series, want every remembered group of both Plans", round, len(order))
		}
		batches := (len(order) + batchLimit - 1) / batchLimit
		state := newReadAheadState(readAhead.store, order, batchLimit)
		state.overlap = true
		evaluator := &readAheadEvaluator{Evaluator: readAhead.evaluator, state: state, batches: batches, ahead: map[int]bool{}}
		stream, err := readAhead.round(ctx, batchLimit, state, evaluator)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		for batch := 1; batch < batches; batch++ {
			if !state.overlappedBatch(batch) {
				t.Fatalf("round %d: batch %d's State read was not in flight while batch %d was evaluated: read in turn, not ahead",
					round, batch+1, batch)
			}
		}
		if !readAheadReads(t, state, order, batchLimit) {
			t.Fatalf("fixture: round %d read no batch of both Plans' series", round)
		}
		for batch := 0; batch+1 < batches; batch++ {
			if ahead, evaluated := evaluator.ahead[batch]; !evaluated || !ahead {
				t.Fatalf("round %d: batch %d of %d was evaluated before the next batch's State read began (evaluated %t)",
					round, batch+1, batches, evaluated)
			}
		}
		gotEvaluated, gotRemembered := readAheadOutput(t, stream)
		if !bytes.Equal(gotEvaluated, wantEvaluated) {
			t.Fatalf("round %d evaluated, read a batch ahead:\n%s\nwant, read in one batch:\n%s", round, gotEvaluated, wantEvaluated)
		}
		if !bytes.Equal(gotRemembered, wantRemembered) {
			t.Fatalf("round %d remembered, read a batch ahead:\n%s\nwant:\n%s", round, gotRemembered, wantRemembered)
		}
		readAhead.commit(ctx, stream)
	}
}

// A batch whose evaluation fails ends the rounds with the failure, and only
// once the read of the batch after it, begun ahead, has come back: no read
// outlives the rounds. The same when the Slot's context ends while that read
// is in flight (014 next9 ⑦ case 2).
func TestANoDataBatchThatFailsReturnsOnceTheReadAheadOfItIsBack(t *testing.T) {
	const batchLimit, failing = 4, 1
	refused := errors.New("alarmd test: the evaluation refused the series")
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint("cancelled=", cancelled), func(t *testing.T) {
			rig := newReadAheadRig(t)
			_, _, order, _ := readInOne(context.Background(), newReadAheadRig(t))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := newReadAheadState(rig.store, order, batchLimit)
			state.hold, state.slow = failing+1, 200*time.Millisecond
			var once sync.Once
			evaluator := &readAheadEvaluator{Evaluator: rig.evaluator, state: state, ahead: map[int]bool{}}
			evaluator.fail = func(ctx context.Context, batch int) error {
				if batch != failing {
					return nil
				}
				select {
				case <-state.began[failing+1]:
				case <-time.After(2 * time.Second): // fixed-wait: the bound of a wait for a read that, read in turn, never begins
				}
				once.Do(func() {
					if cancelled {
						cancel()
						return
					}
					// Let go while the rounds, failed, wait for it.
					time.AfterFunc(200*time.Millisecond, func() { close(state.release) }) // fixed-wait: long enough for rounds that do not wait for the read to return before it is let go
				})
				if cancelled {
					return ctx.Err()
				}
				return refused
			}
			_, err := rig.round(ctx, batchLimit, state, evaluator)
			if err == nil || !errors.Is(err, refused) && !errors.Is(err, context.Canceled) {
				t.Fatalf("the rounds returned %v, want the batch's failure", err)
			}
			if left := state.inFlight.Load(); left != 0 {
				t.Fatalf("the rounds returned with %d State reads in flight", left)
			}
			select {
			case <-state.began[failing+1]:
			default:
				t.Fatalf("the batch after the failing one was not read ahead of it")
			}
		})
	}
}

// A Plan whose round fails on its own (noDataLocalOutcome) after it said
// some of its series - one name it remembers the no-data package could not
// have written, read back two names at a time in the order of their digests
// - has those series evaluated as they always were, read a batch ahead
// beside the next Plan's: the same results as the round read in one batch,
// the Plan's outcome SKIPPED_DERIVATION_FAILED and nothing of its memory
// written (014 next9 ⑦ case 3).
func TestNoDataSeriesSaidBeforeAPlansOwnFailureAreEvaluatedAsBefore(t *testing.T) {
	const batchLimit, window = 4, 2
	ctx := context.Background()
	digest := func(key string) string {
		sum := sha256.Sum256([]byte(key))
		return string(sum[:])
	}
	var good []string
	for group := 0; group < readAheadGroups; group++ {
		good = append(good, digest(readAheadGroupKey(0, group)))
	}
	sort.Strings(good)
	// A key with no no-data tag, placed where a window before it says a
	// whole batch and one after it is left unsaid.
	bad, said := "", 0
	for candidate := 1; bad == "" && candidate < 256; candidate++ {
		key := fmt.Sprintf("bk_target_cloud_id=0,bk_target_ip=198.51.100.%d", candidate)
		rank := sort.SearchStrings(good, digest(key))
		if before := rank / window * window; before >= batchLimit && before+window <= readAheadGroups {
			bad, said = key, before
		}
	}
	if bad == "" {
		t.Fatal("fixture: no key falls where its window is after a batch's names")
	}
	reference, readAhead := newReadAheadRig(t, bad), newReadAheadRig(t, bad)
	reference.segment, readAhead.segment = window, window
	wantEvaluated, wantRemembered, order, _ := readInOne(ctx, reference)
	batches := (len(order) + batchLimit - 1) / batchLimit
	state := newReadAheadState(readAhead.store, order, batchLimit)
	evaluator := &readAheadEvaluator{Evaluator: readAhead.evaluator, state: state, batches: batches, ahead: map[int]bool{}}
	stream, err := readAhead.round(ctx, batchLimit, state, evaluator)
	if err != nil {
		t.Fatalf("a Plan's own failure failed the rounds: %v", err)
	}
	failed, other := readAhead.dues[0], readAhead.dues[1]
	evaluatedOf := map[execution.PlanIdentity]int{}
	for _, plan := range stream.evaluated.Plans {
		evaluatedOf[plan.Plan] += len(plan.StateResults)
	}
	if evaluatedOf[failed.Identity] != said || evaluatedOf[other.Identity] < readAheadGroups {
		t.Fatalf("evaluated %d series of the failed Plan and %d of the other, want the %d it said before it failed and every one",
			evaluatedOf[failed.Identity], evaluatedOf[other.Identity], said)
	}
	if outcome := stream.noDataOutcomes[stream.noDataOutcomeIndex[failed.Key()]]; outcome != nodata.OutcomeSkippedDerivationFailed {
		t.Fatalf("the failed Plan's outcome is %s, want %s", outcome, nodata.OutcomeSkippedDerivationFailed)
	}
	for _, mutation := range stream.noDataMutations {
		if mutation.Identity == failed.NoDataIdentity() {
			t.Fatalf("the failed Plan's round remembered %+v", mutation)
		}
	}
	gotEvaluated, gotRemembered := readAheadOutput(t, stream)
	if !bytes.Equal(gotEvaluated, wantEvaluated) || !bytes.Equal(gotRemembered, wantRemembered) {
		t.Fatalf("read a batch ahead, the rounds evaluated\n%s\nand remembered\n%s\nwant, read in one batch\n%s\nand\n%s",
			gotEvaluated, gotRemembered, wantEvaluated, wantRemembered)
	}
}
