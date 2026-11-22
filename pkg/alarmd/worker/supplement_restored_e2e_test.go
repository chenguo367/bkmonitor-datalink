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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// providerRead is the rig's provider read again for a supplement: what a
// Slot handed over is supplemented on, every series of its window as the
// provider answers it now.
type providerRead struct{ provider *shadowProvider }

func (read providerRead) Keeps(execution.PhysicalQueryDigest) bool { return true }

func (read providerRead) Replay(ctx context.Context, spec execution.PhysicalQuerySpec,
	sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	return read.provider.Execute(ctx, execution.QueryAttempt{Spec: spec, AttemptNo: 1}, sink)
}

// stateValues is every runtime State key the rig's Redis holds, with its
// bytes.
func (rig *shadowRig) stateValues() map[string]string {
	rig.t.Helper()
	ctx := context.Background()
	values := map[string]string{}
	for _, key := range rig.client.Keys(ctx, "*").Val() {
		if strings.Contains(key, "runtime3") {
			values[key] = rig.client.Get(ctx, key).Val()
		}
	}
	return values
}

// supplementAll supplements the Slot at evaluation over every series the
// provider answers for it now, as a Slot handed over is (014 B4 H1).
func (rig *shadowRig) supplementAll(evaluation execution.EvaluationTime) execution.SupplementFacts {
	rig.t.Helper()
	return rig.supplementKept(evaluation, providerRead{provider: rig.provider})
}

// supplementRequest is the supplement of the Slot at evaluation over every
// series of a read, its Plan frozen as a supplement freezes it.
func (rig *shadowRig) supplementRequest(evaluation execution.EvaluationTime) execution.SlotExecutionRequest {
	rig.t.Helper()
	rig.frozen.mu.Lock()
	due := rig.frozen.plan.DuePlans[0]
	rig.frozen.mu.Unlock()
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
	request.Operation, request.AttemptNo = execution.OperationSupplement, 1
	request.Supplement = &execution.SupplementScope{All: true}
	return request
}

// supplementKept supplements the Slot at evaluation over every series read
// keeps.
func (rig *shadowRig) supplementKept(evaluation execution.EvaluationTime, read access.KeptRead) execution.SupplementFacts {
	rig.t.Helper()
	result, err := rig.coordinator.Execute(access.WithKeptRead(context.Background(), read), rig.supplementRequest(evaluation))
	if err != nil || result.Supplement == nil {
		rig.t.Fatalf("the supplement: %+v, %v", result, err)
	}
	return *result.Supplement
}

// restoredLookback is a lookback wired to the rig as a process wires its own:
// its rereads the rig's provider; a Slot handed over planned by the rig's
// query source, read in the slices the coordinator reads the Query Group in
// and keeping what the coordinator says its segments do not cover; its
// supplements the coordinator's, each told on seen.
func (rig *shadowRig) restoredLookback(now func() time.Time, seen *restoredSeen) *lookback.Engine {
	rig.t.Helper()
	return rig.restoredLookbackShaped(now, seen, nil)
}

// restoredLookbackShaped is restoredLookback whose process knows the Query
// Group's slices only while shaped says so (nil: always), as one that took
// it over and has not completed a Slot of it yet: its shape check and its
// planning answer none before.
func (rig *shadowRig) restoredLookbackShaped(now func() time.Time, seen *restoredSeen, shaped func() bool) *lookback.Engine {
	rig.t.Helper()
	engine, err := lookback.New(lookback.Options{Now: now,
		Recheck: func(ctx context.Context, spec execution.PhysicalQuerySpec, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			completion, err := rig.provider.Execute(ctx, execution.QueryAttempt{Spec: spec, Slice: execution.InputSliceFrom(ctx), AttemptNo: 1}, sink)
			seen.note(fmt.Sprintf("recheck %v: %v %v", execution.InputSliceFrom(ctx).Conditions, completion.Completeness, err))
			return completion, err
		},
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1 },
		Memory: func(bytes uint64) bool {
			seen.mu.Lock()
			defer seen.mu.Unlock()
			seen.asked = append(seen.asked, bytes)
			return true
		},
		Respec: func(ctx context.Context, _ execution.QueryGroupIdentity, evaluation execution.EvaluationTime, _ int64) (lookback.RestoredPlan, error) {
			request := rig.supplementRequest(evaluation)
			header, specs, err := rig.source.PlannedExecution(ctx, request.Contract)
			if err != nil {
				seen.note(fmt.Sprintf("respec: %v", err))
				return lookback.RestoredPlan{}, err
			}
			plan := lookback.RestoredPlan{Specs: specs}
			if shaped != nil && !shaped() {
				seen.note("respec: no shape")
				return plan, nil
			}
			plan.Slices, plan.Shaped = worker.RestoredSlices(rig.coordinator, request)
			plan.Covered = worker.RestoredCoverage(ctx, rig.coordinator, request, header)
			seen.note(fmt.Sprintf("respec: %d specs, %d slices, shaped %t, covered %t", len(specs), len(plan.Slices), plan.Shaped,
				plan.Covered != nil))
			return plan, nil
		},
		Supplement: func(_ context.Context, job lookback.SupplementJob) lookback.SupplementOutcome {
			facts := rig.supplementKept(job.EvaluationTime, job.Read)
			seen.mu.Lock()
			seen.supplements = append(seen.supplements, facts)
			seen.mu.Unlock()
			return lookback.SupplementOutcome{Ran: true, Facts: facts}
		}})
	if err != nil {
		rig.t.Fatal(err)
	}
	return engine
}

// restoredSeen is what a restored lookback asked the memory line for, and
// the facts of each supplement it ran.
type restoredSeen struct {
	mu          sync.Mutex
	asked       []uint64
	supplements []execution.SupplementFacts
	// notes is what the lookback's calls into the rig came to, in order.
	notes []string
}

func (seen *restoredSeen) note(line string) {
	seen.mu.Lock()
	defer seen.mu.Unlock()
	seen.notes = append(seen.notes, line)
}

func (seen *restoredSeen) read() ([]uint64, []execution.SupplementFacts) {
	seen.mu.Lock()
	defer seen.mu.Unlock()
	return append([]uint64(nil), seen.asked...), append([]execution.SupplementFacts(nil), seen.supplements...)
}

// A Slot handed over is supplemented over every series its window holds
// now, its first read's series not known (014 B4 H1), and decides only the
// series that read did not hold: through Execute, over a real State store,
// the members its stable segment covered at the Slot - open ones, and one
// closed after it - are told decided by the segment, their State not read
// and not written again; a series written at the Slot is told by its
// State's version; a series late for the Slot - absent from its round and
// closed before it - is read, decided and written. Only its key changes.
func TestASlotHandedOverDecidesOnlyTheSeriesItsFirstReadDidNotHold(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	step := func(absent []string, value map[string]string) {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, absent, value, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	for round := 0; round < 6; round++ {
		step(nil, nil)
	}
	// The Slot handed over: host-02 an exception, written; host-03 late,
	// absent from its round.
	step([]string{"host-03"}, map[string]string{"host-02": "10"})
	slot := at
	// After it, host-04 goes, closed at the Slot; host-02 and host-03 stay
	// away, so nothing later decides them.
	step([]string{"host-02", "host-03", "host-04"}, nil)
	open, closed := rig.storedMembers(at)
	if open < 2 || closed < 2 {
		t.Fatalf("the book holds %d open and %d closed members, want host-00 and host-01 open, host-03 and host-04 closed", open, closed)
	}
	before := rig.stateValues()
	// The Slot read again: every host, host-03's late point among them.
	rig.provider.round(int64(slot), nil, map[string]string{"host-02": "10"}, false)
	facts := rig.supplementAll(slot)
	if facts.Candidates != 5 || facts.CoveredT != 3 || facts.CrossedT != 4 || facts.Unreached != 1 || facts.Admitted != 1 {
		t.Fatalf("facts %+v, want host-00, host-01 and host-04 covered, host-02 crossed by its version, host-03 read and admitted", facts)
	}
	after := rig.stateValues()
	changed := 0
	for key, value := range after {
		if before[key] != value {
			changed++
		}
	}
	if changed != 1 || len(after) != len(before) {
		t.Fatalf("%d State keys changed of %d (%d before), want host-03's alone", changed, len(after), len(before))
	}
}

// A Slot handed over of a Query Group its stable segments cover nineteen of
// twenty series of is read again by its next owner's lookback, through the
// query source and coordinator a process reads with, keeping only the one
// they do not cover: the others dropped as the read streams, counted decided
// by their segment, so the memory line is asked for one series' room and
// the supplement sees that series alone, which it reads and decides.
func TestARestoredSlotsRereadKeepsOnlyTheSeriesItsSegmentsDoNotCover(t *testing.T) {
	rig := newThresholdShadowRig(t, 20, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	if slices, shaped := worker.RestoredSlices(rig.coordinator, rig.supplementRequest(at)); shaped {
		t.Fatalf("before any Slot here, the reread is cut in %v; want it held for one", slices)
	}
	step := func(absent []string) {
		t.Helper()
		at += 60
		if completed, err := rig.run(at, absent, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	for round := 0; round < 6; round++ {
		step(nil)
	}
	// The Slot handed over, host-03 late for it; after it, host-03 stays away.
	step([]string{"host-03"})
	slot := at
	step([]string{"host-03"})
	if open, _ := rig.storedMembers(at); open != 19 {
		outcomes, members := rig.observed.snapshot()
		rig.observed.mu.Lock()
		classes := fmt.Sprint(rig.observed.classes)
		rig.observed.mu.Unlock()
		t.Fatalf("the book holds %d open members, want the nineteen hosts other than host-03 (outcomes %v, members %v, classes %s)",
			open, outcomes, members, classes)
	}
	if slices, shaped := worker.RestoredSlices(rig.coordinator, rig.supplementRequest(slot)); !shaped || len(slices) != 1 || !slices[0].IsZero() {
		t.Fatalf("the Query Group read whole here: the reread cut in %v (%t), want it whole", slices, shaped)
	}
	rig.provider.round(int64(slot), nil, nil, false)

	clock := time.Unix(1_788_000_000, 0)
	seen := &restoredSeen{}
	engine := rig.restoredLookback(func() time.Time { return clock }, seen)
	queryGroup := rig.base.Contract.Slot.QueryGroup
	engine.Restore(queryGroup, &lookback.Handover{SeriesLate: &lookback.HandoverSeriesLate{Rung: 0, SinceMillis: clock.UnixMilli()},
		Slots: []lookback.HandoverSlot{{Evaluation: slot, Rung: 0, StepMillis: 60_000, ReadAtMillis: clock.UnixMilli(),
			Source: "bk_monitor/time_series"}}})
	clock = clock.Add(time.Duration(lookback.RungSteps[0] * float64(time.Minute)))
	// Stepped until the supplement's facts are counted, once it returns.
	deadline := testwait.Deadline(t)
	for {
		engine.Step(context.Background())
		if _, supplements := seen.read(); len(supplements) > 0 && engine.Stats().RestoredDedup[lookback.RestoredDedupRead] > 0 {
			break
		}
		if time.Now().After(deadline) {
			stats := engine.Stats()
			seen.mu.Lock()
			notes := append([]string(nil), seen.notes...)
			seen.mu.Unlock()
			windows := map[string]string{}
			for source, facts := range stats.Sources {
				windows[source] = fmt.Sprintf("%v %v", facts.SupplementWindows, facts.SupplementUnobserved)
			}
			t.Fatalf("the restored Slot was not supplemented: calls %q, handover slots %v, windows %v", notes, stats.HandoverSlots, windows)
		}
		time.Sleep(10 * time.Millisecond) // fixed-wait: the poll interval of a condition wait bounded by the test deadline
	}
	stats := engine.Stats()
	asked, supplements := seen.read()
	if stats.RestoredDedup[lookback.RestoredDedupSegment] != 19 || stats.RestoredDedup[lookback.RestoredDedupRead] != 1 {
		t.Fatalf("restored dedup %v, want nineteen by their segment and host-03 read", stats.RestoredDedup)
	}
	if len(supplements) != 1 || supplements[0].Candidates != 1 || supplements[0].Unreached != 1 || supplements[0].Admitted != 1 {
		t.Fatalf("supplements %+v, want one, of host-03 alone, read and admitted", supplements)
	}
	if len(asked) != 1 {
		t.Fatalf("the memory line was asked %d times (%v), want once, for host-03's room alone", len(asked), asked)
	}
}

// datasetSink keeps every series a provider read delivers, as the lookback
// is handed a first read's.
type datasetSink struct {
	batches []execution.ProviderSeriesBatch
}

func (sink *datasetSink) ConsumeProviderSeries(_ context.Context, batch execution.ProviderSeriesBatch) error {
	sink.batches = append(sink.batches, batch)
	return nil
}

// A series late both in a Slot its previous owner handed over and in the new
// owner's next Slot has both its points in State: the handed-over Slot,
// due while the new owner has completed no Slot of its Query Group so knows
// no slices to read it in, waits for them, and the new owner's own Slot,
// due after, waits for it - so the series is supplemented at the earlier
// Slot first, and its point there is not found decided by the later Slot's
// write. Through Execute on the real State store, the lookback handed the
// new owner's first read as the query source hands it.
func TestASeriesLateInARestoredSlotAndTheNextOneHasBothPointsInState(t *testing.T) {
	rig := newThresholdShadowRig(t, 5, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 6; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	// The previous owner's last Slot, host-03 late for it.
	at += 60
	if completed, err := rig.run(at, []string{"host-03"}, nil, false); !completed {
		t.Fatalf("the handed-over Slot: completed=%t err=%v", completed, err)
	}
	handed := at

	clock := time.Unix(1_788_100_000, 0)
	var mu sync.Mutex
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	set := func(to time.Time) {
		mu.Lock()
		clock = to
		mu.Unlock()
	}
	var shaped atomic.Bool
	seen := &restoredSeen{}
	engine := rig.restoredLookbackShaped(now, seen, shaped.Load)
	queryGroup := rig.base.Contract.Slot.QueryGroup
	readAt := now()
	engine.Restore(queryGroup, &lookback.Handover{SeriesLate: &lookback.HandoverSeriesLate{Rung: 0, SinceMillis: readAt.UnixMilli()},
		Slots: []lookback.HandoverSlot{{Evaluation: handed, Rung: 0, StepMillis: 60_000, ReadAtMillis: readAt.UnixMilli(),
			Source: "bk_monitor/time_series"}}})
	// Due before the new owner has completed a Slot of the Query Group: it
	// waits. Its planning, which freezes the rig's Plan, done before the
	// rig runs a Slot of its own.
	set(readAt.Add(90 * time.Second))
	engine.Step(context.Background())
	testwait.Until(t, "the restored Slot found no shape", func() bool {
		seen.mu.Lock()
		defer seen.mu.Unlock()
		return len(seen.notes) > 0
	})
	// The new owner's first Slot, host-03 late again, and its first read
	// handed to the lookback as the query source hands it.
	next := handed + 60
	if completed, err := rig.run(next, []string{"host-03"}, nil, false); !completed {
		t.Fatalf("the new owner's Slot: completed=%t err=%v", completed, err)
	}
	request := rig.supplementRequest(next)
	_, specs, err := rig.source.PlannedExecution(context.Background(), request.Contract)
	if err != nil || len(specs) != 1 {
		t.Fatalf("the new owner's Slot planned %d queries: %v", len(specs), err)
	}
	first := &datasetSink{}
	if _, err := rig.provider.Execute(context.Background(), execution.QueryAttempt{Spec: specs[0], AttemptNo: 1}, first); err != nil {
		t.Fatal(err)
	}
	read := engine.Begin(lookback.Query{Contract: request.Contract, Spec: specs[0], Operation: execution.OperationNormal, AttemptNo: 1,
		ReadyAt: now()})
	if read == nil {
		t.Fatal("the new owner's first read was not kept for its directed read")
	}
	for _, batch := range first.batches {
		read.Series(batch.Dataset, batch.Delivery.Bytes)
	}
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	// host-03's points land; the new owner's Slot comes due while its
	// Query Group's slices are still not known here - its Slot read, not
	// yet completed - and is held back behind the restored one.
	rig.provider.round(int64(next), nil, nil, false)
	set(now().Add(90 * time.Second))
	for step := 0; step < 20; step++ {
		engine.Step(context.Background())
		time.Sleep(10 * time.Millisecond) // fixed-wait: lets a dispatched reread and supplement run before the next Step
	}
	if _, supplements := seen.read(); len(supplements) != 0 {
		t.Fatalf("%d supplements while the restored Slot waits for its slices, want none", len(supplements))
	}
	shaped.Store(true)
	deadline := testwait.Deadline(t)
	for {
		engine.Step(context.Background())
		if _, supplements := seen.read(); len(supplements) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			seen.mu.Lock()
			notes := append([]string(nil), seen.notes...)
			seen.mu.Unlock()
			t.Fatalf("the two Slots were not both supplemented: calls %q", notes)
		}
		set(now().Add(time.Second))
		time.Sleep(10 * time.Millisecond) // fixed-wait: the poll interval of a condition wait bounded by the test deadline
	}
	_, supplements := seen.read()
	for index, slot := range []execution.EvaluationTime{handed, next} {
		if supplements[index].Admitted != 1 {
			t.Fatalf("supplement %d, of the Slot at %d: %+v; want host-03's point admitted at each Slot", index, slot, supplements[index])
		}
	}
}
