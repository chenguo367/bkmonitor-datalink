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
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/activecopy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// eventsOfKind is the events of a kind a strategy wrote on the worker for
// a Slot.
func (worker *workerOfTwo) eventsOfKind(slot execution.EvaluationTime, strategy, kind string) int {
	count := 0
	for _, event := range worker.eventsFor(slot, strategy) {
		if event.EventKind == kind {
			count++
		}
	}
	return count
}

// On a deployment whose recovery gate answers from the alerts the process
// opened (no link), an alert the last owner of a Query Group opened is ended
// by the next one: the Query Group moved to worker-b while its alert was
// open, its next Slot under the threshold writes the RECOVERY, which the gate
// would hold for good had worker-a not handed its record over. Moved back to
// worker-a with an alert worker-b opened, the same: a process that let the
// Query Group go and holds it again reads the ledger as any next owner does.
func TestAnAlertTheLastOwnerOpenedIsEndedByTheNextOwnerAfterAHandover(t *testing.T) {
	// No link, the consumer's own protocol: the recovery gate answers from
	// the alerts this process opened.
	fixture := startTwoWorkerFixtureWith(t, func(cfg *config.Config) {
		cfg.PhaseTwo.Linkd = config.LinkdConfig{}
		cfg.PhaseTwo.Output.Protocol = config.OutputProtocolNative
	}, func(ctx context.Context, client *redis.Client, _ config.Config) {
		// The consumer's protocol takes a strategy with its revision: the
		// fixture's own strategy 1002 too.
		if stored, err := client.Get(ctx, "alarm-config.strategy_1002").Bytes(); err == nil {
			var strategy map[string]any
			if err := json.Unmarshal(stored, &strategy); err != nil {
				t.Fatal(err)
			}
			strategy["strategy_revision"] = 7
			revised, err := json.Marshal(strategy)
			if err != nil {
				t.Fatal(err)
			}
			client.Set(ctx, "alarm-config.strategy_1002", revised, 0)
		}
		installSharedGroup(t, ctx, client, map[int64][]byte{1001: sharedGroupStrategy(t, 1001, 1725000000, func(strategy map[string]any) {
			strategy["strategy_revision"] = 7
		})})
	}, 1)
	queryGroup := fixture.queryGroup
	a, b := fixture.a, fixture.b
	for _, worker := range []*workerOfTwo{a, b} {
		if got := worker.cacheReading(t, "alarmd_recovery_gate_source", "source", activecopy.GateOwn); got != 1 {
			t.Fatalf("%s's recovery gate answers from own = %v, want 1: the case is about a deployment without the link", worker.id, got)
		}
	}
	ledger := func(worker *workerOfTwo, result string) float64 {
		return worker.cacheReading(t, "alarmd_handover_ledger_total", "result", result)
	}
	carried := func(worker *workerOfTwo, direction string) float64 {
		return worker.cacheReading(t, "alarmd_handover_ledger_open_alerts_total", "direction", direction)
	}
	held := func(worker *workerOfTwo) float64 {
		return worker.cacheReading(t, "alarmd_trigger_open_alert_gate_total", "outcome", "held_no_open_alert")
	}

	// openOn runs the Query Group's Slots on the worker over the threshold
	// until one writes its ABNORMAL, from slot on; endOn runs them under it
	// until one writes its RECOVERY, and says whether one did before the
	// gate held one.
	openOn := func(worker *workerOfTwo, slot execution.EvaluationTime) execution.EvaluationTime {
		t.Helper()
		fixture.value.Store(aboveThreshold)
		for last := slot + 300; slot <= last; slot += 60 {
			fixture.runUntilEnded(worker, queryGroup, slot)
			if worker.eventsOfKind(slot, "1001", contract.TriggerEventAbnormal) > 0 {
				return slot
			}
		}
		t.Fatalf("%s wrote no ABNORMAL of strategy 1001 by Slot %d; endings %v", worker.id, slot, worker.slotEndings(queryGroup))
		return 0
	}
	endOn := func(worker *workerOfTwo, slot execution.EvaluationTime) execution.EvaluationTime {
		t.Helper()
		fixture.value.Store(belowThreshold)
		before := held(worker)
		for last := slot + 300; slot <= last; slot += 60 {
			fixture.runUntilEnded(worker, queryGroup, slot)
			if worker.eventsOfKind(slot, "1001", contract.TriggerEventRecovery) > 0 {
				return slot
			}
			if got := held(worker); got != before {
				t.Fatalf("%s's recovery gate held %v RECOVERYs by Slot %d: the alert the last owner opened was not handed over",
					worker.id, got-before, slot)
			}
		}
		t.Fatalf("%s wrote no RECOVERY of strategy 1001 by Slot %d; endings %v", worker.id, slot, worker.slotEndings(queryGroup))
		return 0
	}
	move := func(to, from *workerOfTwo) {
		t.Helper()
		fixture.placement.pin(to.id, queryGroup)
		fixture.reconcile("the move to "+to.id, func() bool {
			return !from.holds()[queryGroup] && to.holds()[queryGroup] && settledRunner(t, to.bundle, queryGroup) != nil
		})
	}

	opened := openOn(a, execution.EvaluationTime(fixture.base+60))
	move(b, a)
	if ledger(a, observability.HandoverLedgerWritten) != 1 || carried(a, "exported") != 1 {
		t.Fatalf("worker-a let the Query Group go with %v ledgers written carrying %v open alerts, want one carrying its alert",
			ledger(a, observability.HandoverLedgerWritten), carried(a, "exported"))
	}
	if ledger(b, observability.HandoverLedgerLoaded) != 1 || carried(b, "imported") != 1 {
		t.Fatalf("worker-b took the Query Group over with %v ledgers loaded carrying %v open alerts, want worker-a's with its alert",
			ledger(b, observability.HandoverLedgerLoaded), carried(b, "imported"))
	}
	ended := endOn(b, opened+60)

	reopened := openOn(b, ended+60)
	move(a, b)
	if ledger(b, observability.HandoverLedgerWritten) != 1 || ledger(a, observability.HandoverLedgerLoaded) != 1 {
		t.Fatalf("moved back: worker-b wrote %v ledgers and worker-a loaded %v, want one each",
			ledger(b, observability.HandoverLedgerWritten), ledger(a, observability.HandoverLedgerLoaded))
	}
	endOn(a, reopened+60)
	for _, worker := range []*workerOfTwo{a, b} {
		for _, result := range []string{observability.HandoverLedgerWriteFailed, observability.HandoverLedgerStale,
			observability.HandoverLedgerUnreadable, observability.HandoverLedgerReadFailed} {
			if got := ledger(worker, result); got != 0 {
				t.Fatalf("%s counted %v handover ledgers %s, want none", worker.id, got, result)
			}
		}
	}
}

// ledgerFakeStore is the ownership store as the ledger uses it: the value
// each Query Group's ledger holds, the fences it was written under, and a
// failure to answer with.
type ledgerFakeStore struct {
	values   map[execution.QueryGroupIdentity][]byte
	fences   []execution.OwnerFence
	writeErr error
	readErr  error
}

func (store *ledgerFakeStore) FencedHashUpdate(_ context.Context, request ownership.FencedHashRequest) error {
	if request.Namespace != handoverLedgerNamespace || request.TTL != handoverLedgerLife || len(request.Set) != 1 || len(request.Delete) != 0 {
		return errors.New("unexpected ledger write")
	}
	if store.writeErr != nil {
		return store.writeErr
	}
	store.fences = append(store.fences, request.Fence)
	store.values[request.Fence.QueryGroup] = request.Set[handoverLedgerField]
	return nil
}

func (store *ledgerFakeStore) ReadControlHashFieldBatch(_ context.Context, queryGroups []execution.QueryGroupIdentity,
	namespace, field string) ([]ownership.ControlRead, error) {
	if namespace != handoverLedgerNamespace || field != handoverLedgerField {
		return nil, errors.New("unexpected ledger read")
	}
	if store.readErr != nil {
		return nil, store.readErr
	}
	values := make([]ownership.ControlRead, len(queryGroups))
	for index, queryGroup := range queryGroups {
		raw, ok := store.values[queryGroup]
		values[index] = ownership.ControlRead{Raw: raw, Missing: !ok}
	}
	return values, nil
}

// epochLeases acquires a Query Group at the epoch the case sets: all a
// session's holder reads of it here is its current lease.
type epochLeases struct {
	ownership.LeaseStore
	epoch uint64
}

func (leases *epochLeases) Acquire(_ context.Context, queryGroup execution.QueryGroupIdentity, workerID string, _ time.Time,
	_ time.Duration) (ownership.Lease, error) {
	return ownership.Lease{Fence: execution.OwnerFence{QueryGroup: queryGroup, OwnerID: workerID, OwnerEpoch: leases.epoch,
		LeaseToken: "token"}}, nil
}

func ledgerSession(t *testing.T, queryGroup execution.QueryGroupIdentity, worker string, epoch uint64) *ownership.Session {
	t.Helper()
	session, err := ownership.OpenSession(context.Background(), &epochLeases{epoch: epoch}, queryGroup, worker, time.Time{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

// ledgerCounts is what the ledger counted, by result, and the open alerts
// it carried each way.
type ledgerCounts struct {
	results            map[string]int
	exported, imported int
}

func (counts *ledgerCounts) record(result string, exported, imported int) {
	counts.results[result]++
	counts.exported += exported
	counts.imported += imported
}

// acknowledgedAbnormal is the ABNORMAL the broker took of an alert of the
// strategy, at level 1.
func acknowledgedAbnormal(strategy, fingerprint string) contract.TriggerEventV1 {
	return contract.TriggerEventV1{TenantID: "system", EventKind: contract.TriggerEventAbnormal, DedupeMD5: fingerprint,
		StrategyRef: &contract.StrategySnapshotRef{}, PlanRef: contract.RuntimePlanRefV1{StrategyID: strategy}, PrimaryLevelID: 1,
		LevelResults: []contract.LevelResultV1{{LevelID: 1, Priority: 1, Result: contract.LevelResultAbnormal}}}
}

const (
	ledgerAlertA = "0123456789abcdef0123456789abcdef"
	ledgerAlertB = "fedcba9876543210fedcba9876543210"
)

// A ledger is taken only from the owner just before this one: written under
// epoch 4 and read at 5, its alert joins the record and passes; read at 6 -
// an owner at 5 left none - it is stale and taken from nobody. A Query Group
// this process rebuilds, one epoch on from the one it held, is not read at
// all; nor one it held under the same epoch. One absent, unreadable or not
// read is counted and changes nothing.
func TestALedgerIsTakenOnlyFromTheOwnerJustBeforeThisOne(t *testing.T) {
	now := func() time.Time { return time.Unix(1_788_000_000, 0) }
	store := &ledgerFakeStore{values: map[execution.QueryGroupIdentity][]byte{}}
	previous, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	writer := newProductionHandoverLedger(store, previous, nil, now, counts.record)
	writer.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-a", 4))
	writer.notePlans("query-group-1", []activecopy.StrategyKey{{TenantID: "system", StrategyID: "1001"}})
	previous.Acknowledged([]contract.TriggerEventV1{acknowledgedAbnormal("1001", ledgerAlertA), acknowledgedAbnormal("2002", ledgerAlertB)})
	writer.handOver(context.Background(), "query-group-1")
	if counts.results[observability.HandoverLedgerWritten] != 1 || counts.exported != 1 ||
		!reflect.DeepEqual(store.fences, []execution.OwnerFence{{QueryGroup: "query-group-1", OwnerID: "worker-a", OwnerEpoch: 4, LeaseToken: "token"}}) {
		t.Fatalf("handed over: %+v under %v, want one ledger of strategy 1001's alert alone, under epoch 4", counts, store.fences)
	}

	take := func(epoch uint64) (*activecopy.Own, *ledgerCounts) {
		own, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
		reader := newProductionHandoverLedger(store, own, nil, now, counts.record)
		reader.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-b", epoch))
		reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
		return own, counts
	}
	if own, counts := take(5); counts.results[observability.HandoverLedgerLoaded] != 1 || counts.imported != 1 ||
		!own.Contains("system", "1001", ledgerAlertA) || own.Contains("system", "2002", ledgerAlertB) {
		t.Fatalf("read at epoch 5: %+v, want the alert of strategy 1001 taken and passing", counts)
	}
	if own, counts := take(6); counts.results[observability.HandoverLedgerStale] != 1 || own.Known() != 0 {
		t.Fatalf("read at epoch 6: %+v and %d alerts taken, want it stale and nothing taken", counts, own.Known())
	}

	// Rebuilt here: held at 5, released and acquired at 6. Read once at 5,
	// not again at 6, nor again at 6.
	own, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	reader := newProductionHandoverLedger(store, own, nil, now, counts.record)
	first := ledgerSession(t, "query-group-1", "worker-b", 5)
	reader.bind("query-group-1", first)
	reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	reader.unbind("query-group-1", first)
	reader.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-b", 6))
	reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	if !reflect.DeepEqual(counts.results, map[string]int{observability.HandoverLedgerLoaded: 1}) {
		t.Fatalf("rebuilt at epoch 6: %v, want the ledger read once, at 5", counts.results)
	}

	for name, value := range map[string][]byte{"absent": nil, "unreadable": []byte(`{"version":1}`), "read_failed": nil} {
		store.values = map[execution.QueryGroupIdentity][]byte{}
		store.readErr = nil
		switch name {
		case "unreadable":
			store.values["query-group-1"] = value
		case "read_failed":
			store.readErr = errors.New("redis unanswered")
		}
		if own, counts := take(5); !reflect.DeepEqual(counts.results, map[string]int{name: 1}) || own.Known() != 0 {
			t.Fatalf("%s: %v and %d alerts taken, want it counted and nothing taken", name, counts.results, own.Known())
		}
	}
}

// What a process hands over is the alerts of the strategies the Query
// Group's Slots evaluated here and of the ones its own ledger named - one
// taken over and let go before a Slot ran hands on what it took - and
// nothing when there is none: no write. A write that fails is counted and
// nothing else; the next owner finds the ledger before it, stale.
func TestAProcessHandsOverWhatItTookAndCountsAWriteThatFailed(t *testing.T) {
	now := func() time.Time { return time.Unix(1_788_000_000, 0) }
	store := &ledgerFakeStore{values: map[execution.QueryGroupIdentity][]byte{}}
	first := activecopy.NewOwn(now)
	writer := newProductionHandoverLedger(store, first, nil, now, (&ledgerCounts{results: map[string]int{}}).record)
	writer.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-a", 4))
	writer.notePlans("query-group-1", []activecopy.StrategyKey{{TenantID: "system", StrategyID: "1001"}})
	first.Acknowledged([]contract.TriggerEventV1{acknowledgedAbnormal("1001", ledgerAlertA)})
	writer.handOver(context.Background(), "query-group-1")

	// Taken over at 5, let go before any Slot ran: what it took goes on.
	second, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	passing := newProductionHandoverLedger(store, second, nil, now, counts.record)
	passing.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-b", 5))
	passing.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	passing.handOver(context.Background(), "query-group-1")
	var written handoverLedgerValue
	if err := json.Unmarshal(store.values["query-group-1"], &written); err != nil || written.OwnerEpoch != 5 ||
		len(written.Own) != 1 || written.Own[0].Fingerprint != ledgerAlertA || counts.results[observability.HandoverLedgerWritten] != 1 {
		t.Fatalf("let go before a Slot ran: wrote %+v (%v), counted %v, want the alert it took under epoch 5", written, err, counts.results)
	}

	// Nothing to hand over: nothing written.
	empty, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	nothing := newProductionHandoverLedger(store, empty, nil, now, counts.record)
	nothing.bind("query-group-2", ledgerSession(t, "query-group-2", "worker-b", 2))
	nothing.notePlans("query-group-2", []activecopy.StrategyKey{{TenantID: "system", StrategyID: "1001"}})
	nothing.handOver(context.Background(), "query-group-2")
	if _, wrote := store.values["query-group-2"]; wrote || !reflect.DeepEqual(counts.results, map[string]int{observability.HandoverLedgerEmpty: 1}) {
		t.Fatalf("nothing to hand over: wrote %v, counted %v, want nothing written and it counted empty", wrote, counts.results)
	}

	// A write refused: counted, and the next owner at 7 finds epoch 5's.
	store.writeErr = ownership.ErrStaleFence
	refused, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	failing := newProductionHandoverLedger(store, refused, nil, now, counts.record)
	failing.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-c", 6))
	failing.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	failing.handOver(context.Background(), "query-group-1")
	if !reflect.DeepEqual(counts.results, map[string]int{observability.HandoverLedgerLoaded: 1, observability.HandoverLedgerWriteFailed: 1}) ||
		counts.exported != 0 {
		t.Fatalf("a refused write: counted %v and %d exported, want it loaded, the write failed and nothing exported", counts.results, counts.exported)
	}
	store.writeErr = nil
	next, counts := activecopy.NewOwn(now), &ledgerCounts{results: map[string]int{}}
	reader := newProductionHandoverLedger(store, next, nil, now, counts.record)
	reader.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-d", 7))
	reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
	if !reflect.DeepEqual(counts.results, map[string]int{observability.HandoverLedgerStale: 1}) || next.Known() != 0 {
		t.Fatalf("after a refused write: counted %v and %d alerts taken, want the ledger before it stale", counts.results, next.Known())
	}
}

// ledgerLookback is a lookback engine that reads nothing, over the clock
// given.
func ledgerLookback(t *testing.T, now func() time.Time) *lookback.Engine {
	t.Helper()
	engine, err := lookback.New(lookback.Options{Now: now,
		Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, errors.New("no reads here")
		},
		Permit: func() (func(), <-chan struct{}, string) { return nil, nil, "none" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// What the lookback had of a Query Group as it was forgotten on its way out
// goes in its ledger, with or without alerts of Own's beside it, and its
// next owner's lookback takes it from there: its Slot on the reread ladder
// and its series_late state restored. A ledger read under an epoch not the
// next one restores nothing.
func TestTheLookbackAQueryGroupHadGoesWithItToItsNextOwner(t *testing.T) {
	at := time.Unix(1_788_000_000, 0)
	now := func() time.Time { return at }
	store := &ledgerFakeStore{values: map[execution.QueryGroupIdentity][]byte{}}
	previous := ledgerLookback(t, now)
	handed := &lookback.Handover{SeriesLate: &lookback.HandoverSeriesLate{Rung: 1, SinceMillis: at.Add(-time.Hour).UnixMilli()},
		Slots: []lookback.HandoverSlot{{Evaluation: 1_787_999_940, Rung: 1, StepMillis: 60_000, ReadAtMillis: at.UnixMilli(), ReadHoldMillis: 30_000}}}
	previous.Restore("query-group-1", handed)
	writer := newProductionHandoverLedger(store, nil, previous, now, (&ledgerCounts{results: map[string]int{}}).record)
	writer.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-a", 4))
	writer.holdLookback("query-group-1", previous.ForgetHandingOver("query-group-1", lookback.ForgottenMoved))
	writer.handOver(context.Background(), "query-group-1")
	var written handoverLedgerValue
	if err := json.Unmarshal(store.values["query-group-1"], &written); err != nil || written.Lookback == nil ||
		!reflect.DeepEqual(written.Lookback, handed) || len(written.Own) != 0 {
		t.Fatalf("wrote %+v (%v), want the lookback handed over as it was", written, err)
	}

	for epoch, restored := range map[uint64]uint64{5: 1, 6: 0} {
		next := ledgerLookback(t, now)
		counts := &ledgerCounts{results: map[string]int{}}
		reader := newProductionHandoverLedger(store, nil, next, now, counts.record)
		reader.bind("query-group-1", ledgerSession(t, "query-group-1", "worker-b", epoch))
		reader.load(context.Background(), []execution.QueryGroupIdentity{"query-group-1"})
		stats := next.Stats()
		if stats.HandoverSlots[lookback.HandoverSlotRestored] != restored || stats.HandoverLate[lookback.HandoverLateRestored] != restored {
			t.Fatalf("read at epoch %d: slots %v, series_late %v, want %d restored", epoch, stats.HandoverSlots, stats.HandoverLate, restored)
		}
	}
}

// A Query Group taken out of the Runner set for another owner - moved,
// declined as hung, or at shutdown - hands what the lookback had of it to its
// ledger as the lookback forgets it; one lost - its lease, its content - is
// forgotten and hands nothing: no ledger is written on a lost path.
func TestALetGoQueryGroupsLookbackIsHeldForItsLedgerAndALostOnesIsNot(t *testing.T) {
	at := time.Unix(1_788_000_000, 0)
	now := func() time.Time { return at }
	for reason, handsOver := range map[string]bool{
		lookback.ForgottenMoved: true, lookback.ForgottenDeclinedHung: true, lookback.ForgottenShutdown: true,
		lookback.ForgottenLeaseLost: false, lookback.ForgottenContentSwitch: false,
	} {
		engine := ledgerLookback(t, now)
		engine.Restore("query-group-1", &lookback.Handover{SeriesLate: &lookback.HandoverSeriesLate{Rung: 1, SinceMillis: at.Add(-time.Hour).UnixMilli()},
			Slots: []lookback.HandoverSlot{{Evaluation: 1_787_999_940, Rung: 1, StepMillis: 60_000, ReadAtMillis: at.UnixMilli(), ReadHoldMillis: 30_000}}})
		held := map[execution.QueryGroupIdentity]*lookback.Handover{}
		bundle := &phaseTwoWorkerBundle{runners: map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{"query-group-1": {}},
			dependencies: phaseTwoWorkerBundleDependencies{Lookback: engine,
				HoldLookbackHandover: func(queryGroup execution.QueryGroupIdentity, handover *lookback.Handover) {
					held[queryGroup] = handover
				}}}
		bundle.removeRunnerLocked("query-group-1", reason)
		handover, got := held["query-group-1"]
		if got != handsOver || handsOver && (handover == nil || len(handover.Slots) != 1 || handover.SeriesLate == nil) {
			t.Fatalf("%s: held %v (%+v), want held %t with the Slot and the series_late state", reason, got, handover, handsOver)
		}
		if stats := engine.Stats(); stats.GroupsForgotten[reason] != 1 {
			t.Fatalf("%s: forgotten %v, want the Query Group forgotten for it", reason, stats.GroupsForgotten)
		}
	}
}
