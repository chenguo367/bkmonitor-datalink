// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The shape of these cases: a fifteen-second Query Group whose first
// unfinished Slot is eight intervals behind the clock, far past the three a
// replay may be behind.
const (
	heldRangeInterval  = int64(15)
	heldRangeFirstSlot = execution.EvaluationTime(600)
)

// heldRangeSource is a production Slot source at at over that Query Group,
// reporting to the observer it returns.
func heldRangeSource(t *testing.T, at time.Time) (*ProductionSlotSource, *distanceExpiryObserver) {
	t.Helper()
	schedule := schedulerSchedule(t, heldRangeInterval, heldRangeFirstSlot, nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceWithRecoveryForTest(t, catalog,
		foundProgress(heldRangeFirstSlot, heldRangeFirstSlot-execution.EvaluationTime(heldRangeInterval)), at, testRecoveryLimits())
	observer := &distanceExpiryObserver{}
	source.observer = observer
	return source, observer
}

// stagesOf counts what an observer was told, by stage.
func stagesOf(observations []observability.Observation) map[observability.Stage]int {
	stages := map[observability.Stage]int{}
	for _, observation := range observations {
		stages[observation.Stage]++
	}
	return stages
}

// A Slot past its distance while the pool holds its Query Group is the hold's
// to keep: the source builds no range and reports neither the expiry nor a
// gate refusal, and the Slot it returns is one the Runner holds. With the hold
// over, the same clock closes the run in one range and reports it once. Before,
// the source built a one-step range at every grid point of the hold and the
// Runner let each go, a Slot a grid point.
func TestAHeldGroupsRunPastItsDistanceIsKeptWholeAndClosedOnceItsHoldEnds(t *testing.T) {
	at := time.Unix(int64(heldRangeFirstSlot)+8*heldRangeInterval, 0)
	flight := context.WithValue(context.Background(), rangeFlightContextKey{}, execution.QueryGroupIdentity("query-group-1"))

	// Held, as the Runner calls it: the pool holds and has the Query Group.
	pooled := withQueryCooldownPooled(flight, &observability.HeldByFacts{Decision: "query_cooldown"})
	source, observer := heldRangeSource(t, at)
	slot, due, _, err := source.Next(withQueryCooldownHeld(pooled, true), "query-group-1")
	if err != nil || !due {
		t.Fatalf("held: Next() due=%v err=%v", due, err)
	}
	if slot.ExpiredRange != nil || slot.Recovery.Disposition != ReplayExpired || slot.Recovery.Reason != ReplayExpiredByQueryCooldown ||
		slot.Recovery.Distance <= testRecoveryLimits().MaxReplaySlots {
		t.Fatalf("held: range %v, recovery %+v; want the one Slot, past its distance, named the pool's", slot.ExpiredRange != nil, slot.Recovery)
	}
	if !heldPastDistance(slot) {
		t.Fatalf("held: the Runner would not hold %+v", slot.Recovery)
	}
	if stages := stagesOf(observer.seen); stages[observability.StageReplayExpired]+stages[observability.StageRangeGateDecided]+
		stages[observability.StageRangeDistanceExpired] != 0 {
		t.Fatalf("held: the source reported %v for a Slot the hold keeps", stages)
	}

	// The hold over and the probe still to come: still the pool's.
	source, observer = heldRangeSource(t, at)
	slot, due, _, err = source.Next(pooled, "query-group-1")
	if err != nil || !due || slot.ExpiredRange == nil {
		t.Fatalf("hold over: Next() range=%v due=%v err=%v; want the run closed in one range", slot.ExpiredRange != nil, due, err)
	}
	if slot.Recovery.Reason != ReplayExpiredByQueryCooldown {
		t.Fatalf("hold over: the range is given up as %s, want the pool's gap", slot.Recovery.Reason)
	}
	if proof := slot.ExpiredRange; proof.EligibilityV2 == nil || proof.EligibilityV2.Reason != execution.RangeDistanceExpired || proof.Count < 2 {
		t.Fatalf("hold over: range %+v of %d Slots; want the distance branch over more than one", proof.EligibilityV2, proof.Count)
	}
	if stages := stagesOf(observer.seen); stages[observability.StageReplayExpired] != 1 || stages[observability.StageRangeGateDecided] != 1 {
		t.Fatalf("hold over: the source reported %v; want the expiry once and the range it applied once", stages)
	}
	for _, observation := range observer.seen {
		if observation.Stage != observability.StageReplayExpired {
			continue
		}
		if observation.ReplayExpiry == nil || observation.ReplayExpiry.HeldBy == nil || observation.ReplayExpiry.HeldBy.Decision != "query_cooldown" {
			t.Fatalf("hold over: the expiry is held by %+v, want the pool", observation.ReplayExpiry)
		}
	}
}

// Past the Slot's recovery bound the hold keeps nothing: the age branch builds
// its range as it always did. One interval past it, the first step the age
// branch counts (a second past it, the range would be under one step and the
// Slot given up alone, held or not).
func TestAHeldGroupsSlotAtItsRecoveryBoundIsClosedByAge(t *testing.T) {
	at := time.Unix(int64(heldRangeFirstSlot)+8*heldRangeInterval, 0)
	flight := context.WithValue(context.Background(), rangeFlightContextKey{}, execution.QueryGroupIdentity("query-group-1"))
	source, _ := heldRangeSource(t, at)
	held, _, _, err := source.Next(withQueryCooldownHeld(flight, true), "query-group-1")
	if err != nil {
		t.Fatal(err)
	}
	bound := time.UnixMilli(held.RecoveryUntilUnixMilli).Add(time.Duration(heldRangeInterval)*time.Second + time.Second)
	source, _ = heldRangeSource(t, bound)
	slot, due, _, err := source.Next(withQueryCooldownHeld(flight, true), "query-group-1")
	if err != nil || !due || slot.ExpiredRange == nil {
		t.Fatalf("at the recovery bound: Next() range=%v due=%v err=%v; want the age branch's range", slot.ExpiredRange != nil, due, err)
	}
	if proof := slot.ExpiredRange; proof.EligibilityV2 == nil || proof.EligibilityV2.Reason != execution.RangeAgeExpired {
		t.Fatalf("at the recovery bound: range %+v; want the age branch", proof.EligibilityV2)
	}
}

// pooledRunner is a Runner the pool has taken in: three rounds whose queries
// were unavailable, last the result of whatever runs next. It returns the
// Runner, the source whose Slot the case shapes, and the clock it reads.
func pooledRunner(t *testing.T, last execution.SlotExecutionResult) (*Runner, *fakeSlotSource, *time.Time) {
	t.Helper()
	now := time.Unix(100, 0)
	source := &fakeSlotSource{slot: frozenSlot("query-group-1"), facts: SlotDueFacts{IntervalSeconds: 10}}
	source.slot.EarliestQueryDeadlineUnixMilli = 150_000
	executor := &scriptedExecutor{results: []execution.SlotExecutionResult{
		unavailableResult(), unavailableResult(), unavailableResult(), last,
	}}
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner("query-group-1", &fakeSession{fence: source.slot.Dispatch.OwnerFence}, source, executor, flights, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		source.slot.Contract.DuePlanSetDigest = execution.DuePlanSetDigest(fmt.Sprintf("slot-digest-%d", i))
		source.slot.DuePlanTargets.DuePlanSetDigest = source.slot.Contract.DuePlanSetDigest
		source.slot.Contract.Slot.EvaluationTime = execution.EvaluationTime(100 + i)
		source.slot.ExpectedNextSlot = source.slot.Contract.Slot.EvaluationTime
		if _, attempted, err := runner.RunOne(context.Background()); err != nil || !attempted {
			t.Fatalf("round %d: %t %v", i, attempted, err)
		}
	}
	if !runner.queryCooldownHolds() {
		t.Fatal("three unavailable rounds did not put the Query Group in the pool")
	}
	source.slot.Contract.Slot.EvaluationTime++
	source.slot.ExpectedNextSlot++
	source.slot.Contract.DuePlanSetDigest = "next-slot-digest"
	source.slot.DuePlanTargets.DuePlanSetDigest = source.slot.Contract.DuePlanSetDigest
	return runner, source, &now
}

// A Slot past its distance (or its wait) is held while the pool holds, and the
// Runner wakes when the hold ends or at the recovery bound, not at the Slot's
// recheck - the next grid point plus its read hold, where every pooled Query
// Group used to wake in the same second and give one Slot up.
func TestTheHoldKeepsASlotPastItsDistanceAndWakesAtItsEnd(t *testing.T) {
	for _, reason := range []ReplayExpiryReason{ReplayExpiredByQueryCooldown, ReplayExpiredByDistance, ReplayExpiredByWait} {
		runner, source, now := pooledRunner(t, gapSkippedResult())
		// The recheck falls before the hold's end (at least a period after
		// the third failure), so a wake at the recheck is told apart from one
		// at the end.
		source.slot.Recovery = SlotRecoveryFacts{Disposition: ReplayExpired, Reason: reason, Distance: 4,
			RecheckAtUnixMilli: now.Add(5 * time.Second).UnixMilli()}
		source.slot.RecoveryUntilUnixMilli = now.Add(time.Hour).UnixMilli()
		source.slot.KeepUntilUnixMilli = source.slot.RecoveryUntilUnixMilli + time.Minute.Milliseconds()
		if _, attempted, err := runner.RunOne(context.Background()); err != nil || attempted {
			t.Fatalf("%s: attempted=%t err=%v; want the Slot held", reason, attempted, err)
		}
		if wake := runner.queryCooldown.wakeAt; !wake.Equal(runner.queryCooldown.until) {
			t.Fatalf("%s: wakes at %v, want the hold's end %v and not the recheck", reason, wake, runner.queryCooldown.until)
		}
		// The due index's bound is the same instant: the fleet's overdue
		// reading compares the clock to it, so a Query Group the hold keeps
		// however far behind is not late before the hold ends.
		if bound := runner.DueBound(); bound.NotDueUntilUnix != runnerBoundSecond(runner.queryCooldown.until) {
			t.Fatalf("%s: not due until %d, want the hold's end %d", reason, bound.NotDueUntilUnix, runnerBoundSecond(runner.queryCooldown.until))
		}
	}
}

// A Slot at its recovery bound - given up for its age, which is the bound - is
// let go whatever the pool does: the age branch closes what the hold kept.
func TestTheHoldLetsASlotAtItsRecoveryBoundGo(t *testing.T) {
	runner, source, now := pooledRunner(t, gapSkippedResult())
	source.slot.Recovery = SlotRecoveryFacts{Disposition: ReplayExpired, Reason: ReplayExpiredByQueryCooldown, Distance: 1}
	// The bound a few seconds on, past the Slot's query deadline, and the
	// clock at it while the hold still runs.
	evaluation := int64(source.slot.Contract.Slot.EvaluationTime) * 1000
	source.slot.EarliestQueryDeadlineUnixMilli = evaluation + 1_000
	source.slot.RecoveryUntilUnixMilli = evaluation + 2_000
	*now = time.UnixMilli(source.slot.RecoveryUntilUnixMilli)
	if !runner.queryCooldownHolds() {
		t.Fatalf("the hold ran out by %v; the case needs it still running at the bound", *now)
	}
	if _, attempted, err := runner.RunOne(context.Background()); err != nil || !attempted {
		t.Fatalf("attempted=%t err=%v; want the aged Slot let go", attempted, err)
	}
}

// A Query Group the pool never had is given up for its distance under the
// distance's own reason, in a range: the pool's word is left to the pool. The
// range the pool gives up from the same Progress at the same clock is the
// same proof, digest and all: the pool is named on the Slot's facts and the
// completion's cause, never in what is written down.
func TestAGroupThePoolNeverHadIsGivenUpForItsDistance(t *testing.T) {
	at := time.Unix(int64(heldRangeFirstSlot)+8*heldRangeInterval, 0)
	flight := context.WithValue(context.Background(), rangeFlightContextKey{}, execution.QueryGroupIdentity("query-group-1"))
	source, _ := heldRangeSource(t, at)
	slot, due, _, err := source.Next(flight, "query-group-1")
	if err != nil || !due || slot.ExpiredRange == nil || slot.Recovery.Reason != ReplayExpiredByDistance {
		t.Fatalf("Next() range=%v reason=%s due=%v err=%v; want a range given up for its distance", slot.ExpiredRange != nil, slot.Recovery.Reason, due, err)
	}
	source, _ = heldRangeSource(t, at)
	pooled, due, _, err := source.Next(withQueryCooldownPooled(flight, &observability.HeldByFacts{Decision: "query_cooldown"}), "query-group-1")
	if err != nil || !due || pooled.ExpiredRange == nil {
		t.Fatalf("pooled: Next() range=%v due=%v err=%v", pooled.ExpiredRange != nil, due, err)
	}
	if pooled.ExpiredRange.Digest == "" || pooled.ExpiredRange.Digest != slot.ExpiredRange.Digest ||
		pooled.ExpiredRange.CompletionKind() != slot.ExpiredRange.CompletionKind() ||
		pooled.ExpiredRange.CompletionReason() != slot.ExpiredRange.CompletionReason() {
		t.Fatalf("the pool's range is sealed %q (%s %s), the same range outside the pool %q (%s %s): the pool reached the proof",
			pooled.ExpiredRange.Digest, pooled.ExpiredRange.CompletionKind(), pooled.ExpiredRange.CompletionReason(),
			slot.ExpiredRange.Digest, slot.ExpiredRange.CompletionKind(), slot.ExpiredRange.CompletionReason())
	}
}

// skipLines records the slot_skipped lines a Runner reports.
type skipLines struct{ seen []observability.Observation }

func (lines *skipLines) Observe(_ context.Context, observation observability.Observation) {
	if observation.Stage == observability.StageSlotSkipped {
		lines.seen = append(lines.seen, observation)
	}
}

// The run the pool gives up when its hold ends names the pool on its skip
// line - held_by=query_cooldown with the pool's facts - though the round
// before it was one that ran: the fleet files the loss as the pool's
// (LossAfterCooldown) by that word, and without it the run read as detection
// abandoned. A Query Group the pool never had gives the same Slot up under
// what its own previous round did.
func TestTheRunThePoolGivesUpNamesThePoolOnItsSkipLine(t *testing.T) {
	pooledGap := gapSkippedResult()
	pooledGap.QueryFreeCause = execution.CauseQueryCooldown
	runner, source, now := pooledRunner(t, pooledGap)
	lines := &skipLines{}
	runner.WithSlotSkipReport(lines, nil)
	*now = runner.queryCooldown.until
	source.slot.Recovery = SlotRecoveryFacts{Disposition: ReplayExpired, Reason: ReplayExpiredByQueryCooldown, Distance: 5}
	source.slot.RecoveryUntilUnixMilli = now.Add(time.Hour).UnixMilli()
	source.slot.KeepUntilUnixMilli = source.slot.RecoveryUntilUnixMilli + time.Minute.Milliseconds()
	if _, attempted, err := runner.RunOne(context.Background()); err != nil || !attempted {
		t.Fatalf("hold over: attempted=%t err=%v; want the expired Slot given up", attempted, err)
	}
	if len(lines.seen) != 1 || lines.seen[0].HeldBy == nil || lines.seen[0].HeldBy.Decision != "query_cooldown" ||
		lines.seen[0].HeldBy.QueryCooldownUntilMilli == 0 {
		t.Fatalf("the pool's gap reported %+v; want one skip line held by the query cooldown, with its facts", lines.seen)
	}

	never := &fakeSlotSource{slot: frozenSlot("query-group-1"), facts: SlotDueFacts{IntervalSeconds: 10}}
	never.slot.Recovery = SlotRecoveryFacts{Disposition: ReplayExpired, Reason: ReplayExpiredByDistance, Distance: 5}
	never.slot.RecoveryUntilUnixMilli = now.Add(time.Hour).UnixMilli()
	never.slot.KeepUntilUnixMilli = never.slot.RecoveryUntilUnixMilli + time.Minute.Milliseconds()
	clock := *now
	flights, err := NewFlightCoordinatorWithRecovery(testRecoveryLimits(), func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := NewRunner("query-group-1", &fakeSession{fence: never.slot.Dispatch.OwnerFence}, never,
		&scriptedExecutor{results: []execution.SlotExecutionResult{gapSkippedResult()}}, flights, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	freshLines := &skipLines{}
	fresh.WithSlotSkipReport(freshLines, nil)
	if _, attempted, err := fresh.RunOne(context.Background()); err != nil || !attempted {
		t.Fatalf("never pooled: attempted=%t err=%v", attempted, err)
	}
	if len(freshLines.seen) != 1 || (freshLines.seen[0].HeldBy != nil && freshLines.seen[0].HeldBy.Decision == "query_cooldown") {
		t.Fatalf("never pooled: %+v; want one skip line not held by the query cooldown", freshLines.seen)
	}
}

// A cooldown that ends inside the replay window gives nothing up: the Slot at
// the cursor, a second short of three intervals behind and so at a distance of
// three, the most the replay allows, is replayed as any Slot is - no range, no
// expiry - and a hold over it is the probe.
func TestACooldownEndingInsideTheReplayWindowReplaysItsSlot(t *testing.T) {
	at := time.Unix(int64(heldRangeFirstSlot)+int64(testRecoveryLimits().MaxReplaySlots)*heldRangeInterval-1, 0)
	flight := context.WithValue(context.Background(), rangeFlightContextKey{}, execution.QueryGroupIdentity("query-group-1"))
	source, observer := heldRangeSource(t, at)
	slot, due, _, err := source.Next(withQueryCooldownPooled(flight, &observability.HeldByFacts{Decision: "query_cooldown"}), "query-group-1")
	if err != nil || !due {
		t.Fatalf("Next() due=%v err=%v", due, err)
	}
	if slot.ExpiredRange != nil || slot.Recovery.Disposition == ReplayExpired || slot.Dispatch.Operation != execution.OperationReplay ||
		slot.Recovery.Distance != testRecoveryLimits().MaxReplaySlots {
		t.Fatalf("range %v, recovery %+v, operation %s; want the Slot replayed", slot.ExpiredRange != nil, slot.Recovery, slot.Dispatch.Operation)
	}
	if stages := stagesOf(observer.seen); stages[observability.StageReplayExpired] != 0 {
		t.Fatalf("reported %v for a Slot inside the window", stages)
	}
}
