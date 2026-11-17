// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// occupancyWorker is a ready Worker that reports a pool, a retained
// high-water over a full hour, and, when share is not zero, its share.
func occupancyWorker(id string, pool, high, share uint64, live time.Time) ownership.WorkerRegistration {
	worker := byteWorker(id, pool, live)
	worker.Load.RetainedHigh = &ownership.RetainedHighReading{Bytes: high, WindowSeconds: 3600}
	worker.Load.RetainedShareBytes = share
	return worker
}

// occupancyRig is a Leader term over a ledger: the term began long enough
// ago that byte moves are made, unless a case says otherwise.
type occupancyRig struct {
	now    time.Time
	ledger *CostLedger
}

func newOccupancyRig(t *testing.T, owners map[execution.QueryGroupIdentity]string, peaks map[execution.QueryGroupIdentity]uint64) *occupancyRig {
	t.Helper()
	rig := &occupancyRig{now: time.Unix(1_700_000_000, 0)}
	rig.ledger = NewCostLedger(func() time.Time { return rig.now })
	rig.report(owners, peaks)
	rig.ledger.Lead(1)
	records := map[execution.QueryGroupIdentity]ownership.AssignmentRecord{}
	for queryGroup, owner := range owners {
		records[queryGroup] = ownership.AssignmentRecord{QueryGroup: queryGroup, DesiredWorkerID: owner, AssignedAt: rig.now.Add(-3 * time.Hour)}
	}
	rig.ledger.Settle(records)
	rig.now = rig.now.Add(InFlightWindow + time.Minute)
	return rig
}

// report is each holder reporting its Query Groups' peaks itself.
func (rig *occupancyRig) report(owners map[execution.QueryGroupIdentity]string, peaks map[execution.QueryGroupIdentity]uint64) {
	byWorker := map[string][]QueryGroupCostReport{}
	for queryGroup, owner := range owners {
		if peak, read := peaks[queryGroup]; read {
			byWorker[owner] = append(byWorker[owner], QueryGroupCostReport{QueryGroup: queryGroup, RetainedBytesPeak: peak})
		}
	}
	for workerID, reports := range byWorker {
		rig.ledger.Record(workerID, reports)
	}
}

func groupsOf(prefix string, count int, owner string, peak uint64, owners map[execution.QueryGroupIdentity]string, peaks map[execution.QueryGroupIdentity]uint64) {
	for index := 0; index < count; index++ {
		queryGroup := execution.QueryGroupIdentity(fmt.Sprintf("%s-%04d", prefix, index))
		owners[queryGroup] = owner
		peaks[queryGroup] = peak
	}
}

// The incident shape is still relieved under occupancy: three Query Groups
// of 35% of the pool that ran together put the Worker's high-water at 105%,
// and the largest goes to the Worker with the most room.
func TestOccupancyStillRelievesAWorkerWhoseQueryGroupsRanTogether(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"qg-1": "a", "qg-2": "a", "qg-3": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"qg-1": 350, "qg-2": 350, "qg-3": 350}
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 1050, 0, live), occupancyWorker("b", 1000, 0, 0, live), occupancyWorker("c", 1000, 0, 0, live)}
	readings := rig.ledger.Readings(owners, workers)
	plan := NewRouter(nil).PlanByteMoves(owners, workers, readings, rig.now)
	if plan.Judgement != ByteJudgementOccupancy || plan.Held || plan.AllOver || plan.Sum["a"] != 1050 || plan.PeakSum["a"] != 1050 {
		t.Fatalf("plan = %+v, want a judged by occupancy at its high-water", plan)
	}
	if !reflect.DeepEqual(plan.Moves, []ByteMove{{QueryGroup: "qg-1", From: "a", To: "b", Bytes: 350}}) {
		t.Fatalf("moves = %+v, want the first of the largest to b", plan.Moves)
	}
}

// The freeze the sum of peaks made (014 C1 section 8.1): two Workers whose
// many small Query Groups' peaks sum far past the line while what they ever
// held at once is a quarter of the pool. By the sum nothing fits anywhere and
// the counts never even out; by occupancy both have room and the count
// correction moves. One Worker that reports no high-water puts the whole
// fleet back on the sum.
func TestOccupancyLetsTheCountCorrectionMoveWhereTheSumOfPeaksFroze(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{}
	peaks := map[execution.QueryGroupIdentity]uint64{}
	groupsOf("a", 1500, "a", 1, owners, peaks)
	groupsOf("b", 1000, "b", 1, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)

	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 290, 0, live), occupancyWorker("b", 1000, 268, 0, live)}
	readings := rig.ledger.Readings(owners, workers)
	if readings.Occupancy["a"] != 290 || readings.Occupancy["b"] != 268 || len(readings.NotReporting) != 0 {
		t.Fatalf("occupancy = %v (not reporting %v), want each Worker's high-water", readings.Occupancy, readings.NotReporting)
	}
	bytePlan := router.PlanByteMoves(owners, workers, readings, rig.now)
	if len(bytePlan.Overloaded) != 0 || bytePlan.PeakSum["a"] != 1500 {
		t.Fatalf("byte plan = %+v, want nobody over by occupancy whatever the sums", bytePlan)
	}
	plan := router.PlanRebalanceWithin(owners, workers, readings, rig.now, -1)
	if len(plan.Moves) != rebalanceBatch(2500) {
		t.Fatalf("count correction moved %d, want a full batch of %d", len(plan.Moves), rebalanceBatch(2500))
	}

	// b on the previous release: no high-water. The fleet is judged by the
	// sum whole and is frozen, as before - not a's occupancy against b's sum.
	mixed := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 290, 0, live), byteWorker("b", 1000, live)}
	readings = rig.ledger.Readings(owners, mixed)
	if readings.Occupancy != nil || !reflect.DeepEqual(readings.NotReporting, []string{"b"}) {
		t.Fatalf("mixed readings = occupancy %v, not reporting %v; want the sum, b named", readings.Occupancy, readings.NotReporting)
	}
	mixedBytes := router.PlanByteMoves(owners, mixed, readings, rig.now)
	if mixedBytes.Judgement != ByteJudgementPeakSum || mixedBytes.AllOver || len(mixedBytes.Moves) != 0 {
		t.Fatalf("mixed byte plan = %+v, want the sum, no all-over mode, nothing moved", mixedBytes)
	}
	if frozen := router.PlanRebalanceWithin(owners, mixed, readings, rig.now, -1); len(frozen.Moves) != 0 {
		t.Fatalf("mixed count correction moved %d, want the sum's freeze", len(frozen.Moves))
	}
}

// A Query Group past the share every other Worker reported is not moved -
// it would be refused wherever it went - and is named; a Worker that reports
// no share is not checked.
func TestAQueryGroupPastEveryOtherShareIsNamedNotMoved(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"big": "a", "small": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"big": 700, "small": 100}
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)

	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 900, 672, live), occupancyWorker("b", 1000, 0, 672, live), occupancyWorker("c", 1000, 0, 672, live)}
	plan := router.PlanByteMoves(owners, workers, rig.ledger.Readings(owners, workers), rig.now)
	if !reflect.DeepEqual(plan.OverShare, []execution.QueryGroupIdentity{"big"}) {
		t.Fatalf("over share = %v, want big named", plan.OverShare)
	}
	if !reflect.DeepEqual(plan.Moves, []ByteMove{{QueryGroup: "small", From: "a", To: "b", Bytes: 100}}) {
		t.Fatalf("moves = %+v, want the one that fits a share moved instead", plan.Moves)
	}

	unchecked := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 900, 672, live), occupancyWorker("b", 1000, 0, 672, live), occupancyWorker("c", 1000, 0, 0, live)}
	plan = router.PlanByteMoves(owners, unchecked, rig.ledger.Readings(owners, unchecked), rig.now)
	if len(plan.OverShare) != 0 || !reflect.DeepEqual(plan.Moves, []ByteMove{{QueryGroup: "big", From: "a", To: "c", Bytes: 700}}) {
		t.Fatalf("plan = %+v, want big to c, which reported no share", plan)
	}
}

// The Leader's account of what moved: a destination whose high-water has not
// seen what landed is judged with it, so the count correction does not go on
// filling it round after round; the source's high-water still holds what
// left, and is judged without it, so the byte constraint does not go on
// sending its largest off. Past the hour, and once the destination reports
// a Query Group itself, its high-water is what counts.
func TestTheInFlightAccountHoldsWhatTheHighWatersHaveNotSeen(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{}
	peaks := map[execution.QueryGroupIdentity]uint64{}
	groupsOf("a", 10, "a", 100, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(3 * time.Hour)
	router := NewRouter(nil)
	// b's pool is small: its line is 240, room for two.
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 900, 0, live), occupancyWorker("b", 300, 0, 0, live)}

	round := func() (BytePlan, RebalancePlan) {
		readings := rig.ledger.Readings(owners, workers)
		bytePlan := router.PlanByteMoves(owners, workers, readings, rig.now)
		moves := make([]RebalanceMove, 0, len(bytePlan.Moves))
		for _, move := range bytePlan.Moves {
			moves = append(moves, RebalanceMove{QueryGroup: move.QueryGroup, From: move.From, To: move.To})
			owners[move.QueryGroup] = move.To
		}
		plan := router.PlanRebalanceWithin(owners, workers, readings.Moved(moves), rig.now, -1)
		for _, move := range plan.Moves {
			owners[move.QueryGroup] = move.To
		}
		rig.ledger.Retain(owners)
		return bytePlan, plan
	}
	first, firstCounts := round()
	if len(first.Moves) != 1 || len(firstCounts.Moves) != 1 {
		t.Fatalf("round 1: byte moves %+v, count moves %+v; want one of each to b", first.Moves, firstCounts.Moves)
	}
	// The high-waters have not moved: a still reads 900, b still 0.
	rig.now = rig.now.Add(time.Minute)
	readings := rig.ledger.Readings(owners, workers)
	if readings.Occupancy["b"] != 200 || readings.Occupancy["a"] != 700 ||
		readings.Parts["b"].Arriving != 2 || readings.Parts["a"].Leaving != 2 {
		t.Fatalf("occupancy %v, parts %+v; want b at its two arrivals, a less its two departures", readings.Occupancy, readings.Parts)
	}
	second, secondCounts := round()
	if len(second.Overloaded) != 0 || len(second.Moves) != 0 || len(secondCounts.Moves) != 0 {
		t.Fatalf("round 2: byte plan %+v, count moves %+v; want a no longer over and nothing more onto b", second, secondCounts.Moves)
	}

	// The hour passes. b has not reported one of its two: it is still
	// landing, and said so; the other, reported, is b's high-water's.
	var reported, quiet execution.QueryGroupIdentity
	for queryGroup, owner := range owners {
		if owner == "b" && reported == "" {
			reported = queryGroup
		} else if owner == "b" {
			quiet = queryGroup
		}
	}
	rig.now = rig.now.Add(InFlightWindow)
	rig.ledger.Record("b", []QueryGroupCostReport{{QueryGroup: reported, RetainedBytesPeak: 100}})
	workers[1] = occupancyWorker("b", 300, 100, 0, live)
	rig.ledger.Retain(owners)
	readings = rig.ledger.Readings(owners, workers)
	if part := readings.Parts["b"]; part.Arriving != 1 || part.Waiting != 1 || readings.Occupancy["b"] != 200 || !readings.Arrived[quiet] || readings.Arrived[reported] {
		t.Fatalf("after the hour: parts %+v, occupancy %v, arrived %v; want the unreported one still landing", part, readings.Occupancy, readings.Arrived)
	}
	if part := readings.Parts["a"]; part.Leaving != 0 {
		t.Fatalf("after the hour a still takes off %+v", part)
	}
}

// While every Worker is over the line, a move is made only where the
// destination ends below what the source holds: the byte constraint takes
// the largest Query Group that does, the count correction moves only one
// that does, and nothing that has just moved moves again. The rounds end.
func TestWhileEveryWorkerIsOverOnlyRelievingMovesAreMade(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"a-300": "a", "a-200": "a", "a-100": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"a-300": 300, "a-200": 200, "a-100": 100}
	groupsOf("b", 10, "b", 10, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 1000, 0, live), occupancyWorker("b", 1000, 850, 0, live)}
	readings := rig.ledger.Readings(owners, workers)

	plan := router.PlanByteMoves(owners, workers, readings, rig.now)
	if !plan.AllOver {
		t.Fatalf("plan = %+v, want the all-over mode", plan)
	}
	// 850 + 300 and 850 + 200 do not end below 1000; 850 + 100 does.
	if len(plan.Moves) == 0 || plan.Moves[0] != (ByteMove{QueryGroup: "a-100", From: "a", To: "b", Bytes: 100}) {
		t.Fatalf("moves = %+v, want a-100 first: the largest that relieves", plan.Moves)
	}
	sums := map[string]uint64{"a": 1000, "b": 850}
	for _, move := range plan.Moves {
		if sums[move.To]+move.Bytes >= sums[move.From] {
			t.Fatalf("move %+v over %v does not relieve", move, sums)
		}
		sums[move.From] -= move.Bytes
		sums[move.To] += move.Bytes
	}

	// The count correction: a holds 3, b 10. The most owned, b, is the less
	// occupied: a move to a would make a heavier still, and is not made.
	counts := router.PlanRebalanceWithin(owners, workers, readings, rig.now, -1)
	if len(counts.Moves) != 0 {
		t.Fatalf("count moves = %+v, want none: a is the heavier", counts.Moves)
	}
	// Turned round, the most owned is the heavier, and a move relieves it.
	turned := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 850, 0, live), occupancyWorker("b", 1000, 1000, 0, live)}
	if moved := router.PlanRebalanceWithin(owners, turned, rig.ledger.Readings(owners, turned), rig.now, -1); len(moved.Moves) != 1 {
		t.Fatalf("count moves = %+v, want one from b, the heavier, to a", moved.Moves)
	}

	// Rounds over the readings the moves leave: each QG moves at most once,
	// and the rounds stop.
	movedOnce := map[execution.QueryGroupIdentity]int{}
	current := readings
	for index := 0; index < 20; index++ {
		step := router.PlanByteMoves(owners, workers, current, rig.now)
		if len(step.Moves) == 0 {
			return
		}
		moves := make([]RebalanceMove, 0, len(step.Moves))
		for _, move := range step.Moves {
			movedOnce[move.QueryGroup]++
			if movedOnce[move.QueryGroup] > 1 {
				t.Fatalf("round %d moved %s again", index, move.QueryGroup)
			}
			owners[move.QueryGroup] = move.To
			moves = append(moves, RebalanceMove{QueryGroup: move.QueryGroup, From: move.From, To: move.To})
		}
		current = current.Moved(moves)
	}
	t.Fatal("the relieving moves did not end in twenty rounds")
}

// A new term: the arrivals are read from the records - assigned within the
// hour, or not yet reported by the holder - and for the term's first hour no
// byte move is made and the all-over mode is off; the departures before the
// term are not known to it. Another term starts the account over.
func TestANewTermRebuildsTheArrivalsAndHoldsByteMovesForAnHour(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	ledger := NewCostLedger(func() time.Time { return now })
	owners := map[execution.QueryGroupIdentity]string{"recent": "b", "old": "a", "quiet": "a", "big": "a"}
	ledger.Record("a", []QueryGroupCostReport{{QueryGroup: "old", RetainedBytesPeak: 50}, {QueryGroup: "big", RetainedBytesPeak: 300}})
	ledger.Record("b", []QueryGroupCostReport{{QueryGroup: "recent", RetainedBytesPeak: 100}})
	ledger.Lead(7)
	ledger.Settle(map[execution.QueryGroupIdentity]ownership.AssignmentRecord{
		"recent": {QueryGroup: "recent", DesiredWorkerID: "b", AssignedAt: now.Add(-10 * time.Minute)},
		"old":    {QueryGroup: "old", DesiredWorkerID: "a", AssignedAt: now.Add(-3 * time.Hour)},
		"quiet":  {QueryGroup: "quiet", DesiredWorkerID: "a", AssignedAt: now.Add(-3 * time.Hour)},
		"big":    {QueryGroup: "big", DesiredWorkerID: "a", AssignedAt: now.Add(-3 * time.Hour)},
	})
	live := now.Add(3 * time.Hour)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 900, 0, live), occupancyWorker("b", 1000, 850, 0, live)}
	readings := ledger.Readings(owners, workers)
	if !readings.HoldByteMoves || !readings.Arrived["recent"] || !readings.Arrived["quiet"] || readings.Arrived["old"] || readings.Arrived["big"] {
		t.Fatalf("readings hold %v, arrived %v; want held, recent and quiet landing", readings.HoldByteMoves, readings.Arrived)
	}
	if part := readings.Parts["a"]; part.Arriving != 1 || part.Waiting != 1 || !reflect.DeepEqual(part.WaitingSample, []execution.QueryGroupIdentity{"quiet"}) {
		t.Fatalf("a's parts %+v, want quiet landing past the hour, unreported, and named", part)
	}
	plan := NewRouter(nil).PlanByteMoves(owners, workers, readings, now)
	if !plan.Held || plan.AllOver || len(plan.Moves) != 0 || len(plan.Overloaded) != 2 {
		t.Fatalf("plan = %+v, want both over, held, no all-over mode, nothing moved", plan)
	}

	now = now.Add(InFlightWindow)
	ledger.Lead(7)
	plan = NewRouter(nil).PlanByteMoves(owners, workers, ledger.Readings(owners, workers), now)
	if plan.Held || !plan.AllOver {
		t.Fatalf("an hour into the term: plan = %+v, want moves made and the all-over mode on", plan)
	}

	ledger.Lead(8)
	if readings := ledger.Readings(owners, workers); !readings.HoldByteMoves || len(readings.Arrived) != 0 {
		t.Fatalf("a new term: hold %v, arrived %v; want held and nothing known until its records are read", readings.HoldByteMoves, readings.Arrived)
	}
}

// A Worker that leaves while every Worker is over the line still has every
// Query Group it held placed: a placement asks only whether a Query Group
// fits, and falls back to rendezvous, reported unfit, when it fits nowhere
// (014 C1 section 8.3). The relieving criterion is the moves', not a
// placement's: its source has gone.
func TestAWorkerLeavingWhileEveryWorkerIsOverHasEveryQueryGroupPlaced(t *testing.T) {
	workers := projectionWorkers("w-1", "w-2", "w-3")
	records := map[execution.QueryGroupIdentity]ownership.AssignmentRecord{}
	queryGroups := []execution.QueryGroupIdentity{}
	for index := 0; index < 6; index++ {
		queryGroup := execution.QueryGroupIdentity(fmt.Sprintf("leaving-%d", index))
		queryGroups = append(queryGroups, queryGroup)
		records[queryGroup] = ownership.AssignmentRecord{QueryGroup: queryGroup, DesiredWorkerID: "gone", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: rolloutNow.Add(-2 * time.Hour)}
	}
	for index := range workers {
		workers[index].Load = &ownership.WorkerLoad{RetainedPoolBytes: projectionPool,
			RetainedHigh: &ownership.RetainedHighReading{Bytes: 900, WindowSeconds: 3600}}
	}
	store := &countingAssignmentStore{workers: workers, records: records}
	reconciler := newRolloutReconciler(t, store)
	readings := func(map[execution.QueryGroupIdentity]string) ByteReadings {
		peaks := map[execution.QueryGroupIdentity]uint64{}
		for _, queryGroup := range queryGroups {
			peaks[queryGroup] = 100
		}
		reading := projectionReadings(peaks, "w-1", "w-2", "w-3")
		reading.Occupancy = map[string]uint64{"w-1": 900, "w-2": 900, "w-3": 900}
		return reading
	}
	if projection := newByteProjection(nil, readings(nil)); !projection.allOver {
		t.Fatal("the case needs every Worker over the line")
	}
	settlement, err := reconciler.ReconcileRoundSettlingWithBytes(context.Background(), ownership.PublicationAuthority{},
		queryGroups, workers, rolloutNow, ContentScopes{}, readings)
	if err != nil {
		t.Fatal(err)
	}
	for _, queryGroup := range queryGroups {
		if holder := settlement.Records[queryGroup].DesiredWorkerID; holder == "" || holder == "gone" {
			t.Fatalf("%s held by %q after the round, want a ready Worker", queryGroup, holder)
		}
	}
	if settlement.Unfit != len(queryGroups) {
		t.Fatalf("unfit = %d, want every placement reported unfit", settlement.Unfit)
	}
}

// The count correction plans after the round's byte moves and judges a
// destination with what they just put on it: a Worker whose room the byte
// move took is not filled past its line by the correction that follows.
func TestTheCountCorrectionJudgesTheByteMovesItFollows(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{}
	peaks := map[execution.QueryGroupIdentity]uint64{}
	groupsOf("a", 10, "a", 100, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)
	// b's line is 160: room for one.
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 900, 0, live), occupancyWorker("b", 200, 0, 0, live)}
	readings := rig.ledger.Readings(owners, workers)
	bytePlan := router.PlanByteMoves(owners, workers, readings, rig.now)
	if len(bytePlan.Moves) != 1 || bytePlan.Moves[0].To != "b" {
		t.Fatalf("byte moves = %+v, want one to b", bytePlan.Moves)
	}
	moved := []RebalanceMove{{QueryGroup: bytePlan.Moves[0].QueryGroup, From: "a", To: "b"}}
	owners[moved[0].QueryGroup] = "b"
	after := readings.Moved(moved)
	if after.Occupancy["b"] != 100 || after.Occupancy["a"] != 800 || !after.Arrived[moved[0].QueryGroup] {
		t.Fatalf("after the byte move: occupancy %v, arrived %v", after.Occupancy, after.Arrived)
	}
	if plan := router.PlanRebalanceWithin(owners, workers, after, rig.now, -1); len(plan.Moves) != 0 {
		t.Fatalf("count moves = %+v, want none: b has no room left after the byte move", plan.Moves)
	}
	if readings.Occupancy["b"] != 0 {
		t.Fatalf("Moved changed the readings it was given: %v", readings.Occupancy)
	}
}

// A departure is taken off only while the source's window can still hold
// it: a source that restarted since has a high-water that never saw the
// Query Group, and taking it off would read the source emptier than it is.
func TestADepartureTheSourcesWindowNoLongerHoldsIsNotTakenOff(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"left": "a", "stays": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"left": 300, "stays": 100}
	rig := newOccupancyRig(t, owners, peaks)
	owners["left"] = "b"
	rig.ledger.Retain(owners)
	rig.now = rig.now.Add(20 * time.Minute)
	live := rig.now.Add(time.Minute)
	held := occupancyWorker("a", 1000, 500, 0, live)
	restarted := occupancyWorker("a", 1000, 500, 0, live)
	restarted.Load.RetainedHigh.WindowSeconds = 600
	b := occupancyWorker("b", 1000, 0, 0, live)
	if got := rig.ledger.Readings(owners, []ownership.WorkerRegistration{held, b}).Occupancy["a"]; got != 200 {
		t.Fatalf("a's window spans the departure: occupancy %d, want 500 less 300", got)
	}
	if got := rig.ledger.Readings(owners, []ownership.WorkerRegistration{restarted, b}).Occupancy["a"]; got != 500 {
		t.Fatalf("a restarted ten minutes ago: occupancy %d, want its high-water whole", got)
	}
}

// The relieving criterion is strict: a move that leaves the destination
// level with the source changes nothing and could be undone by the next.
func TestAMoveThatLeavesTheDestinationLevelWithTheSourceIsNotMade(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"a-100": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"a-100": 100}
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 1000, 0, live), occupancyWorker("b", 1000, 900, 0, live)}
	plan := NewRouter(nil).PlanByteMoves(owners, workers, rig.ledger.Readings(owners, workers), rig.now)
	if !plan.AllOver || len(plan.Moves) != 0 {
		t.Fatalf("plan = %+v, want the all-over mode and no move: 900 + 100 is not below 1000", plan)
	}
}

// The count correction, while every Worker is over the line, leaves a Query
// Group that is still landing where it is, and takes what it moves off the
// source as it goes: the source's occupancy falls with each move, and the
// moves stop once the next would no longer relieve it.
func TestTheCountCorrectionRelievesStepByStepAndLeavesWhatIsLanding(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{}
	peaks := map[execution.QueryGroupIdentity]uint64{}
	groupsOf("a", 3, "a", 10, owners, peaks)
	groupsOf("b", 40, "b", 10, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 970, 0, live), occupancyWorker("b", 1000, 1000, 0, live)}
	readings := rig.ledger.Readings(owners, workers)
	// 970 + 10 < 1000 relieves; then a holds 980 and b 990, and 980 + 10
	// is not below 990.
	if plan := router.PlanRebalanceWithin(owners, workers, readings, rig.now, -1); len(plan.Moves) != 1 {
		t.Fatalf("count moves = %+v, want one: the second would leave a level with b", plan.Moves)
	}
	landing := readings
	landing.Arrived = map[execution.QueryGroupIdentity]bool{}
	for queryGroup, owner := range owners {
		if owner == "b" {
			landing.Arrived[queryGroup] = true
		}
	}
	if plan := router.PlanRebalanceWithin(owners, workers, landing, rig.now, -1); len(plan.Moves) != 0 {
		t.Fatalf("count moves = %+v, want none: every one of b's is still landing", plan.Moves)
	}
}

// While every Worker is over the line, the byte step leaves a Query Group
// that is still landing where it is, even when moving it would relieve: the
// high-waters have not seen it settle, and it would be the next to move
// back once they do.
func TestTheByteStepLeavesALandingQueryGroupWhileEveryWorkerIsOver(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	live := now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 0, 0, live), occupancyWorker("b", 1000, 0, 0, live)}
	owners := map[execution.QueryGroupIdentity]string{"a-1": "a", "landed": "b", "b-1": "b"}
	readings := ByteReadings{Pool: map[string]uint64{"a": 1000, "b": 1000},
		Peak:      map[execution.QueryGroupIdentity]uint64{"a-1": 10, "landed": 100, "b-1": 10},
		Occupancy: map[string]uint64{"a": 850, "b": 1000},
		Arrived:   map[execution.QueryGroupIdentity]bool{"landed": true}}
	plan := NewRouter(nil).PlanByteMoves(owners, workers, readings, now)
	if !plan.AllOver || !reflect.DeepEqual(plan.Moves, []ByteMove{{QueryGroup: "b-1", From: "b", To: "a", Bytes: 10}}) {
		t.Fatalf("plan = %+v, want b-1 moved and the landing one left: 850 + 100 would relieve, but it is still landing", plan)
	}
}

// A round reads its own changes before they are recorded: a Query Group
// the settlement took off a live holder this round is counted on its new
// holder and off the old one at once, so the byte step that follows does
// not read the old holder as still over.
func TestARoundsOwnChangesAreReadBeforeTheyAreRecorded(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{"moved": "a", "stays": "a"}
	peaks := map[execution.QueryGroupIdentity]uint64{"moved": 300, "stays": 100}
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 850, 0, live), occupancyWorker("b", 1000, 0, 0, live)}
	owners["moved"] = "b"
	readings := rig.ledger.Readings(owners, workers)
	if readings.Occupancy["a"] != 550 || readings.Occupancy["b"] != 300 || !readings.Arrived["moved"] {
		t.Fatalf("occupancy %v, arrived %v; want the round's change on both sides before Retain", readings.Occupancy, readings.Arrived)
	}
	if plan := NewRouter(nil).PlanByteMoves(owners, workers, readings, rig.now); len(plan.Overloaded) != 0 {
		t.Fatalf("plan = %+v, want a no longer over", plan)
	}
}

// The catch-up after a rollout (014 C1 section 8.5): the first replica up
// runs every Query Group's overdue Slots at once and its high-water holds
// that peak for the hour. Below the line it is not handled apart: the
// replica is not over and gives Query Groups to the next one by the counts
// as usual, and is no destination for one that would take it past the line
// until the hour has passed.
func TestACatchUpBelowTheLineIsCarriedForTheHourAndHandledByNothingElse(t *testing.T) {
	owners := map[execution.QueryGroupIdentity]string{}
	peaks := map[execution.QueryGroupIdentity]uint64{}
	groupsOf("first", 40, "first", 10, owners, peaks)
	rig := newOccupancyRig(t, owners, peaks)
	live := rig.now.Add(time.Minute)
	router := NewRouter(nil)
	// 93% of the line of 800.
	workers := []ownership.WorkerRegistration{occupancyWorker("first", 1000, 744, 0, live), occupancyWorker("second", 1000, 0, 0, live)}
	readings := rig.ledger.Readings(owners, workers)
	bytePlan := router.PlanByteMoves(owners, workers, readings, rig.now)
	if len(bytePlan.Overloaded) != 0 || len(bytePlan.Moves) != 0 || bytePlan.AllOver {
		t.Fatalf("byte plan = %+v, want nothing over and nothing moved for the catch-up", bytePlan)
	}
	plan := router.PlanRebalanceWithin(owners, workers, readings, rig.now, -1)
	if len(plan.Moves) != rebalanceBatch(40) {
		t.Fatalf("count moves = %d, want a full batch from the first replica to the second", len(plan.Moves))
	}
	readings.Peak["big"] = 60
	if projection := newByteProjection(owners, readings); projection.fits("first", "big") || !projection.fits("second", "big") {
		t.Fatal("want a Query Group of 60 to fit the second replica and not the first: 744 + 60 is past its line of 800")
	}
}

// Under occupancy a Query Group's peak can be more than its source's
// occupancy - a long-period one the window did not see - and the source is
// left empty by the move, not wrapped round to full: a later move in the
// round still finds the room it has.
func TestASourceWhosePeakIsPastItsOccupancyIsLeftEmpty(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	live := now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{occupancyWorker("a", 1000, 0, 0, live), occupancyWorker("b", 2000, 0, 0, live), occupancyWorker("c", 1000, 0, 0, live)}
	owners := map[execution.QueryGroupIdentity]string{"big": "a", "small": "c"}
	readings := ByteReadings{Pool: map[string]uint64{"a": 1000, "b": 2000, "c": 1000},
		Peak:      map[execution.QueryGroupIdentity]uint64{"big": 1000, "small": 50},
		Occupancy: map[string]uint64{"a": 900, "b": 0, "c": 900}}
	plan := NewRouter(nil).PlanByteMoves(owners, workers, readings, now)
	want := []ByteMove{{QueryGroup: "big", From: "a", To: "b", Bytes: 1000}, {QueryGroup: "small", From: "c", To: "a", Bytes: 50}}
	if !reflect.DeepEqual(plan.Moves, want) {
		t.Fatalf("moves = %+v, want big to b and then small to a, left empty with the most room", plan.Moves)
	}
}

// The landings counted only because their holder has not reported them are
// named, a few of them, lowest first: which ones, if their count grows.
func TestTheWaitingLandingsAreNamed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	ledger := NewCostLedger(func() time.Time { return now })
	owners := map[execution.QueryGroupIdentity]string{}
	records := map[execution.QueryGroupIdentity]ownership.AssignmentRecord{}
	for index := 7; index > 0; index-- {
		queryGroup := execution.QueryGroupIdentity(fmt.Sprintf("quiet-%d", index))
		owners[queryGroup] = "a"
		records[queryGroup] = ownership.AssignmentRecord{QueryGroup: queryGroup, DesiredWorkerID: "a", AssignedAt: now.Add(-3 * time.Hour)}
	}
	ledger.Lead(1)
	ledger.Settle(records)
	part := ledger.Readings(owners, []ownership.WorkerRegistration{occupancyWorker("a", 1000, 0, 0, now.Add(time.Minute))}).Parts["a"]
	if part.Waiting != 7 || !reflect.DeepEqual(part.WaitingSample, []execution.QueryGroupIdentity{"quiet-1", "quiet-2", "quiet-3", "quiet-4", "quiet-5"}) {
		t.Fatalf("parts = %+v, want seven waiting and the five lowest named", part)
	}
}
