// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// Retained bytes are a capacity constraint on placement, not a balance
// target (decision-020 section 5.7). Each Worker holds one retained-byte
// pool, and each Query Group it runs peaks somewhere inside it once a
// round; the budget keeps every Query Group inside its own share, and
// nobody kept the sum. Three Query Groups of one strategy landed on one
// replica at 175-404 MB each, every one inside its share, and together
// filled a 1 GiB pool for thirteen refusals every ten minutes. Placement
// by count is blind to it and never corrects it: counts do not change when
// a replica's bytes fill.
//
// The rule: a ready Worker whose Query Groups' peaks sum past this share of
// its pool is overloaded, and the round moves its largest Query Group to
// the ready Worker with the most headroom, one move per overloaded Worker
// per round, over the same handover a count rebalance uses. This is a
// feasibility move, not a balancing one: it is not subject to the count
// rebalance's stop spread, and the count rebalance is kept from undoing it
// by the same reading (PlanRebalanceWithBytes).
//
// The constraint is judged only where the numbers are known: a Worker that
// registered no pool is not judged and is listed as such, and a Query
// Group with no reported peak counts nothing toward its Worker's sum and is
// counted as unread. An unknown is reported, never read as "no pressure" -
// on either side of a move. A Worker with an unread Query Group has a sum
// that is a lower bound: enough to say it is overloaded when its known part
// already is, and as a destination each of its unread Query Groups is
// counted at the ninetieth percentile of the peaks read this round.
//
// Unread is not a passing state. A Query Group has a peak only after it
// has run a Slot in the process that reports it, so after a restart every
// Query Group on an hourly to sixty-hour cadence stays unread until its
// first round, and the Leader that restarted with it holds no earlier
// reading. Refusing any Worker with one unread Query Group as a
// destination refused every Worker for as long as the longest cadence, and
// an overloaded Worker was never relieved - the state the constraint
// exists for. The long unread tail is drawn from the same Query Groups the
// read peaks describe, and a destination the estimate undercounts goes past
// the share by at most what that Query Group exceeds it, is judged
// overloaded on its next reading and moves its largest Query Group on;
// the retained-byte budget still refuses at the pool. With no peak read at
// all there is nothing to estimate from, and such a Worker is not a
// destination. The ledger still carries a moved Query Group's last reading
// over to its new holder as provisional, so the Worker a move just landed
// on is not the emptiest Worker in the fleet by the next round.
const byteConstraintPercent = 80

// unreadEstimatePercentile is the percentile of the round's read peaks an
// unread Query Group is counted at on a destination; see above.
const unreadEstimatePercentile = 90

// unreadSampleLimit bounds the unread Query Groups named per Worker.
const unreadSampleLimit = 5

// PeakDistribution is the read peaks of one round: how many, and the median,
// the ninetieth and ninety-ninth percentiles and the largest, by the same
// nearest-rank rule as the estimate.
type PeakDistribution struct {
	Count, P50, P90, P99, Max uint64
}

// peakDistribution is the distribution of peaks; zero when there are none.
func peakDistribution(peaks []uint64) PeakDistribution {
	if len(peaks) == 0 {
		return PeakDistribution{}
	}
	sorted := append([]uint64(nil), peaks...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	rank := func(percentile int) uint64 { return sorted[(len(sorted)*percentile+99)/100-1] }
	return PeakDistribution{Count: uint64(len(sorted)), P50: rank(50), P90: rank(90), P99: rank(99), Max: sorted[len(sorted)-1]}
}

// ByteConstraintPercent is the share above, for a reader that reports a
// plan.
const ByteConstraintPercent = byteConstraintPercent

// ByteReadings is what the byte constraint is judged from: each ready
// Worker's retained-byte pool as it registered it, and each Query Group's
// largest per-Slot retained bytes as the Worker that runs it last reported
// on its heartbeat. A Worker absent from Pool is not judged; a Query Group
// absent from Peak has no reading.
type ByteReadings struct {
	Pool map[string]uint64
	Peak map[execution.QueryGroupIdentity]uint64
	// Occupancy is each judged Worker's concurrent occupancy as the Leader
	// accounts it (014 C1 section 8.3): its reported high-water and the peaks
	// its window may not have seen, plus the Query Groups landing on it and
	// less the ones that left it. Set only when every judged Worker reported
	// a high-water; nil, and the constraint judges each Worker by the sum of
	// its Query Groups' peaks as before - a fleet of mixed readings is judged
	// by the older one whole, or the moves would only ever go one way.
	Occupancy map[string]uint64
	// Share is the most one Query Group may hold on each Worker, as the
	// Worker reported it; a Worker absent from it is not checked.
	Share map[string]uint64
	// HoldByteMoves is a new Leader's first hour: the Query Groups that left
	// a Worker before it began are not known to it, so a Worker's occupancy
	// may still carry them - no byte move and no all-over move is made.
	// TermAge is how long the term has been led.
	HoldByteMoves bool
	TermAge       time.Duration
	// Parts is what each Worker's occupancy was made of; Arrived the Query
	// Groups counted as landing, which stay where they are while every
	// Worker is over the line - the high-water has not seen them yet, and a
	// move judged on it could take one straight back. NotReporting names the
	// ready Workers with a pool that reported no high-water, the reason the
	// fleet was judged by the sum of peaks.
	Parts        map[string]OccupancyParts
	Arrived      map[execution.QueryGroupIdentity]bool
	NotReporting []string
}

// Moved is the readings after the moves the round has published: each
// Query Group's peak counted on the Worker it went to and taken off the one
// it left, for the planning that follows within the round. Under the sum of
// peaks the owners say where each peak is, and the readings are unchanged.
func (readings ByteReadings) Moved(moves []RebalanceMove) ByteReadings {
	if readings.Occupancy == nil || len(moves) == 0 {
		return readings
	}
	occupancy := make(map[string]uint64, len(readings.Occupancy))
	for workerID, held := range readings.Occupancy {
		occupancy[workerID] = held
	}
	arrived := make(map[execution.QueryGroupIdentity]bool, len(readings.Arrived)+len(moves))
	for queryGroup := range readings.Arrived {
		arrived[queryGroup] = true
	}
	for _, move := range moves {
		peak := readings.Peak[move.QueryGroup]
		if _, judged := occupancy[move.To]; judged {
			occupancy[move.To] += peak
		}
		if held, judged := occupancy[move.From]; judged {
			occupancy[move.From] = held - min(held, peak)
		}
		arrived[move.QueryGroup] = true
	}
	readings.Occupancy, readings.Arrived = occupancy, arrived
	return readings
}

// ByteJudgement is how a round judges the constraint: occupancy, by the
// Workers' concurrent occupancy; peak_sum, by each Worker's sum of peaks.
const (
	ByteJudgementOccupancy = "occupancy"
	ByteJudgementPeakSum   = "peak_sum"
)

// held is a new Leader's first hour under occupancy. Under the sum of peaks
// nothing that left a Worker is in its sum, and nothing is held.
func (readings ByteReadings) held() bool {
	return readings.Occupancy != nil && readings.HoldByteMoves
}

func (readings ByteReadings) judgement() string {
	if readings.Occupancy != nil {
		return ByteJudgementOccupancy
	}
	return ByteJudgementPeakSum
}

// shareFits is whether a Query Group's peak is within the Worker's reported
// share: one past it is refused there whatever is free, and moving it only
// moves the refusal.
func (readings ByteReadings) shareFits(workerID string, peak uint64) bool {
	share, known := readings.Share[workerID]
	return !known || share == 0 || peak <= share
}

func (readings ByteReadings) pool(workerID string) (uint64, bool) {
	if readings.Pool == nil {
		return 0, false
	}
	pool, known := readings.Pool[workerID]
	return pool, known && pool > 0
}

func (readings ByteReadings) peak(queryGroup execution.QueryGroupIdentity) (uint64, bool) {
	if readings.Peak == nil {
		return 0, false
	}
	peak, known := readings.Peak[queryGroup]
	return peak, known
}

// byteLimit is the share of a pool the constraint allows.
func byteLimit(pool uint64) uint64 {
	return pool / 100 * byteConstraintPercent
}

// ByteMove names one Query Group a round moves for the byte constraint,
// with the peak it was judged by.
type ByteMove struct {
	QueryGroup execution.QueryGroupIdentity
	From       string
	To         string
	Bytes      uint64
}

// BytePlan describes one round's byte-constraint planning over the desired
// owners the Control Leader just reconciled. Sum is each judged Worker's
// sum of peaks before the moves.
type BytePlan struct {
	// Judged is how many ready Workers registered a pool; PoolUnknown names
	// the ready Workers that did not, and are not judged.
	Judged      int
	PoolUnknown []string
	// Unread is how many Query Groups on judged Workers have no reported
	// peak. They count nothing toward their Worker's sum, and Unsettled
	// names the judged Workers holding one: their sums are lower bounds. As
	// a destination such a Worker is judged with each unread Query Group at
	// UnreadEstimate, and is not one only when that is zero.
	Unread    int
	Unsettled []string
	// UnreadEstimate is what each unread Query Group on a destination was
	// counted at: the ninetieth percentile of the peaks read this round.
	// Zero when no peak was read, and then a Worker holding an unread Query
	// Group is not a destination.
	UnreadEstimate uint64
	// UnreadBy is Unread per judged Worker, and UnreadSample up to
	// unreadSampleLimit of each Worker's unread Query Groups, lowest first.
	// The total alone said every Worker was unsettled and not which Query
	// Groups kept it so -- whether they were ones that had not run since a
	// restart or ones that never report -- and that was what decided the fix.
	UnreadBy     map[string]int
	UnreadSample map[string][]execution.QueryGroupIdentity
	// ReadPeaks is the distribution the estimate was taken from.
	ReadPeaks PeakDistribution
	Sum       map[string]uint64
	// Overloaded names the judged Workers over the constraint before the
	// moves; Unplaceable the ones among them the round found no move for -
	// no Query Group of theirs with a reading fits any other judged Worker.
	Overloaded  []string
	Unplaceable []string
	Moves       []ByteMove
	// MostLoadedShare is the largest judged Worker's sum of peaks over its
	// limit (the share of its pool the constraint allows), before the moves:
	// above one is overloaded. Zero when no Worker is judged.
	MostLoadedShare float64
	// Judgement is what Sum is: occupancy or peak_sum. AllOver is every
	// judged Worker over the line under occupancy, when a move is made only
	// where the destination ends below the source's occupancy; Held is a
	// new Leader's first hour, no byte move made. OverShare names the Query
	// Groups an overloaded Worker could not move for being past every other
	// Worker's share: theirs to split, not to move.
	Judgement string
	AllOver   bool
	Held      bool
	OverShare []execution.QueryGroupIdentity
	// Parts, NotReporting and TermAge are the readings' own, for the facts.
	Parts        map[string]OccupancyParts
	NotReporting []string
	TermAge      time.Duration
	// PeakSum is each judged Worker's sum of peaks, whatever the judgement:
	// what Sum was before occupancy, kept for the comparison.
	PeakSum map[string]uint64
}

// PlanByteMoves computes the byte-constraint moves of one round. Only
// ready, unexpired Workers take part, as in PlanRebalance; the Workers are
// judged in identity order, and for each overloaded one the round takes
// its Query Groups largest first and lands the first that fits the judged
// Worker with the most headroom left - largest first so one move relieves
// the most, first that fits so a Query Group too large for anywhere does
// not leave the Worker stuck behind it. Headroom is kept as the round
// moves, so two overloaded Workers do not both fill the same destination.
// Ties resolve by identity so the plan is deterministic.
func (router *Router) PlanByteMoves(
	owners map[execution.QueryGroupIdentity]string,
	workers []ownership.WorkerRegistration,
	readings ByteReadings,
	at time.Time,
) BytePlan {
	plan := BytePlan{Sum: map[string]uint64{}, Judgement: readings.judgement(), Held: readings.held(),
		Parts: readings.Parts, NotReporting: readings.NotReporting, TermAge: readings.TermAge, PeakSum: map[string]uint64{}}
	if router == nil || at.IsZero() {
		return plan
	}
	ready := make(map[string]ownership.WorkerRegistration, len(workers))
	judged := make([]string, 0, len(workers))
	for _, worker := range workers {
		if worker.Validate() != nil || worker.AssignmentReadiness != ownership.WorkerReady || !worker.ExpiresAt.After(at) {
			continue
		}
		if _, duplicate := ready[worker.WorkerID]; duplicate {
			continue
		}
		ready[worker.WorkerID] = worker
		if _, known := readings.pool(worker.WorkerID); !known {
			plan.PoolUnknown = append(plan.PoolUnknown, worker.WorkerID)
			continue
		}
		judged = append(judged, worker.WorkerID)
		plan.Sum[worker.WorkerID] = 0
		plan.PeakSum[worker.WorkerID] = 0
	}
	sort.Strings(plan.PoolUnknown)
	sort.Strings(judged)
	plan.Judged = len(judged)
	owned := make(map[string][]execution.QueryGroupIdentity, len(judged))
	unreadBy := make(map[string]int, len(judged))
	unreadGroups := make(map[string][]execution.QueryGroupIdentity, len(judged))
	readPeaks := make([]uint64, 0, len(owners))
	for queryGroup, owner := range owners {
		if _, isJudged := plan.Sum[owner]; !isJudged {
			continue
		}
		peak, read := readings.peak(queryGroup)
		if !read {
			plan.Unread++
			unreadBy[owner]++
			unreadGroups[owner] = append(unreadGroups[owner], queryGroup)
			continue
		}
		plan.PeakSum[owner] += peak
		if readings.Occupancy == nil {
			plan.Sum[owner] += peak
		}
		owned[owner] = append(owned[owner], queryGroup)
		readPeaks = append(readPeaks, peak)
	}
	if readings.Occupancy != nil {
		// Concurrent occupancy: what the Worker held at once, and the
		// Query Groups landing on it or gone from it. A Query Group with no
		// reading is in its high-water already or among the ones landing,
		// and adds nothing more.
		for workerID := range plan.Sum {
			plan.Sum[workerID] = readings.Occupancy[workerID]
		}
		unreadBy = map[string]int{}
	}
	plan.UnreadEstimate = unreadEstimate(readPeaks)
	plan.ReadPeaks = peakDistribution(readPeaks)
	plan.UnreadBy = unreadBy
	plan.UnreadSample = make(map[string][]execution.QueryGroupIdentity, len(unreadGroups))
	for workerID, groups := range unreadGroups {
		sort.Slice(groups, func(left, right int) bool { return groups[left] < groups[right] })
		plan.UnreadSample[workerID] = groups[:min(len(groups), unreadSampleLimit)]
	}
	for _, workerID := range judged {
		if unreadBy[workerID] > 0 {
			plan.Unsettled = append(plan.Unsettled, workerID)
		}
	}
	if len(judged) < 2 {
		for _, workerID := range judged {
			if pool, _ := readings.pool(workerID); plan.Sum[workerID] > byteLimit(pool) {
				plan.Overloaded = append(plan.Overloaded, workerID)
				plan.Unplaceable = append(plan.Unplaceable, workerID)
			}
		}
		return plan
	}
	sum := make(map[string]uint64, len(plan.Sum))
	for workerID, total := range plan.Sum {
		sum[workerID] = total
	}
	plan.AllOver = readings.Occupancy != nil && !plan.Held
	for _, workerID := range judged {
		pool, _ := readings.pool(workerID)
		plan.AllOver = plan.AllOver && sum[workerID] > byteLimit(pool)
	}
	for _, workerID := range judged {
		pool, _ := readings.pool(workerID)
		if limit := byteLimit(pool); limit > 0 {
			plan.MostLoadedShare = max(plan.MostLoadedShare, float64(sum[workerID])/float64(limit))
		}
		if sum[workerID] <= byteLimit(pool) {
			continue
		}
		plan.Overloaded = append(plan.Overloaded, workerID)
		candidates := owned[workerID]
		sort.Slice(candidates, func(left, right int) bool {
			leftPeak, rightPeak := readings.Peak[candidates[left]], readings.Peak[candidates[right]]
			if leftPeak != rightPeak {
				return leftPeak > rightPeak
			}
			return candidates[left] < candidates[right]
		})
		if plan.Held {
			continue
		}
		moved := false
		for _, queryGroup := range candidates {
			peak := readings.Peak[queryGroup]
			if peak == 0 {
				break
			}
			if !router.shareFitsAnywhere(workerID, judged, readings, peak) {
				plan.OverShare = append(plan.OverShare, queryGroup)
				continue
			}
			destination := router.byteDestination(queryGroup, workerID, judged, ready, readings, sum, unreadBy, plan.UnreadEstimate, peak, at)
			if destination == "" && plan.AllOver && !readings.Arrived[queryGroup] {
				destination = router.relievingDestination(queryGroup, workerID, judged, ready, readings, sum, peak, at)
			}
			if destination == "" {
				continue
			}
			plan.Moves = append(plan.Moves, ByteMove{QueryGroup: queryGroup, From: workerID, To: destination, Bytes: peak})
			// Under occupancy a Query Group's peak may be more than the
			// source's occupancy - a long-period one its window did not see -
			// and the source is then empty, not wrapped round.
			sum[workerID] -= min(peak, sum[workerID])
			sum[destination] += peak
			moved = true
			break
		}
		if !moved {
			plan.Unplaceable = append(plan.Unplaceable, workerID)
		}
	}
	return plan
}

// byteDestination is the judged Worker with the most headroom that the
// Query Group fits and is eligible for, or "" when there is none. A Worker
// with an unread Query Group is judged by its projection, each unread one at
// the estimate (projectedSum); with no estimate it is not one, its sum only
// a lower bound and the room it appears to have perhaps exactly what it does
// not.
func (router *Router) byteDestination(
	queryGroup execution.QueryGroupIdentity,
	from string,
	judged []string,
	ready map[string]ownership.WorkerRegistration,
	readings ByteReadings,
	sum map[string]uint64,
	unreadBy map[string]int,
	unreadEstimate uint64,
	peak uint64,
	at time.Time,
) string {
	destination, headroom := "", uint64(0)
	for _, workerID := range judged {
		if workerID == from {
			continue
		}
		if unreadBy[workerID] > 0 && unreadEstimate == 0 {
			continue
		}
		projected := projectedSum(sum[workerID], unreadBy[workerID], unreadEstimate)
		pool, _ := readings.pool(workerID)
		limit := byteLimit(pool)
		if projected > limit || peak > limit-projected || !readings.shareFits(workerID, peak) {
			continue
		}
		if router.additionalEligibility != nil && !router.additionalEligibility.Eligible(queryGroup, ready[workerID], at) {
			continue
		}
		left := limit - projected
		if destination == "" || left > headroom || (left == headroom && workerID < destination) {
			destination, headroom = workerID, left
		}
	}
	return destination
}

// shareFitsAnywhere is whether some judged Worker other than from would hold
// a Query Group of peak within its share.
func (router *Router) shareFitsAnywhere(from string, judged []string, readings ByteReadings, peak uint64) bool {
	for _, workerID := range judged {
		if workerID != from && readings.shareFits(workerID, peak) {
			return true
		}
	}
	return false
}

// relievingDestination is, when every judged Worker is over the line, the
// least occupied one the Query Group would leave below the occupancy it
// leaves - O(destination) + peak < O(source) - within its share and
// eligible, or "". Such a move never makes the heavier of the two heavier
// and strictly lowers the sum of squares of the occupancies, so moves of
// this kind end, and none is undone (014 C1 section 8.3).
func (router *Router) relievingDestination(
	queryGroup execution.QueryGroupIdentity,
	from string,
	judged []string,
	ready map[string]ownership.WorkerRegistration,
	readings ByteReadings,
	sum map[string]uint64,
	peak uint64,
	at time.Time,
) string {
	destination := ""
	for _, workerID := range judged {
		if workerID == from || sum[workerID]+peak >= sum[from] || !readings.shareFits(workerID, peak) {
			continue
		}
		if router.additionalEligibility != nil && !router.additionalEligibility.Eligible(queryGroup, ready[workerID], at) {
			continue
		}
		if destination == "" || sum[workerID] < sum[destination] || (sum[workerID] == sum[destination] && workerID < destination) {
			destination = workerID
		}
	}
	return destination
}

// PlanByteMoves exposes the router's byte-constraint planning to the owner
// of the reconcile loop.
func (reconciler *Reconciler) PlanByteMoves(
	owners map[execution.QueryGroupIdentity]string,
	workers []ownership.WorkerRegistration,
	readings ByteReadings,
	at time.Time,
) BytePlan {
	if reconciler == nil {
		return BytePlan{Sum: map[string]uint64{}}
	}
	return reconciler.router.PlanByteMoves(owners, workers, readings, at)
}

// unreadEstimate is the nearest-rank ninetieth percentile of the peaks read
// this round, or zero when none was read.
// projectedSum is what a Worker is judged to hold as a destination: the
// peaks it has reported and, for each Query Group with none yet, the
// round's estimate (decision-020 section 5.7). Every planner that asks
// whether a Query Group fits a Worker - the byte constraint, the count
// correction and a placement - asks it of this one sum, so none of them
// fills a Worker another would then move a Query Group off again.
func projectedSum(known uint64, unread int, estimate uint64) uint64 {
	return known + uint64(unread)*estimate
}

// byteProjection is one round's projected sums over the owners it began
// with, kept current as the round moves and places Query Groups, for the
// planners that choose a destination by whether a Query Group fits.
type byteProjection struct {
	readings ByteReadings
	known    map[string]uint64
	unread   map[string]int
	estimate uint64
	// allOver is every judged Worker over the line under occupancy, out of
	// a new Leader's first hour: a move is then made only where it relieves
	// (relieves).
	allOver bool
}

// newByteProjection projects the owners' Workers. The estimate is the one
// the byte constraint uses: the ninetieth percentile of the peaks read this
// round on Workers whose pool is known.
func newByteProjection(owners map[execution.QueryGroupIdentity]string, readings ByteReadings) *byteProjection {
	projection := &byteProjection{readings: readings, known: map[string]uint64{}, unread: map[string]int{}}
	peaks := make([]uint64, 0, len(owners))
	for queryGroup, owner := range owners {
		if _, judged := readings.pool(owner); !judged {
			continue
		}
		if peak, read := readings.peak(queryGroup); read {
			if readings.Occupancy == nil {
				projection.known[owner] += peak
			}
			peaks = append(peaks, peak)
			continue
		}
		if readings.Occupancy == nil {
			projection.unread[owner]++
		}
	}
	projection.estimate = unreadEstimate(peaks)
	if readings.Occupancy != nil {
		judged := 0
		projection.allOver = !readings.held()
		for workerID, held := range readings.Occupancy {
			pool, known := readings.pool(workerID)
			if !known {
				continue
			}
			judged++
			projection.known[workerID] = held
			projection.allOver = projection.allOver && held > byteLimit(pool)
		}
		projection.allOver = projection.allOver && judged >= 2
	}
	return projection
}

// peakOf is the Query Group's reported peak, or the estimate when it has
// none yet.
func (projection *byteProjection) peakOf(queryGroup execution.QueryGroupIdentity) (uint64, bool) {
	if peak, read := projection.readings.peak(queryGroup); read {
		return peak, true
	}
	return projection.estimate, false
}

// fits reports whether the Query Group may land on the Worker. A Worker
// with no pool is not judged. While the round has no estimate - no peak
// read at all - an unknown counts as nothing, the rule before the estimate
// existed, and with nothing read every sum is zero: nothing is refused at
// the start of a term.
func (projection *byteProjection) fits(workerID string, queryGroup execution.QueryGroupIdentity) bool {
	if projection == nil {
		return true
	}
	pool, judged := projection.readings.pool(workerID)
	if !judged {
		return true
	}
	peak, _ := projection.peakOf(queryGroup)
	return projectedSum(projection.known[workerID], projection.unread[workerID], projection.estimate)+peak <= byteLimit(pool) &&
		projection.readings.shareFits(workerID, peak)
}

// relieves reports whether, with every judged Worker over the line, moving
// the Query Group from one Worker to another leaves the destination below
// the occupancy of the source: the only moves the count and weighted
// balance make then. Never for a placement, whose source has gone
// (selectFitting asks fits alone).
func (projection *byteProjection) relieves(to, from string, queryGroup execution.QueryGroupIdentity) bool {
	if projection == nil || !projection.allOver || projection.readings.Arrived[queryGroup] {
		return false
	}
	if _, judged := projection.readings.pool(to); !judged {
		return false
	}
	peak, _ := projection.peakOf(queryGroup)
	return projection.known[to]+peak < projection.known[from] && projection.readings.shareFits(to, peak)
}

// leave takes the Query Group's projected peak off the Worker it moves
// from, under occupancy: its share of that Worker's high-water goes with it
// as the Leader accounts a departure. Under the sum of peaks the Worker it
// leaves is not taken down, as before.
func (projection *byteProjection) leave(queryGroup execution.QueryGroupIdentity, from string) {
	if projection == nil || projection.readings.Occupancy == nil {
		return
	}
	peak, _ := projection.peakOf(queryGroup)
	projection.known[from] -= min(peak, projection.known[from])
}

// land adds the Query Group's projected peak to the Worker it moves or is
// placed onto. The Worker it leaves is not taken down: within a round it is
// never a destination again - a holder that left the ready set, one no
// longer eligible, or the count correction's most loaded Worker.
func (projection *byteProjection) land(queryGroup execution.QueryGroupIdentity, to string) {
	if projection == nil {
		return
	}
	peak, read := projection.peakOf(queryGroup)
	if read || projection.readings.Occupancy != nil {
		projection.known[to] += peak
	} else {
		projection.unread[to]++
	}
}

func unreadEstimate(peaks []uint64) uint64 {
	if len(peaks) == 0 {
		return 0
	}
	sorted := append([]uint64(nil), peaks...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	rank := (len(sorted)*unreadEstimatePercentile + 99) / 100
	return sorted[rank-1]
}
