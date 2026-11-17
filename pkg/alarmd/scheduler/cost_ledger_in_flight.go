// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// InFlightWindow is how long a Query Group that changed holder is accounted
// on both sides (014 C1 section 8.3): the hour a Worker's retained
// high-water spans. Until then the destination's high-water may not have
// seen it run, and the source's still holds what it ran there.
const InFlightWindow = time.Hour

// inFlight is one Leader term's account of the Query Groups that changed
// holder: what the Workers' high-waters do not say yet. It is the Leader's
// own, never a Worker's report.
type inFlight struct {
	// term is the control epoch the account is for, and began when this
	// process first led it.
	term  uint64
	began time.Time
	// held is the owners the last round ended with: a holder that differs
	// from it is a change the account has not recorded yet. Nil until the
	// term's records were read (Settle).
	held map[execution.QueryGroupIdentity]string
	// arrivals is each Query Group that landed within the account, on the
	// Worker it landed on. It is counted on the Worker until the hour has
	// passed and the Worker has reported the Query Group itself - it has run
	// there, inside the high-water.
	arrivals map[execution.QueryGroupIdentity]arrival
	// departures is the Query Groups that left a Worker having run there,
	// taken off that Worker's high-water for the hour it still holds them.
	departures []departure
}

type arrival struct {
	to string
	at time.Time
}

type departure struct {
	queryGroup execution.QueryGroupIdentity
	from       string
	at         time.Time
}

// Lead brings the account to the control epoch a round is led under: a new
// term starts it over, the moves the previous Leader made unknown to it
// until Settle reads them from the records. Called at the start of every
// round, before any readings.
func (ledger *CostLedger) Lead(epoch uint64) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.moving.term == epoch && !ledger.moving.began.IsZero() {
		return
	}
	ledger.moving = inFlight{term: epoch, began: ledger.now(), arrivals: map[execution.QueryGroupIdentity]arrival{}}
}

// Settle reads the term's arrivals from the Assignment records the first
// round settled, once a term. A record carries no previous holder, so only
// the landing side can be rebuilt: a Query Group assigned within the hour,
// or whose holder has not reported it yet, is landing on its holder from
// when it was assigned. The leaving side cannot be, and no byte move is made
// until the term is an hour old (HoldByteMoves).
func (ledger *CostLedger) Settle(records map[execution.QueryGroupIdentity]ownership.AssignmentRecord) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.moving.held != nil || ledger.moving.began.IsZero() {
		return
	}
	now := ledger.now()
	held := make(map[execution.QueryGroupIdentity]string, len(records))
	for queryGroup, record := range records {
		if record.DesiredWorkerID == "" {
			continue
		}
		held[queryGroup] = record.DesiredWorkerID
		// AssignedAt is the deciding Leader's clock, subtracted here and at
		// the arrival's end from this one's: only to bound the hour, off by
		// the two clocks' skew.
		recent := !record.AssignedAt.IsZero() && now.Sub(record.AssignedAt) < InFlightWindow
		if recent || !ledger.reportedLocked(record.DesiredWorkerID, queryGroup) {
			ledger.moving.arrivals[queryGroup] = arrival{to: record.DesiredWorkerID, at: record.AssignedAt}
		}
	}
	ledger.moving.held = held
}

// reportedLocked is whether the Worker reported the Query Group itself: it
// has run it. A reading carried over from a previous holder is not.
func (ledger *CostLedger) reportedLocked(workerID string, queryGroup execution.QueryGroupIdentity) bool {
	entry, known := ledger.entries[workerID][queryGroup]
	return known && !entry.Provisional
}

// peakLocked is the Query Group's peak as the ledger holds it, under its
// holder or, while the holder has not had it carried over yet, under the
// holder it had.
func (ledger *CostLedger) peakLocked(queryGroup execution.QueryGroupIdentity, holders ...string) (uint64, bool) {
	for _, holder := range holders {
		if entry, known := ledger.entries[holder][queryGroup]; known {
			return entry.RetainedBytesPeak, true
		}
	}
	return 0, false
}

// recordMovesLocked records the round's changes of holder - every one,
// whatever made it: a byte or count move, the re-placement of a Worker's
// Query Groups as it leaves, a session lost and taken over - and lets go
// of what has run its course. Under ledger.mu, before Retain carries
// entries over.
func (ledger *CostLedger) recordMovesLocked(owners map[execution.QueryGroupIdentity]string) {
	moving := &ledger.moving
	if moving.held == nil {
		return
	}
	now := ledger.now()
	for queryGroup, owner := range owners {
		previous := moving.held[queryGroup]
		if owner == previous {
			continue
		}
		if owner != "" {
			moving.arrivals[queryGroup] = arrival{to: owner, at: now}
		}
		if previous != "" && ledger.reportedLocked(previous, queryGroup) {
			moving.departures = append(moving.departures, departure{queryGroup: queryGroup, from: previous, at: now})
		}
	}
	for queryGroup, landed := range moving.arrivals {
		if owners[queryGroup] != landed.to {
			delete(moving.arrivals, queryGroup)
			continue
		}
		if now.Sub(landed.at) >= InFlightWindow && ledger.reportedLocked(landed.to, queryGroup) {
			delete(moving.arrivals, queryGroup)
		}
	}
	kept := moving.departures[:0]
	for _, left := range moving.departures {
		if now.Sub(left.at) < InFlightWindow {
			kept = append(kept, left)
		}
	}
	moving.departures = kept
	held := make(map[execution.QueryGroupIdentity]string, len(owners))
	for queryGroup, owner := range owners {
		held[queryGroup] = owner
	}
	moving.held = held
}

// OccupancyParts is what a Worker's occupancy was made of: its reported
// high-water and the window it spans, the peaks of its Query Groups whose
// period the window may not have seen, and the Query Groups landing on it
// and gone from it as the Leader accounts them.
type OccupancyParts struct {
	HighBytes       uint64
	WindowSeconds   int64
	LongPeriodBytes uint64
	ArrivingBytes   uint64
	Arriving        int
	// Waiting is how many of the arriving are past the hour and counted
	// only because the Worker has not reported them yet - one it never runs
	// is counted for as long as it holds it - and WaitingSample up to
	// unreadSampleLimit of them, lowest first: which ones, read at once.
	Waiting       int
	WaitingSample []execution.QueryGroupIdentity
	LeavingBytes  uint64
	Leaving       int
}

// occupancyLocked judges the round by concurrent occupancy when every ready
// Worker with a pool reported a high-water, and says which did not when one
// did not: then the fleet is judged by the sum of peaks whole.
func (ledger *CostLedger) occupancyLocked(
	readings *ByteReadings,
	owners map[execution.QueryGroupIdentity]string,
	workers []ownership.WorkerRegistration,
) {
	moving := &ledger.moving
	now := ledger.now()
	// RETIRE(0.7): the hold goes once each Worker's high-water reading
	// carries the bytes of the Query Groups that left it within its window
	// and every registration on every deployment carries them (no Worker on
	// the rebalance line without them): a new term then reads what left from
	// the Workers, not from a ledger only its predecessor had. It depends on
	// a design change made apart; until then the hold stays.
	readings.HoldByteMoves = moving.began.IsZero() || moving.held == nil || now.Sub(moving.began) < InFlightWindow
	if !moving.began.IsZero() {
		readings.TermAge = now.Sub(moving.began)
	}
	high := map[string]ownership.RetainedHighReading{}
	for _, worker := range workers {
		if worker.Load == nil || worker.Load.RetainedPoolBytes == 0 || worker.AssignmentReadiness != ownership.WorkerReady {
			continue
		}
		// RETIRE(0.7): judging the whole fleet by the sum of peaks when a
		// Worker reports no high-water - a rollout mixing builds from before
		// RetainedHigh - goes once every rebalance line on every deployment
		// for 7 days reads byte_constraint_judgement=occupancy with no
		// byte_constraint_not_reporting. A ready Worker with a pool that
		// reports none then holds the round's byte moves and is named on the
		// line, in place of the fallback.
		if worker.Load.RetainedHigh == nil {
			readings.NotReporting = append(readings.NotReporting, worker.WorkerID)
			continue
		}
		high[worker.WorkerID] = *worker.Load.RetainedHigh
	}
	sort.Strings(readings.NotReporting)
	if len(readings.NotReporting) > 0 || len(high) == 0 {
		return
	}
	read := make([]uint64, 0, len(readings.Peak))
	for _, peak := range readings.Peak {
		read = append(read, peak)
	}
	estimate := unreadEstimate(read)
	parts := make(map[string]OccupancyParts, len(high))
	for workerID, reading := range high {
		parts[workerID] = OccupancyParts{HighBytes: reading.Bytes, WindowSeconds: reading.WindowSeconds, LongPeriodBytes: reading.LongPeriodBytes}
	}
	arrived := map[execution.QueryGroupIdentity]bool{}
	land := func(queryGroup execution.QueryGroupIdentity, to string, waiting bool) {
		part, judged := parts[to]
		if !judged {
			return
		}
		peak, known := ledger.peakLocked(queryGroup, to, moving.held[queryGroup])
		if !known {
			peak = estimate
		}
		part.ArrivingBytes += peak
		part.Arriving++
		if waiting {
			part.Waiting++
			part.WaitingSample = append(part.WaitingSample, queryGroup)
		}
		parts[to] = part
		arrived[queryGroup] = true
	}
	leave := func(queryGroup execution.QueryGroupIdentity, from string, at time.Time) {
		part, judged := parts[from]
		// A departure the Worker's window no longer covers - it restarted
		// since, or the hour has passed - is not in its high-water to take
		// off.
		if !judged || now.Sub(at) >= time.Duration(part.WindowSeconds)*time.Second {
			return
		}
		peak, known := ledger.peakLocked(queryGroup, owners[queryGroup], from)
		if !known {
			return
		}
		part.LeavingBytes += peak
		part.Leaving++
		parts[from] = part
	}
	for queryGroup, landed := range moving.arrivals {
		if owners[queryGroup] == landed.to {
			land(queryGroup, landed.to, now.Sub(landed.at) >= InFlightWindow)
		}
	}
	for _, left := range moving.departures {
		leave(left.queryGroup, left.from, left.at)
	}
	if moving.held != nil {
		// The changes this round made before these readings, not recorded
		// until it ends: placements the settlement made and moves already
		// published.
		for queryGroup, owner := range owners {
			previous := moving.held[queryGroup]
			if owner == previous {
				continue
			}
			if landed, recorded := moving.arrivals[queryGroup]; owner != "" && (!recorded || landed.to != owner) {
				land(queryGroup, owner, false)
			}
			if previous != "" && ledger.reportedLocked(previous, queryGroup) {
				leave(queryGroup, previous, now)
			}
		}
	}
	readings.Occupancy = make(map[string]uint64, len(parts))
	for workerID, part := range parts {
		sort.Slice(part.WaitingSample, func(left, right int) bool { return part.WaitingSample[left] < part.WaitingSample[right] })
		part.WaitingSample = part.WaitingSample[:min(len(part.WaitingSample), unreadSampleLimit)]
		parts[workerID] = part
		held := part.HighBytes + part.LongPeriodBytes + part.ArrivingBytes
		readings.Occupancy[workerID] = held - min(held, part.LeavingBytes)
	}
	readings.Parts = parts
	readings.Arrived = arrived
}
