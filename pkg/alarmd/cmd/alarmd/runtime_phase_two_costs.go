package main

import (
	"sort"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// costReportThresholdPercent is how far a reading must move before it is
// reported again. decision-020 section 5.2: the heartbeat carries what
// changed, not the whole roster every tick.
const costReportThresholdPercent = 10

// costEWMAWeightPercent is how much of a new reading enters the smoothed cost.
// Low enough that one slow round does not move an object, high enough that a
// genuine change is reported within a few heartbeats.
const costEWMAWeightPercent = 30

// workerCostSource is what this Worker's heartbeat reports of what its Query
// Groups cost, for the Leader's byte-feasibility moves.
//
// The retained-byte peak is read from the process's cost summary rather than
// accumulated here. It is the same number the read-only column shows, taken by
// the same expression over the same two windows, so the Leader's per-Worker
// sum and that column agree by construction. A second accumulator over the
// same observations would answer the same question with its own number, and
// nothing on either page would show which one was wrong.
//
// The window is deliberately the summary's, not the last round's. The question
// the Leader asks of this number is whether a replica's objects fit at once in
// the worst round, so an upper bound is what it needs: the last round's peak
// reads low on a bimodal object and biases toward leaving it where it is,
// which is the direction that fills a pool. The window maximum reads high, and
// the cost of high is at most one extra move, bounded to one per replica per
// round and stopping once the replica is back under its share.
type workerCostSource struct {
	summary *observability.CostSummary
	mu      sync.Mutex
	// owned is set after the bundle exists, which is after the view client is
	// built, so it is read under the lock: the heartbeat can ask for costs
	// from the moment the client starts.
	ownedBound func() int
	// reported is what was last sent for each Query Group, which is what the
	// threshold is measured against - not what was last read. Measuring
	// against the last read would let a reading drift past the threshold in
	// steps that are each under it, and never be sent.
	//
	// Both dimensions, because either can move on its own: an object whose
	// retained bytes hold steady while its compute triples is one the Leader
	// must hear about, and a threshold that watched only the bytes would leave
	// it placed on the cost of its first reading forever.
	reported map[execution.QueryGroupIdentity]reportedCost
	// cost is the smoothed cost per second and what it was last derived from.
	cost map[execution.QueryGroupIdentity]*costRate
	// lastAsked is when Costs was last answered, which is the span the wall
	// difference is a rate over. The heartbeat's own cadence rather than a
	// configured period: it is the only interval both ends of the difference
	// were taken across.
	lastAsked time.Time
	now       func() time.Time
}

// reportedCost is what was last sent for one Query Group.
type reportedCost struct {
	peak  uint64
	milli uint64
}

// costRate is one Query Group's smoothed cost and the reading it was last
// advanced from.
type costRate struct {
	// lastWallNS is the window's compute wall at the previous ask, so the
	// difference is what was spent since. The summary's window rotates under
	// this, which makes a difference negative; a negative difference is
	// dropped rather than clamped to zero, because zero is a rate and "the
	// window rotated" is not a reading at all.
	lastWallNS int64
	seeded     bool
	milli      uint64
}

func newWorkerCostSource(summary *observability.CostSummary, now func() time.Time) *workerCostSource {
	if now == nil {
		now = time.Now
	}
	return &workerCostSource{summary: summary, now: now,
		reported: make(map[execution.QueryGroupIdentity]reportedCost),
		cost:     make(map[execution.QueryGroupIdentity]*costRate)}
}

// boundByOwned sets what bounds the report: how many Query Groups this Worker
// holds. Until it is set the report is bounded only by what the summary holds,
// which is the resource budget's group capacity.
func (source *workerCostSource) boundByOwned(owned func() int) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.ownedBound = owned
}

// Costs is asked once per heartbeat for the entries worth sending.
//
// An entry is sent when it has never been sent from this Worker, or when it
// has moved by a tenth or more since it was last sent. The first reading is
// not subject to the threshold and cannot be: the Leader has nothing for that
// Query Group until one arrives, so a first reading withheld for being close
// to a number the Leader never saw would leave the object unread for as long
// as it stayed steady - which is exactly the object that is safe to move and
// would never be considered.
func (source *workerCostSource) Costs() []viewstream.QueryGroupCost {
	if source == nil || source.summary == nil {
		return nil
	}
	return source.report(source.summary.RetainedPeaks())
}

// report decides what to send from one set of readings. Separate from Costs so
// the decision can be exercised without a live summary behind it: what is
// under test here is which entries go out, not where the readings came from.
func (source *workerCostSource) report(peaks []observability.CostRetainedPeak) []viewstream.QueryGroupCost {
	if len(peaks) == 0 {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()

	at := source.now()
	elapsed := at.Sub(source.lastAsked)
	source.lastAsked = at

	costs := make([]viewstream.QueryGroupCost, 0, len(peaks))
	for _, peak := range peaks {
		group := execution.QueryGroupIdentity(peak.QueryGroupKey)
		milli := source.advanceCost(group, peak, elapsed)
		last, sent := source.reported[group]
		if sent && !costReadingMoved(last.peak, peak.RetainedBytesPeak) &&
			!costReadingMoved(last.milli, milli) {
			continue
		}
		costs = append(costs, viewstream.QueryGroupCost{
			QueryGroup: group, RetainedBytesPeak: peak.RetainedBytesPeak,
			CostPerSecondMilli: milli,
		})
	}
	// Largest first, so a report cut to the bound carries the objects that
	// decide feasibility rather than whichever ones sorted first by name.
	sort.Slice(costs, func(i, j int) bool {
		if costs[i].RetainedBytesPeak != costs[j].RetainedBytesPeak {
			return costs[i].RetainedBytesPeak > costs[j].RetainedBytesPeak
		}
		return costs[i].QueryGroup < costs[j].QueryGroup
	})
	if source.ownedBound != nil {
		if bound := source.ownedBound(); bound >= 0 && len(costs) > bound {
			costs = costs[:bound]
		}
	}
	// Recorded after the bound, so an entry cut from this report is still
	// unreported and goes out whole next time rather than being remembered as
	// sent.
	for _, cost := range costs {
		source.reported[cost.QueryGroup] = reportedCost{peak: cost.RetainedBytesPeak, milli: cost.CostPerSecondMilli}
	}
	return costs
}

// advanceCost folds this ask's compute wall into the Query Group's smoothed
// cost per second of schedule, and returns what to report.
//
// Four asks cannot derive a rate: the first one for a Query Group, one with no
// span to divide by, one whose window carries observations that were never
// measured, and one where the window rotated between the two ends of the
// difference. None of them invents a number - what they return is the last
// rate that WAS derivable, and zero only while there has never been one.
//
// Carrying the last value forward rather than reporting zero is deliberate.
// The window rotates every few minutes by construction, so zeroing on rotation
// would send the Leader a "not reported" and then a recovery on a fixed
// cadence, for an object whose cost never moved - manufacturing exactly the
// change the threshold exists to filter out. The last derived rate is the
// honest answer to "what does this object cost": it is a measurement that was
// taken, just not in this window.
//
// What none of them do is guess. The number decides which object moves, and a
// rate invented low leaves an object exactly where it should not be with
// nothing on any page saying so.
func (source *workerCostSource) advanceCost(
	group execution.QueryGroupIdentity, reading observability.CostRetainedPeak, elapsed time.Duration,
) uint64 {
	rate := source.cost[group]
	if rate == nil {
		rate = &costRate{}
		source.cost[group] = rate
	}
	previous, wall := rate.lastWallNS, reading.ComputeWallNS
	seeded := rate.seeded
	rate.lastWallNS, rate.seeded = wall, true
	switch {
	// Each of these leaves rate.milli untouched, so the value returned is the
	// last one that was derived from a measured window.
	case !seeded, elapsed <= 0, reading.ComputeWallUnknown > 0, wall < previous:
		return rate.milli
	}
	spent := wall - previous
	// Thousandths of a second of compute per second of schedule.
	sample := uint64(spent * 1000 / elapsed.Nanoseconds())
	if rate.milli == 0 {
		rate.milli = sample
		return rate.milli
	}
	rate.milli = (rate.milli*(100-costEWMAWeightPercent) + sample*costEWMAWeightPercent) / 100
	return rate.milli
}

// costReadingMoved reports whether a reading has moved by the threshold.
//
// A move away from zero always counts: a tenth of zero is zero, so a
// proportional test alone would report every change from an object that had
// none and none from an object that has just acquired one.
func costReadingMoved(last, current uint64) bool {
	if last == current {
		return false
	}
	if last == 0 || current == 0 {
		return true
	}
	high, low := last, current
	if current > last {
		high, low = current, last
	}
	return (high-low)*100 >= last*costReportThresholdPercent
}
