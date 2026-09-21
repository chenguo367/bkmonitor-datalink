package main

import (
	"sort"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// costReportThresholdPercent is how far a reading must move before it is
// reported again. decision-020 section 5.2: the heartbeat carries what
// changed, not the whole roster every tick.
const costReportThresholdPercent = 10

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
	// reported is the last value sent for each Query Group, which is what the
	// threshold is measured against - not the last value read. Measuring
	// against the last read would let a reading drift past the threshold in
	// steps that are each under it, and never be sent.
	reported map[execution.QueryGroupIdentity]uint64
}

func newWorkerCostSource(summary *observability.CostSummary) *workerCostSource {
	return &workerCostSource{summary: summary,
		reported: make(map[execution.QueryGroupIdentity]uint64)}
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
	peaks := source.summary.RetainedPeaks()
	if len(peaks) == 0 {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()

	costs := make([]viewstream.QueryGroupCost, 0, len(peaks))
	for _, peak := range peaks {
		group := execution.QueryGroupIdentity(peak.QueryGroupKey)
		last, sent := source.reported[group]
		if sent && !costReadingMoved(last, peak.RetainedBytesPeak) {
			continue
		}
		costs = append(costs, viewstream.QueryGroupCost{
			QueryGroup: group, RetainedBytesPeak: peak.RetainedBytesPeak,
			// CostPerSecondMilli stays zero until the Worker reports it:
			// zero is "not reported", and the Leader's byte moves read only
			// the peak.
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
		source.reported[cost.QueryGroup] = cost.RetainedBytesPeak
	}
	return costs
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
