package main

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The threshold is measured against what was last sent, not what was last
// read.
//
// A reading that climbs in steps each under a tenth would, measured against
// the last read, never be sent: every step compares against the step before
// it. The object drifts arbitrarily far from what the Leader holds while every
// individual comparison says "close enough", and the Leader places it on a
// number that is no longer true.
func TestTheCostThresholdIsMeasuredAgainstWhatWasSent(t *testing.T) {
	// Three steps: each is under a tenth against the step before it, so
	// measured that way none would ever be sent, and the first two are under a
	// tenth of the 1000 that was sent. The third has drifted 16% from it and
	// must go out.
	steps := []uint64{1040, 1080, 1160}
	for index, reading := range steps {
		if index == 0 {
			continue
		}
		if costReadingMoved(steps[index-1], reading) {
			t.Fatalf("step %d -> %d is under a tenth; a case that compares consecutive reads "+
				"cannot show the drift this guards against", steps[index-1], reading)
		}
	}
	if !costReadingMoved(1000, steps[len(steps)-1]) {
		t.Fatalf("%d against the 1000 that was sent was withheld, so the Leader keeps placing this "+
			"object on a number it has drifted 16%% away from", steps[len(steps)-1])
	}
	if costReadingMoved(1000, steps[0]) {
		t.Fatal("a reading 4% above what was sent was reported; the threshold is a tenth")
	}

	// And the source measures against the sent value rather than the read one.
	source := newWorkerCostSource(nil, nil)
	group := execution.QueryGroupIdentity("qg-1")
	source.boundByOwned(func() int { return 4 })
	if costs := source.report([]observability.CostRetainedPeak{{QueryGroupKey: "qg-1", RetainedBytesPeak: 1000}}); len(costs) != 1 {
		t.Fatalf("the first reading was withheld: %+v", costs)
	}
	for _, reading := range steps[:2] {
		if costs := source.report([]observability.CostRetainedPeak{{QueryGroupKey: "qg-1", RetainedBytesPeak: reading}}); len(costs) != 0 {
			t.Fatalf("reading %d was reported; it is under a tenth of the 1000 sent", reading)
		}
	}
	costs := source.report([]observability.CostRetainedPeak{{QueryGroupKey: "qg-1", RetainedBytesPeak: steps[2]}})
	if len(costs) != 1 || costs[0].RetainedBytesPeak != steps[2] {
		t.Fatalf("the drifted reading was withheld: %+v", costs)
	}
	if source.reported[group] != steps[2] {
		t.Fatalf("what was sent was recorded as %d, want %d", source.reported[group], steps[2])
	}
}

// A reading that appears or disappears is always reported.
//
// A tenth of zero is zero, so a proportional test alone reports every change
// from an object that had no peak and none from an object that has just
// acquired one - the two cases a placement decision most needs to hear about.
func TestACostReadingAppearingOrGoingIsAlwaysReported(t *testing.T) {
	if !costReadingMoved(0, 1) {
		t.Fatal("an object that acquired a peak was withheld")
	}
	if !costReadingMoved(1, 0) {
		t.Fatal("an object whose peak went to zero was withheld")
	}
	if costReadingMoved(0, 0) {
		t.Fatal("an object with no peak before or after was reported")
	}
}

// The rate is not derived until there are two readings and a span to divide.
//
// Every case here returns "not reported" rather than a number, because the
// value decides which object moves: a rate that is quietly low leaves an
// object exactly where it should not be, and there is no reading that says so.
func TestTheCostRateIsNotGuessedBeforeItCanBeDerived(t *testing.T) {
	const second = time.Second
	for _, testCase := range []struct {
		name     string
		first    observability.CostRetainedPeak
		second   observability.CostRetainedPeak
		elapsed  time.Duration
		wantZero bool
	}{
		{name: "the first ask has nothing to difference against",
			first: observability.CostRetainedPeak{ComputeWallNS: int64(second)}, elapsed: second, wantZero: true},
		{name: "a window carrying an unmeasured observation is short by an unknown amount",
			first:   observability.CostRetainedPeak{ComputeWallNS: 0},
			second:  observability.CostRetainedPeak{ComputeWallNS: int64(second), ComputeWallUnknown: 1},
			elapsed: second, wantZero: true},
		{name: "a window that rotated makes the difference negative, which is not a rate of zero",
			first:   observability.CostRetainedPeak{ComputeWallNS: int64(10 * second)},
			second:  observability.CostRetainedPeak{ComputeWallNS: int64(second)},
			elapsed: second, wantZero: true},
		{name: "no span to divide by",
			first:   observability.CostRetainedPeak{ComputeWallNS: 0},
			second:  observability.CostRetainedPeak{ComputeWallNS: int64(second)},
			elapsed: 0, wantZero: true},
		// The control: with two readings, a span, and nothing unmeasured, the
		// rate is derived. Without it every case above would pass on a source
		// that never reports anything at all.
		{name: "two readings a second apart over a second of schedule",
			first:   observability.CostRetainedPeak{ComputeWallNS: 0},
			second:  observability.CostRetainedPeak{ComputeWallNS: int64(second / 2)},
			elapsed: second, wantZero: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := newWorkerCostSource(nil, nil)
			group := execution.QueryGroupIdentity("qg-1")
			source.advanceCost(group, testCase.first, testCase.elapsed)
			got := source.advanceCost(group, testCase.second, testCase.elapsed)
			if (got == 0) != testCase.wantZero {
				t.Fatalf("rate = %d, want zero = %t", got, testCase.wantZero)
			}
			if !testCase.wantZero && got != 500 {
				t.Fatalf("rate = %d, want 500 milli: half a second of compute per second of schedule", got)
			}
		})
	}
}

// An entry cut by the owned bound is not recorded as sent.
//
// Recording it would make the next ask compare a reading against a value the
// Leader never received, and an object that then stayed steady would be
// withheld by the threshold forever - silently, because from here it looks
// exactly like an object already reported.
func TestAnEntryCutByTheBoundIsSentInFull(t *testing.T) {
	source := newWorkerCostSource(nil, nil)
	source.boundByOwned(func() int { return 0 })
	group := execution.QueryGroupIdentity("qg-1")

	costs := source.report([]observability.CostRetainedPeak{{QueryGroupKey: "qg-1", RetainedBytesPeak: 100}})
	if len(costs) != 0 {
		t.Fatalf("the bound of zero reported %d entries", len(costs))
	}
	if _, sent := source.reported[group]; sent {
		t.Fatal("an entry cut by the bound was recorded as sent, so it will be withheld once it steadies")
	}
	source.boundByOwned(func() int { return 4 })
	costs = source.report([]observability.CostRetainedPeak{{QueryGroupKey: "qg-1", RetainedBytesPeak: 100}})
	if len(costs) != 1 || costs[0].RetainedBytesPeak != 100 {
		t.Fatalf("the entry did not go out whole on the next ask: %+v", costs)
	}
}
