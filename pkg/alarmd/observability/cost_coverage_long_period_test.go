package observability

import (
	"context"
	"testing"
	"time"
)

// A Slot longer than the window straddles it once its completion falls in
// the window: the latest such Slot, and none when its completion is past the
// end or before the start.
func TestAStraddlingSlotIsTheLatestWhoseCompletionFellInTheWindow(t *testing.T) {
	hourly := costDue{interval: 3600}
	for name, testCase := range map[string]struct {
		start, end int64
		slot       int64
		want       bool
	}{
		"completed in the window":      {6900, 7400, 3600, true},
		"completion on the start":      {7200, 7500, 3600, true},
		"completion a second early":    {7201, 7500, 3600, false},
		"not yet completed":            {6600, 7199, 0, false},
		"a short slot is dueIn's":      {7000, 7400, 0, false},
		"nothing before the alignment": {0, 300, 0, false},
	} {
		slot, ok := hourly.straddling(testCase.start, testCase.end)
		if name == "a short slot is dueIn's" {
			slot, ok = costDue{interval: 60}.straddling(testCase.start, testCase.end)
		}
		if ok != testCase.want || (ok && slot != testCase.slot) {
			t.Errorf("%s: straddling(%d, %d) = %d, %v; want %d, %v", name, testCase.start, testCase.end, slot, ok, testCase.slot, testCase.want)
		}
	}
}

// longPeriodFixture tracks the hourly group from before its Slot at 3600 and
// returns the summary and its clock.
func longPeriodFixture(t *testing.T) (*CostSummary, *time.Time) {
	t.Helper()
	now := time.Unix(3000, 0)
	c := NewCostSummary(CostSummaryOptions{ProcessID: "process-a", Window: 5 * time.Minute, GroupCapacity: 8, PlanCapacity: 8,
		MetadataBytes: 4096, TopN: 2, Now: func() time.Time { return now }})
	c.Reconcile([]CostGroup{costHourly}, true)
	return c, &now
}

func hourlySlot(c *CostSummary, evaluationTime int64, evaluated bool, result Result) {
	trace := TraceFields{QueryGroupKey: "hourly", QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", EvaluationTime: evaluationTime}
	c.Observe(context.Background(), Observation{Stage: StageSlotStarted, Result: ResultStarted, Trace: trace})
	c.Observe(context.Background(), Observation{Stage: StageSlotCompleted, Result: result, Duration: time.Millisecond, DurationKnown: true, Trace: trace})
	if evaluated {
		c.Observe(context.Background(), Observation{Stage: StageEvaluationCompleted, Result: ResultSuccess, Duration: time.Millisecond, DurationKnown: true,
			EvaluationOwner: costB, EvaluationRecordsKnown: true, Trace: trace})
	}
}

// An hourly Plan is judged by its last Slot once that Slot's completion falls
// in the window, though the Slot ran forty minutes before: evaluated, it is
// seen; not evaluated and failed, the failure accounts for it; with no record
// of that Slot -- only of the one before -- it went unseen.
func TestAnHourlyPlanIsJudgedByItsLastSlotLongAfterItRan(t *testing.T) {
	c, now := longPeriodFixture(t)
	*now = time.Unix(4800, 0)
	hourlySlot(c, 3600, true, ResultSuccess)
	*now = time.Unix(7400, 0)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DuePlans != 1 || coverage.UnobservedDuePlans != 0 || coverage.DueGroups != 1 ||
		coverage.UnobservedDueGroups != 0 || coverage.Incomplete {
		t.Fatalf("an hourly Plan evaluated forty minutes before = %+v, want due, seen and the window complete", coverage)
	}

	c, now = longPeriodFixture(t)
	*now = time.Unix(4800, 0)
	hourlySlot(c, 3600, false, ResultFailed)
	*now = time.Unix(7400, 0)
	c.Publish(*now)
	coverage := c.Snapshot().Coverage
	if coverage.DuePlans != 1 || coverage.UnobservedDuePlans != 0 || unevaluatedOf(t, coverage)["failed"] != 1 {
		t.Fatalf("an hourly Plan whose last Slot failed = %+v, want it accounted for by the failure", coverage)
	}

	c, now = longPeriodFixture(t)
	*now = time.Unix(3300, 0)
	hourlySlot(c, 0, true, ResultSuccess)
	*now = time.Unix(7400, 0)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DuePlans != 1 || coverage.UnobservedDuePlans != 1 || coverage.UnobservedDueGroups != 1 || !coverage.Incomplete {
		t.Fatalf("an hourly Plan with a record only of the Slot before = %+v, want its last Slot unseen and the window incomplete", coverage)
	}
}

// A record answers only for the Slot it names: the "ok" of a Slot never
// stands in for a later one the Plan lost, however recent it was.
func TestAnOlderSlotsRecordDoesNotAnswerForALaterOne(t *testing.T) {
	c, now := longPeriodFixture(t)
	*now = time.Unix(4800, 0)
	hourlySlot(c, 3600, true, ResultSuccess)
	*now = time.Unix(11000, 0)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DuePlans != 1 || coverage.UnobservedDuePlans != 1 {
		t.Fatalf("the Slot at 7200 judged by the record of 3600 = %+v, want it unseen", coverage)
	}

	// Nor does the group's: a failure of the Slot before does not account
	// for a later one nothing was observed of.
	c, now = longPeriodFixture(t)
	*now = time.Unix(4800, 0)
	hourlySlot(c, 3600, false, ResultFailed)
	*now = time.Unix(11000, 0)
	c.Publish(*now)
	coverage := c.Snapshot().Coverage
	if coverage.UnobservedDuePlans != 1 || unevaluatedOf(t, coverage)["failed"] != 0 {
		t.Fatalf("the Slot at 7200 accounted by the failure at 3600 = %+v, want it unseen", coverage)
	}
}
