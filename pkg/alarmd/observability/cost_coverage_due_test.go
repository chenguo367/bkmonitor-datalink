package observability

import (
	"context"
	"testing"
	"time"
)

// A Slot is due in a window when the first evaluation time at or after the
// window's start completes by its end. The deadline landing exactly on the
// end is due, a second later is not; the first evaluation time follows the
// alignment; a short Plan's own completion offset is what it is given; and a
// schedule nobody knows is due in every window.
func TestASlotIsDueWhenItsDeadlineFallsInsideTheWindow(t *testing.T) {
	for name, testCase := range map[string]struct {
		due        costDue
		start, end int64
		want       bool
	}{
		"deadline on the end":           {costDue{interval: 300}, 900, 1200, true},
		"deadline a second past it":     {costDue{interval: 300}, 900, 1199, false},
		"first time after an alignment": {costDue{interval: 60, alignment: 30}, 900, 990, true},
		"aligned one second too late":   {costDue{interval: 60, alignment: 30}, 900, 989, false},
		"an hour's in minutes":          {costDue{interval: 3600}, 900, 1500, false},
		"a short Plan's own offset":     {costDue{interval: 10, offset: 30}, 900, 930, true},
		"one second short of it":        {costDue{interval: 10, offset: 30}, 900, 929, false},
		"no offset means the interval":  {costDue{interval: 60}, 900, 960, true},
		"a schedule nobody knows":       {costDue{}, 900, 901, true},
	} {
		if got := testCase.due.dueIn(testCase.start, testCase.end); got != testCase.want {
			t.Errorf("%s: dueIn(%d, %d) = %v, want %v", name, testCase.start, testCase.end, got, testCase.want)
		}
	}
}

// costDueFixture tracks two groups from before the window began: one on a
// minute, due in every window, and one on an hour, due in none of these.
// The window is two five-minute buckets, 900 to 1440.
func costDueFixture(t *testing.T, groups ...CostGroup) (*CostSummary, *time.Time) {
	t.Helper()
	now := time.Unix(800, 0)
	c := NewCostSummary(CostSummaryOptions{ProcessID: "process-a", Window: 5 * time.Minute, GroupCapacity: 8, PlanCapacity: 8,
		MetadataBytes: 4096, TopN: 2, Now: func() time.Time { return now }})
	c.Reconcile(groups, true)
	now = time.Unix(1440, 0)
	return c, &now
}

var (
	costMinute = CostGroup{QueryGroupKey: "minute", QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", Members: []CostPlanIdentity{costA},
		Schedules: []CostSchedule{{Plan: costA, IntervalSeconds: 60, CompletionOffsetSeconds: 60}}}
	costHourly = CostGroup{QueryGroupKey: "hourly", QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", Members: []CostPlanIdentity{costB},
		Schedules: []CostSchedule{{Plan: costB, IntervalSeconds: 3600, CompletionOffsetSeconds: 3600}}}
)

// ran reports one Slot of the group and, when a Plan is named, that Plan's
// evaluation, all timed.
func ran(c *CostSummary, group string, plan CostPlanIdentity) {
	trace := TraceFields{QueryGroupKey: group, QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", EvaluationTime: 1320}
	c.Observe(context.Background(), Observation{Stage: StageSlotCompleted, Result: ResultSuccess, Duration: time.Millisecond, DurationKnown: true, Trace: trace})
	if plan.valid() {
		c.Observe(context.Background(), Observation{Stage: StageEvaluationCompleted, Result: ResultSuccess, Duration: time.Millisecond, DurationKnown: true,
			EvaluationOwner: plan, EvaluationRecordsKnown: true, Trace: trace})
	}
}

// A window is incomplete for a group it should have seen and did not - not
// for one whose next Slot is not due in it. On a live deployment every
// replica tracked a handful of groups on intervals longer than the window,
// had no observation of them, and so read incomplete in every window; the
// flag said nothing. A group or a Plan that was due and left nothing in the
// window is still what makes it incomplete.
func TestAWindowIsIncompleteForWhatWasDueInItAndWentUnseen(t *testing.T) {
	c, now := costDueFixture(t, costMinute, costHourly)
	ran(c, "minute", costA)
	c.Publish(*now)
	coverage := c.Snapshot().Coverage
	if coverage.TrackedGroups != 2 || coverage.ObservedGroups != 1 || coverage.DueGroups != 1 || coverage.UnobservedDueGroups != 0 ||
		coverage.DuePlans != 1 || coverage.UnobservedDuePlans != 0 || coverage.Incomplete {
		t.Fatalf("the hourly group not due in the window = %+v, want complete: nothing due went unseen", coverage)
	}

	c, now = costDueFixture(t, costMinute, costHourly)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.UnobservedDueGroups != 1 || coverage.UnobservedDuePlans != 1 || !coverage.Incomplete {
		t.Fatalf("the minute group missing from its window = %+v, want it counted and the window incomplete", coverage)
	}

	c, now = costDueFixture(t, costMinute, costHourly)
	ran(c, "minute", CostPlanIdentity{})
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.UnobservedDueGroups != 0 || coverage.UnobservedDuePlans != 1 || !coverage.Incomplete {
		t.Fatalf("a due Plan whose group ran without evaluating it = %+v, want the Plan counted and the window incomplete", coverage)
	}

	// A Plan the roster carried two schedules for - a split Plan, one per
	// piece - is read by the one due most often: it is due in this window
	// when either piece is, and unseen it makes the window incomplete.
	split := costHourly
	split.Schedules = []CostSchedule{costHourly.Schedules[0], {Plan: costB, IntervalSeconds: 60, CompletionOffsetSeconds: 60}}
	c, now = costDueFixture(t, costMinute, split)
	ran(c, "minute", costA)
	ran(c, "hourly", CostPlanIdentity{})
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DuePlans != 2 || coverage.UnobservedDuePlans != 1 || !coverage.Incomplete {
		t.Fatalf("a split Plan with a piece due, unevaluated = %+v, want it due and the window incomplete", coverage)
	}

	// A group the roster carried no schedule for is due in every window: a
	// missing schedule does not read as nothing expected.
	unknown := costHourly
	unknown.Schedules = nil
	c, now = costDueFixture(t, costMinute, unknown)
	ran(c, "minute", costA)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DueGroups != 2 || coverage.UnobservedDueGroups != 1 || !coverage.Incomplete {
		t.Fatalf("a group with no schedule, unseen = %+v, want it due and the window incomplete", coverage)
	}
}

// Due is read across the whole window, both buckets: a five-minute group due
// only in the earlier one and unseen makes it incomplete. A group with two
// schedules is due when either is, whichever the roster listed first.
func TestAGroupIsDueAcrossTheWholeWindowAndOnAnyOfItsSchedules(t *testing.T) {
	fiveMinutes := CostGroup{QueryGroupKey: "five", QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", Members: []CostPlanIdentity{costB},
		Schedules: []CostSchedule{{Plan: costB, IntervalSeconds: 300, CompletionOffsetSeconds: 300}}}
	c, now := costDueFixture(t, costMinute, fiveMinutes)
	ran(c, "minute", costA)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DueGroups != 2 || coverage.UnobservedDueGroups != 1 || !coverage.Incomplete {
		t.Fatalf("a group due in the earlier bucket only, unseen = %+v, want it due and the window incomplete", coverage)
	}

	both := costHourly
	both.Schedules = []CostSchedule{costHourly.Schedules[0], {Plan: costB, IntervalSeconds: 60, CompletionOffsetSeconds: 60}}
	c, now = costDueFixture(t, costMinute, both)
	ran(c, "minute", costA)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.DueGroups != 2 || coverage.UnobservedDueGroups != 1 {
		t.Fatalf("a group with an hourly schedule listed before a minute one = %+v, want it due", coverage)
	}
}

// A group's schedules are kept all or none. Kept in part under the metadata
// bound, a group could be read on the one not due and missed on the one
// that was; with none it is due in every window, which only over-reports.
func TestAGroupsSchedulesAreKeptAllOrNone(t *testing.T) {
	split := CostGroup{QueryGroupKey: "split", QueryRevision: "q", SnapshotRevision: "s", ScheduleRevision: "r", Members: []CostPlanIdentity{costB},
		Schedules: []CostSchedule{{Plan: costB, IntervalSeconds: 3600, CompletionOffsetSeconds: 3600}, {Plan: costB, IntervalSeconds: 60, CompletionOffsetSeconds: 60}}}
	now := time.Unix(800, 0)
	// Room for the group, its one Plan and one schedule of its two.
	bytes := len("split") + len("qsr") + len(costB.TenantID) + len(costB.BusinessID) + len(costB.StrategyID) + costDueBytes
	c := NewCostSummary(CostSummaryOptions{ProcessID: "process-a", Window: 5 * time.Minute, GroupCapacity: 8, PlanCapacity: 8,
		MetadataBytes: bytes, TopN: 2, Now: func() time.Time { return now }})
	c.Reconcile([]CostGroup{split}, true)
	now = time.Unix(1440, 0)
	c.Publish(now)
	if coverage := c.Snapshot().Coverage; coverage.TrackedGroups != 1 || coverage.TrackedPlans != 1 || coverage.DueGroups != 1 || coverage.UnobservedDueGroups != 1 {
		t.Fatalf("a group whose schedules did not all fit = %+v, want it tracked, due in the window and unseen", coverage)
	}
}

// A group whose rounds the scheduler held - a query cooldown - has no work
// in the window, and no cost is missing from it: it and its Plans are held,
// not unseen, and the window is complete. A round with nothing due is not a
// hold, and a group whose only rounds were those is unseen. A held round of
// a group the roster does not track is not cost at all.
func TestAGroupTheSchedulerHeldIsHeldNotMissing(t *testing.T) {
	held := func(c *CostSummary, group, outcome string) {
		c.Observe(context.Background(), Observation{Component: ComponentScheduler, Stage: StageRunnerReturned, Result: ResultTerminal,
			RunOutcome: outcome, Trace: TraceFields{QueryGroupKey: group}})
	}
	c, now := costDueFixture(t, costMinute, costHourly)
	held(c, "minute", "query_cooldown")
	held(c, "elsewhere", "query_cooldown")
	c.Publish(*now)
	coverage := c.Snapshot().Coverage
	if coverage.DueGroups != 1 || coverage.HeldDueGroups != 1 || coverage.UnobservedDueGroups != 0 || coverage.HeldDuePlans != 1 ||
		coverage.UnobservedDuePlans != 0 || coverage.ObservedGroups != 0 || coverage.UntrackedObservations != 0 || coverage.Incomplete {
		t.Fatalf("a group held by its cooldown all window = %+v, want it and its Plan held, nothing unseen or untracked, the window complete", coverage)
	}

	// A group that ran as well as being held is not held: a round of it ran
	// without evaluating its due Plan, and that Plan's cost is missing.
	c, now = costDueFixture(t, costMinute, costHourly)
	held(c, "minute", "query_cooldown")
	ran(c, "minute", CostPlanIdentity{})
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.HeldDuePlans != 0 || coverage.UnobservedDuePlans != 1 || !coverage.Incomplete {
		t.Fatalf("a group that ran and was held, its due Plan unevaluated = %+v, want the Plan unseen and the window incomplete", coverage)
	}

	c, now = costDueFixture(t, costMinute, costHourly)
	held(c, "minute", "source_not_due")
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.HeldDueGroups != 0 || coverage.UnobservedDueGroups != 1 || !coverage.Incomplete {
		t.Fatalf("a group whose rounds only had nothing due = %+v, want it unseen and the window incomplete", coverage)
	}
}

// Every word the scheduler returns a round with is decided here, held or
// not, and nothing else is: a new word fails this until someone says which
// it is, instead of reading as not held. Each held word, alone, holds a due
// group for the window.
func TestEveryRunOutcomeIsDecidedHeldOrNot(t *testing.T) {
	notHeld := map[string]bool{"execute_returned": true, "source_not_due": true, "panic": true, "other_error": true}
	known := map[string]bool{}
	for _, outcome := range RunOutcomes {
		known[outcome] = true
		if costHeldOutcomes[outcome] == notHeld[outcome] {
			t.Errorf("run outcome %q is held=%v and not held=%v, want exactly one", outcome, costHeldOutcomes[outcome], notHeld[outcome])
		}
	}
	for outcome := range costHeldOutcomes {
		if !known[outcome] {
			t.Errorf("held outcome %q is no run outcome", outcome)
		}
	}
	for outcome := range costHeldOutcomes {
		c, now := costDueFixture(t, costMinute, costHourly)
		c.Observe(context.Background(), Observation{Component: ComponentScheduler, Stage: StageRunnerReturned, Result: ResultTerminal,
			RunOutcome: outcome, Trace: TraceFields{QueryGroupKey: "minute"}})
		c.Publish(*now)
		if coverage := c.Snapshot().Coverage; coverage.HeldDueGroups != 1 || coverage.Incomplete {
			t.Errorf("a group held by %q = %+v, want it held and the window complete", outcome, coverage)
		}
	}
}

// A group held only in the window's earlier bucket is held for the window.
func TestAGroupHeldInTheEarlierBucketIsHeld(t *testing.T) {
	c, now := costDueFixture(t, costMinute, costHourly)
	*now = time.Unix(1000, 0)
	c.Observe(context.Background(), Observation{Component: ComponentScheduler, Stage: StageRunnerReturned, Result: ResultTerminal,
		RunOutcome: "query_cooldown", Trace: TraceFields{QueryGroupKey: "minute"}})
	*now = time.Unix(1440, 0)
	c.Publish(*now)
	if coverage := c.Snapshot().Coverage; coverage.HeldDueGroups != 1 || coverage.UnobservedDueGroups != 0 || coverage.Incomplete {
		t.Fatalf("a group held in the earlier bucket only = %+v, want it held and the window complete", coverage)
	}
}
