package worker_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func assertQueryAvailability(t *testing.T, result execution.SlotExecutionResult, want execution.QueryAvailability) {
	t.Helper()
	if result.QueryAvailability != want {
		t.Fatalf("QueryAvailability=%v, want %v", result.QueryAvailability, want)
	}
}

// An unavailable result also says why, in the primary binding's own words,
// and what that query had asked for: the query cooldown it feeds reports its
// entries under that reason and keeps those targets. A result that is not
// unavailable carries neither; the query line names the targets of any query
// that came back unavailable, whichever the result.
func TestUnavailableQueryAvailabilityRequiresBackendFailure(t *testing.T) {
	unavailable, targetMissing := execution.ReasonCode(contract.ReasonQueryUnavailable), execution.ReasonCode(contract.ReasonQueryTargetMissing)
	timeout := execution.ReasonCode("QUERY_TIMEOUT")
	transportTimeout := execution.TransportRouteDetail(execution.TransportFailureTimeout)
	bodyTimeout := execution.BodyRouteDetail(execution.TransportFailureTimeout)
	// window is a failed attempt's timing: how late it went out, its budget
	// from there, how long it waited, and how much of that was alarmd's own.
	window := func(startLate, budget, elapsed, local int64) *execution.AttemptTiming {
		return &execution.AttemptTiming{StartLateMillis: startLate, BudgetMillis: budget, ElapsedMillis: elapsed, LocalMillis: local}
	}
	for _, test := range []struct {
		name, detail string
		reason       execution.ReasonCode
		want         execution.QueryAvailability
		timing       *execution.AttemptTiming
	}{
		{"backend status", execution.HTTPStatusRouteDetail(503), unavailable, execution.QueryAvailabilityUnavailable, nil},
		{"backend response", "response=status_table_not_found", unavailable, execution.QueryAvailabilityUnavailable, nil},
		{"target missing", "response=status_space_table_id_field_is_not_exists", targetMissing, execution.QueryAvailabilityUnavailable, nil},
		// An answer past a response limit is the same answer every time it is
		// fetched, so it is evidence for the degraded pool like a backend
		// status, not an unknown.
		{"response limit", execution.ResponseRouteDetail(execution.ResponseFailureLimitTotalSeries), unavailable, execution.QueryAvailabilityUnavailable, nil},
		{"admission or unknown", "", unavailable, execution.QueryAvailabilityUnknown, nil},
		{"transport", transportTimeout, unavailable, execution.QueryAvailabilityUnknown, nil},
		// A query that went out on time and waited out its deadline on the
		// backend is the backend not answering, the costliest failure there
		// is: it holds a permit to the deadline every round. It is evidence
		// for the pool like a status the backend sent, under its own reason.
		{"timeout the backend ran out", transportTimeout, timeout, execution.QueryAvailabilityUnavailable, window(200, 40_000, 40_000, 0)},
		{"timeout the backend ran out mid-answer", bodyTimeout, timeout, execution.QueryAvailabilityUnavailable, window(200, 40_000, 40_000, 500)},
		// A query begun late -- most of its window spent waiting for a permit
		// or behind alarmd's own work -- that then times out says nothing
		// about the backend. The line is where more of the window went to the
		// backend than to alarmd: equal is not enough, one millisecond more is.
		{"timeout of a query begun late", transportTimeout, timeout, execution.QueryAvailabilityUnknown, window(30_000, 2_000, 2_000, 0)},
		{"timeout split evenly with alarmd", transportTimeout, timeout, execution.QueryAvailabilityUnknown, window(10_000, 10_000, 10_000, 0)},
		{"timeout one millisecond more on the backend", transportTimeout, timeout, execution.QueryAvailabilityUnavailable, window(10_000, 10_001, 10_001, 0)},
		{"timeout mostly delivering the answer", bodyTimeout, timeout, execution.QueryAvailabilityUnknown, window(200, 40_000, 40_000, 30_000)},
		{"timeout with no timing", transportTimeout, timeout, execution.QueryAvailabilityUnknown, nil},
		{"delivery timeout", execution.DeliveryTimeoutRouteDetail, timeout, execution.QueryAvailabilityUnknown, window(200, 40_000, 40_000, 39_000)},
		{"refused connection with time to spare", execution.TransportRouteDetail(execution.TransportFailureConnectionRefused), unavailable, execution.QueryAvailabilityUnknown, window(200, 40_000, 40_000, 0)},
		// Only a query that ran out its own deadline is a timeout here: a
		// transport that gave up on its own timer names the query unavailable,
		// not timed out, and a deadline that ran out while alarmd delivered the
		// answer is alarmd's, whatever the timing says.
		{"a transport timer under another reason", transportTimeout, unavailable, execution.QueryAvailabilityUnknown, window(200, 40_000, 40_000, 0)},
		{"delivery timeout however the window went", execution.DeliveryTimeoutRouteDetail, timeout, execution.QueryAvailabilityUnknown, window(200, 40_000, 40_000, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			asked := &observability.RequestTargetsFacts{Count: 1, Digest: "0123456789abcdef",
				Targets: []observability.RequestTarget{{TableID: "system.cpu", FieldName: "usage", SpaceUID: "bkcc__2"}}}
			plans, requirements := baseDuePlanAndRequirements()
			fixture, request := newCompletionOnlyFixture(t, plans, requirements, func(completion *execution.QueryExecutionCompletion) {
				physical := &completion.PhysicalQueries[0]
				physical.Completeness, physical.DataState = execution.CompletenessUnavailable, execution.DataStateUnknown
				physical.RouteFacts.Attempts = []execution.RouteAttemptFact{{AttemptNo: 1, Result: execution.RouteAttemptFailed, ReasonCode: test.reason, Detail: test.detail, Timing: test.timing}}
				physical.RouteFacts.RequestTargets = asked
				for i := range completion.CompletionBindings {
					binding := &completion.CompletionBindings[i]
					binding.Dataset, binding.View = nil, nil
					binding.Completeness, binding.DataState = execution.CompletenessUnavailable, execution.DataStateUnknown
					binding.Disposition, binding.ReasonCode = execution.AccessUnavailable, test.reason
				}
			})
			result, err := fixture.coordinator.Execute(context.Background(), request)
			if err != nil || !result.Completed {
				t.Fatalf("Execute() result=%+v error=%v", result, err)
			}
			assertQueryAvailability(t, result, test.want)
			if want := map[bool]execution.ReasonCode{true: test.reason}[test.want == execution.QueryAvailabilityUnavailable]; result.QueryUnavailableReason != want {
				t.Fatalf("QueryUnavailableReason=%q, want %q", result.QueryUnavailableReason, want)
			}
			if want := map[bool]*observability.RequestTargetsFacts{true: asked}[test.want == execution.QueryAvailabilityUnavailable]; result.QueryUnavailableTargets != want {
				t.Fatalf("QueryUnavailableTargets=%+v, want %+v", result.QueryUnavailableTargets, want)
			}
			lines := 0
			for _, observation := range *fixture.observations {
				if observation.Stage == observability.StageQueryCompleted {
					lines++
					if !reflect.DeepEqual(observation.RequestTargets, asked) {
						t.Fatalf("query_completed request targets = %+v, want %+v", observation.RequestTargets, asked)
					}
				}
			}
			if lines == 0 {
				t.Fatal("no query_completed line")
			}
		})
	}
}

func TestQueryAvailabilityRequiresProgressCommit(t *testing.T) {
	plans, requirements := baseDuePlanAndRequirements()
	fixture, request := newCompletionOnlyFixture(t, plans, requirements, nil)
	fixture.ports.progressConflict = true
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err == nil || result.Completed {
		t.Fatalf("Execute() result=%+v error=%v", result, err)
	}
	assertQueryAvailability(t, result, execution.QueryAvailabilityUnknown)
}

// A round whose queries all answered names no request targets, on its result
// or its query line, though its route carried them.
func TestAnAnsweredRoundCarriesNoRequestTargets(t *testing.T) {
	plans, requirements := baseDuePlanAndRequirements()
	fixture, request := newCompletionOnlyFixture(t, plans, requirements, func(completion *execution.QueryExecutionCompletion) {
		completion.PhysicalQueries[0].RouteFacts.RequestTargets = &observability.RequestTargetsFacts{Count: 1,
			Targets: []observability.RequestTarget{{TableID: "system.cpu", FieldName: "usage", SpaceUID: "bkcc__2"}}}
	})
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v error=%v", result, err)
	}
	assertQueryAvailability(t, result, execution.QueryAvailabilityAvailable)
	if result.QueryUnavailableTargets != nil {
		t.Fatalf("an answered round kept request targets %+v", result.QueryUnavailableTargets)
	}
	for _, observation := range *fixture.observations {
		if observation.Stage == observability.StageQueryCompleted && observation.RequestTargets != nil {
			t.Fatalf("an answered query line carries request targets %+v", observation.RequestTargets)
		}
	}
}
