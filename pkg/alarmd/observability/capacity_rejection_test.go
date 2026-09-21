package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCapacityRejectionLogWhitelistAndUnknownOwnUsage(t *testing.T) {
	zero := uint64(0)
	for _, test := range []struct {
		phase     string
		own       *uint64
		wantPhase string
	}{
		{"normal_output", &zero, "normal_output"},
		{"query_free", nil, "query_free"},
		{"https://user:secret@example.test/query", nil, "other"},
	} {
		var output bytes.Buffer
		limiter, _ := NewWindowLogLimiter(WindowLogLimiterConfig{Window: time.Hour, MaxEvents: 10})
		policy, _ := NewBoundedLogPolicy(limiter)
		NewLoggingObserver(New("alarmd", &output), policy).Observe(context.Background(), Observation{
			Component: ComponentResource, Stage: StageResourceHard, Result: ResultPaused, Err: errors.New("reserve https://user:secret@example.test/query: rejected"),
			CapacityBudget: CapacityBudgetRetainedBytes, CapacityRejection: &CapacityRejectionFacts{Phase: test.phase, OwnUsed: test.own, SharedUsed: 60, Requested: 50, Limit: 100},
		})
		if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "https://") {
			t.Fatalf("unbounded log: %s", output.String())
		}
		var row map[string]any
		if err := json.Unmarshal(output.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if row["capacity_phase"] != test.wantPhase {
			t.Fatal(row)
		}
		if row["error"] != "reserve <url>: rejected" {
			t.Fatalf("sanitized error text=%#v", row["error"])
		}
		own, present := row["capacity_own_used"]
		if present != (test.own != nil) || (present && own != float64(0)) {
			t.Fatalf("unknown confused with zero: %v", row)
		}
		if row["capacity_shared_used"] != float64(60) || row["capacity_requested"] != float64(50) || row["capacity_limit"] != float64(100) {
			t.Fatal(row)
		}
	}
}

// A rejection reports what every budget had taken, not only the one that
// refused.
//
// This is the sentence decision-019 could not get anyone to say from the logs:
// the count budget refused while the byte budget it was standing in for was
// almost untouched. With only the budget that was reached on the line, the two
// readings - a process at its memory limit, and a process stopped by a number
// that was supposed to represent memory - arrive identically, and the second
// one went a year without being visible.
func TestARejectionReportsEveryBudgetNotOnlyTheOneThatRefused(t *testing.T) {
	own := uint64(524288)
	var output bytes.Buffer
	limiter, _ := NewWindowLogLimiter(WindowLogLimiterConfig{Window: time.Hour, MaxEvents: 10})
	policy, _ := NewBoundedLogPolicy(limiter)
	NewLoggingObserver(New("alarmd", &output), policy).Observe(context.Background(), Observation{
		Component: ComponentResource, Stage: StageResourceHard, Result: ResultPaused,
		Err:            errors.New("rejected"),
		CapacityBudget: CapacityBudgetStateMutations,
		CapacityRejection: &CapacityRejectionFacts{
			Phase: "normal_output", OwnUsed: &own, SharedUsed: 524288, Requested: 1, Limit: 524288,
			Usage: []CapacityBudgetUsage{
				{Budget: CapacityBudgetStateMutations, OwnUsed: 524288, Limit: 524288},
				{Budget: CapacityBudgetRetainedBytes, OwnUsed: 107374182, Limit: 1073741824},
				{Budget: "a_budget_this_build_does_not_name", OwnUsed: 7, Limit: 9},
			},
		},
	})
	var row map[string]any
	if err := json.Unmarshal(output.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	// The budget that refused, and one that did not, both readable against
	// their own limits. Ten percent of the byte budget while the count is at
	// its ceiling is the whole finding.
	for key, want := range map[string]any{
		"capacity_own_state_mutations":   float64(524288),
		"capacity_limit_state_mutations": float64(524288),
		"capacity_own_retained_bytes":    float64(107374182),
		"capacity_limit_retained_bytes":  float64(1073741824),
	} {
		if row[key] != want {
			t.Fatalf("%s = %#v, want %v; without it the line cannot say which budget was under pressure", key, row[key], want)
		}
	}
	if _, leaked := row["capacity_own_a_budget_this_build_does_not_name"]; leaked {
		t.Fatalf("a budget name this build does not know reached the log line: %v", row)
	}
}

// The completion row carries the same quantities whatever the outcome, so the
// rejections have a distribution to be read against.
func TestTheCompletionRowCarriesBudgetUsageOnASuccess(t *testing.T) {
	var output bytes.Buffer
	limiter, _ := NewWindowLogLimiter(WindowLogLimiterConfig{Window: time.Hour, MaxEvents: 10})
	policy, _ := NewBoundedLogPolicy(limiter)
	NewLoggingObserver(New("alarmd", &output), policy).Observe(context.Background(), Observation{
		Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultSuccess,
		Direction: DirectionInternal,
		SlotBudgetUsage: &SlotBudgetUsageFacts{
			StateMutations: 19539, RetainedBytes: 68681728,
			StateMutationsLimit: 524288, RetainedBytesLimit: 1073741824,
		},
	})
	var row map[string]any
	if err := json.Unmarshal(output.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	usage, present := row["slot_budget_usage"].(map[string]any)
	if !present {
		t.Fatalf("a successful Slot reported no budget usage: %v", row)
	}
	if usage["state_mutations"] != float64(19539) || usage["retained_bytes"] != float64(68681728) {
		t.Fatalf("slot_budget_usage = %v, want what this Slot took", usage)
	}
	// The limits travel with it: a usage nobody can compare is not a reading.
	if usage["state_mutations_limit"] != float64(524288) || usage["retained_bytes_limit"] != float64(1073741824) {
		t.Fatalf("slot_budget_usage = %v, want the limits it was measured against", usage)
	}
}
