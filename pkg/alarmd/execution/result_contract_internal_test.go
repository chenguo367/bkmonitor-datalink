package execution

import (
	"strings"
	"testing"
)

func TestAnomalousDetectFactAllowsAllValidTriggerOutcomes(t *testing.T) {
	for _, outcome := range []LevelOutcomeKind{LevelOutcomeNormal, LevelOutcomeAbnormal, LevelOutcomeRecovery} {
		if !levelFactMatchesOutcome(LevelFactAnomalous, outcome) {
			t.Fatalf("ANOMALOUS detect fact must allow %s trigger outcome", outcome)
		}
	}
}

func TestUnavailableAndErrorDetectFactsRemainConstrained(t *testing.T) {
	if !levelFactMatchesOutcome(LevelFactUnavailable, LevelOutcomeUnknown) ||
		levelFactMatchesOutcome(LevelFactUnavailable, LevelOutcomeNormal) {
		t.Fatal("UNAVAILABLE detect fact must only allow UNKNOWN")
	}
	if !levelFactMatchesOutcome(LevelFactError, LevelOutcomeTerminal) ||
		levelFactMatchesOutcome(LevelFactError, LevelOutcomeRecovery) {
		t.Fatal("ERROR detect fact must only allow TERMINAL")
	}
}

// The description renders what each comparison saw, per Level and per
// series: outcomes are counted for the outcome's own Level only, the fold
// is this round's and empty when none is proposed, the markers are shown as
// loaded and as final, and the State guard is the one after the round.
func TestDescribeMissingGuardRendersEachComparisonPerLevel(t *testing.T) {
	plan := PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	series := SeriesIdentityDigest("series-a")
	identity := StateKeyIdentity{Plan: plan, StateGeneration: "g", SeriesIdentityDigest: series}
	outcome := LevelOutcome{Plan: plan, LevelID: 5, SeriesIdentityDigest: series, Outcome: LevelOutcomeUnknown, ReasonCode: "QUERY_PARTIAL"}
	result := PlanEvaluationResult{
		Plan: plan,
		LevelOutcomes: []LevelOutcome{outcome, {Plan: plan, LevelID: 5, SeriesIdentityDigest: series, Outcome: LevelOutcomeUnknown, ReasonCode: "QUERY_PARTIAL"},
			{Plan: plan, LevelID: 6, SeriesIdentityDigest: series, Outcome: LevelOutcomeNormal},
			{Plan: plan, LevelID: 5, SeriesIdentityDigest: "series-b", Outcome: LevelOutcomeUnknown, ReasonCode: "QUERY_PARTIAL"}},
		StateResults: []StateEvaluation{{Mutation: StateMutation{Identity: identity}}},
	}
	input := InternalExecution{Inputs: []NamedInputBinding{{
		Consumer: ConsumerRef{Plan: plan, LevelID: 5, HasLevel: true}, Completeness: CompletenessPartial, ReasonCode: "QUERY_PARTIAL",
	}}}
	loaded := map[GapScope]GapScopeState{{}: {Status: GapStatusGapped, ReasonCode: "GAP_SKIPPED"}}
	final := map[GapScope]GapScopeState{{}: {Status: GapStatusGapped, ReasonCode: "GAP_SKIPPED"},
		{HasLevel: true, LevelID: 5}: {Status: GapStatusWarming, ReasonCode: "QUERY_PARTIAL"}}
	states := map[StateKeyIdentity]RuntimeStateView{identity: {Identity: identity,
		SeriesGuard: &StateGuardFact{ReasonCode: "RECORD_INVALID"},
		Levels: []RuntimeLevelStateView{{LevelID: 5, HistoryCompleteness: HistoryGapped, GapReasonCode: "GAP_SKIPPED"},
			{LevelID: 6, HistoryCompleteness: HistoryFull}}}}

	got := describeMissingGuard(input, result, loaded, final, states, outcome)
	for _, want := range []string{
		"outcome UNKNOWN", "reason QUERY_PARTIAL", "level 5", "outcomes for level 2", "input full no",
		"round fold QUERY_PARTIAL", "state series guard RECORD_INVALID", "state level guard GAPPED/GAP_SKIPPED",
		"state written yes", "marker plan loaded GAPPED/GAP_SKIPPED", "marker level loaded none",
		"marker plan final GAPPED/GAP_SKIPPED", "marker level final WARMING/QUERY_PARTIAL", "guard proposed no",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("description %q does not say %q", got, want)
		}
	}
	// No fold when the round's inputs for the Level are all FULL, and
	// nothing found reads none, not empty.
	input.Inputs[0].Completeness = CompletenessFull
	bare := describeMissingGuard(input, PlanEvaluationResult{Plan: plan, LevelOutcomes: []LevelOutcome{outcome}}, nil, nil, nil, outcome)
	for _, want := range []string{"outcomes for level 1", "round fold none", "state series guard none", "state level guard none",
		"state written no", "marker plan loaded none", "marker level final none", "input full no"} {
		if !strings.Contains(bare, want) {
			t.Fatalf("bare description %q does not say %q", bare, want)
		}
	}
}
