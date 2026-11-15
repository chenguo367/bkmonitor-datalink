// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package execution

import "testing"

// The steady state this measurement exists for is a series whose Level state
// stands still while the history window moves under it. These build that shape
// explicitly rather than deriving it, so a change to the window's contents
// cannot quietly turn the case being tested into a different one.
func steadyLevels() ([]RuntimeLevelStateView, []RuntimeLevelStateMutation) {
	stored := []RuntimeLevelStateView{{
		LevelID: 1, LevelStateCompatibility: "compat-1", HistoryCompleteness: HistoryFull,
		GapReasonCode: "", WarmupRequirementRef: "warm-1", LastProcessedEventTime: 1700,
	}}
	next := []RuntimeLevelStateMutation{{
		LevelID: 1, LevelStateCompatibility: "compat-1", HistoryCompleteness: HistoryFull,
		GapReasonCode: "", WarmupRequirementRef: "warm-1", LastProcessedEventTime: 1700,
	}}
	return stored, next
}

func TestWriteReuseReportsUnobservedWhenNothingIsStored(t *testing.T) {
	stored, next := steadyLevels()
	view := RuntimeStateView{Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-a", Levels: next}
	// The digests differ here on purpose: with nothing stored there is no
	// comparison to make, and reporting that as "changed" would let a starting
	// worker's warm-up read as a series that is never reusable.
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseUnobserved {
		t.Fatalf("no stored digest classified as %q, want %q", got, StateWriteReuseUnobserved)
	}
}

func TestWriteReuseReportsIdenticalWhenTheWholeBlobRepeats(t *testing.T) {
	stored, next := steadyLevels()
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-a", Levels: next}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseIdentical {
		t.Fatalf("repeated blob classified as %q, want %q", got, StateWriteReuseIdentical)
	}
}

func TestWriteReuseReportsDecisionStableWhenOnlyTheWindowMoved(t *testing.T) {
	stored, next := steadyLevels()
	view := RuntimeStateView{
		PersistedMutationDigest: "digest-a", Levels: stored,
		History: []StateHistoryPoint{{RecordID: "r1", SourceTime: 1700}},
	}
	mutation := StateMutation{
		MutationDigest: "digest-b", Levels: next,
		Points: []StateHistoryPoint{{RecordID: "r2", SourceTime: 1760}},
	}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseDecisionStable {
		t.Fatalf("moved window classified as %q, want %q", got, StateWriteReuseDecisionStable)
	}
}

// A series that never leaves history warming writes the same Level state
// every round, but it decides nothing: it is not the steady series a skip is
// sized for. decision_stable asks the decision, so it reads changed however
// still its metadata stands.
func TestWriteReuseDoesNotReadAStandingWarmingSeriesAsDecisionStable(t *testing.T) {
	stored := []RuntimeLevelStateView{{
		LevelID: 1, LevelStateCompatibility: "compat-1", HistoryCompleteness: HistoryWarming,
		GapReasonCode: "HISTORY_WARMING", WarmupRequirementRef: "warm-1", LastProcessedEventTime: 1700,
	}}
	next := []RuntimeLevelStateMutation{{
		LevelID: 1, LevelStateCompatibility: "compat-1", HistoryCompleteness: HistoryWarming,
		GapReasonCode: "HISTORY_WARMING", WarmupRequirementRef: "warm-1", LastProcessedEventTime: 1760,
	}}
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-b", Levels: next}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassWarming); got != StateWriteReuseChanged {
		t.Fatalf("standing warming series classified as %q, want %q", got, StateWriteReuseChanged)
	}
}

// The processed event time advances every round a Level is processed,
// whatever it decided. Compared, it made every series that was written before
// read as changed, and decision_stable stayed at zero from the first release
// for that reason. A series that decided stable_normal and moved nothing else is
// decision_stable; the same metadata under a round that counted towards a
// trigger or opened an alert is not, because the metadata cannot tell those
// apart and the decision can.
func TestWriteReuseReadsAStableSeriesWhoseProcessedTimeAdvancedAsDecisionStable(t *testing.T) {
	stored, next := steadyLevels()
	next[0].LastProcessedEventTime = 1760
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-b", Levels: next}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseDecisionStable {
		t.Fatalf("stable series with an advanced processed time classified as %q, want %q", got, StateWriteReuseDecisionStable)
	}
	for _, class := range []SeriesClass{SeriesClassCountingTrigger, SeriesClassCountingRecovery, SeriesClassTransition,
		SeriesClassGapped, SeriesClassWarming, SeriesClassOther, SeriesClassNone} {
		if got := ClassifyStateWriteReuse(view, mutation, class); got != StateWriteReuseChanged {
			t.Fatalf("a %q round with the same metadata classified as %q, want %q", class, got, StateWriteReuseChanged)
		}
	}
}

func TestWriteReuseReportsChangedWhenCompletenessMoves(t *testing.T) {
	stored, next := steadyLevels()
	next[0].HistoryCompleteness = HistoryWarming
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-b", Levels: next}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseChanged {
		t.Fatalf("moved completeness classified as %q, want %q", got, StateWriteReuseChanged)
	}
}

func TestWriteReuseReportsChangedWhenTheSeriesGuardAppears(t *testing.T) {
	stored, next := steadyLevels()
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{
		MutationDigest: "digest-b", Levels: next,
		SeriesGuard: &StateGuardFact{Status: HistoryWarming, ReasonCode: "HISTORY_WARMING", WarmupRequirementRef: "warm-1"},
	}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseChanged {
		t.Fatalf("appearing series guard classified as %q, want %q", got, StateWriteReuseChanged)
	}
}

func TestWriteReuseReportsChangedWhenALevelIsAdded(t *testing.T) {
	stored, next := steadyLevels()
	next = append(next, RuntimeLevelStateMutation{
		LevelID: 2, LevelStateCompatibility: "compat-2", HistoryCompleteness: HistoryFull,
		WarmupRequirementRef: "warm-2", LastProcessedEventTime: 1700,
	})
	view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
	mutation := StateMutation{MutationDigest: "digest-b", Levels: next}
	if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseChanged {
		t.Fatalf("added Level classified as %q, want %q", got, StateWriteReuseChanged)
	}
}

// Each field of the decision state is compared on its own: a mutation that
// moves only that field is changed. Left out of the comparison, a field that
// moved would read as a reusable write for a series that moved, which is the
// one answer that must never be given.
func TestWriteReuseReportsChangedWhenAnyComparedFieldMoves(t *testing.T) {
	for name, move := range map[string]func(levels []RuntimeLevelStateMutation, mutation *StateMutation){
		"compatibility": func(levels []RuntimeLevelStateMutation, _ *StateMutation) {
			levels[0].LevelStateCompatibility = "compat-2"
		},
		"completeness": func(levels []RuntimeLevelStateMutation, _ *StateMutation) {
			levels[0].HistoryCompleteness = HistoryWarming
		},
		"gap reason":    func(levels []RuntimeLevelStateMutation, _ *StateMutation) { levels[0].GapReasonCode = "HISTORY_GAPPED" },
		"warm-up ref":   func(levels []RuntimeLevelStateMutation, _ *StateMutation) { levels[0].WarmupRequirementRef = "warm-2" },
		"level missing": func(levels []RuntimeLevelStateMutation, _ *StateMutation) { levels[0].LevelID = 2 },
		"series guard": func(_ []RuntimeLevelStateMutation, mutation *StateMutation) {
			mutation.SeriesGuard = &StateGuardFact{Status: HistoryWarming, ReasonCode: "HISTORY_WARMING", WarmupRequirementRef: "warm-1"}
		},
	} {
		stored, next := steadyLevels()
		view := RuntimeStateView{PersistedMutationDigest: "digest-a", Levels: stored}
		mutation := StateMutation{MutationDigest: "digest-b", Levels: next}
		move(next, &mutation)
		if got := ClassifyStateWriteReuse(view, mutation, SeriesClassStableNormal); got != StateWriteReuseChanged {
			t.Fatalf("%s moved, classified as %q, want %q", name, got, StateWriteReuseChanged)
		}
	}
}
