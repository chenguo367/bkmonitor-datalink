// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import "testing"

// The two facts a limited tracking horizon stores have to reach the record,
// and the store decides what reaches the record by comparing digests. A field
// the digest does not cover is a field two otherwise identical statements
// agree on, and the store answers "already applied" to the second one.
//
// The Plan-level fact is the one at risk: it is not in the group list, so
// nothing else about the memory moves when it is set. A round that only
// exhausts the roster states exactly the same groups as the round before it.
func TestTheDigestCoversThePlanLevelTrackingFact(t *testing.T) {
	group := NoDataGroupMemory{GroupKey: "a", LastSeen: 80, FirstAbsent: 85}

	tracked := noDataUpdate(group)
	exhausted := noDataUpdate(group)
	exhausted.TrackingExhaustedAt = 90

	first, err := BuildPlanNoDataMutation(tracked)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlanNoDataMutation(exhausted)
	if err != nil {
		t.Fatal(err)
	}
	if first.MemoryDigest == second.MemoryDigest {
		t.Fatal("a memory whose roster was exhausted carries the same digest as one still tracking; " +
			"the store answers already-applied on a digest match, so the write that set it is dropped")
	}
	if first.MutationDigest == second.MutationDigest {
		t.Fatal("two statements differing only in the Plan-level tracking fact carry one statement digest")
	}
	if second.TrackingExhaustedAt != 90 {
		t.Fatalf("built mutation carries tracking-exhausted %d, want 90", second.TrackingExhaustedAt)
	}
}

// Suppression is per group and has to survive the delta, which is built by
// listing fields rather than copying the struct. A group whose suppression is
// dropped on the way out is written back as still tracked, and the next round
// resumes producing its absence - the one outcome the horizon exists to stop.
func TestSuppressionReachesTheDeltaAndMovesTheDigest(t *testing.T) {
	absent := NoDataGroupMemory{GroupKey: "a", LastSeen: 80, FirstAbsent: 85}
	suppressed := absent
	suppressed.SuppressedAt = 90

	update := noDataUpdate(suppressed)
	update.Loaded = []NoDataGroupMemory{absent}
	update.LoadedPresentAsOf = 90

	built, err := BuildPlanNoDataMutation(update)
	if err != nil {
		t.Fatal(err)
	}
	if len(built.Set) != 1 {
		t.Fatalf("a round that suppressed a group wrote %d changes, want the one group", len(built.Set))
	}
	if built.Set[0].Absent == nil {
		t.Fatal("a suppressed group was written in the compressed present form")
	}
	if built.Set[0].Absent.SuppressedAt != 90 {
		t.Fatalf("delta carries suppressed-at %d, want 90; the group would be read back as still tracked",
			built.Set[0].Absent.SuppressedAt)
	}
	// Same memory but for the suppression: the digests must differ, or a
	// same-version retry of the round before this one reads as already applied.
	tracked, err := BuildPlanNoDataMutation(noDataUpdate(absent))
	if err != nil {
		t.Fatal(err)
	}
	if tracked.MemoryDigest == built.MemoryDigest {
		t.Fatal("suppressing a group left the memory digest where it was")
	}
}

// A round that changes nothing still writes nothing, suppression included.
// Without this the pair above would pass on an implementation that wrote every
// group every round, which is the shape the per-group record replaced.
func TestASuppressedGroupThatDidNotChangeIsNotWrittenAgain(t *testing.T) {
	suppressed := NoDataGroupMemory{GroupKey: "a", LastSeen: 80, FirstAbsent: 85, SuppressedAt: 90}
	update := noDataUpdate(suppressed)
	update.Loaded = []NoDataGroupMemory{suppressed}
	update.LoadedPresentAsOf = 90

	built, err := BuildPlanNoDataMutation(update)
	if err != nil {
		t.Fatal(err)
	}
	if len(built.Set) != 0 || len(built.Del) != 0 {
		t.Fatalf("a round that changed nothing wrote %d sets and %d deletes", len(built.Set), len(built.Del))
	}
}

// A record written before the horizon existed carries neither fact, and zero
// is the right reading for both: no build that wrote one had a horizon to stop
// anything with. This is the whole of "new process reads old".
func TestARecordWithoutTheTrackingFactsReadsAsStillTracking(t *testing.T) {
	built, err := BuildPlanNoDataMutation(noDataUpdate(
		NoDataGroupMemory{GroupKey: "a", LastSeen: 80, FirstAbsent: 85},
	))
	if err != nil {
		t.Fatal(err)
	}
	if built.TrackingExhaustedAt != 0 {
		t.Fatalf("a Plan nobody exhausted carries tracking-exhausted %d", built.TrackingExhaustedAt)
	}
	if built.Set[0].Absent == nil || built.Set[0].Absent.SuppressedAt != 0 {
		t.Fatal("a group nobody suppressed carries a suppression")
	}
	if !NoDataMemoryReadable(NoDataMemorySchemaV2) || !NoDataMemoryReadable(NoDataMemorySchemaV1) {
		t.Fatal("this build must go on reading the records written before it")
	}
}
