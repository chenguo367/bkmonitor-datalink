// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// partReplicas are tallyReplicas with pooled objects whose cooldowns have
// ended at different moments or not yet, and empty-every-round rows.
func partReplicas() []Snapshot {
	snapshots := tallyReplicas()
	for index := range snapshots {
		snapshot := &snapshots[index]
		ended := now.Add(-time.Duration(index+1) * 7 * time.Minute)
		snapshot.Demoted[0].QueryCooldown = &observability.QueryCooldownFacts{Until: ended}
		// Each replica's cooling reason dated differently, so the earliest
		// is one replica's and not every replica's.
		snapshot.Demoted[0].ReasonSince = now.Add(-time.Duration(index+1) * time.Hour)
		later := snapshot.Demoted[0]
		later.QueryGroup += "-later"
		later.QueryCooldown = &observability.QueryCooldownFacts{Until: now.Add(time.Hour)}
		snapshot.Demoted = append(snapshot.Demoted, later)
		snapshot.TotalDemoted++
		// Two periods on each replica, one of them shared, and each replica's
		// census of them.
		for row := range snapshot.Anomalies {
			interval := int64(60 * (1 + (row+index)%2))
			snapshot.Anomalies[row].Wake = &WakeFacts{Known: true, IntervalSeconds: interval, DueAt: now.Add(time.Minute)}
		}
		// More of each list than the first screen carries on one replica,
		// fewer on the others, with values that interleave across replicas.
		snapshot.PrunedSkips = map[string]PrunedSkip{}
		for n := 0; n < 3+index*4; n++ {
			queryGroup := fmt.Sprintf("%s-pruned-%d", snapshot.Replica, n)
			snapshot.PrunedSkips[queryGroup] = PrunedSkip{From: 0, To: int64(60 * (3*n + index)), At: now.Add(-time.Hour)}
			snapshot.RetainedShare = append(snapshot.RetainedShare, Anomaly{QueryGroup: queryGroup, Replica: snapshot.Replica,
				RetainedShare: &RetainedShareFacts{PercentOfShare: uint64(50 + 3*n + index)}})
			snapshot.ReadEarly = append(snapshot.ReadEarly, Anomaly{QueryGroup: queryGroup, Replica: snapshot.Replica,
				ReadEarly: &ReadEarlyFacts{CurrentDelaySeconds: 0, SuggestedDelaySeconds: int64(15 * (3*n + index))}})
		}
		snapshot.Schedule = &ScheduleCensus{Waiting: 90, Cooling: 2, Cohorts: []ScheduleCohort{
			{IntervalSeconds: 60, Objects: 50 + index, Cooling: 1}, {IntervalSeconds: 120, Objects: 40, Cooling: 1}}}
		for n := 0; n <= index; n++ {
			snapshot.NoData = append(snapshot.NoData, Anomaly{QueryGroup: snapshot.Replica + "-empty-" + string(rune('a'+n)),
				Kind: KindEmptyEveryRound, ReasonCode: "FULL_EMPTY_COMPLETED", Replica: snapshot.Replica,
				Since: now.Add(-time.Duration(index*3+n+1) * 37 * time.Minute)})
		}
		// Each replica's rows failing since a different moment, a few objects
		// no replica has said anything conclusive about, a loss in progress
		// on two replicas at different moments, and a pooled object's skip.
		for row := range snapshot.Anomalies {
			snapshot.Anomalies[row].Since = now.Add(-3*time.Hour - time.Duration(index*10+row)*time.Minute)
		}
		snapshot.Determined -= index
		if snapshot.GapSkips == nil {
			snapshot.GapSkips = map[string]SkippedSpan{}
		}
		if index > 0 {
			snapshot.GapSkips[snapshot.Replica+"-lost"] = SkippedSpan{FirstSlot: 1, LastSlot: 3, Slots: 3,
				At: now.Add(-time.Duration(index*3) * time.Minute), Replica: snapshot.Replica}
		}
		snapshot.GapSkips[snapshot.Demoted[0].QueryGroup] = SkippedSpan{FirstSlot: 1, LastSlot: 2, Slots: 2,
			At: now.Add(-time.Duration(index+1) * 4 * time.Minute), Replica: snapshot.Replica}
	}
	return snapshots
}

func decidedView(snapshots []Snapshot) View {
	names := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		names = append(names, snapshot.Replica)
	}
	view := Aggregate(Expectation{QueryGroups: 300, Known: true}, snapshots, names, now, freshness)
	Decide(&view, now, 10*time.Minute)
	return view
}

// The replicas' parts, each read from a view of its own snapshot and
// merged, give every number the whole view gives from rows: the verdict's
// counts, each replica's split, the empty-every-round count, the pooled
// objects due and how long the earliest has waited, and the impact.
func TestReplicaPartsAddUpToTheWholeViewsRowNumbers(t *testing.T) {
	snapshots := partReplicas()
	whole := decidedView(snapshots)
	parts := make([]ReplicaPart, 0, len(snapshots))
	for _, snapshot := range snapshots {
		part := ReplicaPartOf(decidedView([]Snapshot{snapshot}), now)
		if part.Replica != snapshot.Replica {
			t.Fatalf("part of %s names %q", snapshot.Replica, part.Replica)
		}
		parts = append(parts, part)
		for _, replica := range whole.PerReplica {
			if replica.Replica != snapshot.Replica {
				continue
			}
			tally := part.Attribution
			if replica.Ours != tally.Ours || replica.External != tally.External || replica.Unattributed != tally.Unknown+tally.Other {
				t.Errorf("%s split ours/external/unattributed = %d/%d/%d, part %+v", replica.Replica,
					replica.Ours, replica.External, replica.Unattributed, tally)
			}
		}
	}
	merged := MergeReplicaParts(parts...)
	unattributed := 0
	for _, replica := range whole.PerReplica {
		unattributed += replica.Unattributed
	}
	if merged.Attribution.Unknown+merged.Attribution.Other != unattributed {
		t.Errorf("merged unattributed %+v, the replicas' split adds up to %d", merged.Attribution, unattributed)
	}
	if merged.Attribution.Ours != OursCount(whole.Anomalies) || merged.Attribution.Unknown != UnattributedCount(whole.Anomalies) {
		t.Errorf("merged attribution %+v, whole view ours %d unattributed %d", merged.Attribution,
			OursCount(whole.Anomalies), UnattributedCount(whole.Anomalies))
	}
	if merged.EmptyEveryRound != whole.EmptyEveryRoundTotal {
		t.Errorf("empty every round %d, whole view %d", merged.EmptyEveryRound, whole.EmptyEveryRoundTotal)
	}
	if merged.DemotedDue != whole.DemotedDue || merged.DemotedDueOldestSeconds(now) != whole.DemotedDueOldestSeconds {
		t.Errorf("pooled due %d oldest %ds, whole view %d oldest %ds", merged.DemotedDue, merged.DemotedDueOldestSeconds(now),
			whole.DemotedDue, whole.DemotedDueOldestSeconds)
	}
	if merged.Impact.Impact() != ImpactOf(whole, now) {
		t.Errorf("impact %+v, whole view %+v", merged.Impact.Impact(), ImpactOf(whole, now))
	}
	columns := viewColumns(&whole)
	sameJSON(t, "cohorts", merged.Cohorts(whole.Schedule), Cohorts(&whole, columns))
	sameJSON(t, "cooling", merged.CoolingAt(whole.Schedule, now), Cooling(&whole, columns, now))
	sameJSON(t, "load", merged.Load(&whole), LoadOf(&whole, now))
	screen := Report(&whole, now)
	sameJSON(t, "check lines", merged.Checks(&whole, now), screen.Checks)
	sameJSON(t, "check lines read again", merged.Checks(&whole, now), screen.Checks)
	sameJSON(t, "to-do", merged.Todo(merged.Checks(&whole, now), &whole), screen.Todo)
	if screen.Todo.Objects == 0 || screen.Todo.UndeterminedObjects == 0 || screen.Todo.Ongoing+screen.Todo.AfterRestart == 0 {
		t.Fatalf("fixture to-do %+v, want objects on both sides and a loss in progress", screen.Todo)
	}
	if len(screen.Checks) < 4 {
		t.Fatalf("fixture check lines %d, want several", len(screen.Checks))
	}
	pruned, retained, readEarly := prunedSkipList(whole.PrunedSkips), retainedShareList(whole.RetainedShare), readEarlyList(whole.ReadEarly)
	sameJSON(t, "pruned skips", merged.PrunedSkips, firstScreenList(pruned))
	sameJSON(t, "retained share", merged.RetainedShare, firstScreenList(retained))
	sameJSON(t, "read early", merged.ReadEarly, firstScreenList(readEarly))
	if merged.PrunedSkipsTotal != len(pruned) || merged.RetainedShareTotal != len(retained) || merged.ReadEarlyTotal != len(readEarly) {
		t.Errorf("totals %d/%d/%d, whole view %d/%d/%d", merged.PrunedSkipsTotal, merged.RetainedShareTotal, merged.ReadEarlyTotal,
			len(pruned), len(retained), len(readEarly))
	}
	if len(pruned) != 21 || len(merged.PrunedSkips) != FirstScreenListBound {
		t.Fatalf("fixture lists: %d pruned, %d carried", len(pruned), len(merged.PrunedSkips))
	}
	if loss := LoadOf(&whole, now).Loss; loss.Ongoing+loss.AfterRestart == 0 {
		t.Fatalf("fixture loss %+v, want the skip counted as in progress", loss)
	}
	// The pooled rows carry no period and make a cohort of their own at 0.
	if cohorts := Cohorts(&whole, columns); len(cohorts) != 3 || cohorts[1].Listed == 0 || cohorts[2].Listed == 0 ||
		cohorts[1].Objects != 153 {
		t.Fatalf("fixture cohorts %+v, want two periods with rows and their census, and the pooled rows at 0", cohorts)
	}
	if cooling := Cooling(&whole, columns, now); cooling.Listed < 6 || cooling.OldestSinceSeconds == 0 {
		t.Fatalf("fixture cooling %+v, want every pooled row cooling and an oldest reason", cooling)
	}
	// Numbers the fixture must reach, or the equalities above are between
	// zeros: several due with different waits, several empty rows, and rows
	// on each side of the verdict.
	if whole.DemotedDue != 3 || whole.DemotedDueOldestSeconds != 21*60 || whole.EmptyEveryRoundTotal != 6 ||
		OursCount(whole.Anomalies) == 0 || OursCount(whole.Anomalies) == len(whole.Anomalies) {
		t.Fatalf("fixture: due %d oldest %ds, empty %d, ours %d of %d", whole.DemotedDue, whole.DemotedDueOldestSeconds,
			whole.EmptyEveryRoundTotal, OursCount(whole.Anomalies), len(whole.Anomalies))
	}
}

// sameJSON compares two readings as the route serves them, so an empty map
// and a missing one are the same answer, as they are on the wire.
func sameJSON(t *testing.T, what string, got, want any) {
	t.Helper()
	encodedGot, _ := json.Marshal(got)
	encodedWant, _ := json.Marshal(want)
	if string(encodedGot) != string(encodedWant) {
		t.Errorf("%s from parts:\n%s\nfrom the whole view:\n%s", what, encodedGot, encodedWant)
	}
}

// Each health list is in one order across replicas, ties broken by object,
// so the first few are the same objects whichever replica's rows came first.
func TestTheHealthListsBreakTiesByObject(t *testing.T) {
	rows := func(facts func(*Anomaly)) []Anomaly {
		list := []Anomaly{}
		for _, queryGroup := range []string{"qg-c", "qg-a", "qg-b"} {
			row := Anomaly{QueryGroup: queryGroup}
			facts(&row)
			list = append(list, row)
		}
		return list
	}
	order := func(groups ...string) string { return fmt.Sprint(groups) }
	early := readEarlyList(rows(func(row *Anomaly) { row.ReadEarly = &ReadEarlyFacts{SuggestedDelaySeconds: 60} }))
	retained := retainedShareList(rows(func(row *Anomaly) { row.RetainedShare = &RetainedShareFacts{PercentOfShare: 90} }))
	pruned := prunedSkipList(map[string]PrunedSkip{"qg-c": {To: 60}, "qg-a": {To: 60}, "qg-b": {To: 60}})
	got := []string{}
	for _, list := range [][]string{{early[0].QueryGroup, early[1].QueryGroup, early[2].QueryGroup},
		{retained[0].QueryGroup, retained[1].QueryGroup, retained[2].QueryGroup},
		{pruned[0].QueryGroup, pruned[1].QueryGroup, pruned[2].QueryGroup}} {
		got = append(got, order(list...))
	}
	want := order("qg-a", "qg-b", "qg-c")
	for index, name := range []string{"read early", "retained share", "pruned skips"} {
		if got[index] != want {
			t.Errorf("%s ties in order %s, want %s", name, got[index], want)
		}
	}
}

// Each attribution is counted in its own place, the field left unset
// included -- which the per-replica split counts as unattributed and the
// verdict does not -- and parts add place by place.
func TestAPartCountsEachAttributionAndPartsAddThem(t *testing.T) {
	view := View{Anomalies: []Anomaly{{Attribution: AttributionOurs}, {Attribution: AttributionExternal},
		{Attribution: AttributionUnknown}, {}}}
	part := ReplicaPartOf(view, now)
	one := AttributionTally{Ours: 1, External: 1, Unknown: 1, Other: 1}
	if part.Attribution != one {
		t.Fatalf("attribution %+v, want %+v", part.Attribution, one)
	}
	if merged := MergeReplicaParts(part, part).Attribution; merged != (AttributionTally{Ours: 2, External: 2, Unknown: 2, Other: 2}) {
		t.Fatalf("merged attribution %+v, want every place doubled", merged)
	}
}
