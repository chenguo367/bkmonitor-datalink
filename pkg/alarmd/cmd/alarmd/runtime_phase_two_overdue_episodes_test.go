package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

// An object overdue at one publish and not at the last begins an episode
// with its read hold then; still overdue, nothing more; overdue no more, the
// episode ends and is kept, the latest fleet.MaxOverdueEpisodes of them, and
// the next snapshot carries them.
func TestTheFleetPublisherKeepsOverdueEpisodesWithTheirHold(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(1_791_400_000, 0)}
	type heard struct {
		episode fleet.OverdueEpisode
		began   bool
	}
	var events []heard
	publisher := fleetPublisher{
		tracker: fleet.NewTracker(nil, "replica-1", clock.now), replica: "replica-1", now: clock.now,
		owned: func() []execution.QueryGroupIdentity { return nil },
		holdOf: func(queryGroup string) (int64, bool) {
			if queryGroup == "qg-held" {
				return 308_000, true
			}
			return 0, queryGroup != "qg-unread"
		},
		onOverdue: func(episode fleet.OverdueEpisode, began bool) { events = append(events, heard{episode, began}) },
	}
	at := clock.now()
	overdue := []fleet.OverdueObject{{QueryGroup: "qg-held", IntervalSeconds: 60}, {QueryGroup: "qg-unread"}}
	publisher.noteOverdue(overdue, at)
	publisher.noteOverdue(overdue, at.Add(time.Minute))
	if len(events) != 2 || !events[0].began || !events[1].began {
		t.Fatalf("events %+v, want two beginnings and nothing for still overdue", events)
	}
	began := map[string]fleet.OverdueEpisode{}
	for _, event := range events {
		began[event.episode.QueryGroup] = event.episode
	}
	if held := began["qg-held"]; held.HoldClass() != "positive" || held.ReadHoldMillis != 308_000 || !held.Onset.Equal(at) || held.Replica != "replica-1" {
		t.Fatalf("held episode %+v", held)
	}
	if unread := began["qg-unread"]; unread.HoldClass() != "unknown" {
		t.Fatalf("unread episode %+v, want its hold unknown", unread)
	}
	publisher.noteOverdue(overdue[:1], at.Add(2*time.Minute))
	if len(events) != 3 || events[2].began || events[2].episode.QueryGroup != "qg-unread" || !events[2].episode.Clear.Equal(at.Add(2*time.Minute)) {
		t.Fatalf("events %+v, want qg-unread ended at its clear", events)
	}
	if snapshot := publisher.snapshot(context.Background()); len(snapshot.OverdueEpisodes) != 1 || snapshot.OverdueEpisodes[0].QueryGroup != "qg-unread" {
		t.Fatalf("snapshot episodes %+v, want the ended one", snapshot.OverdueEpisodes)
	}
	for i := 0; i < fleet.MaxOverdueEpisodes+5; i++ {
		object := []fleet.OverdueObject{{QueryGroup: fmt.Sprintf("qg-%d", i)}}
		publisher.noteOverdue(append(object, overdue[0]), at.Add(time.Duration(3+2*i)*time.Minute))
		publisher.noteOverdue(overdue[:1], at.Add(time.Duration(4+2*i)*time.Minute))
	}
	kept := publisher.overdueEpisodes
	if len(kept) != fleet.MaxOverdueEpisodes || kept[len(kept)-1].QueryGroup != fmt.Sprintf("qg-%d", fleet.MaxOverdueEpisodes+4) {
		t.Fatalf("kept %d, last %+v, want the latest %d", len(kept), kept[len(kept)-1], fleet.MaxOverdueEpisodes)
	}
	if _, open := publisher.overdueOpen["qg-held"]; !open || len(publisher.overdueOpen) != 1 {
		t.Fatalf("open %+v, want only the object still overdue", publisher.overdueOpen)
	}
}

// The scrape exports the running strategies only from a view read whole:
// snapshots deferred under the memory line or unread leave no series, and so
// does a part from a build without the counts.
func TestTheVerdictExportsRunningStrategiesOnlyFromAViewReadWhole(t *testing.T) {
	at := time.Unix(1_791_400_000, 0)
	part := fleet.ReplicaPart{RunningStrategies: map[fleet.StateWord]int{fleet.StateDetecting: 120, fleet.StateDataAbsent: 30}}
	whole := fleet.View{Health: fleet.HealthHealthy, Replicas: []string{"pod-a", "pod-b"}}
	verdict := fleetVerdictOf(whole, part, at)
	if len(verdict.RunningStrategies) != len(fleet.StateWords) {
		t.Fatalf("running %+v, want every state", verdict.RunningStrategies)
	}
	for _, count := range verdict.RunningStrategies {
		if count.Value == string(fleet.StateDetecting) && count.Count != 120 {
			t.Fatalf("DETECTING %d, want 120", count.Count)
		}
	}
	for _, kind := range []fleet.GapKind{fleet.GapSnapshotsDeferred, fleet.GapSnapshotsUnreadable, fleet.GapReplicaMissing} {
		view := whole
		view.Gaps = []fleet.Gap{{Kind: kind}}
		if verdict := fleetVerdictOf(view, part, at); verdict.RunningStrategies != nil {
			t.Fatalf("%s: running %+v exported from a view not read whole", kind, verdict.RunningStrategies)
		}
	}
	if verdict := fleetVerdictOf(whole, fleet.ReplicaPart{}, at); verdict.RunningStrategies != nil {
		t.Fatalf("running %+v exported from a part without the counts", verdict.RunningStrategies)
	}
}

// The strategies evaluating on the objects a replica holds, each once.
func TestEvaluatingStrategiesAreEachStrategyOnce(t *testing.T) {
	refs := map[string][]fleet.StrategyRef{
		"qg-a": {{StrategyID: "9", BusinessID: "2"}, {StrategyID: "10", BusinessID: "2"}},
		"qg-b": {{StrategyID: "9", BusinessID: "2"}},
	}
	list := evaluatingStrategies([]execution.QueryGroupIdentity{"qg-a", "qg-b", "qg-none"}, func(qg string) []fleet.StrategyRef { return refs[qg] })
	if len(list) != 2 || list[0].StrategyID != "10" || list[1].StrategyID != "9" {
		t.Fatalf("list %+v, want 10 and 9 once each", list)
	}
}

// The snapshot says which strategies evaluate on the objects the replica
// holds, and says nothing of them without the tracker's names.
func TestTheFleetPublisherSaysWhichStrategiesEvaluate(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(1_791_400_000, 0)}
	publisher := fleetPublisher{
		tracker: fleet.NewTracker(nil, "replica-1", clock.now), replica: "replica-1", now: clock.now,
		owned: func() []execution.QueryGroupIdentity { return []execution.QueryGroupIdentity{"qg-a"} },
	}
	if snapshot := publisher.snapshot(context.Background()); snapshot.EvaluatingStrategiesKnown || len(snapshot.EvaluatingStrategies) != 0 {
		t.Fatalf("a publisher without strategy names said %+v (known %t)", snapshot.EvaluatingStrategies, snapshot.EvaluatingStrategiesKnown)
	}
	publisher.strategies = func(string) []fleet.StrategyRef { return []fleet.StrategyRef{{StrategyID: "4101", BusinessID: "2"}} }
	snapshot := publisher.snapshot(context.Background())
	if !snapshot.EvaluatingStrategiesKnown || len(snapshot.EvaluatingStrategies) != 1 || snapshot.EvaluatingStrategies[0].StrategyID != "4101" {
		t.Fatalf("snapshot strategies %+v (known %t)", snapshot.EvaluatingStrategies, snapshot.EvaluatingStrategiesKnown)
	}
}
