package fleet

import (
	"encoding/json"
	"testing"
	"time"
)

func TestReadHoldStandingRequiresOneCurrentOwner(t *testing.T) {
	now := time.Unix(1800, 0)
	facts := StrategyLookupFacts{Found: true, Plans: []StrategyPlanRef{{Tenant: "tenant", Business: "2", QueryGroup: "qg"}}}
	first := Snapshot{Replica: "alice", TakenAt: now, Owned: 1, OwnedObjects: []string{"qg"}, ReadHolds: map[string]ReadHoldFacts{"qg": {Millis: 60000, Annotation: "alarmd 当前自动推后 60 秒"}}}
	view := Aggregate(Expectation{Known: true, IDs: []string{"qg"}, QueryGroups: 1}, []Snapshot{first}, []string{"alice"}, now, time.Minute)
	standing := StrategyStandingOf("101", "", "", "alice", facts, &view, now)
	if standing.Plans[0].ReadHold == nil || standing.Plans[0].ReadHold.Millis != 60000 {
		t.Fatal("single owner's hold was lost")
	}
	if SummaryOf(first, first.OwnedObjects, time.Minute).Head.ReadHolds != nil {
		t.Fatal("per-QG holds inflated summary head")
	}
	second := Snapshot{Replica: "bob", TakenAt: now, Owned: 1, OwnedObjects: []string{"qg"}, ReadHolds: map[string]ReadHoldFacts{"qg": {Millis: 120000}}}
	for _, replicas := range [][]string{{"alice", "bob"}, {"bob", "alice"}} {
		view = Aggregate(Expectation{Known: true, IDs: []string{"qg"}, QueryGroups: 1}, []Snapshot{first, second}, replicas, now, time.Minute)
		standing = StrategyStandingOf("101", "", "", "alice", facts, &view, now)
		if standing.Plans[0].ReadHold != nil {
			t.Fatal("ambiguous owner was presented as a current hold")
		}
	}
}
func TestReadHoldSnapshotRespectsItsExistingByteBudget(t *testing.T) {
	facts := map[string]ReadHoldFacts{"a": {Millis: 1}, "b": {Millis: 2}}
	one, _ := json.Marshal(map[string]ReadHoldFacts{"a": facts["a"]})
	kept := withinReadHoldBudget(facts, len(one))
	if len(kept) != 1 || kept["a"].Millis != 1 {
		t.Fatalf("unstable or oversized prefix: %+v", kept)
	}
}
