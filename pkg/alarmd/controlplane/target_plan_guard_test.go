package controlplane_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// A target_plan this build cannot read must not be ignored, decoded as the
// old target, or served by the last good Plan. A sibling using the old
// protocol still runs. The refusal names the field that decided it, down to
// the leaf; the scenarios here check the item it sits on.
func TestTargetPlanIsRefusedWithoutFallingBackToLegacyTargets(t *testing.T) {
	payload, err := os.ReadFile("testdata/two_threshold_strategies.json")
	if err != nil {
		t.Fatal(err)
	}
	var documents []json.RawMessage
	if err := json.Unmarshal(payload, &documents); err != nil {
		t.Fatal(err)
	}
	identity := controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}
	originals := []controlplane.SourceStrategy{
		{SourceID: "1001", Document: documents[0], Identity: identity},
		{SourceID: "1002", Document: documents[1], Identity: identity},
	}
	previous, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)},
	})
	if err != nil || !reflect.DeepEqual(catalogStrategyIDs(previous), []string{"1001", "1002"}) {
		t.Fatalf("legacy baseline: catalog=%+v err=%v", previous, err)
	}
	lastGood := &controlplane.PublishedSnapshot{
		Publication: controlplane.SnapshotPublicationRef{SnapshotRevision: previous.SnapshotRevision, PublicationEpoch: 1},
		QueryGroups: previous.QueryGroups,
	}
	for _, test := range []struct {
		name       string
		plan       string
		target     string
		secondItem bool
		incomplete bool
		cached     bool
		field      string
	}{
		{name: "null is present", plan: `null`, field: "items[0].target_plan"},
		{name: "empty object is present", plan: `{}`, field: "items[0].target_plan"},
		{name: "unknown version", plan: `{"schema_version":99}`, field: "items[0].target_plan"},
		{name: "wrong type", plan: `[]`, field: "items[0].target_plan"},
		{name: "new target object cannot trigger stale fallback", plan: `{"schema_version":1}`, target: `{"schema_version":1,"model_id":"host","selectors":[]}`, field: "items[0].target_plan"},
		{name: "new field on later item", plan: `{}`, secondItem: true, field: "items[1].target_plan"},
		{name: "incomplete source cannot retain old target", plan: `{}`, incomplete: true, field: "items[0].target_plan"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cache *controlplane.CandidateCache
			wantQueryCompilations := 1
			if test.cached {
				cache = controlplane.NewCandidateCache()
				warm, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
					Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)}, Cache: cache,
				})
				if err != nil || !reflect.DeepEqual(warm, previous) {
					t.Fatalf("warm cache changed the legacy catalog: err=%v", err)
				}
				wantQueryCompilations = 0 // The healthy sibling is already cached.
			}
			var document map[string]json.RawMessage
			if err := json.Unmarshal(documents[1], &document); err != nil {
				t.Fatal(err)
			}
			var items []map[string]json.RawMessage
			if err := json.Unmarshal(document["items"], &items); err != nil {
				t.Fatal(err)
			}
			index := 0
			if test.secondItem {
				items = append(items, map[string]json.RawMessage{"id": json.RawMessage(`2`)})
				index = 1
			}
			items[index]["target_plan"] = json.RawMessage(test.plan)
			if test.target != "" {
				items[index]["target"] = json.RawMessage(test.target)
			}
			document["items"], err = json.Marshal(items)
			if err != nil {
				t.Fatal(err)
			}
			changed := originals[1]
			changed.Document, err = json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if test.incomplete {
				changed.SourceDisposition = &controlplane.ObjectDisposition{SourceID: "1002", Scope: "STRATEGY", Disposition: controlplane.DispositionSourceIncomplete, Reason: "SOURCE_READ_INCOMPLETE"}
			}
			planner := &recordingPlanner{facts: queryFacts(t)}
			request := controlplane.BuildRequest{
				Strategies: []controlplane.SourceStrategy{originals[0], changed}, Planner: planner, LastGood: lastGood, Cache: cache,
			}
			catalog, err := controlplane.BuildCatalog(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if got := catalogStrategyIDs(catalog); !reflect.DeepEqual(got, []string{"1001"}) || planner.calls != wantQueryCompilations {
				t.Fatalf("new target was executed or retained: plans=%v query compilations=%d dispositions=%+v", got, planner.calls, catalog.Dispositions)
			}
			found := false
			for _, disposition := range catalog.Dispositions {
				if disposition.SourceID == "1002" && disposition.Disposition == controlplane.DispositionUnsupported && disposition.Reason == "UNSUPPORTED_TARGET_PLAN" && strings.HasPrefix(disposition.FieldPath, test.field) {
					found = true
				}
			}
			if !found {
				t.Fatalf("new target was not refused by name and field: %+v", catalog.Dispositions)
			}
			if test.cached {
				repeated, err := controlplane.BuildCatalog(context.Background(), request)
				compiled, reused := cache.Stats()
				if err != nil || !reflect.DeepEqual(repeated, catalog) || compiled != 0 || reused != 2 || planner.calls != 0 {
					t.Fatalf("cached refusal changed: compiled=%d reused=%d query compilations=%d err=%v", compiled, reused, planner.calls, err)
				}
			}
			// Removing the new field returns to the exact original catalog,
			// including its frozen Plan bytes and revision.
			restored, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
				Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)}, Cache: cache,
				LastGood: &controlplane.PublishedSnapshot{QueryGroups: catalog.QueryGroups},
			})
			if err != nil || !reflect.DeepEqual(restored, previous) {
				t.Fatalf("legacy catalog changed after removing the new field: err=%v", err)
			}
		})
	}
}

// A target_plan that reads is the whole target: the Plan freezes it, the
// item's old target is not read at all - here it is a shape the old
// compiler refuses, and the strategy compiles anyway - and the healthy
// sibling's frozen bytes and the shared revision derivation are untouched.
// Removing the field returns to the exact original catalog.
func TestAValidTargetPlanIsFrozenAndTheLegacyTargetIsNotRead(t *testing.T) {
	payload, err := os.ReadFile("testdata/two_threshold_strategies.json")
	if err != nil {
		t.Fatal(err)
	}
	var documents []json.RawMessage
	if err := json.Unmarshal(payload, &documents); err != nil {
		t.Fatal(err)
	}
	identity := controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}
	originals := []controlplane.SourceStrategy{
		{SourceID: "1001", Document: documents[0], Identity: identity},
		{SourceID: "1002", Document: documents[1], Identity: identity},
	}
	previous, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)},
	})
	if err != nil || !reflect.DeepEqual(catalogStrategyIDs(previous), []string{"1001", "1002"}) {
		t.Fatalf("legacy baseline: catalog=%+v err=%v", previous, err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(documents[1], &document); err != nil {
		t.Fatal(err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(document["items"], &items); err != nil {
		t.Fatal(err)
	}
	items[0]["target_plan"] = json.RawMessage(`{"schema_version":1,"model_id":"cw-Host","target_rule":"host_id","failure_policy":"no_match",
		"static_targets":[{"bk_host_id":42},{"bk_host_id":7}],"dynamic_groups":[{"dynamic_group_id":"1001"}],
		"dynamic_topologies":[{"bk_biz_id":2,"bk_obj_id":"set","bk_inst_id":12}]}`)
	// The old target is not consulted in any of its shapes: a perfectly
	// valid list the old compiler would happily turn into a scope (the shape
	// the writer emits today beside the plan -- the one a fallback would
	// silently prefer), a field the old compiler refuses, a selection object,
	// and a list the old decoder cannot read at all - which, read, would have
	// retained the last good Plan.
	want := &contract.TargetPlanV1{SchemaVersion: 1, ModelID: "cw-Host", Rule: contract.TargetPlanRuleHostID,
		Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}}, StaticKeys: []string{"42", "7"},
		DynamicGroups:     []string{"1001"},
		DynamicTopologies: []contract.TargetPlanTopologyV1{{BusinessID: "2", ObjectID: "set", InstanceID: "12"}}}
	frozenPlanOf := func(catalog controlplane.Catalog, strategyID string) *controlplane.FrozenPlan {
		for index := range catalog.QueryGroups {
			for planIndex := range catalog.QueryGroups[index].Plans {
				if plan := &catalog.QueryGroups[index].Plans[planIndex]; plan.Identity.StrategyID == strategyID {
					return plan
				}
			}
		}
		return nil
	}
	var catalog controlplane.Catalog
	for _, legacy := range []string{
		`[[{"field":"bk_target_ip","method":"eq","value":[{"bk_target_ip":"192.0.2.10","bk_target_cloud_id":0}]}]]`,
		`[[{"field":"host_set_template","method":"eq","value":[{"bk_obj_id":"set","bk_inst_id":1}]}]]`,
		`{"schema_version":1,"model_id":"cw-Host","selectors":[]}`,
		`[[{"field":5}]]`,
	} {
		items[0]["target"] = json.RawMessage(legacy)
		document["items"], err = json.Marshal(items)
		if err != nil {
			t.Fatal(err)
		}
		changed := originals[1]
		changed.Document, err = json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		planner := &recordingPlanner{facts: queryFacts(t)}
		catalog, err = controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
			Strategies: []controlplane.SourceStrategy{originals[0], changed}, Planner: planner,
			LastGood: &controlplane.PublishedSnapshot{Publication: controlplane.SnapshotPublicationRef{SnapshotRevision: previous.SnapshotRevision, PublicationEpoch: 1}, QueryGroups: previous.QueryGroups},
		})
		if err != nil {
			t.Fatal(err)
		}
		if catalog.RetainedStaleRevisions != 0 {
			t.Fatalf("legacy target %s: the last good Plan was retained beside a target plan", legacy)
		}
		for _, disposition := range catalog.Dispositions {
			if disposition.Disposition != controlplane.DispositionAccepted {
				t.Fatalf("legacy target %s: disposition %+v, want every strategy accepted", legacy, disposition)
			}
		}
		// Asserted per shape, not once at the end: a fallback that prefers a
		// compilable old target would pass every other shape and fail only
		// the valid list, and the end-of-loop read would never see it.
		if compiled := frozenPlanOf(catalog, "1002"); compiled == nil || !reflect.DeepEqual(compiled.Plan.TargetPlan, want) || compiled.Plan.TargetScope != nil {
			t.Fatalf("legacy target %s: frozen plan = %+v, want the target plan %+v and no scope", legacy, compiled, want)
		}
	}
	if got := catalogStrategyIDs(catalog); !reflect.DeepEqual(got, []string{"1001", "1002"}) {
		t.Fatalf("catalog = %v, dispositions %+v; want both strategies", got, catalog.Dispositions)
	}
	var frozen *controlplane.FrozenPlan
	var sibling *controlplane.FrozenPlan
	for index := range catalog.QueryGroups {
		for planIndex := range catalog.QueryGroups[index].Plans {
			plan := &catalog.QueryGroups[index].Plans[planIndex]
			switch plan.Identity.StrategyID {
			case "1002":
				frozen = plan
			case "1001":
				sibling = plan
			}
		}
	}
	if frozen == nil || sibling == nil {
		t.Fatalf("plans not found in %+v", catalog.QueryGroups)
	}
	if !reflect.DeepEqual(frozen.Plan.TargetPlan, want) || frozen.Plan.TargetScope != nil {
		t.Fatalf("frozen target plan = %+v scope = %+v, want %+v and no scope", frozen.Plan.TargetPlan, frozen.Plan.TargetScope, want)
	}
	var previousSibling controlplane.FrozenPlan
	for _, group := range previous.QueryGroups {
		for _, plan := range group.Plans {
			if plan.Identity.StrategyID == "1001" {
				previousSibling = plan
			}
		}
	}
	if !reflect.DeepEqual(*sibling, previousSibling) {
		t.Fatalf("the sibling's frozen Plan changed: %+v vs %+v", *sibling, previousSibling)
	}
	// The object contract: the Query Group carrying the target plan is a v2
	// object with a digest of its own; the sibling's object is the v1 it was.
	for _, group := range catalog.QueryGroups {
		object := controlplane.BuildQueryGroupObject(group)
		carries := false
		for _, plan := range object.Plans {
			carries = carries || plan.TargetPlan != nil
		}
		if carries && object.ContractVersion != "alarmd-query-group-object-v2" {
			t.Fatalf("object carrying a target plan is %s, want v2 so a reader that predates the field refuses it", object.ContractVersion)
		}
		if !carries && object.ContractVersion != "alarmd-query-group-object-v1" {
			t.Fatalf("object without a target plan is %s, want v1 unchanged", object.ContractVersion)
		}
	}
	restored, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)},
	})
	if err != nil || !reflect.DeepEqual(restored, previous) {
		t.Fatalf("legacy catalog changed after removing the new field: err=%v", err)
	}
}

// A target that has moved to the selection protocol without a target_plan
// beside it is the writer switching protocols out of order, and a target
// list the old decoder cannot read is a document nobody can show the
// target of. Both are refused by name - not compiled as a strategy with no
// target, not kept on the last good Plan of the old target, which is what a
// whole-document decode failure used to do.
func TestASelectionOrUnreadableTargetWithoutATargetPlanIsRefusedByName(t *testing.T) {
	payload, err := os.ReadFile("testdata/two_threshold_strategies.json")
	if err != nil {
		t.Fatal(err)
	}
	var documents []json.RawMessage
	if err := json.Unmarshal(payload, &documents); err != nil {
		t.Fatal(err)
	}
	identity := controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}
	originals := []controlplane.SourceStrategy{
		{SourceID: "1001", Document: documents[0], Identity: identity},
		{SourceID: "1002", Document: documents[1], Identity: identity},
	}
	previous, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
		Strategies: originals, Planner: &recordingPlanner{facts: queryFacts(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(documents[1], &document); err != nil {
		t.Fatal(err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(document["items"], &items); err != nil {
		t.Fatal(err)
	}
	for legacy, reason := range map[string]string{
		`{"schema_version":1,"model_id":"cw-Host","selectors":[{"type":"instances","instances":[{"model_id":"cw-Host","model_inst_id":"101","entity_uid":"cw-Host|101"}]}]}`: "TARGET_PLAN_MISSING",
		`[[{"field":5}]]`: "UNSUPPORTED_TARGET_SCOPE",
	} {
		items[0]["target"] = json.RawMessage(legacy)
		document["items"], _ = json.Marshal(items)
		changed := originals[1]
		changed.Document, _ = json.Marshal(document)
		planner := &recordingPlanner{facts: queryFacts(t)}
		catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{
			Strategies: []controlplane.SourceStrategy{originals[0], changed}, Planner: planner,
			LastGood: &controlplane.PublishedSnapshot{Publication: controlplane.SnapshotPublicationRef{SnapshotRevision: previous.SnapshotRevision, PublicationEpoch: 1}, QueryGroups: previous.QueryGroups},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := catalogStrategyIDs(catalog); !reflect.DeepEqual(got, []string{"1001"}) || planner.calls != 1 || catalog.RetainedStaleRevisions != 0 {
			t.Fatalf("target %s was executed or retained: plans=%v compilations=%d retained=%d dispositions=%+v", legacy, got, planner.calls, catalog.RetainedStaleRevisions, catalog.Dispositions)
		}
		found := false
		for _, disposition := range catalog.Dispositions {
			if disposition.SourceID == "1002" && disposition.Disposition == controlplane.DispositionUnsupported &&
				disposition.Reason == reason && disposition.FieldPath == "items[0].target" {
				found = true
			}
		}
		if !found {
			t.Fatalf("target %s was not refused as %s: %+v", legacy, reason, catalog.Dispositions)
		}
	}
}
