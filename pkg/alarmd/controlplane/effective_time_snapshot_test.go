package controlplane_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

func TestEffectiveTimeSourceProjectionRoundTripAndStateIdentity(t *testing.T) {
	build := func(zone string) controlplane.QueryGroup {
		document := map[string]any{}
		if err := json.Unmarshal(realThresholdDocuments(t)[0], &document); err != nil {
			t.Fatal(err)
		}
		for _, raw := range document["detects"].([]any) {
			raw.(map[string]any)["trigger_config"].(map[string]any)["uptime"] = map[string]any{"time_ranges": []any{map[string]any{"start": "09:00:59", "end": "10:00:59"}}, "calendars": []int{7}}
		}
		document["effective_time_snapshot"] = map[string]any{"schema_version": 1, "status": "READY", "business_timezone": zone, "calendars": []any{map[string]any{"id": 7, "bk_tenant_id": "tenant-a", "status": "PRESENT", "items": []any{map[string]any{"id": 8, "time_kind": "UNIX_SECONDS", "start_time": 1700000000, "end_time": 1700000300, "time_zone": "Asia/Kolkata", "parent_id": 3, "repeat": map[string]any{"freq": "week", "interval": 2, "every": []int{1, 3}, "until": nil, "exclude_date": []int64{1790006400, 1790010000}, "exclude_date_encoding_timezone": "Asia/Shanghai"}}}}}}
		encoded, _ := json.Marshal(document)
		catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{Strategies: []controlplane.SourceStrategy{{SourceID: "1001", Document: encoded, Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}}}, Planner: &recordingPlanner{facts: queryFacts(t)}})
		if err != nil {
			t.Fatal(err)
		}
		if len(catalog.QueryGroups) != 1 {
			t.Fatalf("catalog=%+v", catalog)
		}
		return catalog.QueryGroups[0]
	}
	before, after := build("UTC"), build("Asia/Shanghai")
	if before.Identity != after.Identity {
		t.Fatal("timezone changed query identity")
	}
	old := compileWithEvaluationCore(t, before.Plans[0].Plan, before.QueryPlan.Normalization.DatasetContract)
	updated := compileWithEvaluationCore(t, after.Plans[0].Plan, after.QueryPlan.Normalization.DatasetContract)
	if old.StateCompatibilityHash() != updated.StateCompatibilityHash() {
		t.Fatal("timezone reset state")
	}
	if !old.HasEffectiveTimeSnapshot() {
		t.Fatal("snapshot lost")
	}
	objectBytes, _ := json.Marshal(controlplane.BuildQueryGroupObject(before))
	var readObject controlplane.QueryGroupObject
	if err := json.Unmarshal(objectBytes, &readObject); err != nil {
		t.Fatal(err)
	}
	readGroup, err := controlplane.AssembleQueryGroup(readObject, map[execution.PlanIdentity]controlplane.OutputContextObject{before.Plans[0].Identity: controlplane.BuildOutputContext(before.Plans[0])})
	if err != nil {
		t.Fatal(err)
	}
	read := readGroup.Plans[0]
	if string(read.Plan.EffectiveTimeSnapshot) != string(before.Plans[0].Plan.EffectiveTimeSnapshot) {
		t.Fatal("snapshot changed through publication")
	}
	compiled := compileWithEvaluationCore(t, read.Plan, before.QueryPlan.Normalization.DatasetContract)
	fact, err := compiled.ResolveEffectiveTime(context.Background(), 9*3600+30*60)
	if err != nil {
		t.Fatal(err)
	}
	if fact.Status() != strategy.EffectiveTimeActive {
		t.Fatal(fact.Status())
	}
	fact, err = updated.ResolveEffectiveTime(context.Background(), 9*3600+30*60)
	if err != nil {
		t.Fatal(err)
	}
	if fact.Status() != strategy.EffectiveTimeInactive {
		t.Fatal(fact.Status())
	}
	object := controlplane.BuildQueryGroupObject(before)
	encoded, _ := json.Marshal(object)
	if !strings.Contains(string(encoded), "alarmd-query-group-object-v3") {
		t.Fatal("old reader could silently drop calendar snapshot")
	}
}

func TestEffectiveTimeMultipleLevelsKeepTheirOwnUptime(t *testing.T) {
	document := map[string]any{}
	if err := json.Unmarshal(realThresholdDocuments(t)[0], &document); err != nil {
		t.Fatal(err)
	}
	detects := document["detects"].([]any)
	document["detects"] = append(detects, map[string]any{"level": 2, "trigger_config": map[string]any{"count": 1, "check_window": 1, "uptime": map[string]any{"time_ranges": []any{map[string]any{"start": "09:00", "end": "10:00"}}}}})
	item := document["items"].([]any)[0].(map[string]any)
	algorithms := item["algorithms"].([]any)
	copyAlgorithm := map[string]any{}
	for key, value := range algorithms[0].(map[string]any) {
		copyAlgorithm[key] = value
	}
	copyAlgorithm["level"] = 2
	item["algorithms"] = append(algorithms, copyAlgorithm)
	encoded, _ := json.Marshal(document)
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{Strategies: []controlplane.SourceStrategy{{SourceID: "1001", Document: encoded, Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}}}, Planner: &recordingPlanner{facts: queryFacts(t)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.QueryGroups) != 1 {
		t.Fatalf("different per-level uptime refused: %+v", catalog.Dispositions)
	}
	group := catalog.QueryGroups[0]
	compiled := compileWithEvaluationCore(t, group.Plans[0].Plan, group.QueryPlan.Normalization.DatasetContract)
	levels := compiled.Levels()
	if len(levels) != 2 || levels[0].EffectiveTimeRequirement().Kind() != strategy.EffectiveTimeAlways || levels[1].EffectiveTimeRequirement().Kind() != strategy.EffectiveTimeStaticSchedule {
		t.Fatalf("lost level requirements: %+v", levels)
	}
}
