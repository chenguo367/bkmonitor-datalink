package controlplane_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// A writer may carry keys of its own next to the ones the platform reads, such
// as a calendar list of its own in every detect's uptime. Python reads only
// time_ranges, calendars and active_calendars there
// (alarm_backends/core/control/strategy.py in_alarm_time) and runs the
// strategy. Refusing the key took every Level of such a strategy out as
// LEVEL_INVALID at compile time, after the Catalog had accepted it.
func TestAnUptimeKeyThePlatformDoesNotReadLeavesTheLevelAsItWas(t *testing.T) {
	build := func(uptime map[string]any) controlplane.QueryGroup {
		t.Helper()
		document := map[string]any{}
		if err := json.Unmarshal(realThresholdDocuments(t)[0], &document); err != nil {
			t.Fatal(err)
		}
		for _, raw := range document["detects"].([]any) {
			raw.(map[string]any)["trigger_config"].(map[string]any)["uptime"] = uptime
		}
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
	ranges := []any{map[string]any{"start": "09:00", "end": "18:00"}}
	plain := build(map[string]any{"time_ranges": ranges, "calendars": []any{}, "active_calendars": []any{}})
	for name, extra := range map[string]any{"empty": []any{}, "calendar ids": []any{7}} {
		t.Run(name, func(t *testing.T) {
			carried := build(map[string]any{"time_ranges": ranges, "calendars": []any{}, "active_calendars": []any{}, "own_calendars": extra})
			want := compileEveryLevel(t, plain)
			got := compileEveryLevel(t, carried)
			if got.StateCompatibilityHash() != want.StateCompatibilityHash() {
				t.Fatal("a key the platform does not read changed how the strategy runs")
			}
			if compiledFrom(t, carried) != compiledFrom(t, plain) {
				t.Fatal("a key the platform does not read reached the published plan")
			}
		})
	}
	// Python fails on a non-empty uptime without time_ranges (KeyError), so
	// one holding nothing but a key it passes over still does not run.
	t.Run("nothing the platform reads", func(t *testing.T) {
		carried := build(map[string]any{"own_calendars": []any{7}})
		result := evaluationCoreResult(t, carried.Plans[0].Plan, carried.QueryPlan.Normalization.DatasetContract)
		if terminals := result.LevelTerminals(); len(terminals) == 0 {
			t.Fatal("an uptime Python fails on was run")
		}
	})
}

// The same holds for the history-comparison algorithms: Python reads each
// one's parameters by name (bkmonitor/strategy/serializers.py) and passes over
// anything else in the config.
func TestAComparisonConfigKeyThePlatformDoesNotReadLeavesTheLevelAsItWas(t *testing.T) {
	planner, err := controlplane.NewLegacyPrimaryQueryCompiler("uq-primary-v1", "UTC", testLegacyQueryRuntimeFacts())
	if err != nil {
		t.Fatal(err)
	}
	build := func(config map[string]any) controlplane.QueryGroup {
		t.Helper()
		document := g4LegacyStrategyDocument(t, 300, strategy.DetectorKindAdvancedYearRound, "latency", "custom.application", []string{"service"}, config)
		catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{Strategies: []controlplane.SourceStrategy{{SourceID: "300", Document: document, Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}}}, Planner: planner})
		if err != nil {
			t.Fatal(err)
		}
		if len(catalog.QueryGroups) != 1 {
			t.Fatalf("catalog=%+v", catalog)
		}
		return catalog.QueryGroups[0]
	}
	plain := build(map[string]any{"ceil": 20, "ceil_interval": 2, "fetch_type": "avg"})
	carried := build(map[string]any{"ceil": 20, "ceil_interval": 2, "fetch_type": "avg", "hover": false, "algorithmUnit": "%"})
	want := compileEveryLevel(t, plain)
	got := compileEveryLevel(t, carried)
	if got.StateCompatibilityHash() != want.StateCompatibilityHash() {
		t.Fatal("a key the platform does not read changed how the strategy runs")
	}
	if compiledFrom(t, carried) != compiledFrom(t, plain) {
		t.Fatal("a key the platform does not read reached the published plan")
	}
}

// compiledFrom is what the published plan hands the compiler. The plan also
// carries the source document as it came, for the output the platform writes
// (legacy_output), and that keeps every key.
func compiledFrom(t *testing.T, group controlplane.QueryGroup) string {
	t.Helper()
	encoded, err := json.Marshal(group.Plans[0].Plan.StrategyIR)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func compileEveryLevel(t *testing.T, group controlplane.QueryGroup) *strategy.CompiledPlan {
	t.Helper()
	result := evaluationCoreResult(t, group.Plans[0].Plan, group.QueryPlan.Normalization.DatasetContract)
	if terminals := result.LevelTerminals(); len(terminals) != 0 {
		t.Fatalf("levels refused: %+v", terminals)
	}
	return compileWithEvaluationCore(t, group.Plans[0].Plan, group.QueryPlan.Normalization.DatasetContract)
}
