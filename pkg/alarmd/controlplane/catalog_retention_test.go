// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package controlplane

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The Catalog measures the retained window of every Level it compiles, which
// is what says how much decision-022 R5's recovery slack costs a deployment.
//
// Measured here and nowhere else because here is where every Plan is compiled.
// The alternative offered itself: the composition downstream already walks
// every Plan, and deriving the window from the strategy document there would
// have needed no new field. It would also have put the compiler's reading of
// the trigger and recovery plans in a second place, where the two can disagree
// with nothing comparing them.
//
// Two Levels, and the difference between them is the whole measurement: one
// spans longer than the hole tolerance and retains a slack, one spans less and
// retains exactly its window. A fixture of only the second kind would report
// zero slack and pass against an implementation that never grants any.
func TestRuntimeExecutableCatalogMeasuresRetainedPoints(t *testing.T) {
	compiler, semantics := runtimeClosureCompiler(t)

	// Window 1, recovery run 1, on a one minute interval: a nine second span
	// against a ten minute tolerance, so the span gate refuses it the slack.
	short := runtimeClosureLevel(1, strategy.DetectorKindThreshold)
	// Window 30, recovery run 1440: a short window and a long recovery run,
	// the shape a day-long recovery span takes. The slack its walk needs is
	// small beside the window it already keeps.
	long := runtimeClosureLevel(2, strategy.DetectorKindThreshold)
	long.TriggerPlan.Config = json.RawMessage(`{"window_size":30,"required_anomalies":1,"step_seconds":60}`)
	long.RecoveryPlan.Config = json.RawMessage(`{"enabled":true,"consecutive_windows":1440}`)

	plan := runtimeClosureNamedFrozenPlan(t, "1001", "1", []contract.LevelIRV2{short, long})
	catalog := runtimeClosureCatalog(runtimeClosureQueryGroup(t, "1", plan))
	built, err := retainRuntimeExecutableCatalog(context.Background(), catalog, nil, compiler, semantics)
	if err != nil {
		t.Fatal(err)
	}

	// What the compiler actually produced, read back rather than restated, so
	// the expectation cannot drift from the derivation it is checking.
	compiled := runtimeClosureCompile(t, plan.Plan, 2)
	var wantRequired, wantRetention uint64
	for _, level := range compiled.Levels() {
		wantRequired += uint64(level.StateRequirement().RequiredDetectHistoryPoints)
		wantRetention += uint64(level.StateRequirement().RetentionPoints)
	}

	got := built.Retention
	if got.RequiredPoints != wantRequired || got.RetentionPoints != wantRetention {
		t.Fatalf("retention = %+v, want required %d and retained %d", got, wantRequired, wantRetention)
	}
	if got.RetentionPoints <= got.RequiredPoints {
		t.Fatalf("retention = %+v: the long Level retains no slack, so this case cannot tell a build that "+
			"grants it from one that does not", got)
	}
	// Exactly one of the two pays, and it is the one whose recovery run is the
	// larger term. Counting them apart is the point: the slack follows the
	// trigger window while the required size follows both, so a single count
	// would read the same for a Level that costs a few points and one that
	// doubles.
	if got.LevelsWithSlack != 1 || got.LevelsWithSlackRecoveryDominant != 1 || got.LevelsWithSlackWindowDominant != 0 {
		t.Fatalf("retention = %+v, want one paying Level counted as recovery dominant", got)
	}

	// The composition carries the measurement out unchanged. It is the only
	// path it travels, so a composition that dropped it would leave the
	// metric reading zero on a deployment that pays.
	if composed := ComposeCatalog(built).Retention; composed != got {
		t.Fatalf("composition retention = %+v, want the Catalog's own %+v", composed, got)
	}
}
