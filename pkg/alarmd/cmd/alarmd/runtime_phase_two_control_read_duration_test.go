// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Through the production bundle on a real Redis, a reconcile round's control
// reads say what they took: every control_reads_spent line whose read made a
// round trip has its milliseconds, and control_read_duration_seconds{read}
// sums them. A duration the store set on a copy its return had already taken
// read 0 on every round, every observation of both reads in the histogram's
// first bucket.
func TestARoundsControlReadsSayWhatTheyTook(t *testing.T) {
	fixture := startCutoverFixture(t, nil)
	spent := func() []observability.ControlReadFacts {
		var out []observability.ControlReadFacts
		for _, observation := range fixture.observed() {
			if observation.Stage == observability.StageControlReadsSpent && observation.ControlReads != nil {
				out = append(out, *observation.ControlReads)
			}
		}
		return out
	}
	waitUntil(t, "a round that read the registry and the term's records", func() bool {
		registry, assignments := false, false
		for _, facts := range spent() {
			registry = registry || facts.RegistryRoundTrips > 0
			assignments = assignments || facts.AssignmentRoundTrips > 0
		}
		return registry && assignments
	})
	for index, facts := range spent() {
		if facts.RegistryRoundTrips > 0 && facts.RegistryMilliseconds <= 0 {
			t.Fatalf("round %d read the registry in %d round trips and says it took %v ms, want what it took",
				index, facts.RegistryRoundTrips, facts.RegistryMilliseconds)
		}
		if facts.AssignmentRoundTrips > 0 && facts.AssignmentMilliseconds <= 0 {
			t.Fatalf("round %d read %d assignment records in %d round trips and says it took %v ms, want what it took",
				index, facts.AssignmentKeys, facts.AssignmentRoundTrips, facts.AssignmentMilliseconds)
		}
	}
	families, err := fixture.recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "alarmd_control_read_duration_seconds" {
			continue
		}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if label.GetName() == "read" {
					sums[label.GetValue()] += sample.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	if sums["registry"] <= 0 || sums["assignment"] <= 0 {
		t.Fatalf("control_read_duration_seconds sums %v, want both reads' time", sums)
	}
}
