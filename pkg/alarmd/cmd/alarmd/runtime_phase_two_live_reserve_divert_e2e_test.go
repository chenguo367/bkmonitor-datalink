// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"
)

// slotDiversions reads alarmd_slot_diversions_total for reason.
func slotDiversions(t *testing.T, fixture *cutoverStallFixture, reason string) float64 {
	t.Helper()
	families, err := fixture.bundle.dependencies.Recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "alarmd_slot_diversions_total" {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return series.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// Through the production runtime, a round is judged short of time against the
// deadline its read is cut at under the runtime's measured post-query tail.
// The tail here is thirty seconds, well past the frozen reserve: twenty-eight
// seconds after evaluation the round has two seconds before that deadline,
// short of the five a read is given, and goes to replay as readiness_late,
// though by its frozen deadline it had well over five.
func TestTheProductionRuntimeDivertsByTheMeasuredTail(t *testing.T) {
	fixture := startCutoverFixture(t, nil)
	fixture.production.dependencies.Flights.LiveReserve().Set(30 * time.Second)
	before := slotDiversions(t, fixture, "readiness_late")
	fixture.clock.Store((fixture.base + 60 + 28) * 1000)
	if _, _, err := fixture.runner.RunOne(context.Background()); err != nil {
		t.Fatalf("RunOne() = %v", err)
	}
	if after := slotDiversions(t, fixture, "readiness_late"); after != before+1 {
		t.Fatalf("readiness_late diversions %v -> %v, want one more", before, after)
	}
}
