// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import (
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func slotPlan(scope *contract.TargetScopeV2, dimensions []string) *contract.EvaluationPlanV2 {
	return &contract.EvaluationPlanV2{
		PlanID: "1001", TargetScope: scope,
		NoData: &contract.NoDataConfigV1{Continuous: 3, Level: 2, AggDimension: dimensions},
	}
}

// A Plan that does not detect no-data is answered with "nothing to do" rather
// than an error or a zero decision that looks like one. The caller asks every
// Plan and this is what makes that safe.
func TestEvaluateSlotSaysNothingForAPlanThatDoesNotDetectNoData(t *testing.T) {
	for name, plan := range map[string]*contract.EvaluationPlanV2{
		"no plan":    nil,
		"no section": {PlanID: "1001"},
	} {
		t.Run(name, func(t *testing.T) {
			result, evaluated, err := EvaluateSlot(SlotInput{Plan: plan})
			if err != nil || evaluated {
				t.Fatalf("EvaluateSlot() = %+v, %t, %v; want no evaluation and no error", result, evaluated, err)
			}
		})
	}
}

// The whole chain: a static target's hosts are expected, the one that reported
// is normal and the one that did not is an anomaly with its absence clock
// started. Nothing about the values reaches any of it.
func TestEvaluateSlotDecidesAStaticTargetFromWhichSeriesReported(t *testing.T) {
	scope := hostScope("10.0.0.1|0", "10.0.0.2|0")
	present := hostTargetGroup(HostIdentity{IP: "10.0.0.1", CloudID: "0"})
	absent := hostTargetGroup(HostIdentity{IP: "10.0.0.2", CloudID: "0"})

	result, evaluated, err := EvaluateSlot(SlotInput{
		Plan:           slotPlan(scope, []string{HostIPDimension, HostCloudDimension}),
		EvaluationTime: 1000, PeriodSeconds: 60, Completeness: execution.CompletenessFull,
		Series: []map[string]string{
			{HostIPDimension: "10.0.0.1", HostCloudDimension: "0", "device": "eth0"},
		},
		KnownHosts:    knownHosts("10.0.0.1|0", "10.0.0.2|0"),
		Memory:        map[string]GroupMemory{},
		RosterVersion: "v1",
	})
	if err != nil || !evaluated {
		t.Fatalf("EvaluateSlot() = %t, %v", evaluated, err)
	}
	if got := result.Verdicts[present.Key()]; got != VerdictNormal {
		t.Fatalf("the host that reported is %q, want %q", got, VerdictNormal)
	}
	if got := result.Verdicts[absent.Key()]; got != VerdictAnomaly {
		t.Fatalf("the host that did not report is %q, want %q", got, VerdictAnomaly)
	}
	if entry := result.Memory[absent.Key()]; entry.FirstAbsent != 1000 {
		t.Fatalf("Memory[absent] = %+v, want its absence clock started at this round", entry)
	}
	if result.Facts.RosterSource != RosterTargetStatic || result.Facts.RosterVersion != "v1" {
		t.Fatalf("Facts = %+v, want the target source and the stated version", result.Facts)
	}
}

// A series whose dimensions do not carry the item's no-data dimensions is
// dropped and counted, and the group it would have been is not invented: the
// host that did report is still absent as far as this item can tell.
func TestEvaluateSlotCountsASeriesItCannotProject(t *testing.T) {
	scope := hostScope("10.0.0.1|0")
	result, _, err := EvaluateSlot(SlotInput{
		Plan:           slotPlan(scope, []string{HostIPDimension, HostCloudDimension}),
		EvaluationTime: 1000, PeriodSeconds: 60, Completeness: execution.CompletenessFull,
		Series:     []map[string]string{{HostIPDimension: "10.0.0.1"}},
		KnownHosts: knownHosts("10.0.0.1|0"),
		Memory:     map[string]GroupMemory{},
	})
	if err != nil {
		t.Fatalf("EvaluateSlot() error = %v", err)
	}
	if result.Facts.Dropped != 1 {
		t.Fatalf("Dropped = %d, want the series with no cloud dimension counted", result.Facts.Dropped)
	}
	group := hostTargetGroup(HostIdentity{IP: "10.0.0.1", CloudID: "0"})
	if got := result.Verdicts[group.Key()]; got != VerdictAnomaly {
		t.Fatalf("verdict = %q, want %q: a series that could not be projected did not arrive", got, VerdictAnomaly)
	}
}

// The completeness gate reaches through the seam. A Slot that did not see the
// whole round says nothing about absence, and the memory it was given comes
// back unchanged.
func TestEvaluateSlotPassesTheCompletenessGateThrough(t *testing.T) {
	scope := hostScope("10.0.0.1|0")
	group := hostTargetGroup(HostIdentity{IP: "10.0.0.1", CloudID: "0"})
	memory := map[string]GroupMemory{group.Key(): {LastSeen: 500}}
	result, _, err := EvaluateSlot(SlotInput{
		Plan:           slotPlan(scope, []string{HostIPDimension, HostCloudDimension}),
		EvaluationTime: 1000, PeriodSeconds: 60, Completeness: execution.CompletenessPartial,
		KnownHosts: knownHosts("10.0.0.1|0"), Memory: memory,
	})
	if err != nil {
		t.Fatalf("EvaluateSlot() error = %v", err)
	}
	if got := result.Verdicts[group.Key()]; got != VerdictUnavailable {
		t.Fatalf("verdict = %q, want %q on a Slot that did not see the whole round", got, VerdictUnavailable)
	}
	if entry := result.Memory[group.Key()]; entry != memory[group.Key()] {
		t.Fatalf("Memory = %+v, want it untouched", entry)
	}
}

// A Plan the compiler should have refused is refused here too rather than
// evaluated into an empty decision. The seam is where a Plan that got past the
// compiler stops, and it stops loudly.
func TestEvaluateSlotRefusesAPlanWhoseRosterCannotBeDerived(t *testing.T) {
	excluded := excludedHostScope("10.0.0.1|0")
	_, evaluated, err := EvaluateSlot(SlotInput{
		Plan:           slotPlan(excluded, []string{HostIPDimension, HostCloudDimension}),
		EvaluationTime: 1000, PeriodSeconds: 60, Completeness: execution.CompletenessFull,
	})
	var unsupported *RosterUnsupportedError
	if !errors.As(err, &unsupported) || evaluated {
		t.Fatalf("EvaluateSlot() = %t, %v; want a RosterUnsupportedError", evaluated, err)
	}
}
