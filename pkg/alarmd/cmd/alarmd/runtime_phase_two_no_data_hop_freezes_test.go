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
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The no-data hops a worker reports count its freezes of a Slot's contract,
// on the process's catalog: a diagnostic read of a Slot through the CLI's
// resolver freezes it on a catalog of its own that no observer hears, and
// counts nothing - neither hop, nor a generation skew; a supplement's freeze,
// on the process's catalog, counts as every freeze there does.
func TestACLIReadOfASlotCountsNoNoDataHopAndASupplementsFreezeDoes(t *testing.T) {
	ctx := context.Background()
	fixture := startNoDataFixtureOn(t, config.OutputProtocolNative)
	fixture.hasData.Store(true)
	for round := int64(1); round <= 2; round++ {
		fixture.runSlot(ctx, round)
	}
	read := func() map[string]float64 {
		t.Helper()
		counts := map[string]float64{}
		for _, hop := range []string{observability.NoDataHopAssembledBytes, observability.NoDataHopAssembled, observability.NoDataHopFrozen} {
			counts[hop], _ = servedSample(t, fixture.recorder, "alarmd_no_data_plans_by_hop_total", map[string]string{"hop": hop})
		}
		for _, kind := range observability.StateGenerationSkewKinds {
			counts["skew_"+kind], _ = servedSample(t, fixture.recorder, "alarmd_state_generation_skew_total", map[string]string{"kind": kind})
		}
		return counts
	}
	slot := execution.SlotIdentity{QueryGroup: fixture.queryGroup, EvaluationTime: execution.EvaluationTime(fixture.base + fixture.interval)}

	before := read()
	plan, err := newCLISlotResolverAt(fixture.cfg, fixture.client, fixture.now)(ctx, slot)
	if err != nil {
		t.Fatalf("the CLI's read of the Slot: %v", err)
	}
	if after := read(); !reflect.DeepEqual(after, before) {
		t.Fatalf("the CLI's read of a Slot moved the process's counts from %v to %v, want none moved", before, after)
	}

	runtime := fixture.runner.(*settlingRuntime).phaseTwoQueryGroupRuntime.(*productionPhaseTwoQueryGroup)
	if _, _, err := runtime.SupplementContract(ctx, slot.EvaluationTime, plan.Contract.ReadHoldMillis); err != nil {
		t.Fatalf("the supplement's freeze: %v", err)
	}
	after := read()
	for _, hop := range []string{observability.NoDataHopAssembled, observability.NoDataHopFrozen} {
		if after[hop] != before[hop]+1 {
			t.Fatalf("hop %s %v after the supplement's freeze, want %v: one no-data Plan, one freeze", hop, after[hop], before[hop]+1)
		}
	}
}
