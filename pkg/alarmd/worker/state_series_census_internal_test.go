// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The sink writes the batch but refuses one series' event: that series keeps
// its State and is output_partial; its Plan's other series and the other
// Plan's are written. Three admitted, two written, one not, on the one census
// the completed attempt reports.
func TestAPartlyRefusedOutputCountsTheRefusedSeriesAsPartial(t *testing.T) {
	fixture := newOutputIsolationFixture(t)
	if _, err := fixture.finalize(t); err != nil {
		t.Fatalf("finalizePrepared() error = %v", err)
	}
	var reports []*observability.FrozenStateRenewalFacts
	for _, observation := range fixture.observations {
		if observation.Stage == observability.StageFrozenStateRenewed {
			reports = append(reports, observation.FrozenStateRenewal)
		}
	}
	if len(reports) != 1 {
		t.Fatalf("census reported %d times, want once", len(reports))
	}
	facts := reports[0]
	if facts.Admitted() != 3 || facts.Written != 2 || facts.NotWrittenTotal() != 1 ||
		facts.NotWritten[observability.StateNotWrittenOutputPartial] != 1 {
		t.Fatalf("census admitted %d written %d not written %v, want 3, 2 and one output_partial",
			facts.Admitted(), facts.Written, facts.NotWritten)
	}
}

// A Slot reports one census per attempt: one that completed is settled by
// its completion, and the Slot's end leaves it alone; one that never reached
// its completion is settled there, its writes as written and the rest it
// admitted as slot_failed.
func TestTheSlotsEndSettlesOnlyAnAttemptItsCompletionDidNot(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{}
	var reported []observability.FrozenStateRenewalFacts
	coordinator.ports.Observer = observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		if observation.FrozenStateRenewal != nil {
			reported = append(reported, *observation.FrozenStateRenewal)
		}
	})
	ctx, sink := withSlotCensusSink(context.Background())
	settled, failed := &slotApply{coordinator: coordinator}, &slotApply{coordinator: coordinator}
	registerSlotApply(ctx, settled)
	registerSlotApply(ctx, failed)
	for _, apply := range []*slotApply{settled, failed} {
		for range 3 {
			apply.frozenRenewals.RecordSeriesClass(observability.StateSeriesClassStableNormal, observability.StateSeriesReal)
		}
	}
	coordinator.observeFrozenStateRenewal(ctx, execution.OperationNormal, settled.settledCensus(ctx, seriesCensus{Due: 3, Read: 3, Written: 3}, false))
	failed.written = 1
	coordinator.settleSlotCensus(ctx, execution.OperationNormal, sink, seriesCensus{Due: 3, Read: 3})
	if len(reported) != 2 {
		t.Fatalf("census reported %d times, want once for each attempt", len(reported))
	}
	if got := reported[1]; got.Written != 1 || got.NotWritten[observability.StateNotWrittenSlotFailed] != 2 {
		t.Fatalf("failed attempt's census written %d not written %v, want 1 and two slot_failed", got.Written, got.NotWritten)
	}
	if reported[0].NotWrittenTotal() != 0 {
		t.Fatalf("the completed attempt's census was settled again: %v", reported[0].NotWritten)
	}
}

// On an attempt that completed every admitted mutation left by a named exit:
// a rest either way is unaccounted, by its size, and the attempt says so on a
// failed line of its own - not folded into slot_failed, which only a failed
// attempt's rest is.
func TestACompletedAttemptsRestIsUnaccountedEitherWay(t *testing.T) {
	for written, want := range map[int]int64{2: 1, 4: 1, 3: 0} {
		coordinator := &SlotExecutionCoordinator{}
		var lines []observability.Observation
		coordinator.ports.Observer = observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			lines = append(lines, observation)
		})
		apply := &slotApply{coordinator: coordinator}
		for range 3 {
			apply.frozenRenewals.RecordSeriesClass(observability.StateSeriesClassStableNormal, observability.StateSeriesReal)
		}
		facts := apply.settledCensus(context.Background(), seriesCensus{Due: 3, Read: 3, Written: written}, false)
		if got := facts.NotWritten[observability.StateNotWrittenUnaccounted]; got != want {
			t.Fatalf("written %d: unaccounted %d, want %d", written, got, want)
		}
		if facts.NotWritten[observability.StateNotWrittenSlotFailed] != 0 {
			t.Fatalf("written %d: a completed attempt's rest went to slot_failed", written)
		}
		failedLines := 0
		for _, line := range lines {
			if line.Result == observability.ResultFailed && line.ReasonCode == observability.ReasonInternalUnknown {
				failedLines++
			}
		}
		if failedLines != int(want) {
			t.Fatalf("written %d: %d unaccounted lines, want %d", written, failedLines, want)
		}
	}
}
