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
	"runtime"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The Slot's census sink, on its context, keeps an attempt's census and
// nothing else of it: while the context lives on - as one does past its
// Slot when a child of it is never cancelled - the attempt and its State
// keys are let go with the attempt, its census still reported at the Slot's
// end, which empties the sink.
func TestTheCensusSinkKeepsNoneOfAnAttemptButItsCensus(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{}
	var reported []observability.FrozenStateRenewalFacts
	coordinator.ports.Observer = observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		if observation.FrozenStateRenewal != nil {
			reported = append(reported, *observation.FrozenStateRenewal)
		}
	})
	ctx, sink := withSlotCensusSink(context.Background())
	collected := make(chan struct{})
	func() {
		apply := &slotApply{coordinator: coordinator, attemptCensus: &attemptCensus{}, keys: make(slotStateKeys)}
		apply.keys[execution.StateKeyIdentity{}] = slotKeyApply
		registerSlotApply(ctx, apply)
		apply.frozenRenewals.RecordSeriesClass(observability.StateSeriesClassStableNormal, observability.StateSeriesReal)
		runtime.SetFinalizer(apply, func(*slotApply) { close(collected) })
	}()
	expired := testwait.Expired(t)
	for gone := false; !gone; {
		runtime.GC()
		select {
		case <-collected:
			gone = true
		case <-expired:
			t.Fatal("the attempt and its State keys are kept by the census sink on the Slot's context")
		default:
			runtime.Gosched()
		}
	}
	coordinator.settleSlotCensus(ctx, execution.OperationNormal, sink, seriesCensus{Due: 1, Read: 1})
	if len(reported) != 1 || reported[0].NotWritten[observability.StateNotWrittenSlotFailed] != 1 {
		t.Fatalf("census reported %v, want the attempt's one admitted series as slot_failed", reported)
	}
	if sink.attempts != nil {
		t.Fatalf("the Slot's end left %d censuses in the sink", len(sink.attempts))
	}
	runtime.KeepAlive(ctx)
}
