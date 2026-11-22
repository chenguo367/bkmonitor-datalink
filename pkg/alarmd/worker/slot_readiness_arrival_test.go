// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// arrivals counts the readiness arrivals the access source reports.
type arrivals struct {
	mu    sync.Mutex
	count int
}

func (counted *arrivals) Observe(_ context.Context, observation observability.Observation) {
	if observation.Stage == observability.StageSlotReadinessArrival {
		counted.mu.Lock()
		counted.count++
		counted.mu.Unlock()
	}
}

// A Slot arrives once, at its first read, whether it is read whole or in
// slices: through the coordinator and the real access source, a Slot read in
// slices - in turn and ahead - reports one readiness arrival, as one read
// whole does. Counted per read, the slices after the first arrived late by
// what the reads before them took.
func TestASlotArrivesOnceAtItsFirstReadWholeOrInSlices(t *testing.T) {
	for _, test := range []struct {
		name    string
		padding int
		sliced  bool
	}{{"read whole", 0, false}, {"read in slices", 40 << 10, true}} {
		t.Run(test.name, func(t *testing.T) {
			counted := &arrivals{}
			rig := newRingRatioRigObserved(t, 24, test.padding, counted)
			result, err := rig.coordinator.Execute(context.Background(), rig.request)
			if err != nil || !result.Completed {
				t.Fatalf("result %+v, error %v, want the Slot completed", result, err)
			}
			if sliced := result.Usage.InputSlices > 1; sliced != test.sliced {
				t.Fatalf("read in %d slices, want sliced %v", result.Usage.InputSlices, test.sliced)
			}
			counted.mu.Lock()
			defer counted.mu.Unlock()
			if counted.count != 1 {
				t.Fatalf("%d readiness arrivals for a Slot read in %d slices, want one", counted.count, result.Usage.InputSlices)
			}
		})
	}
}
