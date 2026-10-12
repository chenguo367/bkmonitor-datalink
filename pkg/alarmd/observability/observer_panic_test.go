// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"context"
	"testing"
)

// A panic in one observer of a fan-out is recovered there, and every
// observer after it still receives the observation - also inside a fan-out
// that is itself a member, and also when the panicking member forwards to
// that inner fan-out before it panics, as the tracker does.
func TestAPanickingObserverLeavesTheOthersTheirObservation(t *testing.T) {
	received := map[string]int{}
	receiving := func(name string) Observer {
		return ObserverFunc(func(context.Context, Observation) { received[name]++ })
	}
	panicking := ObserverFunc(func(context.Context, Observation) { panic("observer defect") })
	inner := Multi(panicking, receiving("logging"))
	tracker := ObserverFunc(func(ctx context.Context, observation Observation) {
		defer inner.Observe(ctx, observation)
		panic("tracker defect")
	})
	fanOut := Multi(tracker, panicking, receiving("target_flow"), nil, panicking, receiving("last"))
	fanOut.Observe(context.Background(), Observation{Component: ComponentRuntime})
	if received["logging"] != 1 || received["target_flow"] != 1 || received["last"] != 1 {
		t.Fatalf("an observer after a panicking one missed the observation: %v", received)
	}
}
