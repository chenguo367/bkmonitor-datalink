// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A worker without a no-data store does not start.
//
// The alternative is a worker that evaluates every Plan's thresholds and none
// of their absence, where the only sign is no-data alerts that never fire -
// which on a quiet deployment is indistinguishable from nothing being absent.
// Every other port here is required for the same reason, and this one gets its
// own test because the failure it prevents leaves nothing behind to find.
func TestAWorkerWithoutANoDataStoreDoesNotStart(t *testing.T) {
	trace := make([]string, 0)
	ports := &recordingPorts{trace: &trace}
	observer := observability.ObserverFunc(func(context.Context, observability.Observation) {})
	budget := worker.ProvisionalBudget{
		MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 100, MaxEvents: 100, MaxGapMutations: 10,
	}
	complete := worker.Ports{
		OpenAlerts: ports, Finalization: ports, Activation: ports, Query: ports, Sequencer: ports,
		Evaluator: ports, Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
		Events: ports, State: ports, Progress: ports, Observer: observer,
	}
	// The control: a complete set starts, so the refusal below is about the one
	// port that was taken away and not about the fixture.
	if _, err := worker.NewSlotExecutionCoordinator(complete, budget); err != nil {
		t.Fatalf("fixture: a complete set of ports was refused: %v", err)
	}

	for name, remove := range map[string]func(*worker.Ports){
		"no no-data store": func(ports *worker.Ports) { ports.NoData = nil },
		// Without it every declared host reads as unknown, so every static
		// target expects nothing: a silence that looks exactly like health.
		"no host lookup": func(ports *worker.Ports) { ports.Hosts = nil },
	} {
		t.Run(name, func(t *testing.T) {
			without := complete
			remove(&without)
			if _, err := worker.NewSlotExecutionCoordinator(without, budget); err == nil {
				t.Fatal("the worker started without a port no-data cannot work without, and the failure " +
					"that follows leaves nothing behind to find")
			}
		})
	}
}
