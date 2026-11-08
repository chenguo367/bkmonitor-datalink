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
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A refresh round the process stops in the middle of is not a failed round:
// no failed snapshot_refreshed line, and no renewal on the context that was
// called off, which would only fail and be written as a failure in turn. The
// same source failing under a live context is a failed round, written and
// renewed after as before.
func TestARefreshRoundCalledOffIsNotAFailedRound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		calledOff bool
		err       error
	}{
		{name: "called off", calledOff: true, err: context.Canceled},
		{name: "source failing under a live context", err: errors.New("source unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			publication := controlplane.SnapshotPublicationRef{SnapshotRevision: "snapshot-current", PublicationEpoch: 2}
			reconciler := &fakeSourceReconciler{results: []controlplane.SourceRefreshResult{{}}, errs: []error{tc.err}}
			repository := &fakeProductionCatalogRepository{activation: controlplane.ActivationState{RecordRevision: 2, Current: publication},
				snapshot: controlplane.PublishedSnapshot{Publication: publication}}
			var failed []observability.Observation
			control, err := newProductionPhaseTwoControl(productionPhaseTwoControlDependencies{
				Source: fakeStrategySource{}, Planner: fakePrimaryQueryCompiler{}, Reconciler: reconciler,
				Activator: &fakeInitialScheduleActivator{state: repository.activation}, Repository: repository,
				Schedules: &fakeScheduleProjection{}, Progress: &fakeProductionProgressReader{},
				Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					if observation.Result == observability.ResultFailed && observation.Stage == observability.StageSnapshotRefreshed {
						failed = append(failed, observation)
					}
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.calledOff {
				cancel()
			}
			_, _ = control.Refresh(ctx)
			if tc.calledOff {
				if len(failed) != 0 || repository.renewCalls != 0 {
					t.Fatalf("a round called off: failed lines %+v, renewals %d; want none", failed, repository.renewCalls)
				}
				return
			}
			if len(failed) != 1 || repository.renewCalls != 1 {
				t.Fatalf("a source failing under a live context: failed lines %+v, renewals %d; want one each", failed, repository.renewCalls)
			}
		})
	}
}
