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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// generationFromSlot answers the Plans' activations as the recording ports
// do, under generation for a Slot from from on: the Plan's activation after
// an edit that moved its generation, the Slot before it frozen under the one
// before.
type generationFromSlot struct {
	*recordingPorts
	from       execution.EvaluationTime
	generation execution.StateGeneration
}

func (activations *generationFromSlot) LoadActivations(ctx context.Context, request execution.PlanActivationRequest) (
	execution.PlanActivationResult, error) {
	result, err := activations.recordingPorts.LoadActivations(ctx, request)
	if err == nil && request.Contract.Slot.EvaluationTime >= activations.from {
		for index := range result.Facts {
			result.Facts[index].Selected.StateGeneration = activations.generation
		}
	}
	return result, err
}

// A replay short of time - distance one, age zero, sent to replay before
// its deadline for a read that would not fit - notes its Plans' generations
// as the normal round in its place does (014 next9 2, the readers that took
// a normal round alone): a supplement of the Slot before it, written under
// the generation the replay ran past, counts generation_moved. A group
// diverted round after round has no normal Slot, and its supplements read as
// never moved. A replay past its deadline is late for its time and notes
// nothing.
func TestAReplayShortOfTimeNotesItsPlansGenerationAsTheNormalRoundWould(t *testing.T) {
	for _, c := range []struct {
		name      string
		ageMillis int64
		moved     bool
	}{
		{"short of time", 0, true},
		{"past its deadline", 5_000, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			header := supplementHeader(t)
			ports, evaluator, _ := workerG4Coordinator(t)
			ports.gapMissing = true
			store := &rememberingState{recordingPorts: ports, applied: map[execution.StateKeyIdentity]execution.StateMutation{}}
			// The Slot after, under the Plan's next generation, run as a replay;
			// its activation is that generation's, the Slot before's its own.
			next := header
			next.DuePlans = append([]execution.DuePlan(nil), header.DuePlans...)
			next.DuePlans[0].StateGeneration = header.DuePlans[0].StateGeneration + "-next"
			next.Contract.Slot.EvaluationTime += 60
			activations := &generationFromSlot{recordingPorts: ports, from: next.Contract.Slot.EvaluationTime,
				generation: next.DuePlans[0].StateGeneration}
			coordinator, err := worker.NewSlotExecutionCoordinator(worker.Ports{OpenAlerts: ports,
				Finalization: ports, Activation: activations, Query: ports, Sequencer: ports, Evaluator: evaluator,
				Admission: ports, GapGuard: ports, NoData: worker.SharedNoDataStore, Hosts: worker.SharedHostBusiness,
				Events: ports, State: store, Progress: ports,
				Observer: observability.ObserverFunc(func(context.Context, observability.Observation) {}),
			}, worker.ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxStateMutations: 100, MaxEvents: 100, MaxGapMutations: 10})
			if err != nil {
				t.Fatal(err)
			}
			digest, err := execution.DeriveDuePlanSetDigest(next.DuePlans, next.Requirements)
			if err != nil {
				t.Fatal(err)
			}
			next.Contract.DuePlanSetDigest = digest
			batches, completion := supplementRead(t, next, map[execution.SeriesIdentityDigest]string{hotSeries: "80"})
			ports.executeOverride = streamExecution(next, batches, completion)
			slot := workerSlotRequest(next.Contract)
			slot.DuePlanTargets.DuePlanSetDigest = digest
			slot.Operation, slot.ReplayDistance, slot.ReplayAgeMillis = execution.OperationReplay, 1, c.ageMillis
			if result, err := coordinator.Execute(context.Background(), slot); err != nil || !result.Completed {
				t.Fatalf("the replay: result %+v error %v", result, err)
			}

			// The supplement of the Slot before, under the generation before.
			batches, completion = supplementRead(t, header, map[execution.SeriesIdentityDigest]string{hotSeries: "80", calmSeries: "90"})
			ports.executeOverride = streamExecution(header, batches, completion)
			result, err := coordinator.Execute(context.Background(), supplementRequest(header.Contract, hotSeries, calmSeries))
			if err != nil || result.Supplement == nil || result.Supplement.Admitted == 0 {
				t.Fatalf("the supplement: facts %+v error %v, want a series admitted", result.Supplement, err)
			}
			if facts := result.Supplement; (facts.GenerationMoved == facts.Admitted) != c.moved || (facts.GenerationMoved == 0) == c.moved {
				t.Fatalf("the supplement after a replay %s: %+v; want generation_moved %t", c.name, *facts, c.moved)
			}
		})
	}
}
