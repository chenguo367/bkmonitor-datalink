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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// A reservation charges the Slot's total in the step that judged it against
// the share: once it returns, the next one is judged on a total that counts
// it, whoever asks next. Charged after, two asks of the same Slot each
// judged on a total counting neither both passed - the share's three
// quarters of a 100-byte pool taken by 80 (015 C1 appendix A 4).
func TestAReservationIsChargedInTheStepThatJudgesIt(t *testing.T) {
	budget := ProvisionalBudget{MaxSeries: 10, MaxRetainedBytes: 100, MaxStateMutations: 10, MaxEvents: 10, MaxGapMutations: 10}
	asks := map[string]func(*SlotExecutionCoordinator, *streamedExecution) error{
		"input": func(co *SlotExecutionCoordinator, stream *streamedExecution) error {
			return co.acquireProvisional(0, 40, stream, "normal_input", retainPhaseInput)
		},
		"side effects": func(co *SlotExecutionCoordinator, stream *streamedExecution) error {
			return co.acquireEffects(effectCounts{states: 1}, stream, "normal_output",
				retainedCharges{retainPhaseState: 30, retainPhaseOutput: 10})
		},
		"a hold": func(co *SlotExecutionCoordinator, stream *streamedExecution) error {
			return co.holdRetained(stream, 40, "frames")
		},
	}
	for name, ask := range asks {
		t.Run(name+", one after another", func(t *testing.T) {
			co := &SlotExecutionCoordinator{budget: budget}
			stream := &streamedExecution{slotStream: &slotStream{coordinator: co, began: true}}
			if err := ask(co, stream); err != nil {
				t.Fatalf("the first ask of 40 was refused: %v", err)
			}
			if total := stream.retainedTotal(); total != 40 {
				t.Fatalf("the Slot's total after its reservation is %d, want the 40 it was judged for", total)
			}
			var exceeded *provisionalBudgetExceededError
			if err := ask(co, stream); !errors.As(err, &exceeded) || !exceeded.share {
				t.Fatalf("the second ask of 40 ended %v, want it refused by the share on the first's 40", err)
			}
		})
	}
	t.Run("side by side", func(t *testing.T) {
		co := &SlotExecutionCoordinator{budget: budget}
		stream := &streamedExecution{slotStream: &slotStream{coordinator: co, began: true}}
		var passed atomic.Int32
		var wait sync.WaitGroup
		for range 8 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				if co.acquireProvisional(0, 40, stream, "normal_input", retainPhaseInput) == nil {
					passed.Add(1)
				}
			}()
		}
		wait.Wait()
		if passed.Load() != 1 || RetainedReserved(co) != 40 {
			t.Fatalf("%d of eight asks of 40 passed, the pool holding %d; want one within the share of 75", passed.Load(), RetainedReserved(co))
		}
	})
}
