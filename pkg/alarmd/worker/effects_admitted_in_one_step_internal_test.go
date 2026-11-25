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

// A Slot's side effects in hand are judged against its per-Slot caps and
// added in one step: an admitted merge counts for the next one whoever
// merges next, a refused one adds nothing, and merges side by side admit no
// more than the cap holds (015 C1 appendix A 5).
func TestASlotsSideEffectsAreJudgedAndAddedInOneStep(t *testing.T) {
	budget := ProvisionalBudget{MaxStateMutations: 10, MaxEvents: 10, MaxGapMutations: 10}
	stream := &streamedExecution{slotStream: &slotStream{}}
	if _, err := stream.admitEffects(effectCounts{states: 6}, budget); err != nil {
		t.Fatalf("six of ten were refused: %v", err)
	}
	count, err := stream.admitEffects(effectCounts{states: 6}, budget)
	var exceeded *provisionalBudgetExceededError
	if !errors.As(err, &exceeded) || count.states != 12 || stream.effects.states != 6 {
		t.Fatalf("six more ended %v judged at %+v holding %+v, want refused at twelve with the first six held", err, count, stream.effects)
	}
	stream.giveBackEffects(effectCounts{states: 6})
	if _, err := stream.admitEffects(effectCounts{states: 6}, budget); err != nil {
		t.Fatalf("six of ten after the first six were given back were refused: %v", err)
	}

	side := &streamedExecution{slotStream: &slotStream{}}
	var admitted atomic.Int32
	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := side.admitEffects(effectCounts{states: 1}, budget); err == nil {
				admitted.Add(1)
			}
		}()
	}
	wait.Wait()
	if admitted.Load() != 10 || side.effects.states != 10 {
		t.Fatalf("%d of twenty merges of one admitted, holding %+v; want the cap's ten", admitted.Load(), side.effects)
	}
}
