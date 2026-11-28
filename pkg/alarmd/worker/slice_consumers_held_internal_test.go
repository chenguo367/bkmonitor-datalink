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
	"sync"
	"sync/atomic"
	"testing"
)

// What a consumer counts of the dispatch is read on the consumer's goroutine
// while the dispatch, on its own, lands the windows it had in flight and
// takes on more. Counted lower than the dispatch holds beside those windows,
// a consumer's limit would be wider than the share leaves it - the Slot past
// its share - so a consumer never counts less: not while a landing takes
// bytes off what the dispatch holds, nor before what it takes on since is
// counted. Run under -race, the reads and the dispatch's changes share
// nothing but its atomic counters.
func TestAConsumerNeverCountsTheDispatchBelowWhatItHoldsBesideItsWindowsInFlight(t *testing.T) {
	dispatch := &streamedExecution{slotStream: &slotStream{}}
	// As the consumers begin: 1,000 bytes of input and 200 of State, of
	// which its windows in flight give back all the input and 150 of the
	// State as they land; 50 it holds beside them, for the Slot.
	const landings, chunk = 100, 10
	dispatch.ownNow[retainPhaseInput].Store(landings * chunk)
	dispatch.ownNow[retainPhaseState].Store(200)
	const beside = 50
	counted := func() uint64 {
		var total uint64
		for phase := range retainPhaseCount {
			total += dispatch.heldForConsumers(phase)
		}
		return total
	}
	// taken is what the dispatch took on since, published once it is held.
	var taken atomic.Uint64
	var done atomic.Bool
	var wait sync.WaitGroup
	failures := make(chan string, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for !done.Load() {
				least := beside + taken.Load()
				if got := counted(); got < least {
					select {
					case failures <- "counted below what the dispatch holds beside its windows in flight":
					default:
					}
					return
				}
			}
		}()
	}
	for index := range landings {
		// A window landing: its State share and its input off what the
		// dispatch holds, the same off what is in flight.
		if index%2 == 0 {
			dispatch.ownNow[retainPhaseState].Add(^uint64(3 - 1))
		}
		dispatch.ownNow[retainPhaseInput].Add(^uint64(chunk - 1))
		// And some taken on: held first, published after.
		dispatch.ownNow[retainPhaseGap].Add(7)
		taken.Add(7)
	}
	done.Store(true)
	wait.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	if got := dispatch.heldForConsumers(retainPhaseInput); got != 0 {
		t.Fatalf("every window landed, the dispatch's input is counted at %d, want none: it was all in flight", got)
	}
	if got, want := counted(), uint64(beside+landings*7); got != want {
		t.Fatalf("every window landed, the dispatch is counted at %d, want %d: what it held beside them and took on since", got, want)
	}
}
