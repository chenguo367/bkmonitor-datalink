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
	"sort"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// sliceLedger is, slice by slice, what each slice of a Slot contributed to
// what is read of the Slot "so far": the series it bound to each Plan, the
// bindings it kept, and the series of each Plan it wrote. A slice's head
// reads the slices numbered up to its own - whatever order they completed
// in - and the Slot's end reads them all. A slice's number is the order it
// was dispatched in (sliceReading.dispatched), a Slot read whole being one
// slice numbered 0; a slice let go before it completed has a number no
// entry carries.
type sliceLedger struct {
	mu     sync.Mutex
	slices map[int]*sliceLedgerEntry
}

// sliceLedgerEntry is one completed slice's part. completion is its
// completion bindings, of which the Slot keeps the lowest-numbered slice's
// whole and every other slice's that did not complete (through).
type sliceLedgerEntry struct {
	series     map[execution.PlanIdentity]int
	streamed   []execution.NamedInputBinding
	completion []execution.NamedInputBinding
	// landed is the series of each Plan the slice wrote, set as its last
	// window was cut (land); nil before.
	landed map[execution.PlanIdentity]uint64
}

// complete records a slice's part as its completion leaves it: the series it
// bound to each Plan, its streamed bindings - one entry per shared binding -
// and its completion bindings.
func (ledger *sliceLedger) complete(number int, series map[execution.PlanIdentity]int,
	streamed, completion []execution.NamedInputBinding) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.slices == nil {
		ledger.slices = make(map[int]*sliceLedgerEntry)
	}
	ledger.slices[number] = &sliceLedgerEntry{series: series, streamed: streamed, completion: completion}
}

// land records the series of each Plan a completed slice wrote.
func (ledger *sliceLedger) land(number int, landed map[execution.PlanIdentity]uint64) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if entry := ledger.slices[number]; entry != nil {
		entry.landed = landed
	}
}

// numbers is the completed slices' numbers up to through, in order.
func (ledger *sliceLedger) numbers(through int) []int {
	numbers := make([]int, 0, len(ledger.slices))
	for number := range ledger.slices {
		if number <= through {
			numbers = append(numbers, number)
		}
	}
	sort.Ints(numbers)
	return numbers
}

// through is what the completed slices numbered up to number hold, in
// number order: the series each bound to each Plan, summed, and the bindings
// the Slot keeps of them - each slice's streamed bindings, the
// lowest-numbered slice's completion bindings and every other slice's that
// did not complete - sorted by Plan, Level and requirement, a slice's before
// a later one's where those are equal.
//
// A completion's bindings name the same queries every slice, so one slice's
// stand for all; a later slice's that did not complete are kept beside them:
// its query left out series of that slice, which no other slice reads, so
// the Plan's round is as short as its shortest slice for the no-data
// judgement and the completion row, whether or not the slice held any series
// of the Plan.
func (ledger *sliceLedger) through(number int) (map[execution.PlanIdentity]int, []execution.NamedInputBinding) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	series := make(map[execution.PlanIdentity]int)
	var bindings []execution.NamedInputBinding
	for position, slice := range ledger.numbers(number) {
		entry := ledger.slices[slice]
		for plan, count := range entry.series {
			series[plan] += count
		}
		bindings = append(bindings, entry.streamed...)
		for _, binding := range entry.completion {
			if position == 0 || binding.Completeness != execution.CompletenessFull {
				bindings = append(bindings, binding)
			}
		}
	}
	sort.SliceStable(bindings, func(i, j int) bool {
		left, right := bindings[i], bindings[j]
		if left.Consumer.Plan != right.Consumer.Plan {
			return planIdentityLess(left.Consumer.Plan, right.Consumer.Plan)
		}
		if left.Consumer.LevelID != right.Consumer.LevelID {
			return left.Consumer.LevelID < right.Consumer.LevelID
		}
		return left.RequirementID < right.RequirementID
	})
	return series, bindings
}

// landedBefore is the series of each Plan the slices numbered below number
// wrote, and how many of those slices held any of it.
func (ledger *sliceLedger) landedBefore(number int) (series, slices map[execution.PlanIdentity]uint64) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	series, slices = make(map[execution.PlanIdentity]uint64), make(map[execution.PlanIdentity]uint64)
	for _, slice := range ledger.numbers(number - 1) {
		for plan, count := range ledger.slices[slice].landed {
			series[plan] += count
			slices[plan]++
		}
	}
	return series, slices
}
