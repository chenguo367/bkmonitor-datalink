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
	// letGone is the numbers of the slices let go before they completed
	// (letGo); changed is closed, and replaced, as a slice completes or is
	// let go, for those waiting on the slices below theirs (waitBelow).
	letGone map[int]struct{}
	changed chan struct{}
	// headed is the numbers of the slices whose head was handed to the apply
	// side, or that write none - the Slot's first (noteHeaded).
	headed map[int]struct{}
}

// noteHeaded records that the slice numbered number handed its head, or
// has none to hand.
func (ledger *sliceLedger) noteHeaded(number int) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.headed == nil {
		ledger.headed = make(map[int]struct{})
	}
	ledger.headed[number] = struct{}{}
	ledger.changedLocked()
}

// waitHeadsBelow waits until every slice numbered below number handed its
// head, had none, or was let go: the apply side takes the tasks in the order
// they are handed, so heads handed in slice order are written in it.
func (ledger *sliceLedger) waitHeadsBelow(ctx context.Context, number int) error {
	return ledger.waitFor(ctx, func() bool {
		for below := range number {
			_, headed := ledger.headed[below]
			_, gone := ledger.letGone[below]
			if !headed && !gone {
				return false
			}
		}
		return true
	})
}

// waitFor waits until done, read under mu, holds.
func (ledger *sliceLedger) waitFor(ctx context.Context, done func() bool) error {
	for {
		ledger.mu.Lock()
		if done() {
			ledger.mu.Unlock()
			return nil
		}
		if ledger.changed == nil {
			ledger.changed = make(chan struct{})
		}
		changed := ledger.changed
		ledger.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// changedLocked wakes every waitBelow. Under mu.
func (ledger *sliceLedger) changedLocked() {
	if ledger.changed != nil {
		close(ledger.changed)
		ledger.changed = nil
	}
}

// letGo records that the slice numbered number was let go before it
// completed - split, or its dimensions forgotten - so no slice above it
// waits for it.
func (ledger *sliceLedger) letGo(number int) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.letGone == nil {
		ledger.letGone = make(map[int]struct{})
	}
	ledger.letGone[number] = struct{}{}
	ledger.changedLocked()
}

// waitBelow waits until every slice numbered below number has completed or
// been let go, and says whether none of them completed: the slice is then
// the Slot's first. With k consumers a slice's head and what it reads so far
// are said by number (015 C1 step 3.2): read one at a time, every slice below
// is done by the time one completes, and it does not wait.
func (ledger *sliceLedger) waitBelow(ctx context.Context, number int) (bool, error) {
	for {
		ledger.mu.Lock()
		done, first := true, true
		for below := range number {
			if _, completed := ledger.slices[below]; completed {
				first = false
			} else if _, gone := ledger.letGone[below]; !gone {
				done = false
				break
			}
		}
		if done {
			ledger.mu.Unlock()
			return first, nil
		}
		if ledger.changed == nil {
			ledger.changed = make(chan struct{})
		}
		changed := ledger.changed
		ledger.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// sliceLedgerEntry is one completed slice's part. completion is its
// completion bindings, of which the Slot keeps the lowest-numbered slice's
// whole and every other slice's that did not complete (through).
type sliceLedgerEntry struct {
	series     map[execution.PlanIdentity]int
	streamed   []execution.NamedInputBinding
	completion []execution.NamedInputBinding
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
	ledger.changedLocked()
}

// last is the highest number a slice completed under, -1 with none.
func (ledger *sliceLedger) last() int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	last := -1
	for number := range ledger.slices {
		last = max(last, number)
	}
	return last
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
