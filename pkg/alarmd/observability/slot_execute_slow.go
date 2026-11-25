// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"context"
	"time"
)

// A Slot attempt whose execution ran long is said on a line of its own,
// beside its slot_completed, which it does not change: which Query Group, its
// round and operation, how long against how long it was allowed, and where
// the time went - the Slot's phases, its queries' waits and bodies, its input
// slices and their seams, and the waits before it ran and for its queries'
// permits. Execution taking long is no failure and nothing reports one: the
// count of them is all a reading had, and it named no Query Group.

// StageSlotExecuteSlow is a Slot attempt whose execution took
// SlowSlotExecution of its period or longer (SlotExecuteSlowFacts).
const StageSlotExecuteSlow = "slot_execute_slow"

// ReasonSlotExecuteSlow is a slot_execute_slow line's reason.
const ReasonSlotExecuteSlow ReasonCode = "SLOT_EXECUTE_SLOW"

// SlowSlotExecutionFloor and SlowSlotExecutionCeiling bound the execution
// a Slot attempt is allowed before it is said to be slow (SlowSlotExecution).
const (
	SlowSlotExecutionFloor   = 5 * time.Second
	SlowSlotExecutionCeiling = 30 * time.Second
)

// SlowSlotExecution is how long a Slot attempt of a Query Group of this
// period may execute before it is slow: half the period, no less than the
// floor and no more than the ceiling; the ceiling for a period not known - a
// Slot no dispatch started, or a Query Group before its first round.
func SlowSlotExecution(period time.Duration) time.Duration {
	if period <= 0 {
		return SlowSlotExecutionCeiling
	}
	return min(max(period/2, SlowSlotExecutionFloor), SlowSlotExecutionCeiling)
}

// SlotExecuteSlowFacts is where a slow Slot attempt's time went. The period
// is the Query Group's shortest due interval as its dispatch knew it, zero
// when it did not; the threshold what that allowed. The phases are the
// Slot's (as on slot_completed, SlotTimingFacts). The query waits and bodies,
// summed and the longest, and the input slices and the seams between them
// are its budget usage's. ExecutionSlotWaitMillis is what its dispatch
// waited for an execution slot before it started; the permit waits are its
// queries' waits for a query permit or the recovery channels, summed and the
// longest - parallel, so the sum is no share of the attempt's time.
type SlotExecuteSlowFacts struct {
	PeriodSeconds   int64
	ThresholdMillis int64
	Timing          SlotTimingFacts

	QueryWaitMillis, QueryBodyMillis       uint64
	QueryWaitMillisMax, QueryBodyMillisMax uint64
	QueryBodyMicros, QueryBodyMicrosMax    uint64
	InputSlices                            uint64
	InputSliceTailMillis                   uint64
	InputSliceTailMillisMax                uint64

	ExecutionSlotWaitMillis                         float64
	QueryPermitWaitMillis, QueryPermitWaitMillisMax int64
	// Usage is the attempt's budget usage, written on the line as a
	// completion row writes it (slot_budget_usage): its series, bytes,
	// slices' seams and reads ahead, the reading a slow Slot that completed
	// has no completion row for (015 O1). Readers of the usage take a
	// completed Slot's alone; this line is not one.
	Usage *SlotBudgetUsageFacts
}

// SlotExecuteSlowOperations is every operation slow attempts are counted
// under (slot_execute_slow_total): the Slot operations, and other.
var SlotExecuteSlowOperations = ExecutionSlotWaitOperations

// ObserveSlotExecution says a Slot attempt that executed for elapsed, when
// that is its threshold or longer (SlowSlotExecution of facts' period): a
// slot_execute_slow line, degraded with SLOT_EXECUTE_SLOW, carrying facts
// with the threshold filled in. It reports whether it said one. The line's
// hourly sample is its Query Group's (the limiter); the count is every one.
func ObserveSlotExecution(ctx context.Context, observer Observer, operation Operation, trace TraceFields,
	elapsed time.Duration, facts SlotExecuteSlowFacts) bool {
	threshold := SlowSlotExecution(time.Duration(facts.PeriodSeconds) * time.Second)
	if observer == nil || elapsed < threshold {
		return false
	}
	facts.ThresholdMillis = threshold.Milliseconds()
	// Observability is a fail-open side channel, as at every other boundary.
	defer func() { _ = recover() }()
	observer.Observe(ctx, Observation{
		Component: ComponentScheduler, Stage: StageSlotExecuteSlow, Result: Result(ResultDegraded), Operation: operation,
		ReasonCode: ReasonSlotExecuteSlow, Trace: trace, Duration: elapsed, DurationKnown: true, SlotExecuteSlow: &facts,
		SlotBudgetUsage: facts.Usage,
	})
	return true
}
