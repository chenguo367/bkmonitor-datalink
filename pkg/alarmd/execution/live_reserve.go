// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import (
	"sync/atomic"
	"time"
)

// LiveReserve is one worker runtime's downstream execution reserve: the time a
// Slot keeps after its query for everything that follows it - state reads and
// writes, evaluation, output to Kafka.
//
// The control plane freezes one value into every plan
// (DataRequirementConsumer.DownstreamExecutionReserveMilliSec). That value
// stays the plan's validity basis, the Slot's own deadlines and recovery
// window, and the capability verdict's basis, so plans, handover and the
// supported-interval check do not move with load. The worker sizes only the
// deadline its queries run under from what the reserve has to cover in this
// runtime: the measured post-query tail, from a query's return to its Slot's
// return, which the runtime's scheduler shaper publishes here. Until the first
// measurement there is none and the frozen value - today's fixed reserve - is
// used, as it always was.
//
// It belongs to the runtime that measures it, not to the process: two runtimes
// in one process (tests, a split deployment) each size from their own tail. A
// nil LiveReserve is a runtime with none, and every method answers with the
// frozen reserve.
type LiveReserve struct {
	reserveMilli atomic.Int64
	// fallbacks counts query deadlines that fell back to the frozen reserve
	// because the live one left the query no time.
	fallbacks atomic.Uint64
}

// Set publishes the runtime's measured post-query tail. Zero or negative
// clears it, so the frozen reserve applies again.
func (live *LiveReserve) Set(reserve time.Duration) {
	if live != nil {
		live.reserveMilli.Store(max(reserve.Milliseconds(), 0))
	}
}

// Reserve is the published post-query tail; zero before any.
func (live *LiveReserve) Reserve() time.Duration {
	if live == nil {
		return 0
	}
	return time.Duration(live.reserveMilli.Load()) * time.Millisecond
}

// Fallbacks is how many query deadlines fell back to the frozen reserve.
func (live *LiveReserve) Fallbacks() uint64 {
	if live == nil {
		return 0
	}
	return live.fallbacks.Load()
}

// CountFallback counts a query that ran under the frozen reserve because the
// live one left it no time.
func (live *LiveReserve) CountFallback() {
	if live != nil {
		live.fallbacks.Add(1)
	}
}

// QueryDeadlineUnixMilli is the consumer's query deadline in this runtime: the
// consumer deadline less the live reserve when one is measured, less the
// frozen reserve otherwise.
//
// notBeforeUnixMilli is the earliest the query can run (its readiness, or now
// when it is about to start). A live reserve that would put the deadline at or
// before it - a slow tail hour on a short interval - would turn every query of
// that Slot into an instant failure. In that case the frozen reserve is used
// for this Slot and fellBack is true; the query paths count it
// (CountFallback). A spike in the tail therefore never stops detection.
func (live *LiveReserve) QueryDeadlineUnixMilli(consumer DataRequirementConsumer, notBeforeUnixMilli int64) (deadline int64, fellBack bool) {
	frozen := consumer.ConsumerDeadlineUnixMilli - consumer.DownstreamExecutionReserveMilliSec
	reserve := live.Reserve().Milliseconds()
	if reserve <= 0 {
		return frozen, false
	}
	if candidate := consumer.ConsumerDeadlineUnixMilli - reserve; candidate > notBeforeUnixMilli {
		return candidate, false
	}
	return frozen, true
}

// QueryBudgetMilli is the time a recovery query of a consumer with this
// evaluation interval may use: the interval less the live reserve when one is
// measured and it leaves a positive budget, less the frozen reserve otherwise.
func (live *LiveReserve) QueryBudgetMilli(consumer DataRequirementConsumer, intervalMillis int64) (budget int64, fellBack bool) {
	frozen := intervalMillis - consumer.DownstreamExecutionReserveMilliSec
	reserve := live.Reserve().Milliseconds()
	if reserve <= 0 {
		return frozen, false
	}
	if candidate := intervalMillis - reserve; candidate > 0 {
		return candidate, false
	}
	return frozen, true
}
