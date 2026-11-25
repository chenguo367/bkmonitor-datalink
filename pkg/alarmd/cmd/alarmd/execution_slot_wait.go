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
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// dispatchSlotWait is what one dispatch waited for an execution slot, handed
// from the dispatcher to the Slot it starts. The dispatcher knows the wait
// and the Slot knows its operation, so the wait rides the dispatch's context
// to where the Slot starts and is counted there. The first Slot to start
// takes it; a later Slot of the same round ran on the slot the dispatch
// already held and did not wait.
type dispatchSlotWait struct {
	wait   time.Duration
	cohort string
	// interval is the Query Group's shortest due interval, in seconds, its
	// queue place was given under: the period its Slot's execution is
	// judged slow against (observability.SlowSlotExecution).
	interval int64
	taken    atomic.Bool
}

type dispatchSlotWaitKey struct{}

// withDispatchSlotWait is ctx carrying one dispatch's wait for an execution
// slot, and the cohort and interval its queue place was given under.
func withDispatchSlotWait(ctx context.Context, wait time.Duration, cohort string, interval int64) context.Context {
	return context.WithValue(ctx, dispatchSlotWaitKey{}, &dispatchSlotWait{wait: max(wait, 0), cohort: cohort,
		interval: max(interval, 0)})
}

// dispatchPeriod is the period, in seconds, of the Query Group the dispatch
// under ctx started: its shortest due interval when queued; zero for a Slot
// no dispatch started, or a Query Group before its first round.
func dispatchPeriod(ctx context.Context) int64 {
	carried, _ := ctx.Value(dispatchSlotWaitKey{}).(*dispatchSlotWait)
	if carried == nil {
		return 0
	}
	return carried.interval
}

// takeDispatchSlotWait is the wait for the Slot starting under ctx: the
// dispatch's for the first Slot, zero for any after it, nil for a Slot no
// dispatch started.
func takeDispatchSlotWait(ctx context.Context) *observability.ExecutionSlotWaitFacts {
	carried, _ := ctx.Value(dispatchSlotWaitKey{}).(*dispatchSlotWait)
	if carried == nil {
		return nil
	}
	facts := &observability.ExecutionSlotWaitFacts{Cohort: carried.cohort}
	if carried.taken.CompareAndSwap(false, true) {
		facts.WaitMillis = float64(carried.wait) / float64(time.Millisecond)
	}
	return facts
}
