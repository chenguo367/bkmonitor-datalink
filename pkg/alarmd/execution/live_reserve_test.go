// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import (
	"context"
	"testing"
	"time"
)

func withLiveReserve(reserve time.Duration) *LiveReserve {
	live := &LiveReserve{}
	live.Set(reserve)
	return live
}

// consumer: deadline at 60 s, frozen reserve 5 s, so the frozen query deadline is 55 s.
var liveReserveConsumer = DataRequirementConsumer{ConsumerDeadlineUnixMilli: 60_000, DownstreamExecutionReserveMilliSec: 5_000}

func TestQueryDeadlineUsesTheFrozenReserveUntilATailIsMeasured(t *testing.T) {
	live := withLiveReserve(0)
	deadline, fellBack := live.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000)
	if deadline != 55_000 || fellBack {
		t.Fatalf("got %d fellBack=%v, want the frozen 55000", deadline, fellBack)
	}
}

func TestQueryDeadlineFollowsTheMeasuredTail(t *testing.T) {
	// a tail shorter than today's fixed reserve gives the query more time ...
	live := withLiveReserve(2 * time.Second)
	if deadline, fellBack := live.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); deadline != 58_000 || fellBack {
		t.Fatalf("short tail: got %d fellBack=%v, want 58000", deadline, fellBack)
	}
	// ... and a longer one keeps more of the period for what follows the query
	live.Set(12 * time.Second)
	if deadline, fellBack := live.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); deadline != 48_000 || fellBack {
		t.Fatalf("long tail: got %d fellBack=%v, want 48000", deadline, fellBack)
	}
}

func TestQueryDeadlineFallsBackWhenTheLiveReserveLeavesNoTime(t *testing.T) {
	// a 55 s tail would put the deadline at 5 s, before the query can start at 10 s
	live := withLiveReserve(55 * time.Second)
	deadline, fellBack := live.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000)
	if deadline != 55_000 || !fellBack {
		t.Fatalf("got %d fellBack=%v, want the frozen 55000 and a fallback", deadline, fellBack)
	}
	// exactly at the start is no time either
	live.Set(50 * time.Second)
	if _, fellBack := live.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); !fellBack {
		t.Fatal("a deadline equal to the start must fall back")
	}
}

func TestQueryBudgetFollowsTheTailAndFallsBack(t *testing.T) {
	live := withLiveReserve(0)
	if budget, fellBack := live.QueryBudgetMilli(liveReserveConsumer, 60_000); budget != 55_000 || fellBack {
		t.Fatalf("no tail: got %d fellBack=%v", budget, fellBack)
	}
	live.Set(3 * time.Second)
	if budget, fellBack := live.QueryBudgetMilli(liveReserveConsumer, 60_000); budget != 57_000 || fellBack {
		t.Fatalf("measured tail: got %d fellBack=%v", budget, fellBack)
	}
	live.Set(70 * time.Second)
	if budget, fellBack := live.QueryBudgetMilli(liveReserveConsumer, 60_000); budget != 55_000 || !fellBack {
		t.Fatalf("tail past the interval: got %d fellBack=%v, want the frozen budget", budget, fellBack)
	}
}

// Each runtime owns its reserve: one runtime's tail never reaches another's deadlines, and a runtime with none (nil)
// runs every query under the frozen reserve.
func TestLiveReservesAreIndependentAndNilIsFrozen(t *testing.T) {
	slow, fast := withLiveReserve(20*time.Second), withLiveReserve(time.Second)
	if deadline, _ := slow.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); deadline != 40_000 {
		t.Fatalf("slow runtime: %d", deadline)
	}
	if deadline, _ := fast.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); deadline != 59_000 {
		t.Fatalf("fast runtime: %d", deadline)
	}
	var none *LiveReserve
	if deadline, fellBack := none.QueryDeadlineUnixMilli(liveReserveConsumer, 10_000); deadline != 55_000 || fellBack {
		t.Fatalf("no reserve: %d fellBack=%v", deadline, fellBack)
	}
	none.Set(time.Second)
	none.CountFallback()
	if none.Reserve() != 0 || none.Fallbacks() != 0 {
		t.Fatal("a nil reserve holds nothing")
	}
}

func TestStageMarkerQueryReturnedIsPerExecution(t *testing.T) {
	marker := &StageMarker{}
	marker.Begin(SlotStageSource)
	ctx := WithStageMarker(context.Background(), marker)
	if !marker.QueryReturnedAt().IsZero() {
		t.Fatal("no query returned yet")
	}
	at := time.Unix(100, 0)
	MarkQueryReturned(ctx, at)
	if !marker.QueryReturnedAt().Equal(at) {
		t.Fatalf("got %v", marker.QueryReturnedAt())
	}
	marker.Begin(SlotStageSource)
	if !marker.QueryReturnedAt().IsZero() {
		t.Fatal("the next execution must not inherit the last one's query return")
	}
	MarkQueryReturned(context.Background(), at) // no marker: no effect, no panic
}
