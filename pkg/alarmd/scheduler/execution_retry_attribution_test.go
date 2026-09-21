// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"context"
	"errors"
	"testing"
)

// A failed state read schedules a retry of the Slot - unless the attempt spent
// so long failing that the Slot's own deadline went first.
//
// This is the pair the retry stacking turned on. With the client taking one
// attempt, a 3 s read timeout returns while the completion context is still
// live, the error reaches here as an ordinary failure, and the Slot is parked
// for a retry that re-reads current state. With the client's default three
// retries the same failure takes 12 s, which on a short-period Plan - a 10 s or
// 15 s interval with a 30 s completion offset - outlives the deadline, and an
// expired context is the first thing this refuses. Nothing records the attempt
// and nothing retries the Slot.
//
// Both halves are stated because either alone is satisfied by the wrong rule:
// always backing off would lose cancellation, never backing off would lose
// every retry.
func TestAFailedStateReadBacksOffUnlessItOutlivedTheSlot(t *testing.T) {
	readTimedOut := errors.New("alarmd worker: series state preflight: state: Redis MGET: i/o timeout")

	if !executionErrorBacksOff(context.Background(), readTimedOut) {
		t.Fatal("a state read that failed inside the Slot's deadline recorded no attempt, so the Slot is " +
			"never retried - the read is retryable and the next attempt re-reads current state")
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if executionErrorBacksOff(expired, readTimedOut) {
		t.Fatal("the same failure backed off after the Slot's context had already gone; a cancelled Slot " +
			"is not a failed attempt of it")
	}
	if executionErrorBacksOff(context.Background(), context.Canceled) {
		t.Fatal("cancellation counted as a failed attempt of the frozen Slot")
	}
	if executionErrorBacksOff(context.Background(), nil) {
		t.Fatal("a Slot that returned no error counted as a failed attempt")
	}
}
