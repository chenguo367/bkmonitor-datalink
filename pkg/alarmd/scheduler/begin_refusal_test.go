// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.
package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// An attempt the owner fence refused at its first write is the round's
// ownership rejection, as the Runner's own check before dispatch reported
// it: run_one says ownership_rejected, the attempt does not count as
// executed, and nothing backs off -- the next round reads ownership again.
// An execution that ran and failed is still an execution_returned with its
// backoff.
func TestAnAttemptRefusedAtItsBeginIsAnOwnershipRejection(t *testing.T) {
	for _, refusal := range []error{ownership.ErrStaleFence, ownership.ErrContentScopeMoved} {
		clock := newMutableClock(time.Unix(200, 0))
		executor := &failingExecutor{err: &execution.SlotBeginRefusedError{Status: execution.ProgressStaleOwner, Err: refusal}, failures: 1}
		runner := newBackoffTestRunner(t, clock, executor)
		observed := []observability.Observation{}
		runner.flights.observer = observability.ObserverFunc(func(_ context.Context, o observability.Observation) {
			if o.Stage == observability.StageRunnerReturned {
				observed = append(observed, o)
			}
		})
		_, attempted, err := runner.RunOne(context.Background())
		if attempted || !errors.Is(err, refusal) {
			t.Fatalf("RunOne() attempted=%v err=%v, want an unattempted round carrying %v", attempted, err, refusal)
		}
		if len(observed) != 1 || observed[0].RunOutcome != "ownership_rejected" {
			t.Fatalf("run_one lines %+v, want one ownership_rejected", observed)
		}
		if !runner.NextReadyAt().IsZero() {
			t.Fatalf("NextReadyAt() = %v, want no backoff after an ownership rejection", runner.NextReadyAt())
		}
	}
}
