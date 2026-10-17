// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// A held Query Group run through the production Worker - its Slots frozen with the hold, dispatched by its Runner,
// executed by access, the lookback stepped as its loop steps it - reserves each earlier-read trial at the Slot's
// first attempt, not a hold later at its readiness retry.
//
// Before, the lookback judged the group's rest at the moment Prepare ran: a rest that ended between a held Slot's
// first attempt and its readiness retry - most of them, on a hold near the period - left the first attempt to skip
// and the retry to reserve the trial a hold past its own candidate point, overtaken before it read. Here that was
// every trial, prepared a whole hold late.
//
// What this does not show is a trial completing. With the data step equal to the period, as a strategy's
// agg_interval makes it, a hold under two steps lowers to no hold and leaves the trial no time before its point,
// and a longer one has its Slot's first attempt behind the previous Slot's held read, past the point. Those are
// the trial's design, not when it is prepared, and are taken up on their own.
func TestAHeldSlotReservesItsEarlierReadTrialAtItsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	f := startCutoverFixture(t, nil)
	engine := f.bundle.dependencies.Lookback
	if engine == nil {
		t.Fatal("fixture: the bundle runs no lookback")
	}
	holds := f.bundle.dependencies.ReadHolds
	// The group measures its data landing a minute late, as the lookback reports a window read early: its hold
	// rises near the period, and its next Slots are frozen held.
	last := f.progress(ctx).LastCompletion
	if last == nil {
		t.Fatal("fixture: no Slot completed")
	}
	if err := holds.controller.Observe(ctx, readhold.Evidence{Contract: last.Contract, ArrivalAge: time.Minute,
		Rung: lookback.RungNames[0], Buckets: []int64{int64(last.Contract.Slot.EvaluationTime)}}); err != nil {
		t.Fatal(err)
	}
	const step = 5 * time.Second
	var hold time.Duration
	trials := func() (prepared uint64, lateMax float64) {
		for _, source := range engine.Stats().Sources {
			for _, count := range source.EarlierReads {
				prepared += count
			}
			lateMax = max(lateMax, source.EarlierPreparedLateMaxSeconds)
		}
		return prepared, lateMax
	}
	for deadline := f.now().Add(3 * time.Hour); f.now().Before(deadline); f.clock.Store(f.now().Add(step).UnixMilli()) {
		if prepared, _ := trials(); prepared >= 3 {
			break
		}
		if !f.runner.NextReadyAt().After(f.now()) {
			if _, _, err := f.runner.RunOne(ctx); err != nil {
				t.Logf("RunOne at %s: %v", f.now().UTC().Format(time.TimeOnly), err)
			}
		}
		engine.StepEarly(ctx)
		hold = max(hold, holds.controller.ReadHold(f.queryGroup))
	}
	prepared, lateMax := trials()
	if hold < 30*time.Second || prepared == 0 {
		t.Fatalf("fixture: hold %v, %d trials; want a held group whose trials are reserved", hold, prepared)
	}
	// Reserved at the first attempt, a trial is late at most by when the Runner got to the Slot - here within the
	// loop's step - never by the hold.
	if lateMax*float64(time.Second) >= float64(hold/2) {
		t.Fatalf("trials reserved up to %.1f s past their candidate point on a %v hold: reserved at the readiness retry", lateMax, hold)
	}
}
