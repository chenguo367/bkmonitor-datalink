// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func calibrationState(tracked int) *indexState {
	state := &indexState{options: IndexOptions{ReconcileBatch: 1, RefreshInterval: time.Second, ReconcileInterval: 30 * time.Minute, CycleTimeout: 5 * time.Second}}
	for index := 0; index < tracked; index++ {
		state.order = append(state.order, StrategyKey{TenantID: "t", StrategyID: fmt.Sprint(index)})
	}
	return state
}

// A round calibrates enough strategies to cover every tracked one within the
// calibration interval, with a quarter to spare: 1440 strategies over 1800
// one-second rounds need one a round, 1441 need two, 3600 need three. Few
// strategies keep the floor of one.
func TestARoundCalibratesWhatTheIntervalNeeds(t *testing.T) {
	for _, tc := range []struct{ tracked, needed int }{{0, 1}, {300, 1}, {1440, 1}, {1441, 2}, {3600, 3}} {
		state := calibrationState(tc.tracked)
		state.noteCalibration(10 * time.Millisecond)
		if needed, _ := state.calibrationPlan(tc.tracked); needed != tc.needed {
			t.Errorf("%d tracked: %d a round, want %d", tc.tracked, needed, tc.needed)
		}
	}
}

// The Console bounds a round: at a p90 of one second, a round's 2.5 s holds
// two calls, so 4000 tracked strategies, which need three a round, get two,
// and the 400 a whole interval at two cannot reach are named as short.
// Before any call is timed the round keeps its floor.
func TestTheConsoleBoundsARoundAndTheShortfallIsSaid(t *testing.T) {
	state := calibrationState(4000)
	if needed, capacity := state.calibrationPlan(4000); needed != 3 || capacity != 1 {
		t.Fatalf("untimed: needed %d capacity %d, want 3 and the floor 1", needed, capacity)
	}
	for index := 0; index < 20; index++ {
		state.noteCalibration(time.Second)
	}
	needed, capacity := state.calibrationPlan(4000)
	if needed != 3 || capacity != 2 {
		t.Fatalf("needed %d capacity %d, want 3 and 2", needed, capacity)
	}
	if short := max(0, 4000-capacity*state.calibrationRounds()); short != 400 {
		t.Fatalf("short %d, want 400", short)
	}
	// A faster Console lifts the bound: 64 calls of 100 ms replace them.
	for index := 0; index < calibrationSamples; index++ {
		state.noteCalibration(100 * time.Millisecond)
	}
	if _, capacity := state.calibrationPlan(4000); capacity != 25 {
		t.Fatalf("capacity %d at 100 ms, want 25", capacity)
	}
}

// The copy's stats say what a round needs and can do, and name the
// strategies past CalibrationMaxAge, oldest first: one calibrated four hours
// ago is named, one calibrated now is not.
func TestStatsNameTheStrategiesPastTheirCalibrationAge(t *testing.T) {
	c := &clock{at: time.Unix(1700000000, 0)}
	options := indexOptions(c)
	calls := 0
	options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
		calls++
		return Reconciliation{}, nil
	})
	cache := mustIndex(t, options)
	old, fresh := StrategyKey{TenantID: "t", StrategyID: "1"}, StrategyKey{TenantID: "t", StrategyID: "2"}
	if err := cache.SetTracked([]StrategyKey{old}); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())
	c.at = c.at.Add(4 * time.Hour)
	if err := cache.SetTracked([]StrategyKey{old, fresh}); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	cache.index.entries[old].lastReconcileAttempt = c.at
	cache.mu.Unlock()
	cache.Refresh(context.Background())
	stats := cache.Stats()
	if stats.CalibrationNeeded < 1 || stats.CalibrationCapacity < 1 || len(stats.CalibrationOverAge) != 1 ||
		stats.CalibrationOverAge[0].Strategy != old || stats.CalibrationOverAge[0].Age != 4*time.Hour {
		t.Fatalf("stats needed %d capacity %d over-age %+v, want strategy 1 at 4 h", stats.CalibrationNeeded, stats.CalibrationCapacity, stats.CalibrationOverAge)
	}
}

// A calibration call is timed as it runs: one that takes a second of the
// copy's clock, in a 10 s cycle, sets the round's capacity to five calls.
func TestACalibrationIsTimedAsItRuns(t *testing.T) {
	c := &clock{at: time.Unix(1700000000, 0)}
	options := indexOptions(c)
	options.CycleTimeout, options.ReconcileBatch = 10*time.Second, 1
	options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
		c.at = c.at.Add(time.Second)
		return Reconciliation{}, nil
	})
	cache := mustIndex(t, options)
	if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())
	if capacity := cache.Stats().CalibrationCapacity; capacity != 5 {
		t.Fatalf("capacity %d after a one-second call, want 5", capacity)
	}
}

// A refresh round calibrates the planned number: 16 tracked strategies over
// an interval of four rounds need five a round once a call has been timed;
// the first round, before any, keeps the floor.
func TestARoundCalibratesThePlannedNumber(t *testing.T) {
	c := &clock{at: time.Unix(1700000000, 0)}
	options := indexOptions(c)
	options.ReconcileBatch, options.RefreshInterval, options.ReconcileInterval = 1, time.Second, 4*time.Second
	options.LocalRetention, options.CycleTimeout = time.Second, 10*time.Second
	calls := 0
	options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
		calls++
		c.at = c.at.Add(10 * time.Millisecond)
		return Reconciliation{}, nil
	})
	cache := mustIndex(t, options)
	keys := make([]StrategyKey, 16)
	for index := range keys {
		keys[index] = StrategyKey{TenantID: "t", StrategyID: fmt.Sprint(index)}
	}
	if err := cache.SetTracked(keys); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())
	if calls != 1 {
		t.Fatalf("first round calibrated %d, want the floor 1", calls)
	}
	c.at = c.at.Add(2 * time.Second)
	calls = 0
	cache.Refresh(context.Background())
	if calls != 5 {
		t.Fatalf("second round calibrated %d, want 5", calls)
	}
}
