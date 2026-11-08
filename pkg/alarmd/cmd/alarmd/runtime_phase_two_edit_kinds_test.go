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
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// editedInFlight is strategy 1001 edited while a Slot of its Query Group is
// in flight: the Slot the fixture's first is followed by, begun under the
// initial Segment and stopped in its read, then the edit cut over a second
// after that Slot's time, so the Slot falls in the Segment the cutover
// closes.
type editedInFlight struct {
	*cutoverStallFixture
	inFlight, first     execution.EvaluationTime
	generationBefore    execution.StateGeneration
	generationAfter     execution.StateGeneration
	observedBeforeAfter int
}

func editInFlight(t *testing.T, edit func(first map[string]any)) *editedInFlight {
	t.Helper()
	fixture := startCutoverFixture(t, nil)
	ctx := context.Background()
	edited := &editedInFlight{cutoverStallFixture: fixture, inFlight: execution.EvaluationTime(fixture.base + 60),
		first: execution.EvaluationTime(fixture.base + 120)}
	before, err := fixture.repository.LoadActivation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	edited.generationBefore = activationGenerationOf(t, before.Plans, "1001")

	fixture.clock.Store(firstRoundAtMillis(fixture.cfg, fixture.queryGroup, int64(edited.inFlight), 60))
	roundCtx, stopRound := context.WithCancel(ctx)
	fixture.stopInRead.Store(&stopRound)
	result, attempted, err := fixture.runner.RunOne(roundCtx)
	stopRound()
	if !attempted || result.Completed || !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped Slot RunOne = (%+v, %t, %v), want an attempted, uncompleted Slot stopped in its read", result, attempted, err)
	}

	edited.observedBeforeAfter = len(fixture.observed())
	installEditedStrategies(t, ctx, fixture.redisClient, 1725000600, edit)
	boundary := edited.inFlight + 1
	fixture.clock.Store(int64(boundary) * 1000)
	for round := 0; round < 2; round++ {
		if err := fixture.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatalf("leader round %d: %v", round, err)
		}
	}
	after, err := fixture.repository.LoadActivation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	edited.generationAfter = activationGenerationOf(t, after.Plans, "1001")
	closed, err := fixture.production.dependencies.Catalog.ReadFrozenSchedule(ctx, fixture.queryGroup, edited.inFlight)
	if err != nil || closed.Segment.End == nil || *closed.Segment.End != boundary {
		t.Fatalf("the in-flight Slot's Segment after the edit = (%+v, %v), want it closed at %d: the edit did not cut over",
			closed.Segment, err, boundary)
	}

	fixture.clock.Store((int64(edited.inFlight) + 2) * 1000)
	if result, attempted, err := fixture.runner.RunOne(ctx); err != nil || !attempted || !result.Completed {
		t.Fatalf("the in-flight Slot RunOne = (%+v, %t, %v), want it completed", result, attempted, err)
	}
	fixture.clock.Store((int64(edited.first) + 1) * 1000)
	if result, attempted, err := fixture.runner.RunOne(ctx); err != nil || !attempted || !result.Completed {
		t.Fatalf("the new Segment's first Slot RunOne = (%+v, %t, %v), want it completed", result, attempted, err)
	}
	return edited
}

// planCarry is the carry decided for strategy 1001's Plan at the edit, and
// whether one was decided at all.
func (edited *editedInFlight) planCarry() (string, bool) {
	for _, observation := range edited.observed()[edited.observedBeforeAfter:] {
		if facts := observation.StateCarry; facts != nil && facts.Scope == "plan" {
			return facts.Result, true
		}
	}
	return "", false
}

func thresholdTo(value int) func(first map[string]any) {
	return func(first map[string]any) {
		for _, algorithm := range first["items"].([]any)[0].(map[string]any)["algorithms"].([]any) {
			for _, group := range algorithm.(map[string]any)["config"].([]any) {
				for _, condition := range group.([]any) {
					condition.(map[string]any)["threshold"] = value
				}
			}
		}
	}
}

// Every kind of edit that keeps the strategy's Query Group loses no Slot
// (design 013 section 5): the Slot in flight at the edit completes under the
// configuration it was frozen with, and the new Segment's first Slot is
// evaluated - not spent on a warm-up marker - whether the edit moved the
// state generation and carried the state (carried, or carried with the
// detection changed), or touched only what the Query Group's object holds.
func TestEveryEditKindThatKeepsTheQueryGroupLosesNoSlot(t *testing.T) {
	for _, kind := range []struct {
		name string
		edit func(first map[string]any)
		// carry is the Plan's carry outcome at the edit; empty for an edit
		// that leaves the state generation, which decides no carry.
		carry string
		// warms is an edit that lengthens a window past the points the
		// series holds: the series warms up on its own window - the first
		// Slot is evaluated and reads HISTORY_WARMING - and the Slot after
		// it is FULL. Not a warm-up marker: no Slot is spent.
		warms bool
	}{
		{name: "threshold", edit: thresholdTo(81), carry: "carried_detect_changed"},
		{name: "algorithm method", edit: func(first map[string]any) {
			algorithm := first["items"].([]any)[0].(map[string]any)["algorithms"].([]any)[0].(map[string]any)
			algorithm["config"].([]any)[0].([]any)[0].(map[string]any)["method"] = "gt"
		}, carry: "carried_detect_changed"},
		{name: "trigger window", edit: func(first map[string]any) {
			for _, detect := range first["detects"].([]any) {
				detect.(map[string]any)["trigger_config"].(map[string]any)["check_window"] = cutoverStallTriggerWindow + 1
			}
		}, carry: "carried"},
		{name: "recovery window", edit: func(first map[string]any) {
			for _, detect := range first["detects"].([]any) {
				detect.(map[string]any)["recovery_config"].(map[string]any)["check_window"] = 3
			}
		}, carry: "carried", warms: true},
		{name: "level added", edit: func(first map[string]any) {
			item := first["items"].([]any)[0].(map[string]any)
			item["algorithms"] = append(item["algorithms"].([]any), map[string]any{"level": 2, "type": "Threshold", "unit_prefix": "",
				"config": []any{[]any{map[string]any{"method": "gte", "threshold": 90}}}})
			first["detects"] = append(first["detects"].([]any), map[string]any{"level": 2, "connector": "and",
				"trigger_config": map[string]any{"count": 1, "check_window": cutoverStallTriggerWindow},
				"recovery_config": map[string]any{"check_window": 1}})
		}, carry: "carried"},
		{name: "no-data added", edit: func(first map[string]any) {
			first["items"].([]any)[0].(map[string]any)["no_data_config"] = map[string]any{"is_enabled": true, "continuous": 5, "level": 2,
				"agg_dimension": []any{"host"}}
		}},
		{name: "effective time", edit: func(first map[string]any) {
			for _, detect := range first["detects"].([]any) {
				detect.(map[string]any)["trigger_config"].(map[string]any)["uptime"] = map[string]any{
					"time_ranges": []any{map[string]any{"start": "00:00", "end": "23:59"}}, "calendars": []any{}}
			}
		}},
	} {
		t.Run(kind.name, func(t *testing.T) {
			edited := editInFlight(t, kind.edit)
			if moved := edited.generationAfter != edited.generationBefore; moved != (kind.carry != "") {
				t.Fatalf("the %s edit moved the state generation %t (%s -> %s), want %t", kind.name, moved,
					edited.generationBefore, edited.generationAfter, kind.carry != "")
			}
			if carry, decided := edited.planCarry(); carry != kind.carry || decided != (kind.carry != "") {
				t.Fatalf("the %s edit decided carry %q (decided %t), want %q", kind.name, carry, decided, kind.carry)
			}
			endings := edited.slotEndings()
			if ended := endings[edited.inFlight]; ended != fullEnding {
				t.Fatalf("the Slot in flight at the %s edit ended %q, want FULL: it was thrown away; endings %v", kind.name, ended, endings)
			}
			want := fullEnding
			if kind.warms {
				want = historyWarmingEnding
			}
			if ended := endings[edited.first]; ended != want {
				t.Fatalf("the new Segment's first Slot after the %s edit ended %q, want %q; endings %v", kind.name, ended, want, endings)
			}
			if kind.warms {
				next := edited.first + 60
				edited.clock.Store((int64(next) + 1) * 1000)
				if result, attempted, err := edited.runner.RunOne(context.Background()); err != nil || !attempted || !result.Completed {
					t.Fatalf("the Slot after RunOne = (%+v, %t, %v), want it completed", result, attempted, err)
				}
				if ended := edited.slotEndings()[next]; ended != fullEnding {
					t.Fatalf("the Slot after the warming one ended %q, want FULL on the window it filled", ended)
				}
			}
		})
	}
}

// A series the edit's carry finds no previous record for starts from empty
// on its own window (old_missing) beside the series that carry; that is no
// reason to warm the Plan up, and the new Segment's first Slot is evaluated.
func TestASeriesWithNoPreviousRecordStartsOnItsOwnWindow(t *testing.T) {
	fixture := startCutoverFixture(t, nil)
	ctx := context.Background()
	first := execution.EvaluationTime(fixture.base + 60)
	fixture.uqHosts = func(end int64) []string {
		if end >= int64(first) {
			return []string{"127.0.0.1", "127.0.0.2"}
		}
		return []string{"127.0.0.1"}
	}
	observed := len(fixture.observed())
	installEditedStrategies(t, ctx, fixture.redisClient, 1725000600, thresholdTo(81))
	fixture.clock.Store((fixture.base + 59) * 1000)
	for round := 0; round < 2; round++ {
		if err := fixture.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatalf("leader round %d: %v", round, err)
		}
	}
	fixture.clock.Store(firstRoundAtMillis(fixture.cfg, fixture.queryGroup, int64(first), 60))
	if result, attempted, err := fixture.runner.RunOne(ctx); err != nil || !attempted || !result.Completed {
		t.Fatalf("the new Segment's first Slot RunOne = (%+v, %t, %v), want it completed", result, attempted, err)
	}
	series := map[string]int{}
	for _, observation := range fixture.observed()[observed:] {
		if facts := observation.StateCarry; facts != nil && facts.Scope == "series" {
			series[facts.Result] += facts.Count
		}
	}
	if series["carried_across_detect"] != 1 || series["old_missing"] != 1 {
		t.Fatalf("series carried %v, want the series that ran before carried and the new one old_missing", series)
	}
	if ended := fixture.slotEndings()[first]; ended != fullEnding {
		t.Fatalf("the new Segment's first Slot ended %q, want it evaluated (FULL)", ended)
	}
}
