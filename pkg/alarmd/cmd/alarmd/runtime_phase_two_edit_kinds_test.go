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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
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
	return editInFlightThen(t, edit, nil)
}

// editInFlightThen is editInFlight with between run after the cutover and
// before the Slot in flight resumes.
func editInFlightThen(t *testing.T, edit func(first map[string]any), between func(*cutoverStallFixture)) *editedInFlight {
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

	if between != nil {
		between(fixture)
	}
	fixture.clock.Store((int64(edited.inFlight) + 2) * 1000)
	edited.runUntilEnded(edited.inFlight)
	fixture.clock.Store(max(fixture.clock.Load(), (int64(edited.first)+1)*1000))
	edited.runUntilEnded(edited.first)
	return edited
}

// runUntilEnded runs the Query Group's rounds, each at its next ready
// instant, until slot has ended: a round that retries is followed by the
// next.
func (edited *editedInFlight) runUntilEnded(slot execution.EvaluationTime) {
	edited.t.Helper()
	for attempt := 0; attempt < 30; attempt++ {
		if _, ended := edited.slotEndings()[slot]; ended {
			return
		}
		if nextAt := edited.runner.NextReadyAt(); nextAt.After(edited.now()) {
			edited.clock.Store(nextAt.UnixMilli() + 1)
		}
		if _, attempted, err := edited.runner.RunOne(context.Background()); err != nil {
			edited.t.Fatalf("Slot %d RunOne: %v", slot, err)
		} else if !attempted {
			edited.clock.Store(edited.now().Add(time.Second).UnixMilli())
		}
	}
	edited.t.Fatalf("Slot %d did not end; endings %v", slot, edited.slotEndings())
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

// countTwoOfThree fires strategy 1001 on every Slot and alerts on the
// second anomaly within three: one stored anomaly decides whether a Slot's
// own makes an alert.
func countTwoOfThree(first map[string]any) {
	thresholdTo(1)(first)
	for _, detect := range first["detects"].([]any) {
		trigger := detect.(map[string]any)["trigger_config"].(map[string]any)
		trigger["count"], trigger["check_window"] = 2, 3
	}
}

// A query edit moves the Plan to a new Query Group and leaves its state
// generation (design 013 section 5): the state is keyed without the Query
// Group, as Python's detection results are, so the new group's first Slot
// reads the state the old one wrote, is not warmed up, and alerts on the
// anomaly the old group stored with its own - one anomaly of its own alone
// would not make the second of two.
func TestAQueryEditReadsTheStateTheOldGroupWrote(t *testing.T) {
	fixture := startCutoverFixtureWith(t, nil, observability.Discard(observability.ComponentRuntime), func(ctx context.Context, client *redis.Client, _ config.Config) {
		installEditedStrategies(t, ctx, client, 1725000000, countTwoOfThree)
	})
	ctx := context.Background()
	if events := fixture.eventsAt(execution.EvaluationTime(fixture.base)); len(events) != 0 {
		t.Fatalf("one anomaly made an alert: %+v", events)
	}
	before, err := fixture.repository.LoadActivation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	installEditedStrategies(t, ctx, fixture.redisClient, 1725000600, func(first map[string]any) {
		countTwoOfThree(first)
		first["items"].([]any)[0].(map[string]any)["query_configs"].([]any)[0].(map[string]any)["agg_method"] = "max"
	})
	old := fixture.queryGroup
	fixture.clock.Store((fixture.base + 59) * 1000)
	for round := 0; round < 2; round++ {
		if err := fixture.bundle.refreshAndReconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	after, err := fixture.repository.LoadActivation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if activationGenerationOf(t, after.Plans, "1001") != activationGenerationOf(t, before.Plans, "1001") {
		t.Fatal("the query edit moved the state generation; the case needs one that leaves it")
	}
	for _, candidate := range fixture.bundle.queryGroups {
		schedule, err := fixture.production.dependencies.Catalog.ReadInitialFrozenSchedule(ctx, candidate)
		if err == nil && candidate != old && len(schedule.Plans) == 1 && schedule.Plans[0].Identity.StrategyID == "1001" {
			fixture.queryGroup = candidate
		}
	}
	if fixture.queryGroup == old {
		t.Fatal("the query edit did not move the Plan to a new Query Group")
	}
	fixture.runner = settledRunner(fixture.bundle, fixture.queryGroup)
	first := execution.EvaluationTime(fixture.base + 60)
	if ended := runSlot(t, fixture, fixture.queryGroup, fixture.runner, first); ended != fullEnding {
		t.Fatalf("the new group's first Slot ended %q, want FULL: not warmed up", ended)
	}
	if events := fixture.eventsAt(first); len(events) == 0 {
		t.Fatal("the new group's first Slot made no alert: it did not read the anomaly the old group stored")
	}
}

// queryFreeAcrossAnEdit is the Slot in flight at a threshold edit ending
// query-free: its snapshot is gone, as content past its retention is, so the
// Slot is skipped. It returns the observations from that Slot on.
func queryFreeAcrossAnEdit(t *testing.T) (*editedInFlight, int) {
	t.Helper()
	var observed int
	edited := editInFlightThen(t, thresholdTo(81), func(fixture *cutoverStallFixture) {
		expireOldContent(t, fixture, fixture.initialSchedule)
		if err := fixture.repository.ConfigureObjectCache(1, 1); err != nil {
			t.Fatal(err)
		}
		observed = len(fixture.observed())
	})
	// Query-free either way the snapshot's loss is judged: skipped, or
	// completed without its read.
	if ended := edited.slotEndings()[edited.inFlight]; ended != "GAP_SKIPPED/GAP_SKIPPED" &&
		ended != "COMPLETED_WITH_UNAVAILABLE/SNAPSHOT_UNAVAILABLE" {
		t.Fatalf("the Slot in flight ended %q, want it query-free for its snapshot", ended)
	}
	return edited, observed
}

// The Slot in flight at an edit, ending query-free, guards the generation it
// ran under (design 013 section 3.2): its Segment answers the Plan it ran,
// so its completion writes the Plan's gap marker on that generation, GAPPED
// from that Slot. Before, the Segment answered no Plan, and the old
// generation's window had the hole with no marker over it.
func TestASlotEndingQueryFreeAcrossAnEditGuardsTheGenerationItRanUnder(t *testing.T) {
	edited, _ := queryFreeAcrossAnEdit(t)
	plan := edited.initialSchedule.Plans[0].Identity
	key, err := state.PlanGapKeyV2(edited.cfg.Redis.StatePrefix, execution.PlanGapIdentity{Plan: plan, StateGeneration: edited.generationBefore})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := edited.redisClient.Get(context.Background(), key).Bytes()
	if err != nil {
		t.Fatalf("the generation the Slot ran under has no gap marker: %v", err)
	}
	var marker struct {
		Scopes []struct {
			Status           string
			ReasonOriginSlot int64
		} `json:"scopes"`
	}
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	if len(marker.Scopes) == 0 || marker.Scopes[0].Status != "GAPPED" || marker.Scopes[0].ReasonOriginSlot != int64(edited.inFlight) {
		t.Fatalf("the old generation's marker %s, want it GAPPED from the Slot that ended query-free", raw)
	}
}
