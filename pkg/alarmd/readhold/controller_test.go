// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package readhold

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

type memoryControl struct {
	values   map[execution.QueryGroupIdentity][]byte
	requests []ownership.FencedCASRequest
	refused  ownership.FencedCASStatus
	readErr  map[execution.QueryGroupIdentity]error
}

func (store *memoryControl) ReadControlBatch(_ context.Context, groups []execution.QueryGroupIdentity, _ string) ([]ownership.ControlRead, error) {
	reads := make([]ownership.ControlRead, len(groups))
	for index, qg := range groups {
		raw, found := store.values[qg]
		reads[index] = ownership.ControlRead{Raw: append([]byte(nil), raw...), Missing: !found, Err: store.readErr[qg]}
	}
	return reads, nil
}

func (store *memoryControl) FencedCompareAndSet(_ context.Context, request ownership.FencedCASRequest) (ownership.FencedCASStatus, error) {
	store.requests = append(store.requests, request)
	if store.refused != "" {
		return store.refused, nil
	}
	current, found := store.values[request.Fence.QueryGroup]
	if request.ExpectedMissing == found || !bytes.Equal(request.Expected, current) {
		return ownership.FencedCASConflict, nil
	}
	store.values[request.Fence.QueryGroup] = append([]byte(nil), request.Value...)
	return ownership.FencedCASApplied, nil
}

func controllerFixture(t *testing.T) (*Controller, *memoryControl, *time.Time) {
	t.Helper()
	now := time.Unix(1_800_000, 0)
	store := &memoryControl{values: make(map[execution.QueryGroupIdentity][]byte), readErr: make(map[execution.QueryGroupIdentity]error)}
	controller, err := NewController(Options{Control: store, Prefix: "schedule", MaxHold: 10 * time.Minute, Now: func() time.Time { return now }, Owner: func(qg execution.QueryGroupIdentity) (Owner, error) {
		return Owner{Fence: holdFence(qg), ContentScope: "view"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return controller, store, &now
}

func holdFence(qg execution.QueryGroupIdentity) execution.OwnerFence {
	return execution.OwnerFence{QueryGroup: qg, OwnerID: "worker", OwnerEpoch: 1, LeaseToken: "lease"}
}

func planRef() PlanRef {
	return PlanRef{Key: execution.PlanKey{PlanIdentity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "business", StrategyID: "strategy"}}, Route: "source/metric"}
}

func groupSpec(qg execution.QueryGroupIdentity, delay time.Duration) GroupSpec {
	return GroupSpec{QueryGroup: qg, Plans: []PlanRef{planRef()}, Delay: delay, SettlingWait: 30 * time.Second, HoldLimit: 10 * time.Minute}
}

func prepare(t *testing.T, controller *Controller, spec GroupSpec) {
	t.Helper()
	if err := controller.Configure(spec); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{spec.QueryGroup}); err != nil {
		t.Fatal(err)
	}
}

func scheduleFor(t *testing.T, qg execution.QueryGroupIdentity, start execution.EvaluationTime, end *execution.EvaluationTime) execution.FrozenQueryGroupSchedule {
	t.Helper()
	spec := execution.ScheduleSpec{EvaluationIntervalSeconds: 60, Timezone: "UTC", CompletionDeadlineOffsetSeconds: 55}
	revision, err := execution.DerivePlanScheduleRevision(spec)
	if err != nil {
		t.Fatal(err)
	}
	plans := []execution.FrozenPlanSchedule{{Identity: planRef().Key.PlanIdentity, ScheduleRevision: revision, Spec: spec}}
	groupRevision, err := execution.DeriveQueryGroupScheduleRevision(plans)
	if err != nil {
		t.Fatal(err)
	}
	return execution.FrozenQueryGroupSchedule{Segment: execution.ScheduleSegmentFact{Publication: execution.SnapshotPublicationRef{PublicationEpoch: 1, SnapshotRevision: "snapshot"}, QueryGroup: qg, QueryRevision: "query", ScheduleRevision: groupRevision, Start: start, End: end}, Plans: plans}
}

func holdContract(qg execution.QueryGroupIdentity, slot execution.EvaluationTime, hold int64) execution.FrozenExecutionContractRef {
	return execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: qg, EvaluationTime: slot}, SnapshotRevision: "snapshot", QueryRevision: "query", ScheduleRevision: "schedule", ScheduleSegmentStart: 60, DuePlanSetDigest: "due", ReadHoldMillis: hold}
}

func observeEarly(t *testing.T, c *Controller, qg execution.QueryGroupIdentity, arrival, first time.Duration, hold int64) {
	t.Helper()
	if err := c.Observe(context.Background(), Evidence{Contract: holdContract(qg, 120, hold), ArrivalAge: arrival, FirstReadAge: first, WholeWindow: true, Confirmed: true, Rung: "rung", Buckets: []int64{120}}); err != nil {
		t.Fatal(err)
	}
}

func TestWholeWindowRaisesButNoiseAndPartialDoNot(t *testing.T) {
	c, store, _ := controllerFixture(t)
	prepare(t, c, groupSpec("qg", 0))
	ctx := context.Background()
	if hold, err := c.SlotReadHold(ctx, scheduleFor(t, "qg", 60, nil), 120, holdFence("qg")); err != nil || hold != 0 {
		t.Fatalf("initial hold %v, %v", hold, err)
	}
	if len(store.requests) != 0 {
		t.Fatal("an initial zero hold wrote Redis")
	}
	observeEarly(t, c, "qg", 180*time.Second, 30*time.Second, 0)
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatal("arrival age was not translated from the window end")
	}
	prepare(t, c, groupSpec("queued", 0))
	observeEarly(t, c, "queued", 180*time.Second, 40*time.Second, 0)
	if c.ReadHold("queued") != 150*time.Second {
		t.Fatal("permit delay in the first read was subtracted from the next round's hold")
	}
	before, _ := c.Reading("qg")
	for _, e := range []Evidence{
		{Contract: holdContract("qg", 120, 0), ArrivalAge: 900 * time.Second, FirstReadAge: 30 * time.Second, Confirmed: true},
		{Contract: holdContract("qg", 120, 0), ArrivalAge: 900 * time.Second, FirstReadAge: 30 * time.Second, WholeWindow: true},
		{Contract: holdContract("qg", 120, 0), Noise: true},
	} {
		if err := c.Observe(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := c.Reading("qg")
	if c.ReadHold("qg") != 150*time.Second || after.Noise != before.Noise+1 {
		t.Fatalf("noise or partial raised hold: %+v", after)
	}
	spec := groupSpec("qg", 0)
	spec.HoldLimit = 200 * time.Second
	if err := c.Configure(spec); err != nil {
		t.Fatal(err)
	}
	observeEarly(t, c, "qg", 900*time.Second, 30*time.Second, 0)
	after, _ = c.Reading("qg")
	if c.ReadHold("qg") != 200*time.Second || !after.AtLimit {
		t.Fatalf("retention margin not enforced: %+v", after)
	}
	spec.HoldLimit = time.Duration(execution.MaxReadHoldMillis) * time.Millisecond
	if err := c.Configure(spec); err != nil {
		t.Fatal(err)
	}
	observeEarly(t, c, "qg", 900*time.Second, 30*time.Second, 0)
	if c.ReadHold("qg") != 10*time.Minute {
		t.Fatal("MaxReplayAge hold bound not enforced")
	}
}

func TestLoweringNeedsQuietAndThreeObservedEarlierReads(t *testing.T) {
	c, _, now := controllerFixture(t)
	prepare(t, c, groupSpec("qg", 0))
	observeEarly(t, c, "qg", 180*time.Second, 30*time.Second, 0)
	ctx := context.Background()
	earlier := func(slot execution.EvaluationTime, observed, equal bool) {
		t.Helper()
		if err := c.EarlierRead(ctx, EarlierEvidence{Contract: holdContract("qg", slot, 150_000), CandidateHold: 75 * time.Second, Observed: observed, Equal: equal}); err != nil {
			t.Fatal(err)
		}
	}
	for index := range 3 {
		earlier(execution.EvaluationTime(180+index*60), true, true)
	}
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatal("lowered before the single-event quiet hour")
	}
	*now = now.Add(time.Hour)
	earlier(360, false, true)
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatal("an unobserved read lowered the hold")
	}
	earlier(420, true, false)
	*now = now.Add(time.Hour)
	for index := range 2 {
		earlier(execution.EvaluationTime(480+index*60), true, true)
	}
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatal("fewer than three fresh matches lowered the hold")
	}
	earlier(600, true, true)
	if c.ReadHold("qg") != 75*time.Second {
		t.Fatal("three matches after quiet did not halve the hold")
	}
	observeEarly(t, c, "qg", 180*time.Second, 105*time.Second, 75_000)
	reading, _ := c.Reading("qg")
	if c.ReadHold("qg") != 150*time.Second || reading.RaisedAfterLowering != 1 {
		t.Fatalf("raise after lowering was not counted: %+v", reading)
	}
	*now = now.Add(2 * time.Hour)
	observeEarly(t, c, "qg", 180*time.Second, 180*time.Second, 150_000)
	reading, _ = c.Reading("qg")
	if reading.MaxEarlyIntervalMillis != 2*time.Hour.Milliseconds() {
		t.Fatalf("event interval %+v", reading)
	}
	*now = now.Add(time.Hour)
	for index := range 3 {
		earlier(execution.EvaluationTime(660+index*60), true, true)
	}
	if c.ReadHold("qg") != 150*time.Second {
		t.Fatal("lowered before the longest observed event interval")
	}
}

func TestTransitionUsesClosedSchedulesLastSlotAndSurvivesRestore(t *testing.T) {
	c, store, _ := controllerFixture(t)
	prepare(t, c, groupSpec("old", 0))
	observeEarly(t, c, "old", 180*time.Second, 30*time.Second, 0)
	ctx := context.Background()
	boundary := execution.EvaluationTime(1200)
	newSpec := groupSpec("new", 120*time.Second)
	newSpec.Previous = []Previous{{QueryGroup: "old", ClosedAt: boundary}}
	prepare(t, c, newSpec)
	newSchedule := scheduleFor(t, "new", boundary, nil)
	if _, err := c.SlotReadHold(ctx, newSchedule, boundary, holdFence("new")); !errors.Is(err, ErrPreviousOpen) {
		t.Fatalf("unclosed predecessor = %v", err)
	}
	oldSchedule := scheduleFor(t, "old", 60, &boundary)
	if err := c.CloseSchedule(ctx, oldSchedule, holdFence("old")); err != nil {
		t.Fatal(err)
	}
	oldReading, _ := c.Reading("old")
	if oldReading.Plans[0].PreviousSlot != 1140 || oldReading.Plans[0].PreviousHoldMillis != 150_000 {
		t.Fatalf("closed record %+v", oldReading)
	}
	observeEarly(t, c, "old", 600*time.Second, 30*time.Second, 0)
	oldReading, _ = c.Reading("old")
	if c.ReadHold("old") != 150*time.Second || oldReading.Plans[0].PreviousHoldMillis != 150_000 {
		t.Fatal("a closed group raised or rewrote its transition hold")
	}
	for index, want := range []time.Duration{115 * time.Second, 55 * time.Second, 30 * time.Second} {
		slot := boundary + execution.EvaluationTime(index*60)
		got, err := c.SlotReadHold(ctx, newSchedule, slot, holdFence("new"))
		if err != nil || got != want {
			t.Fatalf("Slot %d hold %s, %v; want %s", slot, got, err, want)
		}
		if index == 0 && int64(slot)*1000+30_000+got.Milliseconds() < int64(1140)*1000+150_000+55_000 {
			t.Fatal("new read preceded old completion deadline")
		}
	}
	// A fresh controller recovers both the learned base and transition from
	// the same record before it considers any replay.
	next, err := NewController(c.options)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Configure(newSpec); err != nil {
		t.Fatal(err)
	}
	if err := next.RestoreBatch(ctx, []execution.QueryGroupIdentity{"new", "old"}); err != nil {
		t.Fatal(err)
	}
	if next.ReadHoldAt("new", 1320) != 30*time.Second {
		t.Fatal("takeover did not restore the hold")
	}
	farSpec := groupSpec("far", 300*time.Second)
	farSpec.Previous = []Previous{{QueryGroup: "old", ClosedAt: boundary}}
	prepare(t, c, farSpec)
	if got, err := c.SlotReadHold(ctx, scheduleFor(t, "far", 1500, nil), 1500, holdFence("far")); err != nil || got != 0 {
		t.Fatalf("longer delay did not replace automatic hold: %s %v", got, err)
	}
	if store.requests[len(store.requests)-1].TTL != RecordTTL {
		t.Fatal("record lifetime is not seven days")
	}
}

func TestFencedRefusalChangesNoMemoryAndRenewalIsDaily(t *testing.T) {
	c, store, now := controllerFixture(t)
	prepare(t, c, groupSpec("qg", 0))
	observeEarly(t, c, "qg", 180*time.Second, 30*time.Second, 0)
	before, _ := c.Reading("qg")
	store.refused = ownership.FencedCASStaleOwner
	if err := c.Observe(context.Background(), Evidence{Contract: holdContract("qg", 180, 0), ArrivalAge: 240 * time.Second, FirstReadAge: 30 * time.Second, Confirmed: true, WholeWindow: true}); !errors.Is(err, ownership.ErrStaleFence) {
		t.Fatalf("stale fence=%v", err)
	}
	after, _ := c.Reading("qg")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("stale writer changed local hold")
	}
	store.refused = ""
	writes := len(store.requests)
	*now = now.Add(23 * time.Hour)
	if err := c.RenewDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != writes {
		t.Fatal("renewed before a day")
	}
	*now = now.Add(time.Hour)
	if err := c.RenewDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != writes+1 || store.requests[len(store.requests)-1].TTL != RecordTTL {
		t.Fatal("daily renewal did not retain seven days")
	}
	store.readErr["broken"] = errors.New("unanswered")
	if err := c.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{"qg", "broken", "missing"}); err == nil {
		t.Fatal("unanswered restore was reported complete")
	}
	if !c.Inspect("qg").Loaded || c.Inspect("broken").Loaded || !c.Inspect("missing").Missing {
		t.Fatal("batch restore confused success, failure and missing")
	}
}

func TestSharedGroupReopensWhileMigratedPlanKeepsClosedHold(t *testing.T) {
	c, _, _ := controllerFixture(t)
	a, b := planRef(), planRef()
	b.Key.StrategyID = "retained"
	oldSpec := groupSpec("old", 0)
	oldSpec.Plans = []PlanRef{a, b}
	prepare(t, c, oldSpec)
	observeEarly(t, c, "old", 180*time.Second, 30*time.Second, 0)
	boundary := execution.EvaluationTime(1200)
	oldSchedule := scheduleFor(t, "old", 60, &boundary)
	retainedPlan := oldSchedule.Plans[0]
	retainedPlan.Identity = b.Key.PlanIdentity
	oldSchedule.Plans = append(oldSchedule.Plans, retainedPlan)
	oldSchedule.Segment.ScheduleRevision, _ = execution.DeriveQueryGroupScheduleRevision(oldSchedule.Plans)
	if err := c.CloseSchedule(context.Background(), oldSchedule, holdFence("old")); err != nil {
		t.Fatal(err)
	}
	closed, _ := c.Reading("old")
	fixed := closed.Plans[0]
	if fixed.ClosedAt != boundary || fixed.PreviousHoldMillis != 150_000 {
		t.Fatalf("Plan A was not closed: %+v", fixed)
	}
	// Only A moves to the new delay. The same old Query Group starts a new
	// schedule segment for B and must still learn later whole-window reads.
	retainedSpec := oldSpec
	retainedSpec.Plans = []PlanRef{b}
	if err := c.Configure(retainedSpec); err != nil {
		t.Fatal(err)
	}
	retainedSchedule := scheduleFor(t, "old", boundary, nil)
	retainedSchedule.Plans[0].Identity = b.Key.PlanIdentity
	retainedSchedule.Segment.ScheduleRevision, _ = execution.DeriveQueryGroupScheduleRevision(retainedSchedule.Plans)
	if got, err := c.SlotReadHold(context.Background(), retainedSchedule, boundary, holdFence("old")); err != nil || got != 150*time.Second {
		t.Fatalf("reopened group = %s %v", got, err)
	}
	newContract := holdContract("old", 1260, 150_000)
	newContract.ScheduleSegmentStart = boundary
	if err := c.Observe(context.Background(), Evidence{Contract: newContract, ArrivalAge: 330 * time.Second, FirstReadAge: 180 * time.Second, Confirmed: true, WholeWindow: true}); err != nil {
		t.Fatal(err)
	}
	if c.ReadHold("old") != 300*time.Second {
		t.Fatal("retained Plan could not raise h after the shared group reopened")
	}
	observeEarly(t, c, "old", 600*time.Second, 30*time.Second, 0)
	oldReading, _ := c.Reading("old")
	if oldReading.Closed || oldReading.SegmentStart != boundary || oldReading.Plans[0] != fixed || c.ReadHold("old") != 300*time.Second {
		t.Fatalf("old evidence altered a migrated Plan's fixed bridge: %+v", oldReading)
	}
	newSpec := groupSpec("new", 120*time.Second)
	newSpec.Previous = []Previous{{QueryGroup: "old", ClosedAt: boundary}}
	prepare(t, c, newSpec)
	if got, err := c.SlotReadHold(context.Background(), scheduleFor(t, "new", boundary, nil), boundary, holdFence("new")); err != nil || got != 115*time.Second {
		t.Fatalf("migrated Plan used the reopened group's h: %s %v", got, err)
	}
}

func TestMissingPredecessorRequiresIndependentZeroFact(t *testing.T) {
	c, store, _ := controllerFixture(t)
	boundary := execution.EvaluationTime(1200)
	prepare(t, c, groupSpec("missing", 0))
	newSpec := groupSpec("new", 0)
	newSpec.Previous = []Previous{{QueryGroup: "missing", ClosedAt: boundary}}
	prepare(t, c, newSpec)
	schedule := scheduleFor(t, "new", boundary, nil)
	if _, err := c.SlotReadHold(context.Background(), schedule, boundary, holdFence("new")); !errors.Is(err, ErrPreviousHoldUnknown) {
		t.Fatalf("absence became a zero predecessor: %v", err)
	}
	newSpec.Previous[0].ZeroConfirmed = true
	if err := c.Configure(newSpec); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SlotReadHold(context.Background(), schedule, boundary, holdFence("new")); !errors.Is(err, ErrPreviousHoldUnknown) {
		t.Fatalf("zero h without the old completion timeline became a complete bridge: %v", err)
	}
	old := scheduleFor(t, "missing", 60, &boundary)
	newSpec.Previous[0].ZeroConfirmed, newSpec.Previous[0].Schedule = true, &old
	if err := c.Configure(newSpec); err != nil {
		t.Fatal(err)
	}
	if got, err := c.SlotReadHold(context.Background(), schedule, boundary, holdFence("new")); err != nil || got != 0 {
		t.Fatalf("confirmed zero = %s %v", got, err)
	}
	if _, exists := store.values["missing"]; exists {
		t.Fatal("new owner wrote the old zero record")
	}
	// A reused Query Group record that predates this cutover must seed again;
	// having any record does not establish this Plan's newest closed bridge.
	existing := Record{HoldMillis: 150_000, SinceSlot: 60, SegmentStart: 60}
	store.values["new"], _ = json.Marshal(existing)
	if err := c.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{"new"}); err != nil {
		t.Fatal(err)
	}
	if got, err := c.SlotReadHold(context.Background(), schedule, boundary, holdFence("new")); err != nil || got != 150*time.Second {
		t.Fatalf("existing active h = %s %v", got, err)
	}
	reading, _ := c.Reading("new")
	if len(reading.Plans) == 0 || reading.Plans[0].ClosedAt != boundary {
		t.Fatal("the reused record skipped the newest bridge")
	}
}

func TestSamePreviousGroupKeepsEachPlansClosedBoundary(t *testing.T) {
	c, store, _ := controllerFixture(t)
	a, b := planRef(), planRef()
	b.Key.StrategyID = "second"
	old := Record{HoldMillis: 300_000, SinceSlot: 1260, SegmentStart: 1500, Plans: []PlanRecord{
		{PlanRef: a, ArrivalAgeMillis: 180_000, ClosedAt: 1200, PreviousHoldMillis: 150_000, PreviousSlot: 1140, CompletionOffsetMillis: 55_000},
		{PlanRef: b, ArrivalAgeMillis: 330_000, ClosedAt: 1500, PreviousHoldMillis: 300_000, PreviousSlot: 1440, CompletionOffsetMillis: 55_000},
	}}
	store.values["old"], _ = json.Marshal(old)
	if err := c.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{"old"}); err != nil {
		t.Fatal(err)
	}
	spec := groupSpec("new", 120*time.Second)
	spec.Plans = []PlanRef{a, b}
	spec.Previous = []Previous{{QueryGroup: "old", ClosedAt: 1200, Plans: []PlanRef{a}}, {QueryGroup: "old", ClosedAt: 1500, Plans: []PlanRef{b}}}
	prepare(t, c, spec)
	schedule := scheduleFor(t, "new", 1500, nil)
	second := schedule.Plans[0]
	second.Identity = b.Key.PlanIdentity
	schedule.Plans = append(schedule.Plans, second)
	schedule.Segment.ScheduleRevision, _ = execution.DeriveQueryGroupScheduleRevision(schedule.Plans)
	if got, err := c.SlotReadHold(context.Background(), schedule, 1500, holdFence("new")); err != nil || got != 265*time.Second {
		t.Fatalf("two Plan bridges = %s %v; want later deadline 265s", got, err)
	}
	reading, _ := c.Reading("new")
	if len(reading.Plans) != 2 || reading.Plans[0].ClosedAt != 1200 || reading.Plans[1].ClosedAt != 1500 {
		t.Fatalf("one Query Group link overwrote another boundary: %+v", reading.Plans)
	}
}

func TestEvidenceWriteCannotBypassNewPlansPredecessorSeed(t *testing.T) {
	c, store, _ := controllerFixture(t)
	a, b := planRef(), planRef()
	b.Key.StrategyID = "incoming"
	prepare(t, c, groupSpec("existing", 120*time.Second))
	observeEarly(t, c, "existing", 180*time.Second, 150*time.Second, 0)
	if c.ReadHold("existing") != 30*time.Second {
		t.Fatal("existing group's learned base")
	}
	spec := groupSpec("existing", 120*time.Second)
	spec.Plans = []PlanRef{a, b}
	spec.Previous = []Previous{{QueryGroup: "previous", ClosedAt: 1200, Plans: []PlanRef{b}}}
	if err := c.Configure(spec); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{"previous"}); err != nil {
		t.Fatal(err)
	}
	// A previous-segment sample completes after Configure but before the new
	// segment freezes. Its durable metrics cannot acknowledge an unread bridge.
	if err := c.Observe(context.Background(), Evidence{Contract: holdContract("existing", 180, 0), Noise: true}); err != nil {
		t.Fatal(err)
	}
	schedule := scheduleFor(t, "existing", 1200, nil)
	second := schedule.Plans[0]
	second.Identity = b.Key.PlanIdentity
	schedule.Plans = append(schedule.Plans, second)
	schedule.Segment.ScheduleRevision, _ = execution.DeriveQueryGroupScheduleRevision(schedule.Plans)
	if _, err := c.SlotReadHold(context.Background(), schedule, 1200, holdFence("existing")); !errors.Is(err, ErrPreviousHoldUnknown) {
		t.Fatalf("noise write bypassed the missing predecessor: %v", err)
	}
	previous := Record{HoldMillis: 150_000, SinceSlot: 60, SegmentStart: 60, Closed: true, Plans: []PlanRecord{{PlanRef: b,
		ArrivalAgeMillis: 180_000, ClosedAt: 1200, PreviousHoldMillis: 150_000, PreviousSlot: 1140, CompletionOffsetMillis: 55_000}}}
	store.values["previous"], _ = json.Marshal(previous)
	if err := c.RestoreBatch(context.Background(), []execution.QueryGroupIdentity{"previous"}); err != nil {
		t.Fatal(err)
	}
	if got, err := c.SlotReadHold(context.Background(), schedule, 1200, holdFence("existing")); err != nil || got != 115*time.Second {
		t.Fatalf("incoming Plan did not extend first read to the old completion deadline: %s %v", got, err)
	}
}
