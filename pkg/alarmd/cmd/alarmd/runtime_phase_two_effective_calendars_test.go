package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// alertDaysOnCalendar makes the strategy alert only on the days calendar 7
// lists, all day long: whether it is effective is the calendar's answer
// alone.
func alertDaysOnCalendar(plan *contract.EvaluationPlanV2) {
	plan.StrategyIR.Levels[0].TriggerPlan.Config = json.RawMessage(`{"window_size":1,"required_anomalies":1,"step_seconds":60,` +
		`"timezone_ref":"BUSINESS_LOCAL","uptime":{"time_ranges":[{"start":"00:00","end":"23:59"}],"active_calendars":[7],"calendars":[]}}`)
}

// A calendar item that does not cover the fixture's moment.
const calendarItemElsewhere = `{"id":1,"time_kind":"UNIX_SECONDS","start_time":100,"end_time":200,"time_zone":"UTC","parent_id":null,"repeat":{}}`

// Every calendar the strategies name reads deleted at once: the writer's
// calendar source answered nothing, and each calendar it could not find
// arrived marked deleted. The strategy that alerts only on calendar days is
// inactive, as Python reads it - detection is unchanged - and its alerts
// are not closed for it; the held close is counted by name.
func TestEveryCalendarReadingDeletedAtOnceClosesNothing(t *testing.T) {
	snapshot := `{"schema_version":1,"status":"READY","business_timezone":"UTC","calendars":[` +
		`{"id":7,"bk_tenant_id":"tenant-a","status":"DELETED","items":[]}]}`
	f := newMaintenanceTestFixture(t, snapshot, maintenanceTime(8, 0), []openalerts.Alert{maintenanceAlert("native", "critical")}, alertDaysOnCalendar)
	plan := f.m.catalog.(*maintenanceTestCatalog).plans[0].Compiled
	fact, err := plan.ResolveEffectiveTime(context.Background(), maintenanceTime(8, 0).Unix())
	if err != nil || fact.Status() != strategy.EffectiveTimeInactive {
		t.Fatalf("detection reads the effective time as (%v, %v), want inactive as before", fact.Status(), err)
	}

	f.m.step(context.Background())

	if f.runner.lastErr != nil || len(f.writer.batches) != 0 {
		t.Fatalf("err=%v batches=%v, want no close while every calendar reads deleted", f.runner.lastErr, f.writer.batches)
	}
	if held := f.m.Stats()[closeOutcomeCalendarsAllDeleted]; held != 1 {
		t.Fatalf("held closes counted %d, want 1", held)
	}
}

// One calendar deleted among ones still present is a deletion. The strategy
// that named the deleted calendar as its only alert days is inactive and is
// closed, as Python closes it.
func TestOneCalendarDeletedAmongPresentOnesStillCloses(t *testing.T) {
	snapshot := `{"schema_version":1,"status":"READY","business_timezone":"UTC","calendars":[` +
		`{"id":7,"bk_tenant_id":"tenant-a","status":"DELETED","items":[]},` +
		`{"id":8,"bk_tenant_id":"tenant-a","status":"PRESENT","items":[` + calendarItemElsewhere + `]}]}`
	withRestDays := func(plan *contract.EvaluationPlanV2) {
		plan.StrategyIR.Levels[0].TriggerPlan.Config = json.RawMessage(`{"window_size":1,"required_anomalies":1,"step_seconds":60,` +
			`"timezone_ref":"BUSINESS_LOCAL","uptime":{"time_ranges":[{"start":"00:00","end":"23:59"}],"active_calendars":[7],"calendars":[8]}}`)
	}
	f := newMaintenanceTestFixture(t, snapshot, maintenanceTime(8, 0), []openalerts.Alert{maintenanceAlert("native", "critical")}, withRestDays)

	f.m.step(context.Background())

	if f.runner.lastErr != nil || len(f.writer.batches) != 1 {
		t.Fatalf("err=%v batches=%v, want the inactive strategy closed", f.runner.lastErr, f.writer.batches)
	}
	if held := f.m.Stats()[closeOutcomeCalendarsAllDeleted]; held != 0 {
		t.Fatalf("held closes counted %d, want 0", held)
	}
}

// maintenanceCatalogByGroup answers each Query Group from its own catalog.
type maintenanceCatalogByGroup map[execution.QueryGroupIdentity]*maintenanceTestCatalog

func (catalogs maintenanceCatalogByGroup) CurrentPlans(ctx context.Context, qg execution.QueryGroupIdentity,
	at execution.EvaluationTime) (controlplane.MaintenancePlans, error) {
	return catalogs[qg].CurrentPlans(ctx, qg, at)
}

// A source-wide loss reaches the owned groups one read at a time. In the
// step that has read only the first of two groups again, the second still
// holds its calendar present from before, and the first's strategy - now
// inactive on a deleted calendar - is still not closed.
func TestTheFirstGroupReadAfterACalendarLossIsNotClosedOnTheOthersOldRead(t *testing.T) {
	at := maintenanceTime(8, 0)
	deleted := `{"schema_version":1,"status":"READY","business_timezone":"UTC","calendars":[` +
		`{"id":7,"bk_tenant_id":"tenant-a","status":"DELETED","items":[]}]}`
	covering := fmt.Sprintf(`{"id":1,"time_kind":"UNIX_SECONDS","start_time":%d,"end_time":%d,"time_zone":"UTC","parent_id":null,"repeat":{}}`,
		at.Add(-time.Hour).Unix(), at.Add(time.Hour).Unix())
	present := `{"schema_version":1,"status":"READY","business_timezone":"UTC","calendars":[` +
		`{"id":7,"bk_tenant_id":"tenant-a","status":"PRESENT","items":[` + covering + `]}]}`
	f := newMaintenanceTestFixture(t, deleted, at, []openalerts.Alert{maintenanceAlert("native", "critical")}, alertDaysOnCalendar)
	afterLoss := f.m.catalog.(*maintenanceTestCatalog).plans
	beforeLoss := newMaintenanceTestFixture(t, present, at, nil, alertDaysOnCalendar).m.catalog.(*maintenanceTestCatalog).plans

	first := &maintenanceTestRunner{check: func(context.Context) error { return nil }, scope: "obj", revision: 1}
	second := &maintenanceTestRunner{check: func(context.Context) error { return nil }, scope: "obj", revision: 1}
	f.m.bundle.runners = map[execution.QueryGroupIdentity]*phaseTwoQueryGroupLifecycle{"qg-a": {runner: first}, "qg-b": {runner: second}}
	catalogs := maintenanceCatalogByGroup{"qg-a": {plans: beforeLoss}, "qg-b": {plans: beforeLoss}}
	f.m.catalog = catalogs
	f.m.step(context.Background())
	if len(f.writer.batches) != 0 {
		t.Fatalf("setup: closed %v while the calendar covers the moment", f.writer.batches)
	}

	// The loss: both groups' Plans change, and one group is read per step.
	catalogs["qg-a"].plans, catalogs["qg-b"].plans = afterLoss, afterLoss
	first.revision, second.revision = 2, 2
	f.m.capacity.GroupBatch = 1
	f.m.step(context.Background())
	if known := f.m.groups["qg-a"]; known == nil || known.timelineRevision != 2 || f.m.groups["qg-b"].timelineRevision != 1 {
		t.Fatal("setup: want the first group read again and the second not yet")
	}
	if len(f.writer.batches) != 0 {
		t.Fatalf("closed %v on the second group's read from before the loss", f.writer.batches)
	}
}

// Only groups read under the lease they hold now count. A group whose lease
// has moved still holds its calendars as they were before the change, and
// counting it would let the groups already read again close in the minute a
// source-wide loss arrives.
func TestAGroupWhoseLeaseMovedDoesNotSpeakForTheCalendars(t *testing.T) {
	read := &maintenanceTestRunner{scope: "obj-a", revision: 2}
	moved := &maintenanceTestRunner{scope: "obj-b", revision: 3}
	groups := map[execution.QueryGroupIdentity]*maintenanceGroup{
		"qg-read":  {contentScope: "obj-a", timelineRevision: 2, calendars: map[int64]bool{7: false}},
		"qg-moved": {contentScope: "obj-b", timelineRevision: 2, calendars: map[int64]bool{7: true}},
	}
	runners := map[execution.QueryGroupIdentity]maintenanceRunner{"qg-read": read, "qg-moved": moved}

	if !calendarsAllDeleted(readUnderCurrentLease(groups, runners)) {
		t.Fatal("a group read before its lease moved kept the deleted calendars counted as present")
	}
	moved.revision = 2
	if calendarsAllDeleted(readUnderCurrentLease(groups, runners)) {
		t.Fatal("with both groups current, a calendar one of them holds present still reads deleted")
	}
}
