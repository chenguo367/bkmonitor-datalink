package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

type absentTestControl struct {
	snapshot    controlplane.ObservedSnapshot
	haveSnaphot bool
	departed    []controlplane.DepartedStrategy
	published   []controlplane.DepartedStrategy
	refused     uint64
}

func (control *absentTestControl) ObservedSnapshot() (controlplane.ObservedSnapshot, bool) {
	return control.snapshot, control.haveSnaphot
}
func (control *absentTestControl) DepartedStrategies() ([]controlplane.DepartedStrategy, uint64) {
	return control.departed, control.refused
}
func (control *absentTestControl) PublishedStrategies() []controlplane.DepartedStrategy {
	return control.published
}

type absentTestIndex struct {
	holding map[openalerts.StrategyKey]bool
	err     error
}

func (index *absentTestIndex) HasOpenAlerts(_ context.Context, key openalerts.StrategyKey) (bool, error) {
	if index.err != nil {
		return false, index.err
	}
	return index.holding[key], nil
}

type absentTestAlerts struct {
	alerts []openalerts.Alert
	err    error
}

func (a *absentTestAlerts) Reconcile(context.Context, openalerts.StrategyKey) (openalerts.Reconciliation, error) {
	if a.err != nil {
		return openalerts.Reconciliation{}, a.err
	}
	return openalerts.Reconciliation{Alerts: a.alerts}, nil
}

type absentTestWriter struct {
	batches [][]linkdoutput.CloseRequest
	err     error
}

func (w *absentTestWriter) WriteCloseBatch(_ context.Context, requests []linkdoutput.CloseRequest) error {
	if w.err != nil {
		return w.err
	}
	w.batches = append(w.batches, append([]linkdoutput.CloseRequest(nil), requests...))
	return nil
}

// liveSnapshot is a source observation of the given size that never lists
// the departed strategy.
func liveSnapshot(observation string, at time.Time, size int) controlplane.ObservedSnapshot {
	snapshot := controlplane.ObservedSnapshot{Observation: observation, ReadAt: at}
	for i := 0; i < size; i++ {
		snapshot.Strategies = append(snapshot.Strategies, controlplane.DepartedStrategy{
			TenantID: "system", StrategyID: "live-" + itoaAbsent(i), BusinessID: 2})
	}
	return snapshot
}

func itoaAbsent(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

type absentTestFixture struct {
	loop    *absentStrategyClose
	control *absentTestControl
	writer  *absentTestWriter
	alerts  *absentTestAlerts
	now     time.Time
}

func newAbsentFixture(t *testing.T, alerts []openalerts.Alert) *absentTestFixture {
	t.Helper()
	start := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	fixture := &absentTestFixture{now: start}
	fixture.control = &absentTestControl{
		snapshot: liveSnapshot("observation-one", start, 100), haveSnaphot: true,
		departed: []controlplane.DepartedStrategy{{TenantID: "system", StrategyID: "10", BusinessID: 2, Revision: 7, At: start.Add(-time.Hour)}},
	}
	fixture.writer = &absentTestWriter{}
	fixture.alerts = &absentTestAlerts{alerts: alerts}
	bundle := &phaseTwoWorkerBundle{dependencies: phaseTwoWorkerBundleDependencies{
		Now: func() time.Time { return fixture.now }, Observer: observability.NopObserver{}}}
	bundle.controlLeader = true
	index := &absentTestIndex{holding: map[openalerts.StrategyKey]bool{{TenantID: "system", StrategyID: "10"}: true}}
	fixture.loop = newAbsentStrategyClose(bundle, fixture.control, index, fixture.alerts, fixture.writer, "native", true)
	return fixture
}

func nativeAlert(id, fingerprint string) openalerts.Alert {
	return openalerts.Alert{AlertID: id, EventSourceID: "native", Fingerprint: fingerprint, Severity: "1"}
}

// The whole capability, end to end: a strategy the catalog let go, still
// holding an alert this deployment produced, gets it closed - but not on
// the first round, and not against one observation.
func TestTheAlertsOfADeletedStrategyAreClosedAfterTheGraceAndASecondObservation(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	ctx := context.Background()

	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("a candidate was closed on the round it was first seen: %+v", fixture.writer.batches)
	}
	if fixture.loop.Stats()[absentalerts.OutcomeWithinGrace] != 1 {
		t.Fatalf("the first round did not report why it closed nothing: %+v", fixture.loop.Stats())
	}

	// The grace has passed but the source has not been read again: one
	// observation cannot confirm what one observation found.
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("one observation confirmed an absence: %+v", fixture.writer.batches)
	}
	if fixture.loop.Stats()[absentalerts.OutcomeUnconfirmed] != 1 {
		t.Fatalf("the second round did not report why it closed nothing: %+v", fixture.loop.Stats())
	}

	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 1 || len(fixture.writer.batches[0]) != 1 {
		t.Fatalf("the alert of a deleted strategy was not closed: %+v", fixture.writer.batches)
	}
	request := fixture.writer.batches[0][0]
	if request.Reason != linkdoutput.CloseReasonAbsent {
		t.Fatalf("the close went out under the wrong reason: %q", request.Reason)
	}
	if request.StrategyID != 10 || request.BusinessID != 2 || request.StrategyRevision != 7 {
		t.Fatalf("the close lost the identity only the catalog remembered: %+v", request)
	}
	if _, err := linkdoutput.ConvertClose(request); err != nil {
		t.Fatalf("the close this loop builds is not a close the contract accepts: %v", err)
	}
}

// Whose alert it is, is a fact the alert carries. An alert another producer
// wrote is not closed, and neither is one that names no producer: "it does
// not say it is someone else's" is not "it is ours".
func TestOnlyThisDeploymentsOwnAlertsAreClosed(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{
		nativeAlert("mine", "0123456789abcdef0123456789abcdef"),
		{AlertID: "theirs", EventSourceID: "another-source", Fingerprint: "1123456789abcdef0123456789abcdef", Severity: "1"},
		{AlertID: "nameless", Fingerprint: "2123456789abcdef0123456789abcdef", Severity: "1"},
	})
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 1 || len(fixture.writer.batches[0]) != 1 {
		t.Fatalf("the close batch is not exactly this deployment's own alert: %+v", fixture.writer.batches)
	}
	if fixture.writer.batches[0][0].AlertInstanceID != "mine" {
		t.Fatalf("a close went to an alert this deployment did not produce: %+v", fixture.writer.batches[0][0])
	}
	stats := fixture.loop.Stats()
	if stats[absentalerts.OutcomeProducerForeign] != 1 || stats[absentalerts.OutcomeProducerUnknown] != 1 {
		t.Fatalf("the alerts that were not closed were not reported: %+v", stats)
	}
}

// A replica that is not the control leader takes no difference, and says so
// rather than reporting a deployment with nothing to close.
func TestAFollowerClosesNothingAndSaysWhy(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	fixture.loop.bundle.controlLeader = false
	fixture.loop.step(context.Background())
	if len(fixture.writer.batches) != 0 || fixture.loop.Stats()[absentalerts.OutcomeNotLeader] != 1 {
		t.Fatalf("a follower decided something, or did not say it was one: %+v", fixture.loop.Stats())
	}
}

// Losing the term restarts every candidate's clock: an absence observed
// under a term this replica no longer holds must not mature into a close
// after it comes back.
func TestLosingTheControlTermRestartsTheGrace(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.loop.bundle.controlLeader = false
	fixture.loop.step(ctx)
	fixture.loop.bundle.controlLeader = true
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("a close matured on an absence observed under a term the replica had lost: %+v", fixture.writer.batches)
	}
}

// Without the reconciliation endpoint there is no authoritative alert
// metadata. Every round says so; none of them reports a clean zero.
func TestWithoutTheReconciliationEndpointNothingIsClosedAndTheRoundSaysSo(t *testing.T) {
	fixture := newAbsentFixture(t, nil)
	fixture.loop.alerts = nil
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if fixture.loop.Stats()[absentalerts.OutcomeEvidenceUnavailable] != 1 {
		t.Fatalf("a deployment that cannot read its alerts reported a clean round: %+v", fixture.loop.Stats())
	}
}

// A snapshot that lost a large share of its strategies refuses the whole
// round by name, whatever the difference happens to look like.
func TestASnapshotThatShrankRefusesTheRoundByName(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 40)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("a round decided on a snapshot that had lost most of its strategies: %+v", fixture.writer.batches)
	}
	if fixture.loop.Rounds()[absentalerts.RefusalSnapshotShrunk] != 1 {
		t.Fatalf("the refusal was not named: %+v", fixture.loop.Rounds())
	}
}

// An index that could not be read is not an index that is empty: the
// strategy stays a candidate and the round reports the read it could not
// make.
func TestAnUnreadableIndexIsReportedAndClosesNothing(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	fixture.loop.index = &absentTestIndex{err: errors.New("index unavailable")}
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("an unread index produced a close: %+v", fixture.writer.batches)
	}
	if fixture.loop.Stats()[absentalerts.OutcomeIndexUnreadable] == 0 {
		t.Fatalf("an unread index was not reported: %+v", fixture.loop.Stats())
	}
}

// A strategy the fleet is still running a Plan of is detecting, whatever
// the departure memory says.
func TestAStrategyTheFleetStillRunsIsNeverClosed(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	fixture.control.published = []controlplane.DepartedStrategy{{TenantID: "system", StrategyID: "10", BusinessID: 2, Revision: 7}}
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("a strategy with a published Plan had its alerts closed: %+v", fixture.writer.batches)
	}
}

// Until the deployment arms it, the difference runs in full and sends
// nothing: what it would close is counted as a decision, and what went out
// stays at zero. That is how the numbers can be read before they are acted
// on, which is the only way to know whether this deployment's numbers are
// the expected ones.
func TestAnUnarmedDifferenceDecidesEverythingAndSendsNothing(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{nativeAlert("alert-1", "0123456789abcdef0123456789abcdef")})
	fixture.loop.send = false
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("an unarmed difference sent a close: %+v", fixture.writer.batches)
	}
	stats := fixture.loop.Stats()
	if stats[absentalerts.OutcomeClosed] != 1 {
		t.Fatalf("an unarmed difference did not report what it would have closed: %+v", stats)
	}
	if stats[absentalerts.OutcomeAlertClosed] != 0 {
		t.Fatalf("an unarmed difference counted alerts as closed: %+v", stats)
	}
	if stats[absentalerts.OutcomeWouldSend] != 1 {
		t.Fatalf("an unarmed difference did not say how many alerts arming would send: %+v", stats)
	}
	if fixture.loop.Difference()["send_armed"] != 0 {
		t.Fatalf("the reading does not say the close is unarmed: %+v", fixture.loop.Difference())
	}
}

// The guard against closing another deployment's alerts has to be read
// while the close is unarmed, which means the alerts have to be read then
// too. A producer_foreign of zero that comes from never having looked is
// indistinguishable from one that comes from looking, and it is exactly the
// number someone would arm the close on.
func TestAnUnarmedDifferenceStillFilesEveryAlertByItsProducer(t *testing.T) {
	fixture := newAbsentFixture(t, []openalerts.Alert{
		nativeAlert("mine", "0123456789abcdef0123456789abcdef"),
		{AlertID: "theirs", EventSourceID: "another-source", Fingerprint: "1123456789abcdef0123456789abcdef", Severity: "1"},
		{AlertID: "nameless", Fingerprint: "2123456789abcdef0123456789abcdef", Severity: "1"},
		{AlertID: "no-severity", EventSourceID: "native", Fingerprint: "3123456789abcdef0123456789abcdef"},
	})
	fixture.loop.send = false
	ctx := context.Background()
	fixture.loop.step(ctx)
	fixture.now = fixture.now.Add(controlplane.AbsenceGracePeriod + time.Minute)
	fixture.control.snapshot = liveSnapshot("observation-two", fixture.now, 100)
	fixture.loop.step(ctx)
	stats := fixture.loop.Stats()
	if stats[absentalerts.OutcomeProducerForeign] != 1 || stats[absentalerts.OutcomeProducerUnknown] != 1 || stats[absentalerts.OutcomeMetadataMissing] != 1 {
		t.Fatalf("an unarmed round left the per-alert guards unread: %+v", stats)
	}
	if stats[absentalerts.OutcomeWouldSend] != 1 {
		t.Fatalf("an unarmed round did not count what arming would send: %+v", stats)
	}
	if len(fixture.writer.batches) != 0 {
		t.Fatalf("an unarmed round sent something: %+v", fixture.writer.batches)
	}
}

// The rounds are their own family: what this service could do is not what
// the data was, and a reader summing one must not be adding the other.
func TestARefusedRoundIsCountedAsARoundNotAsACandidate(t *testing.T) {
	fixture := newAbsentFixture(t, nil)
	fixture.control.haveSnaphot = false
	fixture.loop.step(context.Background())
	if fixture.loop.Rounds()[absentalerts.RefusalSnapshotUnusable] != 1 {
		t.Fatalf("the refused round was not counted as a round: %+v", fixture.loop.Rounds())
	}
	for outcome, count := range fixture.loop.Stats() {
		if count != 0 {
			t.Fatalf("a refused round put %s in the candidate family: %+v", outcome, fixture.loop.Stats())
		}
	}
}
