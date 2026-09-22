package main

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/absentalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// absentCloseInterval is how often the leader takes the difference. The
// bound is not cost - the round is a handful of point reads - but the
// grace: a candidate has to be seen missing by two rounds under two
// observations before it is closed, and the source is read again at least
// every six minutes, so a round every five minutes is what makes a
// confirmed absence reach a close inside a quarter of an hour.
const absentCloseInterval = 5 * time.Minute

// absentCloseReadBatch bounds the point reads one round makes against the
// alert index. The candidates are the strategies the catalog let go, which
// is a small population; the bound is there so that a deployment that just
// deleted a thousand strategies works through them over rounds instead of
// in one burst on a connection shared with the runtime state.
const absentCloseReadBatch = 64

// absentCloseAlertBatch bounds the alerts one strategy's close sends at
// once.
const absentCloseAlertBatch = 64

// absentStrategyClose closes the unrecovered alerts of strategies that no
// longer exist. See package absentalerts for why the difference is taken
// from the catalog's side and what has to agree before anything is closed.
type absentStrategyClose struct {
	bundle     *phaseTwoWorkerBundle
	reconciler absentCloseControl
	index      absentCloseIndex
	alerts     openalerts.Reconciler
	writer     closeWriter
	sourceID   string
	tracker    *absentalerts.Tracker
	bounds     absentalerts.Bounds
	// previousSnapshot is how large the last snapshot this loop decided on
	// was, which is what the next one's size is judged against.
	previousSnapshot int
	// wasLeader is whether the previous round ran as leader. Losing the
	// term clears the candidate clocks: a replica that comes back after an
	// hour must not close on memory it made in another term.
	wasLeader bool
	countsMu  sync.Mutex
	counts    map[string]uint64
	// lastCounts is the last round's denominators, which the gauge reports.
	lastCounts absentalerts.Counts
}

// absentCloseControl is what the loop asks the control plane: what the
// source says exists, what the catalog let go, and what it is running.
type absentCloseControl interface {
	ObservedSnapshot() (controlplane.ObservedSnapshot, bool)
	DepartedStrategies() ([]controlplane.DepartedStrategy, uint64)
	PublishedStrategies() []controlplane.DepartedStrategy
}

// absentCloseIndex is the one question this loop asks the alert link per
// strategy.
type absentCloseIndex interface {
	HasOpenAlerts(context.Context, openalerts.StrategyKey) (bool, error)
}

func newAbsentStrategyClose(bundle *phaseTwoWorkerBundle, reconciler absentCloseControl,
	index absentCloseIndex, alerts openalerts.Reconciler, writer closeWriter, sourceID string) *absentStrategyClose {
	return &absentStrategyClose{
		bundle: bundle, reconciler: reconciler, index: index, alerts: alerts, writer: writer, sourceID: sourceID,
		tracker: absentalerts.NewTracker(controlplane.MaxDepartedStrategies),
		bounds: absentalerts.Bounds{
			// The loop's own grace, on top of the removal grace the catalog
			// already applied before it let the strategy go.
			Grace:          controlplane.AbsenceGracePeriod,
			MaxSnapshotAge: 30 * time.Minute,
			// A tenth of the deployment's strategies departing and still
			// holding alerts is not a day's deletions.
			MaxDifferenceRatio: 0.1, MinDifferenceForRatio: 20,
			// A strategy list that lost a fifth of its entries is the fact;
			// see RefusalSnapshotShrunk.
			MaxSnapshotShrinkRatio: 0.2, MinSnapshotForShrink: 20,
			MaxCloseStrategies: 8,
		},
		counts: make(map[string]uint64),
	}
}

// Stats is the outcome counts, for the metric that reports every cell.
func (loop *absentStrategyClose) Stats() map[string]uint64 {
	loop.countsMu.Lock()
	defer loop.countsMu.Unlock()
	counts := make(map[string]uint64, len(absentalerts.Outcomes)+len(absentalerts.Refusals))
	for _, outcome := range absentalerts.Outcomes {
		counts[outcome] = loop.counts[outcome]
	}
	for _, refusal := range absentalerts.Refusals {
		counts[refusal] = loop.counts[refusal]
	}
	return counts
}

// Difference is the round's denominators, for the gauge that reports them.
// A zero close count means one thing beside a difference of zero and
// another beside a round that refused, and these are what tell them apart.
func (loop *absentStrategyClose) Difference() map[string]int {
	loop.countsMu.Lock()
	defer loop.countsMu.Unlock()
	return map[string]int{
		"departed": loop.lastCounts.Departed, "with_open_alerts": loop.lastCounts.WithOpenAlerts,
		"candidates": loop.lastCounts.Candidates, "snapshot_strategies": loop.lastCounts.SnapshotStrategies,
		"published_strategies": loop.lastCounts.PublishedStrategies, "returned": loop.lastCounts.Returned,
		"unreadable_index": loop.lastCounts.UnreadableIndex,
	}
}

func (loop *absentStrategyClose) count(outcome string, n int) {
	if n <= 0 {
		return
	}
	loop.countsMu.Lock()
	loop.counts[outcome] += uint64(n)
	loop.countsMu.Unlock()
}

func (loop *absentStrategyClose) run(ctx context.Context) {
	ticker := time.NewTicker(absentCloseInterval)
	defer ticker.Stop()
	for {
		loop.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (loop *absentStrategyClose) step(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, absentCloseInterval/2)
	defer cancel()
	loop.bundle.mu.RLock()
	leader := loop.bundle.controlLeader && !loop.bundle.draining && !loop.bundle.closed
	loop.bundle.mu.RUnlock()
	if !leader {
		if loop.wasLeader {
			// The term ended. Every candidate's clock restarts, so a replica
			// that becomes leader again cannot close on an absence it
			// observed under a term it no longer holds.
			loop.tracker.Forget()
			loop.previousSnapshot = 0
		}
		loop.wasLeader = false
		loop.count(absentalerts.OutcomeNotLeader, 1)
		return
	}
	loop.wasLeader = true
	now := loop.bundle.dependencies.Now()
	observed, haveSnapshot := loop.reconciler.ObservedSnapshot()
	departed, refusedDepartures := loop.reconciler.DepartedStrategies()
	loop.count(absentalerts.OutcomeMemoryFull, int(refusedDepartures)-int(loop.counts[absentalerts.OutcomeMemoryFull]))
	round := absentalerts.Round{
		Departed: departedByKey(departed), Published: publishedKeys(loop.reconciler.PublishedStrategies()),
		SnapshotStrategies: snapshotKeys(observed), SnapshotUsable: haveSnapshot,
		SnapshotObservation: observed.Observation, PreviousSnapshotStrategies: loop.previousSnapshot,
		Now: now,
	}
	if haveSnapshot {
		round.SnapshotAgeSeconds = int64(now.Sub(observed.ReadAt) / time.Second)
	}
	round.WithOpenAlerts, round.Unreadable = loop.readIndex(ctx, round)
	result := loop.tracker.Round(round, loop.bounds)
	if haveSnapshot && result.Refusal == absentalerts.RefusalNone {
		loop.previousSnapshot = result.Counts.SnapshotStrategies
	}
	loop.record(ctx, result)
	for _, absent := range result.Close {
		if ctx.Err() != nil {
			return
		}
		loop.closeStrategy(ctx, absent, now)
	}
}

// readIndex asks the alert link, one departed strategy at a time, whether
// it still holds an unrecovered alert. Strategies the snapshot lists again
// and strategies the fleet is running are not asked about: they are not
// candidates whatever the answer would be.
func (loop *absentStrategyClose) readIndex(ctx context.Context, round absentalerts.Round) (map[absentalerts.Key]struct{}, map[absentalerts.Key]struct{}) {
	holding := make(map[absentalerts.Key]struct{})
	unreadable := make(map[absentalerts.Key]struct{})
	asked := 0
	for _, key := range sortedKeys(round.Departed) {
		if asked >= absentCloseReadBatch || ctx.Err() != nil {
			return holding, unreadable
		}
		if _, present := round.SnapshotStrategies[key]; present {
			continue
		}
		if _, published := round.Published[key]; published {
			continue
		}
		asked++
		has, err := loop.index.HasOpenAlerts(ctx, openalerts.StrategyKey{TenantID: key.TenantID, StrategyID: key.StrategyID})
		if err != nil {
			unreadable[key] = struct{}{}
			continue
		}
		if has {
			holding[key] = struct{}{}
		}
	}
	return holding, unreadable
}

// closeStrategy reads the strategy's current alerts from the alert link and
// closes the ones this deployment produced.
func (loop *absentStrategyClose) closeStrategy(ctx context.Context, absent absentalerts.Absent, now time.Time) {
	if loop.alerts == nil {
		// Without the reconciliation endpoint there is no authoritative
		// alert metadata, so there is nothing to address a close to. Every
		// round says so rather than reporting a clean zero.
		loop.observe(ctx, absentalerts.OutcomeEvidenceUnavailable, errors.New("alert reconciliation endpoint is not configured"), 1)
		return
	}
	key := openalerts.StrategyKey{TenantID: absent.Key.TenantID, StrategyID: absent.Key.StrategyID}
	reconciliation, err := loop.alerts.Reconcile(ctx, key)
	if err != nil {
		loop.observe(ctx, absentalerts.OutcomeEvidenceUnavailable, err, 1)
		return
	}
	strategyID, err := strconv.ParseInt(absent.Key.StrategyID, 10, 64)
	if err != nil {
		loop.observe(ctx, absentalerts.OutcomeIdentityUnknown, err, 1)
		return
	}
	batch := make([]linkdoutput.CloseRequest, 0, absentCloseAlertBatch)
	foreign, unknown, withoutSeverity := 0, 0, 0
	for _, alert := range reconciliation.Alerts {
		switch {
		case alert.EventSourceID == "":
			unknown++
			continue
		case alert.EventSourceID != loop.sourceID:
			foreign++
			continue
		case alert.Severity == "":
			withoutSeverity++
			continue
		}
		batch = append(batch, linkdoutput.CloseRequest{TenantID: key.TenantID, Fingerprint: alert.Fingerprint,
			AlertInstanceID: alert.AlertID, Severity: alert.Severity, StrategyID: strategyID,
			StrategyRevision: absent.Identity.Revision, BusinessID: absent.Identity.BusinessID,
			OccurredAt: now, Reason: linkdoutput.CloseReasonAbsent})
		if len(batch) == absentCloseAlertBatch {
			break
		}
	}
	loop.count(absentalerts.OutcomeProducerForeign, foreign)
	loop.count(absentalerts.OutcomeProducerUnknown, unknown)
	loop.count(absentalerts.OutcomeMetadataMissing, withoutSeverity)
	if len(batch) == 0 {
		return
	}
	if err := loop.writer.WriteCloseBatch(ctx, batch); err != nil {
		loop.observe(ctx, absentalerts.OutcomeSendFailed, err, len(batch))
		return
	}
	loop.count(absentalerts.OutcomeAlertClosed, len(batch))
}

// record writes the round's line: its refusal or its decision, with every
// denominator on it.
func (loop *absentStrategyClose) record(ctx context.Context, result absentalerts.Result) {
	counts := result.Counts
	loop.countsMu.Lock()
	loop.lastCounts = counts
	loop.counts[result.Refusal]++
	loop.countsMu.Unlock()
	loop.count(absentalerts.OutcomeClosed, counts.Closed)
	loop.count(absentalerts.OutcomeStillPublished, counts.StillPublished)
	loop.count(absentalerts.OutcomeWithinGrace, counts.WithinGrace)
	loop.count(absentalerts.OutcomeUnconfirmed, counts.Unconfirmed)
	loop.count(absentalerts.OutcomeIdentityUnknown, counts.IdentityUnknown)
	loop.count(absentalerts.OutcomeRevisionUnknown, counts.RevisionUnknown)
	loop.count(absentalerts.OutcomeDeferred, counts.Deferred)
	loop.count(absentalerts.OutcomeIndexUnreadable, counts.UnreadableIndex)
	outcome := observability.Result(observability.ResultSuccess)
	if result.Refusal != absentalerts.RefusalNone {
		outcome = observability.ResultDegraded
	}
	loop.bundle.dependencies.Observer.Observe(ctx, observability.Observation{
		Component: observability.ComponentControlPlane, Stage: observability.StageAbsentStrategyClose,
		Result: outcome, ReasonCode: observability.ReasonCode(result.Refusal),
		Counts: observability.Counts{Events: int64(counts.Candidates)},
	})
}

func (loop *absentStrategyClose) observe(ctx context.Context, outcome string, err error, count int) {
	loop.count(outcome, count)
	loop.bundle.dependencies.Observer.Observe(ctx, observability.Observation{
		Component: observability.ComponentControlPlane, Stage: observability.StageAbsentStrategyClose,
		Result: observability.ResultDegraded, ReasonCode: observability.ReasonCode(outcome),
		Counts: observability.Counts{Events: int64(count)}, Err: err})
}

func departedByKey(entries []controlplane.DepartedStrategy) map[absentalerts.Key]absentalerts.Departure {
	departed := make(map[absentalerts.Key]absentalerts.Departure, len(entries))
	for _, entry := range entries {
		departed[absentalerts.Key{TenantID: entry.TenantID, StrategyID: entry.StrategyID}] =
			absentalerts.Departure{Identity: absentalerts.Identity{BusinessID: entry.BusinessID, Revision: entry.Revision}, At: entry.At}
	}
	return departed
}

func publishedKeys(entries []controlplane.DepartedStrategy) map[absentalerts.Key]struct{} {
	keys := make(map[absentalerts.Key]struct{}, len(entries))
	for _, entry := range entries {
		keys[absentalerts.Key{TenantID: entry.TenantID, StrategyID: entry.StrategyID}] = struct{}{}
	}
	return keys
}

func snapshotKeys(observed controlplane.ObservedSnapshot) map[absentalerts.Key]struct{} {
	keys := make(map[absentalerts.Key]struct{}, len(observed.Strategies))
	for _, entry := range observed.Strategies {
		keys[absentalerts.Key{TenantID: entry.TenantID, StrategyID: entry.StrategyID}] = struct{}{}
	}
	return keys
}

// sortedKeys asks about the departed strategies in a fixed order, so that a
// round whose read batch is spent leaves off in the same place on every
// replica rather than wherever map order put it.
func sortedKeys(departed map[absentalerts.Key]absentalerts.Departure) []absentalerts.Key {
	keys := make([]absentalerts.Key, 0, len(departed))
	for key := range departed {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TenantID != keys[j].TenantID {
			return keys[i].TenantID < keys[j].TenantID
		}
		return keys[i].StrategyID < keys[j].StrategyID
	})
	return keys
}
