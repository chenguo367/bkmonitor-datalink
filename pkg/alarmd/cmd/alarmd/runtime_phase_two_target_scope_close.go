package main

import (
	"context"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/activecopy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scopeclose"
)

// targetScopeCloseInterval is how often the close decides. The decision
// needs two Slots of the same strategy, and the shortest evaluation interval
// is a minute; twice a minute keeps a confirmed observation from waiting a
// whole Slot for its close without making the step itself a load.
const targetScopeCloseInterval = 30 * time.Second

// targetScopeCloseLoop runs the close's decisions on this replica. The
// observations come from this replica's own admission step, so every replica
// runs its own loop over what it saw; there is no leader and nothing shared.
type targetScopeCloseLoop struct {
	bundle *phaseTwoWorkerBundle
	closer *scopeclose.Closer
}

func (loop targetScopeCloseLoop) run(ctx context.Context) {
	ticker := time.NewTicker(targetScopeCloseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		loop.bundle.mu.RLock()
		stopping := loop.bundle.draining || loop.bundle.closed
		loop.bundle.mu.RUnlock()
		if stopping {
			continue
		}
		step, cancel := context.WithTimeout(ctx, targetScopeCloseInterval/2)
		loop.closer.Step(step)
		cancel()
	}
}

// targetScopeCloseFor builds the close and the sink the admission step
// reports to, only for a deployment with the link's control plane. Without
// it there is no copy to judge against and no filter to close through, so
// the close could never act; wiring it anyway would have every rejection of
// every Plan screened and counted for nothing. The sink is then a nil
// interface, which the access path reads as "nobody listens".
func targetScopeCloseFor(cfg config.Config, now func() time.Time) (*scopeclose.Closer, access.ScopeDropSink) {
	if !cfg.PhaseTwo.Linkd.ControlPlane.Configured() {
		return nil, nil
	}
	budget := config.DeriveLinkdCapacity(config.DetectCapacityInputs())
	closer := scopeclose.New(scopeclose.Options{Now: now,
		MaxEntries: budget.LocalEntries, Batch: budget.CloseBatch})
	return closer, scopeDropSink{closer: closer}
}

// bindTargetScopeClose binds the close to the copy and the link's filter,
// its counts to the metric and its loop to the bundle. Nothing is wired for
// a nil close.
func bindTargetScopeClose(bundle *phaseTwoWorkerBundle, closer *scopeclose.Closer, copy *activecopy.Copy, route *filterRoute,
	recorder *metric.Recorder) {
	if closer == nil {
		return
	}
	var set scopeclose.OpenSet
	if copy != nil {
		set = copy
	}
	if route != nil {
		closer.Bind(set, route.closer, route.source)
	} else {
		closer.Bind(set, nil, "")
	}
	recorder.SetTargetScopeCloseSource(closer.Stats)
	bundle.targetScopeClose = closer
	bundle.dependencies.RunTargetScopeClose = targetScopeCloseLoop{bundle: bundle, closer: closer}.run
}

// scopeDropSink hands the admission step's target rejections to the close:
// the per-query screen, the one-by-one observations of the strategies it
// cleared, and the bulk counts, each word mapped to the close's outcome.
type scopeDropSink struct{ closer *scopeclose.Closer }

func (sink scopeDropSink) Screen(plan execution.PlanIdentity) string {
	return sink.closer.Screen(scopeclose.Key{TenantID: plan.TenantID, StrategyID: plan.StrategyID})
}

func (sink scopeDropSink) Observe(drop access.ScopeDrop) {
	sink.closer.Observe(scopeclose.Drop{TenantID: drop.Plan.TenantID, StrategyID: drop.Plan.StrategyID, Fingerprint: drop.Fingerprint,
		Target: drop.Target, StrategyRevision: drop.StrategyRevision, Round: drop.Round, IntervalSeconds: drop.IntervalSeconds,
		ReadOffset: drop.ReadOffset, CloseReason: drop.CloseReason})
}

func (sink scopeDropSink) Count(reporter access.ScopeDropReporter, word string, n int) {
	plan := reporter.Plan
	outcome := word
	switch word {
	case access.ScopeDropIndefinite:
		outcome = scopeclose.OutcomeIndefinite
	case access.ScopeDropCacheUnavailable:
		outcome = scopeclose.OutcomeCacheUnavailable
	case access.ScopeDropFingerprintUnsupported:
		outcome = scopeclose.OutcomeFingerprintUnsupported
	case access.ScopeDropNoFingerprint:
		outcome = scopeclose.OutcomeNotMember
	}
	sink.closer.Count(scopeclose.Key{TenantID: plan.TenantID, StrategyID: plan.StrategyID},
		reporter.Instance(), int64(reporter.Slot.EvaluationTime), outcome, n)
}

// withTargetScopeClose puts the close's reading beside the copy it judges
// from, in the replica's facts and so in fleet.get and the health view.
func withTargetScopeClose(source func() *fleet.ActiveCopyFacts, closer *scopeclose.Closer) func() *fleet.ActiveCopyFacts {
	return func() *fleet.ActiveCopyFacts {
		facts := source()
		if facts != nil && closer != nil {
			facts.TargetScopeClose = targetScopeCloseFacts(closer.Facts())
		}
		return facts
	}
}

// targetScopeCloseFacts carries the close's facts field for field.
func targetScopeCloseFacts(facts scopeclose.Facts) *fleet.TargetScopeCloseFacts {
	result := &fleet.TargetScopeCloseFacts{Pending: facts.Pending, Confirmed: facts.Confirmed,
		MaxEntries: facts.MaxEntries, Outcomes: facts.Outcomes}
	for _, row := range facts.Strategies {
		result.Strategies = append(result.Strategies, fleet.TargetScopeCloseStrategy{TenantID: row.TenantID,
			StrategyID: row.StrategyID, Pending: row.Pending, Confirmed: row.Confirmed, Outcomes: row.Outcomes,
			PendingSample: row.PendingSample, DecidedSample: row.DecidedSample})
	}
	return result
}
