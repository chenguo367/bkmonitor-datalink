package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// Maintenance uses the existing per-QG flight and lease, not a second owner or
// an outbox. It never waits behind detection, and close batches stay small.
func (runtime *productionPhaseTwoQueryGroup) withMaintenance(ctx context.Context, run func(context.Context, func(context.Context) error) error) error {
	// Loading external facts never occupies a detection flight. Only the
	// bounded close write needs exclusion from this owner's in-flight Slot.
	var release func()
	initialLease, accepting := runtime.session.Current()
	if !accepting {
		return errors.New("maintenance owner is not accepting")
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	check := func(ctx context.Context) error {
		if release == nil {
			var ok bool
			release, ok = runtime.flights.TryMaintenance(runtime.queryGroup)
			if !ok {
				return errMaintenanceBusy
			}
		}
		_, current, err := runtime.session.ValidateCurrentWithAssignment(ctx, runtime.now())
		if err == nil && (current.ContentScope != initialLease.ContentScope || current.TimelineRecordRevision != initialLease.TimelineRecordRevision) {
			return errors.New("effective-time content changed before close")
		}
		return err
	}
	if _, err := runtime.session.ValidateCurrent(ctx, runtime.now()); err != nil {
		return err
	}
	if runtime.viewGate != nil {
		var err error
		ctx, err = gateContext(ctx, runtime.viewGate, runtime.queryGroup, runtime.session, nil)
		if err != nil {
			return err
		}
	}
	return run(execution.ContextWithLeaseAuthority(ctx, runtime.session), check)
}

var errMaintenanceBusy = errors.New("detection is executing this Query Group")

type maintenanceCatalog interface {
	CurrentPlans(context.Context, execution.QueryGroupIdentity, execution.EvaluationTime) ([]controlplane.MaintenancePlan, error)
}
type closeWriter interface {
	WriteCloseBatch(context.Context, []linkdoutput.CloseRequest) error
}
type maintenanceRunner interface {
	withMaintenance(context.Context, func(context.Context, func(context.Context) error) error) error
}

type legacyRefresher interface {
	Refresh(context.Context, string, string, []int64) error
}

type effectiveMaintenance struct {
	trackingMu           sync.Mutex
	bundle               *phaseTwoWorkerBundle
	catalog              maintenanceCatalog
	cache                *openalerts.Cache
	writer               closeWriter
	legacy               strategy.EffectiveTimeProvider
	legacyCache          legacyRefresher
	capacity             config.LinkdCapacity
	sourceID             string
	byGroup              map[execution.QueryGroupIdentity][]openalerts.StrategyKey
	refs                 map[openalerts.StrategyKey]int
	calibrationRequested map[openalerts.StrategyKey]time.Time
	// ACK suppresses only immediate repeat sends. It never deletes the
	// external member or declares Linkd closed; reconciliation confirms that.
	sent             map[string]time.Time
	cursor           execution.QueryGroupIdentity
	planCursor       map[execution.QueryGroupIdentity]int
	legacyCursor     map[execution.QueryGroupIdentity]int
	metadataReported map[openalerts.StrategyKey]time.Time
}

func (m *effectiveMaintenance) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		m.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *effectiveMaintenance) step(ctx context.Context) {
	ctx, cancelStep := context.WithTimeout(ctx, 5*time.Second)
	defer cancelStep()
	m.bundle.mu.RLock()
	owned := make(map[execution.QueryGroupIdentity]phaseTwoQueryGroupRuntime, len(m.bundle.runners))
	if !m.bundle.draining && !m.bundle.closed {
		for qg, lifecycle := range m.bundle.runners {
			owned[qg] = lifecycle.runner
		}
	}
	m.trackingMu.Lock()
	if m.byGroup == nil {
		m.byGroup = make(map[execution.QueryGroupIdentity][]openalerts.StrategyKey)
	}
	if m.refs == nil {
		m.refs = make(map[openalerts.StrategyKey]int)
	}
	if m.calibrationRequested == nil {
		m.calibrationRequested = make(map[openalerts.StrategyKey]time.Time)
	}
	if m.planCursor == nil {
		m.planCursor = make(map[execution.QueryGroupIdentity]int)
		m.legacyCursor = make(map[execution.QueryGroupIdentity]int)
		m.metadataReported = make(map[openalerts.StrategyKey]time.Time)
	}
	if m.sent == nil {
		m.sent = make(map[string]time.Time)
	}
	for qg := range m.byGroup {
		if _, keep := owned[qg]; !keep {
			m.releaseGroupLocked(qg)
		}
	}
	for key := range m.calibrationRequested {
		if m.refs[key] == 0 {
			delete(m.calibrationRequested, key)
			delete(m.metadataReported, key)
		}
	}
	m.trackingMu.Unlock()
	m.bundle.mu.RUnlock()

	now := m.bundle.dependencies.Now()
	for key, at := range m.sent {
		if now.Sub(at) >= time.Minute {
			delete(m.sent, key)
		}
	}
	groups := make([]execution.QueryGroupIdentity, 0, len(owned))
	for qg := range owned {
		groups = append(groups, qg)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	start := sort.Search(len(groups), func(i int) bool { return groups[i] > m.cursor })
	for n := 0; n < min(m.capacity.GroupBatch, len(groups)); n++ {
		if ctx.Err() != nil {
			return
		}
		qg := groups[(start+n)%len(groups)]
		m.cursor = qg
		runner, ok := owned[qg].(maintenanceRunner)
		if !ok {
			m.observe(ctx, qg, "unsupported_runner", errors.New("owner runner lacks maintenance capability"), 0)
			continue
		}
		round, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := runner.withMaintenance(round, func(round context.Context, check func(context.Context) error) error { return m.group(round, qg, check) })
		cancel()
		if err != nil && !errors.Is(err, errMaintenanceBusy) {
			m.observe(ctx, qg, "unavailable", err, 0)
		}
	}
}

func (m *effectiveMaintenance) group(ctx context.Context, qg execution.QueryGroupIdentity, check func(context.Context) error) error {
	at := m.bundle.dependencies.Now()
	plans, err := m.catalog.CurrentPlans(ctx, qg, execution.EvaluationTime(at.Unix()))
	if err != nil {
		return err
	} // Keep known tracking on transient reads.
	keys := make([]openalerts.StrategyKey, 0, len(plans))
	for _, plan := range plans {
		if plan.Compiled.WireFormat() == contract.WireFormatStandardRawEvent {
			keys = append(keys, openalerts.StrategyKey{TenantID: plan.Identity.TenantID, StrategyID: plan.Identity.StrategyID})
		}
	}
	if err := m.registerKeys(qg, keys, true); err != nil {
		return err
	}

	// At most one bounded legacy refresh per group per round. Modern snapshot
	// plans must still make progress when a legacy dependency is slow.
	m.refreshLegacy(ctx, qg, plans)
	start := m.planCursor[qg]
	remaining := m.capacity.CloseBatch
	for n := 0; n < len(plans); n++ {
		i := (start + n) % len(plans)
		plan := plans[i]
		m.planCursor[qg] = (i + 1) % len(plans)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if plan.CloseUnavailable || plan.Compiled.WireFormat() != contract.WireFormatStandardRawEvent {
			continue
		}
		fact, err := plan.Compiled.ResolveEffectiveTimeWithProvider(ctx, at.Unix(), plan.Identity.BusinessID, m.legacy)
		if err != nil {
			m.observe(ctx, qg, "effective_time_unknown", err, 0)
			continue
		}
		if fact.Status() != strategy.EffectiveTimeInactive {
			continue
		}
		key := openalerts.StrategyKey{TenantID: plan.Identity.TenantID, StrategyID: plan.Identity.StrategyID}
		alerts := m.cache.ActiveAlerts(key)
		known := make(map[string]bool, len(alerts))
		for _, alert := range alerts {
			known[alert.Fingerprint] = true
		}
		for _, member := range m.cache.Members(key) {
			if !known[member] {
				m.requestCalibration(key, at)
				break
			}
		}
		if len(alerts) == 0 {
			continue
		}
		business, err := strconv.ParseInt(plan.Identity.BusinessID, 10, 64)
		if err != nil {
			m.observe(ctx, qg, "close_identity_invalid", err, 0)
			continue
		}
		ref := plan.Compiled.StrategyRef()
		strategyID, err := strconv.ParseInt(ref.StrategyID, 10, 64)
		if err != nil {
			m.observe(ctx, qg, "close_identity_invalid", err, 0)
			continue
		}
		batch := make([]linkdoutput.CloseRequest, 0, m.capacity.CloseBatch)
		for _, alert := range alerts {
			if alert.EventSourceID != m.sourceID {
				continue
			}
			if alert.Severity == "" {
				if at.Sub(m.metadataReported[key]) >= time.Minute {
					m.metadataReported[key] = at
					m.observe(ctx, qg, "close_metadata_missing", errors.New("Linkd reconciliation does not expose active severity"), 0)
				}
				continue
			}
			sentKey := key.TenantID + "\x00" + alert.EventSourceID + "\x00" + alert.AlertID
			if _, sent := m.sent[sentKey]; sent {
				continue
			}
			batch = append(batch, linkdoutput.CloseRequest{TenantID: key.TenantID, Fingerprint: alert.Fingerprint, AlertInstanceID: alert.AlertID, Severity: alert.Severity,
				StrategyID: strategyID, StrategyRevision: ref.SnapshotRevision, BusinessID: business, OccurredAt: at})
			if len(batch) == remaining {
				break
			}
		}
		if len(batch) == 0 {
			continue
		}
		// Recheck current ownership and rules immediately before side effects;
		// never retry an old inactive judgment across an active boundary.
		if err := check(ctx); err != nil {
			return err
		}
		now := m.bundle.dependencies.Now()
		fact, err = plan.Compiled.ResolveEffectiveTimeWithProvider(ctx, now.Unix(), plan.Identity.BusinessID, m.legacy)
		if err != nil || fact.Status() != strategy.EffectiveTimeInactive {
			continue
		}
		// Sarama's synchronous ACK wait follows the existing producer timeout;
		// a context deadline cannot cancel a message already handed to Kafka.
		if err := m.writer.WriteCloseBatch(ctx, batch); err != nil {
			return fmt.Errorf("inactive close: %w", err)
		}
		for _, request := range batch {
			if len(m.sent) < m.capacity.LocalEntries {
				m.sent[key.TenantID+"\x00"+m.sourceID+"\x00"+request.AlertInstanceID] = now
			}
		}
		m.requestCalibration(key, now)
		m.observe(ctx, qg, "close_acked", nil, len(batch))
		remaining -= len(batch)
		if remaining == 0 {
			break
		}
	}
	return nil
}

func (m *effectiveMaintenance) refreshLegacy(ctx context.Context, qg execution.QueryGroupIdentity, plans []controlplane.MaintenancePlan) {
	if m.legacyCache == nil || len(plans) == 0 {
		return
	}
	start := m.legacyCursor[qg]
	for n := 0; n < len(plans); n++ {
		i := (start + n) % len(plans)
		plan := plans[i]
		if plan.Compiled.HasEffectiveTimeSnapshot() {
			continue
		}
		levels := plan.Compiled.Levels()
		if level := plan.Compiled.NoDataLevel(); level != nil {
			levels = append(levels, *level)
		}
		needed := false
		var ids []int64
		for _, level := range levels {
			r := level.EffectiveTimeRequirement()
			if r.Kind() != strategy.EffectiveTimeAlways {
				needed = true
			}
			ids = append(ids, r.ActiveCalendarIDs()...)
			ids = append(ids, r.InactiveCalendarIDs()...)
		}
		if !needed {
			continue
		}
		m.legacyCursor[qg] = (i + 1) % len(plans)
		round, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		err := m.legacyCache.Refresh(round, plan.Identity.TenantID, plan.Identity.BusinessID, ids)
		cancel()
		if err != nil {
			m.observe(ctx, qg, "legacy_effective_time_unavailable", err, 0)
		}
		return
	}
}

// registerExecutedPlans uses the same ownership registry as background
// maintenance. It is a memory-only registration before the first evaluation,
// including a new Plan added to an already owned QG, so its first ACK survives.
func (m *effectiveMaintenance) registerExecutedPlans(qg execution.QueryGroupIdentity, plans []execution.PlanIdentity) {
	keys := make([]openalerts.StrategyKey, 0, len(plans))
	for _, p := range plans {
		keys = append(keys, openalerts.StrategyKey{TenantID: p.TenantID, StrategyID: p.StrategyID})
	}
	if err := m.registerKeys(qg, keys, false); err != nil {
		m.observe(context.Background(), qg, "unavailable", err, 0)
	}
}
func (m *effectiveMaintenance) registerKeys(qg execution.QueryGroupIdentity, keys []openalerts.StrategyKey, replace bool) error {
	m.trackingMu.Lock()
	defer m.trackingMu.Unlock()
	if m.byGroup == nil {
		m.byGroup = make(map[execution.QueryGroupIdentity][]openalerts.StrategyKey)
		m.refs = make(map[openalerts.StrategyKey]int)
	}
	old := m.byGroup[qg]
	if !replace {
		merged := old
		for _, key := range keys {
			found := false
			for _, v := range merged {
				if v == key {
					found = true
					break
				}
			}
			if !found {
				merged = append(merged, key)
			}
		}
		keys = merged
	}
	if sameStrategyKeys(old, keys) {
		return nil
	}
	if err := m.cache.TrackOwned(keys...); err != nil {
		return err
	}
	for _, key := range keys {
		m.refs[key]++
	}
	for _, key := range old {
		m.refs[key]--
		if m.refs[key] == 0 {
			delete(m.refs, key)
			m.cache.Untrack(key)
		}
	}
	m.byGroup[qg] = append([]openalerts.StrategyKey(nil), keys...)
	return nil
}

func sameStrategyKeys(a, b []openalerts.StrategyKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (m *effectiveMaintenance) releaseGroupLocked(qg execution.QueryGroupIdentity) {
	for _, key := range m.byGroup[qg] {
		m.refs[key]--
		if m.refs[key] <= 0 {
			delete(m.refs, key)

			m.cache.Untrack(key)
		}
	}
	delete(m.byGroup, qg)
	delete(m.planCursor, qg)
	delete(m.legacyCursor, qg)
}

func (m *effectiveMaintenance) requestCalibration(key openalerts.StrategyKey, at time.Time) {
	if at.Sub(m.calibrationRequested[key]) < time.Minute {
		return
	}
	m.calibrationRequested[key] = at
	m.cache.RequestReconcile(key)
}

func (m *effectiveMaintenance) observe(ctx context.Context, qg execution.QueryGroupIdentity, reason string, err error, count int) {
	result := observability.ResultSuccess
	if err != nil {
		result = observability.ResultDegraded
	}
	m.bundle.dependencies.Observer.Observe(ctx, observability.Observation{Component: observability.ComponentRuntime, Stage: observability.StageEffectiveTimeMaintenance, Result: observability.Result(result),
		ReasonCode: observability.ReasonCode(reason), Trace: observability.TraceFields{QueryGroupKey: string(qg)}, Counts: observability.Counts{Events: int64(count)}, Err: err})
}
