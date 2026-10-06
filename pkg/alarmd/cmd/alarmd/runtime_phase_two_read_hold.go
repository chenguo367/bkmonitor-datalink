package main

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// Read holds share the ownership store and schedule timeline. Nothing here
// changes a Plan's state generation or the window its Slot reads.
type productionReadHolds struct {
	controller  *readhold.Controller
	cfg         config.Config
	repository  *controlplane.RedisCatalogRepository
	catalog     *controlplane.RedisCatalogRuntime
	progress    productionPhaseTwoProgressReader
	now         func() time.Time
	logger      *observability.Logger
	mu          sync.Mutex
	groups      map[execution.QueryGroupIdentity]*productionReadHoldGroup
	nextRenew   time.Time
	transitions atomic.Uint64
	overtaken   atomic.Uint64
	// links counts the predecessor links a prepare skipped, by why
	// (self_link, invalid_link, expired); retireCloseFailed the retired
	// groups whose closing failed and that retired all the same.
	linksMu           sync.Mutex
	links             map[string]uint64
	retireCloseFailed atomic.Uint64
}

// readHoldLinkReasons are the links a prepare skips before asking the
// controller: one naming its own group or with a shape no cutover writes
// (ReadHoldPredecessors), and one past its lifetime.
var readHoldLinkReasons = []string{"self_link", "invalid_link", "expired"}

type productionReadHoldGroup struct {
	mu             sync.Mutex
	session        *ownership.Session
	prepared       execution.ScheduleSegmentFact
	lastTransition execution.EvaluationTime
	predecessors   []execution.QueryGroupIdentity
	queryRoute     string
	queryDelay     time.Duration
}

func newProductionReadHolds(cfg config.Config, control readhold.Control, repository *controlplane.RedisCatalogRepository,
	catalog *controlplane.RedisCatalogRuntime, progress productionPhaseTwoProgressReader, now func() time.Time,
	logger *observability.Logger) (*productionReadHolds, error) {
	holds := &productionReadHolds{cfg: cfg, repository: repository, catalog: catalog, progress: progress,
		now: now, logger: logger, groups: make(map[execution.QueryGroupIdentity]*productionReadHoldGroup), links: make(map[string]uint64)}
	var err error
	holds.controller, err = readhold.NewController(readhold.Options{Control: control,
		Prefix: productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "schedule"), MaxHold: cfg.PhaseTwo.Scheduler.MaxReplayAge.Duration(),
		Now: now, Owner: holds.owner})
	return holds, err
}

func (holds *productionReadHolds) owner(qg execution.QueryGroupIdentity) (readhold.Owner, error) {
	holds.mu.Lock()
	group := holds.groups[qg]
	var session *ownership.Session
	if group != nil {
		session = group.session
	}
	holds.mu.Unlock()
	lease, accepting := session.Current()
	if !accepting || !lease.Deadline.After(holds.now()) {
		return readhold.Owner{}, ownership.ErrStaleFence
	}
	return readhold.Owner{Fence: lease.Fence, ContentScope: lease.ContentScope}, nil
}

func (holds *productionReadHolds) bind(qg execution.QueryGroupIdentity, session *ownership.Session) {
	holds.mu.Lock()
	defer holds.mu.Unlock()
	holds.groups[qg] = &productionReadHoldGroup{session: session}
}

func (holds *productionReadHolds) forget(qg execution.QueryGroupIdentity) {
	holds.mu.Lock()
	delete(holds.groups, qg)
	holds.mu.Unlock()
	holds.controller.Forget(qg)
}

// An inherited bridge is already in the owned fenced record. Foreign
// snapshots are only needed while seeding and have no runner to release them.
func (holds *productionReadHolds) releasePredecessors(group *productionReadHoldGroup) {
	holds.mu.Lock()
	defer holds.mu.Unlock()
	for _, qg := range group.predecessors {
		if holds.groups[qg] == nil {
			holds.controller.Forget(qg)
		}
	}
	group.predecessors = nil
}

// Restore only new or invalidated entries. One bad answer leaves that group
// retryable without discarding successfully loaded siblings.
func (holds *productionReadHolds) restore(ctx context.Context, groups []execution.QueryGroupIdentity) {
	var missing []execution.QueryGroupIdentity
	for _, qg := range groups {
		if !holds.controller.Inspect(qg).Loaded {
			missing = append(missing, qg)
		}
	}
	if len(missing) > 0 {
		holds.report("restore_failed", "", holds.controller.RestoreBatch(ctx, missing))
	}
}

// Cache only the immutable source route and delay, not another query body.
func (holds *productionReadHolds) queryBasis(ctx context.Context, schedule execution.FrozenQueryGroupSchedule) (string, time.Duration, error) {
	qg := schedule.Segment.QueryGroup
	holds.mu.Lock()
	owned := holds.groups[qg]
	var route string
	var delay time.Duration
	if owned != nil {
		route, delay = owned.queryRoute, owned.queryDelay
	}
	holds.mu.Unlock()
	if route != "" {
		return route, delay, nil
	}
	group, err := holds.repository.LoadObservedSegmentQueryGroup(ctx, schedule.Segment, schedule.Segment.Start)
	if errors.Is(err, controlplane.ErrCatalogObjectUnavailable) {
		group, err = holds.repository.LoadQueryGroup(ctx, schedule.Segment.Publication.SnapshotRevision, qg)
	}
	if (errors.Is(err, controlplane.ErrSnapshotUnavailable) || errors.Is(err, controlplane.ErrCatalogObjectUnavailable)) && schedule.Segment.End != nil {
		// Only the query's identity is reused. The original schedule below still
		// supplies the old Plans, their waits and full completion deadlines.
		if successor, nextErr := holds.catalog.ReadSuccessorFrozenSchedule(ctx, qg, *schedule.Segment.End); nextErr == nil {
			return holds.queryBasis(ctx, successor)
		}
	}
	if err != nil {
		return "", 0, err
	}
	route, err = controlplane.ReadHoldRoute(group.QueryPlan)
	if err != nil {
		return "", 0, err
	}
	delay = time.Duration(group.QueryPlan.QueryDelaySeconds) * time.Second
	holds.mu.Lock()
	if owned != nil && holds.groups[qg] == owned {
		owned.queryRoute, owned.queryDelay = route, delay
	}
	holds.mu.Unlock()
	return route, delay, nil
}

func (holds *productionReadHolds) spec(ctx context.Context, schedule execution.FrozenQueryGroupSchedule) (readhold.GroupSpec, error) {
	route, delay, err := holds.queryBasis(ctx, schedule)
	if err != nil {
		return readhold.GroupSpec{}, err
	}
	spec := readhold.GroupSpec{QueryGroup: schedule.Segment.QueryGroup, Delay: delay, HoldLimit: holds.cfg.PhaseTwo.Scheduler.MaxReplayAge.Duration()}
	for _, plan := range schedule.Plans {
		spec.Plans = append(spec.Plans, readhold.PlanRef{Key: plan.Key(), Route: route})
		offset := time.Duration(plan.Spec.CompletionOffsetSeconds()) * time.Second
		step := time.Duration(plan.Spec.EvaluationIntervalSeconds) * time.Second
		if len(spec.Plans) == 1 || step < spec.Step {
			spec.Step = step
		}
		spec.MaxCompletionOffset = max(spec.MaxCompletionOffset, offset)
		wait := execution.SettlingWaitWithinQueryBudget(offset-holds.cfg.PhaseTwo.Access.DownstreamExecutionReserve.Duration(), holds.cfg.PhaseTwo.Access.MinReadyDelay.Duration())
		if len(spec.Plans) == 1 || wait < spec.SettlingWait {
			spec.SettlingWait = wait
		}
		margin := phaseTwoObjectRetentionLimit(holds.cfg) - phaseTwoSnapshotMinimumRetention(holds.cfg, offset)
		spec.HoldLimit = min(spec.HoldLimit, max(margin, 0))
	}
	return spec, nil
}

// PrepareSchedule is also called for unfinished Slots: their h is reused,
// but a closed segment still must fix its Plan bridge before the successor.
func (holds *productionReadHolds) PrepareSchedule(ctx context.Context, schedule execution.FrozenQueryGroupSchedule, fence execution.OwnerFence) (prepareErr error) {
	qg := schedule.Segment.QueryGroup
	holds.mu.Lock()
	group := holds.groups[qg]
	holds.mu.Unlock()
	if group == nil {
		return ownership.ErrStaleFence
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if lease, accepting := group.session.Current(); accepting && lease.TimelineRecordRevision > 0 {
		ctx = controlplane.WithTimelineRevisionHint(ctx, lease.TimelineRecordRevision)
	}
	defer func() {
		if prepareErr != nil {
			holds.releasePredecessors(group)
		}
	}()
	holds.restore(ctx, []execution.QueryGroupIdentity{qg})
	if !holds.controller.Inspect(qg).Loaded {
		return readhold.ErrNotRestored
	}
	if reflect.DeepEqual(group.prepared, schedule.Segment) {
		return nil
	}
	// Close our previous segment first, even when the QG still carries other
	// Plans. A cutover may reach this path without a final old-Slot execution.
	if schedule.Segment.Start > 1 && schedule.Segment.Start > group.prepared.Start {
		old, err := holds.catalog.ReadFrozenSchedule(ctx, qg, schedule.Segment.Start-1)
		if err == nil && old.Segment.End != nil {
			oldSpec, err := holds.spec(ctx, old)
			if err != nil {
				return err
			}
			if err := holds.controller.Configure(oldSpec); err != nil {
				return err
			}
			if err := holds.controller.CloseSchedule(ctx, old, fence); err != nil {
				return err
			}
		} else if err != nil && !errors.Is(err, controlplane.ErrScheduleUnavailable) {
			return err
		}
	}
	// Late observations of an older closed segment cannot reconfigure a
	// newer live group with the old segment's delay or Plans.
	if schedule.Segment.Start < group.prepared.Start {
		if schedule.Segment.End != nil {
			return holds.controller.CloseSchedule(ctx, schedule, fence)
		}
		return readhold.ErrSegmentStale
	}
	spec, err := holds.spec(ctx, schedule)
	if err != nil {
		return err
	}
	links, skipped, err := holds.repository.ReadHoldPredecessors(ctx, schedule)
	if err != nil {
		return err
	}
	holds.countLinks(skipped)
	// Nothing about a predecessor can stop this group: every fact it cannot
	// read is read as the hold bound by the controller (previousHold), which
	// costs this group's first few Slots a later read and no Slot.
	expired := execution.EvaluationTime(holds.now().Add(-controlplane.ReadHoldLinkLifetime).Unix())
	record := holds.controller.Inspect(qg).Record
	for _, link := range links {
		if link.ClosedAt < expired {
			holds.countLinks(map[string]int{"expired": 1})
			continue
		}
		wanted := holds.linkedPlans(spec, link)
		if len(wanted) == 0 || inheritedLinks(record, link, wanted) {
			// Once the fenced record carries this bridge, the old group's
			// facts may expire normally. A new owner restores the bridge.
			continue
		}
		group.predecessors = append(group.predecessors, link.QueryGroup)
		// A read that fails, or a record that does not decode, leaves the
		// Inspection saying so; neither is this group's error.
		holds.report("predecessor_unreadable", link.QueryGroup, holds.controller.RestoreBatch(ctx, []execution.QueryGroupIdentity{link.QueryGroup}))
		if !closedFor(holds.controller.Inspect(link.QueryGroup), link.ClosedAt, wanted) {
			holds.predecessorProgress(ctx, link.QueryGroup, wanted)
		}
		spec.Previous = append(spec.Previous, readhold.Previous{QueryGroup: link.QueryGroup, ClosedAt: link.ClosedAt, Links: wanted})
	}
	if err := holds.controller.Configure(spec); err != nil {
		return err
	}
	if schedule.Segment.End != nil {
		if err := holds.controller.CloseSchedule(ctx, schedule, fence); err != nil {
			return err
		}
		holds.releasePredecessors(group)
	}
	group.prepared = schedule.Segment
	return nil
}

func (holds *productionReadHolds) countLinks(skipped map[string]int) {
	if len(skipped) == 0 {
		return
	}
	holds.linksMu.Lock()
	for reason, count := range skipped {
		holds.links[reason] += uint64(count)
	}
	holds.linksMu.Unlock()
}

// linkedPlans is a link's Plans this Segment runs, with what the link says
// of each. A link for a Plan the Segment no longer runs carries nothing.
func (holds *productionReadHolds) linkedPlans(spec readhold.GroupSpec, link controlplane.ReadHoldPredecessor) []readhold.PlanLink {
	var wanted []readhold.PlanLink
	for _, plan := range link.Plans {
		for _, ref := range spec.Plans {
			if ref.Key == plan.Key {
				wanted = append(wanted, readhold.PlanLink{PlanRef: ref, PreviousSlot: plan.PreviousSlot,
					CompletionOffsetMillis: plan.CompletionOffsetMillis, SameRoute: plan.SameRoute})
			}
		}
	}
	return wanted
}

// inheritedLinks is the group's own record carrying the bridge of every
// one of the link's Plans already.
func inheritedLinks(record readhold.Record, link controlplane.ReadHoldPredecessor, wanted []readhold.PlanLink) bool {
	for _, want := range wanted {
		found := false
		for _, plan := range record.Plans {
			found = found || (plan.PlanRef == want.PlanRef && plan.InheritedQueryGroup == link.QueryGroup && plan.InheritedClosedAt == link.ClosedAt)
		}
		if !found {
			return false
		}
	}
	return true
}

// closedFor is a predecessor's record holding the closing fact of every
// one of the Plans at the boundary: the only case its Progress adds nothing.
func closedFor(old readhold.Inspection, closedAt execution.EvaluationTime, wanted []readhold.PlanLink) bool {
	if !old.Loaded || old.Corrupt || old.Missing {
		return false
	}
	for _, want := range wanted {
		found := false
		for _, plan := range old.Record.Plans {
			found = found || (plan.Key == want.Key && plan.ClosedAt == closedAt)
		}
		if !found {
			return false
		}
	}
	return true
}

// predecessorProgress adds to each link what the predecessor's Progress says
// of the link's last Slot: the hold it was frozen with, while the Progress
// still carries its contract, and whether every Slot through it is frozen.
// A Progress that cannot be read says neither, and the controller reads the
// hold bound instead.
func (holds *productionReadHolds) predecessorProgress(ctx context.Context, qg execution.QueryGroupIdentity, links []readhold.PlanLink) {
	identity := execution.ProgressIdentity{QueryGroup: qg}
	loaded, err := holds.progress.LoadProgress(ctx, identity)
	if err != nil || loaded.Validate(identity) != nil || loaded.Progress == nil {
		holds.report("predecessor_progress_unreadable", qg, err)
		return
	}
	progress := loaded.Progress
	for index := range links {
		slot := links[index].PreviousSlot
		if slot <= 0 {
			continue
		}
		var frozen *execution.FrozenExecutionContractRef
		switch {
		case progress.UnfinishedSlot != nil && progress.UnfinishedSlot.Contract.Slot.EvaluationTime == slot:
			frozen = &progress.UnfinishedSlot.Contract
		case progress.LastCompletion != nil && progress.LastCompletion.Slot == slot:
			frozen = &progress.LastCompletion.Contract
		}
		if frozen != nil {
			hold := frozen.ReadHoldMillis
			links[index].FrozenHoldMillis = &hold
		}
		links[index].MovedPast = progress.NextSlot > slot
	}
}

func (holds *productionReadHolds) ReadHold(qg execution.QueryGroupIdentity) time.Duration {
	return holds.controller.ReadHold(qg)
}

func (holds *productionReadHolds) SlotReadHold(ctx context.Context, schedule execution.FrozenQueryGroupSchedule, at execution.EvaluationTime, fence execution.OwnerFence) (time.Duration, error) {
	if err := holds.PrepareSchedule(ctx, schedule, fence); err != nil {
		return 0, err
	}
	hold, err := holds.controller.SlotReadHold(ctx, schedule, at, fence)
	holds.mu.Lock()
	currentGroup := holds.groups[schedule.Segment.QueryGroup]
	holds.mu.Unlock()
	if currentGroup != nil {
		currentGroup.mu.Lock()
		holds.releasePredecessors(currentGroup)
		currentGroup.mu.Unlock()
	}
	if err == nil && hold > holds.controller.ReadHold(schedule.Segment.QueryGroup) {
		holds.mu.Lock()
		group := holds.groups[schedule.Segment.QueryGroup]
		holds.mu.Unlock()
		if group != nil {
			group.mu.Lock()
			if group.lastTransition != at {
				holds.transitions.Add(1)
				group.lastTransition = at
			}
			group.mu.Unlock()
		}
	}
	if errors.Is(err, readhold.ErrNotRestored) {
		holds.mu.Lock()
		group := holds.groups[schedule.Segment.QueryGroup]
		holds.mu.Unlock()
		if group != nil {
			group.mu.Lock()
			group.prepared = execution.ScheduleSegmentFact{}
			group.mu.Unlock()
		}
	}
	return hold, err
}

// RetireCloseFailed counts a retired group that retired without closing.
func (holds *productionReadHolds) RetireCloseFailed(err error) {
	holds.retireCloseFailed.Add(1)
	holds.report("retire_close_failed", "", err)
}

func (holds *productionReadHolds) report(reason string, qg execution.QueryGroupIdentity, err error) {
	if err != nil && holds.logger != nil {
		holds.logger.Warn("read_hold", reason, 0, 0, slog.String("query_group", string(qg)), slog.String("error", err.Error()))
	}
}

func (holds *productionReadHolds) observation(contractRef execution.FrozenExecutionContractRef, apply func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), directoryReadTimeout)
	defer cancel()
	owner, err := holds.owner(contractRef.Slot.QueryGroup)
	if err == nil {
		var schedule execution.FrozenQueryGroupSchedule
		schedule, err = holds.catalog.ReadFrozenSchedule(ctx, contractRef.Slot.QueryGroup, contractRef.Slot.EvaluationTime)
		if err == nil {
			err = holds.PrepareSchedule(ctx, schedule, owner.Fence)
		}
		if err == nil {
			err = apply(ctx)
		}
	}
	holds.report("observation_failed", contractRef.Slot.QueryGroup, err)
}

func (holds *productionReadHolds) bindLookback(options *lookback.Options) {
	options.CurrentReadHold, options.ReadHoldAt = holds.controller.ReadHold, holds.controller.ReadHoldAt
	options.OnWholeWindowReadEarly = func(e lookback.ReadHoldEvidence) {
		holds.observation(e.Contract, func(ctx context.Context) error {
			return holds.controller.Observe(ctx, readhold.Evidence{Contract: e.Contract, ArrivalAge: e.ArrivalAge, FirstReadAge: e.FirstReadAge,
				Confirmed: true, WholeWindow: true, Rung: e.Rung, Buckets: e.Buckets})
		})
	}
	options.OnEarlierRead = func(e lookback.EarlierReadEvidence) {
		holds.observation(e.Contract, func(ctx context.Context) error {
			return holds.controller.EarlierRead(ctx,
				readhold.EarlierEvidence{Contract: e.Contract, CandidateHold: e.CandidateHold, Observed: e.Observed, Equal: e.Equal})
		})
	}
	options.OnReadHoldIgnored = func(e lookback.ReadHoldEvidence, reason string) {
		inspection := holds.controller.Inspect(e.Contract.Slot.QueryGroup)
		if reason != lookback.IgnoredNoWholeWindowArrival || !inspection.Loaded || inspection.Missing || holds.controller.ReadHold(e.Contract.Slot.QueryGroup) == 0 {
			return
		}
		holds.observation(e.Contract, func(ctx context.Context) error {
			return holds.controller.Observe(ctx, readhold.Evidence{Contract: e.Contract, Noise: true})
		})
	}
	// Ignored partial revisions and noise stay in the lookback's counters;
	// they never create or renew a zero-h Redis record.
}

func (holds *productionReadHolds) renew(ctx context.Context) {
	holds.mu.Lock()
	now := holds.now()
	if now.Before(holds.nextRenew) {
		holds.mu.Unlock()
		return
	}
	holds.nextRenew = now.Add(readhold.RenewInterval)
	holds.mu.Unlock()
	err := holds.controller.RenewDue(ctx)
	if err != nil {
		holds.mu.Lock()
		holds.nextRenew = now.Add(time.Minute)
		holds.mu.Unlock()
	}
	holds.report("renew_failed", "", err)
}

func (holds *productionReadHolds) observeOvertaken(ref execution.FrozenExecutionContractRef) {
	if record, known := holds.controller.Reading(ref.Slot.QueryGroup); known &&
		(record.Closed || record.SegmentStart > ref.ScheduleSegmentStart) {
		holds.overtaken.Add(1)
	}
}
