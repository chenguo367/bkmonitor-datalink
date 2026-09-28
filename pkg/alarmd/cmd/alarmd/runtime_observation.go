package main

import (
	"context"
	"crypto/tls"
	"net"
	"slices"
	"sort"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

func observationRedisOptions(connection config.RedisConnectionConfig) *redis.UniversalOptions {
	o := productionRedisOptions(phaseTwoDiagnosticsConnection(connection))
	// A diagnostic command gets one attempt; the next maintenance tick is its
	// retry. Transport limits also bound clients which ignore context deadlines.
	o.MaxRetries = -1
	o.ReadTimeout = min(o.ReadTimeout, time.Second)
	o.WriteTimeout = min(o.WriteTimeout, time.Second)
	o.PoolTimeout = time.Second
	return o
}

// diagnosticDialRetryDelay is the pause before a diagnostic client dials a
// connection a second time. Short: a CLI call is waiting on it.
const diagnosticDialRetryDelay = 100 * time.Millisecond

// redisDial is a Redis client's dialer.
type redisDial func(ctx context.Context, network, addr string) (net.Conn, error)

// goRedisDial is the dial go-redis makes when a client names no dialer of its
// own, with its default timeout when none is set.
func goRedisDial(options *redis.UniversalOptions) redisDial {
	timeout := options.DialTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 5 * time.Minute}
	if options.TLSConfig == nil {
		return dialer.DialContext
	}
	return func(_ context.Context, network, addr string) (net.Conn, error) {
		return tls.DialWithDialer(dialer, network, addr, options.TLSConfig)
	}
}

// withDialRetry dials a connection a second time when the first dial fails.
// A diagnostic client connects on first use and again once its connections
// went idle, so a CLI call often lands on a fresh dial, and one name lookup
// that timed out failed the whole call while the runtime's long-lived
// connections never noticed. A dial sends no command, so dialing again
// repeats nothing -- which a retry of the authorization store's writes could
// not promise. The Sentinel connections are dialed by the same dialer, so an
// unreachable Sentinel gets its second dial too. onRetry hears why the first
// dial failed; a second failure is the caller's error, classified as before.
func withDialRetry(options *redis.UniversalOptions, dial redisDial, onRetry func(reason string)) {
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err == nil || ctx.Err() != nil {
			return conn, err
		}
		if onRetry != nil {
			onRetry(redisfailure.Reason(err))
		}
		pause := time.NewTimer(diagnosticDialRetryDelay)
		defer pause.Stop()
		select {
		case <-ctx.Done():
			return nil, err
		case <-pause.C:
		}
		return dial(ctx, network, addr)
	}
}

func observationSampleLimits(capacity config.ObservationCapacity) (observability.SeriesSampleLimits, bool) {
	queue := min(capacity.SampleBufferBytes/observability.SeriesSampleBufferBytes(), capacity.SampleRecordsPerMinute)
	limits := observability.SeriesSampleLimits{RecordsPerMinute: capacity.SampleRecordsPerMinute, BytesPerMinute: capacity.SampleBytesPerMinute, QueueCapacity: queue}
	return limits, queue > 0 && limits.BytesPerMinute >= observability.SeriesSampleMaxBytes
}

// observationCostOptions is the cost summary sized to the most groups whose
// reservation fits the collector's half of the budget. The reservation grows
// with the groups, so the most that fit is found by bisection. It was the
// budget halved until it fit, which could leave up to half the budget
// unused, and made the count a cliff: a reservation a few hundred bytes a
// group larger halved the groups a replica could track.
func observationCostOptions(capacity config.ObservationCapacity, process string, now func() time.Time) observability.CostSummaryOptions {
	// The other half pays for bounded cross-replica projection I/O and decode.
	collectorBytes := capacity.CostBytes / 2
	best := 0
	for low, high := 1, collectorBytes/2048; low <= high; {
		groups := low + (high-low)/2
		if observability.CostSummaryCapacityBytes(observationCostOptionsFor(capacity, groups, process, now)) <= int64(collectorBytes) {
			best, low = groups, groups+1
		} else {
			high = groups - 1
		}
	}
	if best == 0 {
		return observability.CostSummaryOptions{ProcessID: process, Now: now}
	}
	return observationCostOptionsFor(capacity, best, process, now)
}

// observationCostOptionsFor is the cost summary at a given number of groups,
// the rest derived from the budget as observationCostOptions derives it.
func observationCostOptionsFor(capacity config.ObservationCapacity, groups int, process string, now func() time.Time) observability.CostSummaryOptions {
	o := observability.CostSummaryOptions{ProcessID: process, Window: 5 * time.Minute, Now: now,
		GroupCapacity: groups, PlanCapacity: groups * 2, MetadataBytes: capacity.CostBytes / 2 / 8}
	// TopN rows of every ranking -- two scopes times the dimensions -- at
	// about a contributor row each, inside the projection's publish share.
	// The dimension count is the summary's own, not a literal: it was a
	// literal 12 from the six dimensions the summary began with, and
	// stayed 12 when two more were added.
	rankings := 2 * len(observability.CostDimensions())
	o.TopN = min(20, groups, max(1, (capacity.CostBytes/16)/(rankings*4096)))
	return o
}

func observationProjectionLimits(capacity config.ObservationCapacity, interval time.Duration) fleet.CostProjectionLimits {
	return fleet.CostProjectionLimits{PublishBytes: capacity.CostBytes / 16, ReadBytes: capacity.CostBytes / 16, ReadCommands: capacity.DirectoryCommands / 2, Timeout: time.Second, FreshFor: 3 * interval, TTL: 4 * interval}
}

// The existing maintenance loop publishes small cost projections. Cross-replica
// reads use only these projections and a bounded, read-only registration page;
// neither the full fleet view nor an execution Redis connection is involved.
type observationCostRefresh struct {
	store          *fleet.CostProjectionStore
	registry       *ownership.RedisStore
	reader         redis.Cmdable
	cache          *fleet.CostCandidatesCache
	limits         fleet.CostProjectionLimits
	registryLimits ownership.ObservationRegistryLimits
	replica        string
	interval       time.Duration
	last           time.Time
	offset         int64
	pageVisited    int
	observation    fleet.CostCandidatesSnapshot
}

func (r *observationCostRefresh) publish(ctx context.Context, at time.Time, cost observability.CostSnapshot) {
	if r == nil {
		return
	}
	ctx = redisfailure.WithCaller(ctx, redisfailure.CallerCostProjection)
	published, publishErr := r.store.Publish(ctx, r.replica, at, cost)
	if r.last.IsZero() || at.Sub(r.last) >= r.interval {
		r.last = at
		registry := r.registry.ReadObservationRegistry(ctx, r.reader, at, r.offset, r.registryLimits)
		view := r.store.Load(ctx, registry.ReadyIDs, registry.Complete, at)
		// Both reads can paginate. Keep the registry page until the cost reader
		// has attempted each member, otherwise the two rotating cursors can
		// permanently skip alternating replicas. Re-read registrations on every
		// tick; this cursor never extends a registration's validity.
		if r.observation.Registry.Offset != registry.Offset || !slices.Equal(r.observation.Registry.ReadyIDs, registry.ReadyIDs) {
			r.pageVisited = 0
		}
		r.pageVisited += view.Attempted
		if r.pageVisited >= len(registry.ReadyIDs) {
			r.offset = registry.NextOffset
			r.pageVisited = 0
		}
		r.observation = fleet.CostCandidatesSnapshot{ObservedAt: at, Projection: view, Registry: registry, Limits: r.limits}
	}
	out := r.observation
	out.LocalPublicationFailed = publishErr != nil
	out.LocalPublishedBytes = published.WrittenBytes
	if publishErr != nil {
		out.Projection.Complete = false
		out.Projection.Gaps = append(append([]fleet.CostProjectionGap(nil), out.Projection.Gaps...), fleet.CostProjectionGap{Replica: r.replica, Reason: "LOCAL_PUBLICATION_UNAVAILABLE"})
	}
	r.cache.Update(out)
}

// Directory refresh runs on the fleet publisher's independent maintenance
// loop, not the scheduler/control loop or an HTTP caller. Only identity
// metadata enters the scalar collector; no frozen config is retained twice.
type observationRefresh struct {
	directory *controlplane.ObservationDirectory
	cost      *observability.CostSummary
	now       func() time.Time
	interval  time.Duration
	last      time.Time
	// owned is this replica's Query Groups with the timeline revision each
	// lease names, and identity what each executes under at a time, from
	// memory (controlplane.RedisCatalogRepository.CachedExecutionIdentity).
	owned    func() []ownedLease
	identity func(execution.QueryGroupIdentity, uint64, execution.EvaluationTime) (controlplane.ExecutionIdentity, bool)
}

// ownedLease is one Query Group this replica owns: the timeline revision its
// lease names, and whether its owner is accepting on that lease.
type ownedLease struct {
	queryGroup execution.QueryGroupIdentity
	revision   uint64
	accepting  bool
}

func (r *observationRefresh) publish(ctx context.Context) {
	at := r.now()
	if r.last.IsZero() || at.Sub(r.last) >= r.interval {
		r.last = at
		// The roster is read from memory, not from the directory, so it no
		// longer waits on a directory this replica may not keep.
		if r.directory != nil {
			r.directory.Refresh(ctx, at)
		}
		var owned []ownedLease
		if r.owned != nil {
			owned = r.owned()
		}
		groups, complete := executionCostGroups(owned, r.identity, execution.EvaluationTime(at.Unix()))
		r.cost.Reconcile(groups, complete)
	}
	r.cost.Publish(at)
}

// executionCostGroups is the cost roster: every owned Query Group under the
// Segment it executes at at, with the Plans that Segment activates. The
// events the summary counts carry their Slot's publication, query and
// schedule revisions, which are that Segment's; a roster read from the
// strategy directory named, for Plans still on a publication whose manifest
// has expired, no Query Group at all, and for the latest publication's rows
// revisions no Slot runs with, so on a long-lived deployment almost every
// event was untracked.
//
// Complete only when every owned Query Group answered. One that did not - a
// lease the owner is not accepting on, a timeline not in the control cache
// at the lease's revision - is left out rather than guessed at, and its
// Slots count as untracked.
func executionCostGroups(owned []ownedLease,
	identity func(execution.QueryGroupIdentity, uint64, execution.EvaluationTime) (controlplane.ExecutionIdentity, bool),
	at execution.EvaluationTime) ([]observability.CostGroup, bool) {
	groups := make([]observability.CostGroup, 0, len(owned))
	complete := identity != nil
	for _, lease := range owned {
		if !lease.accepting || identity == nil {
			complete = false
			continue
		}
		facts, ok := identity(lease.queryGroup, lease.revision, at)
		if !ok {
			complete = false
			continue
		}
		group := observability.CostGroup{QueryGroupKey: string(lease.queryGroup), SnapshotRevision: string(facts.SnapshotRevision),
			QueryRevision: string(facts.QueryRevision), ScheduleRevision: string(facts.ScheduleRevision), TotalMembers: len(facts.Plans)}
		for _, plan := range facts.Plans {
			group.Members = append(group.Members, observability.CostPlanIdentity{TenantID: plan.TenantID, BusinessID: plan.BusinessID, StrategyID: plan.StrategyID})
		}
		for _, schedule := range facts.Schedules {
			plan := schedule.Identity
			group.Schedules = append(group.Schedules, observability.CostSchedule{
				Plan:            observability.CostPlanIdentity{TenantID: plan.TenantID, BusinessID: plan.BusinessID, StrategyID: plan.StrategyID},
				IntervalSeconds: schedule.Spec.EvaluationIntervalSeconds, AlignmentSeconds: int64(schedule.Spec.Alignment),
				CompletionOffsetSeconds: schedule.Spec.CompletionOffsetSeconds(),
			})
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].QueryGroupKey < groups[j].QueryGroupKey })
	return groups, complete
}
