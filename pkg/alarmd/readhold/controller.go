// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package readhold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

const (
	RecordTTL              = 7 * 24 * time.Hour
	RenewInterval          = 24 * time.Hour
	SingleEventQuiet       = time.Hour
	EarlierMatchesRequired = 3
)

var (
	ErrNotRestored         = errors.New("alarmd readhold: Query Group not restored")
	ErrNotConfigured       = errors.New("alarmd readhold: Query Group not configured")
	ErrConflict            = errors.New("alarmd readhold: record changed")
	ErrPreviousOpen        = errors.New("alarmd readhold: previous schedule not closed")
	ErrPreviousHoldUnknown = errors.New("alarmd readhold: previous read hold unknown")
	ErrSegmentStale        = errors.New("alarmd readhold: schedule segment already replaced")
)

// Control uses the existing ownership store. A value and its Plan/route
// evidence are one fenced CAS in the owning Query Group's Redis hash slot.
type Control interface {
	ReadControlBatch(context.Context, []execution.QueryGroupIdentity, string) ([]ownership.ControlRead, error)
	FencedCompareAndSet(context.Context, ownership.FencedCASRequest) (ownership.FencedCASStatus, error)
}

type Owner struct {
	Fence        execution.OwnerFence
	ContentScope string
}

type Options struct {
	Control Control
	Prefix  string
	MaxHold time.Duration
	Now     func() time.Time
	Owner   func(execution.QueryGroupIdentity) (Owner, error)
}

type PlanRef struct {
	Key execution.PlanKey `json:"key"`
	// Route excludes time_delay but includes the data source/metric identity:
	// an unrelated metric of the same strategy must not inherit its lateness.
	Route string `json:"route"`
}

// PlanRecord is the bridge to a replacement Query Group. Its previous Slot
// is taken from the closed schedule, never from a lagging runtime cursor.
type PlanRecord struct {
	PlanRef
	ArrivalAgeMillis       int64                        `json:"arrival_age_ms,omitempty"`
	ClosedAt               execution.EvaluationTime     `json:"closed_at,omitempty"`
	ClosedQueryGroup       execution.QueryGroupIdentity `json:"closed_query_group,omitempty"`
	PreviousHoldMillis     int64                        `json:"previous_hold_ms,omitempty"`
	PreviousSlot           execution.EvaluationTime     `json:"previous_slot,omitempty"`
	CompletionOffsetMillis int64                        `json:"completion_offset_ms,omitempty"`
}

type Transition struct {
	Key            execution.PlanKey `json:"key"`
	DeadlineMillis int64             `json:"deadline_ms"`
}

type Previous struct {
	QueryGroup execution.QueryGroupIdentity
	ClosedAt   execution.EvaluationTime
	// Plans identifies this activation's links. Different Plans can leave
	// the same Query Group at different boundaries. Empty means spec.Plans.
	Plans []PlanRef
	// Schedule supplies the predecessor's last legal Slot when its hold was
	// independently established as zero and no hold record ever existed.
	Schedule *execution.FrozenQueryGroupSchedule
	// Absence alone is not a historical hold. The caller may establish this
	// fact for a predecessor that never had a nonzero hold.
	ZeroConfirmed bool
}

type GroupSpec struct {
	QueryGroup   execution.QueryGroupIdentity
	Plans        []PlanRef
	Delay        time.Duration
	SettlingWait time.Duration
	// HoldLimit is snapshot retention minus its existing minimum lifetime.
	// Zero means no margin, not an unset limit.
	HoldLimit time.Duration
	Previous  []Previous
}

type Evidence struct {
	Contract     execution.FrozenExecutionContractRef
	ArrivalAge   time.Duration
	FirstReadAge time.Duration
	Rung         string
	Buckets      []int64
	Confirmed    bool
	WholeWindow  bool
	Noise        bool
}

type EarlierEvidence struct {
	Contract      execution.FrozenExecutionContractRef
	CandidateHold time.Duration
	Observed      bool
	Equal         bool
}

type entry struct {
	mu                         sync.Mutex
	spec                       GroupSpec
	configured, loaded, seeded bool
	record                     Record
	raw                        []byte
}

type Controller struct {
	options Options
	mu      sync.RWMutex
	groups  map[execution.QueryGroupIdentity]*entry
}

func NewController(options Options) (*Controller, error) {
	if options.Control == nil || options.Prefix == "" || options.Owner == nil || options.MaxHold <= 0 ||
		options.MaxHold.Milliseconds() > execution.MaxReadHoldMillis {
		return nil, errors.New("alarmd readhold: invalid controller options")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Controller{options: options, groups: make(map[execution.QueryGroupIdentity]*entry)}, nil
}

func (controller *Controller) group(qg execution.QueryGroupIdentity) *entry {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.groups[qg] == nil {
		controller.groups[qg] = &entry{}
	}
	return controller.groups[qg]
}

func (controller *Controller) Configure(spec GroupSpec) error {
	if spec.QueryGroup == "" || len(spec.Plans) == 0 || spec.Delay < 0 || spec.SettlingWait < 0 ||
		spec.HoldLimit < 0 || spec.HoldLimit.Milliseconds() > execution.MaxReadHoldMillis {
		return errors.New("alarmd readhold: invalid Query Group spec")
	}
	for _, plan := range spec.Plans {
		if plan.Key.PlanIdentity.Validate() != nil || plan.Route == "" {
			return errors.New("alarmd readhold: invalid Plan route")
		}
	}
	for _, previous := range spec.Previous {
		if previous.QueryGroup == "" || previous.QueryGroup == spec.QueryGroup || previous.ClosedAt <= 0 {
			return errors.New("alarmd readhold: invalid predecessor")
		}
		for _, plan := range previous.Plans {
			found := false
			for _, wanted := range spec.Plans {
				found = found || wanted == plan
			}
			if !found {
				return errors.New("alarmd readhold: predecessor Plan not configured")
			}
		}
	}
	spec.Plans = append([]PlanRef(nil), spec.Plans...)
	spec.Previous = append([]Previous(nil), spec.Previous...)
	for index := range spec.Previous {
		spec.Previous[index].Plans = append([]PlanRef(nil), spec.Previous[index].Plans...)
	}
	state := controller.group(spec.QueryGroup)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.spec, state.configured, state.seeded = spec, true, false
	return nil
}

// RestoreBatch runs before replay classification. An unanswered/corrupt entry
// is left unready; successful entries of the same batch can still proceed.
func (controller *Controller) RestoreBatch(ctx context.Context, groups []execution.QueryGroupIdentity) error {
	for _, qg := range groups {
		state := controller.group(qg)
		state.mu.Lock()
		state.loaded = false
		state.mu.Unlock()
	}
	reads, err := controller.options.Control.ReadControlBatch(ctx, groups, controller.options.Prefix+":"+Namespace)
	if err != nil {
		return err
	}
	if len(reads) != len(groups) {
		return errors.New("alarmd readhold: incomplete control batch")
	}
	var first error
	for index, read := range reads {
		decodeErr := read.Err
		record := Record{SinceSlot: 1}
		if !read.Missing && decodeErr == nil {
			record, decodeErr = Decode(read.Raw)
		}
		if decodeErr != nil {
			if first == nil {
				first = fmt.Errorf("readhold %s: %w", groups[index], decodeErr)
			}
			continue
		}
		state := controller.group(groups[index])
		state.mu.Lock()
		state.record, state.raw, state.loaded, state.seeded = record, append([]byte(nil), read.Raw...), true, false
		state.mu.Unlock()
	}
	return first
}

func current(record Record) int64 {
	if record.PendingHoldMillis != nil {
		return *record.PendingHoldMillis
	}
	return record.HoldMillis
}

func holdAt(record Record, at execution.EvaluationTime, wait time.Duration) int64 {
	hold := current(record)
	for _, transition := range record.Transitions {
		hold = max(hold, transition.DeadlineMillis-int64(at)*1000-wait.Milliseconds())
	}
	return max(hold, 0)
}

func (controller *Controller) ReadHold(qg execution.QueryGroupIdentity) time.Duration {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	return time.Duration(current(state.record)) * time.Millisecond
}

func (controller *Controller) ReadHoldAt(qg execution.QueryGroupIdentity, at execution.EvaluationTime) time.Duration {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	return time.Duration(holdAt(state.record, at, state.spec.SettlingWait)) * time.Millisecond
}

func (controller *Controller) Reading(qg execution.QueryGroupIdentity) (Record, bool) {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	return clone(state.record), state.loaded
}

type Inspection struct {
	Record  Record
	Loaded  bool
	Missing bool
}

func (controller *Controller) Inspect(qg execution.QueryGroupIdentity) Inspection {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	return Inspection{Record: clone(state.record), Loaded: state.loaded, Missing: state.loaded && len(state.raw) == 0}
}

func clone(record Record) Record {
	if record.PendingHoldMillis != nil {
		hold := *record.PendingHoldMillis
		record.PendingHoldMillis = &hold
	}
	record.Buckets = append([]int64(nil), record.Buckets...)
	record.Plans = append([]PlanRecord(nil), record.Plans...)
	record.Transitions = append([]Transition(nil), record.Transitions...)
	return record
}

func ready(state *entry) error {
	if !state.loaded {
		return ErrNotRestored
	}
	if !state.configured {
		return ErrNotConfigured
	}
	return nil
}

// persist publishes the new in-memory value only after the fenced CAS wins.
// A conflict invalidates the entry for the caller's next batched reload.
func (controller *Controller) persist(ctx context.Context, qg execution.QueryGroupIdentity, state *entry, next Record, owner *Owner) error {
	if owner == nil {
		value, err := controller.options.Owner(qg)
		if err != nil {
			return err
		}
		owner = &value
	}
	if owner.Fence.QueryGroup != qg {
		return errors.New("alarmd readhold: owner differs from Query Group")
	}
	next.RenewedAtMillis = controller.options.Now().UnixMilli()
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if _, err := Decode(raw); err != nil {
		return err
	}
	status, err := controller.options.Control.FencedCompareAndSet(ctx, ownership.FencedCASRequest{
		Fence: owner.Fence, ContentScope: owner.ContentScope, Namespace: controller.options.Prefix + ":" + Namespace,
		ExpectedMissing: len(state.raw) == 0, Expected: state.raw, Value: raw, TTL: RecordTTL,
	})
	if err != nil {
		return err
	}
	switch status {
	case ownership.FencedCASApplied:
		state.record, state.raw = next, raw
		return nil
	case ownership.FencedCASConflict:
		state.loaded = false
		return ErrConflict
	case ownership.FencedCASStaleOwner:
		return ownership.ErrStaleFence
	case ownership.FencedCASContentMoved:
		return ownership.ErrContentScopeMoved
	default:
		return errors.New("alarmd readhold: invalid CAS status")
	}
}

func inherited(record Record, spec GroupSpec) bool {
	for _, predecessor := range spec.Previous {
		for _, wanted := range previousPlans(spec, predecessor) {
			found := false
			for _, plan := range record.Plans {
				found = found || (plan.PlanRef == wanted && plan.ClosedAt == predecessor.ClosedAt && plan.ClosedQueryGroup == predecessor.QueryGroup)
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func previousPlans(spec GroupSpec, predecessor Previous) []PlanRef {
	if len(predecessor.Plans) > 0 {
		return predecessor.Plans
	}
	return spec.Plans
}

func (controller *Controller) seed(state *entry, predecessors map[execution.QueryGroupIdentity]Inspection) (Record, error) {
	next := clone(state.record)
	if state.seeded || inherited(next, state.spec) {
		return next, nil
	}
	for _, predecessor := range state.spec.Previous {
		old := predecessors[predecessor.QueryGroup]
		previous := old.Record
		if !old.Loaded {
			return Record{}, ErrNotRestored
		}
		if old.Missing {
			if !predecessor.ZeroConfirmed {
				return Record{}, ErrPreviousHoldUnknown
			}
			if predecessor.Schedule == nil {
				return Record{}, ErrPreviousHoldUnknown
			}
			if predecessor.Schedule.Validate() != nil || predecessor.Schedule.Segment.End == nil || *predecessor.Schedule.Segment.End != predecessor.ClosedAt || predecessor.Schedule.Segment.QueryGroup != predecessor.QueryGroup {
				return Record{}, ErrPreviousHoldUnknown
			}
			previous = Record{SinceSlot: 1}
			closeRecord(&previous, state.spec, *predecessor.Schedule)
		}
		for _, wanted := range previousPlans(state.spec, predecessor) {
			matched := false
			for _, plan := range previous.Plans {
				if plan.PlanRef != wanted {
					continue
				}
				if plan.ClosedAt != predecessor.ClosedAt || (plan.ClosedQueryGroup != "" && plan.ClosedQueryGroup != predecessor.QueryGroup) {
					return Record{}, ErrPreviousOpen
				}
				matched = true
				plan.ClosedQueryGroup = predecessor.QueryGroup
				next.ArrivalAgeMillis = max(next.ArrivalAgeMillis, plan.ArrivalAgeMillis)
				mergePlan(&next, plan)
				if plan.PreviousSlot > 0 {
					next.Transitions = append(next.Transitions, Transition{Key: plan.Key,
						DeadlineMillis: int64(plan.PreviousSlot)*1000 + plan.PreviousHoldMillis + plan.CompletionOffsetMillis})
				}
			}
			if !matched {
				return Record{}, ErrPreviousOpen
			}
		}
	}
	if next.ArrivalAgeMillis > 0 {
		hold := max(0, next.ArrivalAgeMillis-state.spec.Delay.Milliseconds()-state.spec.SettlingWait.Milliseconds())
		limit := min(state.spec.HoldLimit, controller.options.MaxHold).Milliseconds()
		next.AtLimit, next.LimitMillis = hold > limit, limit
		hold = min(hold, limit)
		next.PendingHoldMillis = &hold
		next.QuietSinceMillis = controller.options.Now().UnixMilli()
	}
	return next, nil
}

func mergePlan(record *Record, plan PlanRecord) {
	for index, existing := range record.Plans {
		if existing.PlanRef == plan.PlanRef {
			record.Plans[index] = plan
			return
		}
	}
	record.Plans = append(record.Plans, plan)
}

// SlotReadHold is called before a new Slot is frozen. An unfinished Slot
// bypasses it and keeps its own contract's hold.
func (controller *Controller) SlotReadHold(ctx context.Context, schedule execution.FrozenQueryGroupSchedule, at execution.EvaluationTime, fence execution.OwnerFence) (time.Duration, error) {
	if schedule.Validate() != nil || !schedule.Segment.Contains(at) || len(schedule.DuePlanRefs(at)) == 0 {
		return 0, errors.New("alarmd readhold: invalid Slot schedule")
	}
	qg := schedule.Segment.QueryGroup
	state := controller.group(qg)
	// Snapshot predecessor records before locking this group. Cross-group
	// migrations must not acquire two entry locks in opposite order.
	state.mu.Lock()
	spec := state.spec
	state.mu.Unlock()
	predecessors := make(map[execution.QueryGroupIdentity]Inspection, len(spec.Previous))
	for _, previous := range spec.Previous {
		predecessors[previous.QueryGroup] = controller.Inspect(previous.QueryGroup)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ready(state); err != nil {
		return 0, err
	}
	if !reflect.DeepEqual(spec, state.spec) {
		return 0, ErrConflict
	}
	owner, err := controller.options.Owner(qg)
	if err != nil {
		return 0, err
	}
	owner.Fence = fence
	next, err := controller.seed(state, predecessors)
	if err != nil {
		return 0, err
	}
	if schedule.Segment.Start < next.SegmentStart {
		return 0, ErrSegmentStale
	}
	if schedule.Segment.Start > next.SegmentStart {
		next.SegmentStart, next.Closed = schedule.Segment.Start, false
	}
	if schedule.Segment.End != nil {
		closeRecord(&next, state.spec, schedule)
	}
	hold := holdAt(next, at, state.spec.SettlingWait)
	if hold > min(state.spec.HoldLimit, controller.options.MaxHold).Milliseconds() {
		return 0, errors.New("alarmd readhold: transition exceeds retention margin")
	}
	if next.HoldMillis != hold || next.PendingHoldMillis != nil {
		next.PreviousHoldMillis, next.PreviousSinceSlot = next.HoldMillis, next.SinceSlot
		next.HoldMillis, next.SinceSlot = hold, at
		// The base hold can differ from a transition Slot's hold. Keep it
		// pending so the next Slot recomputes its own transition deadline.
		base := current(next)
		if next.PendingHoldMillis == nil {
			base = state.record.HoldMillis
		}
		if base != hold {
			next.PendingHoldMillis = &base
		} else {
			next.PendingHoldMillis = nil
		}
	}
	if !reflect.DeepEqual(next, state.record) {
		// A first zero hold has no durable value to protect. Remember the
		// segment locally without creating a record for the full population.
		if len(state.raw) == 0 && current(next) == 0 && len(next.Plans) == 0 && len(next.Transitions) == 0 {
			state.record, state.seeded = next, true
			return 0, nil
		}
		if err := controller.persist(ctx, qg, state, next, &owner); err != nil {
			return 0, err
		}
	}
	state.seeded = true
	return time.Duration(hold) * time.Millisecond, nil
}

// CloseSchedule fixes the old hold and each Plan's last legal Slot before a
// replacement Query Group may seed itself. The old owner must write it.
func (controller *Controller) CloseSchedule(ctx context.Context, schedule execution.FrozenQueryGroupSchedule, fence execution.OwnerFence) error {
	if schedule.Validate() != nil || schedule.Segment.End == nil {
		return errors.New("alarmd readhold: closed schedule required")
	}
	state := controller.group(schedule.Segment.QueryGroup)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ready(state); err != nil {
		return err
	}
	next := clone(state.record)
	if schedule.Segment.Start < next.SegmentStart {
		for _, plan := range schedule.Plans {
			found := false
			for _, existing := range next.Plans {
				found = found || (existing.Key == plan.Key() && existing.ClosedAt == *schedule.Segment.End && existing.ClosedQueryGroup == schedule.Segment.QueryGroup)
			}
			if !found {
				return ErrSegmentStale
			}
		}
		return nil
	}
	closeRecord(&next, state.spec, schedule)
	if len(state.raw) == 0 && current(next) == 0 && next.ArrivalAgeMillis == 0 && len(next.Transitions) == 0 {
		// The caller establishes the absent zero predecessor using retained
		// progress and its closed timeline; do not invent a durable zero h.
		state.record = next
		return nil
	}
	if reflect.DeepEqual(next, state.record) {
		return nil
	}
	owner, err := controller.options.Owner(schedule.Segment.QueryGroup)
	if err != nil {
		return err
	}
	owner.Fence = fence
	return controller.persist(ctx, schedule.Segment.QueryGroup, state, next, &owner)
}

func closeRecord(record *Record, spec GroupSpec, schedule execution.FrozenQueryGroupSchedule) {
	record.SegmentStart, record.Closed = schedule.Segment.Start, true
	refs := append([]PlanRef(nil), spec.Plans...)
	for _, existing := range record.Plans {
		found := false
		for _, ref := range refs {
			found = found || ref == existing.PlanRef
		}
		if !found {
			refs = append(refs, existing.PlanRef)
		}
	}
	for _, plan := range schedule.Plans {
		last := int64(*schedule.Segment.End) - 1
		last -= (last - int64(plan.Spec.Alignment)) % plan.Spec.EvaluationIntervalSeconds
		if last < int64(schedule.Segment.Start) || !plan.Spec.IsAligned(execution.EvaluationTime(last)) {
			continue
		}
		for _, ref := range refs {
			if ref.Key != plan.Key() {
				continue
			}
			fixed := false
			for _, existing := range record.Plans {
				fixed = fixed || (existing.PlanRef == ref && existing.ClosedAt == *schedule.Segment.End && existing.ClosedQueryGroup == schedule.Segment.QueryGroup)
			}
			if fixed {
				continue
			}
			mergePlan(record, PlanRecord{PlanRef: ref, ArrivalAgeMillis: record.ArrivalAgeMillis,
				ClosedAt:           *schedule.Segment.End,
				ClosedQueryGroup:   schedule.Segment.QueryGroup,
				PreviousHoldMillis: holdAt(*record, execution.EvaluationTime(last), spec.SettlingWait), PreviousSlot: execution.EvaluationTime(last),
				CompletionOffsetMillis: plan.Spec.CompletionOffsetSeconds() * 1000})
		}
	}
}

func (controller *Controller) Observe(ctx context.Context, evidence Evidence) error {
	if evidence.Contract.Validate() != nil || evidence.ArrivalAge < 0 || evidence.FirstReadAge < 0 {
		return errors.New("alarmd readhold: invalid lateness evidence")
	}
	qg := evidence.Contract.Slot.QueryGroup
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ready(state); err != nil {
		return err
	}
	if !evidence.Noise && (!evidence.Confirmed || !evidence.WholeWindow) {
		return nil
	}
	if evidence.Contract.ScheduleSegmentStart < state.record.SegmentStart || state.record.Closed {
		return nil
	}
	next := clone(state.record)
	if next.SegmentStart == 0 {
		next.SegmentStart = evidence.Contract.ScheduleSegmentStart
	}
	if evidence.Noise {
		next.Noise++
	} else {
		now := controller.options.Now().UnixMilli()
		if next.LastEarlyMillis > 0 {
			next.MaxEarlyIntervalMillis = max(next.MaxEarlyIntervalMillis, now-next.LastEarlyMillis)
		}
		next.LastEarlyMillis, next.QuietSinceMillis, next.EarlierMatches = now, now, 0
		next.ArrivalAgeMillis = max(next.ArrivalAgeMillis, evidence.ArrivalAge.Milliseconds())
		next.Rung, next.Buckets = evidence.Rung, append([]int64(nil), evidence.Buckets...)
		for _, ref := range state.spec.Plans {
			plan := PlanRecord{PlanRef: ref, ArrivalAgeMillis: next.ArrivalAgeMillis}
			for _, existing := range next.Plans {
				if existing.PlanRef == ref {
					plan = existing
					if plan.ClosedAt == 0 {
						plan.ArrivalAgeMillis = next.ArrivalAgeMillis
					}
					break
				}
			}
			mergePlan(&next, plan)
		}
		limit := min(state.spec.HoldLimit, controller.options.MaxHold).Milliseconds()
		// Only the configured delay and settling wait recur next round.
		// Permit or scheduling delay in FirstReadAge cannot pay for its hold.
		target := max(0, evidence.ArrivalAge.Milliseconds()-state.spec.Delay.Milliseconds()-state.spec.SettlingWait.Milliseconds())
		next.AtLimit, next.LimitMillis = target > limit, limit
		if !next.Closed && min(target, limit) > current(next) {
			hold := min(target, limit)
			next.PendingHoldMillis = &hold
			if next.Lowered {
				next.RaisedAfterLowering++
				next.Lowered = false
			}
		}
	}
	return controller.persist(ctx, qg, state, next, nil)
}

func (controller *Controller) EarlierRead(ctx context.Context, evidence EarlierEvidence) error {
	if evidence.Contract.Validate() != nil || evidence.CandidateHold < 0 {
		return errors.New("alarmd readhold: invalid earlier-read evidence")
	}
	state := controller.group(evidence.Contract.Slot.QueryGroup)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ready(state); err != nil {
		return err
	}
	if evidence.Contract.ScheduleSegmentStart < state.record.SegmentStart || state.record.Closed {
		return nil
	}
	hold := current(state.record)
	if !evidence.Observed || hold == 0 || evidence.CandidateHold.Milliseconds() != hold/2 || evidence.Contract.Slot.EvaluationTime <= state.record.LastEarlierSlot {
		return nil
	}
	next := clone(state.record)
	next.LastEarlierSlot = evidence.Contract.Slot.EvaluationTime
	now := controller.options.Now().UnixMilli()
	if !evidence.Equal {
		next.EarlierMatches, next.QuietSinceMillis = 0, now
	} else {
		next.EarlierMatches = min(next.EarlierMatches+1, EarlierMatchesRequired)
		quiet := next.MaxEarlyIntervalMillis
		if quiet == 0 {
			quiet = SingleEventQuiet.Milliseconds()
		}
		if next.QuietSinceMillis > 0 && now-next.QuietSinceMillis >= quiet && next.EarlierMatches >= EarlierMatchesRequired {
			candidate := hold / 2
			next.PendingHoldMillis = &candidate
			next.Lowered, next.EarlierMatches, next.QuietSinceMillis = true, 0, now
			next.ArrivalAgeMillis = state.spec.Delay.Milliseconds() + state.spec.SettlingWait.Milliseconds() + candidate
			for index := range next.Plans {
				if next.Plans[index].ClosedAt == 0 {
					next.Plans[index].ArrivalAgeMillis = next.ArrivalAgeMillis
				}
			}
		}
	}
	return controller.persist(ctx, evidence.Contract.Slot.QueryGroup, state, next, nil)
}

// RenewDue reuses CAS with the same value once a day; untouched records expire
// after seven days. A failed renewal changes no local state and is retryable.
func (controller *Controller) RenewDue(ctx context.Context) error {
	controller.mu.RLock()
	groups := make(map[execution.QueryGroupIdentity]*entry, len(controller.groups))
	for qg, state := range controller.groups {
		groups[qg] = state
	}
	controller.mu.RUnlock()
	var first error
	for qg, state := range groups {
		state.mu.Lock()
		if state.loaded && state.configured && !state.record.Closed && len(state.raw) > 0 && current(state.record) > 0 && controller.options.Now().UnixMilli()-state.record.RenewedAtMillis >= RenewInterval.Milliseconds() {
			if err := controller.persist(ctx, qg, state, clone(state.record), nil); err != nil && first == nil {
				first = err
			}
		}
		state.mu.Unlock()
	}
	return first
}

// Forget drops a retired Query Group's local state. Its fenced record ages
// out by TTL rather than an unfenced delete by a replacement owner.
func (controller *Controller) Forget(qg execution.QueryGroupIdentity) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	delete(controller.groups, qg)
}
