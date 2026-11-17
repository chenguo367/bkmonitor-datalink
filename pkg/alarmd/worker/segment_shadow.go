// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The shadow of the Plans' stable segments (014 B3, second stage). Every
// State is written as before; beside it each Plan's book - its member pages
// and its mark - is kept in the store beside its State: read once a Slot
// before its State (loadSegments), changed by the round as the skip would
// change it (3.3), and written once before its Progress (foldSegmentShadow).
// For every open member the skip would not have written, the read it would
// make (segment.Made) is compared with what the round wrote
// (state_segment_shadow_total).
//
// RETIRE(0.7): comparing every round goes once stage 3's skip has run on
// every deployment for 14 days with state_segment_shadow_total
// mismatch_* at 0 in each period class (le_5m, le_1h, le_1d, gt_1d), each
// class compared at least one round. What stays is a sampled comparison:
// one round a Plan's piece every 24 h, staggered by hash(Plan, piece), the
// round that writes the Plan whole anyway and compares on that write - no
// read of its own, so the sample is a self-healing write too - its rate
// fixed in code, the counter kept.

// segmentShadowHorizon is how far past a closed member's close its entry is
// kept, beyond its record's retention twice over: the shadow's stand-in for
// a key's life (its window, the lateness its Levels allow and the replay
// margin), never shorter than it.
const segmentShadowHorizon = 600

// shadowKey is a due Plan's segments under generation: the Plan, its piece
// of a split strategy and the generation, as its State store keys them.
func shadowKey(due execution.DuePlan, generation execution.StateGeneration) segment.PlanKey {
	return segment.PlanKey{Plan: due.Identity, Shard: due.Shard, Generation: generation}
}

// segmentDue is a due Plan whose segments a Slot reads: what its keys live
// by, and the points its window keeps.
type segmentDue struct {
	due       execution.DuePlan
	lifetime  segment.Lifetime
	retention uint32
}

// segmentSlot is one Slot's Plans' segments, as read before its State.
type segmentSlot struct {
	store segment.Store
	plans map[segment.PlanKey]*segmentShadowPlan
	// pieces is each Plan's books under a generation, one a piece: a
	// series is a member of one, found through its digest (Coverage). The
	// generation a Plan's activation carries from is among them, read for
	// the carried read alone (014 B3 3.2: materialized before the carry).
	pieces map[segmentPlanGeneration][]*segmentShadowPlan
	// carried is the books of the generations carried from, held for the
	// Slot like its own and in no round of the shadow.
	carried []*segmentShadowPlan
	// folded is set once the round folded into the books: what they cover
	// is this round's then, not the one the Slot's State was read against,
	// and no read of State comes after (segmentCoverageAfterFold counts one
	// that does).
	folded atomic.Bool
	// skip is the Slot's segment skip word followed: the Leader whose view
	// this worker runs under let it leave its stable members' writes out.
	// writeFailed is an earlier attempt of this Slot having left members
	// unwritten and its segments' write not landed: none is left now.
	skip        bool
	writeFailed bool
	// unread and undecodable are the Plans - by their own generation's key,
	// for a carried one too - whose segments did not answer, and whose
	// stored segments did not decode (014 B3 §9.7).
	unread, undecodable []segment.PlanKey
}

// segmentShadowPlan is one Plan's book as the Slot read it: its members, the
// position of the round its mark was written by (W, zero before the first),
// the mark and the pages it named. fresh is a Plan with no stored book that
// read - none yet, or one that did not read (lost, its members) - whose
// round writes every page.
type segmentShadowPlan struct {
	// stopped is the Plan degraded in a second Slot of its owner's term
	// (segmentDegradations): none of its members is left.
	stopped bool
	// fallback is the pages of the mark read unrenewed for half their
	// lifetime by the worker's clock as the Slot began (segment.Renewal
	// Fallback): their members are written whole this round (3.5).
	fallback  map[int]bool
	key       segment.PlanKey
	lifetime  segment.Lifetime
	ttl       time.Duration
	book      *segment.Book
	position  int64
	mark      segment.Mark
	hadMark   bool
	prior     int
	fresh     bool
	lost      int64
	interval  int64
	retention uint32
}

// segmentDues is the due Plans a Slot reads the segments of: a Plan on a
// grid, whose window keeps points.
func (coordinator *SlotExecutionCoordinator) segmentDues(header execution.InternalExecutionHeader) []segmentDue {
	var dues []segmentDue
	for _, due := range header.DuePlans {
		interval := planInterval(due)
		if interval <= 0 {
			continue
		}
		requirements, err := execution.DeriveStateRetentionRequirement(due.CompiledPlan)
		if err != nil {
			continue
		}
		var retention uint32
		for _, requirement := range requirements {
			retention = max(retention, requirement.RetentionPoints)
		}
		if retention == 0 {
			continue
		}
		dues = append(dues, segmentDue{due: due, retention: retention, lifetime: segment.Lifetime{Retention: requirements,
			Horizon: coordinator.stateHorizon(due), Interval: time.Duration(interval) * time.Second}})
	}
	return dues
}

// loadSegments reads the Slot's due Plans' segments before any of its State
// is read, for every operation: a member's State is read through them
// (Coverage), a supplement's and a probe's too, though neither completes a
// round of the shadow (newSegmentShadowRound). Nil for a store that keeps
// none, and nil with the reason when the read failed - counted by the store
// for its Plans - and the Slot runs without its shadow, its members' State
// read as stored.
//
// Segments a Slot dispatched with a segment skip word could not read fail
// its first attempt (segmentReadFails, SEGMENT_READ_FAILED); its retry reads
// them as stored, degraded (014 B3 §9.7). Plans whose segments did not
// decode are degraded whatever the word. A Plan degraded in a second Slot of
// its owner's term is stopped: none of its members is left.
func (coordinator *SlotExecutionCoordinator) loadSegments(ctx context.Context, request execution.SlotExecutionRequest,
	header execution.InternalExecutionHeader) (*segmentSlot, string, error) {
	store, ok := coordinator.ports.State.(segment.Store)
	if !ok {
		return nil, "", nil
	}
	skip := coordinator.followsSegmentSkip(ctx, request)
	dues := coordinator.segmentDues(header)
	slot := loadSegmentSlot(ctx, store, request, dues)
	var facts observability.SegmentShadowFacts
	defer coordinator.emitSegmentFacts(ctx, request.Operation, &facts)
	if slot == nil {
		keys := make([]segment.PlanKey, 0, len(dues))
		for _, due := range dues {
			keys = append(keys, shadowKey(due.due, due.due.StateGeneration))
		}
		if coordinator.segmentReadFails(request, keys) {
			return nil, "", segmentsUnreadError{why: "the read failed"}
		}
		if request.SegmentSkip != 0 {
			coordinator.degradeSegments(request, keys, observability.SegmentDegradedUnread, &facts)
		}
		return nil, observability.SegmentShadowSkippedReadFailed, nil
	}
	if len(slot.unread) > 0 {
		if coordinator.segmentReadFails(request, slot.unread) {
			return nil, "", segmentsUnreadError{why: "a Plan's, or the generation's it carries from, did not answer"}
		}
		if request.SegmentSkip != 0 {
			coordinator.degradeSegments(request, slot.unread, observability.SegmentDegradedUnread, &facts)
		}
	}
	coordinator.degradeSegments(request, slot.undecodable, observability.SegmentDegradedUndecodable, &facts)
	slot.skip = skip
	slot.writeFailed = coordinator.segmentWriteFailures.failed(request.Contract.Slot.QueryGroup)
	now := coordinator.now().UnixMilli()
	for key, state := range slot.plans {
		state.markFallback(now)
		state.stopped = coordinator.segmentDegradations.stopped(request, key)
	}
	return slot, "", nil
}

// segmentRetry is whether the attempt follows a failed one of its Slot: its
// number past one. The scheduler counts every failed attempt of a Slot and
// numbers the next past it, whatever it is dispatched as - a retry, a probe,
// an expired replay's normal dispatch; a retry operation is only ever such
// an attempt.
func segmentRetry(request execution.SlotExecutionRequest) bool {
	return request.AttemptNo > 1
}

// segmentReadFails is whether segments not read for the Plans keys fail the
// Slot's attempt (014 B3 §9.7): it is dispatched with a segment skip word -
// followed or not, as an ignored one still finds the members an earlier
// round left - it is the Slot's first attempt, and one of the Plans is not
// stopped. A retry reads them as stored instead, degraded, and completes;
// so does a Slot whose Plans are all stopped, every member written whole
// since; and a Slot without a word, as in the second stage.
func (coordinator *SlotExecutionCoordinator) segmentReadFails(request execution.SlotExecutionRequest, keys []segment.PlanKey) bool {
	if request.SegmentSkip == 0 || segmentRetry(request) {
		return false
	}
	for _, key := range keys {
		if !coordinator.segmentDegradations.stopped(request, key) {
			return true
		}
	}
	return false
}

// degradeSegments notes the Plans keys degraded by why in their owner's
// term and counts each once a Slot: one reaching it in a second Slot is
// counted stopped.
func (coordinator *SlotExecutionCoordinator) degradeSegments(request execution.SlotExecutionRequest, keys []segment.PlanKey,
	why string, facts *observability.SegmentShadowFacts) {
	for _, key := range keys {
		noted, stopped := coordinator.segmentDegradations.degrade(request, key)
		if noted {
			facts.Degrade(why)
		}
		if stopped {
			facts.Degrade(observability.SegmentDegradedStopped)
		}
	}
}

// emitSegmentFacts observes what the Slot's segments came to outside a
// round's fold, if anything.
func (coordinator *SlotExecutionCoordinator) emitSegmentFacts(ctx context.Context, operation execution.Operation,
	facts *observability.SegmentShadowFacts) {
	if facts.Empty() {
		return
	}
	coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.Operation(operation),
		ReasonCode: observability.ReasonNone, SegmentShadow: facts,
	})
}

// segmentsUnreadError is a Slot dispatched with a segment skip word whose
// stable segments did not read, in its first attempt (014 B3 §9.7): the
// members an earlier round left unwritten would read short. It fails by
// its own name, SEGMENT_READ_FAILED, retryable: the retry, failing again,
// reads them as stored.
type segmentsUnreadError struct{ why string }

// ErrSegmentsUnread is every such failure, whatever did not read.
var ErrSegmentsUnread = errors.New("alarmd worker: the Slot's stable segments did not read")

func (err segmentsUnreadError) Error() string { return ErrSegmentsUnread.Error() + ": " + err.why }
func (segmentsUnreadError) Unwrap() error     { return ErrSegmentsUnread }

func segmentsUnread(err error) bool {
	_, unread := SegmentReadFailedReason(err)
	return unread
}

// SegmentReadFailedReason is SEGMENT_READ_FAILED for a Slot failed for its
// segments not read.
func SegmentReadFailedReason(err error) (string, bool) {
	if errors.Is(err, ErrSegmentsUnread) {
		return contract.ReasonSegmentReadFailed, true
	}
	return "", false
}

// segmentDegradations is, by Query Group in its owner's term, the Plans
// degraded to the permanent class (014 B3 §9.7: segments that did not
// decode, a round's write not made, or a read-side failure a retry read
// through) and the Slot each last was in: a Plan degraded in a second Slot
// of the term is stopped - its members no longer left on this replica, a
// decodable read not clearing it - until the owner changes or the Query
// Group is released.
type segmentDegradations struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]*groupDegradations
}

type groupDegradations struct {
	epoch   uint64
	last    map[segment.PlanKey]execution.EvaluationTime
	times   map[segment.PlanKey]int
	stopped map[segment.PlanKey]bool
}

// termLocked is the Query Group's record in the request's owner's term,
// begun again when the term changed.
func (degradations *segmentDegradations) termLocked(request execution.SlotExecutionRequest) *groupDegradations {
	queryGroup := request.Contract.Slot.QueryGroup
	group := degradations.byGroup[queryGroup]
	if group == nil || group.epoch != request.OwnerFence.OwnerEpoch {
		group = &groupDegradations{epoch: request.OwnerFence.OwnerEpoch, last: map[segment.PlanKey]execution.EvaluationTime{},
			times: map[segment.PlanKey]int{}, stopped: map[segment.PlanKey]bool{}}
		if degradations.byGroup == nil {
			degradations.byGroup = map[execution.QueryGroupIdentity]*groupDegradations{}
		}
		degradations.byGroup[queryGroup] = group
	}
	return group
}

// degrade notes the Plan degraded in the request's Slot, once a Slot: it
// says whether this is the Slot's first, and whether it stopped the Plan.
func (degradations *segmentDegradations) degrade(request execution.SlotExecutionRequest, key segment.PlanKey) (noted, stopped bool) {
	degradations.mu.Lock()
	defer degradations.mu.Unlock()
	group := degradations.termLocked(request)
	slot := request.Contract.Slot.EvaluationTime
	if at, seen := group.last[key]; seen && at == slot {
		return false, false
	}
	group.last[key] = slot
	group.times[key]++
	if group.times[key] >= 2 && !group.stopped[key] {
		group.stopped[key] = true
		return true, true
	}
	return true, false
}

// stopped is whether the Plan is stopped in the request's owner's term.
func (degradations *segmentDegradations) stopped(request execution.SlotExecutionRequest, key segment.PlanKey) bool {
	degradations.mu.Lock()
	defer degradations.mu.Unlock()
	group := degradations.byGroup[request.Contract.Slot.QueryGroup]
	return group != nil && group.epoch == request.OwnerFence.OwnerEpoch && group.stopped[key]
}

func (degradations *segmentDegradations) forget(queryGroup execution.QueryGroupIdentity) {
	degradations.mu.Lock()
	defer degradations.mu.Unlock()
	delete(degradations.byGroup, queryGroup)
}

// followsSegmentSkip judges the Slot's segment skip word against the view
// this worker has installed, read once as the Slot begins, and counts a word
// it does not follow by why.
func (coordinator *SlotExecutionCoordinator) followsSegmentSkip(ctx context.Context, request execution.SlotExecutionRequest) bool {
	var epoch uint64
	var installed bool
	if read := coordinator.ports.InstalledControlEpoch; read != nil {
		epoch, installed = read()
	}
	follows, ignored := execution.SegmentSkipAdmitted(request.SegmentSkip, epoch, installed)
	if ignored != "" {
		var facts observability.SegmentShadowFacts
		facts.IgnoreSkipFlag(ignored)
		coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageMutationCompared,
			Result: observability.ResultSuccess, Operation: observability.Operation(request.Operation),
			ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
		})
	}
	return follows
}

// planKeys is the Slot's own Plans, in key order.
func (slot *segmentSlot) planKeys() []segment.PlanKey {
	keys := make([]segment.PlanKey, 0, len(slot.plans))
	for key := range slot.plans {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return segmentPlanName(keys[i]) < segmentPlanName(keys[j]) })
	return keys
}

// bookBytes is what the Slot's decoded books hold, at most (BookBytes).
func (slot *segmentSlot) bookBytes() uint64 {
	var bytes uint64
	for _, state := range slot.plans {
		bytes += segment.BookBytes(state.book.Len())
	}
	for _, state := range slot.carried {
		bytes += segment.BookBytes(state.book.Len())
	}
	return bytes
}

// skipSegmentShadow counts the Slot run without its shadow, by why.
func (coordinator *SlotExecutionCoordinator) skipSegmentShadow(ctx context.Context, operation execution.Operation, reason string) {
	var facts observability.SegmentShadowFacts
	facts.Skip(reason)
	coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageMutationCompared,
		Result: observability.ResultSuccess, Operation: observability.Operation(operation),
		ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
	})
}

// segmentCoverageAfterFold counts the coverages asked of books after their
// round folded into them: none, every State read of a Slot coming before
// its fold. A test reads it.
var segmentCoverageAfterFold atomic.Uint64

// segmentPlanGeneration is a Plan under one generation, its pieces' books
// together.
type segmentPlanGeneration struct {
	plan       execution.PlanIdentity
	generation execution.StateGeneration
}

// Coverage is a series' coverage by the books the Slot read, before its own
// round folds into them (014 B3 3.2 rule 3): its entry in its piece's book,
// with the position of the mark that book was written under and the Plan's
// grid. A Plan with no stored book - none yet, or one that did not decode -
// covers no member: their State reads as stored.
func (slot *segmentSlot) Coverage(identity execution.StateKeyIdentity) (segment.Coverage, bool) {
	if slot == nil {
		return segment.Coverage{}, false
	}
	if slot.folded.Load() {
		segmentCoverageAfterFold.Add(1)
	}
	for _, state := range slot.pieces[segmentPlanGeneration{plan: identity.Plan, generation: identity.StateGeneration}] {
		if !state.hadMark || state.fresh {
			continue
		}
		if entry, member := state.book.Entry(identity.SeriesIdentityDigest); member {
			return segment.Coverage{Entry: entry, Position: state.position, Interval: state.interval, Retention: state.retention}, true
		}
	}
	return segment.Coverage{}, false
}

func loadSegmentSlot(ctx context.Context, store segment.Store, request execution.SlotExecutionRequest, dues []segmentDue) *segmentSlot {
	slot := &segmentSlot{store: store, plans: map[segment.PlanKey]*segmentShadowPlan{},
		pieces: map[segmentPlanGeneration][]*segmentShadowPlan{}}
	if len(dues) == 0 {
		return slot
	}
	read := segment.ReadRequest{Contract: request.Contract, OwnerFence: request.OwnerFence}
	for _, due := range dues {
		read.Plans = append(read.Plans, segment.ReadPlan{Key: shadowKey(due.due, due.due.StateGeneration), Lifetime: due.lifetime})
	}
	// The generation a Plan carries from, after the Plans' own: a series
	// with no record of its own is read under it, and what that read found
	// is extended by its book as any read is.
	carries := make([]segmentDue, 0)
	for _, due := range dues {
		if from := carryFrom(due.due); from != "" {
			read.Plans = append(read.Plans, segment.ReadPlan{Key: shadowKey(due.due, from), Lifetime: due.lifetime})
			carries = append(carries, due)
		}
	}
	result, err := store.ReadSegments(ctx, read)
	if err != nil || len(result.Plans) != len(dues)+len(carries) {
		return nil
	}
	for index, stored := range result.Plans[len(dues):] {
		own := shadowKey(carries[index].due, carries[index].due.StateGeneration)
		if stored.Reason == segment.ReadFailed {
			slot.unread = append(slot.unread, own)
			continue
		}
		state, err := storedSegmentPlan(read.Plans[len(dues)+index].Key, carries[index], stored)
		if err != nil {
			slot.undecodable = append(slot.undecodable, own)
		}
		if state != nil {
			slot.carried = append(slot.carried, state)
			pieces := segmentPlanGeneration{plan: state.key.Plan, generation: state.key.Generation}
			slot.pieces[pieces] = append(slot.pieces[pieces], state)
		}
	}
	for index, stored := range result.Plans[:len(dues)] {
		due := dues[index]
		state := &segmentShadowPlan{key: read.Plans[index].Key, lifetime: due.lifetime, ttl: stored.TTL,
			interval: planInterval(due.due), retention: due.retention}
		if stored.Reason == segment.ReadFailed {
			slot.unread = append(slot.unread, state.key)
		}
		book, mark, had, err := stored.Book()
		switch {
		case err != nil:
			// What was lost is not known: the Plan starts over, its pages
			// past one removed by the count its mark named, when it read.
			slot.undecodable = append(slot.undecodable, state.key)
			book, _ = segment.NewBook(nil)
			state.fresh, state.lost = true, int64(stored.Members())
			if had {
				state.prior = mark.Pages
			}
		case !had:
			state.fresh = true
		default:
			state.position, state.mark, state.hadMark, state.prior = mark.Position, mark, true, mark.Pages
		}
		state.book = book
		slot.plans[read.Plans[index].Key] = state
		pieces := segmentPlanGeneration{plan: state.key.Plan, generation: state.key.Generation}
		slot.pieces[pieces] = append(slot.pieces[pieces], state)
	}
	return slot
}

// storedSegmentPlan is a carried generation's book as the Slot read it: nil
// for one with no stored mark or a book that did not decode - with why -
// which covers no member: read as stored.
func storedSegmentPlan(key segment.PlanKey, due segmentDue, stored segment.Stored) (*segmentShadowPlan, error) {
	book, mark, had, err := stored.Book()
	if err != nil || !had {
		return nil, err
	}
	return &segmentShadowPlan{key: key, lifetime: due.lifetime, ttl: stored.TTL, interval: planInterval(due.due), retention: due.retention,
		book: book, position: mark.Position, mark: mark, hadMark: true, prior: mark.Pages}, nil
}

// segmentsOf is the segments the Slot running under ctx read, nil for none.
func segmentsOf(ctx context.Context) *segmentSlot {
	if stream := retainedOwnerFrom(ctx); stream != nil {
		return stream.segments
	}
	return nil
}

// segmentShadowRound is one Slot attempt's shadow, folded into the books and
// written before the attempt's Progress (foldSegmentShadow): an attempt that
// fails before it leaves the books as they were, and a retry - finding a
// round some of whose series are already applied, their membership this
// round unknown - starts those Plans over.
type segmentShadowRound struct {
	slot  *segmentSlot
	plans map[segment.PlanKey]*segmentShadowPlanRound
	// skip is whether the round leaves members' State writes out (014 B3
	// stage 3): the Slot follows its segment skip word, no earlier attempt of
	// it failed its segments' write after leaving members (§9.4), and its
	// operation is a normal one - a retry, a replay, a supplement and a
	// probe write whole.
	skip bool
	// at is the Slot's evaluation time, the round's place in the day of a
	// Plan's sample round (sampleRound).
	at int64
}

type segmentShadowPlanRound struct {
	due       execution.DuePlan
	state     *segmentShadowPlan
	interval  int64
	retention uint32
	class     string
	// present is each series the round wrote, by the source time of its
	// last point; exceptions the open members it would have written whole;
	// compared the ones it would have left, by their last point, since the
	// mark W is the round's latest and known only at the fold; joins the
	// series decided steady and normal, by their last point too.
	present    map[execution.SeriesIdentityDigest]int64
	exceptions map[observability.SegmentShadowOutcome][]execution.SeriesIdentityDigest
	compared   map[int64]*segmentShadowTally
	joins      map[int64][]execution.SeriesIdentityDigest
	retried    bool
	// skipped is the members the round left unwritten.
	skipped int
	// sample is whether the round is the piece's sampled round of the day:
	// no member is left, every one written whole and compared (§9.2).
	sample bool
	// fallbackWritten is the members written whole because their page
	// went half its lifetime unrenewed, taken off their page (3.5).
	fallbackWritten []execution.SeriesIdentityDigest
}

type segmentShadowTally struct {
	members  []execution.SeriesIdentityDigest
	outcomes map[observability.SegmentShadowOutcome]int64
}

func newSegmentShadowRound(request execution.SlotExecutionRequest, slot *segmentSlot) *segmentShadowRound {
	// A supplement completes no Slot - a round's late points, not a round -
	// and a probe writes none.
	if slot == nil || request.Operation == execution.OperationSupplement || request.Operation == execution.OperationProbe {
		return nil
	}
	return &segmentShadowRound{slot: slot, plans: map[segment.PlanKey]*segmentShadowPlanRound{},
		skip: slot.skip && !slot.writeFailed && request.Operation == execution.OperationNormal,
		at:   int64(request.Contract.Slot.EvaluationTime)}
}

// sampleRound is whether the round at evaluation time at is its Plan's
// piece's sampled round of the day (014 B3 §9.2): (at + h) mod 86400 <
// interval, h the piece's hash modulo 86400. Once a day when the interval
// divides a day, at the same time; otherwise once each pass of the day,
// drifting; every round of a Plan run once a day or less often.
func sampleRound(key segment.PlanKey, at, interval int64) bool {
	if interval >= 86400 {
		return true
	}
	if interval <= 0 {
		return false
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(segmentPlanName(key)))
	offset := int64(hash.Sum64() % 86400)
	return ((at+offset)%86400+86400)%86400 < interval
}

// skipMode is what the round did with the Plan's members' State writes
// (segment.SkipModes): the Slot's reasons first, then the Plan's.
func (round *segmentShadowRound) skipMode(planRound *segmentShadowPlanRound) string {
	switch {
	case !round.slot.skip:
		return segment.SkipModeOff
	case round.slot.writeFailed:
		return segment.SkipModeWriteFailed
	case !round.skip:
		return segment.SkipModeOperation
	case planRound.state.stopped:
		return segment.SkipModeStopped
	case planRound.retried:
		return segment.SkipModeRetried
	case planRound.sample:
		return segment.SkipModeSampled
	default:
		return segment.SkipModeSkip
	}
}

// shadowRound is the round label the Plan's shadow outcomes are counted
// under: sampled in its piece's sampled round, else normal.
func (planRound *segmentShadowPlanRound) shadowRound() string {
	if planRound.sample {
		return observability.SegmentRoundSampled
	}
	return observability.SegmentRoundNormal
}

// onFallback is whether the member's page in the mark read is one the
// round writes whole (fallback).
func (state *segmentShadowPlan) onFallback(digest execution.SeriesIdentityDigest) bool {
	if len(state.fallback) == 0 || !state.hadMark {
		return false
	}
	page, err := segment.BucketOf(digest, state.mark.Pages)
	return err == nil && state.fallback[page]
}

// markFallback notes the pages of the mark read that the round writes
// whole: unrenewed for half their lifetime at now, in milliseconds.
func (state *segmentShadowPlan) markFallback(now int64) {
	if !state.hadMark || state.fresh {
		return
	}
	for page, renewal := range state.mark.Renewed {
		if renewal.Fallback(now) {
			if state.fallback == nil {
				state.fallback = map[int]bool{}
			}
			state.fallback[page] = true
		}
	}
}

// leaves is whether the round leaves the series' State write out (3.3 step
// 2): the round skips, its Plan's round was not retried, and the series is
// an open member, present on the grid, with none of the shadow's exceptions,
// whose made read matches what the round would write - the shadow's match.
// A series left is observed here, as observe counts a match, and the caller
// neither writes it nor observes it again; any other is written and observed
// as before.
func (round *segmentShadowRound) leaves(due execution.DuePlan, view execution.RuntimeStateView, mutation execution.StateMutation,
	class execution.SeriesClass, eventsSent bool) bool {
	if round == nil || !round.skip || len(mutation.Points) == 0 {
		return false
	}
	planRound := round.planRound(due, mutation.Identity, mutation.RetentionPoints)
	// A retried or sampled round writes every member and returns before the
	// fallback below: a member on a fallback page stays on it this round,
	// its key renewed by the write, and leaves it in the next round that
	// skips - one round later. Departing from one, it is renewed whole all
	// the same (round.skip && onFallback in the fold).
	// A stopped Plan leaves none (§9.7), nor one whose segments cannot be
	// written for want of a lifetime: its round's write would not be made.
	if planRound == nil || planRound.retried || planRound.sample || planRound.state.stopped || planRound.state.ttl <= 0 {
		return false
	}
	digest := mutation.Identity.SeriesIdentityDigest
	// A member on a page unrenewed for half its lifetime is written whole
	// and taken off its page: its key's life is no longer what the pages'
	// renewals promise (3.5 fallback).
	if planRound.state.onFallback(digest) {
		if entry, member := planRound.state.book.Entry(digest); member && entry.Open() {
			planRound.fallbackWritten = append(planRound.fallbackWritten, digest)
		}
		return false
	}
	// A closed entry - a member back after it departed - is never left: its
	// made read ends at its close, before the round's point, and so never
	// matches the write.
	entry, member := planRound.state.book.Entry(digest)
	if !member || planRound.wouldLeave(view, mutation, class, eventsSent) != "" {
		return false
	}
	last := mutation.Points[len(mutation.Points)-1].SourceTime
	outcomes := compareMadeRead(view, mutation, segment.Coverage{Entry: entry, Position: last,
		Interval: planRound.interval, Retention: planRound.retention})
	if len(outcomes) != 1 || outcomes[0] != observability.SegmentShadowMatch {
		return false
	}
	planRound.present[digest] = last
	tally := planRound.compared[last]
	if tally == nil {
		tally = &segmentShadowTally{outcomes: map[observability.SegmentShadowOutcome]int64{}}
		planRound.compared[last] = tally
	}
	tally.members = append(tally.members, digest)
	tally.outcomes[observability.SegmentShadowMatch]++
	planRound.skipped++
	return true
}

// planInterval is a Plan's evaluation interval in seconds, its schedule's
// and otherwise its compiled semantics'.
func planInterval(due execution.DuePlan) int64 {
	if due.ScheduleSpec.EvaluationIntervalSeconds > 0 {
		return due.ScheduleSpec.EvaluationIntervalSeconds
	}
	if due.CompiledPlan == nil {
		return 0
	}
	return int64(due.CompiledPlan.EvaluationSemantics().EvaluationInterval)
}

func (round *segmentShadowRound) planRound(due execution.DuePlan, identity execution.StateKeyIdentity, retention uint32) *segmentShadowPlanRound {
	key := shadowKey(due, identity.StateGeneration)
	if planRound := round.plans[key]; planRound != nil {
		return planRound
	}
	state := round.slot.plans[key]
	interval := planInterval(due)
	if state == nil || interval <= 0 || retention == 0 {
		return nil
	}
	planRound := &segmentShadowPlanRound{due: due, state: state, interval: interval, retention: retention,
		class: observability.SegmentPeriodClass(interval), present: map[execution.SeriesIdentityDigest]int64{},
		compared: map[int64]*segmentShadowTally{}, exceptions: map[observability.SegmentShadowOutcome][]execution.SeriesIdentityDigest{},
		joins: map[int64][]execution.SeriesIdentityDigest{}, sample: sampleRound(key, round.at, interval)}
	round.plans[key] = planRound
	return planRound
}

// retried is a Plan the attempt found series of applied by one before it,
// resumed before evaluation (an equal version, stream_session's resume): what
// the round did with them is not in this attempt, so the Plan's shadow starts
// over. Every series an equal version reaches is resumed there - real and
// no-data alike come through evaluateCompletedSeriesBatch - so the write's
// preflight, reading the same view, never finds one applied.
func (round *segmentShadowRound) retried(due execution.DuePlan) {
	if round == nil {
		return
	}
	key := shadowKey(due, due.StateGeneration)
	planRound := round.plans[key]
	if planRound == nil {
		state := round.slot.plans[key]
		if state == nil {
			return
		}
		planRound = &segmentShadowPlanRound{due: due, state: state, interval: state.interval, retention: state.retention,
			class: observability.SegmentPeriodClass(state.interval)}
		round.plans[key] = planRound
	}
	planRound.retried = true
}

// observe is one series the round wrote, against the view it was decided
// from.
func (round *segmentShadowRound) observe(due execution.DuePlan, view execution.RuntimeStateView, mutation execution.StateMutation,
	class execution.SeriesClass, eventsSent bool) {
	if round == nil || len(mutation.Points) == 0 {
		return
	}
	planRound := round.planRound(due, mutation.Identity, mutation.RetentionPoints)
	if planRound == nil || planRound.retried {
		return
	}
	digest := mutation.Identity.SeriesIdentityDigest
	last := mutation.Points[len(mutation.Points)-1].SourceTime
	planRound.present[digest] = last
	entry, member := planRound.state.book.Entry(digest)
	switch {
	case member && entry.Open():
		if exception := planRound.wouldLeave(view, mutation, class, eventsSent); exception != "" {
			planRound.exceptions[exception] = append(planRound.exceptions[exception], digest)
			return
		}
		tally := planRound.compared[last]
		if tally == nil {
			tally = &segmentShadowTally{outcomes: map[observability.SegmentShadowOutcome]int64{}}
			planRound.compared[last] = tally
		}
		tally.members = append(tally.members, digest)
		for _, outcome := range compareMadeRead(view, mutation, segment.Coverage{Entry: entry, Position: last,
			Interval: planRound.interval, Retention: planRound.retention}) {
			tally.outcomes[outcome]++
		}
	case member:
		// Back after it was closed: written whole, and off its page.
		planRound.exceptions[observability.SegmentShadowExceptionReturned] =
			append(planRound.exceptions[observability.SegmentShadowExceptionReturned], digest)
	case class == execution.SeriesClassStableNormal && !eventsSent:
		// Steady and normal and quiet, as a member must be to be left.
		planRound.joins[last] = append(planRound.joins[last], digest)
	}
}

// wouldLeave is the exception that keeps the skip from leaving an open
// member present this round unwritten (3.3 step 2), the first in order:
// events sent; not decided steady and normal with its stored Levels and
// guard unchanged (the write-reuse class decision_stable, the decision
// write_reuse counts); a Level not FULL or with a gap reason; its stored
// points not up to the mark before or the round's not on the grid after
// it. None, the skip would leave it.
func (planRound *segmentShadowPlanRound) wouldLeave(view execution.RuntimeStateView, mutation execution.StateMutation,
	class execution.SeriesClass, eventsSent bool) observability.SegmentShadowOutcome {
	if eventsSent {
		return observability.SegmentShadowExceptionEvents
	}
	if execution.ClassifyStateWriteReuse(view, mutation, class) != execution.StateWriteReuseDecisionStable {
		return observability.SegmentShadowExceptionUnsteady
	}
	for _, level := range mutation.Levels {
		if level.HistoryCompleteness != execution.HistoryFull || level.GapReasonCode != "" {
			return observability.SegmentShadowExceptionLevel
		}
	}
	if planRound.state.position == 0 || view.LastProcessedEventTime != planRound.state.position {
		return observability.SegmentShadowExceptionOffGrid
	}
	for index, point := range mutation.Points {
		if point.SourceTime != planRound.state.position+int64(index+1)*planRound.interval {
			return observability.SegmentShadowExceptionOffGrid
		}
	}
	return ""
}

// compareMadeRead is every field the read the skip would make for a member
// differs in from what the round wrote: the points made after the stored
// ones against the round's, and the Levels and guard as the made read holds
// them against the written. None is a match.
func compareMadeRead(view execution.RuntimeStateView, mutation execution.StateMutation, coverage segment.Coverage) []observability.SegmentShadowOutcome {
	made, end, extension := segment.Made(view, coverage)
	if extension != segment.Extended {
		return []observability.SegmentShadowOutcome{observability.SegmentShadowMismatchUnmade}
	}
	var outcomes []observability.SegmentShadowOutcome
	differs := func(outcome observability.SegmentShadowOutcome, differ bool) {
		if differ {
			outcomes = append(outcomes, outcome)
		}
	}
	times, records, facts := len(made) != len(mutation.Points), len(made) != len(mutation.Points), len(made) != len(mutation.Points)
	for index := 0; index < min(len(made), len(mutation.Points)); index++ {
		times = times || made[index].SourceTime != mutation.Points[index].SourceTime
		records = records || made[index].RecordID != mutation.Points[index].RecordID
		facts = facts || !reflect.DeepEqual(made[index].Levels, mutation.Points[index].Levels)
	}
	differs(observability.SegmentShadowMismatchPointTimes, times)
	differs(observability.SegmentShadowMismatchRecordIDs, records)
	differs(observability.SegmentShadowMismatchFacts, facts)
	levels := len(view.Levels) != len(mutation.Levels)
	var completeness, gap, compatibility, warmup, processed, lastReal bool
	for index := 0; !levels && index < len(view.Levels); index++ {
		stored, written := view.Levels[index], mutation.Levels[index]
		// The gap reason's origin is not compared, as the decision and the
		// digest leave it out (decisionStateChanged): it changes nothing a
		// later round decides on.
		levels = levels || stored.LevelID != written.LevelID
		completeness = completeness || stored.HistoryCompleteness != written.HistoryCompleteness
		gap = gap || stored.GapReasonCode != written.GapReasonCode
		compatibility = compatibility || stored.LevelStateCompatibility != written.LevelStateCompatibility
		warmup = warmup || stored.WarmupRequirementRef != written.WarmupRequirementRef
		processed = processed || written.LastProcessedEventTime != end
		// A Level that keeps no last real point (it ends on no data only
		// if it does) writes zero, and the made read leaves it zero.
		expectedReal := int64(0)
		if stored.LastRealPointTime != 0 {
			expectedReal = end
		}
		lastReal = lastReal || written.LastRealPointTime != expectedReal
	}
	differs(observability.SegmentShadowMismatchLevels, levels)
	differs(observability.SegmentShadowMismatchCompleteness, completeness)
	differs(observability.SegmentShadowMismatchGapReason, gap)
	differs(observability.SegmentShadowMismatchCompatibility, compatibility)
	differs(observability.SegmentShadowMismatchWarmupRef, warmup)
	differs(observability.SegmentShadowMismatchLastProcessed, processed)
	differs(observability.SegmentShadowMismatchLastReal, lastReal)
	differs(observability.SegmentShadowMismatchGuard, !reflect.DeepEqual(view.SeriesGuard, mutation.SeriesGuard))
	if len(outcomes) == 0 {
		return []observability.SegmentShadowOutcome{observability.SegmentShadowMatch}
	}
	return outcomes
}

// foldSegmentShadow folds an attempt's shadow into its Plans' books, renews
// their pages due and writes them, before the attempt's Progress (3.3,
// 3.5): the mark is the latest point the round wrote; the members compared
// at it are counted, those whose last point fell short of it are
// exceptions, as the skip would have found them off the grid; exceptions
// leave their page; the series decided steady and normal at the mark join
// it; the open members absent from the round close at the mark before; the
// closed ones past their reach expire; the pages due are renewed; the pages
// changed and the mark are written. A Plan whose output did not all land,
// or that the attempt found already applied in part, is not what the round
// decided: the first is left as it was, the second starts over. A write or
// renewal that fails is counted and lets the Plan's cache go: the Slot goes
// on, its State written whole.
//
// It answers whether the Slot's Progress must wait (014 B3 §9.4): a Plan
// whose round left members unwritten and whose write did not land -
// refused, failed, of unknown result, or never made. The Slot is noted, and
// its next attempt writes every member whole; a round that left none only
// counts a failed write, its State all written.
func (coordinator *SlotExecutionCoordinator) foldSegmentShadow(ctx context.Context, request execution.SlotExecutionRequest,
	round *segmentShadowRound, due []execution.DuePlan, withheld map[execution.PlanIdentity]struct{}) bool {
	if round == nil {
		return false
	}
	round.slot.folded.Store(true)
	facts, folded := foldSegmentShadowRound(round, due, withheld, coordinator.segmentJoinAdmission(ctx))
	writes := settleSegmentShadow(ctx, round.slot.store, request, folded, &facts, coordinator.now)
	landed := map[segment.PlanKey]bool{}
	if len(writes) > 0 {
		written, err := round.slot.store.WriteSegments(ctx, segment.WriteRequest{Contract: request.Contract,
			Fence: execution.StateApplyFence{Fence: request.OwnerFence, ContentScope: request.ContentScope}, Plans: writes})
		// A departing member's key found gone reads MISSING_WARMING when it
		// comes back: counted with the renewals' own (missing).
		for _, gone := range written.DepartedGone {
			facts.Renewal(observability.SegmentRenewalMissing, int64(gone))
		}
		// An error is a request the store could not take (segment.Store):
		// none of it landed, whatever its results say. The State store
		// answers every Plan failed then too, so the two agree.
		for index, result := range written.Plans {
			if err == nil && index < len(writes) && result == segment.WriteApplied {
				landed[writes[index].Key] = true
			}
		}
	}
	// A round's write not made is the permanent class (014 B3 §9.7): a Plan
	// it happens to in a second Slot of its owner's term is stopped.
	made := make(map[segment.PlanKey]bool, len(writes))
	for _, write := range writes {
		made[write.Key] = true
	}
	var absent []segment.PlanKey
	failed := false
	for _, fold := range folded {
		if fold.left > 0 && !landed[fold.state.key] {
			failed = true
		}
		if !made[fold.state.key] {
			absent = append(absent, fold.state.key)
		}
	}
	coordinator.degradeSegments(request, absent, observability.SegmentDegradedWriteAbsent, &facts)
	if failed {
		coordinator.segmentWriteFailures.note(request.Contract.Slot.QueryGroup)
	}
	if !facts.Empty() {
		coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageMutationCompared,
			Result: observability.ResultSuccess, Operation: observability.Operation(request.Operation),
			ReasonCode: observability.ReasonNone, SegmentShadow: &facts,
		})
	}
	return failed
}

// segmentWriteFailures is the Query Groups whose Slot's round left members
// unwritten and whose segments' write did not land (014 B3 §9.4): the next
// attempt on this replica writes every member whole, whatever its operation.
// Let go when the Query Group's Progress is committed - the Slot is written
// down - or it is released; a new owner has none and judges by the mark it
// reads.
type segmentWriteFailures struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]struct{}
}

func (failures *segmentWriteFailures) note(queryGroup execution.QueryGroupIdentity) {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	if failures.byGroup == nil {
		failures.byGroup = map[execution.QueryGroupIdentity]struct{}{}
	}
	failures.byGroup[queryGroup] = struct{}{}
}

// failed is whether the Query Group's segments' write failed since its
// Progress was last committed.
func (failures *segmentWriteFailures) failed(queryGroup execution.QueryGroupIdentity) bool {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	_, noted := failures.byGroup[queryGroup]
	return noted
}

func (failures *segmentWriteFailures) forget(queryGroup execution.QueryGroupIdentity) {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	delete(failures.byGroup, queryGroup)
}

// segmentJoinAdmission reserves the members a round joins as the books its
// Slot read were reserved (readSegments): quietly, the Slot going on whatever
// the pool says. Nil, every join admitted, for a Slot that holds nothing.
func (coordinator *SlotExecutionCoordinator) segmentJoinAdmission(ctx context.Context) func(int) bool {
	stream := retainedOwnerFrom(ctx)
	if stream == nil {
		return nil
	}
	return func(joins int) bool {
		bytes := uint64(joins) * segment.BookMemberBytes
		if err := coordinator.acquireProvisional(0, bytes, stream, "segment_book"); err != nil {
			return false
		}
		stream.retainBytes(retainPhaseState, bytes)
		return true
	}
}

// segmentFolded is one Plan's book as its round left it, to be renewed and
// written: the position of its mark; and, once settled, its pages laid out,
// the ones changed, their renewal table and the ones due.
type segmentFolded struct {
	state    *segmentShadowPlan
	due      execution.DuePlan
	position int64
	// skipped is whether the round left a member unwritten (Mark.Skipped);
	// mode and left what the round did with its members' State writes and
	// how many it left (segment.PlanWrite.SkipMode, Skipped).
	skipped bool
	mode    string
	left    int
	// departed is the members the round closed whose State keys go to the
	// full lifetime with the mark (segment.PlanWrite.Departed).
	departed []execution.SeriesIdentityDigest
	pages    [][]segment.Entry
	dirty    []int
	renewed  []segment.Renewal
	renew    []int
}

// foldSegmentShadowRound is foldSegmentShadow on the books: what it counted,
// and the books it changed. admit, when set, says whether a Plan's joins
// are taken in.
func foldSegmentShadowRound(round *segmentShadowRound, due []execution.DuePlan, withheld map[execution.PlanIdentity]struct{},
	admit func(joins int) bool) (observability.SegmentShadowFacts, []*segmentFolded) {
	var facts observability.SegmentShadowFacts
	if round == nil {
		return facts, nil
	}
	// A due Plan with a book and no series written this round is a round
	// all of whose members were absent: they close.
	for _, plan := range due {
		key := shadowKey(plan, plan.StateGeneration)
		if _, folded := round.plans[key]; folded {
			continue
		}
		if state := round.slot.plans[key]; state != nil && (state.hadMark || state.lost > 0) {
			round.plans[key] = &segmentShadowPlanRound{due: plan, state: state, interval: state.interval,
				retention: state.retention, class: observability.SegmentPeriodClass(state.interval),
				present: map[execution.SeriesIdentityDigest]int64{}, sample: sampleRound(key, round.at, state.interval)}
		}
	}
	var folded []*segmentFolded
	for _, planRound := range round.plans {
		if _, notLanded := withheld[planRound.due.Identity]; notLanded {
			continue
		}
		state, class := planRound.state, planRound.class
		facts.Member(observability.SegmentShadowReset, class, state.lost)
		state.lost = 0
		if planRound.retried {
			facts.Member(observability.SegmentShadowReset, class, int64(state.book.Len()))
			state.book, _ = segment.NewBook(nil)
			state.position, state.fresh = 0, true
			folded = append(folded, &segmentFolded{state: state, due: planRound.due, mode: round.skipMode(planRound)})
			continue
		}
		mark := state.position
		for _, last := range planRound.present {
			mark = max(mark, last)
		}
		if mark <= state.position && len(planRound.present) > 0 {
			// A round at or before the one folded last: a replay, not news.
			continue
		}
		if planRound.sample {
			facts.Sample(class)
		}
		book := state.book
		for last, tally := range planRound.compared {
			if last != mark {
				facts.Outcome(observability.SegmentShadowExceptionShortOfMark, class, planRound.shadowRound(), int64(len(tally.members)))
				for _, digest := range tally.members {
					_ = book.Remove(digest)
				}
				continue
			}
			for outcome, count := range tally.outcomes {
				facts.Outcome(outcome, class, planRound.shadowRound(), count)
			}
		}
		for exception, digests := range planRound.exceptions {
			facts.Outcome(exception, class, planRound.shadowRound(), int64(len(digests)))
			for _, digest := range digests {
				_ = book.Remove(digest)
			}
		}
		// Written whole for their page's fallback, they leave it: they join
		// again on a page renewed from now on.
		for _, digest := range planRound.fallbackWritten {
			_ = book.Remove(digest)
		}
		var departing []execution.SeriesIdentityDigest
		book.Each(func(entry segment.Entry) {
			if _, present := planRound.present[entry.Digest]; entry.Open() && !present {
				departing = append(departing, entry.Digest)
			}
		})
		// A member departing may have lived on the pages' renewals alone -
		// the round left one unwritten, or the round before did - and its
		// key's life is then what they left it, ttl/2 at the least: renewed
		// to the full lifetime with the mark, it comes back within that
		// lifetime to the window it left (014 B3 §9.1). Rounds that wrote
		// every member, this one and the one before, renew none, as in the
		// second stage.
		renewed := planRound.skipped > 0 || (state.hadMark && !state.fresh && state.mark.Skipped)
		var departed []execution.SeriesIdentityDigest
		for _, digest := range departing {
			if book.Close(digest, state.position) == nil {
				facts.Member(observability.SegmentShadowDeparted, class, 1)
				// A member departing from a page gone half its lifetime
				// unrenewed, in a round that skips, is renewed whole too.
				if renewed || (round.skip && state.onFallback(digest)) {
					departed = append(departed, digest)
				}
			}
		}
		// The members it joins are reserved as the books read were; the pool
		// refusing them, they join a later round, written whole as today.
		joins := planRound.joins[mark]
		if len(joins) > 0 && admit != nil && !admit(len(joins)) {
			facts.Skip(observability.SegmentShadowSkippedBookRefused)
			joins = nil
		}
		for _, digest := range joins {
			if book.Join(digest, mark) == nil {
				facts.Member(observability.SegmentShadowJoined, class, 1)
			}
		}
		reach := 2*int64(planRound.retention)*planRound.interval + segmentShadowHorizon
		if expired, err := book.Expire(mark - reach); err == nil {
			facts.Member(observability.SegmentShadowExpired, class, int64(expired))
		}
		folded = append(folded, &segmentFolded{state: state, due: planRound.due, position: mark, skipped: planRound.skipped > 0,
			departed: departed, mode: round.skipMode(planRound), left: planRound.skipped})
		state.position = mark
	}
	return facts, folded
}

// settleSegmentShadow lays the folded books out over their pages, renews
// the pages due (3.5) and returns the writes that leave them. A page is due
// once a quarter of the shorter of the lifetime its last renewal gave and
// this round's has passed by the worker's wall clock (rule 1); its open
// members' State keys are renewed, and its renewal time moves on to the
// wall clock read before the first of them was sent only if every segment
// carrying them answered (rule 3) - otherwise it stays, and the page is
// renewed whole next round. A page unrenewed for half its lifetime is
// counted fallback_write, a page each; in a round that skips, its present
// members were written whole (leaves) and leave it, its departing ones are
// renewed whole, and a round that writes every State does nothing more.
func settleSegmentShadow(ctx context.Context, store segment.Store, request execution.SlotExecutionRequest, folded []*segmentFolded,
	facts *observability.SegmentShadowFacts, clock func() time.Time) []segment.PlanWrite {
	now := clock().UnixMilli()
	var renewals []segment.PlanRenewal
	var renewing []*segmentFolded
	for _, fold := range folded {
		state := fold.state
		fold.dirty = state.book.Dirty()
		fold.pages = state.book.Pages()
		if state.fresh {
			fold.dirty = fold.dirty[:0]
			for page := range fold.pages {
				fold.dirty = append(fold.dirty, page)
			}
		}
		ttl := state.ttl.Milliseconds()
		if ttl <= 0 {
			continue
		}
		fold.renewed = segment.Renewals(state.mark, state.hadMark && !state.fresh, len(fold.pages), segmentPlanName(state.key), now, ttl)
		var pages []segment.PageRenewal
		for page, renewal := range fold.renewed {
			if renewal.Fallback(now) {
				// A page whose lifetime is no more than two of its Plan's
				// intervals goes half of it between two rounds: its fallback
				// every round is by design, not renewals failing - counted
				// apart from fallback_write, which the third stage's bar reads.
				result := observability.SegmentRenewalFallbackWrite
				if renewal.TTL/2 <= state.interval*1000 {
					result = observability.SegmentRenewalFallbackShortLife
				}
				facts.Renewal(result, 1)
			}
			if !renewal.Due(now, ttl) {
				continue
			}
			due := segment.PageRenewal{Page: page}
			for _, entry := range fold.pages[page] {
				if entry.Open() {
					due.Members = append(due.Members, entry.Digest)
				}
			}
			fold.renew = append(fold.renew, page)
			pages = append(pages, due)
		}
		if len(pages) > 0 {
			renewals = append(renewals, segment.PlanRenewal{Key: state.key, Lifetime: state.lifetime, Pages: pages})
			renewing = append(renewing, fold)
		}
	}
	if len(renewals) > 0 {
		at := clock().UnixMilli()
		result, err := store.RenewSegments(ctx, segment.RenewRequest{Contract: request.Contract,
			Fence: execution.StateApplyFence{Fence: request.OwnerFence, ContentScope: request.ContentScope}, Plans: renewals})
		for index, fold := range renewing {
			var plan segment.PlanRenewed
			if err == nil && index < len(result.Plans) {
				plan = result.Plans[index]
			}
			for position, page := range fold.renew {
				if position < len(plan.Renewed) && plan.Renewed[position] {
					fold.renewed[page] = segment.Renewed(at, fold.state.ttl.Milliseconds())
					facts.Renewal(observability.SegmentRenewalRenewed, 1)
					continue
				}
				facts.Renewal(observability.SegmentRenewalFailed, 1)
			}
			facts.Renewal(observability.SegmentRenewalMissing, int64(plan.Missing))
			facts.Renewal(observability.SegmentRenewalOverBudget, int64(plan.OverBudget))
		}
	}
	var writes []segment.PlanWrite
	for _, fold := range folded {
		if write, ok := segmentPlanWrite(fold, request.Contract, facts); ok {
			writes = append(writes, write)
		}
	}
	return writes
}

// segmentPlanName is what a Plan's new pages' renewal times are staggered
// by: a piece of a split strategy's with its matcher digest, a Plan not
// split's as it always was.
func segmentPlanName(key segment.PlanKey) string {
	name := fmt.Sprintf("%s/%s/%s/%s", key.Plan.TenantID, key.Plan.BusinessID, key.Plan.StrategyID, key.Generation)
	if !key.Shard.IsZero() {
		name += "/" + key.Shard.MatcherDigest
	}
	return name
}

// segmentPlanWrite is the write that leaves a Plan's book as its round
// changed and renewed it: the pages it changed - every page of a fresh book
// - and the mark at its position with the pages' renewal table and sizes.
// The pages written and their bytes are counted into facts. False writes
// nothing: a book that does not encode, or a Plan with no life to give its
// keys.
func segmentPlanWrite(fold *segmentFolded, contract execution.FrozenExecutionContractRef, facts *observability.SegmentShadowFacts) (segment.PlanWrite, bool) {
	state, pages := fold.state, fold.pages
	if state.ttl <= 0 || len(fold.renewed) != len(pages) {
		return segment.PlanWrite{}, false
	}
	encoded := make(map[int][]byte, len(fold.dirty))
	var bytes int64
	// A page not written keeps its size: the count did not change, or every
	// page would be written.
	sizes := make([]int, len(pages))
	if state.hadMark && !state.fresh && len(state.mark.Sizes) == len(pages) {
		copy(sizes, state.mark.Sizes)
	}
	for _, page := range fold.dirty {
		raw, err := segment.EncodePage(pages[page])
		if err != nil {
			return segment.PlanWrite{}, false
		}
		encoded[page] = raw
		sizes[page] = len(raw)
		bytes += int64(len(raw))
	}
	version, err := execution.BuildApplyVersion(contract, fold.due.StateApplyEpoch)
	if err != nil {
		return segment.PlanWrite{}, false
	}
	written := segment.Mark{Version: version, Position: fold.position, Pages: len(pages), Renewed: fold.renewed, Sizes: sizes,
		Skipped: fold.skipped}
	mark, err := segment.EncodeMark(written)
	if err != nil {
		return segment.PlanWrite{}, false
	}
	facts.Pages(observability.SegmentPeriodClass(state.interval), int64(len(fold.dirty)), bytes)
	state.book.Settle()
	write := segment.PlanWrite{Key: state.key, Lifetime: state.lifetime, Pages: encoded, Mark: mark, PageCount: len(pages),
		PriorCount: state.prior, Departed: fold.departed, SkipMode: fold.mode, Skipped: fold.left}
	// The book is as written: the next round of this Slot's attempt, were
	// there one, would start from it.
	state.mark, state.hadMark, state.fresh, state.prior = written, true, false, len(pages)
	return write, true
}
