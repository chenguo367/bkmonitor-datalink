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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Slot whose input does not fit its share is read in slices (decision-020
// batch 8 §7.2-7.5): every physical query of the Slot is run once a slice
// with the slice's conditions, the PRIMARY and its dependencies alike, so a
// series is read whole in exactly one slice and needs no join. A slice is
// evaluated, written and let go before the next one is read: its windows land
// as any window does, its inputs are dropped, its series stay in the present
// sets; the Slot's no-data judgement, its completion-only Plans and its tail
// come after the last slice.
//
// A slice whose input passes its limit while it streams is let go there and
// split by the next character from the end of its values (inputslice), the
// children read in its place in the same attempt; the slices already written
// are not touched, so the slices stay disjoint. The first slice is the whole
// Query Group unless this replica's last reading of it says more are needed.

// errSliceOverflow is a slice's input past its limit while it streams: the
// slice is let go and split, not the Slot refused.
var errSliceOverflow = errors.New("alarmd worker: the slice's input passed its limit")

// sliceSampleValues bounds the values a slice keeps of each dimension it may
// be split on, the sample its split classes are drawn from.
const sliceSampleValues = 4096

// sliceReading is the Slot read slice by slice.
type sliceReading struct {
	tree *inputslice.Tree
	// dimensions are the ones the tree splits on, in the order it takes them.
	dimensions []string
	leaf       inputslice.Leaf
	// index is how many slices have completed; last is whether the slice
	// being read is the last one pending.
	index int
	last  bool
	// input is what the slice being read holds of the input phase, largest
	// the most one of its series did, and sample its values on the
	// dimensions.
	input, largest uint64
	// series is the series the slice being read reserved of the pool, and
	// read the input every slice that landed held, in all.
	series, read uint64
	sample       map[string][]string
	seen         map[string]map[string]struct{}
	// unsliceable is a Slot found, at its first overflow, to have nothing it
	// can be sliced on: it is read whole and judged by its share alone, as
	// before slicing.
	unsliceable bool
	// unsupported is why it cannot be sliced (execution.InputSliceReason*).
	unsupported string
	// leaveInput ends the input phase of a slice after the first.
	leaveInput func()
	// landed is how many series of each Plan the slices written so far held,
	// and landedSlices how many of those slices held any.
	landed, landedSlices map[execution.PlanIdentity]uint64
	// unseenSlices and unseenSeries are the slices, and their series, a Plan
	// judged first-seen wrote before a later slice found it had run.
	unseenSlices, unseenSeries uint64
}

// slicing reports whether this Slot is read in slices.
func (stream *streamedExecution) slicing() bool { return stream.slices != nil }

// readInSlices reports whether this Slot's input is being read as more than
// one slice: a Slot read whole, its one read the Query Group, is not, and
// what its completion says of a Plan without series is the completion-only
// path's to decide.
func (stream *streamedExecution) readInSlices() bool {
	return stream.slicing() && stream.slices.tree != nil
}

// sliceLimit is what the slice being read may hold of the input phase: half
// of what its share leaves beside everything else the Slot holds, the other
// half for the windows that write it.
func (stream *streamedExecution) sliceLimit() uint64 {
	share := stream.coordinator.qgShareBytes()
	if share == 0 {
		return ^uint64(0)
	}
	others := stream.retainedTotal() - min(stream.retainedTotal(), stream.slices.input)
	if others >= share {
		return 0
	}
	return (share - others) / 2
}

// admitSliceInput judges one series batch of retained bytes against the
// slice's limit before it is reserved. A batch that takes the slice past its
// limit lets the slice go (errSliceOverflow) - unless one series the slice
// has seen alone does not fit the limit, which no split mends: the Slot is
// then refused by its share there (QG_BUDGET_SHARE_EXCEEDED, 7.3), with what
// it holds on the row, rather than split until K_max and named for its
// values, or read on whole into the half of the share its windows need. A
// Slot with nothing to slice on is read whole and judged by its share alone,
// as before slicing.
func (stream *streamedExecution) admitSliceInput(ctx context.Context, batch execution.SeriesExecutionBatch, retained uint64) error {
	if !stream.slicing() {
		return nil
	}
	if stream.slices.unsliceable {
		return nil
	}
	reading := stream.slices
	reading.largest = max(reading.largest, retained)
	limit := stream.sliceLimit()
	if reading.input+retained <= limit {
		return nil
	}
	if reading.tree == nil {
		if _, err := stream.sliceDimensions(); err != nil {
			reading.unsliceable = true
			reading.unsupported, _ = execution.InputSliceUnsupportedOf(err)
			return nil
		}
	}
	if reading.largest > limit {
		coordinator := stream.coordinator
		err := shareRejection(stream.reservationPhase("normal_input"), stream.retainedTotal(), retained,
			coordinator.qgShareBytes(), stream.ownBudgetUsage(coordinator.budget))
		coordinator.observeCapacityRejection(ctx, stream.request.Operation, observability.CapacityBudgetRetainedBytes, err)
		return err
	}
	stream.sampleSlice(batch)
	return errSliceOverflow
}

// sliceDimensions is what the Slot's physical queries can be sliced on, by
// their frozen facts, the PRIMARY requirements' first so the order is the
// Plan's own group-by (execution.InternalExecutionHeader.SliceableDimensions).
func (stream *streamedExecution) sliceDimensions() ([]string, error) {
	return stream.header.SliceableDimensions()
}

// noteSliceInput counts a batch reserved for the slice being read.
func (stream *streamedExecution) noteSliceInput(batch execution.SeriesExecutionBatch, retained uint64) {
	if !stream.slicing() || stream.slices.unsliceable {
		return
	}
	stream.slices.input += retained
	stream.slices.series += batch.Delivery.Series
	stream.sampleSlice(batch)
}

// sampleSlice keeps a batch's values on the slice's dimensions, from its
// PRIMARY Dataset: a dependency's series are handed on without dimensions.
func (stream *streamedExecution) sampleSlice(batch execution.SeriesExecutionBatch) {
	reading := stream.slices
	feedsPrimary := false
	for _, binding := range batch.Inputs {
		feedsPrimary = feedsPrimary || binding.Role == execution.InputRolePrimary
	}
	record, ok := batch.Dataset.Record(0)
	if !feedsPrimary || !ok {
		return
	}
	dimensions := record.Dimensions()
	if reading.sample == nil {
		reading.sample = make(map[string][]string, len(reading.dimensions))
		reading.seen = make(map[string]map[string]struct{}, len(reading.dimensions))
	}
	for _, dimension := range reading.dimensions {
		if len(reading.sample[dimension]) >= sliceSampleValues {
			continue
		}
		value := sliceValue(dimensions[dimension])
		seen := reading.seen[dimension]
		if seen == nil {
			seen = make(map[string]struct{})
			reading.seen[dimension] = seen
		}
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		reading.sample[dimension] = append(reading.sample[dimension], value)
	}
}

// sliceValue is a dimension's value as a label matcher reads it: the string
// a JSON string holds, a missing or null dimension the empty value, and any
// other scalar its own text.
func sliceValue(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	return string(raw)
}

// beginSlice readies the stream for the next slice's read: the streamed
// state of the slice before it has been dropped as its windows landed, and
// what is left of it is the Slot's.
func (stream *streamedExecution) beginSlice(leaf inputslice.Leaf, last bool) {
	reading := stream.slices
	reading.leaf, reading.last = leaf, last
	reading.input, reading.largest, reading.series, reading.sample, reading.seen = 0, 0, 0, nil, nil
	if reading.index > 0 {
		reading.leaveInput = stream.enter(slotPhaseInput)
	}
}

// abandonSlice lets go everything the slice being read streamed, for its
// children to read again, and returns the sample it kept.
func (stream *streamedExecution) abandonSlice() map[string][]string {
	reading := stream.slices
	if reading.leaveInput != nil {
		reading.leaveInput()
		reading.leaveInput = nil
	}
	stream.unretainInput(reading.input)
	stream.coordinator.releaseProvisional(reading.series, 0)
	stream.series -= min(stream.series, reading.series)
	stream.resetSliceStream()
	sample := reading.sample
	reading.input, reading.series, reading.sample, reading.seen = 0, 0, nil, nil
	return sample
}

// resetSliceStream clears the per-slice streamed state: the next slice's
// series arrive into empty maps, Begin having run once for the Slot.
func (stream *streamedExecution) resetSliceStream() {
	stream.streamed = make(map[streamedInputKey]streamedInput)
	stream.shared = sharedBindings{}
	stream.planSeries = make(map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]struct{})
	stream.inputHolds = nil
	stream.delivered = nil
	stream.hostOverlays = nil
}

// foldSliceCompletion folds one slice's completion into the Slot's: each
// physical query at the worst completeness any slice answered it with, its
// deliveries summed.
func foldSliceCompletion(slot, slice execution.QueryExecutionCompletion) (execution.QueryExecutionCompletion, error) {
	if len(slot.PhysicalQueries) == 0 {
		return slice, nil
	}
	index := make(map[execution.PhysicalQueryDigest]int, len(slot.PhysicalQueries))
	for position, query := range slot.PhysicalQueries {
		index[query.PhysicalQuery] = position
	}
	for _, query := range slice.PhysicalQueries {
		position, found := index[query.PhysicalQuery]
		if !found {
			slot.PhysicalQueries = append(slot.PhysicalQueries, query)
			continue
		}
		folded := &slot.PhysicalQueries[position]
		// A slice that held none of the query's series delivered nothing:
		// its zero delivery adds nothing to fold, and is not a delivery
		// AccumulateSeriesDelivery takes.
		delivery := folded.Delivery
		if query.Delivery.Series != 0 {
			var err error
			if delivery, err = execution.AccumulateSeriesDelivery(folded.Delivery, query.Delivery); err != nil {
				return execution.QueryExecutionCompletion{}, fmt.Errorf("alarmd worker: fold slice delivery: %w", err)
			}
		}
		if completenessRank(query.Completeness) > completenessRank(folded.Completeness) {
			*folded = query
		}
		folded.Delivery = delivery
	}
	slot.CompletionBindings = slice.CompletionBindings
	slot.AllRequiredCompleted = slot.AllRequiredCompleted && slice.AllRequiredCompleted
	if slot.InPlaceWait == nil {
		slot.InPlaceWait = slice.InPlaceWait
	}
	return slot, nil
}

func completenessRank(completeness execution.Completeness) int {
	switch completeness {
	case execution.CompletenessUnavailable:
		return 2
	case execution.CompletenessPartial:
		return 1
	default:
		return 0
	}
}

// sliceTrees keeps, by Query Group, the input bytes this replica read of it
// on its last completed Slot: the reading the next Slot's first slices are
// counted from.
type sliceTrees struct {
	mu    sync.Mutex
	input map[execution.QueryGroupIdentity]slicedReading
}

// slicedReading is one Query Group's last completed read on this replica.
type slicedReading struct {
	bytes      uint64
	dimensions []string
}

func (trees *sliceTrees) lastInput(group execution.QueryGroupIdentity) (uint64, []string) {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	reading := trees.input[group]
	return reading.bytes, reading.dimensions
}

func (trees *sliceTrees) remember(group execution.QueryGroupIdentity, bytes uint64, dimensions []string) {
	trees.mu.Lock()
	defer trees.mu.Unlock()
	if trees.input == nil {
		trees.input = make(map[execution.QueryGroupIdentity]slicedReading)
	}
	trees.input[group] = slicedReading{bytes: bytes, dimensions: append([]string(nil), dimensions...)}
}

// landSlice writes what is left of a slice before the last, as a window of
// its own, and lets its inputs go: the next slice is read into a Slot that
// holds only what it keeps whole. A Slot whose windows could not begin -
// its activations unread - cannot be read in slices and is retried.
func (stream *streamedExecution) landSlice(ctx context.Context) error {
	reading := stream.slices
	if reading.landed == nil {
		reading.landed = make(map[execution.PlanIdentity]uint64)
		reading.landedSlices = make(map[execution.PlanIdentity]uint64)
	}
	held := make(map[execution.PlanIdentity]struct{})
	for _, prepared := range stream.windows.prepared {
		if prepared.due != nil {
			reading.landed[prepared.due.Identity]++
			held[prepared.due.Identity] = struct{}{}
		}
	}
	for plan := range held {
		reading.landedSlices[plan]++
	}
	stream.windows.reached = len(stream.windows.prepared)
	if err := stream.cutWindow(ctx); err != nil {
		return err
	}
	if stream.windows.single {
		return errors.New("alarmd worker: a Slot read in slices could not begin its windows")
	}
	stream.windows.prepared, stream.windows.reached, stream.windows.dropped = nil, 0, 0
	return nil
}

// applySliceHead is a slice's own head, after the first (§7.5): in a
// Sequence of its own before the slice's first events, the open statements
// its series propose are folded into each Plan's marker - grown, never
// shrunk, so every event written is covered by a marker already down - and
// the first-seen judgement is carried on: a Plan judged first-seen that this
// slice finds had run is judged seen from this slice on, warming up, and the
// slices it wrote before as first-seen - the ones that held series of it; a
// slice that held none wrote nothing of it - and their series are counted on
// the completion row.
func (coordinator *SlotExecutionCoordinator) applySliceHead(ctx context.Context, stream *streamedExecution) error {
	windows := &stream.windows
	if !windows.started || windows.apply == nil {
		return nil
	}
	opens, err := stream.headOpenStatements()
	if err != nil {
		return err
	}
	apply := windows.apply
	return coordinator.ports.Sequencer.Sequence(ctx, sequencingScope(stream.header, nil, stream.gapItems), func(sequenceCtx context.Context) error {
		defer stream.enter(slotPhaseCut)()
		if candidates := firstSeenCandidates(apply.forced, stream.gaps); len(candidates) != 0 {
			before := stream.unseenPlans()
			if err := stream.firstSeenPrepass(sequenceCtx, candidates); err != nil {
				return err
			}
			after := stream.unseenPlans()
			if len(after) != len(before) {
				for key := range before {
					if _, still := after[key]; still {
						continue
					}
					for _, due := range stream.header.DuePlans {
						if due.Key() == key && stream.slices.landed[due.Identity] > 0 {
							stream.slices.unseenSlices += stream.slices.landedSlices[due.Identity]
							stream.slices.unseenSeries += stream.slices.landed[due.Identity]
						}
					}
				}
				apply.judgeForced(after)
			}
		}
		for _, due := range sortedDuePlans(stream.header.DuePlans) {
			if apply.changed(due) {
				apply.withheldNoData[due.Identity] = struct{}{}
				continue
			}
			open, found := opens[due.Identity]
			if !found {
				continue
			}
			if previous, written := windows.headStatements[due.Identity]; written {
				grown, grows, err := growOpenStatement(previous, open)
				if err != nil {
					return err
				}
				if !grows {
					continue
				}
				open = grown
			} else if err := coordinator.admit(sequenceCtx, stream.request, due); err != nil {
				return err
			}
			statement := execution.PlanEvaluationResult{Plan: due.Identity, GuardBeforeEvents: []execution.PlanGapMutation{open}}
			if _, err := apply.planGapsBefore(sequenceCtx, due, &statement); err != nil {
				return err
			}
			if windows.headOpened == nil {
				windows.headOpened = make(map[execution.PlanIdentity]struct{})
				windows.headStatements = make(map[execution.PlanIdentity]execution.PlanGapMutation)
			}
			windows.headOpened[due.Identity] = struct{}{}
			windows.headStatements[due.Identity] = open
		}
		return nil
	})
}

// growOpenStatement is the statement that grows the marker an earlier
// slice's head left (previous) by what this slice's open adds, and whether
// it adds anything. It is a second statement at the Slot's version, so it
// says only what the store extends a marker with at its own version
// (execution.ExtendSameSlotGap): a scope the marker does not hold, more
// FULL Slots on one it does, or a worse reason on one it does (UNAVAILABLE
// over PARTIAL, execution.GapReasonRank). Fewer FULL Slots or another reason
// of the same rank is no growth - the store refuses either alone as a
// CONFLICT - so the marker never shrinks and every event written stays
// under it.
func growOpenStatement(previous, open execution.PlanGapMutation) (execution.PlanGapMutation, bool, error) {
	if previous.Identity != open.Identity || previous.ScheduleRevision != open.ScheduleRevision ||
		execution.CompareApplyVersion(previous.ApplyVersion, open.ApplyVersion) != execution.ApplyVersionEqual {
		return execution.PlanGapMutation{}, false, errors.New("alarmd worker: two slices' open statements address different markers")
	}
	scopes := append([]execution.GapScopeMutation(nil), previous.Scopes...)
	grows := false
	for _, proposed := range open.Scopes {
		held := -1
		for index := range scopes {
			if scopes[index].Scope == proposed.Scope {
				held = index
			}
		}
		switch {
		case held < 0:
			scopes = append(scopes, proposed)
			grows = true
		default:
			if proposed.RequiredFullSlots > scopes[held].RequiredFullSlots {
				scopes[held].RequiredFullSlots = proposed.RequiredFullSlots
				grows = true
			}
			// A later slice answering a held scope worse: the worse reason
			// is what the marker says (execution.GapReasonRank).
			if execution.GapReasonRank(proposed.ReasonCode) > execution.GapReasonRank(scopes[held].ReasonCode) {
				scopes[held].ReasonCode = proposed.ReasonCode
				grows = true
			}
		}
	}
	if !grows {
		return previous, false, nil
	}
	sort.Slice(scopes, func(left, right int) bool {
		if scopes[left].Scope.HasLevel != scopes[right].Scope.HasLevel {
			return !scopes[left].Scope.HasLevel
		}
		return scopes[left].Scope.LevelID < scopes[right].Scope.LevelID
	})
	// The marker as the earlier head left it: one revision on.
	grown, err := execution.BuildPlanGapMutation(execution.PlanGapMutation{
		Identity: previous.Identity, ExpectedMarkerRevision: previous.ExpectedMarkerRevision + 1,
		ApplyVersion: previous.ApplyVersion, ScheduleRevision: previous.ScheduleRevision, Scopes: scopes,
	})
	if err != nil {
		return execution.PlanGapMutation{}, false, fmt.Errorf("alarmd worker: grow a slice's open statement: %w", err)
	}
	return grown, true, nil
}

// sliceReadingFor is how this Slot's input is read: slice by slice from its
// first read on, the first slice the whole Query Group, unless this
// replica's last completed Slot of it read more input than one slice may
// hold, in which case the first read is already the slices that reading
// asks for (§7.4). A supplement is read whole: it reads late series of a
// Slot already decided.
func (coordinator *SlotExecutionCoordinator) sliceReadingFor(request execution.SlotExecutionRequest) *sliceReading {
	if request.Supplement != nil {
		return nil
	}
	reading := &sliceReading{}
	share := coordinator.qgShareBytes()
	last, dimensions := coordinator.slicedInput.lastInput(request.Contract.Slot.QueryGroup)
	if share == 0 || last == 0 || len(dimensions) == 0 {
		return reading
	}
	leaves := int((last + share/2 - 1) / (share / 2))
	if leaves <= 1 {
		return reading
	}
	tree, err := inputslice.New(dimensions, min(leaves, inputslice.MaxLeaves-3))
	if err != nil {
		return reading
	}
	reading.tree, reading.dimensions = tree, dimensions
	return reading
}

// pending is the slices left to read: the whole Query Group while no slice
// has been split off it.
func (reading *sliceReading) pending() []inputslice.Leaf {
	if reading.tree == nil {
		return []inputslice.Leaf{{}}
	}
	return reading.tree.Pending()
}

// splitSlice lets the slice being read go and splits it, the children read
// in its place. The first split of a Slot read whole makes the tree, on the
// dimensions every physical query of the Slot groups by. It returns why the
// Slot cannot be read in slices when it cannot: no dimension to split on
// (execution.InputSliceUnsupportedError), or a slice that splitting cannot
// make fit (inputslice.ErrValueConcentrated).
func (stream *streamedExecution) splitSlice(leaf inputslice.Leaf) error {
	reading := stream.slices
	sample := stream.abandonSlice()
	if reading.tree == nil {
		dimensions, err := stream.sliceDimensions()
		if err != nil {
			return err
		}
		tree, err := inputslice.New(dimensions, 1)
		if err != nil {
			return err
		}
		reading.tree, reading.dimensions = tree, dimensions
		leaf = tree.Pending()[0]
	}
	if _, err := reading.tree.Split(leaf.ID, sample); err != nil {
		return fmt.Errorf("alarmd worker: split input slice: %w", err)
	}
	return nil
}

// landSliceRead counts the slice just completed as landed and reports
// whether it was the last.
func (reading *sliceReading) landSliceRead(leaf inputslice.Leaf) bool {
	reading.read += reading.input
	if reading.tree == nil {
		return true
	}
	_ = reading.tree.Land(leaf.ID)
	reading.index++
	return len(reading.tree.Pending()) == 0
}

// rememberSlotInput keeps what this Slot read, for the next Slot of its
// Query Group on this replica to count its first slices from: the bytes its
// slices held of the input phase in all, and the dimensions it split on.
func (coordinator *SlotExecutionCoordinator) rememberSlotInput(request execution.SlotExecutionRequest, stream *streamedExecution) {
	if !stream.slicing() {
		return
	}
	reading := stream.slices
	coordinator.slicedInput.remember(request.Contract.Slot.QueryGroup, reading.read, reading.dimensions)
}

// InputSliceRefusal names why a Slot that has to be read in slices cannot be,
// when err says so: the store, the query or a dimension it cannot be sliced
// on (INPUT_SLICE_UNSUPPORTED), or values no slice can hold
// (INPUT_SLICE_VALUE_CONCENTRATED).
func InputSliceRefusal(err error) (string, bool) {
	if _, unsupported := execution.InputSliceUnsupportedOf(err); unsupported {
		return contract.ReasonInputSliceUnsupported, true
	}
	if errors.Is(err, inputslice.ErrValueConcentrated) {
		return contract.ReasonInputSliceValueConcentrated, true
	}
	var tooLong *inputSliceQueryTooLongError
	if errors.As(err, &tooLong) {
		return contract.ReasonInputSliceQueryTooLong, true
	}
	return "", false
}

// slicesRead, slicesDiscarded, unseenSlices and unseenSeries are the
// completion row's counts of how the Slot's input was read.
func (stream *streamedExecution) slicesRead() uint64 {
	if !stream.slicing() {
		return 0
	}
	if stream.slices.tree == nil {
		return 1
	}
	return uint64(stream.slices.index)
}

func (stream *streamedExecution) slicesDiscarded() uint64 {
	if !stream.slicing() || stream.slices.tree == nil {
		return 0
	}
	return uint64(stream.slices.tree.Discarded())
}

func (stream *streamedExecution) unseenSlices() uint64 {
	if !stream.slicing() {
		return 0
	}
	return stream.slices.unseenSlices
}

func (stream *streamedExecution) unseenSeries() uint64 {
	if !stream.slicing() {
		return 0
	}
	return stream.slices.unseenSeries
}

// sliceCeilingFactor is how many times its first slice's windows a Slot's
// deadline may move: once for each slice read and each let go, at most
// 2*K_max-1 - every query's round trips when the tree is as split as it may
// be. One for a Slot read whole.
func (stream *streamedExecution) sliceCeilingFactor() uint64 {
	if !stream.slicing() || stream.slices.tree == nil {
		return 1
	}
	return min(uint64(stream.slices.tree.Leaves()+stream.slices.tree.Discarded()), 2*inputslice.MaxLeaves-1)
}

// sliceQueryTooLong is a slice whose read the store refused as longer than
// it takes: the slice's conditions made the query so, and the Slot cannot
// be read in that slice (INPUT_SLICE_QUERY_TOO_LONG). A read of the whole
// Query Group refused so is the query's own length, and stays the query's
// rejection.
func sliceQueryTooLong(slice execution.InputSlice, completion execution.QueryExecutionCompletion) error {
	if slice.IsZero() {
		return nil
	}
	for _, query := range completion.PhysicalQueries {
		for _, attempt := range query.RouteFacts.Attempts {
			if attempt.Rejection != nil && attempt.Rejection.Class == execution.RejectionClassQueryTooLong {
				return &inputSliceQueryTooLongError{bytes: slice.ConditionBytes()}
			}
		}
	}
	return nil
}

// inputSliceQueryTooLongError is a slice whose conditions, of bytes bytes,
// made its query longer than the store takes.
type inputSliceQueryTooLongError struct{ bytes int }

func (err *inputSliceQueryTooLongError) Error() string {
	return fmt.Sprintf("alarmd worker: the store refused an input slice's query as too long (the slice's conditions are %d bytes)", err.bytes)
}

// unsliceableShareRefusal is a Slot that could not be sliced refused by its
// share: the share's refusal, its facts and its finalization as they are
// (DeterministicShareRefusal), named for why the Slot was read whole
// (INPUT_SLICE_UNSUPPORTED with the reason on the query failure line).
type unsliceableShareRefusal struct {
	unsupported execution.InputSliceUnsupportedError
	share       error
}

func (err *unsliceableShareRefusal) Error() string {
	return err.unsupported.Error() + ", read whole: " + err.share.Error()
}

func (err *unsliceableShareRefusal) Unwrap() []error {
	return []error{&err.unsupported, err.share}
}

// nameUnsliceableRefusal is err, a refusal of the input the Slot is
// reading, named for why the Slot was read whole when it could not be
// sliced and its share refused it.
func (stream *streamedExecution) nameUnsliceableRefusal(err error) error {
	var exceeded *provisionalBudgetExceededError
	if !stream.slicing() || !stream.slices.unsliceable || !errors.As(err, &exceeded) || !exceeded.share {
		return err
	}
	return &unsliceableShareRefusal{unsupported: execution.InputSliceUnsupportedError{Reason: stream.slices.unsupported}, share: err}
}

// finalSliceRefusal is err, a Slot whose slices no split separates
// (INPUT_SLICE_VALUE_CONCENTRATED) or whose slice the store will not take
// (INPUT_SLICE_QUERY_TOO_LONG), as the deterministic refusal the scheduler
// finalizes a Slot on, the way it does one past its share: the same window
// read again splits the same way and is refused the same way, and every
// retry reads the slices landed before it again. Its code and why stay on
// the refusal (execution.ShareRefusal Reason and Detail). Any other error
// is err.
func (stream *streamedExecution) finalSliceRefusal(err error) error {
	word, sliced := InputSliceRefusal(err)
	if !sliced || word == contract.ReasonInputSliceUnsupported || !stream.slicing() {
		return err
	}
	refusal := execution.ShareRefusal{Phase: "input_slice", OwnUsedBytes: stream.slices.read,
		LimitBytes: stream.coordinator.qgShareBytes(), Reason: word}
	var concentrated *inputslice.ValueConcentratedError
	var tooLong *inputSliceQueryTooLongError
	switch {
	case errors.As(err, &concentrated):
		refusal.Detail = concentrated.Reason + " on " + concentrated.Dimension
	case errors.As(err, &tooLong):
		refusal.Detail = fmt.Sprintf("conditions of %d bytes", tooLong.bytes)
	}
	return &finalSliceRefusalError{err: err, refusal: refusal}
}

// finalSliceRefusalError is a slice refusal the scheduler finalizes on.
type finalSliceRefusalError struct {
	err     error
	refusal execution.ShareRefusal
}

func (err *finalSliceRefusalError) Error() string { return err.err.Error() }

func (err *finalSliceRefusalError) Unwrap() error { return err.err }

func (err *finalSliceRefusalError) DeterministicShareRefusal() (execution.ShareRefusal, bool) {
	return err.refusal, true
}
