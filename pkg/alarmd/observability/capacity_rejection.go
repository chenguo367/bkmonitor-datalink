package observability

import "time"

// CapacityRejectionFacts is a log-only snapshot of one failed shared
// reservation. Values use CapacityBudget units; nil OwnUsed means unknown,
// not zero. No query, key, URL, or raw error is accepted here.
type CapacityRejectionFacts struct {
	Phase                        string
	OwnUsed                      *uint64
	SharedUsed, Requested, Limit uint64
	// OthersUsed is what others held of the pool when it refused a Query
	// Group's Slot within its share (RESOURCE_POOL_HELD_BY_OTHERS): the pool's
	// reserved bytes less the Slot's own. Unset on every other refusal.
	OthersUsed *uint64
	// Usage is what this execution had taken of every budget at the moment one
	// of them refused, each against the limit it was measured with.
	//
	// Without it a rejection names only the budget that was reached, and
	// "the count refused while the byte budget was ninety percent free" cannot
	// be said - which is the one sentence that distinguishes a process at its
	// memory limit from a process stopped by a number standing in for memory.
	// Reading it from the ceilings in /metrics does not work either: those give
	// the limits, never what this Slot had of them.
	Usage []CapacityBudgetUsage
}

// CapacityBudgetUsage is one budget's own usage at the moment of a rejection,
// beside the limit it was compared against.
type CapacityBudgetUsage struct {
	Budget  CapacityBudget
	OwnUsed uint64
	Limit   uint64
}

func normalizeCapacityRejection(observation Observation) *CapacityRejectionFacts {
	if observation.Component != ComponentResource || observation.Stage != StageResourceHard ||
		observation.Err == nil || observation.CapacityBudget == "" || observation.CapacityRejection == nil {
		return nil
	}
	f := *observation.CapacityRejection
	switch f.Phase {
	case "normal_input", "normal_gap", "normal_output", "normal_no_data", "slot_output", "query_free", "snapshot_prepare":
	default:
		f.Phase = "other"
	}
	if f.OwnUsed != nil {
		own := *f.OwnUsed
		f.OwnUsed = &own
	}
	// Copied, and only for budgets this build names: the facts arrive from the
	// caller's own struct, and a row is a log line rather than a place to
	// forward whatever a future budget calls itself.
	if len(f.Usage) != 0 {
		usage := make([]CapacityBudgetUsage, 0, len(f.Usage))
		for _, entry := range f.Usage {
			if !knownCapacityBudget(entry.Budget) {
				continue
			}
			usage = append(usage, entry)
		}
		f.Usage = usage
	}
	return &f
}

func knownCapacityBudget(budget CapacityBudget) bool {
	switch budget {
	case CapacityBudgetSeries, CapacityBudgetRetainedBytes, CapacityBudgetStateMutations,
		CapacityBudgetEvents, CapacityBudgetGapMutations:
		return true
	}
	return false
}

// SlotBudgetUsageFacts is what one Slot took of each budget, reported on its
// completion row whatever the outcome.
//
// It is on the completion row rather than only on rejections because a number
// that exists only when something failed has no distribution behind it: there
// is nothing to take a median of, so "this budget is the one under pressure"
// cannot be told from "this budget is nowhere near its limit". Both halves are
// carried - what was used and what it was measured against - because a usage
// without its limit is not a reading, and the limits in /metrics are the
// process ceilings rather than what this Slot was admitted against.
type SlotBudgetUsageFacts struct {
	StateMutations uint64 `json:"state_mutations"`
	GapMutations   uint64 `json:"gap_mutations"`
	Events         uint64 `json:"events"`
	// EventsWithoutMessage is the events decided and not kept because their
	// protocol has no message for them; Events plus this is what Events
	// counted before a Python-compatible RECOVERY stopped being held.
	EventsWithoutMessage uint64 `json:"events_without_message"`
	RetainedBytes        uint64 `json:"retained_bytes"`
	Series               uint64 `json:"series"`
	// DependencySeriesDiscarded is the series a dependency query answered
	// that no PRIMARY input of the same Plan named, let go at the completion
	// (input_series_discarded_total{reason="dependency_without_primary"}).
	DependencySeriesDiscarded uint64 `json:"dependency_series_discarded"`
	// PointsDropped is the points the Slot's answers' normalization let go
	// rather than evaluate, by reason - the Query Group's own share of
	// access_query_points_dropped_total. {} is none let go.
	PointsDropped PointDrops `json:"points_dropped"`
	// QueryWaitMillis and QueryBodyMillis are the Slot's physical queries'
	// time before their answers' first byte - the provider running them -
	// and after it - read and decoded by alarmd - summed over the queries;
	// the Max fields the longest query's. The Query Group's own share of
	// access_query_answer_phase_seconds.
	QueryWaitMillis    uint64 `json:"query_wait_millis"`
	QueryBodyMillis    uint64 `json:"query_body_millis"`
	QueryWaitMillisMax uint64 `json:"query_wait_millis_max"`
	QueryBodyMillisMax uint64 `json:"query_body_millis_max"`
	// InputSlices is the slices the Slot's input was read in, one for a Slot
	// read whole, and InputSlicesDiscarded the ones let go and split while
	// they streamed. FirstSeenUnseenSlices and FirstSeenUnseenSeries are the
	// slices, and their series, a Plan judged first-seen wrote before a later
	// slice found it had run: they were written without warming up.
	InputSlices           uint64 `json:"input_slices"`
	InputSlicesDiscarded  uint64 `json:"input_slices_discarded"`
	FirstSeenUnseenSlices uint64 `json:"first_seen_unseen_slices"`
	FirstSeenUnseenSeries uint64 `json:"first_seen_unseen_series"`
	// InputSlicesReadAhead is the slices read while the slice before
	// completed and handed to the Slot; the other three the reads ahead let
	// go, by why: the next slice was another, the read yielded to the Slot's
	// own reservation, or it failed. Each let go was read again after.
	// SliceReadAheadOutcomes names the four, in this order.
	InputSlicesReadAhead          uint64 `json:"input_slices_read_ahead,omitempty"`
	InputSlicesReadAheadOtherLeaf uint64 `json:"input_slices_read_ahead_other_leaf,omitempty"`
	InputSlicesReadAheadYielded   uint64 `json:"input_slices_read_ahead_yielded,omitempty"`
	InputSlicesReadAheadFailed    uint64 `json:"input_slices_read_ahead_failed,omitempty"`
	// InputSlicesReadAheadContextEnded is the reads ahead refused, their read
	// having ended with its context ended (SliceReadAheadOutcomes'
	// context_ended).
	InputSlicesReadAheadContextEnded uint64 `json:"input_slices_read_ahead_context_ended,omitempty"`
	// SeriesNotEvaluated is the PRIMARY series the Slot bound to a Plan that
	// never reached its evaluation, its resume or its skip, by the Plan's
	// algorithm family (AlgorithmFamily, or none): absent is none lost.
	// In the order of SeriesNotEvaluatedFamilies.
	SeriesNotEvaluated [SeriesNotEvaluatedFamilyCount]uint64 `json:"series_not_evaluated"`
	// ReadAheadYields<Phase> is the reads ahead yielded by the phase of the
	// reservation they yielded to (SliceReadAheadYieldPhases, in this order),
	// together InputSlicesReadAheadYielded; ReadAheadYieldedBytes what they
	// held, ReadAheadYieldDistanceMax the farthest of them from the slice
	// read, in slices, ReadAheadWaitMillisMax the longest wait for room.
	ReadAheadYieldsNormalInput  uint64 `json:"read_ahead_yields_normal_input,omitempty"`
	ReadAheadYieldsNormalOutput uint64 `json:"read_ahead_yields_normal_output,omitempty"`
	ReadAheadYieldsNormalGap    uint64 `json:"read_ahead_yields_normal_gap,omitempty"`
	ReadAheadYieldsNormalNoData uint64 `json:"read_ahead_yields_normal_no_data,omitempty"`
	ReadAheadYieldsSlotOutput   uint64 `json:"read_ahead_yields_slot_output,omitempty"`
	ReadAheadYieldsFrames       uint64 `json:"read_ahead_yields_frames,omitempty"`
	ReadAheadYieldsWindowBound  uint64 `json:"read_ahead_yields_window_bound,omitempty"`
	ReadAheadYieldsQueryFree    uint64 `json:"read_ahead_yields_query_free,omitempty"`
	ReadAheadYieldsOther        uint64 `json:"read_ahead_yields_other,omitempty"`
	ReadAheadYieldedBytes       uint64 `json:"read_ahead_yielded_bytes,omitempty"`
	ReadAheadYieldDistanceMax   uint64 `json:"read_ahead_yield_distance_max,omitempty"`
	ReadAheadWaitMillisMax      uint64 `json:"read_ahead_wait_millis_max,omitempty"`
	// InputSliceTail* and InputSliceChange* time the seams of the Slot's
	// reads, summed and the longest; SharedFrameWait* are its shared-schema
	// answers' frame waits, summed (execution.SlotBudgetUsage).
	InputSliceTailMillis          uint64 `json:"input_slice_tail_millis,omitempty"`
	InputSliceTailMillisMax       uint64 `json:"input_slice_tail_millis_max,omitempty"`
	InputSliceChangeMillis        uint64 `json:"input_slice_change_millis,omitempty"`
	InputSliceChangeMillisMax     uint64 `json:"input_slice_change_millis_max,omitempty"`
	SharedFrameWaitConsumerMillis uint64 `json:"shared_frame_wait_consumer_millis,omitempty"`
	SharedFrameWaitReaderMillis   uint64 `json:"shared_frame_wait_reader_millis,omitempty"`
	// InputSliceTimeouts is the slices that ran out of time and were split
	// and read again, the Slot completing: at most one a Slot.
	InputSliceTimeouts uint64 `json:"input_slice_timeouts,omitempty"`
	// InputSliceShape* is what the Slot did with its Query Group's shape,
	// one each at most (InputSliceShapeOutcomes).
	InputSliceShapeStarted              uint64 `json:"input_slice_shape_started,omitempty"`
	InputSliceShapeKept                 uint64 `json:"input_slice_shape_kept,omitempty"`
	InputSliceShapeMerged               uint64 `json:"input_slice_shape_merged,omitempty"`
	InputSliceShapeVoidedRevision       uint64 `json:"input_slice_shape_voided_revision,omitempty"`
	InputSliceShapeVoidedDimensions     uint64 `json:"input_slice_shape_voided_dimensions,omitempty"`
	InputSliceShapeVoidedMaxLeaves      uint64 `json:"input_slice_shape_voided_max_leaves,omitempty"`
	InputSliceShapeRefusedNotALeaf      uint64 `json:"input_slice_shape_refused_not_a_leaf,omitempty"`
	InputSliceShapeRefusedNotAPartition uint64 `json:"input_slice_shape_refused_not_a_partition,omitempty"`
	InputSliceShapeRefusedMaxLeaves     uint64 `json:"input_slice_shape_refused_max_leaves,omitempty"`
	InputSliceShapeEvictedIdle          uint64 `json:"input_slice_shape_evicted_idle,omitempty"`
	InputSliceShapeRefusedCondition     uint64 `json:"input_slice_shape_refused_condition_budget,omitempty"`
	InputSliceShapeRefusedSingleValue   uint64 `json:"input_slice_shape_refused_single_value,omitempty"`
	InputSliceShapeRefusedSplitError    uint64 `json:"input_slice_shape_refused_split_error,omitempty"`
	InputSliceShapeMergeRefused         uint64 `json:"input_slice_shape_merge_refused,omitempty"`
	InputSliceShapeSizedKept            uint64 `json:"input_slice_shape_sized_kept,omitempty"`
	// InputSliceCutOn is where the dimension the Slot's starting slices
	// were cut on came from (InputSliceCutOnSources), empty for a Slot not
	// started in slices counted: read whole, or in its Query Group's shape.
	InputSliceCutOn string `json:"input_slice_cut_on,omitempty"`
	// InputSliceDimension is the dimension the Slot's slices were cut on at
	// their root, however they started - cut counted, or in the Query
	// Group's shape - empty for a Slot read whole.
	InputSliceDimension string `json:"input_slice_dimension,omitempty"`
	// InputSlicesDeferred is the slices read last for the provider having
	// timed them out the Slot before, completing degraded: judged against
	// the slices read before them.
	InputSlicesDeferred uint64 `json:"input_slices_deferred,omitempty"`
	// InputSliceStartDiscarded is 1 when a slice the Slot started in from
	// its memory - the cut a read before chose, or its shape - was let go
	// for its size: the cut did not fit the input.
	InputSliceStartDiscarded uint64 `json:"input_slice_start_discarded,omitempty"`
	// NoDataStagesAhead* is how the Slot's no-data first writes staged as
	// their windows landed ended, a Plan each (NoDataStageAheadOutcomes);
	// NoDataStageAheadDiffers* the differs among them by why
	// (nodata.StagedDiffersReasons); NoDataGroupsStagedAhead the groups
	// they staged.
	NoDataStagesAheadCarried             uint64 `json:"no_data_stages_ahead_carried,omitempty"`
	NoDataStagesAheadDiffers             uint64 `json:"no_data_stages_ahead_differs,omitempty"`
	NoDataStagesAheadDiscarded           uint64 `json:"no_data_stages_ahead_discarded,omitempty"`
	NoDataStagesAheadStopped             uint64 `json:"no_data_stages_ahead_stopped,omitempty"`
	NoDataStageAheadDiffersKeptOtherwise uint64 `json:"no_data_stage_ahead_differs_kept_otherwise,omitempty"`
	NoDataStageAheadDiffersCountMismatch uint64 `json:"no_data_stage_ahead_differs_count_mismatch,omitempty"`
	NoDataStageAheadDiffersPresentAsOf   uint64 `json:"no_data_stage_ahead_differs_present_as_of,omitempty"`
	NoDataGroupsStagedAhead              uint64 `json:"no_data_groups_staged_ahead,omitempty"`

	// RetainedBytes split by what the memory was held for, summing to it.
	//
	// These stay on the row where the limits do not, because the limits are
	// process constants a reader can look up once while these are this Slot's
	// own and vary round to round. Reported at every value including zero: a
	// phase that is usually nothing and occasionally the whole budget is the
	// one worth finding, and omitting its zeros would leave it with no
	// denominator to be occasional against.
	RetainedInputBytes  uint64 `json:"retained_input_bytes"`
	RetainedGapBytes    uint64 `json:"retained_gap_bytes"`
	RetainedOutputBytes uint64 `json:"retained_output_bytes"`
	RetainedStateBytes  uint64 `json:"retained_state_bytes"`

	// The limits stay off the row and are carried for readers that hold the
	// facts rather than the log line.
	//
	// They are process constants: every Slot on a replica is measured against
	// the same five, they are already published once per process, and repeating
	// them on every completion row doubles the row for nothing. At one row per
	// object per Slot that is most of a gigabyte a day to restate numbers that
	// did not change.
	StateMutationsLimit uint64 `json:"-"`
	GapMutationsLimit   uint64 `json:"-"`
	EventsLimit         uint64 `json:"-"`
	RetainedBytesLimit  uint64 `json:"-"`
	SeriesLimit         uint64 `json:"-"`
	// RetainedShareBytes is the one Query Group's share of the retained pool
	// this Slot was admitted against, from the producer that refuses by it.
	RetainedShareBytes uint64 `json:"-"`

	// Windows is how many windows the Slot was applied in, zero for one
	// applied whole; WindowCeilingWindows how many the watchdog's ceiling
	// allows it, and WindowCeilingRaisedWindows how many of those a no-data
	// round's absences added after its scan (decision-020 batch 8, 2.2 step
	// 6 b). A Slot claimed past its ceiling is read against these.
	Windows                    uint64 `json:"windows"`
	WindowCeilingWindows       uint64 `json:"window_ceiling_windows"`
	WindowCeilingRaisedWindows uint64 `json:"window_ceiling_raised_windows"`
	// WindowZeroBoundCuts* are the windows cut while the window bound was
	// zero, by the retained phase holding the most outside the window
	// (WindowZeroBoundReasons, slot_window_zero_bound_cuts_total).
	WindowZeroBoundCutsInput  uint64 `json:"window_zero_bound_cuts_input,omitempty"`
	WindowZeroBoundCutsGap    uint64 `json:"window_zero_bound_cuts_gap,omitempty"`
	WindowZeroBoundCutsOutput uint64 `json:"window_zero_bound_cuts_output,omitempty"`
	WindowZeroBoundCutsState  uint64 `json:"window_zero_bound_cuts_state,omitempty"`
	// WindowOutsideClamped and SliceOthersClamped are one when the window
	// bound's or the slice limit's take-off ever came to more than the Slot
	// held (slot_window_accounting_clamped_slots_total), zero otherwise.
	WindowOutsideClamped uint64 `json:"window_outside_clamped,omitempty"`
	SliceOthersClamped   uint64 `json:"slice_others_clamped,omitempty"`
}

// WindowAccountingClamps are the words for the window accounting a Slot found
// taking off more than it held: window_outside (the window bound,
// windowOutside) and slice_others (the slice limit, sliceOthers).
var WindowAccountingClamps = [2]string{"window_outside", "slice_others"}

// WindowZeroBoundReasons are the words for the retained phase holding the
// most outside a window cut while the window bound was zero, in the order of
// execution.SlotBudgetUsage.WindowZeroBoundCuts: input (the series' inputs,
// the reads ahead), gap (the gap facts), output (the results and summary
// outside the window), state (the State held outside the window).
var WindowZeroBoundReasons = [4]string{"input", "gap", "output", "state"}

// SlotTimingFacts is where one Slot's wall clock went, in milliseconds, on its
// completion row.
//
// Its own object beside the budget usage because it answers a different
// question. The usage says what the Slot consumed of what it was allowed; this
// says where its period went, and the two are read by different people at
// different moments -- one when a budget is near its limit, the other when a
// Slot did not finish in time.
//
// Slot is the whole of it and the three parts do not sum to it: the remainder
// is everything else the completion does, and it is only visible because the
// total is carried beside the parts.
//
// Input is this Slot waiting for its records and consuming them, from the
// moment the replica took the Slot and not from the first record. Not the
// query's own latency -- that runs on the view stream's side, and a Slot that
// waited its turn waited here with a backend that was never slow.
//
// Reported at every value including zero, for the same reason the retained
// phases are: a phase that is usually nothing and occasionally the whole Slot
// is the one worth finding, and its zeros are its denominator.
//
// From the completion on the phases partition the Slot, and Unattributed is
// what none of them claims (see execution.SlotTiming).
//
// apply_millis is in the partition only for a Slot applied whole. A Slot cut
// into windows writes them on its apply goroutine beside its own phases:
// apply_millis is how long that goroutine was busy, and runs beside the
// partition; apply_wait_millis is the Slot's own goroutine waiting on it and
// land_millis taking its landed windows back, both in the partition; and
// apply_overlap_millis is apply_millis less apply_wait_millis, the writes
// the Slot did not wait for. Readings from before the window pipeline: their
// apply_millis compares with apply_millis plus land_millis for what the
// writes cost, and with apply_wait_millis plus land_millis for the time they
// took on the Slot's path.
type SlotTimingFacts struct {
	Slot         uint64 `json:"slot_millis"`
	Input        uint64 `json:"input_millis"`
	Preflight    uint64 `json:"preflight_millis"`
	Evaluate     uint64 `json:"evaluate_millis"`
	Apply        uint64 `json:"apply_millis"`
	ApplyWait    uint64 `json:"apply_wait_millis"`
	Land         uint64 `json:"land_millis"`
	ApplyOverlap uint64 `json:"apply_overlap_millis"`
	RecordIDs    uint64 `json:"record_ids_millis"`
	Cut          uint64 `json:"cut_millis"`
	Merge        uint64 `json:"merge_millis"`
	Series       uint64 `json:"series_millis"`
	NoDataRead   uint64 `json:"no_data_read_millis"`
	NoDataWrite  uint64 `json:"no_data_write_millis"`
	Unattributed uint64 `json:"unattributed_millis"`
}

// SliceReadAheadOutcomes are the words for how a slice read ahead ended, in
// the order of the SlotBudgetUsageFacts fields that count them: replayed
// (InputSlicesReadAhead), other_leaf, yielded, failed and context_ended.
var SliceReadAheadOutcomes = [5]string{"replayed", "other_leaf", "yielded", "failed", "context_ended"}

// SeriesNotEvaluatedNoFamily is the family of a Plan none of whose
// algorithms is one AlgorithmFamily names, for SeriesNotEvaluated.
const SeriesNotEvaluatedNoFamily = "none"

// SeriesNotEvaluatedFamilyCount is how many families SeriesNotEvaluated
// counts under.
const SeriesNotEvaluatedFamilyCount = 12

// SeriesNotEvaluatedFamilies is every family SeriesNotEvaluated counts
// under: the algorithm families, and none.
var SeriesNotEvaluatedFamilies = [SeriesNotEvaluatedFamilyCount]string{string(AlgorithmFamilyThreshold), string(AlgorithmFamilySimpleRingRatio),
	string(AlgorithmFamilyOsRestart), string(AlgorithmFamilyProcPort), string(AlgorithmFamilyPingUnreachable),
	string(AlgorithmFamilySimpleYearRound), string(AlgorithmFamilyAdvancedRingRatio), string(AlgorithmFamilyAdvancedYearRound),
	string(AlgorithmFamilyRingRatioAmplitude), string(AlgorithmFamilyYearRoundAmplitude), string(AlgorithmFamilyYearRoundRange),
	SeriesNotEvaluatedNoFamily}

// SliceReadAheadYieldPhases are the phases of the Slot's reservations a read
// ahead yields to, in the order of the SlotBudgetUsageFacts fields that count
// them (ReadAheadYields<Phase>): the phases the share judges a reservation
// in, window_bound a window cut to fit beside the reads ahead, and other one
// named nowhere here.
var SliceReadAheadYieldPhases = [9]string{"normal_input", "normal_output", "normal_gap", "normal_no_data",
	"slot_output", "frames", "window_bound", "query_free", "other"}

// How a read ahead's wait for room ended, closed: kept, room came and it
// took the bytes; passed, the Slot took the read on while it waited, the
// bytes going to the Slot; stopped, the read was let go; canceled, its
// context ended. And how long it waited, in bands: under a millisecond, to
// ten, to a hundred, to a second, a second or more.
var (
	SliceReadAheadWaitOutcomes  = [4]string{"kept", "passed", "stopped", "canceled"}
	SliceReadAheadWaitDurations = [5]string{"lt_1ms", "1ms_10ms", "10ms_100ms", "100ms_1s", "ge_1s"}
)

// SliceReadAheadWaitDuration is the band of SliceReadAheadWaitDurations a
// wait falls in.
func SliceReadAheadWaitDuration(waited time.Duration) int {
	switch {
	case waited < time.Millisecond:
		return 0
	case waited < 10*time.Millisecond:
		return 1
	case waited < 100*time.Millisecond:
		return 2
	case waited < time.Second:
		return 3
	default:
		return 4
	}
}

// How a slice's read that ran out of time ended, closed: split and read
// again, the Slot's query completing; split and read again, the Slot's query
// failing after; not split - the cooldown held, its time could not be put on
// its size, the window left was too short, or the Slot had split once.
const (
	InputSliceTimeoutSplitSucceeded = "split_succeeded"
	InputSliceTimeoutSplitFailed    = "split_failed"
	InputSliceTimeoutNotSplit       = "not_split"
)

// NoDataStageAheadOutcomes are how a no-data first write staged as its
// windows landed ended, in this order: carried, the statement put the stage
// in place with it; differs, the round kept a staged group otherwise and
// wrote every group with the statement; discarded, the round wrote no
// statement and the stage was taken back; stopped, a segment could not be
// staged and the groups after it went with the statement.
var NoDataStageAheadOutcomes = [4]string{"carried", "differs", "discarded", "stopped"}

// What a Slot did with its Query Group's shape - the leaves a slice that ran
// out of time for its size was split into, which the next Slot starts in -
// closed: started, read in it; kept, a slice split for its time kept in it;
// merged, its last split kept for its time taken back after quick rounds, or
// its last split kept for a slice's size after rounds its leaves held little
// (the worker's shapeAfter and sizedDecay); dropped, by
// why: voided, the query revision or the dimensions changed or it grew past
// a starting tree's leaves, or a Slot started in it was refused
// INPUT_SLICE_LEAVES_EXHAUSTED (voided_max_leaves, counted by the next Slot
// to complete); refused, its leaves not a partition of every
// value (inputslice.FromShape: a split of no leaf, children that are no
// partition, more leaves than a tree may have); evicted_idle, the memories
// of other Query Groups not started for ten of their periods and ten
// minutes at least, shape or none, that a Slot's start dropped. Three more,
// of the splits kept for a slice's size (inputslice.Tree.KeepSizedSplit):
// refused_condition_budget, a split whose children's conditions would pass
// the budget, kept nothing; refused_single_value, one whose sample told none
// of its series apart, kept nothing; refused_split_error, a split that failed
// otherwise, kept nothing; merge_refused, a split taken back whose shape
// would not rebuild without it - the shape is kept as it was, not dropped.
// Two count the splits kept for a slice's size that did keep: sized_split,
// a slice the Slot started in let go for its size and its split kept for the
// next Slot (inputslice.Tree.SplitSized; the Slot's
// input_slice_start_discarded), and sized_kept, a slice read whole past a
// slice's series kept split for the next Slot (KeepSizedSplit).
const (
	InputSliceShapeStarted              = "started"
	InputSliceShapeKept                 = "kept"
	InputSliceShapeMerged               = "merged"
	InputSliceShapeVoidedRevision       = "voided_revision"
	InputSliceShapeVoidedDimensions     = "voided_dimensions"
	InputSliceShapeVoidedMaxLeaves      = "voided_max_leaves"
	InputSliceShapeRefusedNotALeaf      = "refused_not_a_leaf"
	InputSliceShapeRefusedNotAPartition = "refused_not_a_partition"
	InputSliceShapeRefusedMaxLeaves     = "refused_max_leaves"
	InputSliceShapeEvictedIdle          = "evicted_idle"
	InputSliceShapeRefusedCondition     = "refused_condition_budget"
	InputSliceShapeRefusedSingleValue   = "refused_single_value"
	InputSliceShapeRefusedSplitError    = "refused_split_error"
	InputSliceShapeMergeRefused         = "merge_refused"
	InputSliceShapeSizedSplit           = "sized_split"
	InputSliceShapeSizedKept            = "sized_kept"
)

// InputSliceShapeOutcomes is every outcome, in the order of the
// SlotBudgetUsageFacts fields that count them; sized_split's is
// InputSliceStartDiscarded.
var InputSliceShapeOutcomes = [16]string{InputSliceShapeStarted, InputSliceShapeKept, InputSliceShapeMerged,
	InputSliceShapeVoidedRevision, InputSliceShapeVoidedDimensions, InputSliceShapeVoidedMaxLeaves,
	InputSliceShapeRefusedNotALeaf, InputSliceShapeRefusedNotAPartition, InputSliceShapeRefusedMaxLeaves,
	InputSliceShapeEvictedIdle, InputSliceShapeRefusedCondition, InputSliceShapeRefusedSingleValue,
	InputSliceShapeRefusedSplitError, InputSliceShapeMergeRefused, InputSliceShapeSizedSplit, InputSliceShapeSizedKept}

// Where the dimension a Slot's starting slices were cut on came from,
// closed: root_split, the one its Query Group's last read had its root split
// on; whole_sample, the one the sample of its last read, read whole, split
// most evenly; first_dimension_fallback, the first of its dimensions, for a
// Query Group no read ever delivered a series of - its read whole the
// provider timed out - which nothing else can choose for.
const (
	InputSliceCutOnRootSplit      = "root_split"
	InputSliceCutOnWholeSample    = "whole_sample"
	InputSliceCutOnFirstDimension = "first_dimension_fallback"
)

// InputSliceCutOnSources is every source.
var InputSliceCutOnSources = [3]string{InputSliceCutOnRootSplit, InputSliceCutOnWholeSample, InputSliceCutOnFirstDimension}

// InputSliceTimeoutOutcomes is every outcome.
var InputSliceTimeoutOutcomes = []string{InputSliceTimeoutSplitSucceeded, InputSliceTimeoutSplitFailed, InputSliceTimeoutNotSplit}
