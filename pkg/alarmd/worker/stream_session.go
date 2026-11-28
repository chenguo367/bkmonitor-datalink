package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// streamedExecution is the Coordinator-owned provisional lifetime. Begin and
// ConsumeSeries only validate and retain immutable query facts. State/Gap
// reads and evaluation start after the authoritative completion is validated.
type streamedExecution struct {
	// slotStream is the Slot's own: what every substream of the Slot reads
	// and writes alike. The fields below it are this stream's, the slice it
	// consumes (015 C1: one Slot, several slices consumed side by side).
	*slotStream

	streamed map[streamedInputKey]streamedInput
	// shared is what the streamed bindings share with every series of their
	// query, each kept once (streamedInput).
	shared sharedBindings
	// completionInputs is what each series' inputs are built from
	// (preparedInputs), and primaryRecords the most PRIMARY records one series
	// holds.
	completionInputs completionInputs
	// inputHolds is, by series, what the batches of that series were charged
	// to the input phase and how many streamed bindings still point at them,
	// so a window that lets its series' bindings go gives their bytes back
	// once nothing of the Slot reads them (letGoWindowInputs).
	inputHolds map[execution.SeriesIdentityDigest]*seriesInputHold
	// planSeries is, by Plan, the series the stream bound, until the
	// completion: it is read for the evaluation's order there and only
	// counted after, so the completion leaves the counts (planSeriesCounts)
	// and lets the sets go.
	planSeries map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]struct{}
	// planSeriesCounts is, by Plan, the series the slices numbered up to
	// this stream's own bound (sliceLedger.through), filled for every due
	// Plan when the completion lets planSeries go: what this stream's
	// evaluation lines and its slice's head read, and, once every slice is
	// read, the Slot's. Each completion makes a new map, never written after
	// but by planSeriesMatched on this stream, and the lines of a Plan ride
	// on one pointer of it.
	planSeriesCounts map[execution.PlanIdentity]*int
	// sliceNumber is the number of the slice this stream consumes, the order
	// it was dispatched in (sliceLedger); 0 for a Slot read whole.
	sliceNumber int
	// finishing is set as the Slot's evaluation finishes, every slice
	// consumed: what it merges from then on - no-data, the Plans without
	// series, the tail - is after every slice (mergeSlice).
	finishing bool
	// yieldTo is where a stream that consumes a slice beside the one that
	// dispatches them asks for the Slot's reads ahead to yield: they are the
	// dispatch's to let go, on its own goroutine, which serves the request
	// while it waits (serveYield). Nil for the stream that dispatches, the
	// one a Slot read one slice at a time has, which lets them go itself.
	yieldTo chan<- yieldRequest
	// yieldDone ends a request on yieldTo the dispatch will not serve: closed
	// as the Slot ends, a consumer waiting on it judges its reservation as it
	// stands.
	yieldDone <-chan struct{}
	// retainedByPhase is this stream's retained bytes split by what they
	// were retained for. The total is the sum rather than a counter of its
	// own: a sixth budget's worth of memory arriving with no phase attached
	// is the failure this split exists to prevent, and a parallel total can
	// be incremented by a site that names no phase while every assertion on
	// the total still passes.
	retainedByPhase [retainPhaseCount]uint64
	// ownNow is retainedByPhase again, per phase, for the Slot's other
	// streams to read (retainedTotal, phasesNow).
	ownNow [retainPhaseCount]atomic.Uint64
	// keysHeld is what this stream holds, under the state phase, for every State
	// key it loaded and that no window gives back with its views
	// (loadedKeyHeldBytes); summaryHeld is what the summary holds of its
	// series, under the output phase (slotEvaluationSummary.addedBytes). Both
	// are charged as each result merges, on either path, and stay to the
	// Slot's end but for the witnesses a window's writes consumed.
	keysHeld    uint64
	summaryHeld uint64
	// producer is the lane of the Slot's apply pipe this stream hands its
	// tasks in (pipeLane): 0 with one stream and for the dispatch, one for
	// each consumer with several.
	producer uint64
	// ownEffects is the State, event and gap counts this stream holds of the
	// Slot's (effects), under effectsMu: what its windows hold, the one it
	// fills judged by it (ownHeldEffects).
	ownEffects effectCounts
	// dispatch is the stream that dispatches the Slot's slices when it is
	// not this one: what it holds is no consumer's, and each of the k
	// consumers bears 1/k of it in its bounds (boundTotal). Nil when this
	// stream dispatches and consumes, as with one stream.
	dispatch *streamedExecution
	// firstMerged is the Plans whose first result this stream merged, with
	// several consumers: what the Slot keeps of them was charged to this
	// stream, and a window it cuts keeps it (takeWindow).
	firstMerged map[execution.PlanIdentity]struct{}
	// fill is the window this stream is filling, its slice's (sliceWindow);
	// the Slot's windows are slotWindows.
	fill sliceWindow
	// slice is the slice this stream is reading and consuming (sliceRead);
	// the Slot's slices are sliceReading.
	slice sliceRead
	// seams times the seams of this stream's reads: each read's tail and the
	// change into the next (sliceSeams); with k consumers each its own, the
	// dispatch's their sum and longest as they end (fold).
	seams sliceSeams
	// bindings is the Slot's bindings as this stream reads them: those of
	// the slices numbered up to the one it consumes (sliceLedger.through),
	// every slice's once the Slot finishes.
	bindings []execution.NamedInputBinding
	// spentByPhase is this stream's wall clock split by slot phase, in
	// nanoseconds, summed over every call of each phase: each stream of a
	// Slot has its own clock (015 C1, C10), the Slot's row their sum
	// (timing).
	spentByPhase [slotPhaseCount]uint64
	// clockPhase is the phase the clock is charging and clockSince when it
	// last moved; zero until the completion starts it (enter).
	clockPhase slotPhase
	clockSince time.Time
	// hostOverlays holds, by Plan and series, the dimensions the platform's
	// fuller writes for a host CMDB placed by id with another address
	// (SeriesExecutionBatch.HostOverlays). Nil until a series carries one,
	// which most Slots never do.
	hostOverlays map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]map[string]json.RawMessage
	stateItems   []execution.StatePreflightItem
	state        execution.StatePreflightResult
	delivered    []execution.SeriesDelivery
	// stageAheadEnds counts how the Slot's no-data first writes staged
	// ahead ended (observability.NoDataStageAheadOutcomes), and
	// groupsStagedAhead the groups they staged.
	// routineEvaluations is, by Plan in the order first met, the routine
	// evaluations of the batch being evaluated merged into one line each
	// (observability.MergeRoutineEvaluation), said as the batch ends.
	routineEvaluations      map[execution.PlanIdentity]*observability.Observation
	routineEvaluationsOrder []execution.PlanIdentity
	// namedOutputs is the tables of named outputs the Slot's queries aligned,
	// by physical query, kept from the completion for the detection
	// snapshots of their Plans' events. Nil for a Slot whose queries asked
	// for none.
	namedOutputs map[execution.PhysicalQueryDigest]*execution.NamedOutputRecords
	// namedReserved is what this execution's queries reserved beside their
	// series (ReserveRetainedBytes) and still hold - named outputs'
	// alignments, and series a carried APM read holds until its response is
	// whole, which it gives back as it hands them on: part of the input
	// phase, settled to the aligned tables at the completion.
	namedReserved uint64
}

// slotStream is what a Slot's streamedExecution holds for the Slot as a
// whole - its request, its header and loads, its accounting, its windows,
// read-aheads and slice reading - as against the slice a stream consumes.
// One Slot has one, shared by every stream of it (015 C1 appendix B).
type slotStream struct {
	// load is the Slot's loads of what it holds whole (loadSlot): run once
	// for the Slot, by whichever slice's completion reaches them first.
	load           slotLoad
	queryEvidence  queryAvailabilityEvidence
	coordinator    *SlotExecutionCoordinator
	request        execution.SlotExecutionRequest
	header         execution.InternalExecutionHeader
	prepared       preparedNamedInputIndex
	primaryRecords uint64
	// dependencyDiscarded is the dependency series no PRIMARY of their Plan
	// named, let go at the completion (discardDependenciesWithoutPrimary).
	dependencyDiscarded uint64
	// pointsDropped is the points the Slot's answers' normalization let go,
	// by reason, every completion it took together: on its completion line,
	// the Query Group's own.
	pointsDropped observability.PointDrops
	// queryPhases is the Slot's physical queries' time before their answers'
	// first byte and after it, summed and the longest of each, and their
	// shared-schema answers' frame waits.
	queryPhases queryPhaseTotals
	// ledger is what each slice contributed to what is read of the Slot so
	// far - its Plans' series, the bindings it kept, the series it wrote - by
	// the slice's number (sliceLedger).
	ledger sliceLedger
	// completionOnly holds, per Plan without streamed PRIMARY series, the exact
	// set validated by validateCompletionOnlyExactSet: one completion binding
	// per frozen (consumer, requirement). It decides the no-series result.
	completionOnly map[execution.PlanIdentity][]execution.NamedInputBinding
	gapItems       []execution.PlanGapLoadItem
	noDataItems    []execution.PlanNoDataLoadItem
	gaps           execution.GapLoadResult
	noData         execution.NoDataLoadResult
	// noDataRefused is the loaded memories that did not fit the share beside
	// what this Slot holds (reserveNoDataMemory): their Plans skip no-data
	// this Slot.
	noDataRefused  map[execution.PlanNoDataIdentity]execution.NoDataApplyItemResult
	noDataHosts    map[execution.PlanNoDataIdentity]nodata.HostResolution
	noDataOutcomes []nodata.SlotOutcome
	// noDataOutcomeIndex is where each Plan's outcome sits in noDataOutcomes,
	// so a Slot whose results are replaced can correct the one it filed.
	noDataOutcomeIndex map[execution.PlanKey]int
	// noDataJudged is the rounds that judged, held until the Slot's outcomes
	// are final: a judgement the Slot then discards is not reported as made.
	noDataJudged []noDataJudgment
	// targetResolutions is what this Slot resolved each target-plan Plan's
	// target to, by Plan. Absence and admission both read it, so one Slot
	// cannot admit a record under one view of the target and judge absence
	// under another. A Plan with a target plan and no entry here is read as
	// unresolved by both, never as unscoped or as empty.
	targetResolutions map[execution.PlanIdentity]*resolvedTarget
	// answerTruncated is the Plans a primary query of which came back with a
	// suspected cut on its route: whole by its own account, and possibly
	// missing groups the query service dropped at its cap. Absence is not
	// judged for them this Slot, presence is (nodata.OutcomeSkippedAnswerTruncated).
	answerTruncated map[execution.PlanIdentity]bool
	// scopeGroups is what this Slot read the dynamic groups its Plans'
	// target scopes name to, by tenant and group, read once at Begin for
	// every record the Slot admits.
	scopeGroups execution.ScopeGroupMemberships
	// seriesCensus is where this Slot's series went, counted where each
	// decision is made rather than inferred afterwards. See seriesCensus.
	seriesCensus seriesCensus
	// superseded is the series this attempt skipped because their State is
	// a later Slot's, for its result; supersededGaps the Plans whose gap
	// marker a resumed series found from a later Slot, so a Plan of many
	// resumed series counts its one marker once. See superseded_series.go.
	superseded     execution.SupersededSeries
	supersededGaps map[execution.PlanGapIdentity]struct{}
	// noDataPlansSeen is how many of this Slot's Plans detect no-data,
	// counted where they are found rather than where they are judged.
	noDataPlansSeen int
	noDataMutations []execution.PlanNoDataMutation
	// noDataCarried is, for each Plan whose round starts from the memory of
	// the generation its activation carries it from, that memory's header:
	// the round scans that key in place of its own (carryNoDataMemory).
	noDataCarried map[execution.PlanNoDataIdentity]execution.NoDataMemorySnapshot
	// noDataAbsenceStarts carries each Plan's absent series to the output's
	// manual-close check (decision-024).
	noDataAbsenceStarts noDataAbsenceStarts
	// noDataEnds is each Plan's no-data end index as this Slot read it.
	noDataEnds map[execution.PlanIdentity]execution.NoDataEndLoaded
	// noDataEndCarries is the entries each Plan's index took this Slot from
	// the generation its activation carries it from.
	noDataEndCarries map[execution.PlanIdentity]noDataEndCarry
	effective        map[execution.ConsumerRef]strategy.EffectiveTimeFact
	// evaluated is the Slot's results: every Plan merged, its disposition,
	// gap statements and coverage, without what a window holds of it
	// (sliceWindow.results). It and summary change only under mergeMu.
	evaluated execution.EvaluationResult
	// summary is what the Slot-wide readers after evaluation read of the
	// Slot's results, kept beside evaluated as each batch merges
	// (slotEvaluationSummary).
	summary slotEvaluationSummary
	mergeMu sync.Mutex
	// seenMu guards windows.seen (noteSeenLocked).
	seenMu sync.Mutex
	// firstSeen is the Slot's first-seen judgement, set where it is made:
	// the head of a Slot applied window by window, or its finalization
	// (judgingFirstSeen).
	firstSeen firstSeenJudging
	series    uint64
	// peakRetained is the most the Slot held at once, and peakByPhase its
	// phases at that moment, for the completion row: a Slot applied window
	// by window gives bytes back as it goes, so what it holds at its end is
	// not what it needed, and capacity is sized from what it needed. With
	// heldSince and holding they change only under peakMu: every stream of
	// the Slot charges.
	peakMu       sync.Mutex
	peakRetained uint64
	peakByPhase  [retainPhaseCount]uint64
	// consumers is how many streams consume the Slot's slices side by side
	// (k, 015 C1); none set is one. accounts is every stream holding bytes of
	// the Slot when there are several - the dispatch and its consumers - each
	// its own retainedByPhase, the Slot's total their sum (retainedTotal);
	// empty with one stream, which holds them all.
	consumers int
	accounts  []*streamedExecution
	// retainedInputNow is retainedByPhase's input share kept again for a
	// reader on another goroutine: access reads it across an in-place
	// readiness wait while the series path keeps adding.
	retainedInputNow atomic.Uint64
	// ahead is the next slice read while this one completes, and aheadBytes
	// what it holds (slice_read_ahead.go): counted in retainedTotal, so the
	// share, the slice limit and the windows see it.
	aheads []*sliceReadAhead
	// readsInFlight is the Slot's slice reads by how many were in flight as
	// each began (noteReadBegan).
	readsInFlight [6]uint64
	// replayedCancels ends the contexts of the reads ahead replayed, as the
	// Slot ends (letGoReadAhead).
	replayedCancels []context.CancelFunc
	// takenUp is the real series each Plan took up, evaluated, resumed or
	// skipped, against planSeriesCount (seriesNotEvaluated).
	takenUp    map[execution.PlanIdentity]int
	aheadBytes atomic.Uint64
	// aheadAtRefusal is aheadBytes when a reservation of the stream's was
	// last refused (judgeRetainedLocked), for yieldReadAhead.
	aheadAtRefusal atomic.Uint64
	// readAheadEnds counts how the Slot's reads ahead ended (readAheadEnd).
	readAheadEnds [readAheadEnds]uint64
	// readAheadYields is the reads ahead yielded by the phase of the
	// reservation they yielded to (observability.SliceReadAheadYieldPhases);
	// readAheadYieldedBytes what they held, readAheadYieldDistanceMax the
	// farthest of them from the slice being read, in slices; readAheadWaitMax
	// the longest a read ahead waited for room, in milliseconds, written from
	// the reads' goroutines.
	readAheadYields           [len(observability.SliceReadAheadYieldPhases)]uint64
	readAheadYieldedBytes     uint64
	readAheadYieldDistanceMax uint64
	readAheadWaitMax          atomic.Int64
	// readAheadWaiting is the reads ahead waiting for room now, and since
	// when one of them has: the waits of reads ahead side by side overlap,
	// and the Slot's time with one waiting is counted once, as the last of
	// them ends (readAheadWaitEnded).
	readAheadWaitingMu    sync.Mutex
	readAheadWaitingNow   int
	readAheadWaitingSince time.Time
	// segments is the Slot's Plans' stable segments, read as it begins,
	// before any of its State (014 B3 3.2); nil for none.
	segments          *segmentSlot
	stageAheadEnds    [4]uint64
	stageAheadDiffers [3]uint64
	groupsStagedAhead uint64
	// retainedNow is retainedByPhase again for a reader on another goroutine
	// (RetainedHolders), and heldSince when this execution first retained;
	// holding says it has joined the coordinator's holder registry. holding
	// is set once and never cleared, release included: a stream registers
	// once, so heldSince is written once, before any reader can reach it.
	// A stream retaining after its release would show as a reservation no
	// hold names, not rewrite heldSince under a reader.
	retainedNow [retainPhaseCount]atomic.Uint64
	heldSince   time.Time
	holding     bool
	// beganAt is when this execution started, for the total the phases are
	// read against. Zero on an execution that never began, which reports no
	// duration rather than the time since the epoch.
	beganAt time.Time
	// effects is the State, event and gap counts this execution holds of
	// the pool and of the per-Slot caps; effectsSpent is what windows
	// already applied held and gave back, so the two together are the
	// Slot's. effects changes only under effectsMu - judged against the caps
	// and added in one step (admitEffects), given back as a window lands
	// (giveBackEffects) - and is read on the Slot's goroutine.
	effectsMu    sync.Mutex
	effects      effectCounts
	effectsSpent effectCounts
	// pipeMu guards the Slot's apply pipe as it starts (applyPipeline) and
	// the windows handed to it (windows.handed).
	pipeMu sync.Mutex
	// barrier is the Slot's barrier for its consumers (slotBarrier), and
	// barriers how many were held, under sharedMu.
	barrier  slotBarrier
	barriers uint64
	// beginMu has the Slot's streams begin their reads one at a time
	// (Begin); poolMu starts its evaluation pool once (evaluatePool);
	// windowHeadMu starts its windowed apply once (startWindows); presentMu
	// guards the no-data groups its windows hand and their staging
	// (collectPresent).
	beginMu, poolMu, windowHeadMu, presentMu sync.Mutex
	// sharedMu guards what the Slot's streams add to as they read side by
	// side: its series (addSeries), its reads in flight (noteReadBegan), and
	// its census, queries' facts and evidence, the series it took up and the
	// superseded series it found (addShared).
	sharedMu sync.Mutex
	gapFacts uint64
	began    bool
	// windows is this Slot's windowed apply (windowed_apply.go); its zero
	// value is a Slot that has not been cut.
	windows slotWindows
	// windowCeiling is the latest the Slot's windows may move its deadline
	// to, in Unix milliseconds, zero for none (fixWindowCeiling): written on
	// the Slot's goroutine, read by the apply goroutine as each window lands
	// (windowBoundary).
	windowCeiling atomic.Int64
	// applyCtx is the Slot's completion context, which its apply goroutine
	// runs on (applyPipeline): set as the completion's context is.
	applyCtx context.Context
	// applyHeldBytes is what the Slot's apply side holds of its share - the
	// frames kept for its writes, no-data first writes, segment joins - under
	// the state phase, written by the apply goroutine; inflightHeld is what
	// the windows handed to it and not landed give back as they land
	// (windowTask.release), written by the Slot's goroutine. Both read from
	// any goroutine: the reads ahead judge the Slot's side of the share apart
	// from them (sliceReadAhead.fitsShare).
	applyHeldBytes atomic.Uint64
	inflightHeld   atomic.Uint64
	// evaluating is the goroutines the Slot evaluates its series on
	// (evaluatePool), nil before its first batch.
	evaluating *evaluatePool
	// supplement is set on a supplement execution and nil on every Slot.
	// See supplement.go.
	supplement *supplementRun
	// slices is the Slot read slice by slice (sliceReading); nil for a Slot
	// read whole.
	slices *sliceReading
}

// slotPhase says which part of a Slot a stretch of wall clock was spent in.
//
// From the completion on they are a partition of the Slot: the clock charges
// one phase at a time (enter), a phase entered inside another takes its time
// from it, and what no phase claims is slotPhaseOther, reported as the Slot's
// unattributed time rather than left to be inferred from the total.
type slotPhase int

const (
	// slotPhaseInput runs from Begin to the completion arriving: this Slot
	// waiting for its records and consuming them, the wait before the first
	// one included.
	//
	// From Begin and not from the first record, deliberately: a Slot that
	// waited its turn spent that time, and charging from the first record
	// would hide exactly the stretch this field is read to find. It contains
	// the query's own latency and is not it, which is why it is not named for
	// the query -- the query runs on the view stream's side.
	slotPhaseInput slotPhase = iota
	slotPhasePreflight
	slotPhaseEvaluate
	// slotPhaseApply is the writes: every window's events, State and gaps,
	// and the tail, or the one apply of a Slot not cut into windows, the wait
	// for its Sequence included.
	slotPhaseApply
	// slotPhaseRecordIDs is deriving the record ids of the series a window
	// reads.
	slotPhaseRecordIDs
	// slotPhaseCut is cutting a window while the Slot is evaluated: the
	// window's head (its open statements, the first-seen judgement) less the
	// reads and writes inside it, which are Preflight and Apply.
	slotPhaseCut
	// slotPhaseMerge is folding one series' result into the Slot's.
	slotPhaseMerge
	// slotPhaseSeries is one series' work around its evaluation: its
	// bindings, its effective-time facts and its result, the evaluation and
	// the reads inside it apart.
	slotPhaseSeries
	// slotPhaseNoDataRead is a no-data round's reading of its memory and its
	// judgement, the windows it cuts apart.
	slotPhaseNoDataRead
	// slotPhaseNoDataWrite is writing the no-data memory and its ends.
	slotPhaseNoDataWrite
	// slotPhaseApplyWait is the Slot's goroutine waiting on its apply
	// goroutine (windowed_pipeline.go): for it to take a window, to drain, or
	// for the last window and the tail. The apply goroutine's own time is not
	// a phase of the Slot's: it runs beside them (applyBusy).
	slotPhaseApplyWait
	// slotPhaseLand is taking a landed window back on the Slot's goroutine:
	// what it held given back, its inputs let go (landWindow).
	slotPhaseLand
	// slotPhaseOther is every moment after the completion no phase above
	// claims: the Slot's unattributed time.
	slotPhaseOther
	slotPhaseCount
)

// enter charges this execution's clock to phase until the returned leave is
// called, and to the phase that was charging before from then on. A phase
// entered inside another takes its time from that one, so from the completion
// on the phases partition the Slot. Before the completion the clock is not
// running - the input phase is the whole of that stretch - and enter does
// nothing.
func (stream *streamedExecution) enter(phase slotPhase) (leave func()) {
	if stream == nil || stream.clockSince.IsZero() {
		return func() {}
	}
	previous := stream.clockPhase
	stream.chargeClock()
	stream.clockPhase = phase
	return func() {
		stream.chargeClock()
		stream.clockPhase = previous
	}
}

// chargeClock gives the time since the clock last moved to the phase it is
// charging.
func (stream *streamedExecution) chargeClock() {
	now := time.Now()
	if elapsed := now.Sub(stream.clockSince); elapsed > 0 {
		stream.spentByPhase[stream.clockPhase] += uint64(elapsed)
	}
	stream.clockSince = now
}

// spend records wall clock against the phase that spent it, from since to now.
//
// The only way to add to this execution's clock, for the same reason
// retainBytes is: a phase's time arriving with no phase attached is the
// failure the split exists to prevent. A zero since is a call that was never
// started and adds nothing.
func (stream *streamedExecution) spend(phase slotPhase, since time.Time) {
	if since.IsZero() {
		return
	}
	elapsed := time.Since(since)
	if elapsed < 0 {
		return
	}
	stream.spentByPhase[phase] += uint64(elapsed)
}

// spentMillis is what one phase took, rounded to milliseconds.
func (stream *streamedExecution) spentMillis(phase slotPhase) uint64 {
	if stream == nil {
		return 0
	}
	return uint64(time.Duration(stream.spentByPhase[phase]).Milliseconds())
}

// slotMillis is how long this execution has been running, zero if it never
// began.
func (stream *streamedExecution) slotMillis() uint64 {
	if stream == nil || stream.beganAt.IsZero() {
		return 0
	}
	elapsed := time.Since(stream.beganAt)
	if elapsed < 0 {
		return 0
	}
	return uint64(elapsed.Milliseconds())
}

// retainPhase says which part of a Slot a retained byte was held for. The
// three names are the ones the reservation path already reports on a refusal
// (normal_input / normal_gap / normal_output), so a rejection line and a
// completion row describe the same three quantities.
type retainPhase int

const (
	retainPhaseInput retainPhase = iota
	retainPhaseGap
	retainPhaseOutput
	// retainPhaseState is the Runtime State this Slot loaded and holds: the
	// retained window, per series, for as long as the Slot runs.
	//
	// Its own phase because it is neither of the two it used to be read as.
	// It was charged to the output phase, where it dominated -- a 1,466-point
	// window costs 48 + len(RecordID) + 40 x levels per point, per series, and
	// every effect this Slot will write is small beside it. So
	// retained_output_bytes tracked the retention bound rather than the
	// output, and no number anywhere answered "how much will this Slot
	// write". The total is unchanged; only which of the three it is counted
	// under.
	retainPhaseState
	retainPhaseCount
)

// retainNone is a reservation that charges no phase of a stream: the
// worker's own preparation, which belongs to no Slot.
const retainNone = retainPhaseCount

// retainBytes records retained bytes against the phase that retained them,
// bytes no reservation judged: the copies every goroutine reads first
// (chargeRetained), then this execution's own (retainedCharged).
//
// With retainedCharged after a reservation that charged them, the only way
// to add to this execution's retention. An out-of-range phase panics here
// rather than being dropped into the total unattributed.
func (stream *streamedExecution) retainBytes(phase retainPhase, retained uint64) {
	stream.chargeRetained(phase, retained)
	stream.retainedCharged(phase, retained)
}

// chargeRetained adds bytes to this execution's total under phase and to
// the copies of it any goroutine reads (retainedNow), the input's own copy
// included. A reservation calls it under reservations.mu, in the step that
// judged the bytes against the share (acquireProvisional, acquireEffects,
// holdRetained): the share is judged on that total under the same lock, so
// two goroutines reserving for one Slot never both pass on a total that
// counts neither.
func (stream *streamedExecution) chargeRetained(phase retainPhase, retained uint64) {
	stream.retainedByPhase[phase] += retained
	stream.ownNow[phase].Add(retained)
	stream.retainedNow[phase].Add(retained)
	if phase == retainPhaseInput {
		stream.retainedInputNow.Add(retained)
	}
}

// retainedCharged counts, on this execution's own goroutine, bytes a
// reservation charged as it judged them (chargeRetained): its peak, and its
// place among the pool's holders.
func (stream *streamedExecution) retainedCharged(phase retainPhase, retained uint64) {
	stream.peakMu.Lock()
	defer stream.peakMu.Unlock()
	if total := stream.retainedTotal(); total > stream.peakRetained {
		stream.peakRetained, stream.peakByPhase = total, stream.phasesNow()
	}
	if retained > 0 && !stream.holding {
		stream.holding, stream.heldSince = true, time.Now()
		stream.coordinator.holders.add(stream.holder())
	}
}

// holder is the stream the coordinator's holder registry names for the
// Slot: its dispatch, or this stream with one.
func (stream *streamedExecution) holder() *streamedExecution {
	if stream.dispatch != nil {
		return stream.dispatch
	}
	return stream
}

// slotStreams is every stream holding a part of the Slot: the dispatch and
// its k consumers (accounts), or the one stream.
func (stream *streamedExecution) slotStreams() []*streamedExecution {
	if len(stream.accounts) == 0 {
		return []*streamedExecution{stream}
	}
	return stream.accounts
}

// phasesNow is what the Slot holds by phase: this stream's, and every other
// stream's of the Slot (ownNow).
func (stream *streamedExecution) phasesNow() [retainPhaseCount]uint64 {
	phases := stream.retainedByPhase
	for _, other := range stream.accounts {
		if other == stream {
			continue
		}
		for phase := range phases {
			phases[phase] += other.ownNow[phase].Load()
		}
	}
	return phases
}

// ownTotalNow is what this stream holds, safe to read from another stream
// of the Slot.
func (stream *streamedExecution) ownTotalNow() uint64 {
	var total uint64
	for phase := range stream.ownNow {
		total += stream.ownNow[phase].Load()
	}
	return total
}

// consumerCount is k, the streams consuming the Slot's slices side by side.
func (stream *streamedExecution) consumerCount() uint64 {
	return uint64(max(stream.consumers, 1))
}

// boundTotal is what a consumer's window bound and slice limits count of the
// Slot. With one stream it is everything the Slot holds (retainedTotal).
// With k it is this consumer's own, 1/k of what is no consumer's - the reads
// ahead and what the dispatch holds - and what the apply side holds, which
// the bounds take back off as they do with one. The other consumers' own
// are theirs: each consumer's bound is S/(2k) less this, so the Slot side
// stays within S/2 however the k fill (015 C1 4.1).
func (stream *streamedExecution) boundTotal() uint64 {
	k := stream.consumerCount()
	if k == 1 {
		return stream.retainedTotal()
	}
	slot := stream.aheadBytes.Load()
	if stream.dispatch != nil && stream.dispatch != stream {
		for phase := range retainPhaseCount {
			slot += stream.dispatch.heldForConsumers(phase)
		}
	}
	total := slot/k + stream.applyHeld()
	for _, retained := range stream.retainedByPhase {
		total += retained
	}
	return total
}

// heldForConsumers is what the dispatch holds of phase that its consumers'
// bounds count, safe to read from a consumer's goroutine: all it holds, its
// windows in flight with it. Counted beside them, the dispatch is never
// counted at less than it holds: its windows in flight are taken off the
// lane before what they held is given back (landResult), and a count less
// what is in flight would, for that while, count less than is held. What it
// holds beside its windows - the Plans it merged first, its summary, the
// keys no window consumed - it holds to the Slot's end, as a consumer holds
// its own. Its windows are counted until they land: the dispatch takes its
// own landings while its consumers read (consumeSideBySide).
//
// What that comes to, measured on a Query Group of 150,000 series at two
// consumers on a share S of 768 MiB: as its consumers begin, the window of
// the slice it read before they began is cut and in flight, and counted here
// until it lands - its input a consumer's slice at most (limitConsumers),
// 55.8 MiB, beside output and State of 160.9 and 148.4 MiB, the Slot's
// retained peak at that moment. It lands once written, giving back its
// input and most of its State; until then each consumer counts 1/k
// of it, and a first slice judged then had a limit of 3.9 MiB, was let go
// and split, and the Query Group's shape grew from 17 slices to 33 (015 C1,
// the k = 2 share).
func (dispatch *streamedExecution) heldForConsumers(phase retainPhase) uint64 {
	return dispatch.ownNow[phase].Load()
}

// RetainedInputBytesNow is the input this execution has retained so far,
// safe to read from any goroutine (execution.RetainedInputReporter).
func (stream *streamedExecution) RetainedInputBytesNow() uint64 {
	return stream.retainedInputNow.Load()
}

// ReserveRetainedBytes reserves bytes a query of this execution holds
// beside its series - a named-outputs alignment as its answer is read, or
// the series a carried APM read holds before it hands them on -
// against the same budget its series are reserved against, judged the same
// way: the share one Query Group's Slot may hold, by this execution's own
// total, then the pool (reserveRetainedInput). It refuses at once, as a
// series is refused, and never waits (execution.RetainedBytesReserver). The
// access source calls it under the lock series are delivered under.
func (stream *streamedExecution) ReserveRetainedBytes(ctx context.Context, bytes uint64) error {
	if err := stream.reserveRetainedInput(ctx, bytes); err != nil {
		return err
	}
	stream.namedReserved += bytes
	return nil
}

// ReleaseRetainedBytes gives back bytes ReserveRetainedBytes reserved, no
// more than it holds.
func (stream *streamedExecution) ReleaseRetainedBytes(bytes uint64) {
	bytes = min(bytes, stream.namedReserved)
	stream.namedReserved -= bytes
	stream.unretainInput(bytes)
}

// reserveRetainedInput reserves input bytes beside the series: the share
// first, against this execution's own total, then the pool through the
// reservation a series takes; refused the way either refuses, observed as
// a capacity rejection.
func (stream *streamedExecution) reserveRetainedInput(ctx context.Context, bytes uint64) error {
	return stream.reserveRetained(ctx, retainPhaseInput, "normal_input", bytes)
}

// reserveRetained reserves bytes under one phase, judged as
// reserveRetainedInput judges input: the share by this execution's total,
// then the pool. label is the phase a refusal names.
func (stream *streamedExecution) reserveRetained(ctx context.Context, retained retainPhase, label string, bytes uint64) error {
	if bytes == 0 {
		return nil
	}
	if err := stream.reserveProvisionalAt(ctx, 0, bytes, stream.reservationPhase(label), retained); err != nil {
		return err
	}
	stream.retainedCharged(retained, bytes)
	return nil
}

// unretainInput gives back input bytes this execution reserved, to the
// pool and from its own total.
func (stream *streamedExecution) unretainInput(bytes uint64) {
	stream.unretain(retainPhaseInput, bytes)
}

// unretain gives back bytes this execution retained under one phase, to the
// pool and from its own total, no more than the phase holds. The one way to
// take from a phase before the stream releases everything, as retainBytes is
// the one way to add: a window applied gives its views, results and inputs
// back here, phase by phase.
func (stream *streamedExecution) unretain(phase retainPhase, bytes uint64) {
	bytes = min(bytes, stream.retainedByPhase[phase])
	if bytes == 0 {
		return
	}
	stream.retainedByPhase[phase] -= bytes
	stream.ownNow[phase].Add(^(bytes - 1))
	stream.retainedNow[phase].Add(^(bytes - 1))
	if phase == retainPhaseInput {
		stream.retainedInputNow.Add(^(bytes - 1))
	}
	stream.coordinator.releaseProvisional(0, bytes)
}

type retainedOwnerKey struct{}

// withRetainedOwner names the execution whose share the bytes held further
// down ctx count against - the frames a State write keeps and the segments a
// no-data first write holds are the Slot's, and judged with the rest of what
// it holds (judgeRetainedLocked) - and, a stream's, the segments its Slot
// read, which every State read under ctx is extended by
// (segment.WithSource): the Slot's preflight, a cache hit, the first-seen
// and no-data pre-reads, a carried generation, the write path's conflict
// decode. On the Slot's apply goroutine the owner is its apply pipe
// (applyPipeline), whose holds are counted apart from the stream's.
func withRetainedOwner(ctx context.Context, stream *streamedExecution) context.Context {
	if stream == nil {
		return context.WithValue(ctx, retainedOwnerKey{}, nil)
	}
	ctx = segment.WithSource(ctx, streamSegments{stream: stream})
	return context.WithValue(ctx, retainedOwnerKey{}, retainedOwner(stream))
}

// streamSegments answers the State store from the segments the stream's
// Slot read: its books once Begin read them, none before or without them.
type streamSegments struct{ stream *streamedExecution }

func (source streamSegments) Coverage(identity execution.StateKeyIdentity) (segment.Coverage, bool) {
	return source.stream.segments.Coverage(identity)
}

func retainedOwnerFrom(ctx context.Context) retainedOwner {
	owner, _ := ctx.Value(retainedOwnerKey{}).(retainedOwner)
	return owner
}

// holdRetained reserves bytes the owner holds outside its phases' own
// bookkeeping - kept frames, first-write segments - judged against its share
// and the pool, and counted in its State phase so its total is all it holds.
// Without an owner that has begun, the pool alone judges them.
func (coordinator *SlotExecutionCoordinator) holdRetained(owner retainedOwner, bytes uint64, phase string) error {
	if bytes == 0 {
		return nil
	}
	for {
		coordinator.reservations.mu.Lock()
		var err error
		if owner != nil {
			err = owner.judgeHeldLocked(bytes, phase)
		} else {
			err = coordinator.judgeRetainedLocked(nil, bytes, phase)
		}
		if err == nil {
			coordinator.reservations.retainedBytes += bytes
			coordinator.noteRetainedHighLocked()
			if owner != nil {
				owner.keepHeldLocked(bytes)
			}
		}
		coordinator.reservations.mu.Unlock()
		if err == nil || owner == nil || !owner.yieldHeld(phase) {
			if err == nil && owner != nil {
				owner.keepHeld(bytes)
			}
			return err
		}
	}
}

// letGoRetained gives back what holdRetained reserved.
func (coordinator *SlotExecutionCoordinator) letGoRetained(owner retainedOwner, bytes uint64) {
	if bytes == 0 {
		return
	}
	if owner != nil {
		// From the owner's State phase and the pool together; a stream
		// already released has given the pool its bytes back.
		owner.letGoHeld(bytes)
		return
	}
	coordinator.releaseProvisional(0, bytes)
}

// retainedTotal is what this execution holds across all phases, its apply
// side's holds included (applyHeld).
func (stream *streamedExecution) retainedTotal() uint64 {
	if stream == nil {
		return 0
	}
	total := stream.aheadBytes.Load() + stream.applyHeld()
	for _, retained := range stream.retainedByPhase {
		total += retained
	}
	// The Slot's other streams', each its own (accounts).
	for _, other := range stream.accounts {
		if other != stream {
			total += other.ownTotalNow()
		}
	}
	return total
}

type streamedInputKey struct {
	consumer    execution.ConsumerRef
	series      execution.SeriesIdentityDigest
	requirement execution.RequirementID
}

type preparedNamedInputIndex struct {
	inputBuilder           *execution.SeriesEvaluationInputBuilder
	queries                map[execution.PhysicalQueryDigest]execution.PlannedPhysicalQueryRef
	requirementsByConsumer map[execution.ConsumerRef][]execution.DataRequirement
	requirementByKey       map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.DataRequirement
	consumersByPlan map[execution.PlanIdentity][]execution.ConsumerRef
	// foldPolicies holds, per frozen (consumer, requirement), the fold policy
	// the compiled algorithm consuming that requirement declares for provider
	// rows that share one series identity. Requirements without a policy keep
	// the one-batch-per-series contract.
	foldPolicies map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]strategy.SeriesFoldPolicy
}

func (stream *streamedExecution) Begin(ctx context.Context, header execution.InternalExecutionHeader) error {
	// The first Begin of the Slot sets what it holds from its header; every
	// stream's reads begin after it, one at a time.
	stream.beginMu.Lock()
	defer stream.beginMu.Unlock()
	if stream.began && stream.slicing() {
		// Every slice's read begins again; the Slot began with the first.
		if header.Contract != stream.request.Contract {
			return errors.New("alarmd worker: execution header changed frozen contract")
		}
		stream.resetSliceStream()
		return nil
	}
	if stream.began {
		return errors.New("alarmd worker: QueryExecutionSource called Begin more than once")
	}
	if header.Contract != stream.request.Contract {
		return errors.New("alarmd worker: execution header changed frozen contract")
	}
	if stream.slicing() && (stream.slices.dimensionsGone(header) || stream.dispatch != nil && stream.slices.tree == nil) {
		// Begun before any query is sent (access.Source): the slices go
		// with nothing read, and the Slot begins again read whole - on its
		// own stream: one of k consumers whose read begins once the slices
		// went with their dimensions reads none of a slice the Slot no
		// longer has.
		return errSliceDimensionsGone
	}
	inputBuilder, err := execution.PrepareSeriesEvaluationInputBuilder(header)
	if err != nil {
		return fmt.Errorf("alarmd worker: invalid execution header: %w", err)
	}
	prepared, err := prepareNamedInputIndex(header)
	if err != nil {
		return fmt.Errorf("alarmd worker: prepare named-input index: %w", err)
	}
	prepared.inputBuilder = inputBuilder
	stream.began = true
	// Listed as a holder from here, holding nothing yet: a Slot that runs is
	// named by the retained holders for as long as it runs, so one that
	// holds its structures while reserving nothing reads as the oldest
	// holder of no bytes rather than not at all. releaseProvisional removes
	// it.
	stream.peakMu.Lock()
	if !stream.holding {
		stream.holding, stream.heldSince = true, time.Now()
		stream.coordinator.holders.add(stream.holder())
	}
	stream.peakMu.Unlock()
	// Started here rather than at the Slot's evaluation time: this is when
	// this replica began working on it, and the gap between the two is the
	// scheduler's to answer for, not the Slot's.
	stream.beganAt = time.Now()
	stream.header = header
	stream.prepared = prepared
	stream.streamed = make(map[streamedInputKey]streamedInput)
	stream.shared = sharedBindings{}
	stream.planSeries = make(map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]struct{})
	stream.completionOnly = make(map[execution.PlanIdentity][]execution.NamedInputBinding)
	effective, err := PrepareEffectiveTimeFacts(ctx, header, stream.coordinator.ports.EffectiveTime, stream.coordinator.now().Unix())
	if err != nil {
		return fmt.Errorf("alarmd worker: prepare EffectiveTime facts: %w", err)
	}
	stream.effective = effective
	// The target plans are resolved here, before any series arrives: the
	// source reads the memberships right after Begin to filter the records,
	// and the absence judgement at completion reads the same resolutions.
	stream.resolveTargetPlans(ctx)
	stream.resolveScopeGroups(ctx)
	// Once a Slot, before the first State preread: a slice's Begin again
	// returned above.
	segments, err := stream.readSegments(ctx, header)
	if err != nil {
		return err
	}
	stream.segments = segments
	return nil
}

// readSegments reads the Slot's Plans' segments and reserves the books it
// decoded from them as the State it holds is reserved (retainPhaseState):
// held the Slot's whole run, growing with its members as its State grows
// with its series. Reserved quietly - a refusal is the shadow's to count
// (book_refused), not a capacity rejection, as the Slot goes on without its
// shadow and its State written whole - and given back with the rest of the
// Slot's reservation. A Slot dispatched with a segment skip word whose books
// are refused fails its first attempt as one whose segments did not read,
// and its retry runs without them, degraded (014 B3 §9.7).
func (stream *streamedExecution) readSegments(ctx context.Context, header execution.InternalExecutionHeader) (*segmentSlot, error) {
	slot, skipped, err := stream.coordinator.loadSegments(ctx, stream.request, header)
	if err != nil {
		return nil, err
	}
	return stream.holdSegments(ctx, slot, skipped)
}

// holdSegments reserves the books slot decoded, or counts why the Slot runs
// without them.
func (stream *streamedExecution) holdSegments(ctx context.Context, slot *segmentSlot, skipped string) (*segmentSlot, error) {
	if slot != nil {
		if bytes := slot.bookBytes(); bytes > 0 {
			if err := stream.coordinator.acquireProvisional(0, bytes, stream, "segment_book", retainPhaseState); err != nil {
				keys := slot.planKeys()
				if stream.coordinator.segmentReadFails(stream.request, keys) {
					return nil, segmentsUnreadError{why: "the pool did not take the books"}
				}
				if stream.request.SegmentSkip != 0 {
					var facts observability.SegmentShadowFacts
					stream.coordinator.degradeSegments(stream.request, keys, observability.SegmentDegradedBookRefused, &facts)
					stream.coordinator.emitSegmentFacts(ctx, stream.request.Operation, &facts)
				}
				slot, skipped = nil, observability.SegmentShadowSkippedBookRefused
			} else {
				stream.retainedCharged(retainPhaseState, bytes)
			}
		}
	}
	if skipped != "" {
		stream.coordinator.skipSegmentShadow(ctx, stream.request.Operation, skipped)
	}
	return slot, nil
}

func (stream *streamedExecution) ConsumeSeries(ctx context.Context, batch execution.SeriesExecutionBatch) error {
	if !stream.began {
		return namedInputError(codeSeriesBeforeBegin, "alarmd worker: QueryExecutionSource delivered series before Begin")
	}
	series, err := stream.validateSeriesBatch(batch)
	if err != nil {
		return fmt.Errorf("alarmd worker: invalid series batch: %w", err)
	}
	retained := streamedRetainedSize(batch)
	// An overlay is kept until the series is evaluated, and charged as it is
	// kept; a series without one is charged nothing more.
	retained += hostOverlaysRetainedBytes(batch.HostOverlays)
	// A series seen for the first time is also its hold and its place among
	// its Plan's series (seriesBookkeepingBytes).
	if _, held := stream.inputHolds[series]; !held {
		retained += seriesBookkeepingBytes
	}
	if err := stream.admitSliceInput(ctx, batch, retained); err != nil {
		return err
	}
	if err := stream.reserveProvisional(ctx, batch.Delivery.Series, retained); err != nil {
		return stream.nameUnsliceableRefusal(err)
	}
	stream.addSeries(int64(batch.Delivery.Series))
	stream.retainedCharged(retainPhaseInput, retained)
	stream.noteSliceInput(batch, retained)
	stream.seams.seriesCame(time.Now())
	hold := stream.holdInput(series, retained)
	folded := make(map[*execution.Dataset]foldedDataset)
	for _, binding := range batch.Inputs {
		key := streamedInputKey{consumer: binding.Consumer, series: series, requirement: binding.RequirementID}
		existing, duplicate := stream.streamed[key]
		if !duplicate {
			// A key folded into is still one binding: the batch's bytes join
			// the series' hold above, and go when its last binding does.
			hold.bindings++
		} else {
			// A second batch for one (consumer, series, requirement) key is a
			// provider row finer than the series identity. Only a requirement
			// whose compiled algorithm declares a fold policy may fold it; every
			// other plan keeps failing deterministically.
			policy, declared := stream.prepared.foldPolicies[struct {
				consumer    execution.ConsumerRef
				requirement execution.RequirementID
			}{consumer: binding.Consumer, requirement: binding.RequirementID}]
			if !declared {
				return namedInputError(codeStreamedNamedInputDuplicate, "alarmd worker: duplicate streamed named input")
			}
			merged, known := folded[existing.dataset]
			if !known {
				var foldErr error
				merged, foldErr = foldStreamedDataset(existing.dataset, batch.Dataset, policy)
				if foldErr != nil {
					return wrapNamedInputError(codeStreamedNamedInputFoldInvalid, fmt.Errorf("alarmd worker: fold streamed named input: %w", foldErr))
				}
				folded[existing.dataset] = merged
			}
			binding.Dataset, binding.View = merged.dataset, merged.view
		}
		if err := stream.keepStreamed(ctx, key, binding); err != nil {
			return err
		}
		if binding.Role == execution.InputRolePrimary || binding.Role == execution.InputRoleMultiDetectInput {
			// An input of an item detected over several names the Plan's
			// series as the PRIMARY does: a series only it holds is a round
			// undecided for the series, never a dependency let go.
			if overlay := batch.HostOverlays[binding.Consumer.Plan]; overlay != nil {
				if stream.hostOverlays == nil {
					stream.hostOverlays = make(map[execution.PlanIdentity]map[execution.SeriesIdentityDigest]map[string]json.RawMessage)
				}
				if stream.hostOverlays[binding.Consumer.Plan] == nil {
					stream.hostOverlays[binding.Consumer.Plan] = make(map[execution.SeriesIdentityDigest]map[string]json.RawMessage)
				}
				stream.hostOverlays[binding.Consumer.Plan][series] = overlay
			}
			seriesByPlan := stream.planSeries[binding.Consumer.Plan]
			if seriesByPlan == nil {
				seriesByPlan = make(map[execution.SeriesIdentityDigest]struct{})
				stream.planSeries[binding.Consumer.Plan] = seriesByPlan
			}
			seriesByPlan[series] = struct{}{}
		}
	}
	merged := false
	for index := range stream.delivered {
		if stream.delivered[index].PhysicalQuery == batch.PhysicalQuery {
			stream.delivered[index], err = execution.AccumulateSeriesDelivery(stream.delivered[index], batch.Delivery)
			if err != nil {
				return fmt.Errorf("alarmd worker: accumulate series delivery: %w", err)
			}
			merged = true
			break
		}
	}
	if !merged {
		stream.delivered = append(stream.delivered, batch.Delivery)
	}
	return nil
}

// discardDependenciesWithoutPrimary lets go, at the completion, of every
// dependency series no PRIMARY input of its Plan named: no series of the Plan
// reads it, so it is held to no end until the Slot's. Each such series is
// counted once (DependencySeriesDiscarded), and its hold given back when it
// was the series' last binding.
func (stream *streamedExecution) discardDependenciesWithoutPrimary() {
	var released uint64
	discarded := make(map[execution.SeriesIdentityDigest]struct{})
	for key, entry := range stream.streamed {
		if entry.shared.Role == execution.InputRolePrimary {
			continue
		}
		if _, named := stream.planSeries[key.consumer.Plan][key.series]; named {
			continue
		}
		delete(stream.streamed, key)
		discarded[key.series] = struct{}{}
		hold := stream.inputHolds[key.series]
		if hold == nil {
			continue
		}
		if hold.bindings--; hold.bindings == 0 {
			delete(stream.inputHolds, key.series)
			released += hold.bytes
		}
	}
	stream.addShared(func() { stream.dependencyDiscarded += uint64(len(discarded)) })
	stream.unretainInput(released)
}

func (stream *streamedExecution) reserveProvisional(ctx context.Context, series, retainedBytes uint64) error {
	return stream.reserveProvisionalAt(ctx, series, retainedBytes, stream.reservationPhase("normal_input"), retainPhaseInput)
}

// reserveProvisionalAt reserves what this execution will hold: judged first
// by its Query Group's share, against its own total, and then by the pool.
// Every reservation a Slot holds goes through it - a series as it streams
// in, its record ids, its gap targets and its no-data memory alike - so a
// Slot over its share is refused by name for its share and never runs the
// pool, shared with the other Query Groups, out.
func (stream *streamedExecution) reserveProvisionalAt(ctx context.Context, series, retainedBytes uint64, phase string,
	charge retainPhase) error {
	err := stream.coordinator.acquireProvisional(series, retainedBytes, stream, phase, charge)
	if err == nil {
		return nil
	}
	var exceeded *provisionalBudgetExceededError
	if errors.As(err, &exceeded) {
		stream.coordinator.observeCapacityRejection(ctx, stream.request.Operation, exceeded.budget, err)
	}
	return err
}

func (stream *streamedExecution) releaseProvisional() {
	// QueryExecutionSource has joined all deliveries before Execute returns.
	// Drop the owning references before advertising reusable capacity.
	stream.header = execution.InternalExecutionHeader{}
	stream.segments = nil
	stream.prepared = preparedNamedInputIndex{}
	stream.streamed, stream.planSeries, stream.completionOnly, stream.effective = nil, nil, nil, nil
	stream.inputHolds, stream.keysHeld, stream.summaryHeld = nil, 0, 0
	stream.planSeriesCounts, stream.takenUp = nil, nil
	stream.bindings, stream.stateItems, stream.gapItems, stream.delivered = nil, nil, nil, nil
	stream.state, stream.gaps = execution.StatePreflightResult{}, execution.GapLoadResult{}
	stream.shared, stream.completionInputs, stream.primaryRecords = sharedBindings{}, completionInputs{}, 0
	stream.dependencyDiscarded = 0
	stream.noDataRefused = nil
	stream.evaluated = execution.EvaluationResult{}
	stream.summary = slotEvaluationSummary{}
	stream.seriesCensus = seriesCensus{}
	stream.coordinator.reservations.mu.Lock()
	over := releaseLocked(nil, &stream.coordinator.reservations.gapFacts, stream.gapFacts, observability.PoolOverReleaseGapFacts)
	stream.coordinator.reservations.mu.Unlock()
	stream.coordinator.observePoolOverReleases(over)
	stream.gapFacts = 0
	stream.coordinator.releaseEffects(stream.takeEffects())
	stream.effectsSpent = effectCounts{}
	stream.dropAllRecordIDs()
	stream.evaluating.end()
	stream.windows, stream.fill = slotWindows{}, sliceWindow{}
	stream.coordinator.releaseProvisional(stream.series, stream.retainedTotal())
	stream.namedOutputs = nil
	stream.series, stream.retainedByPhase, stream.namedReserved = 0, [retainPhaseCount]uint64{}, 0
	stream.retainedInputNow.Store(0)
	for phase := range stream.retainedNow {
		stream.retainedNow[phase].Store(0)
		stream.ownNow[phase].Store(0)
	}
	stream.firstMerged = nil
	if stream.holding {
		stream.coordinator.holders.remove(stream)
	}
}

// retainNamedOutputs settles what the Slot holds for the named outputs its
// queries aligned: the tables (namedOutputRecordsRetainedBytes), kept from
// the completion until the Slot ends and released with it. Their queries
// reserved them as the answers were read (ReserveRetainedBytes) and left
// the tables' own reserved, so the reservation is turned into the charge
// rather than taken again: what was reserved beyond the tables - a query
// whose answer was not used - is given back, and only what no query
// reserved is reserved now, judged as every input is. Nothing for a Slot
// whose queries asked for none.
func (stream *streamedExecution) retainNamedOutputs(ctx context.Context, completion execution.QueryExecutionCompletion) error {
	tables := namedOutputRecordsRetainedBytes(completion)
	reserved := stream.namedReserved
	switch {
	case reserved > tables:
		stream.ReleaseRetainedBytes(reserved - tables)
	case reserved < tables:
		if err := stream.ReserveRetainedBytes(ctx, tables-reserved); err != nil {
			return err
		}
	}
	return nil
}

// primaryNamedOutputs is the table of named outputs the series' primary
// query aligned, which its events' detection snapshots read; nil when that
// query asked for none.
func (stream *streamedExecution) primaryNamedOutputs(inputs []execution.SeriesEvaluationInputRequest) *execution.NamedOutputRecords {
	if stream.namedOutputs == nil {
		return nil
	}
	for _, input := range inputs {
		for _, binding := range input.Inputs {
			if binding.Role == execution.InputRolePrimary {
				if table := stream.namedOutputs[binding.Provenance.PhysicalQuery]; table != nil {
					return table
				}
			}
		}
	}
	return nil
}

// namedOutputRecordsRetainedBytes is what the named-output tables of a
// completion's queries hold (execution.NamedOutputRecords.RetainedBytes).
// Zero for a completion of queries that asked for none.
func namedOutputRecordsRetainedBytes(completion execution.QueryExecutionCompletion) uint64 {
	var total uint64
	for _, query := range completion.PhysicalQueries {
		total += query.NamedOutputs.RetainedBytes()
	}
	return total
}

// hostOverlaysRetainedBytes is what a batch's host overlays hold: their keys
// and values, and a map header for each. Zero for a batch without one.
func hostOverlaysRetainedBytes(overlays map[execution.PlanIdentity]map[string]json.RawMessage) uint64 {
	var total uint64
	for plan, overlay := range overlays {
		total += 48 + uint64(len(plan.TenantID)+len(plan.BusinessID)+len(plan.StrategyID))
		for key, value := range overlay {
			total += 48 + uint64(len(key)+len(value))
		}
	}
	return total
}

func prepareNamedInputIndex(header execution.InternalExecutionHeader) (preparedNamedInputIndex, error) {
	prepared := preparedNamedInputIndex{
		queries:                make(map[execution.PhysicalQueryDigest]execution.PlannedPhysicalQueryRef, len(header.RequiredPhysicalQueries)),
		requirementsByConsumer: make(map[execution.ConsumerRef][]execution.DataRequirement),
		requirementByKey: make(map[struct {
			consumer    execution.ConsumerRef
			requirement execution.RequirementID
		}]execution.DataRequirement),
		consumersByPlan: make(map[execution.PlanIdentity][]execution.ConsumerRef, len(header.DuePlans)),
		foldPolicies: make(map[struct {
			consumer    execution.ConsumerRef
			requirement execution.RequirementID
		}]strategy.SeriesFoldPolicy),
	}
	for _, due := range header.DuePlans {
		for _, level := range due.CompiledPlan.Levels().All() {
			consumer := execution.ConsumerRef{Plan: due.Identity, LevelID: level.Definition().LevelID, HasLevel: true}
			prepared.consumersByPlan[due.Identity] = append(prepared.consumersByPlan[due.Identity], consumer)
			for _, algorithm := range level.Algorithms().All() {
				policy, declared := algorithm.SeriesFoldPolicy()
				if !declared {
					continue
				}
				for _, requirement := range algorithm.InputRequirements() {
					prepared.foldPolicies[struct {
						consumer    execution.ConsumerRef
						requirement execution.RequirementID
					}{consumer: consumer, requirement: execution.RequirementID(requirement.RequirementID)}] = policy
				}
			}
		}
	}
	for _, query := range header.RequiredPhysicalQueries {
		prepared.queries[query.Digest] = query
	}
	for _, requirement := range header.Requirements {
		for _, binding := range requirement.Consumers {
			consumer := binding.Consumer
			if !consumer.HasLevel {
				return preparedNamedInputIndex{}, errors.New("alarmd worker: frozen named input consumer requires a Level")
			}
			key := struct {
				consumer    execution.ConsumerRef
				requirement execution.RequirementID
			}{consumer: consumer, requirement: requirement.RequirementID}
			if _, duplicate := prepared.requirementByKey[key]; duplicate {
				return preparedNamedInputIndex{}, errors.New("alarmd worker: duplicate frozen consumer requirement")
			}
			prepared.requirementByKey[key] = requirement
			prepared.requirementsByConsumer[consumer] = append(prepared.requirementsByConsumer[consumer], requirement)
		}
	}
	for plan, consumers := range prepared.consumersByPlan {
		for _, consumer := range consumers {
			requirements := prepared.requirementsByConsumer[consumer]
			if len(requirements) == 0 {
				return preparedNamedInputIndex{}, fmt.Errorf("alarmd worker: Plan %s Level %d has no frozen named input", plan.StrategyID, consumer.LevelID)
			}
			sort.Slice(requirements, func(i, j int) bool {
				if requirements[i].Role != requirements[j].Role {
					return requirements[i].Role == execution.InputRolePrimary
				}
				return requirements[i].RequirementID < requirements[j].RequirementID
			})
			primary := 0
			for _, requirement := range requirements {
				if requirement.Role == execution.InputRolePrimary {
					primary++
				}
			}
			if primary != 1 {
				return preparedNamedInputIndex{}, fmt.Errorf("alarmd worker: Plan %s Level %d requires exactly one PRIMARY named input", plan.StrategyID, consumer.LevelID)
			}
			prepared.requirementsByConsumer[consumer] = requirements
		}
	}
	return prepared, nil
}

func (stream *streamedExecution) validateSeriesBatch(batch execution.SeriesExecutionBatch) (execution.SeriesIdentityDigest, error) {
	query, found := stream.prepared.queries[batch.PhysicalQuery]
	if !found || query.QueryRevision != batch.QueryRevision || batch.CompletionRef == "" || batch.Dataset == nil ||
		batch.Dataset.Len() == 0 || len(batch.Inputs) == 0 || batch.Delivery.PhysicalQuery != batch.PhysicalQuery ||
		batch.Delivery.QueryRevision != batch.QueryRevision || batch.Delivery.Series != 1 ||
		batch.Delivery.Records != uint64(batch.Dataset.Len()) || batch.Delivery.Digest == "" {
		return "", namedInputError(codeSeriesBatchInvalid, "incomplete batch or frozen query mismatch")
	}
	var series execution.SeriesIdentityDigest
	for index := 0; index < batch.Dataset.Len(); index++ {
		record, ok := batch.Dataset.Record(index)
		candidate := execution.SeriesIdentityDigest(record.DimensionIdentityDigest())
		if !ok || candidate == "" || (series != "" && candidate != series) {
			return "", namedInputError(codeSeriesBatchNotSingleSeries, "batch does not contain exactly one stable series")
		}
		series = candidate
	}
	seen := make(map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]struct{}, len(batch.Inputs))
	for _, binding := range batch.Inputs {
		key := struct {
			consumer    execution.ConsumerRef
			requirement execution.RequirementID
		}{consumer: binding.Consumer, requirement: binding.RequirementID}
		requirement, known := stream.prepared.requirementByKey[key]
		if !known {
			return "", namedInputError(codeSeriesBindingOutsideRequirements, "binding is outside the frozen consumer requirement set")
		}
		if _, duplicate := seen[key]; duplicate {
			return "", namedInputError(codeSeriesBindingDuplicate, "batch repeats a frozen consumer requirement")
		}
		seen[key] = struct{}{}
		if binding.Dataset != batch.Dataset || binding.View == nil || !binding.View.Uses(batch.Dataset) ||
			binding.Consumer != key.consumer || binding.DatasetName != requirement.DatasetName ||
			binding.Role != requirement.Role || binding.QueryWindow != requirement.AbsoluteWindow(stream.header.Contract.Slot.EvaluationTime) ||
			binding.ProviderResult != batch.CompletionRef || binding.Provenance.PhysicalQuery != batch.PhysicalQuery ||
			binding.Provenance.AttemptNo == 0 || execution.LogicalQueryRef(query.QueryRevision) != requirement.LogicalQueryRef ||
			binding.Completeness != execution.CompletenessFull || binding.DataState != execution.DataStateData ||
			binding.Disposition != execution.AccessAvailable {
			return "", namedInputError(codeSeriesBindingMismatch, "binding differs from frozen DataRequirement or physical query")
		}
		// The series of every record was established once above, over the
		// Dataset this binding was just required to be a view of, so only the
		// window is still open per binding. Re-reading the series here cost one
		// identity read per record per consuming Plan, and a Query Group exists
		// precisely so that many Plans consume one delivery.
		for recordIndex := 0; recordIndex < binding.View.Len(); recordIndex++ {
			record, ok := binding.View.Record(recordIndex)
			if !ok || record.SourceTime() < binding.QueryWindow.Start || record.SourceTime() >= binding.QueryWindow.End {
				return "", namedInputError(codeSeriesRecordOutsideWindow, "binding contains a record outside its series or frozen query window")
			}
		}
	}
	return series, nil
}

// noDataPreflightForHeader is the load list for the Plans that detect no-data,
// and only those.
//
// A Plan without the section has no memory to read and never will, so asking
// for it would be one Redis read per Slot per Plan for an answer that is always
// "nothing there" - and it would put those Plans into the load result, where a
// reader counting no-data Plans would find every Plan in the deployment.
func noDataPreflightForHeader(header execution.InternalExecutionHeader) ([]execution.PlanNoDataLoadItem, error) {
	items := make([]execution.PlanNoDataLoadItem, 0, len(header.DuePlans))
	for _, due := range header.DuePlans {
		if due.CompiledPlan.NoData() == nil {
			continue
		}
		version, err := execution.BuildApplyVersion(header.Contract, due.StateApplyEpoch)
		if err != nil {
			return nil, err
		}
		retention, err := execution.DeriveStateRetentionRequirement(due.CompiledPlan)
		if err != nil {
			return nil, err
		}
		items = append(items, execution.PlanNoDataLoadItem{
			Identity:     due.NoDataIdentity(),
			ApplyVersion: version, ScheduleRevision: due.ScheduleRevision, Retention: retention,
		})
	}
	return items, nil
}

func gapPreflightForHeader(header execution.InternalExecutionHeader) ([]execution.PlanGapLoadItem, error) {
	items := make([]execution.PlanGapLoadItem, 0, len(header.DuePlans))
	for _, due := range header.DuePlans {
		version, err := execution.BuildApplyVersion(header.Contract, due.StateApplyEpoch)
		if err != nil {
			return nil, err
		}
		// The Plan's own retention, so the load renews the marker to the
		// lifetime its write gives it rather than to the floor.
		retention, err := execution.DeriveStateRetentionRequirement(due.CompiledPlan)
		if err != nil {
			return nil, err
		}
		items = append(items, execution.PlanGapLoadItem{
			Identity:     due.GapIdentity(),
			ApplyVersion: version, ScheduleRevision: due.ScheduleRevision, Retention: retention,
		})
	}
	return items, nil
}

func mergeProvisional(target *execution.EvaluationResult, next execution.EvaluationResult, budget ProvisionalBudget) error {
	if target.Contract != (execution.FrozenExecutionContractRef{}) && target.Contract != next.Contract {
		return errors.New("alarmd worker: series evaluations changed frozen contract")
	}
	previous, delta := countEffects(*target), addedEffectCounts(*target, next)
	if err := checkEffectCounts(effectCounts{previous.states + delta.states, previous.events + delta.events, previous.gaps + delta.gaps}, budget); err != nil {
		return err
	}
	return appendProvisional(target, next)
}

// appendProvisional merges next into target batch after batch, in the order
// they come (appendProvisionalAt).
func appendProvisional(target *execution.EvaluationResult, next execution.EvaluationResult) error {
	return appendProvisionalAt(target, next, execution.MergeOrder{}, nil)
}

// evaluatedOrders is where in merge order (execution.MergeOrder) what a
// Slot's merged results keep came from: its result's reason, and each Plan's
// first batch and the batch whose disposition it holds.
type evaluatedOrders struct {
	result execution.MergeOrder
	plans  map[execution.PlanIdentity]*planOrders
}

type planOrders struct{ first, disposed execution.MergeOrder }

// of is the Plan's orders, recorded the first time they are asked for: a
// Plan the results hold from before the Slot merged in order stands first.
func (orders *evaluatedOrders) of(plan execution.PlanIdentity) *planOrders {
	if orders.plans == nil {
		orders.plans = make(map[execution.PlanIdentity]*planOrders)
	}
	if orders.plans[plan] == nil {
		orders.plans[plan] = &planOrders{}
	}
	return orders.plans[plan]
}

// appendProvisionalAt is called only after contract and capacity checks succeed.
//
// It can still refuse, for the one thing those checks cannot see: they run on
// each batch's own result, and two batches disagreeing about a Plan's gap
// marker is a shape no single batch has.
//
// With orders, next is merged at its place order among the Slot's batches:
// a reason of the same rank as the one held, a disposition no worse than the
// one held, is taken when next stands before the batch it came from, and a
// Plan new to the results takes its place in order of the Plans' first
// batches - so batches merged in any order merge to what they do in merge
// order, which is the order a Slot read one slice at a time merges them in.
// Without orders, batches are merged as they come.
func appendProvisionalAt(target *execution.EvaluationResult, next execution.EvaluationResult, order execution.MergeOrder,
	orders *evaluatedOrders) error {
	if target.Contract == (execution.FrozenExecutionContractRef{}) {
		target.Contract, target.Result, target.ReasonCode = next.Contract, next.Result, next.ReasonCode
		target.ReasonCarried, target.ReasonOrigin = next.ReasonCarried, next.ReasonOrigin
		if orders != nil {
			orders.result = order
		}
	} else if resultRank(next.Result) > resultRank(target.Result) ||
		orders != nil && resultRank(next.Result) == resultRank(target.Result) && order.Before(orders.result) {
		target.Result, target.ReasonCode = next.Result, next.ReasonCode
		target.ReasonCarried, target.ReasonOrigin = next.ReasonCarried, next.ReasonOrigin
		if orders != nil {
			orders.result = order
		}
	}
	for _, nextPlan := range next.Plans {
		index := -1
		for current := range target.Plans {
			if target.Plans[current].Plan == nextPlan.Plan {
				index = current
				break
			}
		}
		if index < 0 {
			at := len(target.Plans)
			if orders != nil {
				for at > 0 && order.Before(orders.of(target.Plans[at-1].Plan).first) {
					at--
				}
				*orders.of(nextPlan.Plan) = planOrders{first: order, disposed: order}
			}
			target.Plans = slices.Insert(target.Plans, at, nextPlan)
			continue
		}
		if orders != nil && order.Before(orders.of(nextPlan.Plan).first) {
			// Met first in this batch, in merge order: the Plan moves to
			// its place among the Plans' first batches.
			orders.of(nextPlan.Plan).first = order
			moved := target.Plans[index]
			target.Plans = slices.Delete(target.Plans, index, index+1)
			for index > 0 && order.Before(orders.of(target.Plans[index-1].Plan).first) {
				index--
			}
			target.Plans = slices.Insert(target.Plans, index, moved)
		}
		plan := &target.Plans[index]
		if execution.PlanDispositionOutranks(nextPlan.Disposition, plan.Disposition) {
			plan.Disposition, plan.ReasonCode = nextPlan.Disposition, nextPlan.ReasonCode
			if orders != nil {
				orders.of(nextPlan.Plan).disposed = order
			}
		} else if orders != nil && !execution.PlanDispositionOutranks(plan.Disposition, nextPlan.Disposition) &&
			order.Before(orders.of(nextPlan.Plan).disposed) {
			plan.Disposition, plan.ReasonCode = nextPlan.Disposition, nextPlan.ReasonCode
			orders.of(nextPlan.Plan).disposed = order
		}
		plan.LevelOutcomes = append(plan.LevelOutcomes, nextPlan.LevelOutcomes...)
		plan.StateResults = append(plan.StateResults, nextPlan.StateResults...)
		// Read after the merge, by the commit that keeps the no-data end
		// index: every series' updates, not the first one's.
		plan.NoDataEnds = append(plan.NoDataEnds, nextPlan.NoDataEnds...)
		// Counts, so they accumulate across the series of one Slot rather than
		// being replaced by the last one merged. A Slot's window coverage is
		// the whole Slot's, and reporting only the final series would make a
		// query group of five hundred series look like a query group of one.
		plan.HistoryCoverage.Merge(nextPlan.HistoryCoverage)
		if err := mergeGapGuardStatements(plan, nextPlan); err != nil {
			return err
		}
	}
	return nil
}

type provisionalBudgetExceededError struct {
	budget observability.CapacityBudget
	facts  *observability.CapacityRejectionFacts
	// slot marks the Slot's own output exceeding a per-Slot cap, which no
	// retry in this process can satisfy; a shared reservation rejection is
	// left false because concurrent Slots free their share.
	slot bool
	// share marks this Query Group's Slot exceeding the share one object may
	// hold of the pool. Like slot it is not a pause - no amount of freed
	// capacity makes this object fit - but the action differs: the strategy has
	// to be sharded rather than the process resized.
	share bool
	// others marks this Query Group's Slot within its share refused by the
	// pool for what other Query Groups and the worker hold: a pause, as the
	// pool frees when they let go, and not this object's size.
	others bool
}

func (err *provisionalBudgetExceededError) Error() string {
	return "alarmd worker: provisional " + string(err.budget) + " budget exceeded"
}

// DeterministicShareRefusal marks a share refusal the scheduler may stop
// retrying (execution.DeterministicShareRefusal): the share flag with no
// shared usage behind it, so it is the Slot's own total that is over the
// share (acquireEffects measures it against the Slot's own total, never what
// is left of the pool), and the same window re-read is refused the same way.
// A pool rejection - shared usage above zero, the share flag unset - is not
// marked: other Slots release their part and a retry may fit.
//
// SharedUsed != 0 is redundant today - shareRejection never sets it - and
// stays on purpose: the marker's claim is "no other Query Group is part of
// why", and if the share is ever measured against what others hold, a
// refusal that reports their usage must fall back to the pool's retry rather
// than give up Slots a later round could fit.
func (err *provisionalBudgetExceededError) DeterministicShareRefusal() (execution.ShareRefusal, bool) {
	if err == nil || !err.share || err.facts == nil || err.facts.SharedUsed != 0 || err.facts.OwnUsed == nil {
		return execution.ShareRefusal{}, false
	}
	return execution.ShareRefusal{Phase: err.facts.Phase, OwnUsedBytes: *err.facts.OwnUsed,
		RequestedBytes: err.facts.Requested, LimitBytes: err.facts.Limit}, true
}

func resultRank(result observability.Result) int {
	switch result {
	case observability.ResultRetrying:
		return 4
	case observability.ResultTerminal:
		return 3
	case observability.ResultDegraded:
		return 2
	case observability.ResultSuccess:
		return 1
	default:
		return 0
	}
}

func (stream *streamedExecution) complete(ctx context.Context, completion execution.QueryExecutionCompletion) error {
	// A safe point with k consumers: a barrier another holds is waited out
	// here, and this one counts as stopped once out (slotBarrier).
	if err := stream.enterCompletion(ctx); err != nil {
		return err
	}
	defer stream.leaveCompletion()
	if !stream.began {
		return completionContractError(codeCompletionBeforeBegin, "alarmd worker: QueryExecutionSource returned completion before Begin")
	}
	// The input phase ends the moment the completion arrives, whatever this
	// call goes on to decide about it: a completion this Slot refuses still
	// waited for its records, and charging that wait to nothing would make a
	// refused Slot look instantaneous.
	// A Slot read in slices completes once a slice, each slice evaluating
	// its own series: the first completed starts the Slot's clock; loading
	// what the Slot holds whole - its gaps, its no-data memory and ends - is
	// the Slot's own step, run once whichever slice reaches it (loadSlot);
	// and what needs every series - the Plans without series, the no-data
	// round - is the Slot's step once every slice is read (finishSlices).
	// Not read by one of several consumers: the dispatch counts the slices
	// landed (landSliceRead) as the consumers complete theirs.
	first := !stream.slicing() || (stream.dispatch == nil && stream.slices.index == 0)
	if stream.dispatch != nil {
		// One of several consumers: its clock starts at its first
		// completion, and which slice is the Slot's first is said by number
		// once the slices below it are done (waitBelow, below).
		first = false
		if stream.clockSince.IsZero() {
			stream.clockPhase, stream.clockSince = slotPhaseOther, time.Now()
		}
	}
	last := !stream.slicing() || stream.slice.last
	if first {
		stream.spend(slotPhaseInput, stream.beganAt)
		stream.clockPhase, stream.clockSince = slotPhaseOther, time.Now()
	} else if stream.slice.leaveInput != nil {
		stream.slice.leaveInput()
		stream.slice.leaveInput = nil
	}
	if err := completion.Validate(stream.header, stream.delivered); err != nil {
		return wrapCompletionContractError(codeCompletionInvalid, err)
	}
	if err := stream.retainNamedOutputs(ctx, completion); err != nil {
		return err
	}
	for _, query := range completion.PhysicalQueries {
		stream.addShared(func() {
			stream.pointsDropped = stream.pointsDropped.Add(query.Stats.PointsDropped)
			stream.queryPhases.add(query.Stats)
		})
		if query.NamedOutputs != nil {
			if stream.namedOutputs == nil {
				stream.namedOutputs = make(map[execution.PhysicalQueryDigest]*execution.NamedOutputRecords)
			}
			stream.namedOutputs[query.PhysicalQuery] = query.NamedOutputs
		}
	}
	physical := make(map[execution.PhysicalQueryDigest]classifiedPhysicalCompletion, len(completion.PhysicalQueries))
	for _, item := range completion.PhysicalQueries {
		sourceBackend := physicalFailureCategory(item.RouteFacts) == observability.QueryFailureCategorySourceBackend || backendDidNotAnswer(item.RouteFacts)
		physical[item.PhysicalQuery] = classifiedPhysicalCompletion{PhysicalQueryCompletion: item, sourceBackend: sourceBackend,
			sentLate: !sourceBackend && timedOutSentLate(item.RouteFacts)}
	}
	completionBindings := make(map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding, len(completion.CompletionBindings))
	for _, binding := range completion.CompletionBindings {
		key := struct {
			consumer    execution.ConsumerRef
			requirement execution.RequirementID
		}{consumer: binding.Consumer, requirement: binding.RequirementID}
		requirement, known := stream.prepared.requirementByKey[key]
		item, ok := physical[binding.Provenance.PhysicalQuery]
		query, queryOK := stream.prepared.queries[binding.Provenance.PhysicalQuery]
		if !known || !ok || !queryOK || execution.LogicalQueryRef(query.QueryRevision) != requirement.LogicalQueryRef ||
			binding.DatasetName != requirement.DatasetName || binding.Role != requirement.Role ||
			binding.QueryWindow != requirement.AbsoluteWindow(stream.header.Contract.Slot.EvaluationTime) ||
			binding.Provenance.AttemptNo == 0 {
			return completionContractError(codeCompletionBindingMismatch, "alarmd worker: completion binding differs from frozen requirement or physical completion")
		}
		if err := execution.ValidateNamedInputCompletion(binding, item.PhysicalQueryCompletion); err != nil {
			return completionContractError(codeCompletionBindingPhysicalMismatch, "alarmd worker: completion binding differs from frozen requirement or physical completion")
		}
		if _, duplicate := completionBindings[key]; duplicate {
			return completionContractError(codeDuplicateCompletionBinding, "alarmd worker: duplicate completion binding")
		}
		stream.addShared(func() {
			stream.queryEvidence.observe(binding, item.PhysicalQueryCompletion, false, item.sourceBackend)
			stream.queryEvidence.observeSentLate(binding, item.sentLate)
		})
		stream.noteAnswerTruncation(binding, item.RouteFacts)
		completionBindings[key] = binding
	}
	// Each series' binding is settled by its query's completion. What that
	// changes is the same for every series of the query, so the settled
	// shared binding is made once per shared binding and kept shared.
	settled := make(map[*execution.NamedInputBinding]*execution.NamedInputBinding)
	for key, entry := range stream.streamed {
		binding := entry.binding()
		item, ok := physical[binding.Provenance.PhysicalQuery]
		if !ok || item.Ref != binding.ProviderResult {
			return completionContractError(codeStreamedBindingMismatch, "alarmd worker: streamed binding differs from physical completion")
		}
		stream.addShared(func() { stream.queryEvidence.observe(binding, item.PhysicalQueryCompletion, true, item.sourceBackend) })
		stream.noteAnswerTruncation(binding, item.RouteFacts)
		switch item.Completeness {
		case execution.CompletenessFull:
			if binding.Completeness != execution.CompletenessFull {
				return completionContractError(codeStreamedBindingFullMismatch, "alarmd worker: streamed binding differs from FULL physical completion")
			}
		case execution.CompletenessPartial:
			if binding.Completeness != execution.CompletenessFull {
				return completionContractError(codeStreamedBindingPartialMismatch, "alarmd worker: streamed binding differs from PARTIAL physical completion")
			}
		case execution.CompletenessUnavailable:
			entry.dataset, entry.view = nil, nil
		default:
			return completionContractError(codeInvalidCompleteness, "alarmd worker: invalid physical completion completeness")
		}
		if item.Completeness != execution.CompletenessFull {
			next, done := settled[entry.shared]
			if !done {
				changed := *entry.shared
				if item.Completeness == execution.CompletenessPartial {
					changed.Completeness = execution.CompletenessPartial
					changed.Disposition = execution.AccessDegraded
					changed.ReasonCode = execution.ReasonCode(contract.ReasonQueryPartial)
					changed.PartialEvidence = item.PartialEvidence
				} else {
					changed.Completeness = execution.CompletenessUnavailable
					changed.DataState = execution.DataStateUnknown
					changed.Disposition = execution.AccessUnavailable
					changed.ReasonCode, changed.UnavailableAttribution = physicalFailureReason(item.RouteFacts)
					changed.PartialEvidence = nil
				}
				var fresh bool
				next, fresh = stream.shared.share(changed)
				if fresh {
					if err := stream.reserveRetainedInput(ctx, sharedBindingHeldBytes(next)); err != nil {
						return err
					}
				}
				settled[entry.shared] = next
			}
			entry.shared = next
		}
		stream.streamed[key] = entry
	}

	// Every series' inputs are built here, as they always were, so a series
	// whose inputs do not make an exact set fails the Slot before anything is
	// written; they are not kept (preparedSeries), only the most PRIMARY
	// records one series holds, which bounds its events (eventsPerSeries).
	stream.completionInputs = completionInputs{physical: physical, bindings: completionBindings,
		completions: completion.PhysicalQueries}
	preparedSeriesEvaluations := make([]preparedSeries, 0, len(stream.streamed))
	for planIndex := range stream.header.DuePlans {
		due := &stream.header.DuePlans[planIndex]
		series := make([]execution.SeriesIdentityDigest, 0, len(stream.planSeries[due.Identity]))
		for identity := range stream.planSeries[due.Identity] {
			series = append(series, identity)
		}
		sort.Slice(series, func(i, j int) bool { return series[i] < series[j] })
		for _, identity := range series {
			inputs, err := stream.seriesInputs(*due, identity, physical, completionBindings, completion.PhysicalQueries)
			if err != nil {
				return err
			}
			for _, input := range inputs {
				for _, binding := range input.Inputs {
					if binding.Role == execution.InputRolePrimary && binding.View != nil {
						records := uint64(binding.View.Len())
						stream.addShared(func() { stream.primaryRecords = max(stream.primaryRecords, records) })
					}
				}
			}
			preparedSeriesEvaluations = append(preparedSeriesEvaluations, preparedSeries{due: due, identity: identity})
		}
	}
	// The prepared series stay to the Slot's end (stream.fill.prepared), each
	// its Plan and its series.
	if err := stream.reserveRetainedInput(ctx, uint64(cap(preparedSeriesEvaluations))*preparedSeriesBytes); err != nil {
		return err
	}
	// One entry per shared binding, not per series: every reader of the
	// Slot's bindings asks whether there is one, the worst or the first of
	// them, or folds them (completion attribution, gap reasons, the primary
	// input fact), and a series' copy without its Dataset is its shared
	// binding exactly.
	listed := make(map[*execution.NamedInputBinding]struct{})
	var streamed []execution.NamedInputBinding
	for _, entry := range stream.streamed {
		if _, seen := listed[entry.shared]; seen {
			continue
		}
		listed[entry.shared] = struct{}{}
		streamed = append(streamed, *entry.shared)
	}
	// Listed with its query's: what the completion said of the query stays
	// with the Plan whatever series it answered.
	stream.discardDependenciesWithoutPrimary()
	kept := make([]execution.NamedInputBinding, 0, len(completion.CompletionBindings))
	for _, binding := range completion.CompletionBindings {
		kept = append(kept, compactNamedInputBinding(binding))
	}
	// The order is taken; from here on a Plan's series are only counted.
	// What this slice bound and kept is the ledger's, and what the stream
	// reads so far is the slices numbered up to its own: those this slice's
	// head and evaluation lines read, whatever order the slices completed
	// in. A Plan none of them answered is completion-only, which only the
	// Slot's end can say (finishSlices).
	own := make(map[execution.PlanIdentity]int, len(stream.header.DuePlans))
	for _, due := range stream.header.DuePlans {
		own[due.Identity] = len(stream.planSeries[due.Identity])
	}
	stream.ledger.complete(stream.sliceNumber, own, streamed, kept)
	if stream.dispatch != nil {
		// What is read so far is the slices numbered below this one, every
		// one of them done, and the Slot's first slice is the one none of
		// them completed below (015 C1 step 3.2).
		var byNumber bool
		if err := stream.stopped(ctx, func() (err error) {
			byNumber, err = stream.ledger.waitBelow(ctx, stream.sliceNumber)
			return err
		}); err != nil {
			return err
		}
		first = byNumber
	}
	series, bindings := stream.ledger.through(stream.sliceNumber)
	counts := make(map[execution.PlanIdentity]*int, len(stream.header.DuePlans))
	for _, due := range stream.header.DuePlans {
		matched := series[due.Identity]
		counts[due.Identity] = &matched
	}
	stream.planSeries, stream.planSeriesCounts, stream.bindings = nil, counts, bindings
	// A Slot read whole knows its Plans without series here, and refuses
	// an invalid exact set of theirs before it loads or evaluates anything;
	// one read in slices knows them once every slice is read.
	if !stream.readInSlices() {
		if err := stream.validateCompletionOnlyPlans(completionBindings, completion.PhysicalQueries); err != nil {
			return err
		}
	}
	stream.load.once.Do(func() { stream.load.err = stream.loadSlot(ctx, preparedSeriesEvaluations) })
	if stream.load.err != nil {
		return stream.load.err
	}
	stream.fill.prepared, stream.fill.reached, stream.fill.dropped = preparedSeriesEvaluations, 0, 0
	if first {
		stream.fixWindowCeiling(ctx, len(preparedSeriesEvaluations))
	} else {
		stream.raiseSliceCeiling(ctx)
		if stream.dispatch != nil {
			// Heads are written in slice order (015 C1 section 5): this one
			// is handed once every slice below it handed its own.
			if err := stream.stopped(ctx, func() error { return stream.ledger.waitHeadsBelow(ctx, stream.sliceNumber) }); err != nil {
				return err
			}
		}
		if err := stream.coordinator.applySliceHead(ctx, stream); err != nil {
			return err
		}
	}
	if stream.dispatch != nil {
		stream.ledger.noteHeaded(stream.sliceNumber)
	}
	err := stream.evaluateSeries(ctx, preparedSeriesEvaluations)
	if err == nil && !stream.slicing() {
		err = stream.finishEvaluation(ctx, completion, preparedSeriesEvaluations)
	}
	replaced, err := stream.replaceBeyondSlotBudget(ctx, err)
	if err != nil {
		return err
	}
	if stream.slicing() {
		if !last {
			// A consumer finishes no slice: the dispatch finishes on the one
			// numbered last, as it was completed (finishSideBySide).
			stream.slice.replaced = replaced
			return stream.landSlice(ctx)
		}
		// The slice read last: the Slot finishes after it, on what it
		// completed with (finishSlices) - and evaluates nothing more when
		// this slice's own series took it past its budget, the replacement
		// having answered every Plan not yet passed.
		stream.slices.finish = sliceFinish{set: true, completion: completion, bindings: completionBindings,
			prepared: preparedSeriesEvaluations, replaced: replaced}
		return nil
	}
	stream.observeFinished(ctx)
	return nil
}

// slotLoad is the Slot's loads of what it holds whole and how they went:
// done once for the Slot (loadSlot).
type slotLoad struct {
	once sync.Once
	err  error
}

// loadSlot loads what the Slot holds whole: its gaps, its no-data memory -
// and a supplement's own groups of it, by its series, a supplement being
// read whole - and its no-data ends. Every slice's series are evaluated
// against them, so they are loaded once for the Slot, before any series is.
func (stream *streamedExecution) loadSlot(ctx context.Context, prepared []preparedSeries) error {
	if err := stream.loadGaps(ctx); err != nil {
		return err
	}
	if err := stream.loadNoDataMemory(ctx); err != nil {
		return err
	}
	if err := stream.readSupplementNoDataGroups(ctx, prepared); err != nil {
		return err
	}
	return stream.loadNoDataEnds(ctx)
}

// validateCompletionOnlyPlans validates the exact set of every due Plan the
// Slot holds no series of, on the completion bindings and completions that
// decide it. A supplement decides nothing for a Plan without series: that
// judgement was the Slot's, on the Slot's own read.
func (stream *streamedExecution) validateCompletionOnlyPlans(
	bindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding,
	completions []execution.PhysicalQueryCompletion,
) error {
	if stream.supplement != nil {
		return nil
	}
	for _, due := range stream.header.DuePlans {
		if stream.planSeriesCount(due.Identity) != 0 {
			continue
		}
		if err := stream.validateCompletionOnlyExactSet(due, bindings, completions); err != nil {
			return err
		}
	}
	return nil
}

// replaceBeyondSlotBudget answers an evaluation that took the Slot past a
// per-Slot cap with the replacement (completeBeyondSlotBudget) and reports
// that it did; any other error is returned as it is. A supplement over its
// budget fails as it is: the replacement opens a gap on every Plan, and a
// supplement moves no marker.
func (stream *streamedExecution) replaceBeyondSlotBudget(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	var exceeded *provisionalBudgetExceededError
	if !errors.As(err, &exceeded) || !exceeded.slot || stream.supplement != nil {
		return false, err
	}
	// Replacing the Slot's results takes every stream of it at once: with k
	// consumers it is done behind the Slot's barrier, every other consumer
	// stopped and every window landed (015 C1 section 6, C3).
	release, err := stream.holdBarrier(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return true, stream.completeBeyondSlotBudget(ctx)
}

// observeFinished reports the no-data partition once, when the Slot's
// results are final, on the replaced path as on the ordinary one: absence
// and the Plans without series were the Slot's to decide, never a
// supplement's.
func (stream *streamedExecution) observeFinished(ctx context.Context) {
	if stream.supplement == nil {
		stream.observeNoDataOutcomes(ctx)
	}
}

// preparedSeries is one PRIMARY series of a due Plan, ready for the Runtime
// State read and evaluation. It names its Plan (in the header's due Plans)
// and its series and nothing more: its named inputs are built from what the
// Slot keeps of its bindings when its evaluation batch takes it
// (preparedInputs) and go with the batch, so half a million prepared series
// are half a million of these and not of their inputs. A series dropped with
// its window is the zero value, due nil. inputs is set only by a caller that
// has them built already.
type preparedSeries struct {
	due      *execution.DuePlan
	identity execution.SeriesIdentityDigest
	inputs   []execution.SeriesEvaluationInputRequest
}

// completionInputs is what a series' inputs are built from once the
// completion is in (completedBinding, seriesInputs): the physical
// completions classified, the completion bindings by consumer and
// requirement, and the completions themselves. One per query, kept to the
// Slot's end.
type completionInputs struct {
	physical map[execution.PhysicalQueryDigest]classifiedPhysicalCompletion
	bindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding
	completions []execution.PhysicalQueryCompletion
}

// preparedInputs is a prepared series' named inputs, built as the completion
// validated them (seriesInputs).
func (stream *streamedExecution) preparedInputs(prepared preparedSeries) ([]execution.SeriesEvaluationInputRequest, error) {
	if prepared.inputs != nil || prepared.due == nil {
		return prepared.inputs, nil
	}
	inputs := stream.completionInputs
	return stream.seriesInputs(*prepared.due, prepared.identity, inputs.physical, inputs.bindings, inputs.completions)
}

// preparedBindings is every binding of a prepared series' inputs, as
// preparedInputs builds them, without building the requests: what the
// readers that only look at the bindings of every series read. The
// completion built every series' inputs once, so a binding it found is
// found again.
func (stream *streamedExecution) preparedBindings(prepared preparedSeries) []execution.NamedInputBinding {
	if prepared.due == nil {
		return nil
	}
	if prepared.inputs != nil {
		var bindings []execution.NamedInputBinding
		for _, input := range prepared.inputs {
			bindings = append(bindings, input.Inputs...)
		}
		return bindings
	}
	inputs := stream.completionInputs
	var bindings []execution.NamedInputBinding
	for _, consumer := range stream.prepared.consumersByPlan[prepared.due.Identity] {
		for _, requirement := range stream.prepared.requirementsByConsumer[consumer] {
			binding, err := stream.completedBinding(consumer, prepared.identity, requirement, inputs.physical, inputs.bindings)
			if err == nil {
				bindings = append(bindings, binding)
			}
		}
	}
	return bindings
}

// evaluateSeries reads Runtime State and evaluates every prepared series in
// Plan-then-series order. What needs every series of the Slot is
// finishEvaluation's.
func (stream *streamedExecution) evaluateSeries(
	ctx context.Context,
	preparedSeriesEvaluations []preparedSeries,
) error {
	// Runtime State is read in bounded batches ahead of evaluation while
	// evaluation itself keeps the exact Plan-then-series order. A series whose
	// PRIMARY input is incomplete needs no State read; the pending batch is
	// flushed first so its degraded result merges in order.
	batchLimit := stream.statePreflightBatchLimit()
	pending := make([]completedSeries, 0, batchLimit)
	// A full batch's State read starts as the batch is full and is evaluated
	// once the next batch is full, or at flush: the read of one batch
	// overlaps the evaluation and merge of the one before (startPreflightRead).
	// The batches stay in order, and the series of two batches are different
	// keys: a Slot's prepared series hold each (Plan, series) once - they
	// come from the per-Plan series sets the stream keeps (planSeries) - the
	// slices of a Slot read in slices are disjoint, so no series is in two,
	// and the no-data synthetic series, with digests of their own, are
	// evaluated after every real batch has been flushed (evaluateNoData),
	// never read ahead beside one. A goroutine costs nothing beside a Redis round trip, and one more
	// batch of loaded views in flight is a batch's, so every batch is read
	// ahead.
	var read struct {
		batch []completedSeries
		state *preflightRead
	}
	// No read outlives the evaluation that started it, whichever way it
	// returns.
	defer func() {
		if read.state != nil {
			<-read.state.done
		}
	}()
	evaluateRead := func() error {
		if read.state == nil {
			return nil
		}
		batch, state := read.batch, read.state
		read.batch, read.state = nil, nil
		return stream.evaluateReadBatch(ctx, batch, state)
	}
	// next is the series before the next full batch.
	next := func() error {
		if len(pending) == 0 {
			return nil
		}
		batch := pending
		pending = make([]completedSeries, 0, batchLimit)
		state := stream.startPreflightRead(ctx, batch, true)
		err := evaluateRead()
		read.batch, read.state = batch, state
		return err
	}
	flush := func() error {
		if err := next(); err != nil {
			return err
		}
		return evaluateRead()
	}
	// Counted here, before the first branch that can drop a series: this is
	// the Slot's intent, and every later count is measured against it.
	stream.addShared(func() { stream.seriesCensus.Due += len(preparedSeriesEvaluations) })
	if stream.readInSlices() {
		// A slice that held none of a Plan's series and whose PRIMARY did
		// not complete may have left out series of the Plan that no other
		// slice reads: the Plan is answered as a series whose PRIMARY did
		// not complete is, from the slice's completion, so its result says
		// the round was short as its marker and its completion do.
		for _, due := range stream.header.DuePlans {
			held := false
			for _, prepared := range preparedSeriesEvaluations {
				held = held || prepared.due != nil && prepared.due.Identity == due.Identity
			}
			var incomplete []execution.NamedInputBinding
			for _, binding := range stream.completionInputs.bindings {
				if binding.Consumer.Plan == due.Identity && binding.Role == execution.InputRolePrimary &&
					binding.Completeness != execution.CompletenessFull {
					incomplete = append(incomplete, binding)
				}
			}
			if held || len(incomplete) == 0 {
				continue
			}
			// In the order the Slot keeps its bindings, so the reason taken
			// is the same on every attempt.
			sort.Slice(incomplete, func(i, j int) bool {
				if incomplete[i].Consumer.LevelID != incomplete[j].Consumer.LevelID {
					return incomplete[i].Consumer.LevelID < incomplete[j].Consumer.LevelID
				}
				return incomplete[i].RequirementID < incomplete[j].RequirementID
			})
			stream.beginEvaluating(due.Identity)
			if err := stream.mergePrimaryIncompleteSeries(ctx, due, incomplete); err != nil {
				return err
			}
		}
	}
	for position, prepared := range preparedSeriesEvaluations {
		if stream.supplement != nil && !stream.supplementTakes(prepared) {
			// Not the supplement's to decide: taken up, by design, as
			// nothing lost (seriesNotEvaluated).
			stream.noteTakenUp(prepared.due.Identity)
			continue
		}
		// Built as its batch takes it, and gone with the batch.
		inputs, err := stream.preparedInputs(prepared)
		if err != nil {
			return err
		}
		due := *prepared.due
		if incomplete := primaryIncompleteBindings(inputs); len(incomplete) != 0 {
			if err := flush(); err != nil {
				return err
			}
			stream.beginEvaluating(due.Identity)
			// Decided as an incomplete input rather than evaluated: taken
			// up, not lost (seriesNotEvaluated).
			stream.noteTakenUp(due.Identity)
			if err := stream.mergePrimaryIncompleteSeries(ctx, due, incomplete); err != nil {
				return err
			}
			continue
		}
		version, err := execution.BuildApplyVersion(stream.header.Contract, due.StateApplyEpoch)
		if err != nil {
			return err
		}
		stream.addShared(func() { stream.seriesCensus.Read++ })
		pending = append(pending, completedSeries{due: due, series: prepared.identity, inputs: inputs,
			position: position,
			item: execution.StatePreflightItem{Identity: execution.StateKeyIdentity{
				Plan: due.Identity, StateGeneration: due.StateGeneration, SeriesIdentityDigest: prepared.identity,
			}, ApplyVersion: version, CarryFrom: carryFrom(due), Sharded: !due.Shard.IsZero(),
				VerifyCached: stream.segments.sampled(due, due.StateGeneration, segment.FamilyReal)}})
		if len(pending) >= batchLimit {
			if err := next(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// finishEvaluation is what the Slot evaluates once every series of it was:
// absence, then the Plans without series, on the completion it finishes
// with and after the series last prepared. A supplement decides neither:
// they were the Slot's to decide.
func (stream *streamedExecution) finishEvaluation(
	ctx context.Context,
	completion execution.QueryExecutionCompletion,
	preparedSeriesEvaluations []preparedSeries,
) error {
	if stream.supplement != nil {
		return nil
	}
	stream.finishing = true
	// Absence is decided after the Slot's own series, because which groups
	// reported is the evidence it is decided from. The synthetic series it
	// produces then go through the same batch as everything above.
	if err := stream.evaluateNoData(ctx, preparedSeriesEvaluations, stream.statePreflightBatchLimit()); err != nil {
		return err
	}
	if stream.summary.planCount() == 0 {
		return stream.completeWithoutSeries(ctx, completion)
	}
	for _, due := range stream.header.DuePlans {
		if stream.planSeriesCount(due.Identity) != 0 {
			continue
		}
		started := time.Now()
		stream.beginEvaluating(due.Identity)
		result, err := stream.noSeriesPlanResult(due)
		if err != nil {
			return err
		}
		stream.observeCompletionOnlyPlan(ctx, started, due, result)
		if err := stream.mergeProvisional(ctx, result, 0); err != nil {
			return err
		}
	}
	if stream.summary.planCount() != len(stream.header.DuePlans) {
		return completionContractError(codeDuePlanResultMissing, "alarmd worker: trustworthy completion did not produce every due Plan result")
	}
	return nil
}

// validateCompletionOnlyExactSet validates every Level of a Plan without
// streamed PRIMARY series against its frozen requirements and keeps the
// validated exact set as the Plan's completion-only bindings. Only that set
// decides the Plan's no-series result; streamed dependency bindings that no
// PRIMARY series of the Plan consumed are not part of it.
func (stream *streamedExecution) validateCompletionOnlyExactSet(
	due execution.DuePlan,
	completionBindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding,
	completions []execution.PhysicalQueryCompletion,
) error {
	exact := make([]execution.NamedInputBinding, 0)
	for _, consumer := range stream.prepared.consumersByPlan[due.Identity] {
		bindings := make([]execution.NamedInputBinding, 0, len(stream.prepared.requirementsByConsumer[consumer]))
		for _, requirement := range stream.prepared.requirementsByConsumer[consumer] {
			key := struct {
				consumer    execution.ConsumerRef
				requirement execution.RequirementID
			}{consumer: consumer, requirement: requirement.RequirementID}
			if _, found := completionBindings[key]; !found {
				return namedInputError(codeCompletionOnlyRequirementMissing,
					fmt.Sprintf("alarmd worker: completion-only Plan %s Level %d is missing frozen requirement %s",
						due.Identity.StrategyID, consumer.LevelID, requirement.RequirementID))
			}
			bindings = append(bindings, completionBindings[key])
		}
		if err := stream.prepared.inputBuilder.ValidateCompletionOnly(consumer, bindings, completions); err != nil {
			return wrapNamedInputError(codeCompletionOnlyExactSetInvalid,
				fmt.Errorf("alarmd worker: Plan %s Level %d completion-only exact set: %w",
					due.Identity.StrategyID, consumer.LevelID, err))
		}
		for _, binding := range bindings {
			exact = append(exact, compactNamedInputBinding(binding))
		}
	}
	stream.completionOnly[due.Identity] = exact
	return nil
}

func (stream *streamedExecution) loadGaps(ctx context.Context) error {
	var targetBytes uint64
	for _, due := range stream.header.DuePlans {
		targetBytes += retainedObjectBytes(execution.PlanGapLoadItem{Identity: due.GapIdentity()})
	}
	if err := stream.retainTargetBytes(ctx, len(stream.header.DuePlans), targetBytes); err != nil {
		return err
	}
	items, err := gapPreflightForHeader(stream.header)
	if err != nil {
		return err
	}
	stream.gapItems = items
	request := execution.GapLoadRequest{Contract: stream.header.Contract, Items: items}
	started := time.Now()
	stream.gaps, err = stream.loadGapFacts(ctx, request)
	if err == nil {
		err = execution.ValidateGapLoad(request, stream.gaps)
	}
	if err != nil {
		stream.coordinator.observe(ctx, observability.ComponentState, observability.StageGapLoaded,
			stream.request.Operation, started, "", "", err)
		return fmt.Errorf("alarmd worker: gap preflight: %w", err)
	}
	result, reason := summarizeGapLoad(stream.gaps)
	stream.coordinator.observeWithCounts(ctx, observability.ComponentState, observability.StageGapLoaded,
		stream.request.Operation, started, result, reason, observability.Counts{Keys: int64(len(stream.gaps.Items))}, nil)
	if stream.supplement != nil {
		stream.gaps, err = guardsAsOfSlot(stream.header, stream.gaps)
		return err
	}
	stream.observeGapProgress(ctx)
	return nil
}

// loadNoDataMemory reads what each no-data Plan remembers, beside the gap load
// and at the same point in the Slot.
//
// Here rather than later because the memory is evidence the evaluation needs,
// not a detail of writing it back: a Slot that reached its series before
// knowing what it remembered would have to either decide absence without the
// clocks or go back to the store mid-evaluation, and the second is what makes a
// retried Slot stop being a function of its evidence.
func (stream *streamedExecution) loadNoDataMemory(ctx context.Context) error {
	items, err := noDataPreflightForHeader(stream.header)
	if err != nil {
		return err
	}
	stream.noDataItems = items
	if len(items) == 0 {
		// No Plan in this Slot detects no-data. Nothing to read, and an empty
		// request is refused by the store rather than answered with nothing.
		stream.noData = execution.NoDataLoadResult{}
		return nil
	}
	var targetBytes uint64
	for _, item := range items {
		targetBytes += retainedObjectBytes(item)
	}
	if err := stream.retainTargetBytes(ctx, len(items), targetBytes); err != nil {
		return err
	}
	request := execution.NoDataLoadRequest{Contract: stream.header.Contract, Items: items}
	started := time.Now()
	// Each memory is read in parts, and here only its header and its manual
	// closes (decision-020 batch 8, 2.7). A Slot's round scans the groups as
	// it judges them, after every present window (decideNoDataRound); a
	// supplement asks of the memory only whether its own series' groups were
	// recorded absent at the Slot (noDataStandingAt), and reads those groups
	// by name once its series are known (readSupplementNoDataGroups).
	stream.noData, err = stream.coordinator.ports.NoData.ReadNoDataHeaders(ctx, request)
	if err == nil {
		err = execution.ValidateNoDataLoad(request, stream.noData)
	}
	if err != nil {
		stream.coordinator.observe(ctx, observability.ComponentState, observability.StageGapLoaded,
			stream.request.Operation, started, "", "", err)
		return fmt.Errorf("alarmd worker: no-data memory preflight: %w", err)
	}
	stream.coordinator.observeWithCounts(ctx, observability.ComponentState, observability.StageGapLoaded,
		stream.request.Operation, started, observability.ResultSuccess, observability.ReasonNone,
		observability.Counts{Keys: int64(len(stream.noData.Items))}, nil)
	if err := stream.reserveNoDataMemory(ctx); err != nil {
		return err
	}
	stream.observeNoDataRepresentations(ctx)
	stream.observeNoDataRenewals(ctx)
	stream.carryNoDataMemory(ctx, request)
	if stream.supplement != nil {
		// Read for what it recorded at the Slot, not to judge absence: a
		// supplement's series are a subset, and every group outside it would
		// read as absent.
		return nil
	}
	return stream.resolveNoDataRosterHosts()
}

// observeNoDataRenewals reports every renewal that reached the store.
//
// Only those: the gate answers most loads from what this process already knows,
// and counting the skips would bury the attempts that say whether renewal
// works. A failure is a failure of the renewal and not of the load -- the
// record was read and the round goes on -- so it is reported here rather than
// turned into a load status, and it is the only signal there is for it.
func (stream *streamedExecution) observeNoDataRenewals(ctx context.Context) {
	for _, renewal := range stream.noData.Renewals {
		result := observability.Result(observability.ResultSuccess)
		reason := observability.ReasonCode(observability.ReasonNone)
		if renewal.ReasonCode != "" {
			result = observability.ResultDegraded
			reason = observability.ReasonCode(renewal.ReasonCode)
		}
		stream.coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageNoDataMemoryRenewed,
			Operation: observability.Operation(stream.request.Operation),
			Result:    result, ReasonCode: reason,
			Trace: observability.TraceFields{
				StrategyID: renewal.Identity.Plan.StrategyID, BusinessID: renewal.Identity.Plan.BusinessID,
			},
			NoDataMemoryRenewal: &observability.NoDataMemoryRenewalFacts{
				Renewed: renewal.Renewed, TTLSeconds: renewal.TTLSeconds,
			},
		})
	}
}

// observeNoDataRepresentations reports which stored shape each Plan's memory
// came from, one per Plan per round.
//
// Unconditionally, and at the point the answer is known rather than where
// something is decided on it. Every Plan that was asked for is counted,
// including the ones with no memory yet, so the three counts add up to the
// Plans in the request and a reader can check that rather than assume it. A
// signal emitted only when the shape changed would say nothing at all about a
// fleet that has been half migrated for a week, which is the question this
// exists to answer.
func (stream *streamedExecution) observeNoDataRepresentations(ctx context.Context) {
	for _, snapshot := range stream.noData.Items {
		// The store stamps NONE on a snapshot that read no record, so this
		// reads the value rather than deciding it. It used to decide it, and
		// the empty spelling then reached a reader that had no way to tell
		// "no record" from "nobody said".
		representation := snapshot.Representation
		stream.coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageNoDataMemoryRead,
			Operation:  observability.Operation(stream.request.Operation),
			Result:     observability.ResultSuccess,
			ReasonCode: observability.ReasonNone,
			Trace: observability.TraceFields{
				StrategyID: snapshot.Identity.Plan.StrategyID, BusinessID: snapshot.Identity.Plan.BusinessID,
			},
			NoDataMemoryRead: &observability.NoDataMemoryReadFacts{
				Representation: string(representation), ManualClosesUnread: snapshot.ManualClosesUnread,
			},
		})
	}
}

// resolveNoDataRosterHosts asks the CMDB index about every host each no-data
// Plan's roster will consult, once the memory is in hand.
//
// After the memory rather than before it, because a history roster's hosts are
// the ones this Plan remembers: the candidates cannot be known until the record
// has been read. It is the same reason the memory is loaded here at all - it is
// evidence, and the evidence has to be complete before anything is judged.
//
// A Plan whose roster cannot be derived is left out rather than failing the
// Slot. The catalog withholds such a Plan, so reaching one here means the two
// derivations disagree, which the evaluation refuses on its own and says which
// Plan it was; failing the whole Slot would take every other Plan down with it.
func (stream *streamedExecution) resolveNoDataRosterHosts() error {
	if len(stream.noData.Items) == 0 {
		return nil
	}
	stream.noDataHosts = make(map[execution.PlanNoDataIdentity]nodata.HostResolution, len(stream.noData.Items))
	for _, due := range stream.header.DuePlans {
		config := due.CompiledPlan.NoData()
		if config == nil {
			continue
		}
		identity := due.NoDataIdentity()
		snapshot, found := stream.noData.Find(identity)
		if !found {
			continue
		}
		memory := make(map[string]nodata.GroupMemory, len(snapshot.Groups))
		for _, group := range snapshot.Groups {
			memory[group.GroupKey] = nodata.GroupMemory{
				LastSeen: group.LastSeen, FirstAbsent: group.FirstAbsent,
			}
		}
		candidates, err := nodata.HostCandidates(nodata.RosterRequest{
			AggDimension: config.AggDimension,
			Scope:        due.CompiledPlan.TargetScope(),
			Plan:         due.CompiledPlan.TargetPlan(),
			Memory:       memory,
		})
		if err != nil {
			continue
		}
		stream.noDataHosts[identity] = resolveNoDataHosts(
			execution.HostBusinessFor(stream.coordinator.ports.Hosts, due.Identity.TenantID), due.Identity.BusinessID, candidates)
	}
	return nil
}

func (stream *streamedExecution) completeWithoutSeries(
	ctx context.Context,
	completion execution.QueryExecutionCompletion,
) error {
	if stream.request.Operation == execution.OperationProbe {
		for _, binding := range completion.CompletionBindings {
			if binding.Completeness != execution.CompletenessFull {
				stream.evaluated = execution.EvaluationResult{Contract: stream.header.Contract,
					Result: observability.ResultDegraded, ReasonCode: binding.ReasonCode}
				stream.summary, stream.fill.results, stream.firstMerged = slotEvaluationSummary{}, nil, nil
				stream.observeCompletionOnlyProbe(ctx)
				return nil
			}
		}
	}
	stream.evaluated = execution.EvaluationResult{Contract: stream.header.Contract, Result: observability.ResultSuccess,
		ReasonCode: observability.ReasonNone}
	stream.summary, stream.fill.results, stream.firstMerged = slotEvaluationSummary{}, nil, nil
	for _, due := range stream.header.DuePlans {
		started := time.Now()
		planResult, err := stream.noSeriesPlanResult(due)
		if err != nil {
			return err
		}
		stream.observeCompletionOnlyPlan(ctx, started, due, planResult)
		if err := stream.mergeProvisional(ctx, planResult, 0); err != nil {
			return err
		}
	}
	return nil
}

func (stream *streamedExecution) observeCompletionOnlyProbe(ctx context.Context) {
	for _, due := range stream.header.DuePlans {
		started := time.Now()
		result := execution.EvaluationResult{Contract: stream.header.Contract,
			Result: observability.ResultSuccess, ReasonCode: observability.ReasonNone}
		for _, binding := range planBindings(stream.bindings, due.Identity) {
			if binding.Completeness != execution.CompletenessFull {
				result.Result, result.ReasonCode = observability.ResultDegraded, binding.ReasonCode
				break
			}
		}
		stream.observeCompletionOnlyPlan(ctx, started, due, result)
	}
}

// noSeriesPlanResult decides a Plan that produced no evaluated series. The
// judgement reads only the bindings noSeriesBindings returns: the PRIMARY
// completion of every consumer must be FULL for the Plan to complete FULL
// EMPTY; a PARTIAL or UNAVAILABLE PRIMARY completion opens the Plan gap with
// the completion reasons of that same set.
//
// A FULL EMPTY Plan whose every input was whole also recovers its standing
// marker, every scope of it. That is the only place it can happen: with no
// series the evaluator is never called, so the recovery that rides on a state
// mutation never runs, and a Plan whose source has gone empty - or that
// matches no series at all - kept a marker for as long as that lasted. Why a
// whole round with nothing in it counts for Level scopes too is on
// execution.PlanGapRecoveryMutation.
func (stream *streamedExecution) noSeriesPlanResult(due execution.DuePlan) (execution.EvaluationResult, error) {
	bindings := stream.noSeriesBindings(due)
	primary, found := firstNonFullPrimary(bindings)
	if !found {
		if !planCompletedFullEmpty(bindings, due.Identity) {
			return execution.EvaluationResult{}, completionContractError(codeNoSeriesPlanResultInvalid, "alarmd worker: trustworthy completion produced no series or FULL EMPTY Plan")
		}
		plan := execution.PlanEvaluationResult{Plan: due.Identity, Disposition: execution.PlanDecided}
		if emptySourceSlotIsWholeInput(bindings, due.Identity) {
			recovery, err := execution.PlanGapRecoveryMutation(stream.header.Contract, due, stream.gaps)
			if err != nil {
				return execution.EvaluationResult{}, err
			}
			if recovery != nil {
				plan.GuardAfterState = []execution.PlanGapMutation{*recovery}
			}
		}
		return execution.EvaluationResult{Contract: stream.header.Contract, Result: observability.ResultSuccess,
			ReasonCode: observability.ReasonNone,
			Plans:      []execution.PlanEvaluationResult{plan}}, nil
	}
	mutation, err := stream.completionGapMutationFor(due, bindings)
	if err != nil {
		return execution.EvaluationResult{}, err
	}
	disposition := execution.PlanDecidedDegraded
	if primary.Completeness == execution.CompletenessUnavailable {
		disposition = execution.PlanUnavailable
	}
	return execution.EvaluationResult{Contract: stream.header.Contract, Result: observability.ResultDegraded,
		ReasonCode: primary.ReasonCode,
		Plans: []execution.PlanEvaluationResult{{Plan: due.Identity, Disposition: disposition,
			ReasonCode: primary.ReasonCode, GuardBeforeEvents: []execution.PlanGapMutation{mutation}}}}, nil
}

// noSeriesBindings returns the bindings that decide a Plan's no-series result.
// A completion-only Plan (no streamed PRIMARY series) is judged by its
// completion-only exact set: exactly one completion binding per frozen
// (consumer, requirement), the set validateCompletionOnlyExactSet accepted.
// Streamed ALGORITHM_DEPENDENCY bindings that no PRIMARY series of this Plan
// consumed are left out: a dependency query that delivered series for hosts
// absent from the PRIMARY result is not evidence that the Plan had input. A
// Plan that did stream PRIMARY series keeps every binding it received.
func (stream *streamedExecution) noSeriesBindings(due execution.DuePlan) []execution.NamedInputBinding {
	if exact, found := stream.completionOnly[due.Identity]; found {
		return exact
	}
	return planBindings(stream.bindings, due.Identity)
}

func planBindings(bindings []execution.NamedInputBinding, plan execution.PlanIdentity) []execution.NamedInputBinding {
	result := make([]execution.NamedInputBinding, 0)
	for _, binding := range bindings {
		if binding.Consumer.Plan == plan {
			result = append(result, binding)
		}
	}
	return result
}

func firstNonFullPrimary(bindings []execution.NamedInputBinding) (execution.NamedInputBinding, bool) {
	for _, binding := range bindings {
		if binding.Role == execution.InputRolePrimary && binding.Completeness != execution.CompletenessFull {
			return binding, true
		}
	}
	return execution.NamedInputBinding{}, false
}

func compactNamedInputBinding(binding execution.NamedInputBinding) execution.NamedInputBinding {
	binding.Dataset, binding.View = nil, nil
	return binding
}

func planIdentityLess(left, right execution.PlanIdentity) bool {
	if left.TenantID != right.TenantID {
		return left.TenantID < right.TenantID
	}
	if left.BusinessID != right.BusinessID {
		return left.BusinessID < right.BusinessID
	}
	return left.StrategyID < right.StrategyID
}

func (stream *streamedExecution) seriesInputs(
	due execution.DuePlan,
	series execution.SeriesIdentityDigest,
	physical map[execution.PhysicalQueryDigest]classifiedPhysicalCompletion,
	completionBindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding,
	completions []execution.PhysicalQueryCompletion,
) ([]execution.SeriesEvaluationInputRequest, error) {
	consumers := stream.prepared.consumersByPlan[due.Identity]
	inputs := make([]execution.SeriesEvaluationInputRequest, 0, len(consumers))
	for _, consumer := range consumers {
		bindings := make([]execution.NamedInputBinding, 0, len(stream.prepared.requirementsByConsumer[consumer]))
		for _, requirement := range stream.prepared.requirementsByConsumer[consumer] {
			binding, err := stream.completedBinding(consumer, series, requirement, physical, completionBindings)
			if err != nil {
				return nil, fmt.Errorf("alarmd worker: Plan %s Level %d named input: %w",
					due.Identity.StrategyID, consumer.LevelID, err)
			}
			bindings = append(bindings, binding)
		}
		input, err := stream.prepared.inputBuilder.Build(consumer, series, bindings, completions)
		if err != nil {
			return nil, wrapNamedInputError(codeNamedInputExactSetInvalid,
				fmt.Errorf("alarmd worker: Plan %s Level %d named-input exact set: %w",
					due.Identity.StrategyID, consumer.LevelID, err))
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}

func (stream *streamedExecution) completedBinding(
	consumer execution.ConsumerRef,
	series execution.SeriesIdentityDigest,
	requirement execution.DataRequirement,
	physical map[execution.PhysicalQueryDigest]classifiedPhysicalCompletion,
	completionBindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding,
) (execution.NamedInputBinding, error) {
	key := streamedInputKey{consumer: consumer, series: series, requirement: requirement.RequirementID}
	if entry, found := stream.streamed[key]; found {
		return entry.binding(), nil
	}
	templateKey := struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}{consumer: consumer, requirement: requirement.RequirementID}
	if binding, found := completionBindings[templateKey]; found {
		return binding, nil
	}
	query, err := stream.queryForRequirement(consumer, requirement, completionBindings)
	if err != nil {
		return execution.NamedInputBinding{}, err
	}
	completed, found := physical[query.Digest]
	if !found {
		return execution.NamedInputBinding{}, namedInputError(codePhysicalCompletionMissing, "authoritative physical completion is missing")
	}
	binding := execution.NamedInputBinding{
		Consumer: consumer, RequirementID: requirement.RequirementID, DatasetName: requirement.DatasetName,
		Role: requirement.Role, ProviderResult: completed.Ref,
		QueryWindow:  requirement.AbsoluteWindow(stream.header.Contract.Slot.EvaluationTime),
		Completeness: completed.Completeness, Disposition: execution.AccessAvailable, ImpactScope: execution.ImpactLevel,
		PartialEvidence: completed.PartialEvidence,
		Provenance:      execution.InputProvenance{PhysicalQuery: query.Digest, AttemptNo: stream.request.AttemptNo},
	}
	switch completed.Completeness {
	case execution.CompletenessFull:
		binding.DataState = execution.DataStateEmpty
		binding.Dataset = execution.NewDataset(nil)
		binding.View, _ = execution.NewDatasetView(binding.Dataset, nil)
	case execution.CompletenessPartial:
		binding.DataState = execution.DataStateEmpty
		binding.Dataset = execution.NewDataset(nil)
		binding.View, _ = execution.NewDatasetView(binding.Dataset, nil)
		binding.Disposition = execution.AccessDegraded
		binding.ReasonCode = execution.ReasonCode(contract.ReasonQueryPartial)
	case execution.CompletenessUnavailable:
		binding.DataState = execution.DataStateUnknown
		binding.Disposition = execution.AccessUnavailable
		binding.ReasonCode, binding.UnavailableAttribution = physicalFailureReason(completed.RouteFacts)
	default:
		return execution.NamedInputBinding{}, namedInputError(codePhysicalCompletenessInvalid, "invalid physical completion completeness")
	}
	return binding, nil
}

func (stream *streamedExecution) queryForRequirement(
	consumer execution.ConsumerRef,
	requirement execution.DataRequirement,
	completionBindings map[struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}]execution.NamedInputBinding,
) (execution.PlannedPhysicalQueryRef, error) {
	for key, entry := range stream.streamed {
		if key.consumer == consumer && key.requirement == requirement.RequirementID {
			return stream.prepared.queries[entry.shared.Provenance.PhysicalQuery], nil
		}
	}
	templateKey := struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}{consumer: consumer, requirement: requirement.RequirementID}
	if binding, found := completionBindings[templateKey]; found {
		return stream.prepared.queries[binding.Provenance.PhysicalQuery], nil
	}
	var selected execution.PlannedPhysicalQueryRef
	found := false
	for _, query := range stream.prepared.queries {
		if execution.LogicalQueryRef(query.QueryRevision) != requirement.LogicalQueryRef {
			continue
		}
		if found {
			return execution.PlannedPhysicalQueryRef{}, namedInputError(codeRequirementQueryAmbiguous, "frozen requirement physical query is ambiguous")
		}
		selected, found = query, true
	}
	if !found {
		return execution.PlannedPhysicalQueryRef{}, namedInputError(codeRequirementQueryMissing, "frozen requirement physical query is missing")
	}
	return selected, nil
}

// completedSeries is one series whose inputs are complete and whose Runtime
// State view is still to be read. Views are read in one bounded batch and the
// series are then evaluated one by one in their original order.
type completedSeries struct {
	due    execution.DuePlan
	series execution.SeriesIdentityDigest
	inputs []execution.SeriesEvaluationInputRequest
	item   execution.StatePreflightItem
	// closing is a synthetic no-data series carrying the one close of a group
	// the roster stopped expecting (nodata.SyntheticSeries.Closing): its
	// recovery goes past the open-alert gate, see oneTimeClose.
	closing bool
	// position is where the series sits among the Slot's prepared series:
	// every one before it has been merged by the time it is, so a window
	// applied before its merge lets go of the inputs of exactly those. A
	// synthetic no-data series comes after every prepared one.
	position int
}

// kind is what this series is, read off its inputs rather than stored twice.
// Every input of one series carries the same kind - the evaluator refuses a set
// that does not - so the first one answers for all of them.
func (entry completedSeries) kind() execution.SeriesKind {
	if len(entry.inputs) == 0 {
		return execution.SeriesKindReal
	}
	return entry.inputs[0].Kind
}

// statePreflightBatchLimit is the shared batch bound clamped to the per-Slot
// State mutation cap, which stays at or below the store's per-call limit.
func (stream *streamedExecution) statePreflightBatchLimit() int {
	limit := execution.StatePreflightBatchItems
	if budget := stream.coordinator.slotBudget().MaxStateMutations; budget > 0 && budget < uint64(limit) {
		limit = int(budget)
	}
	return limit
}

// completeBeyondSlotBudget replaces the provisional results of a Slot whose
// own output exceeded a per-Slot cap with one UNAVAILABLE result per due
// Plan, each opening a Plan-wide gap. Nothing evaluated so far reaches Events
// or State; Progress records the coverage completion so the Slot is not
// retried into the same deterministic rejection.
//
// That is a Slot none of whose windows was written yet. Once one was, only
// the Plans the evaluation had not got past are answered that way, and the
// windows written stay (completeWindowsBeyondSlotBudget).
func (stream *streamedExecution) completeBeyondSlotBudget(ctx context.Context) error {
	if stream.windows.started {
		return stream.completeWindowsBeyondSlotBudget(ctx)
	}
	// The discarded results give back their State, Event and Gap shares so
	// the replacement gaps are counted against an empty Slot; the retained
	// bytes stay reserved until the stream releases everything.
	stream.coordinator.releaseEffects(stream.takeEffects())
	// The summary goes with the results it summarised: the replacement
	// Plans are the Slot's results now, and nothing read after evaluation
	// may still count a series that will not be written. With k consumers,
	// behind the barrier: every consumer's window in hand goes too.
	stream.mergeMu.Lock()
	stream.evaluated, stream.summary = execution.EvaluationResult{}, slotEvaluationSummary{}
	stream.mergeMu.Unlock()
	for _, each := range stream.slotStreams() {
		if each.fill.results != nil || each.firstMerged != nil {
			each.fill.results, each.firstMerged = nil, nil
		}
	}
	stream.closeNoDataBeyondSlotBudget(ctx, nil)
	reason := execution.ReasonCode(contract.ReasonExecutionBudgetExhausted)
	for _, due := range stream.header.DuePlans {
		mutation, err := stream.gapMutationForReasons(due, map[execution.GapScope]execution.ReasonCode{{}: reason})
		if err != nil {
			return err
		}
		result := execution.EvaluationResult{Contract: stream.header.Contract, Result: observability.ResultDegraded,
			ReasonCode: reason, Plans: []execution.PlanEvaluationResult{{Plan: due.Identity,
				Disposition: execution.PlanUnavailable, ReasonCode: reason,
				GuardBeforeEvents: []execution.PlanGapMutation{mutation}}}}
		if err := stream.mergeProvisional(ctx, result, 0); err != nil {
			return err
		}
	}
	// A Slot read in slices reads the slices left after it, and writes them
	// in windows: every Plan, answered for the Slot, is replaced in those as
	// the windows' own replacement replaces the Plans it answers - none of
	// its series written, its no-data withheld (completeWindowsBeyondSlotBudget).
	if stream.readInSlices() {
		windows := &stream.windows
		if windows.replaced == nil {
			windows.replaced = make(map[execution.PlanIdentity]struct{}, len(stream.header.DuePlans))
		}
		for _, due := range stream.header.DuePlans {
			windows.replaced[due.Identity] = struct{}{}
		}
	}
	return nil
}

func primaryIncompleteBindings(inputs []execution.SeriesEvaluationInputRequest) []execution.NamedInputBinding {
	primaryIncomplete := make([]execution.NamedInputBinding, 0)
	for _, input := range inputs {
		primaryIncomplete = append(primaryIncomplete, primaryIncompleteIn(input.Inputs)...)
	}
	return primaryIncomplete
}

// primaryIncompleteIn is the PRIMARY bindings of a series that are not FULL.
func primaryIncompleteIn(bindings []execution.NamedInputBinding) []execution.NamedInputBinding {
	var primaryIncomplete []execution.NamedInputBinding
	for _, binding := range bindings {
		if binding.Role == execution.InputRolePrimary && binding.Completeness != execution.CompletenessFull {
			primaryIncomplete = append(primaryIncomplete, binding)
		}
	}
	return primaryIncomplete
}

func (stream *streamedExecution) mergePrimaryIncompleteSeries(
	ctx context.Context,
	due execution.DuePlan,
	primaryIncomplete []execution.NamedInputBinding,
) error {
	disposition := execution.PlanDecidedDegraded
	reason := primaryIncomplete[0].ReasonCode
	for _, binding := range primaryIncomplete {
		if binding.Completeness == execution.CompletenessUnavailable {
			disposition = execution.PlanUnavailable
			reason = binding.ReasonCode
			break
		}
	}
	mutation, err := stream.completionGapMutationFor(due, primaryIncomplete)
	if err != nil {
		return err
	}
	return stream.mergeProvisional(ctx, execution.EvaluationResult{Contract: stream.header.Contract,
		Result: observability.ResultDegraded, ReasonCode: reason,
		Plans: []execution.PlanEvaluationResult{{Plan: due.Identity, Disposition: disposition,
			ReasonCode: reason, GuardBeforeEvents: []execution.PlanGapMutation{mutation}}}}, 0)
}

// preflightRead is one preflight batch's State read, started on its own
// goroutine while the batch before it is evaluated (prefetch), or on the
// Slot's. done closes when the read has come back; the read touches only the
// store and its own fields.
type preflightRead struct {
	request execution.StatePreflightRequest
	loaded  execution.StatePreflightResult
	err     error
	started time.Time
	timing  *execution.PreflightTiming
	done    chan struct{}
}

// startPreflightRead reads the batch's Runtime State: on a goroutine of its
// own with prefetch, so the next batch's read overlaps this batch's
// evaluation and merge, or before it returns without.
func (stream *streamedExecution) startPreflightRead(ctx context.Context, batch []completedSeries, prefetch bool) *preflightRead {
	stateItems := make([]execution.StatePreflightItem, len(batch))
	for index, entry := range batch {
		stateItems[index] = entry.item
	}
	read := &preflightRead{request: execution.StatePreflightRequest{Contract: stream.request.Contract, Items: stateItems,
		OwnerFence: stream.request.OwnerFence}, started: time.Now(), done: make(chan struct{})}
	timedCtx, timing := execution.WithPreflightTiming(ctx)
	read.timing = timing
	state := stream.coordinator.ports.State
	run := func() {
		defer close(read.done)
		read.loaded, read.err = state.LoadRuntime(timedCtx, read.request)
		if read.err == nil {
			read.loaded, read.err = execution.ClassifyStatePreflight(read.request, read.loaded)
		}
	}
	if prefetch {
		go run()
	} else {
		run()
	}
	return read
}

// evaluateReadBatch evaluates the batch from its State read, once the read
// has come back.
func (stream *streamedExecution) evaluateReadBatch(ctx context.Context, batch []completedSeries, read *preflightRead) error {
	// The batch's routine evaluations are said as it ends, however it ends:
	// each was evaluated.
	defer stream.sayRoutineEvaluations(ctx)
	// The phase is the wait for the read: a read that overlapped the batch
	// before charges only what it had left when this batch needed it.
	leave := stream.enter(slotPhasePreflight)
	<-read.done
	// Both outcomes: a read that did not come back is the one that took the
	// longest, and leaving it out would make the phase's total fall exactly
	// when the reads started timing out.
	leave()
	stateRequest, loaded, err, started, timing := read.request, read.loaded, read.err, read.started, read.timing
	stateItems := stateRequest.Items
	if err != nil {
		// The read's own facts, because they are the ones that decide what to
		// do. A read that did not come back has no byte count - that is why the
		// size is reported on the rounds that succeed - so the line carries how
		// many keys were asked for and how long the attempt took, which is what
		// separates "the dependency is down" from "we asked for too much".
		//
		// This branch is for a request the store refused outright. A read that
		// timed out arrives instead as per-item views the store has already
		// named, and summarizeStateLoad carries that name onto the row below,
		// which is why the naming lives with the classification rather than
		// being decided a second time here.
		stream.coordinator.observeWithCounts(ctx, observability.ComponentState, observability.StageStatePreflight,
			stream.request.Operation, started, "", "",
			observability.Counts{Keys: int64(len(stateItems))}, err)
		return fmt.Errorf("alarmd worker: series state preflight: %w", err)
	}
	stateResult, stateReason := summarizeStateLoad(loaded)
	stream.coordinator.observeWithCounts(ctx, observability.ComponentState, observability.StageStatePreflight,
		stream.request.Operation, started, stateResult, stateReason,
		observability.Counts{Keys: int64(len(loaded.Items)), StateBytes: loaded.LoadedBytes,
			StateFetchMillis: timing.Fetch.Milliseconds(), StateDecodeMillis: timing.Decode.Milliseconds()}, nil)
	if verify := loaded.CacheVerify; verify.Mismatched > 0 && verify.First != nil {
		// The State cache held what the store did not: let go, and the
		// round went on with what was read (014 next7 3.6).
		stream.coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageStateCacheVerified,
			Operation: observability.Operation(stream.request.Operation), Result: observability.ResultFailed,
			ReasonCode: observability.ReasonCode(contract.ReasonStateStaleVersion),
			StateCacheMismatch: &observability.StateCacheMismatchFacts{Count: verify.Mismatched,
				StrategyID: verify.First.Plan.StrategyID, Kind: verify.First.Kind, Field: verify.First.Field},
		})
	}
	diverged := loaded.CacheDiverged
	if diverged != nil && stream.coordinator.cacheDivergedSaid.CompareAndSwap(false, true) {
		// The State cache found a defect of its compact form: it checks
		// every value it keeps compact from now on, and let every compact
		// member go (014 next9 1). Said once a process.
		stream.coordinator.emitObservation(ctx, observability.Observation{
			Component: observability.ComponentState, Stage: observability.StageStateCacheDiverged,
			Operation: observability.Operation(stream.request.Operation), Result: observability.ResultFailed,
			ReasonCode: observability.ReasonCode(contract.ReasonStateStaleVersion),
			StateCacheMismatch: &observability.StateCacheMismatchFacts{Count: 1,
				StrategyID: diverged.Plan.StrategyID, Kind: diverged.Kind, Field: diverged.Field},
		})
	}
	outcomes := make(map[string]int)
	defer stream.observeCarries(ctx, outcomes)
	// The series are evaluated side by side and merged one after another in
	// the batch's order (evaluateSideBySide): everything that reads or moves
	// the Slot's own state - the window, the Plan in progress, the reservations
	// - stays on this goroutine and in this order. pending is the series
	// prepared and not yet merged; settle evaluates and merges them, and runs
	// before anything else that merges or cuts a window.
	var pending []*seriesEvaluation
	settle := func() error {
		err := stream.evaluateSideBySide(ctx, pending)
		clear(pending)
		pending = pending[:0]
		return err
	}
	for index, entry := range batch {
		// Every real series is taken up here, whatever comes of it - the
		// evaluation, its resume, its skip - and counted against those the
		// Plan was bound (seriesNotEvaluated); a series decided incomplete
		// or left to the Slot by its supplement is taken up where it is
		// set aside, before it would reach here.
		if entry.kind() == execution.SeriesKindReal {
			stream.noteTakenUp(entry.due.Identity)
		}
		// The batch stops pointing at the series' inputs as it is taken: a
		// window applied part way through the batch lets the inputs of the
		// series before go, and the batch is not to keep them alive past it.
		batch[index].inputs = nil
		view, outcome := carryHistory(entry.due, loaded.Items[index])
		if outcome != "" {
			outcomes[outcome]++
		}
		if stream.supplement != nil && stream.supplementReached(view) {
			if err := settle(); err != nil {
				return err
			}
			stream.beginEvaluating(entry.due.Identity)
			continue
		}
		// Before the evaluation and beside the supplement's own skip, for the
		// same reason: a series whose State is already past this Slot has
		// nothing for this Slot to decide. It used to reach the State
		// preflight and fail the whole Slot as STATE_STALE_VERSION, which the
		// store answers the same way on every retry -- it does not go back --
		// so the Query Group stood still until the replay expired.
		if execution.SupersededBy(view.PersistedApplyVersion, entry.item.ApplyVersion) {
			if err := settle(); err != nil {
				return err
			}
			stream.beginEvaluating(entry.due.Identity)
			if err := stream.skipSupersededSeries(ctx, entry, view); err != nil {
				return err
			}
			continue
		}
		if view.VersionComparison == execution.ApplyVersionEqual {
			if err := settle(); err != nil {
				return err
			}
			stream.beginEvaluating(entry.due.Identity)
			if err := stream.mergeResumedSeries(ctx, entry, view); err != nil {
				return err
			}
			continue
		}
		job, err := stream.prepareSeriesEvaluation(ctx, entry, view, settle)
		if err != nil {
			return err
		}
		pending = append(pending, job)
	}
	return settle()
}

// mergeEvaluatedSeries merges one evaluated series' result with the views it
// was evaluated from. Before the merge, a window the result would overfill is
// applied without it (windowBeforeMerge), letting go of the inputs of every
// prepared series before position, and the result starts the next window.
// The views join the window the result is in.
func (stream *streamedExecution) mergeEvaluatedSeries(
	ctx context.Context,
	position int,
	evaluated execution.EvaluationResult,
	retained uint64,
	views []execution.RuntimeStateView,
	items []execution.StatePreflightItem,
) error {
	defer stream.enter(slotPhaseMerge)()
	// A safe point: windows landed since give their bytes back before the
	// window check reads what the Slot holds (windowed_pipeline.go).
	if err := stream.takeLandings(ctx); err != nil {
		return err
	}
	measured := stream.measureMerge(evaluated, retained, uint64(len(views))*loadedKeyHeldBytes)
	stream.fill.reached = position
	if err := stream.windowBeforeMerge(ctx, evaluated, measured); err != nil {
		return err
	}
	if err := stream.mergeMeasured(ctx, evaluated, measured); err != nil {
		return err
	}
	stream.state.Items = append(stream.state.Items, views...)
	stream.stateItems = append(stream.stateItems, items...)
	return nil
}

func uncommittedGapMutations(gaps execution.GapLoadResult, mutations []execution.PlanGapMutation) []execution.PlanGapMutation {
	pending := mutations[:0]
	for _, mutation := range mutations {
		marker, found := gaps.Find(mutation.Identity)
		if found && (marker.Status == execution.GapFound || marker.Status == execution.GapClearedTombstone) &&
			marker.LastScheduleRevision == mutation.ScheduleRevision &&
			execution.CompareApplyVersion(marker.PersistedApplyVersion, mutation.ApplyVersion) == execution.ApplyVersionEqual {
			// Written by an earlier attempt of this Slot. A retry that
			// proposes more - a scope the marker lacks, or a worse reason,
			// as a retry read in other slices or answered worse can - grows
			// it from the revision it found (execution.ExtendSameSlotGap);
			// one that proposes nothing more writes nothing.
			if grown, extends := extendCommittedGap(marker, mutation); extends {
				pending = append(pending, grown)
			}
			continue
		}
		pending = append(pending, mutation)
	}
	return pending
}

// extendCommittedGap is mutation as the growth of a marker this Slot already
// wrote, from the revision it was read at, when it grows it.
func extendCommittedGap(marker execution.GapGuardSnapshot, mutation execution.PlanGapMutation) (execution.PlanGapMutation, bool) {
	if marker.Status != execution.GapFound {
		return execution.PlanGapMutation{}, false
	}
	if _, extends := execution.ExtendSameSlotGap(marker.Scopes, mutation.Scopes); !extends {
		return execution.PlanGapMutation{}, false
	}
	mutation.ExpectedMarkerRevision, mutation.MutationDigest = marker.MarkerRevision, ""
	grown, err := execution.BuildPlanGapMutation(mutation)
	if err != nil {
		return execution.PlanGapMutation{}, false
	}
	return grown, true
}

func (stream *streamedExecution) observeEvaluationCompleted(
	ctx context.Context,
	started time.Time,
	due execution.DuePlan,
	series execution.SeriesIdentityDigest,
	inputs []execution.SeriesEvaluationInputRequest,
	evaluated execution.EvaluationResult,
) {
	if observation, made := stream.evaluationObservation(started, due, series, inputs, evaluated); made {
		stream.sayEvaluation(ctx, due, observation)
	}
}

// evaluationObservation is a series' evaluation line, and false when making
// it panicked, which says no line: it reads the series' own facts, its Plan
// and what the stream froze for the Slot - planSeriesMatched once a Plan's
// count is cached - so the side-by-side evaluation makes it beside the
// evaluation (evaluateSideBySide).
func (stream *streamedExecution) evaluationObservation(
	started time.Time,
	due execution.DuePlan,
	series execution.SeriesIdentityDigest,
	inputs []execution.SeriesEvaluationInputRequest,
	evaluated execution.EvaluationResult,
) (observation observability.Observation, made bool) {
	// A line that panicked half made is not said: made false is all its
	// callers read (no empty line is built for them to drop).
	defer func() {
		if recover() != nil {
			made = false
		}
	}()
	evaluations, namedInputs := stream.algorithmObservationFacts(due, inputs, evaluated)
	observation = observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
		Result: evaluated.Result, Operation: observability.Operation(stream.request.Operation),
		ReasonCode: evaluated.ReasonCode,
		Duration:   time.Since(started), Counts: observability.Counts{Records: evaluationRecordCount(inputs)},
		DurationKnown: true, EvaluationOwner: costEvaluationOwner(due.Identity), EvaluationRecordsKnown: true,
		Trace:                observability.TraceFields{StrategyID: due.Identity.StrategyID, BusinessID: due.Identity.BusinessID, DimensionIdentityDigest: string(series)},
		AlgorithmEvaluations: evaluations, AlgorithmInputs: namedInputs,
		RecoveryGates: recoveryGateFacts(due, evaluated), OpenAlertGates: openAlertGateFacts(due, evaluated),
		RecoveryWithheld: recoveryWithheldFacts(due, evaluated),
		LevelOutcomes:    levelOutcomeFacts(due, evaluated),
		OutputWireFormat: planWireFormat(due), PlanSeriesMatched: stream.planSeriesMatched(due),
		// The series' reason was a stored guard's, and where it came from.
		CarriedReason: execution.CarriedReasonFacts(evaluated.ReasonCarried && evaluated.Result != observability.ResultSuccess,
			evaluated.ReasonOrigin),
	}
	return observation, true
}

// sayEvaluation says one series' evaluation line, or merges it. An observer
// that panics loses the line and fails nothing, as when the line was made
// and said in one step.
func (stream *streamedExecution) sayEvaluation(ctx context.Context, due execution.DuePlan, observation observability.Observation) {
	defer func() { _ = recover() }()
	// A routine evaluation - one the line counts and does not write, with no
	// reason and no gate of its own - is merged with its Plan's others of
	// the batch into one line: every count read from the lines is the same,
	// and a batch of thousands of series is one line a Plan, not one a
	// series (decision-020 0.6.x throughput design 3.4-3).
	if observability.Routine(observation) && observation.DurationKnown && observation.ReasonCode == "" && observation.CarriedReason == nil &&
		len(observation.RecoveryGates) == 0 && len(observation.OpenAlertGates) == 0 && len(observation.RecoveryWithheld) == 0 {
		if stream.routineEvaluations == nil {
			stream.routineEvaluations = make(map[execution.PlanIdentity]*observability.Observation)
		}
		merged := stream.routineEvaluations[due.Identity]
		if merged == nil {
			// Started from the line itself, which the merge then takes as
			// the Plan's first (Counts.Evaluations still zero).
			first := observation
			merged = &first
			stream.routineEvaluations[due.Identity] = merged
			stream.routineEvaluationsOrder = append(stream.routineEvaluationsOrder, due.Identity)
		}
		observability.MergeRoutineEvaluation(merged, observation)
		return
	}
	stream.coordinator.ports.Observer.Observe(ctx, observation)
}

// sayRoutineEvaluations says the batch's routine evaluations, a line a Plan.
func (stream *streamedExecution) sayRoutineEvaluations(ctx context.Context) {
	defer func() { _ = recover() }()
	for _, plan := range stream.routineEvaluationsOrder {
		stream.coordinator.ports.Observer.Observe(ctx, *stream.routineEvaluations[plan])
	}
	clear(stream.routineEvaluations)
	stream.routineEvaluationsOrder = stream.routineEvaluationsOrder[:0]
}

// planSeriesCount is how many series the stream bound for the Plan: from
// planSeries until the completion, from the counts it leaves after.
func (stream *streamedExecution) planSeriesCount(plan execution.PlanIdentity) int {
	if stream.planSeries == nil {
		if counted := stream.planSeriesCounts[plan]; counted != nil {
			return *counted
		}
		return 0
	}
	return len(stream.planSeries[plan])
}

// noteTakenUp counts one real series of the Plan taken up.
func (stream *streamedExecution) noteTakenUp(plan execution.PlanIdentity) {
	stream.sharedMu.Lock()
	defer stream.sharedMu.Unlock()
	if stream.takenUp == nil {
		stream.takenUp = make(map[execution.PlanIdentity]int)
	}
	stream.takenUp[plan]++
}

// seriesNotEvaluated is, by algorithm family, the PRIMARY series the Slot
// bound to each due Plan (planSeriesCount) beyond those it took up: series
// a query answered that reached no evaluation, no resume and no skip. A
// Plan of several families counts its series under each; one of none that
// AlgorithmFamily names under none; in the order of
// observability.SeriesNotEvaluatedFamilies, all zero when every series was
// taken up. It is seriesNotEvaluatedPlans summed by family.
func (stream *streamedExecution) seriesNotEvaluated() [observability.SeriesNotEvaluatedFamilyCount]uint64 {
	var lost [observability.SeriesNotEvaluatedFamilyCount]uint64
	for _, plan := range stream.seriesNotEvaluatedPlans() {
		index := slices.Index(observability.SeriesNotEvaluatedFamilies[:], plan.Family)
		if index < 0 {
			index = slices.Index(observability.SeriesNotEvaluatedFamilies[:], observability.SeriesNotEvaluatedNoFamily)
		}
		lost[index] += plan.Missing
	}
	return lost
}

// seriesNotEvaluatedPlans is the same loss Plan by Plan, for the
// series_not_evaluated line: each due Plan that lost series, once under each
// of its families, in the order the header lists them. Empty when every
// series was taken up.
func (stream *streamedExecution) seriesNotEvaluatedPlans() []observability.SeriesNotEvaluatedPlan {
	var plans []observability.SeriesNotEvaluatedPlan
	for _, due := range stream.header.DuePlans {
		missing := stream.planSeriesCount(due.Identity) - stream.takenUp[due.Identity]
		if missing <= 0 {
			continue
		}
		for _, family := range planAlgorithmFamilies(due) {
			plans = append(plans, observability.SeriesNotEvaluatedPlan{TenantID: due.Identity.TenantID,
				BusinessID: due.Identity.BusinessID, StrategyID: due.Identity.StrategyID, Family: family, Missing: uint64(missing)})
		}
	}
	return plans
}

// planAlgorithmFamilies is the distinct algorithm families of the Plan's
// Levels, as the algorithm counters name them, or none.
func planAlgorithmFamilies(due execution.DuePlan) []string {
	var families []string
	if due.CompiledPlan != nil {
		for _, level := range due.CompiledPlan.Levels().All() {
			for _, algorithm := range level.Algorithms().All() {
				if observed, ok := observeAlgorithm(algorithm); ok && !slices.Contains(families, string(observed.family)) {
					families = append(families, string(observed.family))
				}
			}
		}
	}
	if len(families) == 0 {
		families = append(families, observability.SeriesNotEvaluatedNoFamily)
	}
	return families
}

// planSeriesMatched is how many PRIMARY series the Slot bound to the Plan,
// for the line: the number that separates a guard warming from a guard on a
// Plan with nothing to warm on. Counted from what the stream bound, which is
// what the evaluation ran on: a Slot read whole, every series of it, final
// by the first line because evaluation starts at complete, after every
// delivery has been joined; a Slot read in slices, the series of the slices
// numbered up to the one the line's series is in (sliceLedger.through) -
// the Slot's own once its last is read - whatever order they completed in.
func (stream *streamedExecution) planSeriesMatched(due execution.DuePlan) *int {
	if cached := stream.planSeriesCounts[due.Identity]; cached != nil {
		return cached
	}
	matched := stream.planSeriesCount(due.Identity)
	if stream.planSeriesCounts == nil {
		stream.planSeriesCounts = make(map[execution.PlanIdentity]*int)
	}
	stream.planSeriesCounts[due.Identity] = &matched
	return &matched
}

// planWireFormat is the format the Plan's events go out as, for the
// evaluation line: the word was frozen into the Plan and carried on every
// event and reached no log, so the one question "how many strategies
// publish the standard raw event" had no line to answer it from.
//
// Already resolved: CompiledPlan.WireFormat is
// contract.ResolveOutputWireFormat over the frozen word and the revision,
// the same call the sink and the leader's composition make, so an empty or
// historical frozen word never reaches the line as itself.
func planWireFormat(due execution.DuePlan) string {
	if due.CompiledPlan == nil {
		return ""
	}
	return due.CompiledPlan.WireFormat()
}

func costEvaluationOwner(identity execution.PlanIdentity) observability.CostPlanIdentity {
	return observability.CostPlanIdentity{TenantID: identity.TenantID, BusinessID: identity.BusinessID, StrategyID: identity.StrategyID}
}

func (stream *streamedExecution) observeEvaluationFailure(ctx context.Context, started time.Time, due execution.DuePlan, err error) {
	stream.coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
		Operation: observability.Operation(stream.request.Operation),
		Duration:  time.Since(started), DurationKnown: true, Err: err,
		EvaluationOwner: costEvaluationOwner(due.Identity),
	})
}

// openAlertGateFacts carries what the open alert gate did with the
// Plan's RECOVERY records, one fact per outcome that counted something.
// levelOutcomeFacts is what the evaluation concluded per Level outcome and,
// for the outcomes that carry one, per reason (a withheld recovery's, on
// NORMAL): the line's own reason is the
// Plan's fold, one word for the worst Level, so a Level suppressed by its
// effective time or held on a warming window had no name on the line unless
// it was that word. Sorted by outcome then reason so the line is stable.
func levelOutcomeFacts(due execution.DuePlan, evaluated execution.EvaluationResult) []observability.LevelOutcomeFact {
	if len(evaluated.Plans) != 1 || evaluated.Plans[0].Plan != due.Identity {
		return nil
	}
	type cell struct {
		outcome, reason string
		carried         bool
	}
	counts := make(map[cell]uint64)
	omitted := make(map[cell]uint64)
	for _, outcome := range evaluated.Plans[0].LevelOutcomes {
		key := cell{outcome: string(outcome.Outcome)}
		if outcome.Outcome == execution.LevelOutcomeUnknown || outcome.Outcome == execution.LevelOutcomeTerminal {
			key.reason = string(outcome.ReasonCode)
		}
		// A NORMAL that met its recovery windows and was held NORMAL is
		// counted under why: without it a zero holding alerts open moved
		// counts from RECOVERY to NORMAL with nothing on the family to say
		// which rule did it.
		if outcome.Outcome == execution.LevelOutcomeNormal && outcome.RecoveryWithheld != execution.RecoveryWithheldNone {
			key.reason = outcome.RecoveryWithheld.Reason()
		}
		// A stored guard's reason repeated on this round is its own cell:
		// QUERY_TIMEOUT carried from the round that timed out is not this
		// round timing out.
		key.carried = outcome.Outcome == execution.LevelOutcomeUnknown && outcome.ReasonCarried
		counts[key]++
		// A RECOVERY its record's envelope left out (the alert is at another
		// Level) is still a RECOVERY of its Level; the cell says how many.
		if outcome.Outcome == execution.LevelOutcomeRecovery && outcome.EventOmitted {
			omitted[key]++
		}
	}
	facts := make([]observability.LevelOutcomeFact, 0, len(counts))
	for key, count := range counts {
		facts = append(facts, observability.LevelOutcomeFact{Outcome: key.outcome, Reason: key.reason, Carried: key.carried, Count: count,
			OmittedRecoveries: omitted[key]})
	}
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].Outcome != facts[j].Outcome {
			return facts[i].Outcome < facts[j].Outcome
		}
		if facts[i].Reason != facts[j].Reason {
			return facts[i].Reason < facts[j].Reason
		}
		return !facts[i].Carried && facts[j].Carried
	})
	return facts
}

func openAlertGateFacts(due execution.DuePlan, evaluated execution.EvaluationResult) []observability.OpenAlertGateFact {
	if len(evaluated.Plans) != 1 || evaluated.Plans[0].Plan != due.Identity {
		return nil
	}
	gate := evaluated.Plans[0].OpenAlertGate
	var facts []observability.OpenAlertGateFact
	for _, fact := range []observability.OpenAlertGateFact{
		{Outcome: observability.OpenAlertGatePassed, Records: gate.Passed},
		{Outcome: observability.OpenAlertGatePassedOneTimeClose, Records: gate.PassedOneTimeClose},
		{Outcome: observability.OpenAlertGateHeldNoOpenAlert, Records: gate.HeldNoOpenAlert},
		{Outcome: observability.OpenAlertGateHeldFingerprintUnknown, Records: gate.HeldFingerprintUnknown},
		{Outcome: observability.OpenAlertGateHeldLevelNotOpen, Records: gate.HeldLevelNotOpen},
		{Outcome: observability.OpenAlertGateNotConfigured, Records: gate.NotConfigured},
		{Outcome: observability.OpenAlertGateProtocolNotGated, Records: gate.ProtocolNotGated},
	} {
		if fact.Records > 0 {
			facts = append(facts, fact)
		}
	}
	return facts
}

// recoveryWithheldFacts carries the Plan's Level outcomes whose recovery was
// withheld (LevelOutcome.RecoveryWithheld), for the Plan the observation is
// about; none when nothing was withheld.
func recoveryWithheldFacts(due execution.DuePlan, evaluated execution.EvaluationResult) []observability.RecoveryWithheldFact {
	if len(evaluated.Plans) != 1 || evaluated.Plans[0].Plan != due.Identity {
		return nil
	}
	counts := map[execution.RecoveryWithheldCode]uint64{}
	for _, outcome := range evaluated.Plans[0].LevelOutcomes {
		if outcome.RecoveryWithheld != execution.RecoveryWithheldNone {
			counts[outcome.RecoveryWithheld]++
		}
	}
	var facts []observability.RecoveryWithheldFact
	for _, code := range execution.RecoveryWithheldCodes {
		if counts[code] > 0 {
			facts = append(facts, observability.RecoveryWithheldFact{Reason: observability.RecoveryWithheldReason(code.Reason()), Levels: counts[code]})
		}
	}
	return facts
}

// recoveryGateFacts carries the Plan's RECOVERY records by the state of the
// other Level each was decided beside. Zero counts are left out; the observer
// drops them anyway.
func recoveryGateFacts(due execution.DuePlan, evaluated execution.EvaluationResult) []observability.RecoveryGateFact {
	if len(evaluated.Plans) != 1 || evaluated.Plans[0].Plan != due.Identity {
		return nil
	}
	gate := evaluated.Plans[0].RecoveryGate
	var facts []observability.RecoveryGateFact
	for _, fact := range []observability.RecoveryGateFact{
		{Cause: observability.RecoveryGateLevelUnavailable, Records: gate.BesideLevelUnavailable},
		{Cause: observability.RecoveryGateLevelRecovering, Records: gate.BesideLevelRecovering},
		{Cause: observability.RecoveryGateLevelWithoutRecovery, Records: gate.BesideLevelWithoutRecovery},
	} {
		if fact.Records > 0 {
			facts = append(facts, fact)
		}
	}
	return facts
}

// observeCompletionOnlyPlan reports a Plan decided without a series, from
// its completions alone. The decision is the Plan's evaluation this round,
// short as it is, and is timed like any other: a completion that carried no
// duration read as an evaluation whose cost nobody measured, and every Plan
// without a series left one such line a round.
func (stream *streamedExecution) observeCompletionOnlyPlan(
	ctx context.Context,
	started time.Time,
	due execution.DuePlan,
	evaluated execution.EvaluationResult,
) {
	defer func() { _ = recover() }()
	observation := observability.Observation{
		Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
		Result: evaluated.Result, Operation: observability.Operation(stream.request.Operation),
		ReasonCode: evaluated.ReasonCode,
		Duration:   time.Since(started), DurationKnown: true,
		EvaluationOwner: costEvaluationOwner(due.Identity), EvaluationRecordsKnown: true,
		Trace:             observability.TraceFields{StrategyID: due.Identity.StrategyID, BusinessID: due.Identity.BusinessID},
		AlgorithmInputs:   stream.completionOnlyAlgorithmInputFacts(due),
		OutputWireFormat:  planWireFormat(due),
		PlanSeriesMatched: stream.planSeriesMatched(due),
	}
	stream.coordinator.ports.Observer.Observe(ctx, observation)
}

type observedAlgorithm struct {
	family       observability.AlgorithmFamily
	detector     observability.AlgorithmDetectorKind
	requirements map[execution.RequirementID]struct{}
}

func (stream *streamedExecution) algorithmObservationFacts(
	due execution.DuePlan,
	inputs []execution.SeriesEvaluationInputRequest,
	evaluated execution.EvaluationResult,
) ([]observability.AlgorithmEvaluationFact, []observability.AlgorithmInputFact) {
	if due.CompiledPlan == nil || len(evaluated.Plans) != 1 || evaluated.Plans[0].Plan != due.Identity {
		return nil, nil
	}
	levels := make(map[uint32][]observedAlgorithm, due.CompiledPlan.Levels().Len())
	for _, level := range due.CompiledPlan.Levels().All() {
		for _, algorithm := range level.Algorithms().All() {
			observed, ok := observeAlgorithm(algorithm)
			if ok {
				levels[level.Definition().LevelID] = append(levels[level.Definition().LevelID], observed)
			}
		}
	}
	evaluations := make([]observability.AlgorithmEvaluationFact, 0)
	inputsByLevel := make(map[uint32]execution.SeriesEvaluationInputRequest, len(inputs))
	for _, input := range inputs {
		inputsByLevel[input.Consumer.LevelID] = input
	}
	inputFacts := make([]observability.AlgorithmInputFact, 0)
	for _, outcome := range evaluated.Plans[0].LevelOutcomes {
		algorithms := levels[outcome.LevelID]
		for _, algorithm := range algorithms {
			evaluations = append(evaluations, observability.AlgorithmEvaluationFact{
				SourceAlgorithmFamily: algorithm.family, DetectorKind: algorithm.detector,
				Result: algorithmEvaluationResult(outcome.Outcome), ReasonCode: outcome.ReasonCode,
				ReasonCarried: outcome.Outcome == execution.LevelOutcomeUnknown && outcome.ReasonCarried,
				Provenance:    observability.AlgorithmProvenance{LevelID: outcome.LevelID, SourceTime: outcome.Record.SourceTime},
			})
			input, found := inputsByLevel[outcome.LevelID]
			if !found {
				continue
			}
			for _, binding := range oneBindingARequirement(input.Inputs, func(binding execution.NamedInputBinding) bool {
				return algorithmObservesBinding(algorithm, binding)
			}) {
				inputFacts = append(inputFacts, stream.algorithmInputFacts(algorithm, outcome, binding)...)
			}
		}
	}
	return evaluations, inputFacts
}

func observeAlgorithm(algorithm strategy.CompiledAlgorithmPlan) (observedAlgorithm, bool) {
	observed := observedAlgorithm{requirements: make(map[execution.RequirementID]struct{})}
	switch algorithm.Kind() {
	case strategy.DetectorKindThreshold:
		observed.family = observability.AlgorithmFamilyThreshold
		observed.detector = observability.AlgorithmDetectorKindThreshold
	case strategy.DetectorKindSimpleRingRatio:
		observed.family = observability.AlgorithmFamilySimpleRingRatio
		observed.detector = observability.AlgorithmDetectorKindSimpleRingRatio
	case strategy.DetectorKindOsRestart:
		observed.family = observability.AlgorithmFamilyOsRestart
		observed.detector = observability.AlgorithmDetectorKindOsRestart
	case strategy.DetectorKindProcPort:
		observed.family = observability.AlgorithmFamilyProcPort
		observed.detector = observability.AlgorithmDetectorKindProcPort
	case strategy.DetectorKindSimpleYearRound:
		observed.family = observability.AlgorithmFamilySimpleYearRound
		observed.detector = observability.AlgorithmDetectorKindSimpleYearRound
	case strategy.DetectorKindAdvancedRingRatio:
		observed.family = observability.AlgorithmFamilyAdvancedRingRatio
		observed.detector = observability.AlgorithmDetectorKindAdvancedRingRatio
	case strategy.DetectorKindAdvancedYearRound:
		observed.family = observability.AlgorithmFamilyAdvancedYearRound
		observed.detector = observability.AlgorithmDetectorKindAdvancedYearRound
	case strategy.DetectorKindRingRatioAmplitude:
		observed.family = observability.AlgorithmFamilyRingRatioAmplitude
		observed.detector = observability.AlgorithmDetectorKindRingRatioAmplitude
	case strategy.DetectorKindYearRoundAmplitude:
		observed.family = observability.AlgorithmFamilyYearRoundAmplitude
		observed.detector = observability.AlgorithmDetectorKindYearRoundAmplitude
	case strategy.DetectorKindYearRoundRange:
		observed.family = observability.AlgorithmFamilyYearRoundRange
		observed.detector = observability.AlgorithmDetectorKindYearRoundRange
	default:
		return observedAlgorithm{}, false
	}
	if provenance, ok := algorithm.SourceProvenance(); ok &&
		provenance.SourceAlgorithmFamily == strategy.SourceAlgorithmFamilyPingUnreachable &&
		algorithm.Kind() == strategy.DetectorKindThreshold {
		observed.family = observability.AlgorithmFamilyPingUnreachable
	}
	for _, requirement := range algorithm.InputRequirements() {
		observed.requirements[execution.RequirementID(requirement.RequirementID)] = struct{}{}
	}
	return observed, true
}

func algorithmObservesBinding(algorithm observedAlgorithm, binding execution.NamedInputBinding) bool {
	if len(algorithm.requirements) == 0 {
		return binding.Role == execution.InputRolePrimary
	}
	_, found := algorithm.requirements[binding.RequirementID]
	return found
}

// oneBindingARequirement is the bindings keep takes, one a requirement: a
// Plan answered from its completion in a Slot read in slices carries a
// binding of each slice that did not complete beside the first slice's, for
// the same requirement, and its input is one input of the round,
// counted once. The one taken is the first that did not complete, if one did
// not - the round is as short as its shortest slice - and the first
// otherwise, in the order the requirements first appear.
func oneBindingARequirement(bindings []execution.NamedInputBinding, keep func(execution.NamedInputBinding) bool) []execution.NamedInputBinding {
	chosen := make(map[execution.RequirementID]int, len(bindings))
	order := make([]execution.RequirementID, 0, len(bindings))
	for index, binding := range bindings {
		if !keep(binding) {
			continue
		}
		earlier, seen := chosen[binding.RequirementID]
		if !seen {
			order = append(order, binding.RequirementID)
		}
		if !seen || bindings[earlier].Completeness == execution.CompletenessFull && binding.Completeness != execution.CompletenessFull {
			chosen[binding.RequirementID] = index
		}
	}
	taken := make([]execution.NamedInputBinding, 0, len(order))
	for _, requirement := range order {
		taken = append(taken, bindings[chosen[requirement]])
	}
	return taken
}

func (stream *streamedExecution) completionOnlyAlgorithmInputFacts(
	due execution.DuePlan,
) []observability.AlgorithmInputFact {
	if due.CompiledPlan == nil {
		return nil
	}
	bindings := planBindings(stream.bindings, due.Identity)
	facts := make([]observability.AlgorithmInputFact, 0)
	for _, level := range due.CompiledPlan.Levels().All() {
		levelID := level.Definition().LevelID
		for _, compiled := range level.Algorithms().All() {
			algorithm, observed := observeAlgorithm(compiled)
			if !observed {
				continue
			}
			for _, binding := range oneBindingARequirement(bindings, func(binding execution.NamedInputBinding) bool {
				return binding.Consumer.HasLevel && binding.Consumer.LevelID == levelID && algorithmObservesBinding(algorithm, binding)
			}) {
				facts = append(facts, stream.completionOnlyAlgorithmBindingFacts(algorithm, levelID, binding)...)
			}
		}
	}
	return facts
}

func (stream *streamedExecution) completionOnlyAlgorithmBindingFacts(
	algorithm observedAlgorithm,
	levelID uint32,
	binding execution.NamedInputBinding,
) []observability.AlgorithmInputFact {
	key := struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}{consumer: binding.Consumer, requirement: binding.RequirementID}
	requirement, known := stream.prepared.requirementByKey[key]
	query, queryKnown := stream.prepared.queries[binding.Provenance.PhysicalQuery]
	if !known || !queryKnown || execution.LogicalQueryRef(query.QueryRevision) != requirement.LogicalQueryRef {
		return nil
	}
	result, reason, valid := completionOnlyAlgorithmInputResult(binding)
	if !valid {
		return nil
	}
	points := make([]struct {
		name       observability.AlgorithmInputName
		dependency observability.AlgorithmDependencyPoint
	}, 0, len(requirement.NamedPoints)+1)
	if binding.Role == execution.InputRolePrimary {
		points = append(points, struct {
			name       observability.AlgorithmInputName
			dependency observability.AlgorithmDependencyPoint
		}{name: observability.AlgorithmInputNamePrimary, dependency: observability.AlgorithmDependencyPointCurrent})
	} else {
		for _, point := range requirement.NamedPoints {
			dependency, observed := observedDependencyPoint(point.Name)
			if observed {
				points = append(points, struct {
					name       observability.AlgorithmInputName
					dependency observability.AlgorithmDependencyPoint
				}{name: observability.AlgorithmInputNameHistory, dependency: dependency})
			}
		}
	}
	facts := make([]observability.AlgorithmInputFact, 0, len(points))
	for _, point := range points {
		facts = append(facts, observability.AlgorithmInputFact{
			SourceAlgorithmFamily: algorithm.family, DetectorKind: algorithm.detector,
			InputName: point.name, DependencyPoint: point.dependency, Result: result, ReasonCode: reason,
			Provenance: observability.AlgorithmProvenance{
				LevelID: levelID, RequirementID: string(binding.RequirementID),
				QueryRef: string(binding.Provenance.PhysicalQuery), QueryRevision: string(query.QueryRevision),
				QueryStart: binding.QueryWindow.Start, QueryEnd: binding.QueryWindow.End,
			},
		})
	}
	return facts
}

func completionOnlyAlgorithmInputResult(
	binding execution.NamedInputBinding,
) (observability.AlgorithmInputResult, observability.ReasonCode, bool) {
	switch binding.Completeness {
	case execution.CompletenessFull:
		if binding.DataState == execution.DataStateEmpty && binding.Disposition == execution.AccessAvailable {
			return observability.AlgorithmInputResultMissing, observability.ReasonNone, true
		}
	case execution.CompletenessPartial:
		return observability.AlgorithmInputResultPartial, binding.ReasonCode, true
	case execution.CompletenessUnavailable:
		return observability.AlgorithmInputResultUnavailable, binding.ReasonCode, true
	}
	return "", "", false
}

func algorithmEvaluationResult(outcome execution.LevelOutcomeKind) observability.AlgorithmEvaluationResult {
	switch outcome {
	case execution.LevelOutcomeNormal:
		return observability.AlgorithmEvaluationResultNormal
	case execution.LevelOutcomeAbnormal:
		return observability.AlgorithmEvaluationResultAbnormal
	case execution.LevelOutcomeRecovery:
		return observability.AlgorithmEvaluationResultRecovery
	case execution.LevelOutcomeTerminal:
		return observability.AlgorithmEvaluationResultTerminal
	default:
		return observability.AlgorithmEvaluationResultUnavailable
	}
}

func (stream *streamedExecution) algorithmInputFacts(
	algorithm observedAlgorithm,
	outcome execution.LevelOutcome,
	binding execution.NamedInputBinding,
) []observability.AlgorithmInputFact {
	key := struct {
		consumer    execution.ConsumerRef
		requirement execution.RequirementID
	}{consumer: binding.Consumer, requirement: binding.RequirementID}
	requirement, known := stream.prepared.requirementByKey[key]
	query, queryKnown := stream.prepared.queries[binding.Provenance.PhysicalQuery]
	if !known || !queryKnown || execution.LogicalQueryRef(query.QueryRevision) != requirement.LogicalQueryRef {
		return nil
	}
	points := []struct {
		name       observability.AlgorithmInputName
		dependency observability.AlgorithmDependencyPoint
		sourceTime int64
	}{}
	if binding.Role == execution.InputRolePrimary {
		points = append(points, struct {
			name       observability.AlgorithmInputName
			dependency observability.AlgorithmDependencyPoint
			sourceTime int64
		}{observability.AlgorithmInputNamePrimary, observability.AlgorithmDependencyPointCurrent, outcome.Record.SourceTime})
	} else {
		for _, point := range requirement.NamedPoints {
			dependency, ok := observedDependencyPoint(point.Name)
			if ok {
				points = append(points, struct {
					name       observability.AlgorithmInputName
					dependency observability.AlgorithmDependencyPoint
					sourceTime int64
				}{observability.AlgorithmInputNameHistory, dependency, outcome.Record.SourceTime - point.OffsetSeconds})
			}
		}
	}
	facts := make([]observability.AlgorithmInputFact, 0, len(points))
	for _, point := range points {
		result, reason := algorithmInputResult(binding, outcome.SeriesIdentityDigest, point.sourceTime)
		facts = append(facts, observability.AlgorithmInputFact{
			SourceAlgorithmFamily: algorithm.family, DetectorKind: algorithm.detector,
			InputName: point.name, DependencyPoint: point.dependency, Result: result, ReasonCode: reason,
			Provenance: observability.AlgorithmProvenance{
				LevelID: outcome.LevelID, RequirementID: string(binding.RequirementID),
				QueryRef: string(binding.Provenance.PhysicalQuery), QueryRevision: string(query.QueryRevision),
				SourceTime: point.sourceTime, QueryStart: binding.QueryWindow.Start, QueryEnd: binding.QueryWindow.End,
			},
		})
	}
	return facts
}

func observedDependencyPoint(name string) (observability.AlgorithmDependencyPoint, bool) {
	switch name {
	case "previous":
		return observability.AlgorithmDependencyPointPrevious, true
	case "previous_10m":
		return observability.AlgorithmDependencyPointTenMinute, true
	case "previous_25m":
		return observability.AlgorithmDependencyPointTwentyFiveMinute, true
	default:
		if raw, ok := strings.CutPrefix(name, "history_"); ok {
			if offset, err := strconv.ParseInt(raw, 10, 64); err == nil && offset > 0 {
				return observability.AlgorithmDependencyPointHistorical, true
			}
		}
		return "", false
	}
}

func algorithmInputResult(
	binding execution.NamedInputBinding,
	series execution.SeriesIdentityDigest,
	sourceTime int64,
) (observability.AlgorithmInputResult, observability.ReasonCode) {
	switch binding.Completeness {
	case execution.CompletenessPartial:
		return observability.AlgorithmInputResultPartial, binding.ReasonCode
	case execution.CompletenessUnavailable:
		return observability.AlgorithmInputResultUnavailable, binding.ReasonCode
	case execution.CompletenessFull:
	default:
		return observability.AlgorithmInputResultUnavailable, binding.ReasonCode
	}
	if binding.Disposition != execution.AccessAvailable || binding.Dataset == nil || binding.View == nil {
		return observability.AlgorithmInputResultUnavailable, binding.ReasonCode
	}
	for _, fact := range binding.QualityFacts {
		if inputFactApplies(fact.ImpactScope, fact.SeriesIdentity, fact.SourceTime, series, sourceTime) {
			return observability.AlgorithmInputResultUnavailable, fact.ReasonCode
		}
	}
	for _, terminal := range binding.Terminals {
		if inputFactApplies(terminal.ImpactScope, terminal.SeriesIdentity, terminal.SourceTime, series, sourceTime) {
			return observability.AlgorithmInputResultUnavailable, terminal.ReasonCode
		}
	}
	for index := 0; index < binding.View.Len(); index++ {
		record, ok := binding.View.Record(index)
		if ok && execution.SeriesIdentityDigest(record.DimensionIdentityDigest()) == series && record.SourceTime() == sourceTime {
			return observability.AlgorithmInputResultAvailable, observability.ReasonNone
		}
	}
	return observability.AlgorithmInputResultMissing, observability.ReasonCode(contract.ReasonHistoryGapped)
}

func inputFactApplies(
	scope execution.ImpactScope,
	factSeries execution.SeriesIdentityDigest,
	factSourceTime int64,
	series execution.SeriesIdentityDigest,
	sourceTime int64,
) bool {
	if scope == execution.ImpactPlan || scope == execution.ImpactLevel {
		return true
	}
	return scope == execution.ImpactSeries && factSeries == series && factSourceTime == sourceTime
}

func evaluationRecordCount(inputs []execution.SeriesEvaluationInputRequest) int64 {
	for _, input := range inputs {
		for _, binding := range input.Inputs {
			if binding.Role == execution.InputRolePrimary && binding.View != nil {
				return int64(binding.View.Len())
			}
		}
	}
	return 0
}

func evaluationRetainedSize(state execution.StatePreflightResult, result execution.EvaluationResult) (uint64, error) {
	return retainedObjectBytes(state) + retainedObjectBytes(result), nil
}

// planCompletedFullEmpty reports whether every PRIMARY binding of the Plan is
// the FULL EMPTY share of an available completion and no binding of the Plan
// claims delivered records. It is fed the Plan's no-series bindings, so for a
// completion-only Plan a DATA binding means the completion itself claims
// delivered series that no PRIMARY series of the Plan consumed, which is not a
// valid no-series result. A PARTIAL or UNAVAILABLE ALGORITHM_DEPENDENCY binding
// does not change the judgement: without a PRIMARY series there is no record
// the dependency could have fed, exactly as the Python detect stage never
// reads history for a record it never received. Rejecting it would fail the
// Slot deterministically on every attempt although the PRIMARY completion is
// trustworthy and FULL EMPTY.
func planCompletedFullEmpty(bindings []execution.NamedInputBinding, plan execution.PlanIdentity) bool {
	found := false
	for _, binding := range bindings {
		if binding.Consumer.Plan != plan {
			continue
		}
		if binding.DataState == execution.DataStateData {
			return false
		}
		if binding.Role != execution.InputRolePrimary {
			continue
		}
		found = true
		if binding.Completeness != execution.CompletenessFull || binding.DataState != execution.DataStateEmpty ||
			binding.Disposition != execution.AccessAvailable {
			return false
		}
	}
	return found
}

// emptySourceSlotIsWholeInput says whether a Plan that produced no series is
// evidence that its input was whole this round. That is what a marker's
// warmup counts, and it is a narrower question than the one that decides the
// Plan completed: a round whose dependency query was UNAVAILABLE still
// completes FULL EMPTY, because with no PRIMARY record there was nothing for
// the dependency to feed and this round's conclusions do not rest on it -- but
// it is not evidence that the dependency is answering again, and the first
// round that does return series will need it. Counting such a round towards
// the warmup would lift a guard on the strength of rounds that never asked the
// question the guard is waiting on.
//
// So: FULL EMPTY as the completion judged it, and on top of that every binding
// of this Plan whole and available, dependencies included.
func emptySourceSlotIsWholeInput(bindings []execution.NamedInputBinding, plan execution.PlanIdentity) bool {
	return planCompletedFullEmpty(bindings, plan) && execution.PlanInputsWhole(bindings, plan)
}

// completionGapMutationFor builds the Plan gap mutation for a set of
// incomplete bindings.
func (stream *streamedExecution) completionGapMutationFor(
	due execution.DuePlan,
	bindings []execution.NamedInputBinding,
) (execution.PlanGapMutation, error) {
	reasons, err := completionGapReasons(due, bindings)
	if err != nil {
		return execution.PlanGapMutation{}, err
	}
	return stream.gapMutationForReasons(due, reasons)
}

// completionGapReasons decides the one reason each gap scope carries for a
// set of incomplete bindings: the incomplete inputs of the scope, folded by
// GapReasonFoldOrder.
//
// A marker carries one reason per scope, and the result contract reads it in
// two places -- a degraded Level outcome needs a marker of its scope with the
// outcome's own reason, and a PARTIAL input needs a marker of its scope with
// that input's reason. The fold is what makes those comparisons hold by
// construction: the marker's reason and the Level's UNKNOWN reason are the
// same function of the same inputs, so there is no second derivation for the
// first to disagree with.
func completionGapReasons(
	due execution.DuePlan,
	bindings []execution.NamedInputBinding,
) (map[execution.GapScope]execution.ReasonCode, error) {
	reasons := execution.RoundGapScopeReasons(bindings, due.Identity)
	if len(reasons) == 0 {
		return nil, errors.New("alarmd worker: incomplete named input requires a gap scope")
	}
	return reasons, nil
}

// gapMutationForReasons builds the Plan gap mutation that opens or
// strengthens the Plan's loaded marker with one scope per reason.
func (stream *streamedExecution) gapMutationForReasons(
	due execution.DuePlan,
	reasons map[execution.GapScope]execution.ReasonCode,
) (execution.PlanGapMutation, error) {
	identity := due.GapIdentity()
	marker, found := stream.gaps.Find(identity)
	if !found {
		return execution.PlanGapMutation{}, errors.New("alarmd worker: Plan gap marker is absent from validated result")
	}
	kind, err := ensureGappedKind(marker)
	if err != nil {
		return execution.PlanGapMutation{}, err
	}
	version, err := execution.BuildApplyVersion(stream.header.Contract, due.StateApplyEpoch)
	if err != nil {
		return execution.PlanGapMutation{}, err
	}
	required := requiredFullSlots(due.CompiledPlan)
	if required == 0 {
		return execution.PlanGapMutation{}, errors.New("alarmd worker: Plan gap requires positive FULL warmup slots")
	}
	scopes := make([]execution.GapScope, 0, len(reasons))
	for scope := range reasons {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].HasLevel != scopes[j].HasLevel {
			return !scopes[i].HasLevel
		}
		return scopes[i].LevelID < scopes[j].LevelID
	})
	mutations := make([]execution.GapScopeMutation, 0, len(scopes))
	for _, scope := range scopes {
		mutations = append(mutations, execution.GapScopeMutation{Scope: scope, Kind: kind,
			ReasonCode: reasons[scope], RequiredFullSlots: required})
	}
	mutation, err := execution.BuildPlanGapMutation(execution.PlanGapMutation{
		Identity: identity, ExpectedMarkerRevision: marker.MarkerRevision,
		ApplyVersion: version, ScheduleRevision: due.ScheduleRevision,
		Scopes: mutations,
	})
	if err != nil {
		return execution.PlanGapMutation{}, fmt.Errorf("alarmd worker: build completion Plan gap: %w", err)
	}
	return mutation, nil
}

func requiredFullSlots(plan *strategy.CompiledPlan) uint32 {
	return plan.RequiredFullSlots()
}

// physicalFailureReason picks the code an unavailable physical completion
// carries and where it came from; the walk and the fallback live in
// execution.AttributeUnavailable. The binding keeps both: the code for what
// reads bindings, the attribution for the completion, which names a
// fallback as the fallback.
func physicalFailureReason(facts execution.ProviderRouteFacts) (execution.ReasonCode, execution.UnavailableAttribution) {
	return execution.AttributeUnavailable(facts, execution.ReasonCode(contract.ReasonQueryUnavailable))
}

func provisionalResult(result execution.EvaluationResult) (observability.Result, execution.ReasonCode) {
	return result.Result, result.ReasonCode
}

type classifiedPhysicalCompletion struct {
	execution.PhysicalQueryCompletion
	sourceBackend bool
	// sentLate is a timeout not counted against the backend because the
	// query was sent late (timedOutSentLate).
	sentLate bool
}

// oneTimeClose is the open alert set a no-data close is asked against when it
// is the last word about its group: the round that decides it forgets the
// group, so no later round will decide it again. The consumer's copy may not
// vouch for the alert - a new owner after a restart or a handover answers from
// what it sent, which is nothing about an alert another process raised - and
// would hold the close, leaving the alert open for good. Here it is let
// through: a close the consumer holds no alert for is an orphan there and
// changes nothing, and there is about one per group that leaves, not one per
// round. It says it is a one-time close, so the gate counts these passes
// apart from the ones the consumer's copy vouched for.
type oneTimeClose struct{}

func (oneTimeClose) Contains(string, string, string) bool { return true }

func (oneTimeClose) OneTimeClose() bool { return true }

// queryPhaseTotals is a Slot's physical queries' wait (before the answer's
// first byte, the provider's) and body (after it, read and decoded), each
// summed over the queries and the longest one's.
type queryPhaseTotals struct {
	wait, body, waitMax, bodyMax uint64
	// bodyMicros and bodyMicrosMax are body and bodyMax in microseconds.
	bodyMicros, bodyMicrosMax uint64
	// frameConsumer and frameReader are the shared-schema answers' frame
	// waits, in microseconds.
	frameConsumer, frameReader uint64
}

func (totals *queryPhaseTotals) add(stats execution.ProviderStats) {
	totals.wait += stats.WaitMillis
	totals.body += stats.BodyMillis
	totals.waitMax = max(totals.waitMax, stats.WaitMillis)
	totals.bodyMax = max(totals.bodyMax, stats.BodyMillis)
	totals.bodyMicros += stats.BodyMicros
	totals.bodyMicrosMax = max(totals.bodyMicrosMax, stats.BodyMicros)
	totals.frameConsumer += stats.SharedFrameWaitConsumerMicros
	totals.frameReader += stats.SharedFrameWaitReaderMicros
}
