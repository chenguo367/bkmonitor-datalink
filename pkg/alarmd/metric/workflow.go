package metric

import (
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/prometheus/client_golang/prometheus"
)

type workflowMetrics struct {
	ranges, expiredSlots   *prometheus.CounterVec
	run, execute, progress *prometheus.CounterVec
	progressCauses         *prometheus.CounterVec
	slotReasons            *prometheus.CounterVec
	active, ready, delayed prometheus.Gauge
	abandonedSlots         prometheus.Counter
	shareRefusalGaps       prometheus.Counter
	slotGaps               *prometheus.CounterVec
	permitWait             *prometheus.HistogramVec
	slotWaits              *prometheus.CounterVec
	slotWaitSeconds        *prometheus.CounterVec
}

// slotReasonWords are the words a Slot's end is counted under beside the
// contract catalogue's: none for a Slot that ended whole, reason_not_reported
// for an end that named no reason where one was due, and _other for a word
// outside the catalogue.
var slotReasonWords = []string{string(observability.ReasonNone), string(observability.ReasonNotReported), string(observability.ReasonOther)}

// slotReasonCells is every word a Slot's end can be counted under: the
// words above and every code of the contract catalogue, each once. All of
// them exist from the start, so a zero on any reason is a reading. The
// catalogue is closed, so this is a bound and not a growth: 129 cells today.
func slotReasonCells() []string {
	catalogue := contract.ReasonCatalogV2()
	cells := make([]string, 0, len(slotReasonWords)+len(catalogue))
	cells = append(cells, slotReasonWords...)
	for _, definition := range catalogue {
		cells = append(cells, definition.Code)
	}
	return cells
}

// slotReasonWord is the word a Slot's end is counted under.
func slotReasonWord(reason observability.ReasonCode, result observability.Result) string {
	reason = observability.NormalizeReason(reason, result)
	if reason == observability.ReasonNone || reason == observability.ReasonNotReported || contract.IsKnownReasonV2(string(reason)) {
		return string(reason)
	}
	return string(observability.ReasonOther)
}

func newWorkflowMetrics() workflowMetrics {
	counter := func(name, help, label string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: name, Help: help}, []string{label})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Subsystem: metricSubsystem, Name: name, Help: help})
	}
	metrics := workflowMetrics{
		ranges:       counter("expired_range_total", "Expired range operation returns, not logical Slot completions.", "result"),
		expiredSlots: counter("expired_slots_finalized_total", "Logical age-expired Slots finalized by a new successful range commit.", "reason"),
		run:          counter("run_one_return_total", "RunOne exits including panic, classified once by actual control flow.", "outcome"),
		execute:      counter("execute_return_total", "Executor returns; not successful Slot completions.", "outcome"),
		progress:     counter("progress_completed_total", "Acknowledged Progress commit observations by existing completion kind.", "kind"),
		progressCauses: prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem,
			Name: "progress_completion_causes_total",
			Help: "Acknowledged Slots that completed short of whole - COMPLETED_WITH_UNAVAILABLE, COMPLETED_WITH_PARTIAL_GAP - by " +
				"the cause the completion carried (the most actionable of what the round hit) and that cause's reason: " +
				"which Plan, Level or query it was and when are on the progress_committed line (completion_strategy, " +
				"completion_level, completion_query). NONE is a completion that carried no cause. origin says whose the " +
				"reason is: physical for what the round met itself - a primary input's reason always is - and carried " +
				"for a stored guard's reason a Level outcome repeated on a later round (LEVEL_OUTCOME_UNKNOWN, " +
				"GAP_GUARD_WARMING); the guard's origin Slot is on the progress_committed line " +
				"(evaluation_reason_origin_slot). reason=QUERY_TIMEOUT,origin=physical is a timeout this round. " +
				"A Slot is filed here by its cause, so one that ended PLAN_NOT_ACTIVE while its primary input was " +
				"unavailable is under PRIMARY_INPUT_UNAVAILABLE: how many Slots ended PLAN_NOT_ACTIVE is " +
				"slot_completed_reasons_total{reason=\"PLAN_NOT_ACTIVE\"}, and the two families are not summed."},
			[]string{"completion_kind", "cause", "reason", "origin"}),
		slotReasons: prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem,
			Name: "slot_completed_reasons_total",
			Help: "Slots that ended, by the reason word their slot_completed line carries: one per Slot, counted once, " +
				"when it ends - a round that retries it is not an end - whatever the log samples. none is a Slot that " +
				"ended whole. The answer to how many Slots ended PLAN_NOT_ACTIVE is the PLAN_NOT_ACTIVE cell, whatever " +
				"cause progress_completion_causes_total files them under. The none, reason_not_reported and _other " +
				"cells and one for every word of the contract catalogue exist from the start, so a zero on any of " +
				"them is a reading."},
			[]string{"reason"}),
		abandonedSlots: prometheus.NewCounter(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: "unfinished_slots_abandoned_total",
			Help: "Unfinished Slots the Progress store dropped because a later Segment moved the cursor past them, counted when the write recording the skip applied. Each is one slot_skipped line with finalizing_path=progress_store; its Slot was never queried and only the Progress record's recent gap holds it."}),
		shareRefusalGaps: prometheus.NewCounter(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: "share_refusal_gaps_total",
			Help: "Slots finalized without a query as RESOURCE_SHARE_EXCEEDED: refused for the share - twice the first time a Query Group meets it, once for each later Slot of the group - because their own retained bytes exceed the share one Query Group may hold of the replica's pool (QG_BUDGET_SHARE_EXCEEDED with no shared usage), and given up instead of re-read until the replay age. Each is one slot_skipped line with skip_cause=RESOURCE_SHARE_EXCEEDED naming the Query Group, its own bytes and the share; the strategy has to be sharded, and until it is every Slot of the group lands here."}),
		slotGaps: prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem, Name: "slot_gaps_total",
			Help: "Slots given up without a query, by the kind their completion committed under (GAP_SKIPPED or " +
				"SNAPSHOT_UNAVAILABLE) and its cause, counted in Slots once the commit giving them up applied: a range " +
				"by the Slots it covers, a Slot alone by one. Each commit is one slot_skipped line with its skip_count. " +
				"EXPIRED_REPLAY is a Query Group that fell behind; QUERY_COOLDOWN is the degraded pool's gap, Slots whose " +
				"queries did not run because the pool held them, given up at the end of the hold. The two are different " +
				"populations and are read apart, never added. progress_completion_causes_total counts commits rather " +
				"than Slots and does not carry these kinds. Every cell exists from the start; a cause outside the list " +
				"is other."},
			[]string{"kind", "cause"}),
		active:     gauge("scheduler_active_executions", "Worker task occupancy, including preparation and execution, excluding pending result delivery."),
		ready:      gauge("scheduler_ready_runners", "Runners in the outer ready queue."),
		delayed:    gauge("scheduler_delayed_runners", "Runners in the outer delayed queue, the recovery queue. Beside the rounds coming back from a deferral or a backoff it holds a first round within two seconds of the instant it is due at, which its timer starts there."),
		permitWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{Subsystem: metricSubsystem, Name: "query_permit_wait_seconds", Help: "Actual query admission call wall duration including immediate grants and failures; parallel waits are not additive Slot time.", Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 15, 30, 60}}, []string{"queue_kind"}),
		slotWaits: prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem,
			Name: "scheduler_execution_slot_waits_total",
			Help: "Slots a dispatch started, by operation, by whether the dispatch waited for an execution slot, and by " +
				"the cohort of the Query Group's shortest due interval (the dispatcher's queue cohorts, a one-minute " +
				"Query Group under 60s). The wait is the time the dispatcher held the Query Group ready to go - " +
				"queued and past its readiness - while every execution slot was in use, on the dispatcher's clock, " +
				"the time it slept waiting for a result included; waited=false is a dispatch that found a slot free. " +
				"Counted where the Slot starts, once a dispatch: a second Slot the same round runs did not wait, and " +
				"a Slot started without a dispatch (a supplement holding the flight) is not counted. A gauge cannot " +
				"see this: the slots fill and empty inside one sampling interval, so scheduler_active_executions " +
				"reads below the ceiling on every sample, as the ready queue's gauge did. Read the normal cells beside " +
				"slot_readiness_slack_seconds, every delay a normal Slot met after its data became readable: what " +
				"this leaves of it is the walk held back by a full ready queue (dispatch_queue_turnaways_total), the Query Group " +
				"still busy with its previous round (dispatch_crowded_out_total), the due index's bound floored to " +
				"the second, and the moments from a dispatch to its Slot's start. Every cell exists from the start."},
			[]string{"operation", "waited", "cohort"}),
		slotWaitSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{Subsystem: metricSubsystem,
			Name: "scheduler_execution_slot_wait_seconds_total",
			Help: "The seconds the dispatches in scheduler_execution_slot_waits_total{waited=\"true\"} waited, by " +
				"operation and cohort: the time they were ready to go while every execution slot was in use. Over " +
				"the waits it is the mean wait; it is time taken out of those Slots' windows, since a window runs " +
				"from the moment its data is readable."},
			[]string{"operation", "cohort"}),
	}
	// Every cell from the start, so a word that never happens reads zero.
	for _, kind := range slices.Concat(observability.QueryFreeCompletionKinds, []string{"other"}) {
		for _, cause := range slices.Concat(observability.QueryFreeCompletionCauses, []string{"other"}) {
			metrics.slotGaps.WithLabelValues(kind, cause)
		}
	}
	for _, word := range slotReasonCells() {
		metrics.slotReasons.WithLabelValues(word)
	}
	for _, operation := range observability.ExecutionSlotWaitOperations {
		for _, cohort := range dispatchTurnawayCohorts {
			metrics.slotWaits.WithLabelValues(string(operation), "true", cohort)
			metrics.slotWaits.WithLabelValues(string(operation), "false", cohort)
			metrics.slotWaitSeconds.WithLabelValues(string(operation), cohort)
		}
	}
	return metrics
}
func (m workflowMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.run, m.execute, m.progress, m.progressCauses, m.slotReasons, m.active, m.ready, m.delayed, m.permitWait, m.ranges, m.expiredSlots, m.abandonedSlots, m.shareRefusalGaps, m.slotGaps, m.slotWaits, m.slotWaitSeconds}
}
func (m workflowMetrics) observe(o observability.Observation) {
	switch {
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageExpiredRangeReturned:
		if f := o.ExpiredRange; f != nil {
			switch f.Result {
			case "committed", "retrying", "blocked", "error":
				m.ranges.WithLabelValues(f.Result).Inc()
				if f.Result == "committed" && f.CommittedSlots > 0 {
					switch f.ReasonCode {
					case "", contract.ReasonSnapshotUnavailable: // Original age-only producer had no ReasonCode.
						m.expiredSlots.WithLabelValues("recovery_expired").Add(float64(f.CommittedSlots))
					case contract.ReasonGapSkipped:
						m.expiredSlots.WithLabelValues("replay_distance_expired").Add(float64(f.CommittedSlots))
					}
				}
			}
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageSlotSkipped:
		// The line and the count are one event: the store reports the dropped
		// Slot once its write applied, and nowhere else.
		if f := o.SlotSkipped; f != nil && f.Path == observability.SlotSkippedProgressStore {
			m.abandonedSlots.Inc()
		}
		if f := o.SlotSkipped; f != nil && f.Cause == observability.CompletionCauseResourceShareExceeded {
			m.shareRefusalGaps.Add(float64(max(f.SlotCount, 1)))
		}
		// The Runner writes the line once the commit that gave the Slots up
		// applied, a range once with its count, a Slot alone with one. A
		// line counting none adds none: read as one, a writer that lost a
		// range's count would hide inside the reading.
		if f := o.SlotSkipped; f != nil && (f.Path == observability.SlotSkippedQueryFree || f.Path == observability.SlotSkippedExpiredRange) {
			kind, cause := f.Reason, f.Cause
			if !slices.Contains(observability.QueryFreeCompletionKinds, kind) {
				kind = "other"
			}
			if !slices.Contains(observability.QueryFreeCompletionCauses, cause) {
				cause = "other"
			}
			m.slotGaps.WithLabelValues(kind, cause).Add(float64(f.SlotCount))
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageSlotStarted:
		if slotWait := o.ExecutionSlotWait; slotWait != nil {
			cohort := slotWait.Cohort
			if !knownLabel(dispatchTurnawayCohorts, cohort) {
				cohort = "unknown"
			}
			operation := string(observability.NormalizeExecutionSlotWaitOperation(o.Operation))
			if !slotWait.Waited() {
				m.slotWaits.WithLabelValues(operation, "false", cohort).Inc()
				break
			}
			m.slotWaits.WithLabelValues(operation, "true", cohort).Inc()
			m.slotWaitSeconds.WithLabelValues(operation, cohort).Add(slotWait.WaitMillis / 1000)
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageRunnerReturned:
		if observability.ValidRunOutcome(o.RunOutcome) {
			m.run.WithLabelValues(o.RunOutcome).Inc()
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageSlotCompleted:
		// A round's outcome; a supplement completes no Slot and is counted by
		// the lookback (lookback_supplement_windows_total).
		if observability.ValidExecuteOutcome(o.ExecuteOutcome) && o.Operation != observability.OperationSupplement {
			m.execute.WithLabelValues(o.ExecuteOutcome).Inc()
			if o.ExecuteOutcome == "completed" {
				m.slotReasons.WithLabelValues(slotReasonWord(o.ReasonCode, o.Result)).Inc()
			}
		}
	case o.Component == observability.ComponentProgress && o.Stage == observability.StageProgressCommitted:
		if phaseTwoWorkCompleted(o) && observability.ValidProgressCompletionKind(o.ProgressCompletionKind) {
			m.progress.WithLabelValues(o.ProgressCompletionKind).Inc()
			switch o.ProgressCompletionKind {
			case "COMPLETED_WITH_UNAVAILABLE", "COMPLETED_WITH_PARTIAL_GAP":
				m.progressCauses.WithLabelValues(o.ProgressCompletionKind,
					observability.NormalizeProgressCompletionCause(o.ProgressCompletionCause),
					string(observability.NormalizeReason(observability.ReasonCode(o.ProgressCompletionReason), observability.ResultDegraded)),
					observability.ReasonOriginOf(observability.ReasonCode(o.ProgressCompletionReason), o.CarriedReason != nil)).Inc()
			}
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageDispatcherSnapshot:
		if f := o.Dispatcher; f != nil && f.Active >= 0 && f.Ready >= 0 && f.Delayed >= 0 {
			m.active.Set(float64(f.Active))
			if f.QueuesKnown {
				m.ready.Set(float64(f.Ready))
				m.delayed.Set(float64(f.Delayed))
			}
		}
	case o.Component == observability.ComponentScheduler && o.Stage == observability.StageQueryPermitWait:
		if o.PermitWait != nil && o.Duration >= 0 {
			kind := "normal"
			if o.PermitWait.Recovery {
				kind = "recovery"
			}
			m.permitWait.WithLabelValues(kind).Observe(o.Duration.Seconds())
		}
	}
}
