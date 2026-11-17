// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// The shadow of a Plan's stable segments (014 B3, first stage): every State
// is written as before, and beside it the worker keeps, in process, the
// member pages and mark a round would leave were the steady members not
// written. Each round it compares, for every open member it would not have
// written, the points the segment makes (segment.Made) and the Level state
// it reads as, against what the round wrote. A mismatch is a field the skip
// would have read otherwise than the write left it; the stage moves on only
// at none, over every period class.

// SegmentShadowOutcome is how one member present in a round compared,
// closed: match, every field the same; an exception, a member the skip
// would not have left, so written whole and taken off its page as the skip
// would, by the first of its reasons in this order - events, it sent events
// (the skip would drop them); unsteady, not decided steady and normal with
// its stored Levels and guard unchanged; level, a Level not FULL or with a
// gap reason; off_grid, its stored points not up to the mark before or the
// round's not on the grid after it; returned, a closed member back; and
// short_of_mark, its last point short of the round's mark, found at the
// fold - and, for a member the skip would have left, each field the made
// read and the write differ in, once a field.
type SegmentShadowOutcome string

const (
	SegmentShadowMatch                 SegmentShadowOutcome = "match"
	SegmentShadowExceptionEvents       SegmentShadowOutcome = "exception_events"
	SegmentShadowExceptionUnsteady     SegmentShadowOutcome = "exception_unsteady"
	SegmentShadowExceptionLevel        SegmentShadowOutcome = "exception_level"
	SegmentShadowExceptionOffGrid      SegmentShadowOutcome = "exception_off_grid"
	SegmentShadowExceptionReturned     SegmentShadowOutcome = "exception_returned"
	SegmentShadowExceptionShortOfMark  SegmentShadowOutcome = "exception_short_of_mark"
	SegmentShadowMismatchUnmade        SegmentShadowOutcome = "mismatch_unmade"
	SegmentShadowMismatchPointTimes    SegmentShadowOutcome = "mismatch_point_times"
	SegmentShadowMismatchRecordIDs     SegmentShadowOutcome = "mismatch_record_ids"
	SegmentShadowMismatchFacts         SegmentShadowOutcome = "mismatch_facts"
	SegmentShadowMismatchLevels        SegmentShadowOutcome = "mismatch_levels"
	SegmentShadowMismatchCompleteness  SegmentShadowOutcome = "mismatch_completeness"
	SegmentShadowMismatchGapReason     SegmentShadowOutcome = "mismatch_gap_reason"
	SegmentShadowMismatchCompatibility SegmentShadowOutcome = "mismatch_compatibility"
	SegmentShadowMismatchWarmupRef     SegmentShadowOutcome = "mismatch_warmup_ref"
	SegmentShadowMismatchLastProcessed SegmentShadowOutcome = "mismatch_last_processed"
	SegmentShadowMismatchLastReal      SegmentShadowOutcome = "mismatch_last_real"
	SegmentShadowMismatchGuard         SegmentShadowOutcome = "mismatch_guard"
	SegmentShadowOutcomeOther          SegmentShadowOutcome = "other"
)

// SegmentShadowException is whether an outcome is one of the exceptions.
func SegmentShadowException(outcome SegmentShadowOutcome) bool {
	switch outcome {
	case SegmentShadowExceptionEvents, SegmentShadowExceptionUnsteady, SegmentShadowExceptionLevel,
		SegmentShadowExceptionOffGrid, SegmentShadowExceptionReturned, SegmentShadowExceptionShortOfMark:
		return true
	}
	return false
}

// AllSegmentShadowOutcomes is every outcome, the cells the counter has from
// the start.
func AllSegmentShadowOutcomes() []SegmentShadowOutcome {
	return []SegmentShadowOutcome{SegmentShadowMatch, SegmentShadowExceptionEvents, SegmentShadowExceptionUnsteady,
		SegmentShadowExceptionLevel, SegmentShadowExceptionOffGrid, SegmentShadowExceptionReturned,
		SegmentShadowExceptionShortOfMark, SegmentShadowMismatchUnmade,
		SegmentShadowMismatchPointTimes, SegmentShadowMismatchRecordIDs, SegmentShadowMismatchFacts,
		SegmentShadowMismatchLevels, SegmentShadowMismatchCompleteness, SegmentShadowMismatchGapReason,
		SegmentShadowMismatchCompatibility, SegmentShadowMismatchWarmupRef, SegmentShadowMismatchLastProcessed,
		SegmentShadowMismatchLastReal, SegmentShadowMismatchGuard, SegmentShadowOutcomeOther}
}

// SegmentShadowEvent is what happened to a page's membership in a round,
// closed: joined, a series written and decided steady and normal at the
// mark joined; departed, an open member absent from the round was closed at
// the mark before; expired, a closed member past its window and its key's
// life left; reset, a Plan's shadow given up and started over, its members
// counted - a retry of a Slot some of whose series an attempt before had
// written leaves the round's membership unknown.
type SegmentShadowEvent string

const (
	SegmentShadowJoined     SegmentShadowEvent = "joined"
	SegmentShadowDeparted   SegmentShadowEvent = "departed"
	SegmentShadowExpired    SegmentShadowEvent = "expired"
	SegmentShadowReset      SegmentShadowEvent = "reset"
	SegmentShadowEventOther SegmentShadowEvent = "other"
)

// AllSegmentShadowEvents is every event, the cells the counter has from the
// start.
func AllSegmentShadowEvents() []SegmentShadowEvent {
	return []SegmentShadowEvent{SegmentShadowJoined, SegmentShadowDeparted, SegmentShadowExpired, SegmentShadowReset,
		SegmentShadowEventOther}
}

// Why a Slot ran without the shadow, closed: read_failed, its Plans'
// segments could not be read; book_refused, the retained pool did not take
// the books it read (their bytes reserved as the Slot's State is).
const (
	SegmentShadowSkippedReadFailed  = "read_failed"
	SegmentShadowSkippedBookRefused = "book_refused"
	SegmentShadowSkippedOther       = "other"
)

// AllSegmentShadowSkips is every reason, the cells the counter has from the
// start.
func AllSegmentShadowSkips() []string {
	return []string{SegmentShadowSkippedReadFailed, SegmentShadowSkippedBookRefused, SegmentShadowSkippedOther}
}

// The renewals of the pages due (014 B3 3.5), closed
// (state_segment_renewal_total): renewed and failed, pages whose renewal
// answered whole or did not - their time does not move on, and the page is
// renewed whole next round; missing, member keys a renewal found gone;
// over_budget, renewal scripts that ran longer than their 2.5 ms; and
// fallback_write, pages unrenewed for half their lifetime, whose members
// are written whole - in the second stage counted only, every State being
// written.
const (
	SegmentRenewalRenewed       = "renewed"
	SegmentRenewalFailed        = "failed"
	SegmentRenewalMissing       = "missing"
	SegmentRenewalOverBudget    = "over_budget"
	SegmentRenewalFallbackWrite = "fallback_write"
	SegmentRenewalOther         = "other"
)

// AllSegmentRenewalResults is every result, the cells the counter has from
// the start.
func AllSegmentRenewalResults() []string {
	return []string{SegmentRenewalRenewed, SegmentRenewalFailed, SegmentRenewalMissing, SegmentRenewalOverBudget,
		SegmentRenewalFallbackWrite, SegmentRenewalOther}
}

// Period classes, closed: the band a Plan's evaluation interval falls in, so
// a reading of the shadow says which kinds of Plan it covered.
const (
	SegmentPeriodSubMinute = "lt_1m"
	SegmentPeriodMinute    = "1m"
	SegmentPeriodFive      = "le_5m"
	SegmentPeriodHour      = "le_1h"
	SegmentPeriodDay       = "le_1d"
	SegmentPeriodLonger    = "gt_1d"
	SegmentPeriodOther     = "other"
)

// AllSegmentPeriodClasses is every period class.
func AllSegmentPeriodClasses() []string {
	return []string{SegmentPeriodSubMinute, SegmentPeriodMinute, SegmentPeriodFive, SegmentPeriodHour, SegmentPeriodDay,
		SegmentPeriodLonger, SegmentPeriodOther}
}

// SegmentPeriodClass is the period class of an evaluation interval in
// seconds; other for none.
func SegmentPeriodClass(intervalSeconds int64) string {
	switch {
	case intervalSeconds <= 0:
		return SegmentPeriodOther
	case intervalSeconds < 60:
		return SegmentPeriodSubMinute
	case intervalSeconds == 60:
		return SegmentPeriodMinute
	case intervalSeconds <= 300:
		return SegmentPeriodFive
	case intervalSeconds <= 3600:
		return SegmentPeriodHour
	case intervalSeconds <= 86400:
		return SegmentPeriodDay
	default:
		return SegmentPeriodLonger
	}
}

// SegmentShadowKey and SegmentShadowMemberKey are a cell of each counter.
type SegmentShadowKey struct {
	Outcome     SegmentShadowOutcome
	PeriodClass string
}

type SegmentShadowMemberKey struct {
	Event       SegmentShadowEvent
	PeriodClass string
}

// SegmentShadowFacts is one Slot's shadow: how its open members compared,
// what happened to its pages' membership, and the pages - and their bytes -
// it would have rewritten, by period class.
type SegmentShadowFacts struct {
	Outcomes   map[SegmentShadowKey]int64       `json:"outcomes,omitempty"`
	Members    map[SegmentShadowMemberKey]int64 `json:"members,omitempty"`
	PageWrites map[string]int64                 `json:"page_writes,omitempty"`
	PageBytes  map[string]int64                 `json:"page_bytes,omitempty"`
	// Skipped is the Slot run without the shadow, by why.
	Skipped map[string]int64 `json:"skipped,omitempty"`
	// Renewals is the round's page renewals, by result.
	Renewals map[string]int64 `json:"renewals,omitempty"`
}

// Renewal adds count to a renewal result's cell, its word normalised.
func (facts *SegmentShadowFacts) Renewal(result string, count int64) {
	if count == 0 {
		return
	}
	known := SegmentRenewalOther
	for _, word := range AllSegmentRenewalResults() {
		if result == word {
			known = result
		}
	}
	if facts.Renewals == nil {
		facts.Renewals = map[string]int64{}
	}
	facts.Renewals[known] += count
}

// Skip counts the Slot run without the shadow, its reason normalised.
func (facts *SegmentShadowFacts) Skip(reason string) {
	known := SegmentShadowSkippedOther
	for _, word := range AllSegmentShadowSkips() {
		if reason == word {
			known = reason
		}
	}
	if facts.Skipped == nil {
		facts.Skipped = map[string]int64{}
	}
	facts.Skipped[known]++
}

// Outcome adds count to an outcome's cell, every label normalised.
func (facts *SegmentShadowFacts) Outcome(outcome SegmentShadowOutcome, class string, count int64) {
	if count == 0 {
		return
	}
	if facts.Outcomes == nil {
		facts.Outcomes = map[SegmentShadowKey]int64{}
	}
	facts.Outcomes[SegmentShadowKey{Outcome: normalizeSegmentShadowOutcome(outcome), PeriodClass: NormalizeSegmentPeriodClass(class)}] += count
}

// Member adds count to an event's cell.
func (facts *SegmentShadowFacts) Member(event SegmentShadowEvent, class string, count int64) {
	if count == 0 {
		return
	}
	if facts.Members == nil {
		facts.Members = map[SegmentShadowMemberKey]int64{}
	}
	facts.Members[SegmentShadowMemberKey{Event: normalizeSegmentShadowEvent(event), PeriodClass: NormalizeSegmentPeriodClass(class)}] += count
}

// Pages adds the pages a round would have rewritten and their bytes.
func (facts *SegmentShadowFacts) Pages(class string, pages, bytes int64) {
	if pages == 0 {
		return
	}
	if facts.PageWrites == nil {
		facts.PageWrites, facts.PageBytes = map[string]int64{}, map[string]int64{}
	}
	class = NormalizeSegmentPeriodClass(class)
	facts.PageWrites[class] += pages
	facts.PageBytes[class] += bytes
}

// Empty is whether nothing was counted, so the facts are left off.
func (facts SegmentShadowFacts) Empty() bool {
	return len(facts.Outcomes) == 0 && len(facts.Members) == 0 && len(facts.PageWrites) == 0 && len(facts.Skipped) == 0 &&
		len(facts.Renewals) == 0
}

func normalizeSegmentShadowOutcome(outcome SegmentShadowOutcome) SegmentShadowOutcome {
	for _, known := range AllSegmentShadowOutcomes() {
		if outcome == known {
			return outcome
		}
	}
	return SegmentShadowOutcomeOther
}

func normalizeSegmentShadowEvent(event SegmentShadowEvent) SegmentShadowEvent {
	for _, known := range AllSegmentShadowEvents() {
		if event == known {
			return event
		}
	}
	return SegmentShadowEventOther
}

// NormalizeSegmentPeriodClass keeps a period class in the closed set.
func NormalizeSegmentPeriodClass(class string) string {
	for _, known := range AllSegmentPeriodClasses() {
		if class == known {
			return class
		}
	}
	return SegmentPeriodOther
}
