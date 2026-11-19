// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import "time"

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

// Why a Slot did not follow its segment skip word, closed
// (state_segment_skip_flag_ignored_total): epoch_mismatch, the word names a
// Leader other than the one whose view the worker has installed - one set
// before a Leader change, or left by a Leader a rollback replaced, which
// never takes it back; no_view, the worker has installed no view to read
// the Leader from.
const (
	SegmentSkipIgnoredEpochMismatch = "epoch_mismatch"
	SegmentSkipIgnoredNoView        = "no_view"
)

// AllSegmentSkipIgnoredReasons is every reason, the cells the counter has
// from the start.
func AllSegmentSkipIgnoredReasons() []string {
	return []string{SegmentSkipIgnoredEpochMismatch, SegmentSkipIgnoredNoView}
}

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
// written; fallback_unrenewable, the same of a page no earlier round could
// have renewed - unrenewed for less than its Plan's interval past the quarter
// lifetime it falls due by - by design, not for renewals failing: a page
// whose lifetime is no more than two intervals (a Plan keeping one point, or
// one its horizon cuts short), or a new page, made staggered earlier by up to
// a quarter lifetime, of a Plan whose keys live under four intervals;
// fallback_stalled, the same of a Plan whose rounds stopped: the Slot's
// evaluation time past the one its mark was written in by more than its
// interval (a round not settled between - a Plan not active for a while),
// or the round a replay or a retry, late for the time it was due. fallback_write is the rest, renewals that did not
// keep a page of a Plan settling every round alive, and the third stage's
// bar reads it alone.
const (
	SegmentRenewalRenewed             = "renewed"
	SegmentRenewalFailed              = "failed"
	SegmentRenewalMissing             = "missing"
	SegmentRenewalOverBudget          = "over_budget"
	SegmentRenewalFallbackWrite       = "fallback_write"
	SegmentRenewalFallbackUnrenewable = "fallback_unrenewable"
	SegmentRenewalFallbackStalled     = "fallback_stalled"
	SegmentRenewalOther               = "other"
)

// AllSegmentRenewalResults is every result, the cells the counter has from
// the start.
func AllSegmentRenewalResults() []string {
	return []string{SegmentRenewalRenewed, SegmentRenewalFailed, SegmentRenewalMissing, SegmentRenewalOverBudget,
		SegmentRenewalFallbackWrite, SegmentRenewalFallbackUnrenewable, SegmentRenewalFallbackStalled, SegmentRenewalOther}
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

// Degradations, closed: a Plan whose stable segments the Slot could not use
// as written (014 B3 §9.7), read as stored and written whole. unread: its
// segments, or the generation's it carries from, did not read in a retry of
// a Slot dispatched with a segment skip word, the first attempt having
// failed for it (SEGMENT_READ_FAILED); book_refused: the pool did not take
// the books in such a retry; undecodable: its stored segments did not
// decode, its book started over; write_absent: its round's write was not
// made. Each is the permanent class, a Plan once a Slot; stopped is a Plan
// reaching it a second time in its owner's term, its members no longer left
// unwritten on this replica until the owner changes.
const (
	SegmentDegradedUnread      = "unread"
	SegmentDegradedBookRefused = "book_refused"
	SegmentDegradedUndecodable = "undecodable"
	SegmentDegradedWriteAbsent = "write_absent"
	SegmentDegradedStopped     = "stopped"
)

// AllSegmentDegradations is every degradation, the cells the counter has
// from the start.
func AllSegmentDegradations() []string {
	return []string{SegmentDegradedUnread, SegmentDegradedBookRefused, SegmentDegradedUndecodable, SegmentDegradedWriteAbsent,
		SegmentDegradedStopped}
}

// Shadow rounds, closed: whether a Plan's round was its piece's sampled
// round of the day (014 B3 §9.2), every member written whole and compared -
// sampled - or any other, normal.
const (
	SegmentRoundNormal  = "normal"
	SegmentRoundSampled = "sampled"
)

// AllSegmentShadowRounds is every round.
func AllSegmentShadowRounds() []string {
	return []string{SegmentRoundNormal, SegmentRoundSampled}
}

// SegmentShadowKey and SegmentShadowMemberKey are a cell of each counter.
type SegmentShadowKey struct {
	Outcome     SegmentShadowOutcome
	PeriodClass string
	Round       string
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
	// RenewScripts is how long each renewal script that renewed its keys ran,
	// by the server's clock as the script measured it; counted on the
	// renewal script histogram, not on the line.
	RenewScripts []time.Duration `json:"-"`
	// SkipFlagIgnored is the Slots dispatched with a segment skip word that
	// did not follow it, by why (AllSegmentSkipIgnoredReasons).
	SkipFlagIgnored map[string]int64 `json:"skip_flag_ignored,omitempty"`
	// Samples is the Plans' sampled rounds of the day the Slot ran, by
	// period class.
	Samples map[string]int64 `json:"samples,omitempty"`
	// Degraded is the Plans whose segments the Slot could not use as
	// written, by why (AllSegmentDegradations).
	Degraded map[string]int64 `json:"degraded,omitempty"`
}

// Degrade counts a Plan degraded. A reason outside the closed list is not
// one this build gives and is not counted under a cell that does not exist.
func (facts *SegmentShadowFacts) Degrade(reason string) {
	for _, word := range AllSegmentDegradations() {
		if reason == word {
			if facts.Degraded == nil {
				facts.Degraded = map[string]int64{}
			}
			facts.Degraded[reason]++
			return
		}
	}
}

// Sample counts a Plan's round that was its piece's sampled round.
func (facts *SegmentShadowFacts) Sample(class string) {
	if facts.Samples == nil {
		facts.Samples = map[string]int64{}
	}
	facts.Samples[NormalizeSegmentPeriodClass(class)]++
}

// IgnoreSkipFlag counts a Slot that did not follow its segment skip word.
// A reason outside the closed list is not one this build gives and is not
// counted under a cell that does not exist.
func (facts *SegmentShadowFacts) IgnoreSkipFlag(reason string) {
	for _, word := range AllSegmentSkipIgnoredReasons() {
		if reason == word {
			if facts.SkipFlagIgnored == nil {
				facts.SkipFlagIgnored = map[string]int64{}
			}
			facts.SkipFlagIgnored[reason]++
			return
		}
	}
}

// RenewScript adds one renewal script's own run time.
func (facts *SegmentShadowFacts) RenewScript(elapsed time.Duration) {
	facts.RenewScripts = append(facts.RenewScripts, elapsed)
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

// Outcome adds count to an outcome's cell in a round, every label
// normalised: a round not sampled is normal.
func (facts *SegmentShadowFacts) Outcome(outcome SegmentShadowOutcome, class, round string, count int64) {
	if count == 0 {
		return
	}
	if facts.Outcomes == nil {
		facts.Outcomes = map[SegmentShadowKey]int64{}
	}
	if round != SegmentRoundSampled {
		round = SegmentRoundNormal
	}
	facts.Outcomes[SegmentShadowKey{Outcome: normalizeSegmentShadowOutcome(outcome), PeriodClass: NormalizeSegmentPeriodClass(class),
		Round: round}] += count
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
		len(facts.Renewals) == 0 && len(facts.SkipFlagIgnored) == 0 && len(facts.Samples) == 0 && len(facts.Degraded) == 0 &&
		len(facts.RenewScripts) == 0
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
