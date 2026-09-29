// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"encoding/json"
	"errors"
	"regexp"
	"time"
	"unicode/utf8"
)

// PublisherReport is what the strategy cache's publisher says about its own
// most recent run, read from <prefix>.publisher in the same MGET as the
// active set. It answers the one thing the platform side cannot see: why the
// publisher published the way it did this run -- published, or blocked and
// for what reason. It is the publisher's word and is carried as that;
// nothing in alarmd decides on it. What the publisher is running is read
// from the platform side, independently of whether it says so here.
//
// A publisher that leaves no record is not an error and not an empty
// report: State says PublisherReportNotProvided.
type PublisherReport struct {
	State string
	// Detail names why a record was unreadable, in PublisherReportDetail
	// words. Empty otherwise.
	Detail string
	// ReadAt is when this process read the record, by its own clock.
	ReadAt time.Time
	// The record's own fields, bounded. WrittenAt is zero when the record
	// does not say. The counts are absent where the record wrote null or
	// left them out: the publisher writes null for what it could not tell.
	Writer     string
	Version    string
	Outcome    string
	Reason     string
	WrittenAt  time.Time
	Strategies *int64
	Rejected   *int64
	Generation *int64
	Backfill   *PublisherBackfill
}

// PublisherReportSource is a StrategySource that reads its publisher's
// record beside the active set. Only readers of the report use it.
type PublisherReportSource interface {
	PublisherReport() (PublisherReport, bool)
	PublisherReportCounts() []PublisherReportCount
}

// PublisherBackfill is the publisher's copy of the last backfill of its
// records, when it has one.
type PublisherBackfill struct {
	Source       string
	StrategySets *int64
	Written      *int64
	Failed       *int64
	Start        *int64
	LegacyMax    *int64
	Summary      string
	UpdatedAt    time.Time
}

// PublisherReport states, closed.
const (
	PublisherReportProvided    = "provided"
	PublisherReportNotProvided = "not_provided"
	PublisherReportUnreadable  = "unreadable"
)

// PublisherReport details of an unreadable record, closed.
const (
	// PublisherReportTooLarge: the record is larger than
	// PublisherReportMaxBytes; it is not decoded.
	PublisherReportTooLarge = "too_large"
	// PublisherReportNotJSON: the record is not a JSON object.
	PublisherReportNotJSON = "not_json"
	// PublisherReportFieldType: a field this reader knows holds a value of
	// another type.
	PublisherReportFieldType = "field_type"
	// PublisherReportSchemaVersion: the record's schema_version is missing
	// or not one this reader knows.
	PublisherReportSchemaVersion = "schema_version"
)

// PublisherReportSchemaVersion1 is the one record shape this reader decodes.
const PublisherReportSchemaVersion1 = 1

// PublisherReportMaxBytes bounds the record this reader decodes. The record
// the publisher writes is a few hundred bytes to about a kilobyte; the bound
// is what a record that grew without bound would be cut at, not a budget.
const PublisherReportMaxBytes = 16 << 10

// Bounds on the text the report carries on. The record comes from another
// system; these are what a reader needs to act, not all of what could be
// there.
const (
	publisherWordLimit    = 64
	publisherReasonLimit  = 128
	publisherSummaryLimit = 1024
)

// decodePublisherReport reads one record. A nil value is a publisher that
// left none; anything else is either decoded or named unreadable, never
// dropped.
func decodePublisherReport(value interface{}, readAt time.Time) PublisherReport {
	report := PublisherReport{ReadAt: readAt}
	payload, ok := legacyRedisBytes(value)
	if value == nil || (ok && len(payload) == 0) {
		report.State = PublisherReportNotProvided
		return report
	}
	report.State = PublisherReportUnreadable
	switch {
	case !ok:
		report.Detail = PublisherReportFieldType
		return report
	case len(payload) > PublisherReportMaxBytes:
		report.Detail = PublisherReportTooLarge
		return report
	}
	var dto struct {
		SchemaVersion *int64  `json:"schema_version"`
		Writer        *string `json:"writer"`
		Version       *string `json:"version"`
		WrittenAt     *int64  `json:"written_at"`
		Outcome       *string `json:"outcome"`
		Reason        *string `json:"reason"`
		Strategies    *int64  `json:"strategies"`
		Rejected      *int64  `json:"rejected"`
		Generation    *int64  `json:"generation"`
		Backfill      *struct {
			Source       *string `json:"source"`
			StrategySets *int64  `json:"strategy_sets"`
			Written      *int64  `json:"written"`
			Failed       *int64  `json:"failed"`
			Start        *int64  `json:"start"`
			LegacyMax    *int64  `json:"legacy_max"`
			Summary      *string `json:"summary"`
			UpdatedAt    *int64  `json:"updated_at"`
		} `json:"backfill"`
	}
	if err := json.Unmarshal(payload, &dto); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			report.Detail = PublisherReportFieldType
		} else {
			report.Detail = PublisherReportNotJSON
		}
		return report
	}
	if dto.SchemaVersion == nil || *dto.SchemaVersion != PublisherReportSchemaVersion1 {
		report.Detail = PublisherReportSchemaVersion
		return report
	}
	report.State, report.Detail = PublisherReportProvided, ""
	report.Writer = boundedText(dto.Writer, publisherWordLimit)
	report.Version = boundedText(dto.Version, publisherWordLimit)
	report.Outcome = boundedText(dto.Outcome, publisherWordLimit)
	report.Reason = boundedText(dto.Reason, publisherReasonLimit)
	report.WrittenAt = unixSeconds(dto.WrittenAt)
	report.Strategies, report.Rejected, report.Generation = dto.Strategies, dto.Rejected, dto.Generation
	if backfill := dto.Backfill; backfill != nil {
		report.Backfill = &PublisherBackfill{
			Source: boundedText(backfill.Source, publisherWordLimit), StrategySets: backfill.StrategySets,
			Written: backfill.Written, Failed: backfill.Failed, Start: backfill.Start, LegacyMax: backfill.LegacyMax,
			Summary: boundedText(backfill.Summary, publisherSummaryLimit), UpdatedAt: unixSeconds(backfill.UpdatedAt),
		}
	}
	return report
}

// boundedText cuts text at limit bytes on a rune boundary and says so with a
// trailing "...".
func boundedText(text *string, limit int) string {
	if text == nil {
		return ""
	}
	value := *text
	if len(value) <= limit {
		return value
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + "..."
}

func unixSeconds(seconds *int64) time.Time {
	if seconds == nil || *seconds <= 0 {
		return time.Time{}
	}
	return time.Unix(*seconds, 0)
}

// PublisherOutcomeLabels is the closed set a publisher outcome is counted
// under: the three the record defines, and other for anything else.
var PublisherOutcomeLabels = []string{"published", "blocked", "failed", "other"}

// PublisherReasonLabelLimit bounds how many distinct reasons one process
// counts under their own name; past it they are counted as other.
const PublisherReasonLabelLimit = 16

// publisherReasonWord is the shape of a reason the publisher states as a
// word of its own vocabulary (SPLIT_RECORDS_NOT_BACKFILLED). A failed run's
// reason is an exception's class name, an open set, and is not a label.
var publisherReasonWord = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

// PublisherReportCount is how many distinct records this process has read
// under one outcome and reason.
type PublisherReportCount struct {
	Outcome string
	Reason  string
	Count   uint64
}

// publisherCounter counts distinct records: one record is read on every
// active set read until the publisher writes the next, and is counted once.
type publisherCounter struct {
	last   string
	counts map[[2]string]uint64
}

func (counter *publisherCounter) note(report PublisherReport) {
	if report.State != PublisherReportProvided {
		return
	}
	identity, _ := json.Marshal([]interface{}{report.WrittenAt.Unix(), report.Outcome, report.Reason, report.Generation,
		report.Strategies, report.Rejected})
	if string(identity) == counter.last {
		return
	}
	counter.last = string(identity)
	if counter.counts == nil {
		counter.counts = map[[2]string]uint64{}
	}
	outcome := "other"
	for _, known := range PublisherOutcomeLabels[:3] {
		if report.Outcome == known {
			outcome = known
		}
	}
	reason := ""
	switch {
	case report.Reason == "":
	case publisherReasonWord.MatchString(report.Reason) && counter.admits(outcome, report.Reason):
		reason = report.Reason
	default:
		reason = "other"
	}
	counter.counts[[2]string{outcome, reason}]++
}

// admits says whether a reason may be counted under its own name: one
// already counted, or a new one while the process has counted fewer than
// PublisherReasonLabelLimit.
func (counter *publisherCounter) admits(outcome, reason string) bool {
	if _, seen := counter.counts[[2]string{outcome, reason}]; seen {
		return true
	}
	named := 0
	for key := range counter.counts {
		if key[1] != "" && key[1] != "other" {
			named++
		}
	}
	return named < PublisherReasonLabelLimit
}

func (counter *publisherCounter) snapshot() []PublisherReportCount {
	counts := make([]PublisherReportCount, 0, len(counter.counts))
	for key, count := range counter.counts {
		counts = append(counts, PublisherReportCount{Outcome: key[0], Reason: key[1], Count: count})
	}
	return counts
}
