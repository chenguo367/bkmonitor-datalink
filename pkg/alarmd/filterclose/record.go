// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package filterclose

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openepisodes"
)

// A strategy-wide close (absent, inactive, a scope close) is checked
// against alarmd's own record of the episodes it emitted for the strategy:
// the entries the decision covers are the expectation. Without a record
// the filter is not run as a close at all: each candidate alert the
// search pages is decided from its own labels and dimensions, and the
// ones decided are closed by their alert ids under the decision's filter.

// recordKey is the scope's record.
func recordKey(scope Scope) openepisodes.Key {
	return openepisodes.Key{TenantID: scope.TenantID, StrategyID: scope.StrategyID}
}

// readRecord is the strategy's record, or the word that ends the job:
// OutcomeRecordMissing (counted as missing or truncated) where it is to be
// decided alert by alert, OutcomeRecordUnavailable where nothing is.
func (closer *Closer) readRecord(ctx context.Context, scope Scope) (openepisodes.Record, string) {
	record, err := closer.record.Load(ctx, recordKey(scope))
	switch {
	case err != nil:
		closer.count(OutcomeRecordUnavailable, 1)
		return openepisodes.Record{}, OutcomeRecordUnavailable
	case record.Truncated:
		closer.count(OutcomeRecordTruncated, 1)
		return openepisodes.Record{}, OutcomeRecordMissing
	case !record.Exists:
		closer.count(OutcomeRecordMissing, 1)
		return openepisodes.Record{}, OutcomeRecordMissing
	}
	return record, ""
}

// covered is how many entries the decision covers, an unreadable entry
// counted in, and the fingerprints match selects - the ones a close that
// found nothing prunes.
func covered(record openepisodes.Record, match func(openepisodes.Entry) bool) (int, []string) {
	n := 0
	var fingerprints []string
	for _, entry := range record.Entries {
		switch {
		case match(entry):
			n++
			fingerprints = append(fingerprints, entry.Fingerprint)
		case entry.Unreadable:
			n++
		}
	}
	return n, fingerprints
}

// recorded runs job against the record's entries match selects, prunes
// them when the link holds none of the decision's alerts, and decides
// alert by alert with decide where the strategy has no record.
func (closer *Closer) recorded(ctx context.Context, job Job, match func(openepisodes.Entry) bool,
	decide func(linkdmanage.Alert) bool) string {
	var prune []string
	job.expect = func(ctx context.Context) (Expectation, string) {
		record, word := closer.readRecord(ctx, job.Filter.scope)
		if word != "" {
			return Expectation{}, word
		}
		n, fingerprints := covered(record, match)
		prune = fingerprints
		return Expectation{Count: n, Source: ExpectOwnRecord}, ""
	}
	word := closer.Run(ctx, job)
	switch word {
	case OutcomeRecordMissing:
		return closer.perAlert(ctx, job, decide)
	case OutcomeNothingMatched:
		closer.prune(ctx, job.Filter.scope, prune)
	}
	return word
}

// prune removes the entries of a decision the link holds nothing of.
func (closer *Closer) prune(ctx context.Context, scope Scope, fingerprints []string) {
	if len(fingerprints) == 0 {
		return
	}
	if err := closer.record.Prune(ctx, recordKey(scope), fingerprints); err != nil {
		closer.count(OutcomeRecordPruneFailed, 1)
		return
	}
	closer.count(OutcomeRecordPruned, len(fingerprints))
}

// perAlert is a decision's close without a record: the decision's active
// alerts paged with the search, up to maxAlertsPerTurn a round from where
// the term's last round stopped, each decided from its own record, the
// ones decided closed by their ids under the decision's filter (so an
// alert that left the decision meanwhile is not closed). It returns the
// first word of a close that did not settle (anything but job_done and
// nothing_matched), else OutcomeJobIncomplete while the walk has pages
// left, OutcomeJobDone when a close ran, or OutcomeNothingMatched when
// nothing was decided.
func (closer *Closer) perAlert(ctx context.Context, job Job, decide func(linkdmanage.Alert) bool) string {
	read, err := activeOnly(job.Filter)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	walkKey := "alerts:" + filterKey(job.Filter)
	closer.mu.Lock()
	after := closer.buckets[walkKey]
	closer.mu.Unlock()
	var ids []string
	n := 0
	for n < maxAlertsPerTurn {
		page, err := closer.link.Search(ctx, job.Filter.scope.TenantID, read.body, after, min(linkdmanage.MaxLimit, maxAlertsPerTurn-n))
		if err != nil {
			return closer.failed(err)
		}
		for _, alert := range page.Items {
			n++
			if alert.Status == "active" && decide(alert) {
				ids = append(ids, alert.AlertID)
			}
		}
		after = page.Next
		if page.Next == "" || len(page.Items) == 0 {
			after = ""
			break
		}
	}
	closer.count(OutcomeAlertsRead, n)
	if after != "" {
		closer.count(OutcomeAlertsTruncated, 1)
	}
	closer.mu.Lock()
	closer.buckets[walkKey] = after
	closer.mu.Unlock()
	closer.count(OutcomeAlertDecidedClose, len(ids))
	word := OutcomeNothingMatched
	settled := func(word string) bool { return word == OutcomeNothingMatched || word == OutcomeJobDone }
	recheck := job.Recheck
	if recheck.body == nil {
		recheck = job.Filter
	}
	for start := 0; start < len(ids); start += linkdmanage.MaxLimit {
		batch := ids[start:min(len(ids), start+linkdmanage.MaxLimit)]
		byID, err := narrowed(job.Filter, terms(fieldAlertID, sorted(batch)))
		if err != nil {
			closer.count(OutcomeFilterRefused, 1)
			if settled(word) {
				word = OutcomeFilterRefused
			}
			continue
		}
		batchWord := closer.Run(ctx, Job{Filter: byID, Decision: job.Decision, Version: job.Version, Digest: job.Digest, Revision: job.Revision,
			Expect: Expectation{Count: len(batch), Source: ExpectDecidedIDs}, Reason: job.Reason, Recheck: recheck})
		if batchWord == OutcomeUnsupported {
			return batchWord
		}
		switch {
		case !settled(batchWord) && settled(word), word == OutcomeNothingMatched:
			word = batchWord
		}
	}
	if after != "" && (word == OutcomeJobDone || word == OutcomeNothingMatched) {
		return OutcomeJobIncomplete
	}
	return word
}

// labelIs decides an alert by one label's value.
func labelIs(name, value string) func(linkdmanage.Alert) bool {
	return func(alert linkdmanage.Alert) bool {
		got, ok := alert.Label(name)
		return ok && got == value
	}
}

func everyEntry(openepisodes.Entry) bool { return true }

func versionIs(version string) func(openepisodes.Entry) bool {
	return func(entry openepisodes.Entry) bool { return entry.Version == version }
}
