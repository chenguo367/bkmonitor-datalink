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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/editclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
)

// The decision words a job's operation id is made of, beside the edit
// decisions' own (editclose).
const (
	DecisionAbsent      = "absent"
	DecisionUnsupported = "unsupported"
	DecisionInactive    = "inactive"
	DecisionScope       = "target_removed"
)

// Absent closes every alert of a strategy no longer published (decision
// absent) or refused for a capability (unsupported). The expectation is the
// last count the leader read of the strategy's open alerts.
func (closer *Closer) Absent(ctx context.Context, scope Scope, decision string, revision int64, expect Expectation) string {
	filter, err := StrategyFilter(scope)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	return closer.Run(ctx, Job{Filter: filter, Decision: decision, Revision: revision, Expect: expect,
		Reason: fmt.Sprintf("alarmd: strategy %s %s", scope.StrategyID, decision)})
}

// Fingerprints closes the strategy's active alerts of the given
// fingerprints under one decision (a close reason and its detail): the
// series a caller decided one by one. The expectation is the fingerprints
// themselves - one active alert each at most.
func (closer *Closer) Fingerprints(ctx context.Context, scope Scope, fingerprints []string, decision string, revision int64) string {
	filter, err := FingerprintFilter(scope, fingerprints)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	strategy, err := StrategyFilter(scope)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	return closer.Run(ctx, Job{Filter: filter, Decision: decision, Revision: revision, Recheck: strategy,
		Expect: Expectation{Count: len(fingerprints), Source: ExpectDecidedIDs},
		Reason: fmt.Sprintf("alarmd: strategy %s %s", scope.StrategyID, decision)})
}

// Inactive closes the strategy's alerts outside its effective time; version
// non-empty closes only the alerts opened at it, where the decision is the
// version's alone.
func (closer *Closer) Inactive(ctx context.Context, scope Scope, version string, revision int64, expect Expectation) string {
	filter, err := StrategyFilter(scope)
	if version != "" {
		filter, err = VersionFilter(scope, version)
	}
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	return closer.Run(ctx, Job{Filter: filter, Decision: DecisionInactive, Version: version, Revision: revision, Expect: expect,
		Reason: fmt.Sprintf("alarmd: strategy %s outside its effective time", scope.StrategyID)})
}

// RemovedTargets closes the alerts of targets removed from the strategy's
// scope, batch by batch; each batch's expectation is its targets times
// seriesPerTarget. It returns the words of the batches in order.
func (closer *Closer) RemovedTargets(ctx context.Context, scope Scope, targets []Target, seriesPerTarget int, revision int64) []string {
	filters, err := TargetFilters(scope, targets)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return []string{OutcomeFilterRefused}
	}
	strategy, err := StrategyFilter(scope)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return []string{OutcomeFilterRefused}
	}
	words := make([]string, 0, len(filters))
	done := 0
	for _, filter := range filters {
		var batch struct {
			Bool struct {
				Filter []struct {
					Bool *struct {
						Should []json.RawMessage `json:"should"`
					} `json:"bool"`
				} `json:"filter"`
			} `json:"bool"`
		}
		_ = json.Unmarshal(filter.body, &batch)
		n := 0
		for _, clause := range batch.Bool.Filter {
			if clause.Bool != nil {
				n += len(clause.Bool.Should)
			}
		}
		done += n
		words = append(words, closer.Run(ctx, Job{Filter: filter, Decision: DecisionScope, Revision: revision, Recheck: strategy,
			Expect: Expectation{Count: n * max(seriesPerTarget, 1), Source: ExpectTargets},
			Reason: fmt.Sprintf("alarmd: target removed from strategy %s", scope.StrategyID)}))
	}
	return words
}

// The bounds of one strategy's edit close in a round: the buckets it
// enumerates (one search each; the test environment's widest strategy had
// six versions open), and the alerts it reads one by one.
const (
	maxBuckets       = 64
	maxAlertsPerTurn = 2000
)

// EditInput is one strategy's edit close: its current Plans and the
// expectation for any one of its buckets, the strategy's own count.
type EditInput struct {
	Scope  Scope
	Plan   editclose.Plan
	Expect Expectation
}

// Edit closes the alerts an edit of their strategy orphaned. Each bucket of
// alerts opened at an older version is found with one search (limit 1,
// the buckets seen excluded), judged from that one alert's facts with
// editclose.DecideGroup - every alert of the bucket carries the same facts,
// the digest says so - and closed by the bucket's filter. Alerts opened
// without a digest, and a bucket only each alert's dimensions can judge,
// are read and decided one by one with editclose.Decide and closed by
// their ids under the bucket's filter. The threshold alerts of the older
// versions at a level the current Plans no longer detect are closed by
// one more filter (level_removed). It returns the first word that ended
// the strategy's turn early - OutcomeLinkUnsupported is the caller's cue
// to take its existing path - or empty.
func (closer *Closer) Edit(ctx context.Context, input EditInput) string {
	plan := input.Plan
	if plan.Revision <= 0 {
		closer.count("edit_"+editclose.OutcomeRevisionUnknown, 1)
		return ""
	}
	current := strconv.FormatInt(plan.Revision, 10)
	buckets, samples, word := closer.enumerate(ctx, input.Scope, current)
	if word != "" {
		return word
	}
	var older []string
	seenOlder := map[string]bool{}
	for i, bucket := range buckets {
		if ctx.Err() != nil {
			return ""
		}
		revision, err := strconv.ParseInt(bucket.Version, 10, 64)
		if err == nil && revision > 0 && revision < plan.Revision && !seenOlder[bucket.Version] {
			seenOlder[bucket.Version] = true
			older = append(older, bucket.Version)
		}
		if bucket.Digest == "" {
			if word := closer.oneByOne(ctx, input, bucket); word == OutcomeLinkUnsupported {
				return word
			}
			continue
		}
		facts, ok := sampleFacts(samples[i])
		if !ok {
			closer.count(OutcomeSampleUnreadable, 1)
			continue
		}
		if facts.Digest() != bucket.Digest {
			// Facts this build does not read whole, or a label that is not
			// the digest of the facts beside it: the sample does not speak
			// for the bucket.
			closer.count(OutcomeDigestMismatch, 1)
			continue
		}
		family := "metric_algorithm"
		if bucket.NoData {
			family = "no_data"
		}
		decision, note := editclose.DecideGroup(plan, editclose.Group{Revision: revision, Family: family, Facts: facts})
		closer.count("edit_"+decision, 1)
		closer.count("edit_"+note, 1)
		switch {
		case decision == editclose.OutcomeGroupDimensionsUndecided:
			if word := closer.oneByOne(ctx, input, bucket); word == OutcomeLinkUnsupported {
				return word
			}
		case editclose.Closes(decision):
			filter, err := BucketFilter(input.Scope, bucket)
			if err != nil {
				closer.count(OutcomeFilterRefused, 1)
				continue
			}
			if word := closer.Run(ctx, Job{Filter: filter, Decision: decision, Version: bucket.Version, Digest: bucket.Digest,
				Revision: plan.Revision, Expect: input.Expect, Reason: editReason(input.Scope, decision, bucket.Version)}); word == OutcomeLinkUnsupported {
				return word
			}
		}
	}
	severities, word := editclose.RemovedSeverities(plan)
	closer.count("edit_"+word, 1)
	if len(older) == 0 || len(severities) == 0 {
		return ""
	}
	filter, err := LevelFilter(input.Scope, older, severities)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return ""
	}
	sort.Strings(older)
	versions := ""
	for _, version := range older {
		versions += version + ","
	}
	if word := closer.Run(ctx, Job{Filter: filter, Decision: editclose.OutcomeLevelRemoved, Version: versions, Revision: plan.Revision,
		Expect: input.Expect, Reason: editReason(input.Scope, editclose.OutcomeLevelRemoved, "")}); word == OutcomeLinkUnsupported {
		return word
	}
	return ""
}

func editReason(scope Scope, decision, version string) string {
	if version == "" {
		return fmt.Sprintf("alarmd: strategy %s changed (%s)", scope.StrategyID, decision)
	}
	return fmt.Sprintf("alarmd: strategy %s changed (%s) since version %s", scope.StrategyID, decision, version)
}

// enumerate is the strategy's buckets other than the current version's,
// each with the one alert that found it.
func (closer *Closer) enumerate(ctx context.Context, scope Scope, current string) ([]Bucket, []linkdmanage.Alert, string) {
	var buckets []Bucket
	var samples []linkdmanage.Alert
	for len(buckets) < maxBuckets {
		filter, err := enumerationFilter(scope, current, buckets)
		if err != nil {
			closer.count(OutcomeFilterRefused, 1)
			return nil, nil, OutcomeFilterRefused
		}
		page, err := closer.link.Search(ctx, scope.TenantID, filter.body, "", 1)
		if err != nil {
			return nil, nil, closer.failed(err)
		}
		if len(page.Items) == 0 {
			return buckets, samples, ""
		}
		alert := page.Items[0]
		bucket, ok := bucketOf(alert)
		if !ok {
			// A version the filter required and the alert does not carry as
			// a decimal: no bucket can exclude it, so the walk stops here.
			closer.count(OutcomeSampleUnreadable, 1)
			return buckets, samples, ""
		}
		buckets = append(buckets, bucket)
		samples = append(samples, alert)
	}
	closer.count(OutcomeBucketsTruncated, 1)
	return buckets, samples, ""
}

func bucketOf(alert linkdmanage.Alert) (Bucket, bool) {
	version, ok := alert.Label("strategy_version")
	if !ok || !decimal(version) {
		return Bucket{}, false
	}
	digest, _ := alert.Label("strategy_facts_digest")
	if digest != "" && !hexDigest(digest) {
		return Bucket{}, false
	}
	_, noData := alert.Dimensions[contract.NoDataDimensionTag]
	return Bucket{Version: version, Digest: digest, NoData: noData}, true
}

type extraData struct {
	EvaluationFamily string          `json:"evaluation_family"`
	StrategyFacts    json.RawMessage `json:"strategy_facts"`
}

func sampleFacts(alert linkdmanage.Alert) (contract.StrategyFactsV2, bool) {
	var extra extraData
	var facts contract.StrategyFactsV2
	if json.Unmarshal(alert.ExtraData, &extra) != nil || len(extra.StrategyFacts) == 0 || json.Unmarshal(extra.StrategyFacts, &facts) != nil {
		return contract.StrategyFactsV2{}, false
	}
	return facts, true
}

// oneByOne reads a bucket's alerts, up to maxAlertsPerTurn a round from
// where the last round stopped, decides each from its own record, and
// closes the ones decided by their ids, under the bucket's filter.
func (closer *Closer) oneByOne(ctx context.Context, input EditInput, bucket Bucket) string {
	filter, err := BucketFilter(input.Scope, bucket)
	if err != nil {
		closer.count(OutcomeFilterRefused, 1)
		return OutcomeFilterRefused
	}
	walkKey := filterKey(filter)
	closer.mu.Lock()
	after := closer.buckets[walkKey]
	closer.mu.Unlock()
	decided := map[string][]string{}
	read := 0
	for read < maxAlertsPerTurn {
		page, err := closer.link.Search(ctx, input.Scope.TenantID, filter.body, after, min(linkdmanage.MaxLimit, maxAlertsPerTurn-read))
		if err != nil {
			return closer.failed(err)
		}
		for _, alert := range page.Items {
			read++
			if alert.Status != "active" {
				continue
			}
			decision, note := editclose.Decide(input.Plan, recordOf(alert))
			closer.count("edit_"+decision, 1)
			closer.count("edit_"+note, 1)
			if editclose.Closes(decision) {
				decided[decision] = append(decided[decision], alert.AlertID)
			}
		}
		after = page.Next
		if page.Next == "" || len(page.Items) == 0 {
			after = ""
			break
		}
	}
	closer.count(OutcomeAlertsRead, read)
	if after != "" {
		closer.count(OutcomeAlertsTruncated, 1)
	}
	closer.mu.Lock()
	closer.buckets[walkKey] = after
	closer.mu.Unlock()
	decisions := make([]string, 0, len(decided))
	for decision := range decided {
		decisions = append(decisions, decision)
	}
	sort.Strings(decisions)
	for _, decision := range decisions {
		ids := decided[decision]
		closer.count(OutcomeAlertDecidedClose, len(ids))
		for start := 0; start < len(ids); start += linkdmanage.MaxLimit {
			batch := ids[start:min(len(ids), start+linkdmanage.MaxLimit)]
			byID, err := alertsFilter(input.Scope, bucket, batch)
			if err != nil {
				closer.count(OutcomeFilterRefused, 1)
				continue
			}
			word := closer.Run(ctx, Job{Filter: byID, Decision: decision, Version: bucket.Version, Digest: bucket.Digest,
				Revision: input.Plan.Revision, Expect: Expectation{Count: len(batch), Source: ExpectDecidedIDs},
				Reason: editReason(input.Scope, decision, bucket.Version), Recheck: filter})
			if word == OutcomeLinkUnsupported {
				return word
			}
		}
	}
	return ""
}

// recordOf is what Decide reads of an alert: the record the link returned.
func recordOf(alert linkdmanage.Alert) editclose.Alert {
	record := editclose.Alert{Severity: alert.Severity}
	if version, ok := alert.Label("strategy_version"); ok {
		record.Revision, _ = strconv.ParseInt(version, 10, 64)
	}
	if alert.Dimensions != nil {
		record.DimensionsRead = true
		for name := range alert.Dimensions {
			record.DimensionKeys = append(record.DimensionKeys, name)
		}
		sort.Strings(record.DimensionKeys)
	}
	var extra extraData
	if json.Unmarshal(alert.ExtraData, &extra) == nil {
		record.EvaluationFamily = extra.EvaluationFamily
		if len(extra.StrategyFacts) > 0 && string(extra.StrategyFacts) != "null" {
			record.Facts = extra.StrategyFacts
		}
	}
	return record
}
