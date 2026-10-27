// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

func newLinkdFilterCloseCollector() *outcomeCounterCollector {
	return newOutcomeCounterCollector("linkd_filter_close_total",
		"The strategy closes alarmd runs through the alert link's search and close by filter (package filterclose), by "+
			"outcome. Per job (one decision's filter in one round): job_done, job_incomplete (the round ended or the "+
			"dry run's count was spent), nothing_matched, expect_exceeded (the dry run matched more than the independent "+
			"expectation allows; nothing closed), link_unsupported (501/404/405: the existing path is taken), link_busy "+
			"(429), link_refused (another 4xx), link_failed (timeout, 5xx, transport), command_unavailable, "+
			"filter_refused; and where the job's expectation came from: expect_own_records, expect_roster, "+
			"expect_removed_targets, expect_decided_ids, expect_fixed_ceiling. Per item: item_closed, "+
			"item_already_closed, item_ended_otherwise (409 invalid_transition), item_source_unpublished, "+
			"item_conflict_unknown, item_rederived (429, 504 not_attempted, 409 version_conflict: found again by the "+
			"filter), item_pending (5xx: kept to retry by id), item_pending_full, item_pending_lost, item_refused. Per "+
			"retry of a kept item: pending_closed, pending_already_closed, pending_ended_otherwise, "+
			"pending_left_decision (the decision's filter no longer names the alert: dropped, not closed), "+
			"pending_not_found, pending_retry_failed, pending_given_up, pending_refused, pending_unreadable, "+
			"pending_unavailable. The edit close's own: buckets_truncated, digest_mismatch, sample_unreadable, "+
			"alerts_read, alerts_truncated, alert_decided_close, and edit_<word> for each edit decision and note. "+
			"Every cell exists from the start so a zero is a reading and not an absence.")
}

// SetLinkdFilterCloseSource binds the filter close's counts. Safe before or
// after registration; a nil recorder is a no-op.
func (r *Recorder) SetLinkdFilterCloseSource(words []string, outcomes func() map[string]uint64) {
	if r == nil || r.phaseTwo.linkdFilterClose == nil {
		return
	}
	r.phaseTwo.linkdFilterClose.set(words, outcomes)
}
