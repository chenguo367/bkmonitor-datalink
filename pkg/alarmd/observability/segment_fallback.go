// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import "context"

// StageSegmentRenewalFallback names a Plan's page found half its lifetime
// unrenewed (014 B3 3.5): its members written whole, counted
// state_segment_renewal_total{fallback_write} - renewals failing, or a
// Query Group stopped - or {fallback_unrenewable}, a page no earlier round
// could have renewed. The counts have no Plan; this line says which Plan,
// which page and by what times it was judged.
const StageSegmentRenewalFallback = "segment_renewal_fallback"

// ReasonSegmentRenewalFallback is a fallback_write line's reason: renewals
// did not keep the page alive. A fallback_unrenewable line is a success,
// by design, and names none.
const ReasonSegmentRenewalFallback ReasonCode = "SEGMENT_RENEWAL_FALLBACK"

// SegmentFallbackFacts is one Plan's fallback page: the Plan, its piece and
// generation, the page, the word it was counted under and the times that
// decided it.
//
// SinceRenewalMillis is the wall clock since the page's last renewal
// (now - At); RenewalTTLMillis the lifetime that renewal gave, TTLNowMillis
// this round's, IntervalSeconds the Plan's period. LastMarkEvaluationTime is
// the evaluation time of the round that last wrote the Plan's mark - the
// last round its segments committed - zero for a Plan with none.
// Suppressed is the Plan's fallback pages this worker held back since its
// last line: at most one line a Plan in a lifetime (the worker's).
type SegmentFallbackFacts struct {
	TenantID               string `json:"tenant_id"`
	BusinessID             string `json:"business_id"`
	StrategyID             string `json:"strategy_id"`
	Shard                  string `json:"shard,omitempty"`
	Generation             string `json:"generation"`
	Page                   int    `json:"page"`
	Result                 string `json:"result"`
	SinceRenewalMillis     int64  `json:"since_renewal_ms"`
	RenewalTTLMillis       int64  `json:"renewal_ttl_ms"`
	TTLNowMillis           int64  `json:"ttl_now_ms"`
	IntervalSeconds        int64  `json:"interval_s"`
	LastMarkEvaluationTime int64  `json:"last_mark_evaluation_time"`
	Suppressed             uint64 `json:"suppressed"`
}

// ObserveSegmentFallback writes one Plan's fallback line, outside every
// sample: a sampled line would name the first Plan it met and none after.
// fallback_write is degraded - renewals did not keep the page alive -
// fallback_unrenewable a success, a fallback by design.
func ObserveSegmentFallback(ctx context.Context, observer Observer, trace TraceFields, operation Operation, facts SegmentFallbackFacts) {
	if observer == nil {
		return
	}
	result, reason := Result(ResultSuccess), ReasonCode(ReasonNone)
	if facts.Result == SegmentRenewalFallbackWrite {
		result, reason = ResultDegraded, ReasonSegmentRenewalFallback
	}
	// Observability is a fail-open side channel, as at every other boundary.
	defer func() { _ = recover() }()
	observer.Observe(ctx, Observation{
		Component: ComponentState, Stage: StageSegmentRenewalFallback, Result: result, ReasonCode: reason, Operation: operation, Trace: trace,
		SegmentFallback: &facts,
	})
}
