// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// segmentFallback is one fallback page a round's settle found: the Plan,
// the page, the word it was counted under and the times that decided it,
// in milliseconds but for the Plan's interval.
type segmentFallback struct {
	key                    segment.PlanKey
	page                   int
	result                 string
	sinceRenewal           int64
	renewalTTL, ttlNow     int64
	interval               int64
	lastMarkEvaluationTime int64
}

// segmentFallbackLines is when each Query Group's Plans last wrote their
// fallback line, and what they held back since: a Plan writes at most one in
// the shorter of its page's two lifetimes, the first at once.
type segmentFallbackLines struct {
	mu      sync.Mutex
	byGroup map[execution.QueryGroupIdentity]map[segment.PlanKey]*segmentFallbackLine
}

type segmentFallbackLine struct {
	at, window int64
	suppressed uint64
}

// take is whether the Plan writes its line for a fallback at now (in
// milliseconds), and the fallbacks it held back since its last one.
func (lines *segmentFallbackLines) take(queryGroup execution.QueryGroupIdentity, fallback segmentFallback, now int64) (uint64, bool) {
	lines.mu.Lock()
	defer lines.mu.Unlock()
	plans := lines.byGroup[queryGroup]
	line := plans[fallback.key]
	if line != nil && now-line.at < line.window {
		line.suppressed++
		return 0, false
	}
	var suppressed uint64
	if line != nil {
		suppressed = line.suppressed
	}
	if plans == nil {
		if lines.byGroup == nil {
			lines.byGroup = map[execution.QueryGroupIdentity]map[segment.PlanKey]*segmentFallbackLine{}
		}
		plans = map[segment.PlanKey]*segmentFallbackLine{}
		lines.byGroup[queryGroup] = plans
	}
	plans[fallback.key] = &segmentFallbackLine{at: now, window: min(fallback.renewalTTL, fallback.ttlNow)}
	return suppressed, true
}

// forget lets go of a Query Group this replica released: whoever owns it
// next writes each Plan's first line at once.
func (lines *segmentFallbackLines) forget(queryGroup execution.QueryGroupIdentity) {
	lines.mu.Lock()
	delete(lines.byGroup, queryGroup)
	lines.mu.Unlock()
}

// reportSegmentFallbacks writes the round's fallback pages' lines, each
// Plan's at most once in a lifetime (segmentFallbackLines), the fallbacks
// held back since counted on it.
func (coordinator *SlotExecutionCoordinator) reportSegmentFallbacks(ctx context.Context, request execution.SlotExecutionRequest,
	fallbacks []segmentFallback) {
	if len(fallbacks) == 0 {
		return
	}
	now := coordinator.now().UnixMilli()
	trace := observability.TraceFields{QueryGroupKey: string(request.Contract.Slot.QueryGroup),
		EvaluationTime: int64(request.Contract.Slot.EvaluationTime)}
	for _, fallback := range fallbacks {
		suppressed, write := coordinator.segmentFallbackLines.take(request.Contract.Slot.QueryGroup, fallback, now)
		if !write {
			continue
		}
		observability.ObserveSegmentFallback(ctx, coordinator.ports.Observer, trace, observability.Operation(request.Operation),
			observability.SegmentFallbackFacts{TenantID: fallback.key.Plan.TenantID, BusinessID: fallback.key.Plan.BusinessID,
				StrategyID: fallback.key.Plan.StrategyID, Shard: fallback.key.Shard.MatcherDigest, Generation: string(fallback.key.Generation),
				Page: fallback.page, Result: fallback.result, SinceRenewalMillis: fallback.sinceRenewal,
				RenewalTTLMillis: fallback.renewalTTL, TTLNowMillis: fallback.ttlNow, IntervalSeconds: fallback.interval,
				LastMarkEvaluationTime: fallback.lastMarkEvaluationTime, Suppressed: suppressed})
	}
}
