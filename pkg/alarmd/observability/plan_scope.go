// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import "log/slog"

// PlanScopeSample bounds the ids of each kind a line names of its Slot's due
// Plans.
const PlanScopeSample = 5

// PlanScopeFacts is whose a Slot is: the strategies and businesses its due
// Plans belong to, on its slot_completed and slot_skipped lines, so a Slot
// given up - or one completed short - is read against the strategies it
// held without going back to the Query Group's schedule. Each list is the
// first PlanScopeSample of the distinct ids, sorted, so every line of a Query
// Group names the same few; the counts are over every Plan, so a Query Group
// of hundreds of strategies reads as hundreds, not five. A strategy split
// into several Plans is one strategy. Each id is a value of its own (a JSON
// array), for a reader that takes them one by one.
type PlanScopeFacts struct {
	StrategyIDs   []string
	StrategyCount int
	BusinessIDs   []string
	BusinessCount int
}

func normalizePlanScope(facts *PlanScopeFacts) *PlanScopeFacts {
	if facts == nil {
		return nil
	}
	normalized := *facts
	// A count is never below the ids it was given, however many of them the
	// line keeps.
	normalized.StrategyCount = max(normalized.StrategyCount, len(normalized.StrategyIDs))
	normalized.BusinessCount = max(normalized.BusinessCount, len(normalized.BusinessIDs))
	if len(normalized.StrategyIDs) > PlanScopeSample {
		normalized.StrategyIDs = normalized.StrategyIDs[:PlanScopeSample]
	}
	if len(normalized.BusinessIDs) > PlanScopeSample {
		normalized.BusinessIDs = normalized.BusinessIDs[:PlanScopeSample]
	}
	return &normalized
}

func appendPlanScopeAttributes(attributes []slog.Attr, facts *PlanScopeFacts) []slog.Attr {
	if facts == nil {
		return attributes
	}
	return append(attributes,
		slog.Any("strategy_ids", facts.StrategyIDs),
		slog.Int("strategy_count", facts.StrategyCount),
		slog.Any("business_ids", facts.BusinessIDs),
		slog.Int("business_count", facts.BusinessCount),
	)
}
