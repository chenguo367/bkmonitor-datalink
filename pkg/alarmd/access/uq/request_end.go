// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// requestEnd is the end, in seconds, a query asks the provider for, and why.
//
// The provider evaluates a range query at its start's grid points up to and
// including its end (unify-query v3.9.4910 and v3.9.4892: QueryTs.ToTime
// leaves the end as asked, and the range query runs from the aligned start to
// it); a window ends before its end (AcceptedRange, half-open). The point at
// the end came back only to be let go here (outside_range_after) - and it is
// the next round's first: every one-step round read each point twice.
//
// It is asked a second earlier. Every grid point is a whole second - the
// step is, and so is the start it is aligned from - so a second before the
// end leaves out the point at the end and no other: the window's last point,
// at or before end - step, is asked for as before, its value from its own
// look-back. Not for a step that is not whole seconds, whose grid need not
// be; not for a query not aligned (not_time_align), held back until the first
// readings; and not for a source not confirmed to value each point from its
// own look-back alone (eligibleForTrim).
func requestEnd(spec execution.PhysicalQuerySpec) (int64, string) {
	end := spec.ProviderRange.End
	if !eligibleForTrim(spec.PlanFacts) {
		return end, observability.QueryRequestEndSourceNotEligible
	}
	if spec.PlanFacts.NotTimeAlign {
		return end, observability.QueryRequestEndNotTimeAlign
	}
	// A provider range is never empty (QueryWindow.Validate: its start before
	// its end), so a second earlier never passes its start.
	if step := spec.PlanFacts.RequestStepMillis(); step <= 0 || step%1000 != 0 {
		return end, observability.QueryRequestEndStepNotWholeSeconds
	}
	return end - 1, observability.QueryRequestEndTrimmed
}

// requestEndOf is the end requestEnd asks for.
func requestEndOf(spec execution.PhysicalQuerySpec) int64 {
	end, _ := requestEnd(spec)
	return end
}

// eligibleForTrim is whether a query's end may be asked a second earlier
// (execution.RequestEndTrimEligible): decided in execution, where an item
// detected over several inputs is held to one answer for all of them.
func eligibleForTrim(facts execution.QueryPlanFacts) bool {
	return execution.RequestEndTrimEligible(facts)
}
