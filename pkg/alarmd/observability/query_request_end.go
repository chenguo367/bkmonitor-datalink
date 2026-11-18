// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// The end a query asks its provider for (access/uq requestEnd), closed
// (access_query_request_end_total): trimmed, asked a second before the
// window's end - the provider takes its range's end in, and the point at it
// is the next round's, let go here (outside_range_after); step_not_whole_seconds,
// not_time_align and source_not_eligible, asked as the window ends: a step
// whose grid points need not be whole seconds, a query not aligned to the
// provider's grid, and a clause not confirmed to have each point valued from
// its own look-back alone - a provider that buckets [t, t+step) and filters by
// the range's end would cut the window's last bucket short.
const (
	QueryRequestEndTrimmed             = "trimmed"
	QueryRequestEndStepNotWholeSeconds = "step_not_whole_seconds"
	QueryRequestEndNotTimeAlign        = "not_time_align"
	QueryRequestEndSourceNotEligible   = "source_not_eligible"
)

// QueryRequestEndOutcomes is every outcome, the cells the counter has from
// the start.
var QueryRequestEndOutcomes = []string{QueryRequestEndTrimmed, QueryRequestEndStepNotWholeSeconds, QueryRequestEndNotTimeAlign,
	QueryRequestEndSourceNotEligible}
