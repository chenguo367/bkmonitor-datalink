// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// planViewFor is the one place that decides which levels a series is judged
// against, and it decides it by choosing which view of the Plan the rest of the
// evaluation sees.
//
// Everything downstream - this package, the detector, the trigger, and the
// execution contract's own validators - asks the Plan for its levels, in
// fifteen places across four packages. Threading a level set through all of
// them would have left the sixteenth, added later by someone with no reason to
// know the rule existed. Answering the existing question differently leaves
// nothing to thread.
//
// The choice is made from the kind of series, which is a property of how the
// input was built, never of which inputs turned up. A test fails if any
// non-test source in this package chooses a view anywhere else.
func planViewFor(due execution.DuePlan, kind execution.SeriesKind) (execution.DuePlan, error) {
	switch kind {
	case execution.SeriesKindReal:
		return due, nil
	case execution.SeriesKindNoData:
		view := due.CompiledPlan.NoDataView()
		if view == nil {
			// A synthetic series for a Plan that does not detect no-data. The
			// worker builds these from the Plan's own configuration, so this is
			// the two disagreeing, and guessing which is right would either
			// judge absence a strategy never asked for or feed an answer to a
			// level expecting a measurement.
			return execution.DuePlan{}, errors.New(
				"alarmd evaluation: no-data series for a Plan with no no-data level")
		}
		due.CompiledPlan = view
		return due, nil
	default:
		return execution.DuePlan{}, errors.New("alarmd evaluation: unknown series kind")
	}
}
