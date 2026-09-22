// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

// implicatedStrategy names the one Plan a row's evidence is about, when the
// evidence names exactly one: every held guard on the row belongs to it, and
// every Plan the latest round bound no series to is it.
//
// A row is one object, and an object may run several Plans; the guards and
// the series counts on the row are per Plan. Folding such a row onto every
// strategy the object runs reads one Plan's evidence as every strategy's --
// on a verification cluster five strategies with hundreds of matched series
// each were told "data absent" because a sixth Plan on the same object had
// bound none. When the evidence names no Plan, or names more than one, the
// row stays every strategy's, as before: the fold does not guess.
//
// This is the one place that reads "which Plan is this row about"; the
// strategy fold, the check's strategy group and the card's object sentence
// all take their answer from here, so the three cannot drift apart.
func implicatedStrategy(row Anomaly) (StrategyRef, bool) {
	var named StrategyRef
	found := false
	for _, guard := range row.Guards {
		if found && guard.Plan != named {
			return StrategyRef{}, false
		}
		named, found = guard.Plan, true
	}
	for _, plan := range row.PlanSeries {
		if plan.Matched != 0 {
			continue
		}
		if found && plan.Plan != named {
			return StrategyRef{}, false
		}
		named, found = plan.Plan, true
	}
	if !found {
		return StrategyRef{}, false
	}
	// The named Plan has to be one the row lists, or the row's strategies
	// would lose the row to a Plan none of them is.
	for _, ref := range row.Strategies {
		if ref == named {
			return named, true
		}
	}
	return StrategyRef{}, false
}

// strategiesOf is the strategies a listed row folds onto: the one Plan its
// standing says the words are about, when it says one; every strategy the
// row lists otherwise. It reads the standing rather than the evidence again
// so that the fold and the words cannot disagree about which Plan a row is
// about. A check about the object's whole round (a dependency down, a
// refused query, a defect) never sets About, because every Plan on the
// object lost that round.
func strategiesOf(row Anomaly) []StrategyRef {
	if row.Standing != nil && row.Standing.About != nil {
		return []StrategyRef{*row.Standing.About}
	}
	return row.Strategies
}

// planScopedCheck says whether a check's evidence is per Plan: the two that
// standingOf reads the guards and the series counts for.
func planScopedCheck(check Check) bool {
	return check == CheckWindowUndecided || check == CheckSeriesDataMissing
}
