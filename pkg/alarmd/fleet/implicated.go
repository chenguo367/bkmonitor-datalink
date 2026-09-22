// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

// implicatedStrategy names the one Plan a row's evidence is about, when both
// pieces of evidence name it and nothing else: every held guard on the row
// belongs to it, and it is the one Plan the latest round bound no series to.
//
// A row is one object, and an object may run several Plans; the guards and
// the series counts on the row are per Plan. Folding such a row onto every
// strategy the object runs reads one Plan's evidence as every strategy's --
// on a verification cluster five strategies with hundreds of matched series
// each were told "data absent" because a sixth Plan on the same object had
// bound none.
//
// Both pieces are required because they answer different questions: a held
// guard says "this Plan's verdict is being held", a zero series count says
// "this Plan matched nothing". Only on the same Plan do the two make "this
// strategy is the cause"; either alone, or the two on different Plans, is an
// incomplete statement, and naming the wrong strategy sends its owner to
// look while not naming one costs a sentence. So the row stays every
// strategy's, as before, unless both agree: the fold does not guess.
//
// This is the one place that reads "which Plan is this row about"; aboutOf
// adds the conditions under which a row carries the answer at all, and the
// strategy fold, the check's strategy group and the card's object sentence
// all take it from there.
func implicatedStrategy(row Anomaly) (StrategyRef, bool) {
	var held StrategyRef
	guarded := false
	for _, guard := range row.Guards {
		if guarded && guard.Plan != held {
			return StrategyRef{}, false
		}
		held, guarded = guard.Plan, true
	}
	var unbound StrategyRef
	bare := false
	for _, plan := range row.PlanSeries {
		if plan.Matched != 0 {
			continue
		}
		if bare && plan.Plan != unbound {
			return StrategyRef{}, false
		}
		unbound, bare = plan.Plan, true
	}
	if !guarded || !bare || held != unbound {
		return StrategyRef{}, false
	}
	named := held
	// The named Plan has to be one the row lists, or the row's strategies
	// would lose the row to a Plan none of them is.
	for _, ref := range row.Strategies {
		if ref == named {
			return named, true
		}
	}
	return StrategyRef{}, false
}

// aboutOf is the one Plan a row's words are about, for the rows that carry
// one: a check whose evidence is per Plan, an object running more than one
// Plan, and evidence that names exactly one. This is the single derivation
// the standing (Standing.About) and the check's strategy group both read;
// neither restates its conditions, so the group on the first page and the
// name on the card cannot come apart.
func aboutOf(row Anomaly) (StrategyRef, bool) {
	if !planScopedCheck(row.Finding.Check) || len(row.Strategies) <= 1 {
		return StrategyRef{}, false
	}
	return implicatedStrategy(row)
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
