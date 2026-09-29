// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import "time"

// ImpactPart names one of the counts Impact is made of.
type ImpactPart string

const (
	ImpactAnomalies    ImpactPart = "anomalies"
	ImpactOurs         ImpactPart = "ours"
	ImpactDemoted      ImpactPart = "demoted"
	ImpactUndecidable  ImpactPart = "undecidable"
	ImpactByDesign     ImpactPart = "by_design"
	ImpactBlind        ImpactPart = "blind"
	ImpactAlarmd       ImpactPart = "alarmd"
	ImpactUndetermined ImpactPart = "undetermined"
	ImpactStrategy     ImpactPart = "strategy"
	ImpactData         ImpactPart = "data"
)

// ImpactParts is every part, in Impact's order.
var ImpactParts = []ImpactPart{ImpactAnomalies, ImpactOurs, ImpactDemoted, ImpactUndecidable, ImpactByDesign,
	ImpactBlind, ImpactAlarmd, ImpactUndetermined, ImpactStrategy, ImpactData}

// ImpactTally is what Impact is counted from, kept in the shape that adds
// across replicas: the tally of each replica's rows, merged, is the tally of
// all of them. Objects and rows add, because an object is one replica's;
// strategies are a set, because a strategy with objects on two replicas is
// one strategy -- the reason impactOf takes a union and not a sum.
//
// Partial is not kept: a part is a sample when its objects outnumber the
// rows it was counted from, which is decided on the merged counts.
type ImpactTally struct {
	Parts        map[ImpactPart]*ImpactPartTally
	NoStrategies int
}

// ImpactPartTally is one part: how many objects it has, how many rows it
// was counted from, and the strategies those rows named.
type ImpactPartTally struct {
	Objects    int
	Listed     int
	Strategies map[StrategyRef]struct{}
}

func newImpactTally() ImpactTally {
	tally := ImpactTally{Parts: make(map[ImpactPart]*ImpactPartTally, len(ImpactParts))}
	for _, part := range ImpactParts {
		tally.Parts[part] = &ImpactPartTally{Strategies: map[StrategyRef]struct{}{}}
	}
	return tally
}

// add counts rows into the part and says how many named no strategy.
func (part *ImpactPartTally) add(rows ...[]Anomaly) int {
	unnamed := 0
	for _, list := range rows {
		part.Listed += len(list)
		for _, row := range list {
			if len(row.Strategies) == 0 {
				unnamed++
				continue
			}
			for _, strategy := range row.Strategies {
				part.Strategies[strategy] = struct{}{}
			}
		}
	}
	return unnamed
}

// ImpactTallyOf tallies a view's rows; a view of one replica's snapshot
// gives that replica's part.
func ImpactTallyOf(view View, now time.Time) ImpactTally {
	tally := newImpactTally()
	for _, column := range []struct {
		part  ImpactPart
		total int
		rows  []Anomaly
	}{
		{ImpactAnomalies, view.AnomaliesTotal, view.Anomalies},
		{ImpactDemoted, view.DemotedTotal, view.Demoted},
		{ImpactUndecidable, view.UndecidableTotal, view.Undecidable},
		{ImpactByDesign, view.ByDesignTotal, view.ByDesign},
	} {
		part := tally.Parts[column.part]
		part.Objects = column.total
		tally.NoStrategies += part.add(column.rows)
	}
	blind := tally.Parts[ImpactBlind]
	blind.Objects = view.AnomaliesTotal + view.DemotedTotal
	blind.add(view.Anomalies, view.Demoted)

	ours := make([]Anomaly, 0, len(view.Anomalies))
	for _, anomaly := range view.Anomalies {
		if anomaly.Attribution == AttributionOurs {
			ours = append(ours, anomaly)
		}
	}
	tally.Parts[ImpactOurs].Objects = len(ours)
	tally.Parts[ImpactOurs].add(ours)

	for owner, list := range impactByOwner(view, now) {
		part := tally.Parts[ownerPart[owner]]
		if part == nil {
			continue
		}
		part.Objects = len(distinctObjects(list))
		part.add(list)
	}
	return tally
}

// ownerPart is the part each owner's objects are counted in.
var ownerPart = map[Owner]ImpactPart{
	OwnerAlarmd: ImpactAlarmd, OwnerUndetermined: ImpactUndetermined, OwnerStrategy: ImpactStrategy, OwnerData: ImpactData,
}

// impactByOwner is every column's rows by who acts, from each object's
// line, and the objects losing rounds now from the records.
func impactByOwner(view View, now time.Time) map[Owner][]Anomaly {
	byOwner := map[Owner][]Anomaly{}
	for _, column := range [][]Anomaly{view.Anomalies, view.Demoted, view.Undecidable, view.ByDesign, view.NoData} {
		for _, anomaly := range column {
			if anomaly.Finding.Check == "" {
				continue
			}
			owner := checkAnswers[anomaly.Finding.Check].Owner
			byOwner[owner] = append(byOwner[owner], anomaly)
		}
	}
	rows, _ := skippedRows(&view, map[string]struct{}{}, now)
	for _, row := range rows {
		if row.Loss == LossOngoing || row.Loss == LossAfterRestart {
			byOwner[OwnerAlarmd] = append(byOwner[OwnerAlarmd], row)
		}
	}
	return byOwner
}

// MergeImpactTallies adds replicas' tallies into one.
func MergeImpactTallies(tallies ...ImpactTally) ImpactTally {
	merged := newImpactTally()
	for _, tally := range tallies {
		merged.NoStrategies += tally.NoStrategies
		for name, part := range tally.Parts {
			into := merged.Parts[name]
			if into == nil {
				continue
			}
			into.Objects += part.Objects
			into.Listed += part.Listed
			for strategy := range part.Strategies {
				into.Strategies[strategy] = struct{}{}
			}
		}
	}
	return merged
}

// Impact is the tally as the page reads it.
func (tally ImpactTally) Impact() Impact {
	column := func(part ImpactPart) ColumnImpact {
		counted := tally.Parts[part]
		businesses := map[string]struct{}{}
		for strategy := range counted.Strategies {
			if strategy.BusinessID != "" {
				businesses[strategy.BusinessID] = struct{}{}
			}
		}
		return ColumnImpact{Objects: counted.Objects, Strategies: len(counted.Strategies), Businesses: len(businesses),
			Partial: counted.Objects > counted.Listed}
	}
	impact := Impact{
		Anomalies: column(ImpactAnomalies), Demoted: column(ImpactDemoted), Undecidable: column(ImpactUndecidable),
		ByDesign: column(ImpactByDesign), Blind: column(ImpactBlind), Ours: column(ImpactOurs),
		NoStrategies: tally.NoStrategies,
	}
	// Ours is counted off the published list, so it is a lower bound whenever
	// that list was cut -- the same limit as the column it sits in.
	impact.Ours.Partial = impact.Anomalies.Partial
	// Every owner part is a lower bound when any column was cut: a line draws
	// from all four.
	partial := impact.Anomalies.Partial || impact.Demoted.Partial || impact.Undecidable.Partial || impact.ByDesign.Partial
	owner := func(part ImpactPart) ColumnImpact {
		counted := column(part)
		counted.Partial = partial
		return counted
	}
	impact.Alarmd, impact.Undetermined = owner(ImpactAlarmd), owner(ImpactUndetermined)
	impact.Strategy, impact.Data = owner(ImpactStrategy), owner(ImpactData)
	return impact
}
