// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "slices"

// The reasons an InputSliceUnsupportedError names. The set is closed: a row
// that carries one is read by these words alone.
const (
	// InputSliceReasonPromQL is a query that is one PromQL expression: the
	// conditions would have to go into every selector, and whether a
	// dimension survives each selector's expression to the output is not
	// decided here (decision-020 batch 8, 7.3.1).
	InputSliceReasonPromQL = "promql"
	// InputSliceReasonStorageDialect is a query whose store does not read
	// the conditions' patterns as anchored RE2, or whose store cannot be
	// told from its facts.
	InputSliceReasonStorageDialect = "storage_dialect"
	// InputSliceReasonDimensionNotGrouped is a dimension sliced on that is
	// not a raw group-by label of every physical query of the Slot.
	InputSliceReasonDimensionNotGrouped = "dimension_not_grouped"
	// InputSliceReasonSupplement is a slice asked of a supplement, which
	// replays a kept read of the whole Query Group and sends no query the
	// conditions could go into.
	InputSliceReasonSupplement = "supplement"
)

// sliceableDataSources are the query-list data sources whose conditions the
// provider renders as Prometheus matchers for VictoriaMetrics: the unnamed
// source the metric compiler leaves (the provider reads it as bkmonitor) and
// bkmonitor named. bkdata goes to BkSql, which reads req as the Doris or
// MySQL dialect, and bklog and bkapm go to Elasticsearch, whose Lucene
// regexp has no anchors: both would put a series in no slice or in two.
var sliceableDataSources = map[string]bool{"": true, "bkmonitor": true}

// sliceableSourceSemantics are the platform sources a sliceable query may
// name: metric time series, which the provider reads from VictoriaMetrics. A
// query naming no source is the platform's own metric time series.
var sliceableSourceSemantics = map[string]bool{"bk_monitor/time_series": true, "custom/time_series": true}

// SliceableDimensions is the dimensions a Slot whose physical queries run
// with facts can be sliced on: the group-by labels every query clause of
// every one of them carries, as the store has them, in the order the first
// clause groups by. It is an InputSliceUnsupportedError when the Slot cannot
// be sliced at all: a PromQL query, a store outside the regex dialect the
// slices' partition rests on, or no dimension every query groups by.
//
// A dimension has to be in every query's group-by because the conditions
// filter before aggregating: grouped by it, filtering on it keeps whole
// groups and changes no aggregated value. It has to be the store's own label
// because the conditions are read by the store: a label renamed on the way
// out names nothing the store has.
func SliceableDimensions(facts ...QueryPlanFacts) ([]string, error) {
	if len(facts) == 0 {
		return nil, &InputSliceUnsupportedError{Reason: InputSliceReasonStorageDialect}
	}
	for _, fact := range facts {
		if fact.PromQL != nil {
			return nil, &InputSliceUnsupportedError{Reason: InputSliceReasonPromQL}
		}
	}
	for _, fact := range facts {
		if !sliceableDialect(fact) {
			return nil, &InputSliceUnsupportedError{Reason: InputSliceReasonStorageDialect}
		}
	}
	var candidates []string
	for _, dimension := range facts[0].QueryList[0].Dimensions {
		if dimension != "" && !slices.Contains(candidates, dimension) && groupedByEvery(dimension, facts) {
			candidates = append(candidates, dimension)
		}
	}
	if len(candidates) == 0 {
		return nil, &InputSliceUnsupportedError{Reason: InputSliceReasonDimensionNotGrouped}
	}
	return candidates, nil
}

// CheckInputSlice is whether slice can be read of a Slot whose physical
// queries run with facts: nil for the zero slice, which every Slot can be
// read in, and otherwise the InputSliceUnsupportedError SliceableDimensions
// names, or dimension_not_grouped for a condition on a dimension outside
// them.
func CheckInputSlice(slice InputSlice, facts ...QueryPlanFacts) error {
	if slice.IsZero() {
		return nil
	}
	if err := slice.Validate(); err != nil {
		return err
	}
	dimensions, err := SliceableDimensions(facts...)
	if err != nil {
		return err
	}
	for _, condition := range slice.Conditions {
		if !slices.Contains(dimensions, condition.Dimension) {
			return &InputSliceUnsupportedError{Reason: InputSliceReasonDimensionNotGrouped}
		}
	}
	return nil
}

// SliceableDimensions is SliceableDimensions of the query facts of the
// header's requirements, the primary requirements' first so the order is
// the Plan's own group-by. A requirement whose facts the header does not
// carry leaves the store undetermined, and a store that cannot be told is
// not one the slices can rest on.
func (header InternalExecutionHeader) SliceableDimensions() ([]string, error) {
	facts := make([]QueryPlanFacts, 0, len(header.Requirements))
	for _, primary := range []bool{true, false} {
		for _, requirement := range header.Requirements {
			if (requirement.Role == InputRolePrimary) != primary {
				continue
			}
			fact, found := header.QueryFacts[requirement.LogicalQueryRef]
			if !found {
				return nil, &InputSliceUnsupportedError{Reason: InputSliceReasonStorageDialect}
			}
			facts = append(facts, fact)
		}
	}
	return SliceableDimensions(facts...)
}

// sliceableDialect is whether every clause of facts is read by
// VictoriaMetrics through the provider's Prometheus matchers, as far as the
// facts tell: a structured UQ query with no Elasticsearch routing, of metric
// time series only, from a data source that routes there.
func sliceableDialect(facts QueryPlanFacts) bool {
	if facts.Provider != ProviderUQ || len(facts.TSDBMap) != 0 || len(facts.QueryList) == 0 {
		return false
	}
	for _, source := range facts.SourceSemantics {
		if !sliceableSourceSemantics[source] {
			return false
		}
	}
	for _, clause := range facts.QueryList {
		if !sliceableDataSources[clause.DataSource] {
			return false
		}
	}
	return true
}

// groupedByEvery is whether dimension is a raw group-by label of every
// clause of every facts: in the clause's group-by and in the group-by of
// every aggregating function that names one, removed by no function that
// aggregates without it, and neither renamed to nor from by the facts'
// normalization.
func groupedByEvery(dimension string, facts []QueryPlanFacts) bool {
	for _, fact := range facts {
		for from, to := range fact.Normalization.DimensionAliases {
			if from == dimension || to == dimension {
				return false
			}
		}
		for _, clause := range fact.QueryList {
			if !slices.Contains(clause.Dimensions, dimension) {
				return false
			}
			for _, function := range append([]QueryFunction{clause.TimeAggregation}, clause.Functions...) {
				if len(function.Dimensions) == 0 {
					continue
				}
				if slices.Contains(function.Dimensions, dimension) == function.Without {
					return false
				}
			}
		}
	}
	return true
}
