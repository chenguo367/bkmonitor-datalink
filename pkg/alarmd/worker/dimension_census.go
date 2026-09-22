// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// CensusCandidateSharePercent is how much of this worker's retained-byte pool
// one Query Group's per-Slot peak has to reach for its Plans to be worth
// taking a census of.
//
// It is the split trigger of decision-020 section 4.7.4 read on the worker:
// that trigger is "a Query Group whose peak passes the pool's per-object
// share", and the per-object share is half the pool (decision-019). Both
// numbers are the worker's own - its pool, and the peak the retained-peak
// census already keeps per Query Group - so the gate needs no control-plane
// field, no publication path, and nobody has to tell a worker that it is a
// candidate.
//
// The cost of being local is that it reads only this replica's two windows.
// That is the same reading the Leader plans placement from (section 5.7), so
// the two cannot disagree about which objects are heavy; what it cannot see
// is an object that is heavy only on another replica, which is an object
// that replica is taking the census of.
const CensusCandidateSharePercent = 50

// censusCandidate says whether this Slot's Query Group is heavy enough to be
// worth a census, and returns the share it was judged against so the line can
// be read afterwards.
//
// A share of zero is not a share every Query Group clears, it is no reading:
// a worker with no pool figure has not been told what its pool is, and one
// whose pool is too small to have a share would otherwise admit every Slot on
// the replica. Both are the same answer and they are decided in one place, so
// that place is load-bearing rather than shadowed by a second test of the
// same thing.
func censusCandidate(peakBytes, poolBytes uint64) (bool, uint64) {
	share := poolBytes / 100 * CensusCandidateSharePercent
	if share == 0 {
		return false, 0
	}
	return peakBytes >= share, share
}

// censusBuilders are this Slot's censuses in progress, one per candidate
// Plan, kept apart by where the series came from. Nil for a Slot with no
// candidate, which is the ordinary Slot: a map is only allocated once
// something is counted into it.
//
// Two maps rather than one because the two sources must never be added
// together. The round's series are what the strategy has now; the synthetic
// series a no-data round produces are what that Plan's roster still
// remembers, which by design includes groups that have gone away. Counted
// into one census the remembered ones would read as current values, and a
// split planned on them would cut pieces that stay empty for as long as the
// roster remembers (decision-020 section 4.7.3.1).
type censusBuilders struct {
	byPlan       map[execution.PlanIdentity]*execution.DimensionCensusBuilder
	rosterByPlan map[execution.PlanIdentity]*execution.DimensionCensusBuilder
}

func (builders *censusBuilders) builder(
	identity execution.PlanCensusIdentity, source execution.DimensionCensusSource,
) *execution.DimensionCensusBuilder {
	target := &builders.byPlan
	if source == execution.DimensionCensusFromRoster {
		target = &builders.rosterByPlan
	}
	if *target == nil {
		*target = make(map[execution.PlanIdentity]*execution.DimensionCensusBuilder, 1)
	}
	builder := (*target)[identity.Plan]
	if builder == nil {
		builder = execution.NewDimensionCensusBuilder(identity, source)
		(*target)[identity.Plan] = builder
	}
	return builder
}

// observeSeries counts one evaluated series into the Plan's census for that
// series' source. The dimensions come from one of the series' records: every
// record of a series carries the same identity dimensions, which is what
// makes it that series, so the first one answers for all of them.
func (builders *censusBuilders) observeSeries(
	identity execution.PlanCensusIdentity, source execution.DimensionCensusSource, record execution.RecordView,
) {
	dimensions := record.Dimensions()
	if len(dimensions) == 0 {
		return
	}
	values := make(map[string]string, len(dimensions))
	for dimension, raw := range dimensions {
		if dimension == contract.NoDataDimensionTag {
			// The tag every synthetic series carries and no real one does. It
			// is not a dimension of the strategy: a piece cut by matching it
			// would hold the absence series and none of the series they are
			// about.
			continue
		}
		value, ok := censusDimensionValue(raw)
		if !ok {
			continue
		}
		values[dimension] = value
	}
	builders.builder(identity, source).ObserveSeries(values)
}

// censusDimensionValue reads a dimension the way a split matcher would have
// to write it: a string as itself, a number in its source form. Anything else
// - an object, an array, a null - is not something a value list can match, so
// it is left out rather than stringified into a value no matcher would ever
// equal.
func censusDimensionValue(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	switch raw[0] {
	case '"':
		var value string
		if json.Unmarshal(raw, &value) != nil || value == "" {
			return "", false
		}
		return value, true
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		var number json.Number
		if json.Unmarshal(raw, &number) != nil {
			return "", false
		}
		return number.String(), true
	default:
		return "", false
	}
}

// writeCensuses leaves each candidate Plan's census in the store at the end
// of the Slot, and says what happened to it: how many values it named, how
// many it could not, and the bytes it took. A Plan whose census is refused
// goes on evaluating - the census is a planning input, not part of the round
// - and the refusal is named rather than silently truncated, because a
// census cut to fit reads like a distribution and a planner cannot tell the
// two apart.
func (stream *streamedExecution) writeCensuses(ctx context.Context) {
	store := stream.coordinator.ports.Census
	if store == nil {
		return
	}
	at := int64(stream.header.Contract.Slot.EvaluationTime)
	for _, builder := range stream.censuses.byPlan {
		if !stream.writeCensus(ctx, store, builder, at) {
			return
		}
	}
	for plan, builder := range stream.censuses.rosterByPlan {
		if _, counted := stream.censuses.byPlan[plan]; counted {
			// The round saw this Plan's series, so they are the census. Its
			// roster is a memory of what used to report and has nothing to
			// add to a reading the round could take itself.
			continue
		}
		if !stream.writeCensus(ctx, store, builder, at) {
			return
		}
	}
}

// writeCensus stores one builder's census and says whether the Slot should go
// on: a cancelled context stops the rest, and everything else is this Plan's
// own outcome and nobody else's.
func (stream *streamedExecution) writeCensus(
	ctx context.Context, store execution.PlanCensusStore,
	builder *execution.DimensionCensusBuilder, at int64,
) bool {
	census, taken, err := builder.Build(at)
	if err != nil || !taken {
		return true
	}
	outcome, writeErr := store.WriteCensus(ctx, census)
	if writeErr != nil && ctx.Err() != nil {
		return false
	}
	stream.observeCensus(ctx, census, outcome, writeErr)
	return true
}

func (stream *streamedExecution) observeCensus(
	ctx context.Context,
	census execution.DimensionCensus,
	outcome execution.CensusWriteOutcome,
	err error,
) {
	overflowValues, overflowSeries := census.CensusOverflow()
	result := observability.ResultSuccess
	reason := observability.ReasonNone
	if err != nil || outcome.Status != execution.CensusWritten {
		result = observability.ResultDegraded
		reason = observability.ReasonCode(outcome.ReasonCode)
		if reason == "" {
			reason = observability.ReasonInternalUnknown
		}
	}
	stream.coordinator.emitObservation(ctx, observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageDimensionCensus,
		Result: observability.Result(result), Operation: observability.Operation(stream.request.Operation),
		Direction: observability.DirectionInternal, ReasonCode: reason, Err: err,
		Trace: observability.TraceFields{
			QueryGroupKey: string(stream.header.Contract.Slot.QueryGroup),
			StrategyID:    census.Identity.Plan.StrategyID,
			BusinessID:    census.Identity.Plan.BusinessID,
		},
		DimensionCensus: &observability.DimensionCensusFacts{
			Source: string(census.Source), Status: string(outcome.Status),
			Series: census.Series, Dimensions: len(census.Dimensions), Values: census.CensusValues(),
			OverflowValues: overflowValues, OverflowSeries: overflowSeries,
			Bytes: outcome.Bytes, Limit: outcome.Limit,
			PeakBytes: stream.censusPeakBytes, ShareBytes: stream.censusShareBytes,
		},
	})
}

// countSeriesForCensus counts one series of a candidate Plan into its census.
// A Plan whose Query Group is not a candidate costs one boolean here, which
// is what keeps the census off the ordinary Slot.
//
// The dimensions come from the series' PRIMARY records: a series is its
// dimension identity, so any of its records answers for all of them, and the
// first is the cheapest to reach.
//
// A synthetic no-data series goes through this same path - it is evaluated in
// the same batches as a real one - and it is counted into the roster census
// rather than the round's. It has to be one or the other and it cannot be the
// round's: those series exist because their groups did NOT report, so
// counting them beside the ones that did would put a value carrying no series
// into the distribution a split is cut from.
func (stream *streamedExecution) countSeriesForCensus(due execution.DuePlan, inputs []execution.SeriesEvaluationInputRequest) {
	if !stream.censusCandidate || stream.coordinator.ports.Census == nil || due.StateGeneration == "" {
		return
	}
	identity := execution.PlanCensusIdentity{Plan: due.Identity, StateGeneration: due.StateGeneration}
	for _, input := range inputs {
		source := execution.DimensionCensusFromRound
		if input.Kind == execution.SeriesKindNoData {
			source = execution.DimensionCensusFromRoster
		}
		for _, binding := range input.Inputs {
			if binding.Role != execution.InputRolePrimary || binding.View == nil || binding.View.Len() == 0 {
				continue
			}
			record, ok := binding.View.Record(0)
			if !ok {
				continue
			}
			stream.censuses.observeSeries(identity, source, record)
			return
		}
	}
}
