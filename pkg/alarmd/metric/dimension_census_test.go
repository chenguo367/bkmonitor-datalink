// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func censusObservation(facts *observability.DimensionCensusFacts) observability.Observation {
	return observability.Observation{
		Component: observability.ComponentState, Stage: observability.StageDimensionCensus,
		Result:          observability.ResultSuccess,
		Trace:           observability.TraceFields{QueryGroupKey: "qg-census", StrategyID: "4101", EvaluationTime: 600},
		DimensionCensus: facts,
	}
}

// A census is counted by where its values came from and by what the store
// did, and the values it named are counted beside the ones it could not.
//
// The two families are separate because they answer separate questions: how
// many censuses were taken is not how much of a strategy they could name, and
// a reader asking the second needs the overflow beside the named values or
// the answer is a number with no denominator.
func TestACensusIsCountedBySourceAndOutcomeWithItsOverflowBesideIt(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	const writes = "bkmonitor_alarmd_dimension_census_total"
	if before := gatherFamily(t, r, writes); len(before) !=
		len(observability.DimensionCensusSources())*len(observability.DimensionCensusStatuses()) {
		t.Fatalf("%d series before any round, want every source and outcome so a zero is a reading", len(before))
	}

	ctx := context.Background()
	r.Observe(ctx, censusObservation(&observability.DimensionCensusFacts{
		Source: observability.DimensionCensusSourceRound, Status: observability.DimensionCensusStatusWritten,
		Series: 12000, Dimensions: 2, Values: 4096, OverflowValues: 104, OverflowSeries: 300,
	}))
	r.Observe(ctx, censusObservation(&observability.DimensionCensusFacts{
		Source: observability.DimensionCensusSourceRoster, Status: observability.DimensionCensusStatusWritten,
		Series: 40, Dimensions: 1, Values: 40,
	}))
	r.Observe(ctx, censusObservation(&observability.DimensionCensusFacts{
		Source: observability.DimensionCensusSourceRound, Status: observability.DimensionCensusStatusRejected,
		Series: 90000, Dimensions: 4, Values: 16000, OverflowValues: 12, OverflowSeries: 40,
	}))
	r.Observe(ctx, censusObservation(nil))

	taken := func(source, status string) float64 {
		return testutil.ToFloat64(r.phaseTwo.dimensionCensusWrites.WithLabelValues(source, status))
	}
	if got := taken(observability.DimensionCensusSourceRound, observability.DimensionCensusStatusWritten); got != 1 {
		t.Fatalf("round/WRITTEN = %v, want 1", got)
	}
	// The fallback has to be countable on its own. It is the one whose values
	// are an upper bound rather than a current reading, and a reader that
	// cannot separate it from the round's is reading a distribution that is
	// partly a memory of groups that are gone.
	if got := taken(observability.DimensionCensusSourceRoster, observability.DimensionCensusStatusWritten); got != 1 {
		t.Fatalf("roster/WRITTEN = %v, want 1: the fallback must be countable apart from the round", got)
	}
	if got := taken(observability.DimensionCensusSourceRound, observability.DimensionCensusStatusRejected); got != 1 {
		t.Fatalf("round/REJECTED = %v, want 1", got)
	}
	if got := taken(observability.DimensionCensusSourceRoster, observability.DimensionCensusStatusRejected); got != 0 {
		t.Fatalf("roster/REJECTED = %v, want 0", got)
	}

	named := func(kind string) float64 {
		return testutil.ToFloat64(r.phaseTwo.dimensionCensusValues.WithLabelValues(kind))
	}
	if got := named("named"); got != 4096+40+16000 {
		t.Fatalf("named values = %v, want every census counted, refused ones included: a census the store "+
			"would not keep still says how wide the strategy is", got)
	}
	if got := named("overflow_values"); got != 116 {
		t.Fatalf("overflow values = %v, want 116", got)
	}
	if got := named("overflow_series"); got != 340 {
		t.Fatalf("overflow series = %v, want 340: read against the named values it is what says whether a "+
			"split can be planned at all", got)
	}
}

// A census that arrives with no source or no outcome is counted under a name,
// not under its own text. The label sets are pre-created from the closed
// vocabularies, and a value arriving at runtime is a series nobody declared.
func TestACensusWithNoSourceOrOutcomeIsCountedUnderTheNamedUnknown(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	before := gatherFamily(t, r, "bkmonitor_alarmd_dimension_census_total")

	r.Observe(context.Background(), censusObservation(&observability.DimensionCensusFacts{Series: 1, Values: 1}))

	if got := testutil.ToFloat64(r.phaseTwo.dimensionCensusWrites.WithLabelValues(
		observability.DimensionCensusSourceUnknown, observability.DimensionCensusStatusUnknown)); got != 1 {
		t.Fatalf("unknown/UNKNOWN = %v, want 1", got)
	}
	if after := gatherFamily(t, r, "bkmonitor_alarmd_dimension_census_total"); len(after) != len(before) {
		t.Fatalf("the family grew from %d series to %d: a census with no source must land on a declared "+
			"label, not create one", len(before), len(after))
	}
}
