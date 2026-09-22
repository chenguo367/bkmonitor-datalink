// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// The split planner's closed vocabulary, kept here because this is the
// package both ends can see: the planner in controlplane reads these, and
// the metric labels are pre-created from the same lists. Written twice they
// would be two derivations of one relation, and nothing would fail when one
// of them gained a value.
//
// Every outcome is a word rather than a boolean because "this object was not
// split" is the answer a reader arrives with, and the six reasons behind it
// call for six different actions: wait, raise a bound, shard by hash, fix the
// census, or nothing at all.
const (
	// SplitOutcomePlanned is a split this round could plan: a dimension, the
	// pieces, and what each would carry.
	SplitOutcomePlanned = "PLANNED"
	// SplitOutcomeUnderShare is an object that does not need splitting. The
	// ordinary answer, and counted so that "nothing was planned" can be told
	// from "nothing was looked at".
	SplitOutcomeUnderShare = "UNDER_SHARE"
	// SplitOutcomeNoCensus is an object over its share that nobody has taken
	// a census of yet. Expected for one round after a replica takes an object
	// over, and a standing count here means the census is not being written.
	SplitOutcomeNoCensus = "NO_CENSUS"
	// SplitOutcomeCensusStale is a census too old to plan from: the
	// distribution it describes is not the one the object has now.
	SplitOutcomeCensusStale = "CENSUS_STALE"
	// SplitOutcomeValueTooHeavy is the structural refusal: one dimension
	// value carries more series than a piece may hold, on every dimension the
	// census names. A value cannot be divided by a value-list matcher, so no
	// list assignment is even - this is the object that needs hashing rather
	// than a wider bound or a longer wait.
	SplitOutcomeValueTooHeavy = "VALUE_TOO_HEAVY"
	// SplitOutcomeTailTooLarge is the census's own bound getting in the way:
	// the series on values it could not name would themselves overfill the
	// piece that catches them. Read with the census's overflow.
	SplitOutcomeTailTooLarge = "TAIL_TOO_LARGE"
	// SplitOutcomeSkewUnreachable is a split that could be cut but would not
	// hold: the best assignment is still more lopsided than the resplit
	// threshold, so it would be undone about as fast as it was made.
	SplitOutcomeSkewUnreachable = "SKEW_UNREACHABLE"
	// SplitOutcomeTooFewValues is a census naming fewer values than there are
	// pieces to fill. Not a missing reading and not the object's shape: the
	// strategy simply does not vary enough along any dimension the census
	// names to be cut this many ways.
	SplitOutcomeTooFewValues = "TOO_FEW_VALUES"
	// SplitOutcomeNoReading is the planner with a number missing - no share,
	// no peak, or a census of no series. Never read as "no pressure": an
	// unknown is reported as an unknown.
	SplitOutcomeNoReading = "NO_READING"
)

// SplitOutcomes is the label set the split metrics are pre-created with, so
// every outcome is a series from startup and a zero is a reading rather than
// an absence.
func SplitOutcomes() []string {
	return []string{
		SplitOutcomePlanned, SplitOutcomeUnderShare, SplitOutcomeNoCensus,
		SplitOutcomeCensusStale, SplitOutcomeValueTooHeavy, SplitOutcomeTailTooLarge,
		SplitOutcomeSkewUnreachable, SplitOutcomeTooFewValues, SplitOutcomeNoReading,
	}
}

// normalizeSplitPlanFacts copies the facts and holds the outcome to its
// vocabulary. An outcome outside it becomes NO_READING rather than reaching
// the metric: a label nobody declared is a series nobody pre-created, and one
// arriving at runtime is how a bounded label set stops being bounded. It
// becomes the word for "a number is missing" because that is what an
// unrecognised decision is - the planner reached an answer this build cannot
// name, and reading it as any of the others would be a claim.
func normalizeSplitPlanFacts(facts *SplitPlanFacts) *SplitPlanFacts {
	if facts == nil {
		return nil
	}
	copied := *facts
	known := false
	for _, outcome := range SplitOutcomes() {
		if copied.Outcome == outcome {
			known = true
			break
		}
	}
	if !known {
		copied.Outcome = SplitOutcomeNoReading
	}
	return &copied
}

// SplitPlanFacts is one object's split decision as a dry run reports it: what
// was decided, from which readings, and - when a split was planned - what the
// pieces would carry.
//
// The readings travel with the decision because the decision cannot be
// checked without them. "This object was not split" is the same line for an
// object under its share and an object whose heaviest value is indivisible,
// and those are opposite situations.
type SplitPlanFacts struct {
	Outcome    string `json:"outcome"`
	StrategyID string `json:"strategy_id,omitempty"`
	BusinessID string `json:"business_id,omitempty"`
	// PeakBytes and ShareBytes are the trigger: what this object held, and
	// what one object may hold.
	PeakBytes  uint64 `json:"peak_bytes"`
	ShareBytes uint64 `json:"share_bytes"`
	// Shards is how many pieces were planned and Carrying how many of them
	// carry a value list; the difference is the one piece that catches
	// everything no list matches. ShardsCapped says the arithmetic asked for
	// more than the cap allows - a piece that stays over its share after the
	// split, which is worth seeing rather than rounding away.
	Shards       int  `json:"shards"`
	Carrying     int  `json:"carrying_shards"`
	ShardsCapped bool `json:"shards_capped,omitempty"`
	// Dimension is the one the pieces would be cut on, and Candidates how
	// many the census offered. One of several says the choice was made; one
	// of one says there was nothing to choose from.
	Dimension  string `json:"dimension,omitempty"`
	Candidates int    `json:"dimension_candidates"`
	// Series is what the census counted, and CensusAgeSeconds how old that
	// count is.
	Series           uint32 `json:"series"`
	CensusAgeSeconds int64  `json:"census_age_seconds"`
	CensusSource     string `json:"census_source,omitempty"`
	// HeaviestValueSeries is the largest single value's weight and
	// TargetSeries what one piece should carry. The first above the second is
	// the whole of VALUE_TOO_HEAVY, and the two numbers say how far past it
	// is - which is what decides whether a wider cap would help or only
	// hashing will.
	HeaviestValueSeries uint32 `json:"heaviest_value_series"`
	TargetSeries        uint32 `json:"target_series"`
	// TailSeries is the series on values the census could not name; they all
	// land on the piece that catches what no list matches. That piece is
	// expected to be nearly empty - it exists for values that appear after
	// the census - so it is reported here rather than folded into the skew,
	// where it would make every healthy split look lopsided.
	TailSeries uint32 `json:"tail_series"`
	// SkewPercent is the largest carrying piece over the smallest, in
	// percent, for the assignment that was chosen or the best one that was
	// refused. Percent rather than a ratio so the line carries an integer.
	SkewPercent int `json:"skew_percent"`
	// LargestShardSeries and SmallestShardSeries are that skew's two ends, so
	// a reader can tell a lopsided split from a small one.
	LargestShardSeries  uint32 `json:"largest_shard_series"`
	SmallestShardSeries uint32 `json:"smallest_shard_series"`
	// PlansInGroup is how many Plans the Query Group whose bytes were read
	// carries. One means the peak is this Plan's; more means it was shared
	// out among them, and the estimate is that much softer.
	PlansInGroup int `json:"plans_in_group"`
	// DryRun is true while the planner only reports. It is on the line rather
	// than implied by the build, because the line is the only place a reader
	// can tell a plan that was acted on from one that was not.
	DryRun bool `json:"dry_run"`
}
