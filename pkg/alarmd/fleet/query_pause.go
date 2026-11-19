// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"sort"
	"strconv"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// InstanceCoverage is how many instances an answer read of how many there
// are. Below the whole, every count it gives of the paused objects is a lower
// bound.
type InstanceCoverage struct {
	Counted  int `json:"counted"`
	Expected int `json:"expected"`
}

// whole says every instance was read.
func (coverage InstanceCoverage) whole() bool {
	return coverage.Expected > 0 && coverage.Counted >= coverage.Expected
}

// QueryPause is a strategy whose queries the degraded pool holds paused, on
// its board row and drawer, absent for a strategy none of whose objects is
// paused: since when (the earliest entry among its paused objects), until
// when every one of them has had its retry (the latest end), the most
// failures any has, how many of its objects are paused, and the reason and
// request targets of the one paused first. Request targets are what the
// query asked for - the query service names the status it refused under, not
// the table - and a target the record does not have is absent.
type QueryPause struct {
	Since              time.Time                     `json:"since"`
	Until              *time.Time                    `json:"until,omitempty"`
	Failures           uint32                        `json:"failures"`
	Reason             string                        `json:"reason"`
	Objects            BoardCount                    `json:"objects"`
	RequestTargets     []observability.RequestTarget `json:"request_targets,omitempty"`
	RequestTargetCount int                           `json:"request_target_count,omitempty"`
}

// QueryPauseObject is one paused object of a strategy, on its drawer, named
// by nothing but what it waits on: since when it is paused, when it is next
// retried, its failures, its reason and its request targets.
type QueryPauseObject struct {
	Since              time.Time                     `json:"since"`
	Until              *time.Time                    `json:"until,omitempty"`
	Failures           uint32                        `json:"failures"`
	Reason             string                        `json:"reason"`
	RequestTargets     []observability.RequestTarget `json:"request_targets,omitempty"`
	RequestTargetCount int                           `json:"request_target_count,omitempty"`
}

// pausedHas says whether paused already holds queryGroup.
func pausedHas(paused []Anomaly, queryGroup string) bool {
	for _, row := range paused {
		if row.QueryGroup == queryGroup {
			return true
		}
	}
	return false
}

// pausedSince is when an object entered the pool: its record's entry, or the
// tracker's own reading of it where the record had none.
func pausedSince(row Anomaly) time.Time {
	if row.QueryCooldown != nil && !row.QueryCooldown.EnteredAt.IsZero() {
		return row.QueryCooldown.EnteredAt
	}
	return row.DemotedSince
}

// pauseTargetsKey is the key strategies paused on the same request targets
// share: the whole set's digest and its count, empty when the object's
// targets are not known. Never on a page.
func pauseTargetsKey(row Anomaly) string {
	if row.RequestTargets == nil || row.RequestTargets.Digest == "" {
		return ""
	}
	return row.RequestTargets.Digest + "/" + strconv.Itoa(row.RequestTargets.Count)
}

// pausedInOrder is a strategy's paused objects first paused first: by entry,
// then by request target key, then by object, so the one a row names is the
// same however the replicas' answers were ordered - two objects that entered
// in the same second, as a pool restored at once does, otherwise trade places
// from one computation to the next.
func pausedInOrder(paused []Anomaly) []Anomaly {
	ordered := append([]Anomaly(nil), paused...)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if sinceA, sinceB := pausedSince(a), pausedSince(b); !sinceA.Equal(sinceB) {
			return sinceA.Before(sinceB)
		}
		if keyA, keyB := pauseTargetsKey(a), pauseTargetsKey(b); keyA != keyB {
			return keyA < keyB
		}
		return a.QueryGroup < b.QueryGroup
	})
	return ordered
}

// queryPauseOf is a strategy's pause, nil when none of its objects is paused,
// with the request target key of the object it names; atLeast marks its count
// of objects a lower bound.
func queryPauseOf(decided DiagnosisRow, atLeast bool) (*QueryPause, string) {
	if len(decided.paused) == 0 {
		return nil, ""
	}
	ordered := pausedInOrder(decided.paused)
	first := ordered[0]
	pause := &QueryPause{Since: pausedSince(first), Reason: QueryPauseReasonOf(first.PoolReason),
		Objects: BoardCount{Value: len(ordered), AtLeast: atLeast}}
	if targets := first.RequestTargets; targets != nil {
		pause.RequestTargets, pause.RequestTargetCount = append([]observability.RequestTarget(nil), targets.Targets...), targets.Count
	}
	for _, row := range ordered {
		if row.QueryCooldown == nil {
			continue
		}
		pause.Failures = max(pause.Failures, row.QueryCooldown.Failures)
		if until := row.QueryCooldown.Until; !until.IsZero() && (pause.Until == nil || until.After(*pause.Until)) {
			pause.Until = &until
		}
	}
	return pause, pauseTargetsKey(first)
}

// queryPauseObjectsOf is a strategy's paused objects for its drawer, first
// paused first.
func queryPauseObjectsOf(decided DiagnosisRow) []QueryPauseObject {
	if len(decided.paused) == 0 {
		return nil
	}
	objects := make([]QueryPauseObject, 0, len(decided.paused))
	for _, row := range pausedInOrder(decided.paused) {
		object := QueryPauseObject{Since: pausedSince(row), Reason: QueryPauseReasonOf(row.PoolReason)}
		if cooldown := row.QueryCooldown; cooldown != nil {
			object.Failures = cooldown.Failures
			if until := cooldown.Until; !until.IsZero() {
				object.Until = &until
			}
		}
		if targets := row.RequestTargets; targets != nil {
			object.RequestTargets, object.RequestTargetCount = append([]observability.RequestTarget(nil), targets.Targets...), targets.Count
		}
		objects = append(objects, object)
	}
	return objects
}

// countSameTargets gives each paused row the number of paused strategies
// that name the same request targets, itself included; a row whose targets
// are not known is counted with none. A strategy whose paused objects asked
// for different targets is counted by its first paused object's, the one its
// row names; the drawer lists each object with its own. atLeast marks the
// counts lower bounds.
func countSameTargets(rows []BoardRow, atLeast bool) {
	strategies := map[string]int{}
	for _, row := range rows {
		if row.QueryPause != nil && row.pauseKey != "" {
			strategies[row.pauseKey]++
		}
	}
	for i := range rows {
		if rows[i].QueryPause != nil && rows[i].pauseKey != "" {
			rows[i].SameTargetStrategies = &BoardCount{Value: strategies[rows[i].pauseKey], AtLeast: atLeast}
		}
	}
}

// pauseReading is what a board computation knows of how whole its reading of
// the paused objects is: the instances it read, and whether the pooled rows
// it read were cut to the snapshots' budget.
type pauseReading struct {
	coverage InstanceCoverage
	cut      bool
}

// atLeast says the pause counts are lower bounds: an instance not read, or
// a pooled row cut from what was read.
func (reading pauseReading) atLeast() bool {
	return !reading.coverage.whole() || reading.cut
}

// pauseReadingOf is a view's reading of the paused objects: the replicas it
// counted of those it expected, and whether its pooled rows are fewer than
// the pool.
func pauseReadingOf(view *View) pauseReading {
	if view == nil {
		return pauseReading{cut: true}
	}
	return pauseReading{coverage: InstanceCoverage{Counted: len(view.Replicas), Expected: view.expectedReplicas},
		cut: view.DemotedTotal > len(view.Demoted)}
}

// pauseRankOf orders rows of one urgency and action: a paused row by how many
// paused strategies share its request targets (none when they are not
// known), ahead of every row that is not paused.
func pauseRankOf(row BoardRow) int {
	switch {
	case row.QueryPause == nil:
		return -1
	case row.SameTargetStrategies == nil:
		return 0
	default:
		return row.SameTargetStrategies.Value
	}
}

// QueryPausePart is one replica's paused objects, counted from its own
// pooled rows: how many, by pool reason (every reason of the words, zero
// included, and other), and the earliest entry. Cut says its pooled rows were
// cut to the snapshot's budget. It counts no strategies: a row names the
// strategies the rounds it saw named, and a Query Group restored into the
// pool runs no round, so its row may name none for as long as it is held - a
// count from the rows would read as an exact zero. The board counts paused
// strategies from the catalog (the paused facet). The counts add: a Query
// Group a handover's two owners both list counts on both for that moment, as
// every count of the parts does; the board and drawer read the rows
// themselves and count each once.
type QueryPausePart struct {
	Objects          int            `json:"objects"`
	ByReason         map[string]int `json:"by_reason"`
	LongestEnteredAt time.Time      `json:"longest_entered_at,omitempty"`
	Cut              bool           `json:"cut,omitempty"`
}

// newPauseReasonCounts is every pause reason at zero.
func newPauseReasonCounts() map[string]int {
	counts := make(map[string]int, len(ProductWords().QueryPauseReason))
	for reason := range ProductWords().QueryPauseReason {
		counts[reason] = 0
	}
	return counts
}

// queryPausePartOf is a replica view's paused objects.
func queryPausePartOf(view View) *QueryPausePart {
	part := &QueryPausePart{ByReason: newPauseReasonCounts(), Cut: view.DemotedTotal > len(view.Demoted)}
	for _, row := range view.Demoted {
		if row.QueryCooldown == nil {
			continue
		}
		part.Objects++
		part.ByReason[QueryPauseReasonOf(row.PoolReason)]++
		if since := pausedSince(row); !since.IsZero() && (part.LongestEnteredAt.IsZero() || since.Before(part.LongestEnteredAt)) {
			part.LongestEnteredAt = since
		}
	}
	return part
}

// mergeQueryPause adds a replica's paused objects into the deployment's; a
// part without them (a build before it) leaves the merged counts lower
// bounds.
func mergeQueryPause(merged **QueryPausePart, part *QueryPausePart) {
	if *merged == nil {
		*merged = &QueryPausePart{ByReason: newPauseReasonCounts()}
	}
	into := *merged
	if part == nil {
		into.Cut = true
		return
	}
	into.Objects += part.Objects
	for reason, count := range part.ByReason {
		into.ByReason[QueryPauseReasonOf(reason)] += count
	}
	if !part.LongestEnteredAt.IsZero() && (into.LongestEnteredAt.IsZero() || part.LongestEnteredAt.Before(into.LongestEnteredAt)) {
		into.LongestEnteredAt = part.LongestEnteredAt
	}
	into.Cut = into.Cut || part.Cut
}

// HealthQueryCooldown is the deployment's paused objects on the health
// route: how many, the earliest entry, and how many by pool reason - not how
// many strategies, which the board counts (QueryPausePart). Objects and
// ByReason are the instances' counts added: for the seconds of a handover a
// Query Group its two owners both list is counted twice. AtLeast says only
// that something was not counted - an instance not read, a replica's pooled
// rows cut, or a part from a build before them - not that the total is a
// lower bound, which a handover's double count can still exceed.
// A non-empty pool is no fault of the deployment's - nearly all of it is
// strategies pointing at tables or fields that are not there - and does not
// change the deployment's health.
type HealthQueryCooldown struct {
	Objects          int            `json:"objects"`
	LongestEnteredAt *time.Time     `json:"longest_entered_at,omitempty"`
	ByReason         map[string]int `json:"by_reason"`
	AtLeast          bool           `json:"at_least,omitempty"`
}

// healthQueryCooldownOf is the merged part's paused objects on the health
// route, nil when no part was merged.
func healthQueryCooldownOf(view *View, part *QueryPausePart) *HealthQueryCooldown {
	if part == nil {
		return nil
	}
	health := &HealthQueryCooldown{Objects: part.Objects, ByReason: part.ByReason,
		AtLeast: part.Cut || !pauseReadingOf(view).coverage.whole()}
	if !part.LongestEnteredAt.IsZero() {
		longest := part.LongestEnteredAt
		health.LongestEnteredAt = &longest
	}
	return health
}

// OverviewQueryPause is the overview's runtime line of paused queries: how
// many objects, and since when the longest has been paused, read from the
// health route (HealthQueryCooldown): the instances' counts added, so a
// handover's seconds may count a Query Group twice, and AtLeast only says an
// instance was not read or its rows were cut. How many strategies is the
// board's paused facet, counted from the catalog; the line leads there.
type OverviewQueryPause struct {
	Objects      BoardCount `json:"objects"`
	LongestSince *time.Time `json:"longest_since,omitempty"`
}

// overviewQueryPauseOf is the health route's paused objects as the
// overview's line, nil when the health route has none to say.
func overviewQueryPauseOf(health HealthResponse) *OverviewQueryPause {
	pool := health.QueryCooldown
	if pool == nil {
		return nil
	}
	return &OverviewQueryPause{Objects: BoardCount{Value: pool.Objects, AtLeast: pool.AtLeast}, LongestSince: pool.LongestEnteredAt}
}
