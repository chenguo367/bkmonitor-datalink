// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Why an owned Query Group has no fresh measurement (Stats.Uncovered,
// Engine.Uncovered), closed. A group is counted under the first of these that
// holds for it, so the reasons add up to the groups owned less those covered:
//   - not_seen: owned here, with no state in this process: no first read of
//     it has reached the lookback since it came here.
//   - never_complete: none of its first reads has been whole
//     (Coverage.NeverCompleteFirstRead): nothing to compare a recheck with.
//   - trial_blocking: an earlier-read trial of a Slot before its latest is
//     still reserved, where its next sample would be taken.
//   - unmeasured: samples of it have been taken in this process, none of
//     them completed as a measurement.
//   - first_sample_pending: no sample of it has been taken, and its first is
//     not due yet: first samples are spread over the first rest.
//   - resting: the rest after its latest sample is not over.
//   - awaiting_first_read: its rest is over and its next sample waits for its
//     next formal first read; a group read once an hour spends most of its
//     time here.
const (
	UncoveredNotSeen            = "not_seen"
	UncoveredNeverComplete      = "never_complete"
	UncoveredTrialBlocking      = "trial_blocking"
	UncoveredUnmeasured         = "unmeasured"
	UncoveredFirstSamplePending = "first_sample_pending"
	UncoveredResting            = "resting"
	UncoveredAwaitingFirstRead  = "awaiting_first_read"
)

// UncoveredReasons is every reason an owned group is not covered, in the
// order they are tried.
var UncoveredReasons = []string{UncoveredNotSeen, UncoveredNeverComplete, UncoveredTrialBlocking, UncoveredUnmeasured,
	UncoveredFirstSamplePending, UncoveredResting, UncoveredAwaitingFirstRead}

// coverageLocked says whether a group's measurement is fresh and, when it is
// not, why (UncoveredReasons; never not_seen, which is a group with no state).
// Stats and Uncovered read a group alike through it.
func coverageLocked(state *group, now time.Time) (bool, string) {
	if state.incompleteFirstReads > 0 && !state.completeFirstRead {
		// A read in flight is not a measurement for a group none of whose
		// reads has been whole.
		return false, UncoveredNeverComplete
	}
	switch {
	case state.capturing || state.sample != nil || state.probe != nil:
		return true, ""
	case state.measured:
		// Fresh within twice the longest a sample of it can take: its rest at
		// the most spread, its deepest rung, and the wait for its next first
		// read.
		rest := min(time.Duration(state.rest*float64(state.step)), restCap)
		cycle := rest*5/4 + rungDelay(state.depth-1, state.step) + state.period
		if now.Sub(state.measuredAt) <= 2*cycle {
			return true, ""
		}
	}
	switch trial := state.prepared; {
	case trial != nil && !trial.formal && trial.query.Contract.Slot.EvaluationTime < state.lastSlot:
		return false, UncoveredTrialBlocking
	case state.captured && !state.measured:
		return false, UncoveredUnmeasured
	case now.Before(state.nextAt) && !state.captured:
		return false, UncoveredFirstSamplePending
	case now.Before(state.nextAt):
		return false, UncoveredResting
	}
	return false, UncoveredAwaitingFirstRead
}

// UncoveredGroup is one owned Query Group with no fresh measurement: why
// (UncoveredReasons), since when this process has had state of it, and when
// its next sample may be taken; both zero for a group not seen.
type UncoveredGroup struct {
	QueryGroup execution.QueryGroupIdentity `json:"query_group"`
	Reason     string                       `json:"reason"`
	Since      time.Time                    `json:"since"`
	NextAt     time.Time                    `json:"next_at"`
}

// Uncovered is each of the given owned Query Groups that is not covered, as
// Stats counts it in Stats.Uncovered, in the order given. A group the
// lookback has no state of is not_seen.
func (engine *Engine) Uncovered(owned []execution.QueryGroupIdentity) []UncoveredGroup {
	if engine == nil {
		return nil
	}
	now := engine.options.Now()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	var uncovered []UncoveredGroup
	for _, queryGroup := range owned {
		state := engine.groups[queryGroup]
		if state == nil {
			uncovered = append(uncovered, UncoveredGroup{QueryGroup: queryGroup, Reason: UncoveredNotSeen})
			continue
		}
		if fresh, reason := coverageLocked(state, now); !fresh {
			uncovered = append(uncovered, UncoveredGroup{QueryGroup: queryGroup, Reason: reason, Since: state.reading.since,
				NextAt: state.nextAt})
		}
	}
	return uncovered
}

// uncoveredCounts is every reason at zero, then counted.
func uncoveredCounts() map[string]int {
	counts := make(map[string]int, len(UncoveredReasons))
	for _, reason := range UncoveredReasons {
		counts[reason] = 0
	}
	return counts
}
