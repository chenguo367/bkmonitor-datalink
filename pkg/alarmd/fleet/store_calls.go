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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// Where a span's store failure was seen, closed.
const (
	// DependencyFromObject: the span's own object reported the store failure
	// on a round of its own.
	DependencyFromObject = "object"
	// DependencyFromReplica: the replica's calls to its own store were
	// failing when the span was given up, though the object's rounds never
	// named the store.
	DependencyFromReplica = "replica"
)

// StoreCallFailure is one call to this deployment's own store that failed,
// as the Redis call hook names it: the caller and the reason from the closed
// sets of package redisfailure.
type StoreCallFailure struct {
	Caller string
	Reason string
	At     time.Time
}

// StoreCallRun is the replica's latest run of failed calls to its own store:
// each failure within the skip dependency window of the one before, with
// their reasons counted. A failure further on starts the next run.
type StoreCallRun struct {
	From    time.Time
	To      time.Time
	Reasons map[string]int
}

// storeOwnCallers are the jobs whose data this deployment keeps in its own
// store (redisfailure.Callers): their calls failing is the store failing for
// the rounds. A diagnostics job, the strategy cache another system writes and
// other teams' facts on the same Redis are not, whatever client they share;
// nor is the store census, which reads the source and service stores under
// one caller name and times out on a large store, not a failing one.
var storeOwnCallers = map[string]struct{}{
	redisfailure.CallerControlPlane: {}, redisfailure.CallerOwnership: {}, redisfailure.CallerRuntimeState: {},
	redisfailure.CallerQueryCooldown: {}, redisfailure.CallerFleet: {},
	redisfailure.CallerLeaseConfirmation: {}, redisfailure.CallerEffectiveMaintenance: {},
	redisfailure.CallerCatalogTimelineRenewal: {}, redisfailure.CallerCatalogObjectRenewal: {},
	redisfailure.CallerCatalogContextRenewal: {},
}

// StoreOwnCaller says caller is a job whose data this deployment keeps in its
// own store (StoreOwnCallers).
func StoreOwnCaller(caller string) bool {
	_, own := storeOwnCallers[caller]
	return own
}

// StoreOwnCallers is the closed set of jobs whose failed calls make store
// call segments, sorted.
func StoreOwnCallers() []string {
	callers := make([]string, 0, len(storeOwnCallers))
	for _, caller := range redisfailure.Callers {
		if _, own := storeOwnCallers[caller]; own {
			callers = append(callers, caller)
		}
	}
	return callers
}

// ObserveStoreCall records one failed call to this deployment's own store:
// it extends the latest run when it comes within the skip dependency window
// of that run's end, and starts the next run otherwise. A call of another job
// is not the store's and is left out; a reason outside the closed set is
// counted as other.
func (tracker *Tracker) ObserveStoreCall(failure StoreCallFailure) {
	if _, own := storeOwnCallers[failure.Caller]; !own {
		return
	}
	reason := redisfailure.Other
	for _, known := range redisfailure.Reasons {
		if failure.Reason == known {
			reason = known
		}
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	at := failure.At
	if at.IsZero() {
		at = tracker.now()
	}
	if run := tracker.storeCallRun; run != nil && !at.After(run.To.Add(tracker.skipDependencyWindow)) {
		if at.After(run.To) {
			run.To = at
		}
		run.Reasons[reason]++
		return
	}
	tracker.storeCallRun = &StoreCallRun{From: at, To: at, Reasons: map[string]int{reason: 1}}
}

// storeCallRunFor is the latest run when it has a failure from the skip
// dependency window before firstAt up to at, the span's latest skip: the same
// reach the object's own failure has. Only the latest run can: at is now, and
// an older run that reaches firstAt is followed by one that does too. Called
// with the lock held.
func (tracker *Tracker) storeCallRunFor(firstAt, at time.Time) (StoreCallRun, bool) {
	run := tracker.storeCallRun
	if run == nil || run.From.After(at) || run.To.Before(firstAt.Add(-tracker.skipDependencyWindow)) {
		return StoreCallRun{}, false
	}
	return *run, true
}

// StoreCallRun is a copy of the latest run, and false before any.
func (tracker *Tracker) StoreCallRun() (StoreCallRun, bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.storeCallRun == nil {
		return StoreCallRun{}, false
	}
	run := *tracker.storeCallRun
	run.Reasons = copyCounts(run.Reasons)
	return run, true
}

func copyCounts(counts map[string]int) map[string]int {
	if counts == nil {
		return nil
	}
	out := make(map[string]int, len(counts))
	for name, count := range counts {
		out[name] = count
	}
	return out
}
