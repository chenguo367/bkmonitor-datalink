// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Plan new to the activation is forced to warm up (the control plane
// cannot tell a Plan that never ran from one deleted and put back: the state
// is one key per series, with no index by Plan). The warm-up exists for the
// second kind - state left under the same identity and generation, across a
// hole the points do not show - and costs the first kind its first Slot,
// evaluated and thrown away, so a new strategy began detecting a period
// late. The Worker can tell them apart at that first Slot: it reads the
// state of every series the Slot reads, and the Plan's gap marker and
// no-data memory. When none of them holds anything from before this Slot,
// the Plan never ran under this generation: it is not warmed up, its series
// warm up on their own windows as any new series does, and a satisfied
// marker records the judgement so the next Slot - which finds this Slot's
// state - does not take the Plan for one that ran before.

type unseenPlansKey struct{}

// withUnseenPlans hands finalization the due Plans the Slot found nothing of
// from before it.
func withUnseenPlans(ctx context.Context, plans map[execution.PlanKey]struct{}) context.Context {
	return context.WithValue(ctx, unseenPlansKey{}, plans)
}

func unseenPlansFrom(ctx context.Context) map[execution.PlanKey]struct{} {
	plans, _ := ctx.Value(unseenPlansKey{}).(map[execution.PlanKey]struct{})
	return plans
}

// unseenPlans is the due Plans of which this Slot read nothing written before
// it: no state record of any series it reads other than one this same Slot
// wrote on an earlier attempt, nothing carried from another generation, and
// no no-data memory. A record the store could not read is something: the
// judgement errs toward warming up.
func (stream *streamedExecution) unseenPlans() map[execution.PlanKey]struct{} {
	unseen := make(map[execution.PlanKey]struct{}, len(stream.header.DuePlans))
	for _, due := range stream.header.DuePlans {
		version, err := execution.BuildApplyVersion(stream.header.Contract, due.StateApplyEpoch)
		if err != nil {
			continue
		}
		before := func(persisted execution.ApplyVersion) bool {
			return execution.CompareApplyVersion(persisted, version) != execution.ApplyVersionEqual
		}
		// The views of a Slot's landed windows are gone; what they said of
		// the Plan was kept as they went (noteSeenViews).
		_, seen := stream.windows.seen[due.Identity]
		if !seen {
			seen = viewsSeenBefore(due, version, stream.state.Items)
		}
		for _, memory := range stream.noData.Items {
			if seen {
				break
			}
			if memory.Identity.Plan == due.Identity && memory.Identity.StateGeneration == due.StateGeneration &&
				memory.Status != execution.NoDataMemoryMissing && before(memory.PersistedApplyVersion) {
				seen = true
			}
		}
		if !seen {
			unseen[due.Key()] = struct{}{}
		}
	}
	return unseen
}

// viewsSeenBefore reports whether the views hold anything of the Plan under
// its generation written before the Slot at version: a record of another
// Slot, or history carried from another generation.
func viewsSeenBefore(due execution.DuePlan, version execution.ApplyVersion, views []execution.RuntimeStateView) bool {
	for _, view := range views {
		if view.Identity.Plan != due.Identity || view.Identity.StateGeneration != due.StateGeneration {
			continue
		}
		if view.Carried != nil || (view.Status != execution.StateMissingWarming &&
			execution.CompareApplyVersion(view.PersistedApplyVersion, version) != execution.ApplyVersionEqual) {
			return true
		}
	}
	return false
}

// firstSeenActivations splits the Plans this Slot forces to warm up into
// those that ran before under their generation - still forced - and those
// that never did: new to their activation (Arrived), no gap marker at all,
// and nothing read from before the Slot. Only a Plan without a marker can be
// new; one with a marker ran. A Plan that stayed active while its generation
// moved ran too, under the generation before: its new generation holds
// nothing because nothing was carried, and it warms up.
func firstSeenActivations(
	forced execution.PlanActivationResult,
	loadedGaps execution.GapLoadResult,
	unseen map[execution.PlanKey]struct{},
) (stillForced execution.PlanActivationResult, firstSeen map[execution.PlanKey]execution.PlanActivationFact) {
	stillForced = execution.PlanActivationResult{Contract: forced.Contract}
	firstSeen = make(map[execution.PlanKey]execution.PlanActivationFact)
	for _, fact := range forced.Facts {
		marker, found := loadedGaps.Find(fact.GapIdentity())
		_, nothingBefore := unseen[fact.Key()]
		if fact.Selected.Arrived && nothingBefore && (!found || marker.Status == execution.GapMissing) {
			firstSeen[fact.Key()] = fact
			continue
		}
		stillForced.Facts = append(stillForced.Facts, fact)
	}
	return stillForced, firstSeen
}

// firstSeenUndecided reports whether any Plan this Slot forces to warm up
// could still be judged first-seen: new to its activation and without a
// marker, the two facts firstSeenActivations reads that the Slot has before
// its State is read. The third - nothing of the Plan read from before the
// Slot - is known only once every series of the Plan has been read, so a
// Slot holding such a Plan cannot write any window before its evaluation is
// over (windowed_apply.go): the judgement decides whether the Plan writes at
// all, and a window written first would have decided it.
func firstSeenUndecided(forced execution.PlanActivationResult, loadedGaps execution.GapLoadResult) bool {
	for _, fact := range forced.Facts {
		marker, found := loadedGaps.Find(fact.GapIdentity())
		if fact.Selected.Arrived && (!found || marker.Status == execution.GapMissing) {
			return true
		}
	}
	return false
}

// satisfiedMarker is the marker that records a first-seen Plan as one that
// has run: a clear at this Slot's ApplyVersion on a key that holds nothing,
// which the store writes as a marker with no scope - a cleared tombstone at
// the activation's epoch, which the next Slot reads as satisfied warming.
func satisfiedMarker(
	contractRef execution.FrozenExecutionContractRef,
	fact execution.PlanActivationFact,
) (execution.PlanGapMutation, error) {
	version, err := execution.BuildApplyVersion(contractRef, fact.Selected.StateApplyEpoch)
	if err != nil {
		return execution.PlanGapMutation{}, fmt.Errorf("alarmd worker: build first-seen Plan ApplyVersion: %w", err)
	}
	mutation, err := execution.BuildPlanGapMutation(execution.PlanGapMutation{
		Identity: fact.GapIdentity(), ApplyVersion: version, ScheduleRevision: fact.Selected.ScheduleRevision,
		Scopes: []execution.GapScopeMutation{{Scope: execution.GapScope{}, Kind: execution.GapClear}},
	})
	if err != nil {
		return execution.PlanGapMutation{}, fmt.Errorf("alarmd worker: build first-seen Plan marker: %w", err)
	}
	return mutation, nil
}
