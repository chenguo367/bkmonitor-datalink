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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A member's coverage says its round leaves its State write out, with its
// page's last renewal in the mark the Slot read, exactly when the round
// would: an open member of its own generation's book, in a round that leaves
// members out, its Plan not stopped, renewing its keys, not in its sampled
// round, its page not written whole for want of renewal. Each that does not
// hold says the round writes it.
func TestAMembersCoverageSaysWhetherItsRoundLeavesItsWriteOut(t *testing.T) {
	plan := shadowDue.Identity
	member := shadowSeries(0)
	key := segment.PlanKey{Plan: plan, Generation: "g1"}
	renewal := segment.Renewal{At: 1_700_000_000_000, TTL: 3_000_000}
	identity := execution.StateKeyIdentity{Plan: plan, StateGeneration: "g1", SeriesIdentityDigest: member}
	normalAt := int64(1_700_000_040)
	for sampleRound(key, normalAt, 60) {
		normalAt += 60
	}
	sampledAt := normalAt
	for !sampleRound(key, sampledAt, 60) {
		sampledAt += 60
	}
	build := func(edit func(*segmentSlot, *segmentShadowPlan)) *segmentSlot {
		book, err := segment.NewBook([]segment.Entry{{Digest: member, Start: 60}})
		if err != nil {
			t.Fatal(err)
		}
		state := &segmentShadowPlan{key: key, book: book, hadMark: true, position: 240, interval: 60, retention: 5, ttl: 3_000_000_000_000,
			mark: segment.Mark{Position: 240, Pages: 1, Renewed: []segment.Renewal{renewal}, Sizes: []int{0}}}
		slot := &segmentSlot{plans: map[segment.PlanKey]*segmentShadowPlan{key: state},
			pieces: map[segmentPlanGeneration][]*segmentShadowPlan{{plan: plan, generation: "g1"}: {state}}, leaves: true, at: normalAt}
		if edit != nil {
			edit(slot, state)
		}
		return slot
	}
	coverage, covered := build(nil).Coverage(identity)
	if !covered || !coverage.Leaves || coverage.Renewal != renewal {
		t.Fatalf("an open member of a round that leaves members out: (%+v, %v), want it left out with its page's renewal", coverage, covered)
	}
	for name, edit := range map[string]func(*segmentSlot, *segmentShadowPlan){
		"a round that writes its members": func(slot *segmentSlot, _ *segmentShadowPlan) { slot.leaves = false },
		"the Plan's sampled round":        func(slot *segmentSlot, _ *segmentShadowPlan) { slot.at = sampledAt },
		"a stopped Plan":                  func(_ *segmentSlot, state *segmentShadowPlan) { state.stopped = true },
		"a Plan renewing no key":          func(_ *segmentSlot, state *segmentShadowPlan) { state.ttl = 0 },
		"a page written whole":            func(_ *segmentSlot, state *segmentShadowPlan) { state.fallback = map[int]bool{0: true} },
		"a closed member": func(_ *segmentSlot, state *segmentShadowPlan) {
			if err := state.book.Close(member, 120); err != nil {
				t.Fatal(err)
			}
		},
		"a carried generation's book": func(slot *segmentSlot, _ *segmentShadowPlan) {
			slot.carried = append(slot.carried, slot.plans[key])
			delete(slot.plans, key)
		},
	} {
		coverage, covered := build(edit).Coverage(identity)
		if !covered || coverage.Leaves || coverage.Renewal != (segment.Renewal{}) {
			t.Errorf("%s: (%+v, %v), want covered and written", name, coverage, covered)
		}
	}
	// The Plan's piece in its sampled round has its cached values checked,
	// and not in any other round.
	if slot := build(nil); slot.sampled(shadowDue, "g1") {
		t.Fatal("a round that is not the Plan's sampled round checks its cached values")
	}
	if slot := build(func(slot *segmentSlot, _ *segmentShadowPlan) { slot.at = sampledAt }); !slot.sampled(shadowDue, "g1") {
		t.Fatal("the Plan's sampled round does not check its cached values")
	}
	if (*segmentSlot)(nil).sampled(shadowDue, "g1") {
		t.Fatal("a Slot that read no segments checks its cached values")
	}
	// What a Slot's coverage says is what its round is told: under the word
	// its worker's view admits, a normal operation leaves members out and
	// every other writes them.
	due := shadowDue
	due.CompiledPlan = internalCompiledPlan(t, "9", 5, 60)
	for _, word := range []uint64{7, 0} {
		coordinator := &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: newMemorySegments()},
			InstalledControlEpoch: func() (uint64, bool) { return 7, true }}}
		for _, operation := range []execution.Operation{execution.OperationNormal, execution.OperationRetry, execution.OperationReplay} {
			request := shadowRequest(operation, normalAt)
			request.SegmentSkip = word
			slot, _, _ := coordinator.loadSegments(context.Background(), request, execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{due}})
			round := newSegmentShadowRound(request, slot)
			if slot == nil || round == nil || round.skip != slot.leaves || slot.leaves != (word != 0 && operation == execution.OperationNormal) {
				t.Fatalf("a %s under word %d: the round leaves members out %v, the coverage says %v", operation, word,
					round != nil && round.skip, slot != nil && slot.leaves)
			}
		}
	}
}
