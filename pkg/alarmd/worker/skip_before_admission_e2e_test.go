// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// AdmitRuntime records what the State admission is asked about and refuses
// the series armed for it as a record past the value limit, the rest
// admitted by the store.
func (store *lateLandingStore) AdmitRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateAdmissionResult, error) {
	store.mu.Lock()
	refuse := map[execution.StateKeyIdentity]bool{}
	for _, item := range request.Items {
		store.admitted = append(store.admitted, item.Identity)
		if store.refuse[item.Identity.SeriesIdentityDigest] {
			refuse[item.Identity] = true
		}
	}
	store.mu.Unlock()
	result, err := store.ExecutionStore.AdmitRuntime(ctx, request)
	if err != nil || len(refuse) == 0 {
		return result, err
	}
	for index, item := range result.Items {
		if refuse[item.Identity] {
			result.Items[index] = execution.StateAdmissionItemResult{Identity: item.Identity,
				Status: execution.StateAdmissionDeterministicInvalid, ReasonCode: execution.ReasonCode(contract.ReasonStateBudgetExceeded),
				RefusalText: "the record is past the value limit"}
		}
	}
	return result, nil
}

// skipLedger is what a round of the rig is judged by: the census's classes,
// written and not written, by reason; the shadow's matches, the members it
// left and the matches counted under a gate; what the State admission was
// asked about, by its observations and as the store saw it.
type skipLedger struct {
	classes, written, notWritten, skipped, withheld, rejected int64
	matches, gates                                            int64
	admissionKeys                                             int64
	admitted                                                  int
}

func (rig *shadowRig) skipLedger() skipLedger {
	rig.t.Helper()
	matches, left, gates, _ := rig.observed.matchLedger()
	ledger := skipLedger{matches: matches, skipped: left}
	for _, count := range gates {
		ledger.gates += count
	}
	rig.observed.mu.Lock()
	for _, count := range rig.observed.classes {
		ledger.classes += count
	}
	for _, count := range rig.observed.notWritten {
		ledger.notWritten += count
	}
	ledger.withheld = rig.observed.notWritten[observability.StateNotWrittenOutputWithheld]
	ledger.rejected = rig.observed.notWritten[observability.StateNotWrittenAdmissionRejected]
	ledger.written, ledger.admissionKeys = rig.observed.written, rig.observed.admissionKeys
	rig.observed.mu.Unlock()
	rig.store.mu.Lock()
	ledger.admitted = len(rig.store.admitted)
	rig.store.mu.Unlock()
	return ledger
}

func (ledger skipLedger) minus(before skipLedger) skipLedger {
	return skipLedger{classes: ledger.classes - before.classes, written: ledger.written - before.written,
		notWritten: ledger.notWritten - before.notWritten, skipped: ledger.skipped - before.skipped,
		withheld: ledger.withheld - before.withheld, rejected: ledger.rejected - before.rejected,
		matches: ledger.matches - before.matches, gates: ledger.gates - before.gates,
		admissionKeys: ledger.admissionKeys - before.admissionKeys, admitted: ledger.admitted - before.admitted}
}

// sampledAfter is the first evaluation time from at on that is the rig's
// Plan's sampled round of the day.
func sampledAfter(t *testing.T, at execution.EvaluationTime) execution.EvaluationTime {
	t.Helper()
	for step := 0; step < 2*1440; step++ {
		if worker.SegmentSampledRound(planIdentity(), "state-v1", int64(at), 60) {
			return at
		}
		at += 60
	}
	t.Fatal("no sampled round in two days of rounds")
	return 0
}

// A member the round leaves out is neither admitted nor written (014 n8
// (i)): through Execute on the real State store, round after round with the
// skip word, hosts going quiet, changing and away - every mutation classed
// once and either written or not written by a named reason, the members left
// noted as the shadow's matches once each, and the State admission asked
// about the written alone, by its observations and as the store saw it.
func TestAMemberTheRoundLeavesIsNeitherAdmittedNorWritten(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	left := int64(0)
	for round := 0; round < 14; round++ {
		at += 60
		var absent []string
		value := map[string]string{}
		switch round % 5 {
		case 3:
			value[fmt.Sprintf("host-%02d", round%5)] = "10"
		case 4:
			absent = []string{"host-01"}
		}
		before := rig.skipLedger()
		if completed, err := rig.run(at, absent, value, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
		d := rig.skipLedger().minus(before)
		if d.classes != d.written+d.notWritten {
			t.Fatalf("Slot at %d: %d classed, %d written and %d not: the census does not settle", at, d.classes, d.written, d.notWritten)
		}
		if d.matches != d.skipped+d.gates {
			t.Fatalf("Slot at %d: %d matches against %d left and %d under a gate: a member left noted more than once", at,
				d.matches, d.skipped, d.gates)
		}
		if d.admissionKeys != d.written || int64(d.admitted) != d.written {
			t.Fatalf("Slot at %d: admission asked about %d (%d by the store) of %d written, %d left: a member left was admitted",
				at, d.admissionKeys, d.admitted, d.written, d.skipped)
		}
		left += d.skipped
	}
	if left == 0 {
		t.Fatal("no round left a member")
	}
}

// A Plan whose output does not land, members its round would leave among
// its series, counts every series withheld - those admitted and those left,
// as before admission left them out - and the shadow notes none of them.
func TestAPlanWhoseOutputDoesNotLandCountsItsLeftMembersWithheld(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 8 && rig.skippedSoFar() == 0; round++ {
		at += 60
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	if rig.skippedSoFar() == 0 {
		t.Fatal("no round left a member")
	}
	at += 60
	for worker.SegmentSampledRound(planIdentity(), "state-v1", int64(at), 60) {
		at += 60
	}
	// host-02 sends an event, and its event batch is not acknowledged.
	rig.ports.failEventAckAt = rig.ports.eventAckCalls + 1
	before := rig.skipLedger()
	if completed, _ := rig.run(at, nil, map[string]string{"host-02": "10"}, false); completed {
		t.Fatal("the Slot completed with its output unacknowledged")
	}
	d := rig.skipLedger().minus(before)
	if d.withheld != 5 || d.written != 0 || d.skipped != 0 || d.matches != 0 {
		t.Fatalf("withheld %d, written %d, left %d, matches %d; want every series withheld and none noted", d.withheld, d.written,
			d.skipped, d.matches)
	}
}

// A Plan's sampled round of the day leaves no member: every series is
// admitted and written, and compared on the shadow as the rounds before the
// skip compared it.
func TestASampledRoundLeavesNoMemberAndAdmitsEveryWrite(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	sampled := sampledAfter(t, frozenContract().Slot.EvaluationTime+9*60)
	for at := sampled - 8*60; at < sampled; at += 60 {
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	if rig.skippedSoFar() == 0 {
		t.Fatal("no round before the sampled one left a member")
	}
	before := rig.skipLedger()
	if completed, err := rig.run(sampled, nil, nil, false); !completed {
		t.Fatalf("the sampled Slot: completed=%t err=%v", completed, err)
	}
	d := rig.skipLedger().minus(before)
	if d.skipped != 0 || d.admitted != 5 || d.written != 5 || d.admissionKeys != 5 {
		t.Fatalf("the sampled round left %d, admitted %d (%d observed) and wrote %d; want none left, all five admitted and written",
			d.skipped, d.admitted, d.admissionKeys, d.written)
	}
}

// A Plan whose quiet series send events every round - its recovery walk
// deciding each one - leaves none of them: a series with events is no
// member's, and the Plan's event batches are those of a rig that never
// skips, batch for batch.
func TestAPlanWhoseSeriesSendEventsLeavesNoneAndSendsWhatARigWithoutTheSkipSends(t *testing.T) {
	recovering := `{"enabled":true,"consecutive_windows":1}`
	skipping := newShadowRigWith(t, 5, 0, recovering, 0, nil, 16<<20)
	skipping.skipWord.Store(7)
	writing := newShadowRigWith(t, 5, 0, recovering, 0, nil, 16<<20)
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 10; round++ {
		at += 60
		value := map[string]string{}
		if round == 6 {
			value["host-02"] = "10"
		}
		for _, rig := range []*shadowRig{skipping, writing} {
			if completed, err := rig.run(at, nil, value, false); !completed {
				t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
			}
		}
	}
	if !reflect.DeepEqual(skipping.ports.eventBatches, writing.ports.eventBatches) {
		t.Fatalf("event batches with the skip %v, without %v", skipping.ports.eventBatches, writing.ports.eventBatches)
	}
	if len(skipping.ports.eventBatches) == 0 {
		t.Fatal("the Plan sent no events")
	}
	if left := skipping.skippedSoFar(); left != 0 {
		t.Fatalf("%d series with events left", left)
	}
}

// A member whose new record is past the value limit - a limit lowered under
// it - is left all the same while the round leaves it: its old record was
// admitted when it was written, and the Plan goes on. Its Plan's sampled
// round admits every series and refuses it by name.
func TestAMemberPastTheValueLimitIsLeftUntilItsSampledRoundRefusesIt(t *testing.T) {
	rig := newShadowRigWith(t, 5, 0, shadowRecoveryOff, 0, nil, 16<<20)
	rig.skipWord.Store(7)
	sampled := sampledAfter(t, frozenContract().Slot.EvaluationTime+12*60)
	at := sampled - 11*60
	for ; at < sampled-3*60; at += 60 {
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d: completed=%t err=%v", at, completed, err)
		}
	}
	if rig.skippedSoFar() == 0 {
		t.Fatal("no round left a member")
	}
	// Every series past the limit from here on.
	rig.store.mu.Lock()
	rig.store.refuse = map[execution.SeriesIdentityDigest]bool{}
	for _, host := range []string{"host-00", "host-01", "host-02", "host-03", "host-04"} {
		rig.store.refuse[hostDigest(t, host)] = true
	}
	rig.store.mu.Unlock()
	before := rig.skipLedger()
	for ; at < sampled; at += 60 {
		if completed, err := rig.run(at, nil, nil, false); !completed {
			t.Fatalf("Slot at %d past the limit: completed=%t err=%v", at, completed, err)
		}
	}
	d := rig.skipLedger().minus(before)
	if d.skipped == 0 || d.rejected != d.classes-d.skipped {
		t.Fatalf("before the sampled round %d left and %d refused of %d; want the members left and only the rest refused",
			d.skipped, d.rejected, d.classes)
	}
	before = rig.skipLedger()
	_, _ = rig.run(sampled, nil, nil, false)
	if d := rig.skipLedger().minus(before); d.skipped != 0 || d.rejected != 5 {
		t.Fatalf("the sampled round left %d and refused %d; want every series admitted and refused", d.skipped, d.rejected)
	}
}

// hostDigest is the series identity digest of the rig's host.
func hostDigest(t *testing.T, host string) execution.SeriesIdentityDigest {
	t.Helper()
	digest, err := contract.DeriveDimensionIdentityDigestV2("tenant", "2", ringRatioFields(host))
	if err != nil {
		t.Fatal(err)
	}
	return execution.SeriesIdentityDigest(digest)
}
