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
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// uncoveredFixture is one engine holding a group for each reason a group is
// not covered, and one covered group beside them, each built the way it comes
// about. Two of the states are set on the group directly: a group resting
// with a stale measurement needs a sample to end short of its rungs after one
// that measured, and a trial of a Slot before the group's latest is what the
// fixed Begin no longer leaves (TestATrialLeftByItsSlotDoesNotStopTheNextSlotsSample
// builds it the way it came about, and finds it gone).
func uncoveredFixture(t *testing.T) (*fixture, map[execution.QueryGroupIdentity]string) {
	f := newFixture(t)
	want := map[execution.QueryGroupIdentity]string{}
	own := func(name, reason string) Query {
		qg := execution.QueryGroupIdentity(name)
		f.set(func() { f.owned[qg] = true })
		if reason != "" {
			want[qg] = reason
		}
		return query(name, f.clock.now().Unix(), minute, sourceLog)
	}
	f.set(func() { f.owned = map[execution.QueryGroupIdentity]bool{} })

	// Covered: a sample just read to its end.
	own("covered", "")
	f.sample("covered", sourceLog, minute, "1", func(int) string { return "1" })
	// Owned here and never read by the lookback.
	own("not-seen", UncoveredNotSeen)
	// Its only first read came back in part.
	q := own("never-complete", UncoveredNeverComplete)
	f.engine.Begin(q).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil)
	// A sample taken and let go before it was compared with anything.
	q = own("unmeasured", UncoveredUnmeasured)
	f.engine.Begin(q).Discard()
	// Its first sample is spread past now.
	q = own("first-pending", UncoveredFirstSamplePending)
	f.engine.options.UnspreadFirstSamples = false
	f.engine.Begin(q).Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
	f.engine.options.UnspreadFirstSamples = true
	if next := f.group("first-pending").nextAt; !next.After(f.clock.now()) {
		t.Fatalf("first-pending's first sample is due at %v, not after now: pick another name", next)
	}
	// Measured a day ago; its latest sample ended short of its rungs, so it
	// rests without a fresh measurement.
	q = own("resting", UncoveredResting)
	withGroup(f, q, func(state *group) {
		state.captured, state.measured, state.measuredAt = true, true, f.clock.now().Add(-24*time.Hour)
		state.nextAt = f.clock.now().Add(time.Hour)
	})
	// Measured a day ago and rested since; its next first read has not come.
	q = own("awaiting", UncoveredAwaitingFirstRead)
	withGroup(f, q, func(state *group) {
		state.captured, state.measured, state.measuredAt = true, true, f.clock.now().Add(-24*time.Hour)
		state.nextAt = f.clock.now().Add(-time.Hour)
	})
	// A trial of the Slot before its latest still reserved.
	q = own("trial-blocking", UncoveredTrialBlocking)
	withGroup(f, q, func(state *group) {
		state.lastSlot = q.Contract.Slot.EvaluationTime + 60
		state.prepared = &earlierSample{query: q, source: sourceLog, group: state, done: true, outcome: EarlierPermitRefused}
	})
	return f, want
}

// Every owned group not covered is counted under exactly one reason, the
// reasons adding up to owned less covered; the same reasons name each group
// on Uncovered and on its group reading.
func TestEveryUncoveredGroupIsCountedUnderOneReason(t *testing.T) {
	f, want := uncoveredFixture(t)
	stats := f.engine.Stats()
	coverage := stats.Coverage
	if coverage.Owned != len(want)+1 || coverage.Covered != 1 {
		t.Fatalf("owned %d covered %d, want %d and the one covered group", coverage.Owned, coverage.Covered, len(want)+1)
	}
	sum := 0
	for _, reason := range UncoveredReasons {
		count, present := stats.Uncovered[reason]
		if !present {
			t.Errorf("reason %s is not reported", reason)
		}
		if count != 1 {
			t.Errorf("%d groups under %s, want the one built for it", count, reason)
		}
		sum += count
	}
	if sum != coverage.Owned-coverage.Covered || len(stats.Uncovered) != len(UncoveredReasons) {
		t.Fatalf("uncovered %v adds up to %d, want owned %d less covered %d", stats.Uncovered, sum, coverage.Owned, coverage.Covered)
	}

	owned := []execution.QueryGroupIdentity{"covered"}
	for qg := range want {
		owned = append(owned, qg)
	}
	named := map[execution.QueryGroupIdentity]string{}
	for _, group := range f.engine.Uncovered(owned) {
		named[group.QueryGroup] = group.Reason
		if group.Reason == UncoveredNotSeen && (!group.Since.IsZero() || !group.NextAt.IsZero()) {
			t.Errorf("%s is not seen and has a since or a next sample: %+v", group.QueryGroup, group)
		}
	}
	for qg, reason := range want {
		if named[qg] != reason {
			t.Errorf("Uncovered names %s %q, want %q", qg, named[qg], reason)
		}
		reading, ok := f.engine.GroupReading(qg)
		switch {
		case reason == UncoveredNotSeen && ok:
			t.Errorf("%s has a group reading without state", qg)
		case reason != UncoveredNotSeen && (!ok || reading.Uncovered != reason):
			t.Errorf("%s reads %q on its group page, want %q", qg, reading.Uncovered, reason)
		}
	}
	if _, listed := named["covered"]; listed {
		t.Error("the covered group is named uncovered")
	}
	if reading, _ := f.engine.GroupReading("covered"); reading.Uncovered != "" {
		t.Errorf("the covered group's page says %q", reading.Uncovered)
	}
	if reading, _ := f.engine.GroupReading("trial-blocking"); reading.TrialSlot != execution.EvaluationTime(f.clock.now().Unix()) {
		t.Errorf("trial-blocking's page names trial Slot %d, want its reserved one", reading.TrialSlot)
	}
	if reading, _ := f.engine.GroupReading("resting"); !reading.NextAt.Equal(f.clock.now().Add(time.Hour)) {
		t.Errorf("resting's page says its next sample is at %v", reading.NextAt)
	}
}

// A group taken away between the two reads of ownership is not counted
// unseen below zero, and a covered engine counts no group uncovered.
func TestNoUncoveredReasonIsCountedForACoveredOrAnEmptyEngine(t *testing.T) {
	f := newFixture(t)
	f.set(func() { f.owned = map[execution.QueryGroupIdentity]bool{"qg": true} })
	f.sample("qg", sourceLog, minute, "1", func(int) string { return "1" })
	for reason, count := range f.engine.Stats().Uncovered {
		if count != 0 {
			t.Errorf("%d groups under %s with every owned group covered", count, reason)
		}
	}
	// Owned says none while the engine still holds one: nothing unseen.
	f.set(func() { f.owned = map[execution.QueryGroupIdentity]bool{} })
	if got := f.engine.Stats().Uncovered[UncoveredNotSeen]; got != 0 {
		t.Errorf("not_seen %d with no group owned", got)
	}
	// The Runner set moves between its two reads: Owned counts none, and Owns
	// still says the group the engine holds is here. Not one unseen below
	// zero.
	f.set(func() { f.owned = map[execution.QueryGroupIdentity]bool{"qg": true} })
	f.engine.options.Owned = func() int { return 0 }
	if got := f.engine.Stats().Uncovered[UncoveredNotSeen]; got != 0 {
		t.Errorf("not_seen %d with the Runner set read at none and the group still owned", got)
	}
}

// A group's state forgotten is counted once by why, a reason not named as
// not_owned; a group with no state to drop is not counted.
func TestAForgottenGroupIsCountedByWhyOnlyWhenItHadState(t *testing.T) {
	f := newFixture(t)
	forgotten := func() map[string]uint64 { return f.engine.Stats().GroupsForgotten }
	for _, reason := range ForgottenReasons {
		if count, present := forgotten()[reason]; !present || count != 0 {
			t.Fatalf("%s starts at %d (reported %v)", reason, count, present)
		}
	}
	f.engine.Forget("qg", ForgottenMoved)
	if got := forgotten()[ForgottenMoved]; got != 0 {
		t.Fatalf("a group never seen counted %d forgotten", got)
	}
	f.engine.Begin(query("qg", f.clock.now().Unix(), minute, sourceLog)).Discard()
	f.engine.Forget("qg", ForgottenMoved)
	f.engine.Forget("qg", ForgottenMoved)
	if got := forgotten()[ForgottenMoved]; got != 1 {
		t.Fatalf("moved %d, want the one group with state", got)
	}
	f.engine.Begin(query("qg-b", f.clock.now().Unix(), minute, sourceLog)).Discard()
	f.engine.Forget("qg-b", "renamed")
	if got := forgotten(); got[ForgottenNotOwned] != 1 || len(got) != len(ForgottenReasons) {
		t.Fatalf("an unnamed reason was counted as %v, want not_owned", got)
	}
}

// The lookback's own forgetting - a group found no longer this process's as
// it went to read it - says not_owned at each place it happens.
func TestTheLookbackForgetsWhatItFindsUnownedAsNotOwned(t *testing.T) {
	calls := 0
	for _, name := range []string{"engine.go", "early.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.Contains(line, "engine.Forget(") {
				continue
			}
			calls++
			if !strings.Contains(line, ", ForgottenNotOwned)") {
				t.Errorf("%s: %s, want not_owned", name, strings.TrimSpace(line))
			}
		}
	}
	if calls != 3 {
		t.Errorf("%d calls of Forget in the lookback, want its 3", calls)
	}
}

// A group whose only sample ended unobserved - its last rung not read - rests
// with no measurement: it is unmeasured, not resting, though its next sample
// is not due. Read under the rest, it would be told apart from a group whose
// measurement merely went stale by nothing.
func TestASampleEndedUnobservedLeavesItsGroupUnmeasuredNotResting(t *testing.T) {
	f := newFixture(t)
	f.set(func() { f.owned = map[execution.QueryGroupIdentity]bool{"qg": true} })
	slot := f.clock.now().Unix()
	f.capture(query("qg", slot, minute, sourceLog), point(slot, "1"))
	readAt := f.clock.now()
	f.recheck(sourceLog, readAt, 0, minute, full(point(slot, "2")), RecheckCompared)
	f.clock.set(readAt.Add(rungDelay(1, minute) + rungWindow(1, minute) + time.Second))
	f.engine.Step(context.Background())
	stats := f.engine.Stats()
	if stats.Sources[sourceLog].Samples[OutcomeUnobserved] != 1 {
		t.Fatalf("samples %v, want the one ended unobserved", stats.Sources[sourceLog].Samples)
	}
	if next := f.group("qg").nextAt; !next.After(f.clock.now()) {
		t.Fatalf("its next sample is due at %v, not after now", next)
	}
	if stats.Uncovered[UncoveredUnmeasured] != 1 || stats.Uncovered[UncoveredResting] != 0 {
		t.Fatalf("uncovered %v, want it unmeasured", stats.Uncovered)
	}
}
