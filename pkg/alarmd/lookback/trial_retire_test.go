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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// trialLeft is how the trial of a Slot is left before what ends it comes:
// never read, prepared past its point, or its read refused a permit. Each
// is counted by the outcome it came to first.
var trialLeft = []struct {
	name, outcome, path string
	leave               func(f *fixture, q Query)
}{
	{"undecided", EarlierOvertaken, OvertakenNextSlot, func(f *fixture, q Query) { f.engine.Prepare(q) }},
	{"prepared_late", EarlierOvertaken, OvertakenPreparedLate, func(f *fixture, q Query) {
		// 30 s past the candidate readiness point (baseline 60 s + h/2 60 s).
		f.clock.set(q.ReadyAt.Add(-30 * time.Second))
		f.engine.Prepare(q)
	}},
	{"permit_refused", EarlierPermitRefused, "", func(f *fixture, q Query) {
		f.engine.Prepare(q)
		trial := f.group("qg").prepared
		f.clock.set(trial.at)
		f.set(func() { f.refuse = "waiters" })
		f.engine.StepEarly(context.Background())
		waitTrialDone(f, trial)
		f.set(func() { f.refuse = "" })
	}},
}

// trialFixture is a fixture whose held reads reserve trials, and the first
// read of slice x of its held Slot.
func trialFixture(t *testing.T) (*fixture, Query) {
	f := newFixture(t)
	f.engine.options.OnEarlierRead = func(EarlierReadEvidence) {}
	q := heldQuery(f)
	q.Slice = sliceOf("^x$")
	return f, q
}

// nextSlot is the Slot after q's, its own slice x, its readiness a minute on.
func nextSlot(q Query) Query {
	next := q
	next.Contract.Slot.EvaluationTime += 60
	next.ReadyAt = q.ReadyAt.Add(time.Minute)
	return next
}

// countedOnce fails unless the trials counted are exactly one, of outcome,
// and, overtaken, by path alone.
func countedOnce(t *testing.T, f *fixture, outcome, path string) {
	t.Helper()
	reads := f.engine.Stats().Sources[sourceLog].EarlierReads
	total := uint64(0)
	for _, word := range EarlierReadOutcomes {
		total += reads[word]
	}
	if total != 1 || reads[outcome] != 1 {
		t.Fatalf("trials counted %v, want exactly one %s", reads, outcome)
	}
	paths := overtakenPaths(f)
	switch {
	case outcome == EarlierOvertaken && (len(paths) != 1 || paths[path] != 1):
		t.Fatalf("overtaken by %v, want by %s alone", paths, path)
	case outcome != EarlierOvertaken && len(paths) != 0:
		t.Fatalf("a %s trial was counted overtaken by %v", outcome, paths)
	}
}

// A trial whose Slot's formal read never came to take it - the Slot's first
// attempt failed before its query, and the retry is no first read - stood
// where every later sample of its group is taken, and the next Slot's
// Prepare, returning early on no hold or no candidate, did not end it: the
// group was never sampled again. The next Slot's first read ends it, counted
// once by the outcome it came to first, and is taken as the sample.
func TestATrialLeftByItsSlotDoesNotStopTheNextSlotsSample(t *testing.T) {
	for _, left := range trialLeft {
		for _, prepare := range []struct {
			skip string
			set  func(f *fixture)
		}{
			{EarlierSkipNoHold, func(f *fixture) {
				f.engine.options.CurrentReadHold = func(execution.QueryGroupIdentity) time.Duration { return 0 }
			}},
			{EarlierSkipNoCandidate, func(f *fixture) {
				f.engine.options.CurrentCandidate = func(execution.QueryGroupIdentity) (time.Duration, bool) { return 0, false }
			}},
		} {
			t.Run(left.name+"/"+prepare.skip, func(t *testing.T) {
				f, q := trialFixture(t)
				left.leave(f, q)
				retry := q
				retry.AttemptNo = 2
				f.clock.set(q.ReadyAt)
				if f.engine.Begin(retry) != nil {
					t.Fatal("a retry was seen as a first read")
				}
				next := nextSlot(q)
				prepare.set(f)
				f.engine.Prepare(next)
				if sources, _ := skippedBy(f); sources[prepare.skip] != 1 {
					t.Fatalf("the next Slot's Prepare skipped %v, want %s", sources, prepare.skip)
				}
				if reading, _ := f.engine.GroupReading("qg"); reading.TrialSlot != q.Contract.Slot.EvaluationTime {
					t.Fatalf("the trial of Slot %d stands at %d before the next Slot's read", q.Contract.Slot.EvaluationTime, reading.TrialSlot)
				}
				f.clock.set(next.ReadyAt)
				read := f.engine.Begin(next)
				read.Series(point(int64(next.Contract.Slot.EvaluationTime), "1"), 100)
				read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
				stats := f.engine.Stats()
				if got := stats.Sources[sourceLog].Samples[OutcomeCaptured]; got != 1 {
					t.Fatalf("the next Slot took %d samples, want 1", got)
				}
				if stats.Pending != 1 {
					t.Fatalf("pending %d, want the one sample in flight", stats.Pending)
				}
				reading, _ := f.engine.GroupReading("qg")
				if reading.TrialSlot != 0 || reading.Uncovered == UncoveredTrialBlocking || stats.Uncovered[UncoveredTrialBlocking] != 0 {
					t.Fatalf("trial Slot %d, uncovered %q, gauge %v: the earlier Slot's trial still stands",
						reading.TrialSlot, reading.Uncovered, stats.Uncovered)
				}
				countedOnce(t, f, left.outcome, left.path)
			})
		}
	}
}

// A trial whose slice's formal read is let go past its limit ends with it,
// by the outcome it came to first - except one overtaken only by that formal
// read, which is no read once let go: the slice is what ended it.
func TestATrialEndedByItsDiscardedSliceKeepsItsFirstOutcome(t *testing.T) {
	for _, left := range trialLeft {
		t.Run(left.name, func(t *testing.T) {
			f, q := trialFixture(t)
			left.leave(f, q)
			f.clock.set(q.ReadyAt)
			read := f.engine.Begin(q)
			if read.earlier == nil {
				t.Fatal("slice x's formal read did not take slice x's trial")
			}
			read.Discard()
			path := left.path
			if left.name == "undecided" {
				path = OvertakenSliceDiscarded
			}
			countedOnce(t, f, left.outcome, path)
			if reading, _ := f.engine.GroupReading("qg"); reading.TrialSlot != 0 {
				t.Fatalf("the discarded slice's trial still stands at %d", reading.TrialSlot)
			}
		})
	}
}

// What the next Slot's first read does not end: a trial reserved ahead for a
// later Slot, a trial of its own Slot another slice of which reads first, and
// a trial its formal read has taken and not yet compared.
func TestTheNextSlotsReadEndsOnlyATrialOfAnEarlierSlot(t *testing.T) {
	t.Run("reserved ahead", func(t *testing.T) {
		f := newFixture(t)
		f.engine.options.OnEarlierRead = func(EarlierReadEvidence) {}
		slot := f.clock.now().Unix()
		q := scheduledQuery(t, slot)
		// The first attempt a minute past where its trial had to start
		// reserves the next Slot's trial ahead.
		f.clock.set(q.ReadyAt.Add(-time.Minute))
		f.engine.Prepare(q)
		trial := preparedTrial(f)
		if trial == nil || !trial.ahead {
			t.Fatalf("reserved %+v, want the next Slot's trial ahead", trial)
		}
		f.clock.set(q.ReadyAt)
		read := f.engine.Begin(q)
		if read.sampled || preparedTrial(f) != trial || trial.done {
			t.Fatal("the reserving Slot's read ended the trial reserved ahead for the Slot after it")
		}
		if reads := f.engine.Stats().Sources[sourceTimeSeries].EarlierReads; reads[EarlierOvertaken] != 0 {
			t.Fatalf("trials counted %v", reads)
		}
	})
	t.Run("another slice first", func(t *testing.T) {
		f, q := trialFixture(t)
		f.engine.Prepare(q)
		trial := f.group("qg").prepared
		other := q
		other.Slice = sliceOf("^y$")
		f.clock.set(q.ReadyAt)
		if read := f.engine.Begin(other); read.earlier != nil || read.sampled {
			t.Fatal("slice y's formal read took slice x's trial")
		}
		if f.group("qg").prepared != trial {
			t.Fatal("slice y's formal read ended slice x's trial of the same Slot")
		}
		if read := f.engine.Begin(q); read.earlier != trial {
			t.Fatal("slice x's formal read did not take its trial after slice y's")
		}
	})
	t.Run("taken, not yet compared", func(t *testing.T) {
		f, q := trialFixture(t)
		f.engine.Prepare(q)
		trial := f.group("qg").prepared
		f.clock.set(trial.at)
		f.answers <- full(point(int64(q.Contract.Slot.EvaluationTime), "1"))
		f.engine.StepEarly(context.Background())
		f.waitFor(func(stats Stats) bool { return stats.PendingBytes == summaryEntryBytes })
		f.clock.set(q.ReadyAt)
		read := f.engine.Begin(q)
		if read.earlier != trial {
			t.Fatal("slice x's formal read did not take its trial")
		}
		read.Series(point(int64(q.Contract.Slot.EvaluationTime), "1"), 100)
		next := nextSlot(q)
		f.clock.set(next.ReadyAt)
		f.engine.Begin(next)
		read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
		countedOnce(t, f, EarlierEqual, "")
	})
}

// A group forgotten ends its trial once, by the outcome it came to first,
// owner_lost when it came to none; a trial its formal read had taken is
// counted by that read's Complete or Discard, still once.
func TestAForgottenGroupsTrialIsCountedOnceByItsFirstOutcome(t *testing.T) {
	for _, left := range trialLeft {
		if left.name == "prepared_late" {
			continue
		}
		t.Run(left.name, func(t *testing.T) {
			f, q := trialFixture(t)
			left.leave(f, q)
			f.set(func() { f.owned["qg"] = false })
			f.engine.Forget("qg", ForgottenLeaseLost)
			outcome := left.outcome
			if left.name == "undecided" {
				outcome = EarlierOwnerLost
			}
			countedOnce(t, f, outcome, "")
		})
	}
	for _, end := range []string{"complete", "discard"} {
		t.Run("taken/"+end, func(t *testing.T) {
			f, q := trialFixture(t)
			f.engine.Prepare(q)
			trial := f.group("qg").prepared
			f.clock.set(trial.at)
			f.answers <- full(point(int64(q.Contract.Slot.EvaluationTime), "1"))
			f.engine.StepEarly(context.Background())
			f.waitFor(func(stats Stats) bool { return stats.PendingBytes == summaryEntryBytes })
			f.clock.set(q.ReadyAt)
			read := f.engine.Begin(q)
			read.Series(point(int64(q.Contract.Slot.EvaluationTime), "1"), 100)
			f.set(func() { f.owned["qg"] = false })
			f.engine.Forget("qg", ForgottenLeaseLost)
			if end == "complete" {
				read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
			} else {
				read.Discard()
			}
			countedOnce(t, f, EarlierOwnerLost, "")
		})
	}
}

// A formal read that comes back in part compares nothing, and a trial it
// took keeps the outcome it came to first: one prepared past its point is
// overtaken so, and one the read overtook still running is overtaken by it -
// a read sent, if not whole. A trial that came to none is first_read_incomplete.
func TestAnIncompleteFormalReadKeepsItsTrialsFirstOutcome(t *testing.T) {
	for _, c := range []struct {
		name, outcome, path string
		leave               func(f *fixture, q Query)
	}{
		{"prepared_late", EarlierOvertaken, OvertakenPreparedLate, trialLeft[1].leave},
		{"overtaken by the read", EarlierOvertaken, OvertakenFormalFirst, func(f *fixture, q Query) { f.engine.Prepare(q) }},
		{"read and waiting", EarlierFirstIncomplete, "", func(f *fixture, q Query) {
			f.engine.Prepare(q)
			trial := f.group("qg").prepared
			f.clock.set(trial.at)
			f.answers <- full(point(int64(q.Contract.Slot.EvaluationTime), "1"))
			f.engine.StepEarly(context.Background())
			f.waitFor(func(stats Stats) bool { return stats.PendingBytes == summaryEntryBytes })
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, q := trialFixture(t)
			c.leave(f, q)
			f.clock.set(q.ReadyAt)
			read := f.engine.Begin(q)
			if read.earlier == nil {
				t.Fatal("the formal read took no trial")
			}
			read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessPartial}, nil)
			countedOnce(t, f, c.outcome, c.path)
		})
	}
}
