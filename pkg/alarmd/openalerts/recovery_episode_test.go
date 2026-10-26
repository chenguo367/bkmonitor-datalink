// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The Levels of a two-Level strategy at a 60 s interval: warning recovers
// first while the info alert stays open; info's RECOVERY, which ends the
// alert, was measured up to 11 intervals later.
const (
	episodeWarning = uint32(2)
	episodeInfo    = uint32(3)
	episodeStep    = 60 * time.Second
)

// leveled is an envelope of kind for m whose Levels came to the results
// given, each with a trigger window of five 60 s positions ending now.
func leveled(kind string, at time.Time, results map[uint32]string) contract.TriggerEventV1 {
	return leveledSteps(kind, at, results, nil)
}

// leveledSteps is leveled with a Level's own interval where steps names
// one.
func leveledSteps(kind string, at time.Time, results map[uint32]string, steps map[uint32]time.Duration) contract.TriggerEventV1 {
	event := abnormal(keyA, "m9")
	event.EventKind = kind
	event.LevelResults = nil
	end := at.Unix()
	for level, result := range results {
		step := episodeStep
		if own, ok := steps[level]; ok {
			step = own
		}
		event.LevelResults = append(event.LevelResults, contract.LevelResultV1{LevelID: level, Priority: level, Result: result,
			DecisionWindow: contract.DecisionWindowV1{Trigger: contract.TriggerWindowEvidenceV1{
				WindowStart: end - 5*int64(step/time.Second) + 1, WindowEnd: end, WindowSize: 5}}})
	}
	sort.Slice(event.LevelResults, func(i, j int) bool { return event.LevelResults[i].LevelID < event.LevelResults[j].LevelID })
	return event
}

// episodeFixture is a trusted copy whose reconcile fails and whose set is
// read without m: the gate answers m from this process's record of its
// send alone, which only the episode lets go. started says whether the
// copy has run past a backstop since it was made, so an episode it sees
// begin is its own.
func episodeFixture(t *testing.T, started bool) *unlistedFixture {
	t.Helper()
	f := newUnlistedFixture(t)
	f.members = nil
	f.reconcile = func() (Reconciliation, error) { return Reconciliation{}, errors.New("reconcile: link answered 503") }
	f.round(0)
	if started {
		f.round(episodeBackstopIntervals*episodeStep + time.Minute)
	}
	return f
}

func (f *unlistedFixture) ack(t *testing.T, kind string, results map[uint32]string) {
	t.Helper()
	f.cache.Acknowledged([]contract.TriggerEventV1{leveled(kind, f.c.now(), results)})
}

func (f *unlistedFixture) passes() bool {
	return f.cache.Contains(keyA.TenantID, keyA.StrategyID, "m9")
}

// The sampled shape: warning recovers first and the RECOVERY goes on
// passing, an orphan each cycle while info stays open; info recovers 11
// intervals later and its RECOVERY passes, ending the alert; from then on
// every RECOVERY is held. An ABNORMAL after that starts the next episode.
func TestTheRecordOfASendIsLetGoOnceEveryLevelOfTheEpisodeRecovered(t *testing.T) {
	f := episodeFixture(t, true)
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal, episodeInfo: contract.LevelResultAbnormal})
	for cycle := 1; cycle <= 11; cycle++ {
		f.c.advance(episodeStep)
		if !f.passes() {
			t.Fatalf("cycle %d, info not recovered yet, the gate holds the RECOVERY (snapshot %+v)", cycle, f.cache.Snapshot(keyA))
		}
		if cycle < 11 {
			f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeWarning: contract.LevelResultRecovery, episodeInfo: contract.LevelResultNormal})
		}
	}
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery, episodeWarning: contract.LevelResultNormal})
	for cycle := 1; cycle <= 3; cycle++ {
		f.c.advance(episodeStep)
		if f.passes() {
			t.Fatalf("cycle %d after every Level recovered, the gate passes the RECOVERY, an orphan", cycle)
		}
	}
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal})
	f.c.advance(episodeStep)
	if !f.passes() {
		t.Fatal("an ABNORMAL after the release did not start an episode the gate passes")
	}
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeWarning: contract.LevelResultRecovery})
	f.c.advance(episodeStep)
	if f.passes() {
		t.Fatal("the next episode's only Level recovered and the gate still passes")
	}
}

// A RECOVERY at a Level the alert never stood at in this episode accounts
// for nothing: the gate goes on passing until the episode's own Level has
// recovered.
func TestARecoveryAtALevelTheEpisodeNeverHeldKeepsPassing(t *testing.T) {
	f := episodeFixture(t, true)
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal})
	for cycle := 1; cycle < episodeBackstopIntervals; cycle++ {
		f.c.advance(episodeStep)
		f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery, episodeWarning: contract.LevelResultNormal})
		if !f.passes() {
			t.Fatalf("cycle %d, warning not recovered, the gate holds", cycle)
		}
	}
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeWarning: contract.LevelResultRecovery})
	if f.passes() {
		t.Fatal("warning recovered and the gate still passes")
	}
}

// A Level that never recovers: the record is let go 22 of the strategy's
// intervals after the episode's first RECOVERY, not before.
func TestTheBackstopLetsGoOfAnEpisodeALevelNeverEnds(t *testing.T) {
	f := episodeFixture(t, true)
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal, episodeInfo: contract.LevelResultAbnormal})
	f.c.advance(episodeStep)
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeWarning: contract.LevelResultRecovery, episodeInfo: contract.LevelResultNormal})
	f.c.advance(episodeBackstopIntervals * episodeStep)
	if !f.passes() {
		t.Fatal("at the backstop the gate already holds")
	}
	f.c.advance(time.Second)
	if f.passes() {
		t.Fatal("past the backstop the gate still passes")
	}
}

// A process that did not see the episode begin cannot know its Levels: a
// copy just started (a restart mid-episode), and one whose record of the
// episode was let go while its record of the send stayed, let go only at
// the backstop, however many Levels then recover.
func TestAnEpisodeThisProcessDidNotSeeBeginWaitsForTheBackstop(t *testing.T) {
	t.Run("restarted", func(t *testing.T) {
		f := episodeFixture(t, false)
		f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeInfo: contract.LevelResultAbnormal})
		f.c.advance(episodeStep)
		f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery})
		f.c.advance(episodeStep)
		if !f.passes() {
			t.Fatal("a copy just started let go after the one Level it saw recovered")
		}
		f.c.advance(episodeBackstopIntervals * episodeStep)
		if f.passes() {
			t.Fatal("past the backstop the gate still passes")
		}
	})
	t.Run("record of the episode let go", func(t *testing.T) {
		f := episodeFixture(t, true)
		f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal, episodeInfo: contract.LevelResultAbnormal})
		f.cache.mu.Lock()
		delete(f.cache.episodes, member{key: keyA, fingerprint: "m9"})
		f.cache.mu.Unlock()
		f.c.advance(episodeStep)
		f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery, episodeWarning: contract.LevelResultRecovery})
		f.c.advance(episodeStep)
		if !f.passes() {
			t.Fatal("without the episode's ABNORMAL Levels in memory the gate let go before the backstop")
		}
	})
	t.Run("record of the episode let go, then an ABNORMAL", func(t *testing.T) {
		f := episodeFixture(t, true)
		f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeWarning: contract.LevelResultAbnormal, episodeInfo: contract.LevelResultAbnormal})
		f.cache.mu.Lock()
		delete(f.cache.episodes, member{key: keyA, fingerprint: "m9"})
		f.cache.mu.Unlock()
		f.c.advance(episodeStep)
		f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeInfo: contract.LevelResultAbnormal, episodeWarning: contract.LevelResultNormal})
		f.c.advance(episodeStep)
		f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery})
		f.c.advance(episodeStep)
		if !f.passes() {
			t.Fatal("an ABNORMAL seen after the episode's record was let go was taken as the episode's start")
		}
	})
}

// Letting go of the record of the send never overrides the link: when the
// link did not process the final RECOVERY and the alert is still active
// there, the next answer that carries it - the set's read, or a calibration
// naming it active - passes the RECOVERY again, so the alert cannot stick.
func TestTheLinksAnswerAfterTheReleaseStillPassesTheRecovery(t *testing.T) {
	f := episodeFixture(t, true)
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeInfo: contract.LevelResultAbnormal})
	f.c.advance(episodeStep)
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery})
	if f.passes() {
		t.Fatal("the release did not happen")
	}
	f.members = []string{"m9"}
	f.round(time.Hour + time.Minute)
	if !f.passes() {
		t.Fatalf("the set's read carries the alert and the gate holds (snapshot %+v)", f.cache.Snapshot(keyA))
	}
	f.members = nil
	f.reconcile = func() (Reconciliation, error) {
		return Reconciliation{EventSourceID: "source", Members: []string{"m9"}, Missing: []string{"m9"},
			Alerts: []Alert{{AlertID: "a9", EventSourceID: "source", Fingerprint: "m9"}}}, nil
	}
	f.round(time.Hour + time.Minute)
	if !f.passes() {
		t.Fatalf("a calibration names the alert active and the gate holds (snapshot %+v)", f.cache.Snapshot(keyA))
	}
}

// A Level that goes abnormal again after its RECOVERY owes another one: A
// and B abnormal, A recovers, A abnormal again, B recovers - not released,
// A's first RECOVERY no longer counts - then A recovers, released.
func TestALevelAbnormalAgainOwesAnotherRecovery(t *testing.T) {
	f := episodeFixture(t, true)
	a, b := episodeWarning, episodeInfo
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{a: contract.LevelResultAbnormal, b: contract.LevelResultAbnormal})
	f.c.advance(episodeStep)
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{a: contract.LevelResultRecovery, b: contract.LevelResultNormal})
	f.c.advance(episodeStep)
	f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{a: contract.LevelResultAbnormal, b: contract.LevelResultNormal})
	f.c.advance(episodeStep)
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{b: contract.LevelResultRecovery, a: contract.LevelResultNormal})
	f.c.advance(episodeStep)
	if !f.passes() {
		t.Fatal("B recovered while A, abnormal again, has not, and the gate let go")
	}
	f.ack(t, contract.TriggerEventRecovery, map[uint32]string{a: contract.LevelResultRecovery})
	f.c.advance(episodeStep)
	if f.passes() {
		t.Fatal("every Level recovered and the gate still passes")
	}
}

// Levels at different intervals: the backstop counts the longest. A at
// 60 s, B at 300 s, B never recovers: still passing 22 of A's intervals on,
// let go past 22 of B's.
func TestTheBackstopCountsTheLongestInterval(t *testing.T) {
	f := episodeFixture(t, true)
	a, b := episodeWarning, episodeInfo
	steps := map[uint32]time.Duration{b: 5 * episodeStep}
	ack := func(kind string, results map[uint32]string) {
		f.cache.Acknowledged([]contract.TriggerEventV1{leveledSteps(kind, f.c.now(), results, steps)})
	}
	ack(contract.TriggerEventAbnormal, map[uint32]string{a: contract.LevelResultAbnormal, b: contract.LevelResultAbnormal})
	f.c.advance(episodeStep)
	ack(contract.TriggerEventRecovery, map[uint32]string{a: contract.LevelResultRecovery, b: contract.LevelResultNormal})
	f.c.advance(episodeBackstopIntervals*episodeStep + time.Second)
	if !f.passes() {
		t.Fatal("past 22 of the shorter interval the gate let go, before 22 of the longer")
	}
	f.c.advance(episodeBackstopIntervals * 4 * episodeStep)
	if f.passes() {
		t.Fatal("past 22 of the longer interval the gate still passes")
	}
}

// An alert another replica opened, the series moved here mid-episode: at
// this process's first ABNORMAL of it, with no send of its own behind it,
// the link's own read already names it active. Its Levels before the move
// are not known here, so only the backstop lets it go. The same steps on
// an alert the link did not hold yet are this process's own episode,
// released once its Level recovers.
func TestAnEpisodeTheLinkAlreadyHeldWaitsForTheBackstop(t *testing.T) {
	for _, c := range []struct {
		name    string
		held    bool
		release bool
	}{{name: "opened by another replica", held: true}, {name: "opened here", release: true}} {
		t.Run(c.name, func(t *testing.T) {
			f := episodeFixture(t, true)
			read := func(members []string) {
				f.members = members
				f.cache.indexChanged(keyA)
				f.round(2 * time.Minute)
			}
			if c.held {
				read([]string{"m9"})
			}
			f.ack(t, contract.TriggerEventAbnormal, map[uint32]string{episodeInfo: contract.LevelResultAbnormal})
			read(nil)
			f.ack(t, contract.TriggerEventRecovery, map[uint32]string{episodeInfo: contract.LevelResultRecovery})
			f.c.advance(episodeStep)
			if got := !f.passes(); got != c.release {
				t.Fatalf("after the Level recovered, released %v, want %v", got, c.release)
			}
			f.c.advance(episodeBackstopIntervals * episodeStep)
			if f.passes() {
				t.Fatal("past the backstop the gate still passes")
			}
		})
	}
}
