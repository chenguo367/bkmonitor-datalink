// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// recoveriesOf is the RECOVERY events among events.
func recoveriesOf(events []contract.TriggerEventV1) []contract.TriggerEventV1 {
	var found []contract.TriggerEventV1
	for _, event := range events {
		if event.EventKind == contract.TriggerEventRecovery {
			found = append(found, event)
		}
	}
	return found
}

// noDataEndKeys is the no-data end index keys on the fixture's server.
func (fixture *lifecycleFixture) noDataEndKeys() []string {
	fixture.t.Helper()
	keys, err := fixture.redis.Keys(context.Background(), "*:nodata-end:*").Result()
	if err != nil {
		fixture.t.Fatal(err)
	}
	return keys
}

// A time series written to end on no data (recovery-nodata), N = R = 1, two
// hosts over 80: both alerts open on round 1. Host B then stops reporting.
// Its alert ends (R + T + 1) = 3 periods after its last real point - round 4
// - but only on a round whose answer is complete: round 4 here is partial,
// and the end waits for the complete rounds after it. Between round 2 and 3
// the process is replaced by another on the same Redis, which ends it from
// the index the first one wrote: the entry, its time and the alert's
// identity are on the server, not in a process. Host A, still reporting and
// over the threshold, is not ended.
func TestAnAbsentSeriesWrittenToEndOnNoDataEndsAcrossARollOnCompleteRoundsOnly(t *testing.T) {
	strategy := lifecycleStrategy{revision: 7, noDataOff: true, statusSetter: "recovery-nodata"}
	fixture := startLifecycleFixture(t, strategy, nil)
	fixture.serve(false, 90, lifecycleHostA, lifecycleHostB)
	fixture.mustAttempt(1)
	openedA, openedB := fixture.thresholdEventsFor(lifecycleHostA), fixture.thresholdEventsFor(lifecycleHostB)
	if len(openedA) != 1 || len(openedB) != 1 || openedB[0].EventKind != contract.TriggerEventAbnormal {
		t.Fatalf("round 1 sent A %+v and B %+v, want one ABNORMAL each", openedA, openedB)
	}
	if keys := fixture.noDataEndKeys(); len(keys) != 1 {
		t.Fatalf("index keys after round 1 = %v, want the Plan's one", keys)
	}

	fixture.publishOpenAlert(openedA[0].DedupeMD5)
	fixture.publishOpenAlert(openedB[0].DedupeMD5)
	fixture.serve(false, 90, lifecycleHostA)
	fixture.mustAttempt(2)
	// A new owner reads the consumer's set once its first round has told its
	// copy which strategies it runs; until then nothing it reads about B is a
	// judgement, and nothing is decided.
	fixture.replace("alarmd-worker-1")
	fixture.mustAttempt(3)
	fixture.waitOpenAlertSetRead(3)
	if got := recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB)); len(got) != 0 {
		t.Fatalf("round 3 ended B %+v, two periods after its last point", got)
	}
	fixture.serve(true, 90, lifecycleHostA)
	fixture.mustAttempt(4)
	if got := recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB)); len(got) != 0 {
		t.Fatalf("round 4, partial, ended B %+v: an incomplete answer cannot say B is absent", got)
	}
	fixture.serve(false, 90, lifecycleHostA)
	ended := 0
	for round := int64(5); round <= 8 && ended == 0; round++ {
		fixture.mustAttempt(round)
		ended = len(recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB)))
	}
	ends := recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB))
	if len(ends) != 1 {
		t.Fatalf("B's ends after the partial round = %+v, want one", ends)
	}
	end := ends[0]
	if !end.RecordRef.Absent || end.DedupeMD5 != openedB[0].DedupeMD5 ||
		end.LevelResults[0].DecisionWindow.Recovery.EndedBy != contract.RecoveryEndedByNoData {
		t.Fatalf("B's end = %+v, want an absent RECOVERY ended by no data under B's alert", end)
	}
	if got := recoveriesOf(fixture.thresholdEventsFor(lifecycleHostA)); len(got) != 0 {
		t.Fatalf("A, reporting over the threshold, was ended: %+v", got)
	}
}

// waitOpenAlertJudged waits until the current process's copy of the open
// alert set judges one fingerprint as held or not, as wanted: a change the
// consumer published is read on the copy's own loop, which the bundle's
// clock paces, so the wait moves that clock on within round's half period.
func (fixture *lifecycleFixture) waitOpenAlertJudged(round int64, fingerprint string, held bool) {
	fixture.t.Helper()
	port, ok := fixture.bundle.workerPorts.OpenAlerts.(*openAlertCopyPort)
	if !ok {
		fixture.t.Fatalf("the open alert port is %T", fixture.bundle.workerPorts.OpenAlerts)
	}
	key := openalerts.StrategyKey{TenantID: "tenant-a", StrategyID: lifecycleStrategyID}
	attemptedAt := (fixture.evaluationAt(round) + fixture.interval/2) * 1000
	for deadline, step := testwait.Deadline(fixture.t), int64(1); ; step++ {
		if got, judged := port.cache.Holds(key, fingerprint); judged && got == held {
			return
		}
		if step < fixture.interval/2 {
			fixture.clock.Store(attemptedAt + step*1000)
		}
		time.Sleep(20 * time.Millisecond) // fixed-wait: the poll interval of a condition wait bounded by the test deadline
		if time.Now().After(deadline) {
			fixture.t.Fatalf("the open alert copy does not judge %s held=%v by the test's deadline: %+v", fingerprint, held, port.cache.Stats())
		}
	}
}

// An absent series whose alert the consumer no longer holds - closed by hand
// in the link - is dropped from the index at its due round and nothing is
// sent for it. Under an owner that did not send the alert, so the consumer's
// set is the only word on it: the process that sent an alert counts it held
// for a while whatever the set says, to cover the set's own lag.
func TestAnAbsentSeriesWhoseAlertIsNoLongerOpenIsDroppedNotEnded(t *testing.T) {
	strategy := lifecycleStrategy{revision: 7, noDataOff: true, statusSetter: "recovery-nodata"}
	fixture := startLifecycleFixture(t, strategy, nil)
	fixture.serve(false, 90, lifecycleHostA, lifecycleHostB)
	fixture.mustAttempt(1)
	openedB := fixture.thresholdEventsFor(lifecycleHostB)
	if len(openedB) != 1 {
		t.Fatalf("round 1 sent B %+v, want one ABNORMAL", openedB)
	}
	fixture.publishOpenAlert(openedB[0].DedupeMD5)
	fixture.serve(false, 90, lifecycleHostA)
	fixture.mustAttempt(2)
	fixture.replace("alarmd-worker-1")
	fixture.mustAttempt(3)
	fixture.waitOpenAlertJudged(3, openedB[0].DedupeMD5, true)
	fixture.withdrawOpenAlert(openedB[0].DedupeMD5)
	fixture.waitOpenAlertJudged(3, openedB[0].DedupeMD5, false)
	for round := int64(4); round <= 6; round++ {
		fixture.mustAttempt(round)
	}
	if got := recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB)); len(got) != 0 {
		t.Fatalf("B, closed by hand, was ended on no data: %+v", got)
	}
	fields, err := fixture.redis.HKeys(context.Background(), fixture.noDataEndKeys()[0]).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if len(field) > 0 && fieldNamesSeriesOf(fixture, field, openedB[0]) {
			t.Fatalf("B's entry %s is still indexed after its alert was closed", field)
		}
	}
}

// fieldNamesSeriesOf is whether an index field is the series an event was
// about.
func fieldNamesSeriesOf(_ *lifecycleFixture, field string, event contract.TriggerEventV1) bool {
	return len(field) > 64 && field[:64] == event.RecordRef.DimensionIdentityDigest
}

// A level written with disable_recovery ends nothing by itself: a host back
// under the threshold is not recovered, and one that stopped reporting is not
// ended on no data although the level is written recovery-nodata - the
// switch wins, and no no-data end index is kept for the Plan. Turned off
// again, the level recovers the host that came back.
func TestALevelWithRecoveryDisabledEndsNothingUntilTheSwitchIsOff(t *testing.T) {
	strategy := lifecycleStrategy{revision: 7, noDataOff: true, statusSetter: "recovery-nodata", disableRecovery: true}
	fixture := startLifecycleFixture(t, strategy, nil)
	fixture.serve(false, 90, lifecycleHostA, lifecycleHostB)
	fixture.mustAttempt(1)
	openedA, openedB := fixture.thresholdEventsFor(lifecycleHostA), fixture.thresholdEventsFor(lifecycleHostB)
	if len(openedA) != 1 || len(openedB) != 1 || openedA[0].EventKind != contract.TriggerEventAbnormal {
		t.Fatalf("round 1 sent A %+v and B %+v, want one ABNORMAL each", openedA, openedB)
	}
	fixture.publishOpenAlert(openedA[0].DedupeMD5)
	fixture.publishOpenAlert(openedB[0].DedupeMD5)
	// A under the threshold, B gone: with recovery on, A recovers on round 2
	// and B ends by round 5 (R+T+1 windows past its last point).
	fixture.serve(false, 10, lifecycleHostA)
	for round := int64(2); round <= 8; round++ {
		fixture.mustAttempt(round)
	}
	if got := append(recoveriesOf(fixture.thresholdEventsFor(lifecycleHostA)), recoveriesOf(fixture.thresholdEventsFor(lifecycleHostB))...); len(got) != 0 {
		t.Fatalf("a level with recovery disabled ended %+v", got)
	}
	if keys := fixture.noDataEndKeys(); len(keys) != 0 {
		t.Fatalf("index keys = %v, want none: no level ends on no data", keys)
	}

	strategy.revision, strategy.disableRecovery = 8, false
	first := fixture.change(strategy, 8)
	recovered := 0
	for round := int64(9); round <= first+6 && recovered == 0; round++ {
		fixture.mustAttempt(round)
		recovered = len(recoveriesOf(fixture.thresholdEventsFor(lifecycleHostA)))
	}
	if recovered != 1 {
		t.Fatalf("A recovered %d times within six rounds of recovery turned on (from round %d), want once", recovered, first)
	}
}

// The index serves only a deployment that can ask the consumer's open alert
// set, one with the link's Console. Without it no absent series' alert could
// ever be judged open, so no end would ever be sent, and every entry would be
// a write each round for nothing: no index is kept. The same strategy and
// rounds with the Console keep the Plan's one key.
func TestADeploymentWithoutTheLinkKeepsNoNoDataEndIndex(t *testing.T) {
	strategy := lifecycleStrategy{revision: 7, noDataOff: true, statusSetter: "recovery-nodata"}
	for name, test := range map[string]struct {
		configure func(*config.Config)
		keys      int
	}{
		"with the Console": {keys: 1},
		"without the Console": {configure: func(cfg *config.Config) {
			cfg.PhaseTwo.Linkd.ConsoleURL, cfg.PhaseTwo.Linkd.Username, cfg.PhaseTwo.Linkd.Password = "", "", ""
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := startLifecycleFixtureWith(t, strategy, nil, test.configure)
			fixture.serve(false, 90, lifecycleHostA, lifecycleHostB)
			fixture.mustAttempt(1)
			if opened := fixture.thresholdEventsFor(lifecycleHostB); len(opened) != 1 || opened[0].EventKind != contract.TriggerEventAbnormal {
				t.Fatalf("round 1 sent B %+v, want its ABNORMAL", opened)
			}
			fixture.serve(false, 90, lifecycleHostA)
			fixture.mustAttempt(2)
			if keys := fixture.noDataEndKeys(); len(keys) != test.keys {
				t.Fatalf("index keys after two rounds = %v, want %d", keys, test.keys)
			}
		})
	}
}
