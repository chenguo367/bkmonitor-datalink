// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// decided builds an envelope as the evaluator publishes it: one result per
// Level, position i being Level i+1. A is ABNORMAL, R is RECOVERY and N is
// a Level that decided nothing this round (NORMAL, or held). The kind is
// the contract's aggregation: any ABNORMAL makes it ABNORMAL.
func decided(key StrategyKey, fingerprint, results string) contract.TriggerEventV1 {
	event := abnormal(key, fingerprint)
	event.EventKind = contract.TriggerEventRecovery
	event.LevelResults = nil
	for i, r := range results {
		result := contract.LevelResultNormal
		switch r {
		case 'A':
			result = contract.LevelResultAbnormal
			event.EventKind = contract.TriggerEventAbnormal
		case 'R':
			result = contract.LevelResultRecovery
		}
		level := uint32(i + 1)
		event.LevelResults = append(event.LevelResults, contract.LevelResultV1{LevelID: level, Priority: level, Result: result})
	}
	return event
}

// alertsAt is a calibration's alert list as a current Console gives it: one
// active alert per fingerprint, each named at severity.
func alertsAt(severity string, fingerprints ...string) []Alert {
	alerts := make([]Alert, 0, len(fingerprints))
	for _, fp := range fingerprints {
		alerts = append(alerts, Alert{AlertID: "alert-" + fp, EventSourceID: "s", Fingerprint: fp, Severity: severity})
	}
	return alerts
}

// What the consumer does with each envelope, in its own terms: it holds at
// most one active alert per fingerprint, at one severity. A resolved
// evaluation closes it only when its severity is the alert's; any other is
// recorded as an orphan and changes nothing. A more severe trigger moves
// the alert to that severity (the resolved one in the same message is
// superseded); a less severe one is suppressed. A close and a trigger in
// one message close the alert and open a new one at the trigger. The
// envelope's kind plays no part: the consumer reads the evaluations.
//
// Each case ends in one of two places, and both directions are asserted:
// the copy still holds the alert, or the copy has let it go.
var openLevelCases = []struct {
	name  string
	sends []string
	open  bool
}{
	// The review's case: the alert stands at Level 1 and a round recovers
	// only Level 2. Level 1's alert is still open.
	{"recovery_at_another_level", []string{"A", "NR"}, true},
	// The common one: the alert stands at Level 2, Level 1 is healthy and
	// reports RECOVERY every round, and Level 2 has stopped being abnormal
	// but is still inside its recovery window.
	{"healthy_level_recovers_beside_the_alert", []string{"RA", "RN"}, true},
	{"the_alerts_own_level_recovers", []string{"RA", "RR"}, false},
	{"single_level_recovers", []string{"A", "R"}, false},
	// Upgrade: Level 1 triggers while Level 2's recovery rides along; the
	// alert is now at Level 1, and Level 2 recovering again closes nothing.
	{"upgrade_supersedes_the_old_level", []string{"NA", "AR", "NR"}, true},
	{"upgraded_alert_recovers_at_its_new_level", []string{"NA", "AR", "RN"}, false},
	// Suppression: Level 2 triggers under an alert at Level 1, which stays
	// at Level 1.
	{"suppressed_lower_level_does_not_move_the_alert", []string{"A", "NA", "NR"}, true},
	{"suppressed_alert_recovers_at_its_own_level", []string{"A", "NA", "RN"}, false},
	// Close and reopen in one message: Level 1 recovers while Level 2
	// triggers. A new alert stands at Level 2.
	{"close_and_reopen_lands_on_the_lower_level", []string{"A", "RA", "RN"}, true},
	{"reopened_alert_recovers_at_the_lower_level", []string{"A", "RA", "RR"}, false},
	// Upgrade with nothing resolved beside it.
	{"plain_upgrade_moves_the_alert", []string{"NA", "AN", "NR"}, true},
	{"plain_upgrade_recovers_at_its_new_level", []string{"NA", "AN", "RN"}, false},
	// Two triggers in one message: the alert opens at the more severe.
	{"two_triggers_open_at_the_more_severe", []string{"AA", "NR"}, true},
	{"two_triggers_recover_at_the_more_severe", []string{"AA", "RN"}, false},
	// Level 4 has no name in the consumer's default table, so it cannot be
	// ordered against the others. These two are the copy's own rule, not
	// the consumer's: when it cannot tell where the alert stands, it does
	// not take a recovery for a close.
	{"unordered_triggers_leave_the_level_unknown", []string{"A", "NANA", "RNNN"}, true},
	{"unordered_upgrade_leaves_the_level_unknown", []string{"A", "NNNA", "RNNN"}, true},
}

// An alert whose ABNORMAL was sent while the record was full has no
// record, and so no severity. It is open in the consumer all the same, at
// a severity the copy never saw; once the record has room, a later
// trigger must not be taken as where the alert stands, or a recovery at
// that Level would close an alert that is still open at a higher one.
func TestAnAlertSentWhileTheRecordWasFullIsNotGivenALevelLater(t *testing.T) {
	c := &clock{at: time.Unix(1700000000, 0)}
	options := indexOptions(c)
	options.MaxLocalEntries = 2
	options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return nil, ErrIncomplete })
	cache := mustIndex(t, options)
	if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
		t.Fatal(err)
	}
	ack := func(fingerprint, results string) {
		c.advance(time.Second)
		cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, fingerprint, results)})
	}
	ack("a", "A")
	ack("b", "A")
	ack("fp", "A")
	if got := cache.Stats().OwnOpenRefusals; got != 1 {
		t.Fatalf("fixture: own-open refusals = %d, want the third alert refused", got)
	}
	ack("a", "R")
	if stats := cache.Stats(); stats.OwnOpen != 1 || !cache.Contains(tenant, keyA.StrategyID, "fp") {
		t.Fatalf("fixture: want the record to have room and the refused alert still sent, got own open %d", stats.OwnOpen)
	}
	ack("fp", "NA")
	ack("fp", "NR")
	if !cache.Contains(tenant, keyA.StrategyID, "fp") {
		t.Fatal("a Level 2 recovery closed an alert first sent at Level 1 while the record was full")
	}
}

// The set is unavailable the whole time, so the copy answers from what
// this process sent (the 2026-09-14 ruling: an alert this process opened
// lets its recoveries through). Before the fix any acknowledged RECOVERY
// envelope closed the alert in the copy whatever its Level, and every later
// recovery of the alert that was still open was held for good.
func TestARecoveryClosesTheCopysAlertOnlyAtTheLevelTheAlertStandsAt(t *testing.T) {
	for _, tc := range openLevelCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &clock{at: time.Unix(1700000000, 0)}
			options := indexOptions(c)
			options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return nil, ErrIncomplete })
			cache := mustIndex(t, options)
			if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
				t.Fatal(err)
			}
			for i, send := range tc.sends {
				c.advance(time.Minute)
				cache.Refresh(context.Background())
				cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", send)})
				if i < len(tc.sends)-1 && !cache.Contains(tenant, keyA.StrategyID, "fp") {
					t.Fatalf("fixture: after %q the alert is open in the consumer and the copy says it is not", tc.sends[:i+1])
				}
			}
			for minute := 0; minute <= 600; minute++ {
				if got := cache.Contains(tenant, keyA.StrategyID, "fp"); got != tc.open {
					t.Fatalf("after %q, minute %d: member %v, want %v", tc.sends, minute, got, tc.open)
				}
				c.advance(time.Minute)
				cache.Refresh(context.Background())
			}
		})
	}
}

// The set is available and calibrated, the alert is in it at Level 1, and
// the series keeps sending a Level-2-only RECOVERY every round the gate
// lets it through, as production would. The consumer keeps the alert open,
// so the copy must keep answering member. Before the fix the first ACK hid
// the alert, the read after the retention released it, the resend marked
// it resent, and it stayed hidden until the next calibration.
func TestAnAvailableSetIsNotOverruledByARecoveryAtAnotherLevel(t *testing.T) {
	for _, tc := range []struct {
		name     string
		severity string
		own      bool
	}{
		{"calibration_names_the_level", "critical", false},
		{"this_process_opened_it", "", true},
		// An older Console gives no severity and this process did not open
		// the alert: the copy cannot tell at which Level it stands, so it
		// does not claim a recovery closed it; the set answers.
		{"level_unknown", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &clock{at: time.Unix(1700000000, 0)}
			options := indexOptions(c)
			options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return []string{"fp"}, nil })
			options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
				return Reconciliation{Members: []string{"fp"}, Alerts: []Alert{{AlertID: "a", EventSourceID: "s", Fingerprint: "fp", Severity: tc.severity}}}, nil
			})
			cache := mustIndex(t, options)
			if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
				t.Fatal(err)
			}
			if tc.own {
				cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "A")})
			}
			cache.Refresh(context.Background())
			if !cache.Snapshot(keyA).Calibrated || !cache.Contains(tenant, keyA.StrategyID, "fp") {
				t.Fatalf("fixture: the set is not calibrated with the alert in it: %+v", cache.Snapshot(keyA))
			}
			for minute := 0; minute < 35; minute++ {
				cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "NR")})
				if !cache.Contains(tenant, keyA.StrategyID, "fp") {
					t.Fatalf("minute %d: a Level 2 recovery hid an alert open at Level 1", minute)
				}
				c.advance(time.Minute)
				cache.indexChanged(keyA)
				cache.Refresh(context.Background())
			}
			if got := cache.Stats().RecoveriesResent; got != 0 {
				t.Fatalf("recoveries resent = %d, want 0: none of these recoveries closed anything", got)
			}
		})
	}
}

// The other direction on an available set: a RECOVERY at the alert's own
// Level is a close, and the copy hides the alert through the publisher's
// lag exactly as before.
func TestARecoveryAtTheAlertsLevelStillHidesItThroughTheLag(t *testing.T) {
	for _, severity := range []string{"critical", ""} {
		t.Run(fmt.Sprintf("calibrated_severity_%q", severity), func(t *testing.T) {
			c := &clock{at: time.Unix(1700000000, 0)}
			options := indexOptions(c)
			options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return []string{"fp"}, nil })
			options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
				return Reconciliation{Members: []string{"fp"}, Alerts: []Alert{{AlertID: "a", EventSourceID: "s", Fingerprint: "fp", Severity: severity}}}, nil
			})
			cache := mustIndex(t, options)
			if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
				t.Fatal(err)
			}
			cache.Refresh(context.Background())
			if !cache.Snapshot(keyA).Calibrated || !cache.Contains(tenant, keyA.StrategyID, "fp") {
				t.Fatalf("fixture: the set is not calibrated with the alert in it: %+v", cache.Snapshot(keyA))
			}
			if severity == "" {
				// The set already carries the alert at a severity it does
				// not name; a Level 1 trigger puts it at Level 1 whatever
				// it stood at, since nothing outranks Level 1.
				cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "A")})
			}
			cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "RN")})
			if cache.Contains(tenant, keyA.StrategyID, "fp") {
				t.Fatal("a recovery at the alert's own Level did not hide it while the set catches up")
			}
		})
	}
}

// Against sets the Console has not confirmed the gate answers from what this
// process opened. A recovery at another Level must not take the alert out
// of that record; one at its own Level does.
func TestUnconfirmedSetsKeepAnAlertWhoseOtherLevelRecovered(t *testing.T) {
	for _, tc := range []struct {
		recovery string
		open     bool
	}{{"NR", true}, {"RN", false}} {
		t.Run(tc.recovery, func(t *testing.T) {
			facts := confirmedFacts()
			f := newSetFixture(t, PolicySelfMaintain, facts, "theirs")
			facts.set(true, false, true)
			f.cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "ours", "A")})
			f.reread(time.Minute)
			if f.cache.Trusted() {
				t.Fatal("fixture: the sets are trusted")
			}
			f.cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "ours", tc.recovery)})
			for minute := 0; minute < 60; minute++ {
				f.reread(time.Minute)
				if got := f.cache.Contains(tenant, keyA.StrategyID, "ours"); got != tc.open {
					t.Fatalf("after %q, minute %d: member %v, want %v", tc.recovery, minute, got, tc.open)
				}
			}
		})
	}
}

// The severity lookup is counted in the entry's byte budget: a calibration
// whose alerts name severities costs more than the same one without, by the
// lookup's slots, so a full budget refuses it rather than growing past it.
func TestTheSeverityLookupIsCountedInTheEntrysBytes(t *testing.T) {
	alerts := alertsAt("critical", "fp-1", "fp-2")
	bare := &indexEntry{alerts: alerts}
	_, without := entrySize(keyA, bare)
	named := &indexEntry{alerts: alerts, severities: map[string]string{"fp-1": "critical", "fp-2": "critical"}}
	_, with := entrySize(keyA, named)
	if with-without != 2*48 {
		t.Fatalf("bytes with the lookup = %d, without = %d, want 48 more per named alert", with, without)
	}
}

// A RECOVERY that closed nothing, for a series the copy holds no alert on,
// is an orphan at the consumer: it is not a recovery sent again, and it
// does not hide the series. Two in a row are still not counted as a resend.
func TestARecoveryThatClosedNothingIsNotCountedAsAResend(t *testing.T) {
	c := &clock{at: time.Unix(1700000000, 0)}
	options := indexOptions(c)
	options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return []string{"other"}, nil })
	options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
		return Reconciliation{Members: []string{"other"}, Alerts: alertsAt("critical", "other")}, nil
	})
	cache := mustIndex(t, options)
	if err := cache.SetTracked([]StrategyKey{keyA}); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())
	if !cache.Snapshot(keyA).Calibrated || cache.Contains(tenant, keyA.StrategyID, "fp") {
		t.Fatalf("fixture: want a calibrated set without the series, got %+v", cache.Snapshot(keyA))
	}
	cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "R")})
	c.advance(time.Second)
	cache.Acknowledged([]contract.TriggerEventV1{decided(keyA, "fp", "R")})
	if got := cache.Stats().RecoveriesResent; got != 0 {
		t.Fatalf("recoveries resent = %d, want 0: neither recovery closed an alert", got)
	}
}
