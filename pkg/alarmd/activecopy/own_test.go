// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package activecopy

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The record of this process's own opens holds exactly the alerts this
// process opened on the consumer's protocol: not one it did not open, not
// one on the Python-compatible format. A RECOVERY at the level the alert
// stands at ends it; one without that level keeps it for RecoveredGrace;
// an ABNORMAL after a RECOVERY opens the alert again. The evaluator-driven
// cases are in package trigger (open_alert_gate_own_test.go).
func TestOwnPassesOnlyWhatThisProcessOpenedUntilItsRecovery(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	own := NewOwn(func() time.Time { return now })
	event := func(kind, fingerprint string) contract.TriggerEventV1 {
		result := contract.LevelResultAbnormal
		if kind == contract.TriggerEventRecovery {
			result = contract.LevelResultRecovery
		}
		return contract.TriggerEventV1{EventKind: kind, TenantID: "t", DedupeMD5: fingerprint, PrimaryLevelID: 1,
			LevelResults: []contract.LevelResultV1{{LevelID: 1, Priority: 1, Result: result}},
			StrategyRef:  &contract.StrategySnapshotRef{}, PlanRef: contract.RuntimePlanRefV1{StrategyID: "1"}}
	}
	// A RECOVERY that does not carry the standing level 1.
	beside := event(contract.TriggerEventRecovery, "a")
	beside.LevelResults[0].Result = contract.LevelResultUnavailable
	legacy := event(contract.TriggerEventAbnormal, "compat")
	legacy.LegacyOutput = &contract.LegacyEventContext{}
	own.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventAbnormal, "a"), legacy})
	if !own.Contains("t", "1", "a") || own.Contains("t", "1", "b") || own.Contains("t", "1", "compat") || own.Contains("t", "2", "a") {
		t.Fatal("the record must hold exactly the alert this process opened on the consumer's protocol")
	}
	own.Acknowledged([]contract.TriggerEventV1{beside})
	now = now.Add(RecoveredGrace)
	if !own.Contains("t", "1", "a") {
		t.Fatal("within the grace after its first RECOVERY the alert still passes")
	}
	now = now.Add(time.Second)
	if own.Contains("t", "1", "a") {
		t.Fatal("past the grace after its RECOVERY the alert is let go")
	}
	own.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventAbnormal, "a")})
	own.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventRecovery, "a")})
	if own.Contains("t", "1", "a") {
		t.Fatal("a RECOVERY at the standing level ends the alert")
	}
	own.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventAbnormal, "a")})
	now = now.Add(2 * RecoveredGrace)
	if !own.Contains("t", "1", "a") {
		t.Fatal("an ABNORMAL after a RECOVERY opens the alert again, with no grace running")
	}
	// A strategy tracked again after a gap starts from nothing.
	own.Track(StrategyKey{TenantID: "t", StrategyID: "1"})
	now = now.Add(untrackedGap + time.Second)
	own.Track(StrategyKey{TenantID: "t", StrategyID: "1"})
	if own.Contains("t", "1", "a") || own.Known() != 0 {
		t.Fatal("a strategy tracked again after the gap must start from nothing")
	}
}

// What a process hands over with a Query Group is its record of the
// strategies asked, as it holds it - the standing level an ABNORMAL raised,
// the grace a RECOVERY without it started - and the next owner's record
// takes it so: the alert passes until the RECOVERY at its standing level, a
// grace runs on from where it was, and a strategy taken counts as tracked
// now, so a Track after the gap does not let it go. One the record holds
// from a later ABNORMAL is kept.
func TestOwnHandsOverItsRecordOfAStrategyAndTheNextOwnerTakesItAsHeld(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	event := func(kind, strategy, fingerprint string, level, priority uint32) contract.TriggerEventV1 {
		result := contract.LevelResultAbnormal
		if kind == contract.TriggerEventRecovery {
			result = contract.LevelResultRecovery
		}
		return contract.TriggerEventV1{EventKind: kind, TenantID: "t", DedupeMD5: fingerprint, PrimaryLevelID: level,
			LevelResults: []contract.LevelResultV1{{LevelID: level, Priority: priority, Result: result}},
			StrategyRef:  &contract.StrategySnapshotRef{}, PlanRef: contract.RuntimePlanRefV1{StrategyID: strategy}}
	}
	previous := NewOwn(clock)
	previous.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventAbnormal, "1", "a", 2, 2),
		event(contract.TriggerEventAbnormal, "1", "a", 1, 1), event(contract.TriggerEventAbnormal, "1", "b", 1, 1),
		event(contract.TriggerEventRecovery, "1", "b", 2, 2), event(contract.TriggerEventAbnormal, "2", "c", 1, 1)})
	records := previous.Export([]StrategyKey{{TenantID: "t", StrategyID: "1"}})
	if len(records) != 2 {
		t.Fatalf("exported %+v, want strategy 1's two alerts and not strategy 2's", records)
	}

	now = now.Add(time.Minute)
	next := NewOwn(clock)
	next.Track(StrategyKey{TenantID: "t", StrategyID: "1"})
	now = now.Add(untrackedGap + time.Second)
	if taken := next.Import(records); taken != 2 {
		t.Fatalf("took %d alerts, want 2", taken)
	}
	next.Track(StrategyKey{TenantID: "t", StrategyID: "1"})
	if !next.Contains("t", "1", "a") || !next.Contains("t", "1", "b") || next.Contains("t", "2", "c") {
		t.Fatal("the alerts taken must pass, and a strategy taken must count as tracked now")
	}
	// a stands at level 1 (the most severe it was raised to): a RECOVERY at
	// level 2 starts a grace and ends nothing, one at level 1 ends it.
	next.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventRecovery, "1", "a", 2, 2)})
	if !next.Contains("t", "1", "a") {
		t.Fatal("a RECOVERY without the standing level taken over ends nothing")
	}
	next.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventRecovery, "1", "a", 1, 1)})
	if next.Contains("t", "1", "a") {
		t.Fatal("a RECOVERY at the standing level taken over ends the alert")
	}
	// b's grace began a minute and the gap before it was taken: a second
	// short of its end here.
	now = now.Add(RecoveredGrace - untrackedGap - time.Minute - 2*time.Second)
	if !next.Contains("t", "1", "b") {
		t.Fatal("within the grace it was taken in, the alert still passes")
	}
	now = now.Add(2 * time.Second)
	if next.Contains("t", "1", "b") {
		t.Fatal("the grace taken over runs on from where it began")
	}

	// A later ABNORMAL the record holds is kept over the one taken.
	later := NewOwn(clock)
	later.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventAbnormal, "1", "a", 2, 2)})
	if taken := later.Import(records); taken != 1 {
		t.Fatalf("took %d alerts, want only b: a was sent here later", taken)
	}
	later.Acknowledged([]contract.TriggerEventV1{event(contract.TriggerEventRecovery, "1", "a", 2, 2)})
	if later.Contains("t", "1", "a") {
		t.Fatal("the record's own later ABNORMAL, at level 2, must be the one kept")
	}
}
