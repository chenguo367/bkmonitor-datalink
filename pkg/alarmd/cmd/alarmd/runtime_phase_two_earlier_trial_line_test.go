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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

// unusedControl is a readhold.Control the line never reaches: a group the
// controller has not read is reported without a pending hold.
type unusedControl struct{ readhold.Control }

// A trial overtaken by a moved hold has the hold it was reserved under, the
// hold its Slot froze at and the hold pending on its earlier_trial line, so
// a descent a step per Slot is told apart from any other move without a
// second read; a trial ended otherwise has none of them.
func TestTheEarlierTrialLineOfAMovedHoldHasItsHolds(t *testing.T) {
	var out bytes.Buffer
	holds := &productionReadHolds{logger: observability.New("alarmd", &out)}
	var err error
	holds.controller, err = readhold.NewController(readhold.Options{Control: unusedControl{}, Prefix: "schedule", MaxHold: 10 * time.Minute,
		Owner: func(execution.QueryGroupIdentity) (readhold.Owner, error) { return readhold.Owner{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var options lookback.Options
	holds.bindLookback(&options)
	contract := execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "qg", EvaluationTime: 180}, ReadHoldMillis: 300_000}
	options.OnEarlierTrialEnd(lookback.EarlierTrialEnd{Contract: contract, Outcome: lookback.EarlierOvertaken, OvertakenBy: lookback.OvertakenHoldMoved,
		CandidateHold: 150 * time.Second, AheadSlots: 4, ReservedHold: 5 * time.Minute, FrozenHold: 9 * time.Minute})
	options.OnEarlierTrialEnd(lookback.EarlierTrialEnd{Contract: contract, Outcome: lookback.EarlierEqual, CandidateHold: 150 * time.Second})
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines %q, want two", lines)
	}
	for _, field := range []string{`"reserved_hold_ms":300000`, `"frozen_hold_ms":540000`, `"pending_hold_ms":-1`} {
		if !strings.Contains(lines[0], field) {
			t.Errorf("the hold_moved line %s lacks %s", lines[0], field)
		}
	}
	if strings.Contains(lines[1], "reserved_hold_ms") || strings.Contains(lines[1], "frozen_hold_ms") {
		t.Errorf("a matched trial's line %s has the moved hold's fields", lines[1])
	}
}

// A limit that cuts a group's hold by more Slots than a replay covers is
// logged once, with the holds and the Slots it is expected to skip; a cut a
// replay covers, or the same cut again, is not.
func TestALimitCutPastTheReplayIsLoggedOnce(t *testing.T) {
	var out bytes.Buffer
	holds := &productionReadHolds{logger: observability.New("alarmd", &out)}
	holds.cfg.PhaseTwo.Scheduler.MaxReplaySlots = 3
	cut := readhold.LimitCut{QueryGroup: "qg", Slot: 180, From: 10 * time.Minute, To: 5 * time.Minute, Period: time.Minute}
	holds.limitCut(cut)
	holds.limitCut(cut)
	holds.limitCut(readhold.LimitCut{QueryGroup: "covered", Slot: 180, From: 10 * time.Minute, To: 7 * time.Minute, Period: time.Minute})
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("lines %q, want one", lines)
	}
	for _, field := range []string{`"limit_cut"`, `"query_group":"qg"`, `"from_hold_ms":600000`, `"to_hold_ms":300000`,
		`"max_replay_slots":3`, `"expected_skipped_slots":2`} {
		if !strings.Contains(lines[0], field) {
			t.Errorf("the line %s lacks %s", lines[0], field)
		}
	}
}
