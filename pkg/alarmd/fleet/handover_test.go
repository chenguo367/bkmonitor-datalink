// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// An object the rebalance moved away stops having rounds on the replica it
// left, and that is not the rounds failing to end. The fence's refusal is
// the moment this replica stops running it, so the stall clock stops there:
// read on, a deterministic defect the move carried away was marked stalled
// on the old owner while the new one had not yet listed it, and the first
// screen read it as a stall and then as fixed. The row keeps what it last
// said until the publisher forgets an object this replica no longer owns.
func TestOwnershipRejectionStopsTheStallClockAndKeepsTheRow(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	fail := func() {
		tracker.Observe(context.Background(), observability.Observation{
			ExecuteOutcome: "error", Err: errors.New("alarmd state: gap guard conflict: expected 41 got 43"),
			QueryFailure: &observability.QueryFailureFacts{Stage: "execute", Category: "completion_contract", Code: "GAP_GUARD_CONFLICT"},
			Trace:        observability.TraceFields{QueryGroupKey: "qg-moved", EvaluationTime: 1_700_000_000},
		})
	}
	for round := 0; round < DefaultDegradedRounds; round++ {
		fail()
	}
	before := tracker.Anomalies()
	if len(before) != 1 || before[0].FailingSince.IsZero() {
		t.Fatalf("rows before the move = %+v, want the failing object with its stall clock running", before)
	}
	// The fence refuses the next round: the object is somebody else's now.
	at.at = at.at.Add(time.Minute)
	tracker.Observe(context.Background(), observability.Observation{RunOutcome: "ownership_rejected",
		Trace: observability.TraceFields{QueryGroupKey: "qg-moved"}})
	after := tracker.Anomalies()
	if len(after) != 1 || !after[0].FailingSince.IsZero() {
		t.Fatalf("rows after the refusal = %+v, want the row kept with its stall clock stopped", after)
	}
	if after[0].Failure == nil || after[0].Failure.Code != "GAP_GUARD_CONFLICT" || after[0].LastError == nil || after[0].ReasonCode != "error" {
		t.Fatalf("row after the refusal = %+v, want what it last said kept", after[0])
	}
	// An hour later, still on this replica's list for want of a publish: not
	// stalled, because nothing here is running it.
	MarkStalled(after, now.Add(time.Hour), time.Minute)
	Attribute(after, now.Add(time.Hour))
	if after[0].Stalled || after[0].Finding.Check != CheckDefect {
		t.Fatalf("row an hour after the refusal = stalled %v under %s, want not stalled, under DEFECT", after[0].Stalled, after[0].Finding.Check)
	}
	// And a round that fails again on this replica -- the refusal was the
	// store, not a move -- starts the clock over from that round.
	at.at = at.at.Add(time.Minute)
	fail()
	if rows := tracker.Anomalies(); len(rows) != 1 || !rows[0].FailingSince.Equal(at.at) {
		t.Fatalf("rows after failing again = %+v, want the stall clock restarted at that round", rows)
	}
}

// A code the table files as this deployment's own defect is the line even
// when the object also stalls: a gap guard in conflict with itself stops
// the Slot, so the rounds stop ending too, and the defect is the fact to
// act on -- "restart the replica" is the wrong next step for a conflict that
// repeats until fixed. A stall with no defect code behind it is a stall.
func TestADefectCodeOutranksTheStall(t *testing.T) {
	defect := Anomaly{Kind: KindDegradedRun, ReasonCode: "error", Stalled: true, FailingSince: now.Add(-time.Hour),
		Failure:   &FailureRef{Stage: "execute", Category: "completion_contract", Code: "GAP_GUARD_CONFLICT"},
		LastError: &LastError{Text: "alarmd state: gap guard conflict: expected 41 got 43", At: now}}
	stalled := Anomaly{Kind: KindDegradedRun, ReasonCode: "error", Stalled: true, FailingSince: now.Add(-time.Hour),
		Failure:   &FailureRef{Stage: "execute", Category: "source_backend", Code: "QUERY_TIMEOUT"},
		LastError: &LastError{Text: "context deadline exceeded", At: now}}
	rows := []Anomaly{defect, stalled}
	Attribute(rows, now)
	if rows[0].Finding.Check != CheckDefect || rows[0].Finding.Group != "EVALUATE/NONE/CONTRACT" || !rows[0].Stalled {
		t.Fatalf("a stalled defect = %+v, want under DEFECT on its code, still marked stalled", rows[0].Finding)
	}
	if rows[1].Finding.Check != CheckRoundsStalled {
		t.Fatalf("a stalled timeout = %+v, want under ROUNDS_STALLED", rows[1].Finding)
	}
	// The same object before and after a change of owner reads the same:
	// the replica that lost it and the one that got it both file it as the
	// defect, so the line's count does not dip to zero between them.
	moved := defect
	moved.Stalled, moved.FailingSince = false, time.Time{}
	fresh := []Anomaly{moved}
	Attribute(fresh, now)
	if fresh[0].Finding.Check != CheckDefect || fresh[0].Finding.Group != rows[0].Finding.Group {
		t.Fatalf("the same defect on its new owner = %+v, want the same line and fold as on the old", fresh[0].Finding)
	}
}
