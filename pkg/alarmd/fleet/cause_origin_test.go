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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func degradedRowUnder(t *testing.T, carried *observability.CarriedReasonFacts) Anomaly {
	t.Helper()
	at := &clock{at: now}
	tracker := newTracker(t, at)
	for round := 0; round < DefaultDegradedRounds; round++ {
		tracker.Observe(context.Background(), observability.Observation{
			ProgressCompletionKind: "COMPLETED_WITH_UNAVAILABLE", ProgressCompletionCause: "LEVEL_OUTCOME_UNKNOWN",
			ProgressCompletionReason: "QUERY_TIMEOUT", CarriedReason: carried,
			Trace: observability.TraceFields{QueryGroupKey: "qg-307", EvaluationTime: int64(1_700_000_100 + 60*round)},
		})
		at.at = at.at.Add(time.Minute)
	}
	rows := tracker.Anomalies()
	if len(rows) != 1 || rows[0].Cause != "LEVEL_OUTCOME_UNKNOWN" || rows[0].CauseReason != "QUERY_TIMEOUT" {
		t.Fatalf("rows = %+v, want one LEVEL_OUTCOME_UNKNOWN/QUERY_TIMEOUT row", rows)
	}
	return rows[0]
}

// A row whose Level was held by a guard an earlier round's timeout raised is
// still a row - the series was not evaluated - but it says the timeout was
// carried and from where; a guard stamped before origins were recorded says
// carried with no origin, never physical; a timeout this round says
// physical.
func TestARowSaysWhetherItsCauseReasonWasCarried(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		carried *observability.CarriedReasonFacts
		want    CauseOrigin
	}{
		{name: "physical", want: CauseOrigin{Origin: observability.ReasonOriginPhysical}},
		{name: "carried with its origin", carried: &observability.CarriedReasonFacts{OriginSlot: 1_700_000_040, OriginCause: "QUERY_TIMEOUT"},
			want: CauseOrigin{Origin: observability.ReasonOriginCarried, Slot: 1_700_000_040, Cause: "QUERY_TIMEOUT"}},
		{name: "carried from before origins", carried: &observability.CarriedReasonFacts{},
			want: CauseOrigin{Origin: observability.ReasonOriginCarried}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			row := degradedRowUnder(t, testCase.carried)
			if row.CauseOrigin == nil || *row.CauseOrigin != testCase.want {
				t.Fatalf("cause origin %+v, want %+v", row.CauseOrigin, testCase.want)
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(encoded), `"cause_origin":{"origin":"`+testCase.want.Origin+`"`) {
				t.Fatalf("row JSON %s, want cause_origin.origin=%s", encoded, testCase.want.Origin)
			}
		})
	}
}
