// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// A successful completion read in slices is written under its Query Group's
// repeated-line limit, with its usage - the slices' series, bytes, seams and
// reads ahead (015 O1): a successful round wrote no line, and a Query Group
// read in slices had none of these but on the rounds that failed. The same
// completion read whole is routine, counted and not written.
func TestASuccessfulCompletionReadInSlicesIsWrittenUnderItsGroupsLimit(t *testing.T) {
	var output bytes.Buffer
	limiter, _ := NewScopedLogLimiter(ScopedLogLimiterConfig{Window: time.Hour, MaxEvents: 1, MaxScopes: 1024})
	policy, _ := NewScopedBoundedLogPolicy(limiter)
	observer := NewLoggingObserver(New("alarmd", &output), policy)
	ctx := context.Background()
	for range 3 {
		observer.Observe(ctx, Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultSuccess,
			SlotBudgetUsage: &SlotBudgetUsageFacts{InputSlices: 25, Series: 2, RetainedInputBytes: 3432, QueryWaitMillis: 74000,
				QueryBodyMicros: 380, InputSliceHandoffMillis: 41000, InputSliceReadsInFlight: [6]uint64{4, 21}},
			Trace: TraceFields{QueryGroupKey: "qg-sliced"}})
	}
	for range 4 {
		observer.Observe(ctx, Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultSuccess,
			SlotBudgetUsage: &SlotBudgetUsageFacts{InputSlices: 1, Series: 900}, Trace: TraceFields{QueryGroupKey: "qg-whole"}})
	}
	counts := observer.LineCounts()
	written, limited := counts.Written[StageSlotCompleted], counts.Limited[StageSlotCompleted]
	if written != 1 || limited != 2 || counts.Unwritten[StageSlotCompleted] != 4 {
		t.Fatalf("slot_completed written %d limited %d unwritten %d, want the sliced group's first round written and the two "+
			"after it held back under its limit, and the four rounds read whole unwritten", written, limited, counts.Unwritten[StageSlotCompleted])
	}
	line := strings.TrimSpace(output.String())
	for _, want := range []string{`"stage":"slot_completed"`, `"query_group_key":"qg-sliced"`, `"input_slices":25`, `"series":2`,
		`"retained_input_bytes":3432`, `"query_body_micros":380`, `"input_slice_handoff_millis":41000`, `"input_slice_reads_in_flight":[4,21,0,0,0,0]`} {
		if !strings.Contains(line, want) {
			t.Fatalf("the sliced round's line lacks %s:\n%s", want, line)
		}
	}
	if strings.Contains(line, "qg-whole") {
		t.Fatalf("a round read whole was written:\n%s", line)
	}
}
