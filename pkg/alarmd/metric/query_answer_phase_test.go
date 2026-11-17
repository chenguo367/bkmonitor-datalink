// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"math"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A query whose response's headers came back has its seconds split by
// phase - the wait before them and the body after - under its outcome, an
// upstream 504 among them; a query no response came back to is in neither.
// Every outcome's two phases are there before any query.
func TestAnAnsweredQuerysSecondsAreSplitByPhaseUnderItsOutcome(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	read := func() map[[2]string][2]float64 {
		cells := map[[2]string][2]float64{}
		for _, m := range gatherFamily(t, r, "alarmd_access_query_answer_phase_seconds") {
			labels := map[string]string{}
			for _, label := range m.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			cells[[2]string{labels["outcome"], labels["phase"]}] = [2]float64{float64(m.GetHistogram().GetSampleCount()), m.GetHistogram().GetSampleSum()}
		}
		return cells
	}
	if cells := read(); len(cells) != len(observability.PhysicalQueryOutcomes)*2 {
		t.Fatalf("before any query: %d cells, want every outcome's wait and body", len(cells))
	}
	r.RecordPhysicalQueryAnswer(observability.PhysicalQueryAnswer{Outcome: observability.PhysicalQueryOK, Elapsed: 4500 * time.Millisecond,
		Answered: true, Wait: 3 * time.Second, Body: 1500 * time.Millisecond})
	r.RecordPhysicalQueryAnswer(observability.PhysicalQueryAnswer{Outcome: observability.PhysicalQueryUpstream504, Elapsed: 30200 * time.Millisecond,
		Answered: true, Wait: 30 * time.Second, Body: 200 * time.Millisecond})
	r.RecordPhysicalQueryAnswer(observability.PhysicalQueryAnswer{Outcome: observability.PhysicalQueryUnavailable, Elapsed: 2 * time.Second})
	cells := read()
	want := map[[2]string][2]float64{
		{observability.PhysicalQueryOK, "wait"}: {1, 3}, {observability.PhysicalQueryOK, "body"}: {1, 1.5},
		{observability.PhysicalQueryUpstream504, "wait"}: {1, 30}, {observability.PhysicalQueryUpstream504, "body"}: {1, 0.2},
		{observability.PhysicalQueryUnavailable, "wait"}: {}, {observability.PhysicalQueryUnavailable, "body"}: {},
	}
	for cell, value := range want {
		if got := cells[cell]; got[0] != value[0] || math.Abs(got[1]-value[1]) > 1e-9 {
			t.Fatalf("%v = %v, want %v (all %v)", cell, got, value, cells)
		}
	}
}
