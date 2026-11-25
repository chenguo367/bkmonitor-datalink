// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
)

// recordedCounter is one counter cell of recorder, as /metrics serves it.
func recordedCounter(t *testing.T, recorder *metric.Recorder, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.GetMetric() {
			matched := 0
			for _, label := range sample.GetLabel() {
				if want, named := labels[label.GetName()]; named && want == label.GetValue() {
					matched++
				}
			}
			if matched == len(labels) {
				return sample.GetCounter().GetValue()
			}
		}
		t.Fatalf("%s has no cell %v", name, labels)
	}
	t.Fatalf("%s is not registered", name)
	return 0
}

// What the segment skip left unwritten is counted by the family it was left
// in (015 O1): through Execute on the real store, the rig with both words
// leaves members of the real family and of the no-data family, the rig
// with the segment skip word alone of the real family only, and on each the
// two families sum to state_series_not_written_total{segment_member_skipped}
// the same observations counted - so a family's skip share reads as its
// members left over its series classed.
func TestTheMembersTheSkipLeavesAreCountedByFamilyAndSumToItsNotWritten(t *testing.T) {
	rigs := newNoDataFamilyRigs(t, 3, 1)
	both, realOnly := metric.NewRecorder(metric.BuildInfo{}), metric.NewRecorder(metric.BuildInfo{})
	rigs.skipping.observed.next = both
	rigs.realOnly.observed.next = realOnly
	at := frozenContract().Slot.EvaluationTime
	for round := 0; round < 10; round++ {
		at += 60
		rigs.step(t, at, nil, false)
	}
	for name, recorder := range map[string]*metric.Recorder{"both words": both, "skip word alone": realOnly} {
		real := recordedCounter(t, recorder, "alarmd_state_segment_members_left_total", map[string]string{"family": "real"})
		noData := recordedCounter(t, recorder, "alarmd_state_segment_members_left_total", map[string]string{"family": "no_data"})
		skipped := recordedCounter(t, recorder, "alarmd_state_series_not_written_total", map[string]string{"reason": "segment_member_skipped"})
		if real <= 0 || real+noData != skipped {
			t.Fatalf("%s: members left real %v + no_data %v, not written segment_member_skipped %v: want real members left and the two summing to it",
				name, real, noData, skipped)
		}
		if name == "both words" && noData <= 0 {
			t.Fatalf("both words: no member of the no-data family left (real %v), want the synthetic series left beside the real", real)
		}
		if name == "skip word alone" && noData != 0 {
			t.Fatalf("skip word alone: %v members of the no-data family left, want none: its family follows its own word", noData)
		}
	}
	if left := skippedSoFar(rigs.skipping); float64(left) != recordedCounter(t, both, "alarmd_state_series_not_written_total", map[string]string{"reason": "segment_member_skipped"}) {
		t.Fatalf("the rig's observations left %d, the recorder counted otherwise", left)
	}
}
