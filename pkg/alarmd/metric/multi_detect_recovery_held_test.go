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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The recovery windows undecided rounds held exist at both positions from the
// start and add what each Level's walk held, by position.
func TestTheRecoveryWindowsUndecidedRoundsHeldAreCountedByPosition(t *testing.T) {
	const held = "alarmd_multi_detect_recovery_held_total"
	r := NewRecorder(BuildInfo{})
	initial := gatherFamily(t, r, held)
	if len(initial) != len(observability.MultiDetectHeldPositions) {
		t.Fatalf("positions before any observation = %d series, want both at zero", len(initial))
	}
	r.ObserveMultiDetectRecoveryHeld(2, 1)
	r.ObserveMultiDetectRecoveryHeld(1, 0)
	byPosition := map[string]float64{}
	for _, m := range gatherFamily(t, r, held) {
		if len(m.Label) != 1 || m.Label[0].GetName() != "position" {
			t.Fatalf("series %v, want one position label", m)
		}
		byPosition[m.Label[0].GetValue()] = m.GetCounter().GetValue()
	}
	if len(byPosition) != 2 || byPosition["window_end"] != 3 || byPosition["window_inside"] != 1 {
		t.Fatalf("held by position %v, want window_end 3 and window_inside 1 and nothing else", byPosition)
	}
}

// The undecided rounds exist at every reason from the start and count each
// one heard; a reason outside the closed list counts nothing.
func TestTheUndecidedRoundsAreCountedByReason(t *testing.T) {
	const undecided = "alarmd_multi_detect_undecided_total"
	r := NewRecorder(BuildInfo{})
	if initial := gatherFamily(t, r, undecided); len(initial) != len(observability.MultiDetectUndecidedReasons) {
		t.Fatalf("reasons before any round = %d series, want every reason at zero", len(initial))
	}
	r.ObserveMultiDetectUndecided(observability.MultiDetectUndecidedInputMissing)
	r.ObserveMultiDetectUndecided(observability.MultiDetectUndecidedInputMissing)
	r.ObserveMultiDetectUndecided(observability.MultiDetectUndecidedInputPartial)
	r.ObserveMultiDetectUndecided(observability.MultiDetectUndecidedReason("qg-secret"))
	byReason := map[string]float64{}
	for _, m := range gatherFamily(t, r, undecided) {
		byReason[m.Label[0].GetValue()] = m.GetCounter().GetValue()
	}
	if len(byReason) != 5 || byReason["input_missing"] != 2 || byReason["input_partial"] != 1 || byReason["input_error"] != 0 {
		t.Fatalf("undecided by reason %v, want input_missing 2, input_partial 1, the rest 0, nothing else", byReason)
	}
}
