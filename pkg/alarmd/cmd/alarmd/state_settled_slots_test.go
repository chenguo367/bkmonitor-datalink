// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"

// stateSettledSlots counts the Slots the observations carry completing
// successfully, in order, and how many of them settled their State step as 014 B3 stage 3
// has it: written, a successful state_applied line; or left out, its census
// admitting mutations and naming every one of them segment_member_skipped -
// the stable members of a Slot dispatched with the Leader's word are not
// written, and a Slot whose members all are writes no chunk and has no
// state_applied line. A Slot's State lines come before its completion line;
// the fixtures that read this run one Query Group's runner at a time, so the
// lines between two completions are the later Slot's.
func stateSettledSlots(observations []observability.Observation) (completed, settled int) {
	pending := false
	for _, observation := range observations {
		switch {
		case observation.Stage == observability.StageStateApplied && observation.Result == observability.ResultSuccess:
			pending = true
		case observation.Stage == observability.StageFrozenStateRenewed && observation.FrozenStateRenewal != nil:
			facts := observation.FrozenStateRenewal
			if admitted := facts.Admitted(); admitted > 0 && facts.NotWritten[observability.StateNotWrittenSegmentMemberSkipped] == admitted {
				pending = true
			}
		case observation.Stage == observability.StageSlotCompleted:
			// A completion that is not a success - a readiness wait
			// retried, a failure - ran no State step to settle.
			if observation.Result == observability.ResultSuccess {
				completed++
				if pending {
					settled++
				}
			}
			pending = false
		}
	}
	return completed, settled
}
