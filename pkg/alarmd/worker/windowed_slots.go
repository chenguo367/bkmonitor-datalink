// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"

// The ends of an attempt applied window by window (WindowedSlots).
const (
	windowedCompleted = iota
	windowedFailed
	windowedRetried
)

// WindowedSlotsReading is the attempts this coordinator applied window by
// window - one window or more on its apply goroutine - by how each ended:
// Completed, the Slot completed; Retried, the attempt returned no error and
// did not complete, asking for another (an activation or output retry);
// Failed, it returned an error. The window pipeline's phase seconds
// (slot_apply_wait_seconds_total and its two neighbours) are the Completed
// ones' alone: this is their denominator, and what the pipeline did on the
// attempts that did not complete (015 O1).
type WindowedSlotsReading struct {
	Completed, Failed, Retried uint64
}

// WindowedSlots reads the coordinator's windowed attempts, at the scrape.
func WindowedSlots(coordinator *SlotExecutionCoordinator) WindowedSlotsReading {
	if coordinator == nil {
		return WindowedSlotsReading{}
	}
	return WindowedSlotsReading{Completed: coordinator.windowed[windowedCompleted].Load(),
		Failed: coordinator.windowed[windowedFailed].Load(), Retried: coordinator.windowed[windowedRetried].Load()}
}

// noteWindowed counts an attempt that applied windows window by window by
// how it ended; one applied whole (no window) is not counted.
func (coordinator *SlotExecutionCoordinator) noteWindowed(windows int, result execution.SlotExecutionResult, err error) {
	if windows <= 0 {
		return
	}
	switch {
	case err != nil:
		coordinator.windowed[windowedFailed].Add(1)
	case result.Completed:
		coordinator.windowed[windowedCompleted].Add(1)
	default:
		coordinator.windowed[windowedRetried].Add(1)
	}
}
