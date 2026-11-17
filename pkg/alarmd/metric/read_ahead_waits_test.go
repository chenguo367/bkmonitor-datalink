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
	"time"
)

// Each wait is counted in its cell as it is told, its time added in
// seconds, and the Slot time with one waiting on its own; every cell is
// there from the start, and a word outside the closed lists counts nothing.
func TestEveryReadAheadWaitIsCountedInItsCellAsItEnds(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "0.7.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	if samples := redisSamples(t, recorder, "alarmd_worker_slice_read_ahead_waits_total", nil); len(samples) != 20 {
		t.Fatalf("%d cells before any wait, want every outcome by every band", len(samples))
	}
	recorder.RecordReadAheadWait("kept", "lt_1ms", 400*time.Microsecond)
	recorder.RecordReadAheadWait("stopped", "100ms_1s", 600*time.Millisecond)
	recorder.RecordReadAheadWait("stopped", "100ms_1s", 400*time.Millisecond)
	recorder.RecordReadAheadWait("lost", "lt_1ms", time.Second)
	recorder.RecordReadAheadWaiting(750 * time.Millisecond)
	for cell, want := range map[[2]string]float64{{"kept", "lt_1ms"}: 1, {"stopped", "100ms_1s"}: 2, {"canceled", "ge_1s"}: 0} {
		if got := redisCounterSum(t, recorder, "alarmd_worker_slice_read_ahead_waits_total",
			map[string]string{"outcome": cell[0], "duration": cell[1]}); got != want {
			t.Errorf("%v = %v, want %v", cell, got, want)
		}
	}
	if got := redisCounterSum(t, recorder, "alarmd_worker_slice_read_ahead_waits_total", nil); got != 3 {
		t.Fatalf("%v waits counted, want 3: a word outside the lists counted", got)
	}
	if got := redisCounterSum(t, recorder, "alarmd_worker_slice_read_ahead_wait_seconds_total", nil); got < 1.0003 || got > 1.0005 {
		t.Fatalf("wait seconds %v, want the three waits' 1.0004", got)
	}
	if got := redisCounterSum(t, recorder, "alarmd_worker_slice_read_ahead_waiting_seconds_total", nil); got != 0.75 {
		t.Fatalf("waiting seconds %v, want 0.75", got)
	}
}
