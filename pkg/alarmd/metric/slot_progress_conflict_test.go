// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import "testing"

// A conflict is counted under its word, and one under a word this build does
// not know is counted under OTHER rather than dropped; an unnamed one under
// UNNAMED, the store's word for it.
func TestAProgressConflictUnderAWordNotKnownIsCountedUnderOther(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "1.0.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	recorder.RecordProgressConflict("commit", "CURSOR_MOVED")
	recorder.RecordProgressConflict("commit", "A_WORD_FROM_LATER")
	recorder.RecordProgressConflict("begin", "UNNAMED")
	recorder.RecordProgressConflict("elsewhere", "CURSOR_MOVED")
	for _, cell := range []struct {
		operation, reason string
		want              float64
	}{
		{"commit", "CURSOR_MOVED", 1}, {"commit", "OTHER", 1}, {"begin", "UNNAMED", 1}, {"begin", "CURSOR_MOVED", 0},
	} {
		got := redisCounterSum(t, recorder, "alarmd_progress_conflicts_total", map[string]string{"operation": cell.operation, "reason": cell.reason})
		if got != cell.want {
			t.Errorf("%s/%s = %v, want %v", cell.operation, cell.reason, got, cell.want)
		}
	}
	if total := redisCounterSum(t, recorder, "alarmd_progress_conflicts_total", nil); total != 3 {
		t.Fatalf("the family counts %v, want 3: an operation this build does not know is dropped", total)
	}
}
