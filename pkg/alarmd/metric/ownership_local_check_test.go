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

// A Runner's ownership check is counted under its cell, and one under words
// this build has no cell for under other/other rather than dropped: a reason
// added without its cell shows on the family.
func TestAnOwnershipCheckUnderWordsWithoutACellIsCountedUnderOther(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "1.0.9999", Commit: "0123456789abcdef", SchemaVersion: "v3"})
	recorder.RecordOwnershipLocalCheck("local_pass", "local_pass")
	recorder.RecordOwnershipLocalCheck("fallback_redis", "view_silent")
	recorder.RecordOwnershipLocalCheck("fallback_redis", "a_word_from_later")
	recorder.RecordOwnershipLocalCheck("local_pass", "view_silent")
	for _, cell := range []struct {
		outcome, reason string
		want            float64
	}{
		{"local_pass", "local_pass", 1}, {"fallback_redis", "view_silent", 1}, {"other", "other", 2},
	} {
		got := redisCounterSum(t, recorder, "alarmd_ownership_local_check_total", map[string]string{"outcome": cell.outcome, "reason": cell.reason})
		if got != cell.want {
			t.Errorf("%s/%s = %v, want %v", cell.outcome, cell.reason, got, cell.want)
		}
	}
	if total := redisCounterSum(t, recorder, "alarmd_ownership_local_check_total", nil); total != 4 {
		t.Fatalf("the family counts %v, want every check, 4", total)
	}
}
