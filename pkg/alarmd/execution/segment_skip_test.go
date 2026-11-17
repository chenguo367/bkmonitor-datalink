// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package execution_test

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A Slot follows its segment skip word only when the word names the Leader
// whose view the worker has installed; a Slot without a word neither skips
// nor ignores anything; one whose word it does not follow says why.
func TestASegmentSkipWordIsFollowedOnlyUnderTheLeaderThatSetIt(t *testing.T) {
	for _, test := range []struct {
		name            string
		word, viewEpoch uint64
		installed       bool
		follows         bool
		ignored         string
	}{
		{"no word", 0, 7, true, false, ""},
		{"no word and no view", 0, 0, false, false, ""},
		{"the view's Leader set it", 7, 7, true, true, ""},
		{"an earlier Leader set it", 6, 7, true, false, observability.SegmentSkipIgnoredEpochMismatch},
		{"a later Leader set it", 8, 7, true, false, observability.SegmentSkipIgnoredEpochMismatch},
		{"no view installed", 7, 0, false, false, observability.SegmentSkipIgnoredNoView},
		{"no view installed, its epoch reads as the word", 7, 7, false, false, observability.SegmentSkipIgnoredNoView},
	} {
		follows, ignored := execution.SegmentSkipAdmitted(test.word, test.viewEpoch, test.installed)
		if follows != test.follows || ignored != test.ignored {
			t.Errorf("%s: (%v, %q), want (%v, %q)", test.name, follows, ignored, test.follows, test.ignored)
		}
	}
}
