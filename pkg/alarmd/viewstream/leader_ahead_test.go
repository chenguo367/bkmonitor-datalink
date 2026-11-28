// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package viewstream_test

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// A Leader's heartbeat shows it ahead of the installed view when it names a
// later revision of the same term, or any version of a later term. A Leader
// of an earlier term - one a newer Leader replaced, still answering - is
// not ahead whatever revision it names: a snapshot asked of it would not
// install, and asking every heartbeat would never end.
func TestALeaderIsAheadByALaterRevisionOrALaterTermOnly(t *testing.T) {
	installed := viewstream.Version{ControlEpoch: 8, Revision: 5}
	for _, cell := range []struct {
		name string
		sent viewstream.Version
		want bool
	}{
		{"a later revision of the term", viewstream.Version{ControlEpoch: 8, Revision: 6}, true},
		{"the revision installed", viewstream.Version{ControlEpoch: 8, Revision: 5}, false},
		{"an earlier revision of the term", viewstream.Version{ControlEpoch: 8, Revision: 4}, false},
		{"a later term", viewstream.Version{ControlEpoch: 9, Revision: 1}, true},
		{"an earlier term at a later revision", viewstream.Version{ControlEpoch: 7, Revision: 40}, false},
		{"a heartbeat naming no version", viewstream.Version{}, false},
	} {
		if got := viewstream.LeaderAheadForTest(cell.sent, installed); got != cell.want {
			t.Errorf("%s: ahead = %t, want %t", cell.name, got, cell.want)
		}
	}
}
