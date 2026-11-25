// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"testing"
	"time"
)

// A read let go did not end: its series no longer count, and the change
// runs from the read before's end to the first series of the read that came
// back and ended. A read of no series has no tail, nor the change into it,
// and the next change runs from its end.
func TestALetGoReadIsNoSeamAndAnEmptyOneHasNone(t *testing.T) {
	at := func(seconds int) time.Time {
		return time.Unix(1_788_000_000, 0).Add(time.Duration(seconds) * time.Second)
	}
	var seams sliceSeams
	seams.seriesCame(at(0))
	seams.seriesCame(at(2))
	seams.readEnded(at(3)) // tail 1, no change: the first read
	seams.seriesCame(at(5))
	seams.seriesCame(at(6))
	seams.readLetGo() // let go at 6, split
	seams.seriesCame(at(9))
	seams.readEnded(at(11)) // tail 2, change 9 - 3 = 6
	seams.readEnded(at(12)) // no series: neither
	seams.seriesCame(at(16))
	seams.readEnded(at(16)) // tail 0, change 16 - 12 = 4
	if seams.tail != 3*time.Second || seams.tailMax != 2*time.Second || seams.change != 10*time.Second || seams.changeMax != 6*time.Second {
		t.Fatalf("tails %v (longest %v), changes %v (longest %v): want 3 s (2 s) and 10 s (6 s)", seams.tail, seams.tailMax,
			seams.change, seams.changeMax)
	}
	// Every read's end from the one before's, the read of no series too
	// (015 O1): 11 - 3, 12 - 11, 16 - 12.
	if seams.handoff != 13*time.Second || seams.handoffMax != 8*time.Second {
		t.Fatalf("handoffs %v (longest %v), want 13 s (8 s): every kept read's end from the one before's, an empty one too",
			seams.handoff, seams.handoffMax)
	}
}
