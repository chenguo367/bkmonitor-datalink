// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"slices"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// wholeFirstRead is one whole first read of qg's Slot at evaluation, its
// frozen schedule's next Slot a minute on, sent at the fixture's clock and
// done took later.
func wholeFirstRead(t *testing.T, f *fixture, evaluation int64, took time.Duration) {
	t.Helper()
	q := query("qg", evaluation, minute, sourceLog)
	q.FollowingSlot = execution.EvaluationTime(evaluation + 60)
	read := f.engine.Begin(q)
	if read == nil {
		t.Fatalf("the Slot at %d took no read", evaluation)
	}
	f.clock.set(f.clock.now().Add(took))
	read.Complete(execution.ProviderCompletion{Completeness: execution.CompletenessFull}, nil)
}

// A group read every round has the reading it had before readings aged, round
// by round: the longest of its last sixteen whole first reads (014 next8).
// Its rounds then go unread - each diverted, a replay being no first read -
// and its reading holds until sixteen of its rounds have passed since the
// latest, and then it has none, though it was measured; a first read again
// is a reading at once.
func TestAGroupsReadingIsItsLastSixteenWhileItReadsAndAgesPastSixteenOfItsRounds(t *testing.T) {
	// Sixteen, as the design has it, not as the constant says.
	const kept = 16
	f := newFixture(t)
	start := f.clock.now().Truncate(time.Minute)
	var last []time.Duration
	for round := range 40 {
		took := time.Duration(1+round*7%23) * time.Second
		if round == 5 {
			took = 50 * time.Second
		}
		f.clock.set(start.Add(time.Duration(round) * time.Minute))
		wholeFirstRead(t, f, start.Unix()+int64(round)*60, took)
		last = append(last, took)
		if len(last) > kept {
			last = last[1:]
		}
		if got, ok := f.engine.QueryDuration("qg"); !ok || got != slices.Max(last) {
			t.Fatalf("round %d: the reading is %v %v, want the longest of the last %d, %v", round, got, ok, kept, slices.Max(last))
		}
	}
	latest := f.clock.now()
	f.clock.set(latest.Add(kept*time.Minute - time.Second))
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != last[len(last)-1] {
		t.Fatalf("a second short of %d rounds after the latest reading: %v %v, want the latest, %v", kept, got, ok, last[len(last)-1])
	}
	f.clock.set(latest.Add(kept*time.Minute + time.Second))
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 0 {
		t.Fatalf("past %d rounds after the latest reading: %v %v, want none, the group measured", kept, got, ok)
	}
	f.clock.set(f.clock.now().Truncate(time.Minute).Add(time.Minute))
	wholeFirstRead(t, f, f.clock.now().Unix(), 3*time.Second)
	if got, ok := f.engine.QueryDuration("qg"); !ok || got != 3*time.Second {
		t.Fatalf("a first read again: %v %v, want its 3 s", got, ok)
	}
}
