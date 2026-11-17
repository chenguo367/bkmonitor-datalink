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
	"context"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

type skipFlagObserver struct{ seen []observability.Observation }

func (observer *skipFlagObserver) Observe(_ context.Context, observation observability.Observation) {
	observer.seen = append(observer.seen, observation)
}

// A Slot reads the epoch of the view its worker has installed once as it
// begins and follows its segment skip word only under that Leader; a word
// it does not follow is counted, by why, on the state component's counter,
// and a Slot with no word or one it follows counts nothing.
func TestASlotFollowsItsSegmentSkipWordOnlyUnderTheViewItsWorkerHolds(t *testing.T) {
	installed := func(epoch uint64) func() (uint64, bool) { return func() (uint64, bool) { return epoch, true } }
	for _, test := range []struct {
		name    string
		word    uint64
		view    func() (uint64, bool)
		follows bool
		ignored map[string]int64
	}{
		{"no word", 0, installed(7), false, nil},
		{"the view's Leader", 7, installed(7), true, nil},
		{"an earlier Leader's word", 6, installed(7), false, map[string]int64{observability.SegmentSkipIgnoredEpochMismatch: 1}},
		{"no view yet", 7, func() (uint64, bool) { return 0, false }, false, map[string]int64{observability.SegmentSkipIgnoredNoView: 1}},
		{"no view source", 7, nil, false, map[string]int64{observability.SegmentSkipIgnoredNoView: 1}},
	} {
		observer := &skipFlagObserver{}
		coordinator := &SlotExecutionCoordinator{ports: Ports{Observer: observer, InstalledControlEpoch: test.view}}
		follows := coordinator.followsSegmentSkip(context.Background(), execution.SlotExecutionRequest{
			Operation: execution.OperationNormal, SegmentSkip: test.word,
		})
		var ignored map[string]int64
		for _, observation := range observer.seen {
			if observation.SegmentShadow != nil {
				if observation.Component != observability.ComponentState || observation.Result != observability.ResultSuccess {
					t.Errorf("%s: counted on %+v", test.name, observation)
				}
				ignored = observation.SegmentShadow.SkipFlagIgnored
			}
		}
		if follows != test.follows || !reflect.DeepEqual(ignored, test.ignored) {
			t.Errorf("%s: follows %v, ignored %v; want %v, %v", test.name, follows, ignored, test.follows, test.ignored)
		}
	}
}
