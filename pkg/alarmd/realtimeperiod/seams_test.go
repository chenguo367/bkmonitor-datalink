// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeperiod_test

import (
	"context"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeinput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeperiod"
)

// The empty implementations each side is built against until the other's
// block lands (016 section 12, B-1): the point stream against Buckets, the
// period source against TopicProgress and ChannelStates.
type (
	bucketsStub struct {
		mu    sync.Mutex
		added int
	}
	progressStub struct{}
	channelsStub struct{}
)

func (buckets *bucketsStub) Add(execution.QueryGroupIdentity, []execution.PlanIdentity, realtimeinput.Point) realtimeperiod.AddResult {
	buckets.mu.Lock()
	defer buckets.mu.Unlock()
	buckets.added++
	return realtimeperiod.AddAccepted
}

func (progressStub) Progress(string, string) (map[int32]realtimeinput.PartitionProgress, bool) {
	return nil, false
}

func (channelsStub) NoDataFrom(context.Context, string, string, contract.RealtimeItemKey) (int64, bool, error) {
	return 0, false, nil
}

var (
	_ realtimeperiod.Buckets       = (*bucketsStub)(nil)
	_ realtimeperiod.TopicProgress = progressStub{}
	_ realtimeperiod.ChannelStates = channelsStub{}
)

// The seams' signatures, as the blocks either side of each are written
// against them: Add takes the group, the Plans a point passed and the point,
// from every partition's goroutine at once; Progress the cluster and the
// topic, with whether its partitions are known; NoDataFrom the tenant, the
// table and the item, its error apart from not having taken effect.
func TestTheRealtimeSeamsAreTheOnesEachSideIsWrittenAgainst(t *testing.T) {
	var buckets realtimeperiod.Buckets = &bucketsStub{}
	point := realtimeinput.Point{Metric: "cpu_usage", Value: 1, EventTimeMs: 1_788_000_000_000,
		Labels: []realtimeinput.Label{{Name: "ip", Value: "127.0.0.1"}}}
	plans := []execution.PlanIdentity{{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}}
	var wg sync.WaitGroup
	for partition := 0; partition < 4; partition++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := buckets.Add("query-group", plans, point); got != realtimeperiod.AddAccepted {
				t.Errorf("stub Add = %q", got)
			}
		}()
	}
	wg.Wait()
	if added := buckets.(*bucketsStub).added; added != 4 {
		t.Fatalf("stub took %d points from four goroutines, want 4", added)
	}
	var progress realtimeperiod.TopicProgress = progressStub{}
	if partitions, known := progress.Progress("cluster", "topic"); known || partitions != nil {
		t.Fatalf("stub Progress = %v, %v", partitions, known)
	}
	var channels realtimeperiod.ChannelStates = channelsStub{}
	if from, known, err := channels.NoDataFrom(context.Background(), "tenant", "2_table", contract.RealtimeItemKey{StrategyID: 1, ItemID: 2, QueryMD5: "q"}); from != 0 || known || err != nil {
		t.Fatalf("stub NoDataFrom = %d, %v, %v", from, known, err)
	}
}

// AddResult is a closed set: the four outcomes a point filed has, each
// named once, and nothing else a counter by it could be labelled with.
func TestAddResultsAreTheClosedSet(t *testing.T) {
	want := map[realtimeperiod.AddResult]bool{"accepted": true, "late_after_close": true, "replay_skipped": true, "over_quota": true}
	got := realtimeperiod.AddResults()
	if len(got) != len(want) {
		t.Fatalf("AddResults = %v, want the %d of %v", got, len(want), want)
	}
	seen := map[realtimeperiod.AddResult]bool{}
	for _, result := range got {
		if !want[result] || seen[result] {
			t.Fatalf("AddResults = %v: %q unknown or twice", got, result)
		}
		seen[result] = true
	}
}
