// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
)

// The Control Leader takes a census of each store it writes to, reported by
// family on the scrape and counted under the store_census caller of the
// store's client; a replica that does not lead reports none, and one that
// stops leading forgets its last.
func TestTheLeaderReportsWhatEachStoreHoldsByFamily(t *testing.T) {
	_, server := startPhaseTwoRedis(t)
	recorder := metric.NewRecorder(metric.BuildInfo{Version: "test"})
	client := redis.NewClient(server.Options())
	client.AddHook(recorder.RedisHook("source"))
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	for index := range 40 {
		if err := client.Set(ctx, fmt.Sprintf("alarmd:state:%064x", index), "value", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	census := &storeCensus{now: time.Now, stores: []censusStore{{name: "source", client: client}}}
	if err := recorder.BindStoreCensus(census.read); err != nil {
		t.Fatal(err)
	}
	gather := func() map[string]float64 {
		t.Helper()
		families, err := recorder.Gatherer().Gather()
		if err != nil {
			t.Fatal(err)
		}
		readings := map[string]float64{}
		for _, family := range families {
			for _, series := range family.GetMetric() {
				labels := ""
				for _, label := range series.GetLabel() {
					labels += "/" + label.GetValue()
				}
				readings[family.GetName()+labels] = series.GetGauge().GetValue()
			}
		}
		return readings
	}

	census.measure(ctx, false)
	if readings := gather(); readings["bkmonitor_alarmd_store_census_keys/source"] != 0 {
		t.Fatalf("a replica that does not lead reported %v", readings)
	}
	census.measure(ctx, true)
	readings := gather()
	if readings["bkmonitor_alarmd_store_census_keys/source"] != 40 || readings["bkmonitor_alarmd_store_census_exact/source"] != 1 ||
		readings["bkmonitor_alarmd_store_census_family_keys/source/alarmd:state:*"] != 40 ||
		readings["bkmonitor_alarmd_store_census_family_samples/source/alarmd:state:*"] != 40 ||
		readings["bkmonitor_alarmd_store_census_family_bytes/source/alarmd:state:*"] <= 0 {
		t.Fatalf("the Leader's census = %v, want the store's 40 keys in one family, weighed", readings)
	}
	operations, _ := callerCounts(t, recorder)
	if operations["source/store_census"] == 0 {
		t.Fatalf("operations %v, want the census counted under store_census", operations)
	}
	census.measure(ctx, false)
	if readings := gather(); readings["bkmonitor_alarmd_store_census_keys/source"] != 0 {
		t.Fatalf("a replica that stopped leading still reports %v", readings)
	}
}
