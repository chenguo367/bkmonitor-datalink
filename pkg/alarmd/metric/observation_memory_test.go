// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/memoryline"
)

// The line is read at each scrape: the headroom as one gauge, negative past
// the line, and every consumer's refusals and admitted bytes, each consumer
// present at zero.
func TestTheObservationMemoryLineIsReadAtEachScrape(t *testing.T) {
	recorder := NewRecorder(BuildInfo{Version: "test"})
	reading := memoryline.Reading{HeadroomBytes: 5 << 20, RefusedTotal: map[memoryline.Consumer]uint64{memoryline.ConsumerLookback: 3},
		AdmittedBytes: map[memoryline.Consumer]uint64{memoryline.ConsumerCostSummary: 1 << 20}}
	if err := recorder.BindObservationMemory(func() memoryline.Reading { return reading }); err != nil {
		t.Fatal(err)
	}
	if err := recorder.BindObservationMemory(func() memoryline.Reading { return reading }); err == nil {
		t.Fatal("a second line bound")
	}
	scrape := func() (float64, map[string]float64, map[string]float64) {
		families, err := recorder.Gatherer().Gather()
		if err != nil {
			t.Fatal(err)
		}
		var headroom float64
		refused, admitted := map[string]float64{}, map[string]float64{}
		for _, family := range families {
			for _, series := range family.GetMetric() {
				switch family.GetName() {
				case "bkmonitor_alarmd_observation_memory_headroom_bytes":
					headroom = series.GetGauge().GetValue()
				case "bkmonitor_alarmd_observation_memory_refused_total":
					refused[series.GetLabel()[0].GetValue()] = series.GetCounter().GetValue()
				case "bkmonitor_alarmd_observation_memory_admitted_bytes_total":
					admitted[series.GetLabel()[0].GetValue()] = series.GetCounter().GetValue()
				}
			}
		}
		return headroom, refused, admitted
	}
	headroom, refused, admitted := scrape()
	if headroom != 5<<20 || refused["lookback"] != 3 || admitted["cost_summary"] != 1<<20 ||
		len(refused) != len(memoryline.Consumers) || len(admitted) != len(memoryline.Consumers) {
		t.Fatalf("headroom %v refused %v admitted %v, want the reading with every consumer present", headroom, refused, admitted)
	}
	reading.HeadroomBytes = -7
	if headroom, _, _ := scrape(); headroom != -7 {
		t.Fatalf("headroom %v past the line, want -7 read at this scrape", headroom)
	}
}
