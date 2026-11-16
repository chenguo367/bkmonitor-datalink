// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// scrapedSliceMemory is the worker_slice_memory_entries gauges as /metrics
// serves them (ScrapeGatherer), the coordinator bound as the runtime bundle
// binds it: read at the scrape, no sweep run before.
func scrapedSliceMemory(t *testing.T, coordinator *worker.SlotExecutionCoordinator) map[string]float64 {
	t.Helper()
	recorder := metric.NewRecorder(metric.BuildInfo{})
	if err := recorder.BindSliceMemory(func() metric.SliceMemoryReading {
		reading := worker.SliceMemory(coordinator)
		return metric.SliceMemoryReading{Remembered: reading.Remembered, Unswept: reading.Unswept,
			Leaves: reading.Leaves, LeafSeries: reading.LeafSeries, Deferred: reading.Deferred}
	}); err != nil {
		t.Fatal(err)
	}
	families, err := recorder.ScrapeGatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "alarmd_worker_slice_memory_entries" {
			continue
		}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if label.GetName() == "kind" {
					entries[label.GetValue()] = sample.GetGauge().GetValue()
				}
			}
		}
	}
	if len(entries) != 3 {
		t.Fatalf("scraped entries %v, want leaves, leaf_series and deferred", entries)
	}
	return entries
}

// A Slot that completes in a shape - kept for a slice split for its time,
// and read in it the round after - leaves an entry of its series for every
// slice it landed, and the scrape reads them: leaf_series is the slices
// landed, leaves the shape's. A Slot that failed out of time, its slice
// split for the next, keeps the shape and has landed no read of it whole:
// leaves above one, leaf_series none.
func TestTheSliceMemoryScrapeCountsTheSlicesACompletedShapeLanded(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(8, 1<<10), sliceableFacts(), 200)
	fixture.source.timeoutWhole = true
	request := windowedRequest()
	readWholeTimedOut(t, fixture, request)
	fixture.source.mu.Lock()
	fixture.source.timeoutWhole, fixture.source.queryMillis = false, 1_000
	// The slice, its child and its grandchild each run out of time once
	// asked: the first Slot fails, its slice split for the next.
	fixture.source.timeoutAfter, fixture.source.timeoutTimes = 1, 3
	fixture.source.mu.Unlock()
	if _, err := fixture.coordinator.Execute(context.Background(), request); err == nil {
		t.Fatal("round 1, the slice and its child out of time, completed")
	}
	failed := scrapedSliceMemory(t, fixture.coordinator)
	if failed["leaves"] <= 1 || failed["leaf_series"] != 0 {
		t.Fatalf("after the failed Slot scraped %v, want its kept shape's leaves and no slice series", failed)
	}
	for round := 2; round <= 3; round++ {
		first := len(fixture.source.calls)
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed || result.Usage.InputSliceShapeStarted != 1 {
			t.Fatalf("round %d: result=%+v err=%v, want it completed in the shape", round, result, err)
		}
		landed := answeredSlices(fixture.source.calls[first:])
		scraped := scrapedSliceMemory(t, fixture.coordinator)
		if scraped["leaf_series"] != float64(len(landed)) || scraped["leaves"] <= 1 {
			t.Fatalf("round %d scraped %v, want leaf_series the %d slices landed and the shape's leaves",
				round, scraped, len(landed))
		}
	}
}
