// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Through the production bundle - the query service client, the worker and
// the Slot executor as they are wired - a completed Slot's usage carries its
// queries' bodies in microseconds beside the milliseconds (015 O1): an answer
// of nearly nothing is read in well under a millisecond, which the
// milliseconds round to 0. And the bundle's /metrics serves the two families
// read at the scrape from the worker: the largest shape's leaves and the
// attempts applied window by window.
func TestACompletedSlotsUsageCarriesItsQueriesBodiesInMicroseconds(t *testing.T) {
	fixture := startCutoverFixture(t, nil)
	var usage *observability.SlotBudgetUsageFacts
	for _, observation := range fixture.observed() {
		if observation.Stage == observability.StageSlotCompleted && observation.Err == nil && observation.SlotBudgetUsage != nil &&
			observation.SlotBudgetUsage.QueryWaitMillis+observation.SlotBudgetUsage.QueryBodyMicros > 0 {
			usage = observation.SlotBudgetUsage
		}
	}
	if usage == nil {
		t.Fatal("no completed Slot with a query's usage was observed")
	}
	if usage.QueryBodyMicros == 0 || usage.QueryBodyMicrosMax == 0 || usage.QueryBodyMicros < usage.QueryBodyMicrosMax ||
		usage.QueryBodyMicros/1000 > usage.QueryBodyMillis+1 || usage.QueryBodyMillis > usage.QueryBodyMicros/1000+1 {
		t.Fatalf("usage bodies %d ms (longest %d), %d us (longest %d): want the microseconds counted and agreeing with the milliseconds",
			usage.QueryBodyMillis, usage.QueryBodyMillisMax, usage.QueryBodyMicros, usage.QueryBodyMicrosMax)
	}
	families, err := fixture.recorder.ScrapeGatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	served := map[string]int{}
	for _, family := range families {
		served[family.GetName()] = len(family.GetMetric())
	}
	if served["alarmd_worker_slice_shape_leaves_max"] != 1 || served["alarmd_slot_windowed_total"] != 3 {
		t.Fatalf("the bundle serves worker_slice_shape_leaves_max with %d series and slot_windowed_total with %d, want 1 and 3",
			served["alarmd_worker_slice_shape_leaves_max"], served["alarmd_slot_windowed_total"])
	}
}
