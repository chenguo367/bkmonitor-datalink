// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// The copy's counts become the snapshot's by strategy id, sorted; one id
// under two tenants takes the larger count, and a strategy unknown under one
// tenant and counted under another stays counted.
func TestOpenAlertCountsAreTheCopysByStrategy(t *testing.T) {
	counts := map[openalerts.StrategyKey]int{
		{TenantID: "system", StrategyID: "12"}: 3,
		{TenantID: "other", StrategyID: "12"}:  5,
		{TenantID: "system", StrategyID: "7"}:  1,
	}
	unknown := []openalerts.StrategyKey{{TenantID: "system", StrategyID: "9"}, {TenantID: "system", StrategyID: "7"}}
	got := openAlertCountsOf(counts, unknown)
	want := &fleet.OpenAlertCounts{Counts: []fleet.StrategyOpenCount{{Strategy: "12", Count: 5}, {Strategy: "7", Count: 1}}, Unknown: []string{"9"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
}

// The publisher carries the counts it is given and none when it has no copy.
func TestFleetPublisherCarriesOpenAlertCounts(t *testing.T) {
	clock := &dueIndexClock{at: time.Unix(20_000, 0)}
	counts := &fleet.OpenAlertCounts{Counts: []fleet.StrategyOpenCount{{Strategy: "1", Count: 2}}}
	publisher := fleetPublisher{tracker: fleet.NewTracker(nil, "replica-1", clock.now), replica: "replica-1", now: clock.now,
		owned:           func() []execution.QueryGroupIdentity { return nil },
		openAlertCounts: func() *fleet.OpenAlertCounts { return counts }}
	if snapshot := publisher.snapshot(context.Background()); !reflect.DeepEqual(snapshot.OpenAlerts, counts) {
		t.Fatalf("open alerts = %+v, want %+v", snapshot.OpenAlerts, counts)
	}
	publisher.openAlertCounts = nil
	if snapshot := publisher.snapshot(context.Background()); snapshot.OpenAlerts != nil {
		t.Fatalf("a publisher without a copy published %+v", snapshot.OpenAlerts)
	}
}
