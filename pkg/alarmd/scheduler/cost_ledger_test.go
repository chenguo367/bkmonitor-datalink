// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// The ledger reads a Query Group's peak from the Worker the roster names
// for it and no other, replaces an entry by the latest report, clears by
// the roster and never by absence from a heartbeat, and says nothing for
// a Query Group whose new holder has not reported yet.
func TestCostLedgerReadsTheHoldersReportAndClearsByTheRoster(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	ledger := NewCostLedger(func() time.Time { return now })
	live := now.Add(time.Minute)
	workers := []ownership.WorkerRegistration{byteWorker("a", 1000, live), byteWorker("b", 2000, live), byteWorker("c", 0, live)}
	owners := map[execution.QueryGroupIdentity]string{"qg-1": "a", "qg-2": "a", "qg-3": "b"}

	ledger.Record("a", []QueryGroupCostReport{{QueryGroup: "qg-1", RetainedBytesPeak: 300}, {QueryGroup: "qg-2", RetainedBytesPeak: 200}})
	// b reports qg-1 too - a report from a Worker the roster does not name
	// for it, which is not the running holder's number.
	ledger.Record("b", []QueryGroupCostReport{{QueryGroup: "qg-1", RetainedBytesPeak: 999}, {QueryGroup: "qg-3", RetainedBytesPeak: 400}})
	readings := ledger.Readings(owners, workers)
	if !reflect.DeepEqual(readings.Pool, map[string]uint64{"a": 1000, "b": 2000}) {
		t.Fatalf("pools = %v, want the two registered ones and not the one without", readings.Pool)
	}
	if !reflect.DeepEqual(readings.Peak, map[execution.QueryGroupIdentity]uint64{"qg-1": 300, "qg-2": 200, "qg-3": 400}) {
		t.Fatalf("peaks = %v, want each from its holder", readings.Peak)
	}

	// A later heartbeat from a carries only what moved: qg-2 alone. qg-1
	// keeps its reading; absence says nothing.
	ledger.Record("a", []QueryGroupCostReport{{QueryGroup: "qg-2", RetainedBytesPeak: 250}})
	if readings := ledger.Readings(owners, workers); readings.Peak["qg-1"] != 300 || readings.Peak["qg-2"] != 250 {
		t.Fatalf("peaks after a partial heartbeat = %v, want qg-1 kept and qg-2 replaced", readings.Peak)
	}

	// qg-1 moves to b. Retain drops a's entry for it and b's stray one is
	// the roster's holder now - but a report is a report of what that
	// Worker ran, and b never ran qg-1 when it said 999: the test pins that
	// Retain keeps b's entry, since the ledger cannot know, and the next
	// heartbeat from b - first report unconditional - replaces it.
	owners["qg-1"] = "b"
	ledger.Retain(owners)
	entries := ledger.Entries()
	want := []CostEntry{
		{QueryGroupCostReport: QueryGroupCostReport{QueryGroup: "qg-2", RetainedBytesPeak: 250}, WorkerID: "a", ReportedAt: now},
		{QueryGroupCostReport: QueryGroupCostReport{QueryGroup: "qg-1", RetainedBytesPeak: 999}, WorkerID: "b", ReportedAt: now},
		{QueryGroupCostReport: QueryGroupCostReport{QueryGroup: "qg-3", RetainedBytesPeak: 400}, WorkerID: "b", ReportedAt: now},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries after Retain = %+v, want %+v", entries, want)
	}
	// qg-2 retires: gone from the roster, gone from the ledger.
	delete(owners, "qg-2")
	ledger.Retain(owners)
	if readings := ledger.Readings(owners, workers); len(readings.Peak) != 2 || readings.Peak["qg-1"] != 999 {
		t.Fatalf("peaks after the retirement = %v", readings.Peak)
	}
	if entries := ledger.Entries(); len(entries) != 2 || entries[0].WorkerID != "b" {
		t.Fatalf("entries after the retirement = %+v, want a's emptied out", entries)
	}
	// A nil ledger reads pools and no peaks, and refuses nothing.
	var none *CostLedger
	if readings := none.Readings(owners, workers); len(readings.Pool) != 2 || len(readings.Peak) != 0 {
		t.Fatalf("nil ledger readings = %+v", readings)
	}
	none.Record("a", nil)
	none.Retain(owners)
}
