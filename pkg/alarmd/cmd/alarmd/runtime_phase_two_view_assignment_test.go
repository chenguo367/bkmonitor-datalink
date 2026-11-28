// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// The view's Assignment of a record carries the record's own fields when the
// round published the record it read: no owner given, or the record's own.
// One the round moved to another worker, or rewrote after reading it - moved
// away and back, a skip word restated - names the worker it ends on without
// them (015 R1).
func TestTheViewCarriesARecordsOwnFieldsOnlyForTheRecordTheRoundRead(t *testing.T) {
	record := ownership.AssignmentRecord{
		QueryGroup: "qg-1", DesiredWorkerID: "worker-a", AssignmentGeneration: 4, RecordRevision: 9, ControlEpoch: 3,
		PlacementReason: ownership.PlacementRebalance, AssignedAt: time.UnixMilli(1_700_000_000_000), ContentScope: "scope",
		TimelineRecordRevision: 2, SegmentSkip: 3, SegmentSkipNoData: 3,
	}
	for _, cell := range []struct {
		name      string
		owner     string
		rewritten bool
		desired   string
		fields    bool
	}{
		{"no owner given", "", false, "worker-a", true},
		{"the record's own owner", "worker-a", false, "worker-a", true},
		{"moved to another worker", "worker-b", false, "worker-b", false},
		{"moved away and back", "worker-a", true, "worker-a", false},
		{"a skip word restated", "", true, "worker-a", false},
	} {
		got := viewAssignmentOf(record, cell.owner, cell.rewritten)
		if got.DesiredWorkerID != cell.desired || got.Revision != 9 || got.ContentScope != "scope" || got.TimelineRecordRevision != 2 {
			t.Fatalf("%s: %+v, want %s named at the read revision, content and timeline", cell.name, got, cell.desired)
		}
		if got.Complete() != cell.fields {
			t.Fatalf("%s: the record's own fields sent = %t (%+v), want %t", cell.name, got.Complete(), got, cell.fields)
		}
		if cell.fields && (got.AssignmentGeneration != 4 || got.ControlEpoch != 3 || got.PlacementReason != "REBALANCE" ||
			got.AssignedAtMs != 1_700_000_000_000 || got.SegmentSkip != 3 || got.SegmentSkipNoData != 3) {
			t.Fatalf("%s: %+v, want the record's own fields as it holds them", cell.name, got)
		}
		if !cell.fields && (got.AssignmentGeneration != 0 || got.ControlEpoch != 0 || got.PlacementReason != "" ||
			got.AssignedAtMs != 0 || got.SegmentSkip != 0 || got.SegmentSkipNoData != 0) {
			t.Fatalf("%s: %+v, want none of the record's own fields", cell.name, got)
		}
	}
}
