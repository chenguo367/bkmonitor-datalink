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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// assertTheObjectReadsItsPlanSegmentsAndNextSlot reads the object on the
// production bundle's own route after its round at slot: its one Plan's
// mark is the one that round wrote, under the generation it ran, with what
// this replica last did with it - the read before the round and the write
// it applied - and the object reads where it stands in its cycle once its
// runner returned with nothing due: its next Slot, the one after slot.
func assertTheObjectReadsItsPlanSegmentsAndNextSlot(t *testing.T, bundle *phaseTwoWorkerBundle, queryGroup execution.QueryGroupIdentity,
	plan execution.PlanIdentity, generation execution.StateGeneration, slot execution.EvaluationTime) {
	t.Helper()
	recorder := httptest.NewRecorder()
	bundle.dependencies.FleetAPI.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/objects/"+string(queryGroup), nil))
	var detail fleet.DetailResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &detail); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("object detail = %d %s: %v", recorder.Code, recorder.Body.String(), err)
	}
	if detail.PlanSegments == nil || detail.PlanSegmentsUnavailable != "" || detail.PlanSegments.Total != 1 ||
		len(detail.PlanSegments.Plans) != 1 || !detail.PlanSegments.LastRecorded {
		t.Fatalf("plan_segments = %+v (unavailable %q), want the one Plan with this replica's use", detail.PlanSegments, detail.PlanSegmentsUnavailable)
	}
	read := detail.PlanSegments.Plans[0]
	if read.Plan != plan || read.Generation != generation || read.Mark != segment.MarkFound || read.Round != slot ||
		read.Pages < 1 || len(read.Renewed) != read.Pages || read.RenewedTotal != read.Pages || read.Position <= 0 || int64(slot) < read.Position {
		t.Fatalf("the Plan's segments read %+v, want the mark the round at %d wrote under %s", read, slot, generation)
	}
	if read.Last == nil || read.Last.WriteResult != segment.WriteApplied || read.Last.WriteAt == nil ||
		read.Last.ReadSource == "" || read.Last.ReadReason == "" || read.Last.ReadAt == nil {
		t.Fatalf("this replica's last use %+v, want the round's read and its applied write", read.Last)
	}
	if detail.Wake == nil || !detail.Wake.Known || detail.Wake.IntervalSeconds <= 0 ||
		detail.Wake.NextSlot.Unix() != int64(slot)+detail.Wake.IntervalSeconds {
		t.Fatalf("wake = %+v, want the Slot after %d by its period", detail.Wake, slot)
	}
}

// fakeOpenPlans and fakePlanSegments stand in for the catalog and the store
// in readPlanSegments.
type fakeOpenPlans struct {
	plans []controlplane.OpenPlan
	total int
	err   error
	limit int
}

func (open *fakeOpenPlans) OpenPlans(_ context.Context, _ execution.QueryGroupIdentity, limit int) ([]controlplane.OpenPlan, int, error) {
	open.limit = limit
	return open.plans, open.total, open.err
}

type fakePlanSegments struct {
	keys []segment.PlanKey
	err  error
}

func (store *fakePlanSegments) PlanSegments(_ context.Context, group execution.QueryGroupIdentity, keys []segment.PlanKey) (segment.PlansReading, error) {
	store.keys = keys
	reading := segment.PlansReading{QueryGroup: group, Total: len(keys)}
	for _, key := range keys {
		reading.Plans = append(reading.Plans, segment.PlanReading{Plan: key.Plan, Generation: key.Generation, Mark: segment.MarkAbsent})
	}
	return reading, store.err
}

// plan_segments reads the open Segment's Plans, at most MaxPlanCarries,
// each - a piece of a split strategy by its piece - under the generation it
// runs, and counts the Segment's Plans in all;
// a failed read of either is the read's error, and a counted result.
func TestPlanSegmentsAreTheOpenSegmentsPlansUnderTheirGenerations(t *testing.T) {
	first := execution.PlanIdentity{TenantID: "tenant-a", BusinessID: "2", StrategyID: "1001"}
	second := execution.PlanIdentity{TenantID: "tenant-a", BusinessID: "2", StrategyID: "1002"}
	piece := execution.ShardRef{Dimension: "bk_target_ip", Index: 1, Count: 2, MatcherDigest: strings.Repeat("c", 64)}
	open := &fakeOpenPlans{plans: []controlplane.OpenPlan{{Plan: first, StateGeneration: "g1"}, {Plan: second, Shard: piece, StateGeneration: "g7"}}, total: 60}
	store := &fakePlanSegments{}
	reading, err := readPlanSegments(context.Background(), open, store, "qg-1")
	if err != nil || open.limit != controlplane.MaxPlanCarries || reading.Total != 60 || reading.QueryGroup != "qg-1" {
		t.Fatalf("reading = (%+v, %v) asked by %d, want the Segment's 60 Plans counted and at most %d read", reading, err, open.limit, controlplane.MaxPlanCarries)
	}
	if len(store.keys) != 2 || store.keys[0] != (segment.PlanKey{Plan: first, Generation: "g1"}) || store.keys[1] != (segment.PlanKey{Plan: second, Shard: piece, Generation: "g7"}) {
		t.Fatalf("the store was asked %+v, want each Plan, a piece by its piece, under the generation the Segment runs it by", store.keys)
	}
	for _, test := range []struct {
		open  error
		store error
		want  string
	}{
		{open: controlplane.ErrScheduleUnavailable, want: "no_open_segment"},
		{open: errors.New("timeline"), want: "failed"},
		{store: errors.New("mget"), want: "failed"},
	} {
		_, err := readPlanSegments(context.Background(), &fakeOpenPlans{plans: open.plans, total: 2, err: test.open}, &fakePlanSegments{err: test.store}, "qg-1")
		if err == nil || planCarriesReadResult(err) != test.want {
			t.Fatalf("on (%v, %v) the read = %v, counted %q; want %s", test.open, test.store, err, planCarriesReadResult(err), test.want)
		}
	}
}
