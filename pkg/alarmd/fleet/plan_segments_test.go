// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	alarmdexecution "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// detailOn is the object route over the unlisted fixture with this
// replica's rows, due index and Plans' segments bound as given; nil leaves
// one unbound.
func detailOn(t *testing.T, rows func(string) (Anomaly, bool), wake func(string) WakeFacts,
	segments func(context.Context, string) (segment.PlansReading, error)) func(path string) (DetailResponse, string) {
	t.Helper()
	_, snapshots, expectation := unlistedFixture(t)
	service := mustService(t, stubExpectations{expectation: expectation}, stubRegistry{replicas: replicas()}, stubSnapshots{snapshots: snapshots})
	service.SetLocalRows(rows)
	if wake != nil {
		service.SetLocalWake(wake)
	}
	if segments != nil {
		service.SetPlanSegments(segments)
	}
	handler, err := NewHandler(service, nil, func() time.Time { return now }, 0, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return func(path string) (DetailResponse, string) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, recorder.Code, recorder.Body.String())
		}
		var body DetailResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body, recorder.Body.String()
	}
}

// A healthy object, which no column lists, reads where it stands in its
// cycle - its next Slot - on the object route from the replica whose due
// index holds it, as a listed row does; one tracked before its first round
// here says the index knows nothing yet. A replica that neither tracks it
// nor holds a due entry names none.
func TestAHealthyObjectReadsItsNextSlotOnTheObjectRoute(t *testing.T) {
	tracker, _, _ := unlistedFixture(t)
	due := WakeFacts{Known: true, DueAt: now.Add(23 * time.Hour), IntervalSeconds: 86400, NextSlot: now.Add(22 * time.Hour).Truncate(time.Second)}
	wakeOf := func(known map[string]WakeFacts) func(string) WakeFacts {
		return func(queryGroup string) WakeFacts { return known[queryGroup] }
	}
	untracked := func(string) (Anomaly, bool) { return Anomaly{}, false }
	for _, test := range []struct {
		name  string
		rows  func(string) (Anomaly, bool)
		known map[string]WakeFacts
		want  *WakeFacts
	}{
		{name: "tracked, due entry held", rows: tracker.TrackedRow, known: map[string]WakeFacts{"qg-healthy": due}, want: &due},
		{name: "owned before any round is tracked, due entry held", rows: untracked, known: map[string]WakeFacts{"qg-healthy": due}, want: &due},
		{name: "tracked, no due entry yet", rows: tracker.TrackedRow, known: map[string]WakeFacts{}, want: &WakeFacts{}},
		{name: "neither", rows: untracked, known: map[string]WakeFacts{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, raw := detailOn(t, test.rows, wakeOf(test.known), nil)("/api/objects/qg-healthy")
			same := (body.Wake == nil) == (test.want == nil) && (body.Wake == nil || body.Wake.Known == test.want.Known &&
				body.Wake.IntervalSeconds == test.want.IntervalSeconds && body.Wake.NextSlot.Equal(test.want.NextSlot) &&
				body.Wake.DueAt.Equal(test.want.DueAt))
			if !same {
				t.Fatalf("wake = %+v, want %+v: %s", body.Wake, test.want, raw)
			}
			if test.want != nil && test.want.Known && !strings.Contains(raw, `"next_slot":"`+due.NextSlot.UTC().Format(time.RFC3339)) {
				t.Fatalf("the answer does not carry next_slot by name: %s", raw)
			}
		})
	}
	if body, raw := detailOn(t, tracker.TrackedRow, nil, nil)("/api/objects/qg-healthy"); body.Wake != nil || strings.Contains(raw, `"wake"`) {
		t.Fatalf("with no due index bound the answer names a wake: %s", raw)
	}
}

// An object's Plans' segments are on its detail as read, and when they
// cannot be read the detail says why by one closed word: no open Segment
// (the object runs no Plan here now), or the read failed. Unbound, the
// detail names neither.
func TestAnObjectsPlanSegmentsAreOnItsDetailOrWhyNot(t *testing.T) {
	at := now.Add(-time.Minute)
	reading := segment.PlansReading{QueryGroup: "qg-healthy", Total: 3, LastRecorded: true, Plans: []segment.PlanReading{{
		Plan:       alarmdexecution.PlanIdentity{TenantID: "system", BusinessID: "2", StrategyID: "9"},
		Generation: "g1", Mark: segment.MarkFound, Round: 1_800_000_000, Position: 1_799_999_940, Pages: 2,
		Renewed:      []segment.Renewal{{At: 1_799_999_000_000, TTL: 600_000}, {At: 1_799_999_100_000, TTL: 600_000}},
		RenewedTotal: 2,
		Last:         &segment.LastUse{ReadSource: segment.SourceCache, ReadReason: segment.ReadHit, ReadAt: &at, WriteResult: segment.WriteApplied, WriteAt: &at},
	}}}
	asked := ""
	body, raw := detailOn(t, func(string) (Anomaly, bool) { return Anomaly{}, false }, nil,
		func(_ context.Context, queryGroup string) (segment.PlansReading, error) {
			asked = queryGroup
			return reading, nil
		})("/api/objects/qg-healthy")
	if asked != "qg-healthy" || body.PlanSegments == nil || body.PlanSegmentsUnavailable != "" {
		t.Fatalf("asked %q, detail %s", asked, raw)
	}
	if !reflect.DeepEqual(body.PlanSegments.Plans[0].Plan, reading.Plans[0].Plan) || body.PlanSegments.Plans[0].Position != reading.Plans[0].Position ||
		!reflect.DeepEqual(body.PlanSegments.Plans[0].Renewed, reading.Plans[0].Renewed) || body.PlanSegments.Total != 3 ||
		!body.PlanSegments.LastRecorded || body.PlanSegments.Plans[0].Last.ReadReason != segment.ReadHit ||
		body.PlanSegments.Plans[0].Last.WriteResult != segment.WriteApplied || !body.PlanSegments.Plans[0].Last.ReadAt.Equal(at) {
		t.Fatalf("plan_segments = %+v, want %+v", body.PlanSegments, reading)
	}
	for _, word := range []string{`"position":1799999940`, `"pages":2`, `"renewed":[{"at":1799999000000,"ttl":600000}`, `"read_source":"cache"`, `"read_reason":"hit"`, `"write_result":"applied"`} {
		if !strings.Contains(raw, word) {
			t.Fatalf("the detail does not carry %s: %s", word, raw)
		}
	}
	for _, test := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("open plans: %w", controlplane.ErrScheduleUnavailable), "no_open_segment"},
		{errors.New("redis: i/o timeout"), "read_failed"},
	} {
		body, raw := detailOn(t, func(string) (Anomaly, bool) { return Anomaly{}, false }, nil,
			func(context.Context, string) (segment.PlansReading, error) { return segment.PlansReading{}, test.err })("/api/objects/qg-healthy")
		if body.PlanSegments != nil || body.PlanSegmentsUnavailable != test.want || strings.Contains(raw, "i/o timeout") {
			t.Fatalf("on %v the detail answers %s, want plan_segments_unavailable %s", test.err, raw, test.want)
		}
	}
	if _, raw := detailOn(t, func(string) (Anomaly, bool) { return Anomaly{}, false }, nil, nil)("/api/objects/qg-healthy"); strings.Contains(raw, "plan_segments") {
		t.Fatalf("unbound, the detail names plan segments: %s", raw)
	}
}
