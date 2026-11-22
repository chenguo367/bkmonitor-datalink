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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// fallbackLine is one Plan's fallback page as the worker reports it: the
// line observability.ObserveSegmentFallback writes for a round's settle, in
// the Slot of the object.
func fallbackLine(observer observability.Observer, queryGroup string, slot int64, facts observability.SegmentFallbackFacts) {
	observability.ObserveSegmentFallback(context.Background(), observer,
		observability.TraceFields{QueryGroupKey: queryGroup, EvaluationTime: slot}, observability.OperationNormal, facts)
}

func fallbackFacts(strategy string, page int, result string, suppressed uint64) observability.SegmentFallbackFacts {
	return observability.SegmentFallbackFacts{TenantID: "system", BusinessID: "2", StrategyID: strategy, Generation: "g-1",
		Page: page, Result: result, SinceRenewalMillis: 31_000, RenewalTTLMillis: 60_000, TTLNowMillis: 60_000,
		IntervalSeconds: 60, LastMarkEvaluationTime: 1_700_000_000, Suppressed: suppressed}
}

// A replica keeps the latest MaxFallbackWritesKept pages its rounds wrote
// whole because renewals did not keep them alive, the latest first, each
// with its line's facts, its object, its Slot and when it was seen, and
// counts the older ones it pushed out. A fallback by design
// (fallback_unrenewable) is not kept, and the pages outlive the objects'
// state: a Query Group this replica let go keeps its rows.
func TestAReplicaKeepsItsLatestFallbackWritesAndCountsTheOlderOut(t *testing.T) {
	base := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	at := &clock{at: base}
	tracker := newTracker(t, at)
	if rows, evicted := tracker.FallbackWrites(); rows != nil || evicted != 0 {
		t.Fatalf("a replica that wrote no page whole: %+v, %d pushed out", rows, evicted)
	}
	var want []FallbackWriteFacts
	for index := 0; index < MaxFallbackWritesKept+2; index++ {
		at.at = base.Add(time.Duration(index) * time.Second)
		queryGroup, slot := fmt.Sprintf("qg-%02d", index), int64(1_700_000_060+60*index)
		facts := fallbackFacts(fmt.Sprint(100+index), index%3, observability.SegmentRenewalFallbackWrite, uint64(index))
		fallbackLine(tracker, queryGroup, slot, facts)
		want = append([]FallbackWriteFacts{{At: at.at, QueryGroup: queryGroup, EvaluationTime: slot, SegmentFallbackFacts: facts}}, want...)
		if index == 4 {
			fallbackLine(tracker, "qg-unrenewable", slot, fallbackFacts("999", 0, observability.SegmentRenewalFallbackUnrenewable, 0))
		}
	}
	tracker.Forget(map[string]struct{}{})
	rows, evicted := tracker.FallbackWrites()
	if !reflect.DeepEqual(rows, want[:MaxFallbackWritesKept]) || evicted != 2 {
		t.Fatalf("kept %+v, %d pushed out; want qg-17 down to qg-02 as reported, 2 pushed out", rows, evicted)
	}
	for _, row := range rows {
		if row.QueryGroup == "qg-unrenewable" {
			t.Fatalf("a fallback by design is kept: %+v", row)
		}
	}
}

// The deployment's health names every replica's fallback_write pages
// together, read from the summaries as published: the latest first, each
// naming its replica, cut to MaxFallbackWrites across the replicas, with the
// count of those the replicas pushed out and the cut left out. The public
// summary leaves them out.
func TestTheDeploymentsHealthNamesTheLatestFallbackWritesAcrossReplicas(t *testing.T) {
	at := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	var ids []string
	snapshot := func(replica string, rows int, offset time.Duration, evicted uint64) Snapshot {
		held := replica + "-held"
		ids = append(ids, held)
		s := Snapshot{Replica: replica, TakenAt: at, Owned: 1, Determined: 1, OwnedObjects: []string{held},
			FallbackWritesEvicted: evicted}
		for index := 0; index < rows; index++ {
			s.FallbackWrites = append(s.FallbackWrites, FallbackWriteFacts{
				At:         at.Add(-offset - time.Duration(index)*time.Minute).Truncate(time.Second),
				QueryGroup: fmt.Sprintf("%s-qg-%02d", replica, index), EvaluationTime: 1_700_000_000,
				SegmentFallbackFacts: fallbackFacts(fmt.Sprint(index), 1, observability.SegmentRenewalFallbackWrite, 0)})
		}
		return s
	}
	// pod-b's pages are seen 30 s after pod-a's; pod-c's 15 s after: across
	// the three, b, c, a at each minute.
	snapshots := []Snapshot{snapshot("pod-a", 16, time.Minute, 3), snapshot("pod-b", 16, 30*time.Second, 1),
		snapshot("pod-c", 4, 45*time.Second, 0)}
	health := healthFromSummaries(t, snapshots, Expectation{QueryGroups: len(ids), Known: true, IDs: ids},
		[]string{"pod-a", "pod-b", "pod-c"}, at, time.Minute)
	encoded, err := json.Marshal(health)
	if err != nil {
		t.Fatal(err)
	}
	var read struct {
		FallbackWrites        []FallbackWriteFacts `json:"fallback_writes"`
		FallbackWritesEvicted uint64               `json:"fallback_writes_evicted"`
	}
	if err := json.Unmarshal(encoded, &read); err != nil {
		t.Fatal(err)
	}
	var want []string
	for minute := 0; len(want) < MaxFallbackWrites; minute++ {
		for _, replica := range []string{"pod-b", "pod-c", "pod-a"} {
			if replica == "pod-c" && minute >= 4 {
				continue
			}
			want = append(want, fmt.Sprintf("%s/%s-qg-%02d", replica, replica, minute))
		}
	}
	want = want[:MaxFallbackWrites]
	var got []string
	for _, row := range read.FallbackWrites {
		got = append(got, row.Replica+"/"+row.QueryGroup)
		if row.StrategyID == "" || row.Result != observability.SegmentRenewalFallbackWrite || row.RenewalTTLMillis != 60_000 {
			t.Fatalf("row %s lost its line's facts: %+v", row.QueryGroup, row)
		}
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("health names\n%s\nwant\n%s", strings.Join(got, " "), strings.Join(want, " "))
	}
	// 3 and 1 pushed out by the replicas, 36 kept and 32 named.
	if read.FallbackWritesEvicted != 8 {
		t.Fatalf("health says %d pages not named, want 8", read.FallbackWritesEvicted)
	}
	if public, _ := json.Marshal(PublicHealth(health)); strings.Contains(string(public), "fallback_write") {
		t.Fatalf("the public summary names Plans: %s", public)
	}
}
