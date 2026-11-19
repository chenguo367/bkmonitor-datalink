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
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The owned objects the lookback has no fresh measurement of are named the
// largest first - the series their Plans bound within the window, as the
// State cache's held series count them - names breaking ties, at most
// MaxLookbackUncovered of them, each keeping why and its times.
func TestTheLargestUncoveredObjectsAreNamedFirst(t *testing.T) {
	at := &clock{at: now}
	tracker := newTracker(t, at)
	evaluated := func(queryGroup, strategy string, matched int) {
		ctx := observability.ContextWithTraceFields(context.Background(), observability.TraceFields{QueryGroupKey: queryGroup})
		tracker.Observe(ctx, observability.Observation{
			Component: observability.ComponentEvaluation, Stage: observability.StageEvaluationCompleted,
			Result:            observability.ResultSuccess,
			Trace:             observability.TraceFields{StrategyID: strategy, BusinessID: "2", EvaluationTime: 600},
			PlanSeriesMatched: &matched,
		})
	}
	var uncovered []LookbackUncoveredFacts
	for index := 0; index < MaxLookbackUncovered+8; index++ {
		uncovered = append(uncovered, LookbackUncoveredFacts{QueryGroup: fmt.Sprintf("qg-%02d", index), Reason: "resting"})
	}
	// Two Plans on the largest, one long gone; a tie broken by name; and a
	// group the tracker has never seen, at nothing.
	evaluated("qg-39", "s-gone", 1_000_000)
	at.at = at.at.Add(2 * time.Hour)
	evaluated("qg-39", "s-1", 90_000)
	evaluated("qg-39", "s-2", 6_000)
	evaluated("qg-07", "s-3", 500)
	evaluated("qg-03", "s-4", 500)
	uncovered[39].Reason, uncovered[39].Since, uncovered[39].NextAt = "trial_blocking", now, now.Add(time.Minute)
	ranked := tracker.LargestUncovered(uncovered, time.Hour)
	if len(ranked) != MaxLookbackUncovered {
		t.Fatalf("%d named, want %d", len(ranked), MaxLookbackUncovered)
	}
	first := ranked[0]
	if first.QueryGroup != "qg-39" || first.Series != 96_000 || first.Reason != "trial_blocking" || !first.Since.Equal(now) ||
		!first.NextAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("first %+v, want qg-39 at 96000 series with its reason and times", first)
	}
	if ranked[1].QueryGroup != "qg-03" || ranked[2].QueryGroup != "qg-07" || ranked[1].Series != 500 {
		t.Fatalf("then %+v and %+v, want qg-03 and qg-07 at 500 by name", ranked[1], ranked[2])
	}
	if ranked[3].QueryGroup != "qg-00" || ranked[3].Series != 0 || ranked[MaxLookbackUncovered-1].QueryGroup != "qg-30" {
		t.Fatalf("the rest from %+v to %+v, want the unsized by name, qg-00 to qg-30", ranked[3], ranked[MaxLookbackUncovered-1])
	}
	if uncovered[0].Series != 0 || uncovered[39].Series != 0 {
		t.Fatal("ranking wrote into the caller's list")
	}
	if got := tracker.LargestUncovered(nil, time.Hour); got != nil {
		t.Fatalf("none uncovered named %v", got)
	}
}

// The deployment's health names every replica's uncovered objects together,
// read from the summaries the replicas publish as the reader reads them: the
// largest first, the same size by replica then Query Group, each naming its
// replica and why, cut to MaxLookbackUncovered across the replicas.
func TestTheDeploymentsHealthNamesTheLargestUncoveredObjectsAcrossReplicas(t *testing.T) {
	at := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
	var ids []string
	snapshot := func(replica, prefix string) Snapshot {
		s := Snapshot{Replica: replica, TakenAt: at, Owned: 20, Determined: 20}
		for index := 0; index < 20; index++ {
			qg := fmt.Sprintf("%s-%02d", prefix, index)
			ids = append(ids, qg)
			s.OwnedObjects = append(s.OwnedObjects, qg)
			series := uint64(0)
			if index < 10 {
				series = uint64(1000 - 10*index)
			}
			s.LookbackUncovered = append(s.LookbackUncovered, LookbackUncoveredFacts{QueryGroup: qg, Reason: "awaiting_first_read",
				Since: at.Add(-time.Hour), NextAt: at.Add(time.Minute), Series: series})
		}
		return s
	}
	// pod-b is listed first: the order is the replicas' names, not the read's.
	snapshots := []Snapshot{snapshot("pod-b", "qg-b"), snapshot("pod-a", "qg-a")}
	health := healthFromSummaries(t, snapshots, Expectation{QueryGroups: len(ids), Known: true, IDs: ids},
		[]string{"pod-a", "pod-b"}, at, time.Minute)
	encoded, err := json.Marshal(health)
	if err != nil {
		t.Fatal(err)
	}
	var read struct {
		LookbackUncovered []LookbackUncoveredFacts `json:"lookback_uncovered"`
	}
	if err := json.Unmarshal(encoded, &read); err != nil {
		t.Fatal(err)
	}
	got := read.LookbackUncovered
	if len(got) != MaxLookbackUncovered {
		t.Fatalf("health names %d uncovered objects, want %d of the 40", len(got), MaxLookbackUncovered)
	}
	var want []string
	for index := 0; index < 10; index++ {
		want = append(want, fmt.Sprintf("pod-a/qg-a-%02d", index), fmt.Sprintf("pod-b/qg-b-%02d", index))
	}
	for index := 10; index < 20; index++ {
		want = append(want, fmt.Sprintf("pod-a/qg-a-%02d", index))
	}
	want = append(want, "pod-b/qg-b-10", "pod-b/qg-b-11")
	for index, row := range got {
		if name := row.Replica + "/" + row.QueryGroup; name != want[index] {
			t.Fatalf("row %d is %s (%+v), want %s", index, name, row, want[index])
		}
		if row.Reason != "awaiting_first_read" || !row.NextAt.Equal(at.Add(time.Minute)) {
			t.Fatalf("row %d lost its reason or times: %+v", index, row)
		}
	}
	if got[0].Series != 1000 || got[19].Series != 910 || got[20].Series != 0 {
		t.Fatalf("series %d, %d, %d, want 1000, 910 and 0", got[0].Series, got[19].Series, got[20].Series)
	}
	if public, _ := json.Marshal(PublicHealth(health)); strings.Contains(string(public), "lookback_uncovered") {
		t.Fatalf("the public summary names Query Groups: %s", public)
	}
}
