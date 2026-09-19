// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"testing"
	"time"
)

// outputEntry is the output_kafka endpoint entry as the replica publishes it
// once the sink is opened lazily: ready or not, the attempts so far, and the
// last failure through the log's redaction.
func outputEntry(ready bool, attempts int, failure string) Endpoint {
	entry := Endpoint{Role: EndpointOutputKafka, Kind: "kafka", Address: "kafka-0:9092", Prefix: "alarmd_event",
		Configured: true, Ready: &ready}
	if attempts > 0 {
		entry.Attempts = &attempts
	}
	if failure != "" {
		age := 12.0
		entry.LastFailureAgeSeconds, entry.LastFailure = &age, failure
	}
	return entry
}

// A replica whose output sink is not open is a standing of its own: up,
// ready for nothing, assigned nothing, and on no object row. The view reads
// every replica's own entry -- not the one list it shows -- and the
// standing carries how long (since the process started, because a sink
// once open stays open), how many attempts, and what the last one said.
// The verdict is DEGRADED for it, as for any replica-level standing.
func TestAReplicaWhoseOutputIsNotReadyIsAStandingWithItsFacts(t *testing.T) {
	snapshots := idleSnapshots()
	// pod-a opened its output at once; its list is the newest and is the one
	// the view shows. It is aggregated first (the view walks the expected
	// replicas in order), so a reading that consulted the shown list rather
	// than each replica's own would find an open output when it reached
	// pod-b, and have nothing to say.
	snapshots[0].TakenAt = now.Add(-5 * time.Second)
	snapshots[0].StartedAt = now.Add(-2 * time.Hour)
	snapshots[0].Dependencies = []Endpoint{outputEntry(true, 1, "")}
	// pod-b restarted five minutes ago and has not opened its output since.
	snapshots[1].StartedAt = now.Add(-5 * time.Minute)
	snapshots[1].Dependencies = []Endpoint{outputEntry(false, 12, "kafka: dial tcp 10.0.0.1:9092: i/o timeout")}
	view := Aggregate(Expectation{QueryGroups: 0, Known: true}, snapshots, replicas(), now, freshness)
	if view.DependenciesReplica != "pod-a" {
		t.Fatalf("dependencies shown from %q, want the newest list, pod-a's", view.DependenciesReplica)
	}
	var notReady []Degradation
	for _, degradation := range view.Degradations {
		if degradation.Kind == DegradationOutputNotReady {
			notReady = append(notReady, degradation)
		}
	}
	if len(notReady) != 1 {
		t.Fatalf("degradations = %+v, want exactly one OUTPUT_NOT_READY, pod-b's", view.Degradations)
	}
	standing := notReady[0]
	if standing.Replica != "pod-b" || standing.Stage != "output" || standing.Text != "kafka: dial tcp 10.0.0.1:9092: i/o timeout" {
		t.Errorf("standing = %+v, want pod-b's output failure", standing)
	}
	if standing.AgeSeconds == nil || *standing.AgeSeconds != 300 {
		t.Errorf("age = %v, want 300 seconds: not ready since the process started", standing.AgeSeconds)
	}
	if standing.Attempts == nil || *standing.Attempts != 12 {
		t.Errorf("attempts = %v, want the sink's 12", standing.Attempts)
	}
	if view.Health != HealthDegraded {
		t.Errorf("health = %s, want DEGRADED for a replica that cannot open its output", view.Health)
	}
	// The line folds it under REPLICA_DEGRADED with the facts whole, so the
	// page can say it of each replica.
	var line *CheckReport
	for _, report := range ReportChecks(nil, nil, &view, now) {
		if report.Code == CheckReplicaDegraded {
			copied := report
			line = &copied
		}
	}
	if line == nil {
		t.Fatal("no REPLICA_DEGRADED line for a replica whose output is not ready")
	}
	var group *CheckGroup
	for index := range line.Groups {
		if line.Groups[index].Key == string(DegradationOutputNotReady) {
			group = &line.Groups[index]
		}
	}
	if group == nil || len(group.Replicas) != 1 || group.Replicas[0] != "pod-b" {
		t.Fatalf("groups = %+v, want an OUTPUT_NOT_READY fold naming pod-b", line.Groups)
	}
	if len(group.Degradations) != 1 || group.Degradations[0].AgeSeconds == nil || *group.Degradations[0].AgeSeconds != 300 ||
		group.Degradations[0].Attempts == nil || *group.Degradations[0].Attempts != 12 {
		t.Errorf("fold carries %+v, want the standing whole with its age and attempts", group.Degradations)
	}
}

// An open output is no standing, and an entry from a build before the
// readiness fact existed says nothing either way: the page must not read
// "no fact" as "not ready".
func TestAnOpenOrUnreportedOutputIsNoStanding(t *testing.T) {
	for name, entry := range map[string]Endpoint{
		"open":       outputEntry(true, 3, ""),
		"unreported": {Role: EndpointOutputKafka, Kind: "kafka", Address: "kafka-0:9092", Configured: true},
	} {
		snapshots := idleSnapshots()
		snapshots[0].StartedAt = now.Add(-5 * time.Minute)
		snapshots[0].Dependencies = []Endpoint{entry}
		view := Aggregate(Expectation{QueryGroups: 0, Known: true}, snapshots, replicas(), now, freshness)
		for _, degradation := range view.Degradations {
			if degradation.Kind == DegradationOutputNotReady {
				t.Errorf("%s output read as not ready: %+v", name, degradation)
			}
		}
		if view.Health == HealthDegraded {
			t.Errorf("%s output degrades the verdict: %+v", name, view.Degradations)
		}
	}
}
