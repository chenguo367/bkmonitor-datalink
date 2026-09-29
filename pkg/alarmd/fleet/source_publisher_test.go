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
	"encoding/json"
	"testing"
	"time"
)

// The newest record any replica read is the one the verdict route carries,
// beside the source standing, and it is there on a deployment whose source
// round never succeeded -- no Source at all -- because that is the
// deployment it is for.
func TestHealthCarriesTheNewestPublisherRecordEvenWithoutASourceRound(t *testing.T) {
	blocked := int64(0)
	snapshots := healthySnapshots()
	snapshots[0].SourcePublisher = &SourcePublisherReport{State: "provided", Label: "写方自述", ReadAt: now.Add(-40 * time.Second),
		Outcome: "published", Strategies: &blocked}
	snapshots[1].SourcePublisher = &SourcePublisherReport{State: "provided", Label: "写方自述", ReadAt: now.Add(-10 * time.Second),
		Outcome: "blocked", Reason: "SPLIT_RECORDS_NOT_BACKFILLED", Strategies: &blocked}
	for index := range snapshots {
		snapshots[index].Source = nil
	}
	body := requestJSON(t, handlerWith(t, snapshots, Expectation{QueryGroups: 949, Known: true}, []string{"pod-a", "pod-b"}), "/api/health")
	report, ok := body["source_publisher"].(map[string]any)
	if !ok || report["outcome"] != "blocked" || report["reason"] != "SPLIT_RECORDS_NOT_BACKFILLED" || report["label"] != "写方自述" {
		t.Fatalf("source_publisher = %v, want pod-b's newer blocked record", body["source_publisher"])
	}
	// A zero count is a count, not an absent one.
	if report["strategies"] != float64(0) {
		t.Fatalf("strategies = %v, want 0 carried", report["strategies"])
	}
	if body["source_publisher_replica"] != snapshots[1].Replica {
		t.Fatalf("source_publisher_replica = %v, want %s", body["source_publisher_replica"], snapshots[1].Replica)
	}

	// Nobody read the source: nothing is said, rather than a record that
	// reads as a publisher saying nothing.
	plain := requestJSON(t, handlerWith(t, healthySnapshots(), Expectation{QueryGroups: 949, Known: true}, []string{"pod-a", "pod-b"}), "/api/health")
	if value, present := plain["source_publisher"]; present {
		t.Fatalf("source_publisher with no reader = %v, want absent", value)
	}
}

func TestAPublisherWithoutARecordIsLabelledNotProvidedNotEmpty(t *testing.T) {
	for state, label := range map[string]string{"provided": "写方自述", "not_provided": "写方未提供", "unreadable": "写方自述读不出"} {
		if SourcePublisherLabels[state] != label {
			t.Errorf("label of %s = %q, want %q", state, SourcePublisherLabels[state], label)
		}
	}
	if len(SourcePublisherLabels) != 3 {
		t.Errorf("labels = %v, want exactly the three states", SourcePublisherLabels)
	}
	encoded, err := json.Marshal(SourcePublisherReport{State: "not_provided", Label: SourcePublisherLabels["not_provided"], ReadAt: now})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["state"] != "not_provided" || decoded["label"] != "写方未提供" || decoded["read_at"] == nil {
		t.Fatalf("not provided = %s", encoded)
	}
	for _, field := range []string{"outcome", "reason", "strategies", "backfill", "detail"} {
		if _, present := decoded[field]; present {
			t.Errorf("not provided carries %s: %s", field, encoded)
		}
	}
}

// The public verdict route carries only what it lists; the publisher's
// record is not on it.
func TestThePublicHealthDoesNotCarryThePublisherRecord(t *testing.T) {
	full := HealthResponse{SourcePublisher: &SourcePublisherReport{State: "provided", Outcome: "blocked"}, SourcePublisherReplica: "pod-a"}
	public := PublicHealth(full)
	if public.SourcePublisher != nil || public.SourcePublisherReplica != "" {
		t.Fatalf("public health = %+v, want no publisher record", public.SourcePublisher)
	}
}
