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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// partReplicas are tallyReplicas with pooled objects whose cooldowns have
// ended at different moments or not yet, and empty-every-round rows.
func partReplicas() []Snapshot {
	snapshots := tallyReplicas()
	for index := range snapshots {
		snapshot := &snapshots[index]
		ended := now.Add(-time.Duration(index+1) * 7 * time.Minute)
		snapshot.Demoted[0].QueryCooldown = &observability.QueryCooldownFacts{Until: ended}
		later := snapshot.Demoted[0]
		later.QueryGroup += "-later"
		later.QueryCooldown = &observability.QueryCooldownFacts{Until: now.Add(time.Hour)}
		snapshot.Demoted = append(snapshot.Demoted, later)
		snapshot.TotalDemoted++
		for n := 0; n <= index; n++ {
			snapshot.NoData = append(snapshot.NoData, Anomaly{QueryGroup: snapshot.Replica + "-empty-" + string(rune('a'+n)),
				Kind: KindEmptyEveryRound, ReasonCode: "FULL_EMPTY_COMPLETED", Replica: snapshot.Replica})
		}
	}
	return snapshots
}

func decidedView(snapshots []Snapshot) View {
	names := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		names = append(names, snapshot.Replica)
	}
	view := Aggregate(Expectation{QueryGroups: 300, Known: true}, snapshots, names, now, freshness)
	Decide(&view, now, 10*time.Minute)
	return view
}

// The replicas' parts, each read from a view of its own snapshot and
// merged, give every number the whole view gives from rows: the verdict's
// counts, each replica's split, the empty-every-round count, the pooled
// objects due and how long the earliest has waited, and the impact.
func TestReplicaPartsAddUpToTheWholeViewsRowNumbers(t *testing.T) {
	snapshots := partReplicas()
	whole := decidedView(snapshots)
	parts := make([]ReplicaPart, 0, len(snapshots))
	for _, snapshot := range snapshots {
		part := ReplicaPartOf(decidedView([]Snapshot{snapshot}), now)
		if part.Replica != snapshot.Replica {
			t.Fatalf("part of %s names %q", snapshot.Replica, part.Replica)
		}
		parts = append(parts, part)
		for _, replica := range whole.PerReplica {
			if replica.Replica != snapshot.Replica {
				continue
			}
			tally := part.Attribution
			if replica.Ours != tally.Ours || replica.External != tally.External || replica.Unattributed != tally.Unknown+tally.Other {
				t.Errorf("%s split ours/external/unattributed = %d/%d/%d, part %+v", replica.Replica,
					replica.Ours, replica.External, replica.Unattributed, tally)
			}
		}
	}
	merged := MergeReplicaParts(parts...)
	if merged.Attribution.Ours != OursCount(whole.Anomalies) || merged.Attribution.Unknown != UnattributedCount(whole.Anomalies) {
		t.Errorf("merged attribution %+v, whole view ours %d unattributed %d", merged.Attribution,
			OursCount(whole.Anomalies), UnattributedCount(whole.Anomalies))
	}
	if merged.EmptyEveryRound != whole.EmptyEveryRoundTotal {
		t.Errorf("empty every round %d, whole view %d", merged.EmptyEveryRound, whole.EmptyEveryRoundTotal)
	}
	if merged.DemotedDue != whole.DemotedDue || merged.DemotedDueOldestSeconds(now) != whole.DemotedDueOldestSeconds {
		t.Errorf("pooled due %d oldest %ds, whole view %d oldest %ds", merged.DemotedDue, merged.DemotedDueOldestSeconds(now),
			whole.DemotedDue, whole.DemotedDueOldestSeconds)
	}
	if merged.Impact.Impact() != ImpactOf(whole, now) {
		t.Errorf("impact %+v, whole view %+v", merged.Impact.Impact(), ImpactOf(whole, now))
	}
	// Numbers the fixture must reach, or the equalities above are between
	// zeros: several due with different waits, several empty rows, and rows
	// on each side of the verdict.
	if whole.DemotedDue != 3 || whole.DemotedDueOldestSeconds != 21*60 || whole.EmptyEveryRoundTotal != 6 ||
		OursCount(whole.Anomalies) == 0 || OursCount(whole.Anomalies) == len(whole.Anomalies) {
		t.Fatalf("fixture: due %d oldest %ds, empty %d, ours %d of %d", whole.DemotedDue, whole.DemotedDueOldestSeconds,
			whole.EmptyEveryRoundTotal, OursCount(whole.Anomalies), len(whole.Anomalies))
	}
}
