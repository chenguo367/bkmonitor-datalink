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
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// benchReplica is one replica at the size the design reads (§一): about 700
// objects, of which 120 anomalies, 100 pooled, 12 undecidable, 6 by design,
// 200 empty every round and 12 read early, one strategy per row from a
// catalogue the replicas share.
func benchReplica(index int) Snapshot {
	replica := fmt.Sprintf("pod-%03d", index)
	at := now.Add(-10 * time.Second)
	row := func(n int, kind, reason string) Anomaly {
		strategy := StrategyRef{StrategyID: fmt.Sprintf("%d", 1000+(index*37+n)%3330), BusinessID: fmt.Sprintf("%d", n%40)}
		return Anomaly{QueryGroup: fmt.Sprintf("%s-%d", replica, n), Kind: kind, CauseReason: reason, Replica: replica,
			Since: now.Add(-time.Hour), SinceFrom: SinceBusinessState, Strategies: []StrategyRef{strategy},
			Wake: &WakeFacts{Known: true, IntervalSeconds: int64(60 * (1 + n%3)), DueAt: now.Add(time.Minute)}}
	}
	snapshot := Snapshot{Replica: replica, TakenAt: at, StartedAt: now.Add(-2 * time.Hour), Owned: 700, Determined: 700}
	n := 0
	for ; n < 120; n++ {
		snapshot.Anomalies = append(snapshot.Anomalies, row(n, KindDegradedRun, "QUERY_TIMEOUT"))
	}
	for ; n < 220; n++ {
		pooled := row(n, KindQueryCooldown, "QUERY_UNAVAILABLE")
		pooled.QueryCooldown = &observability.QueryCooldownFacts{Until: now.Add(time.Duration(n%7-3) * time.Minute)}
		snapshot.Demoted = append(snapshot.Demoted, pooled)
	}
	for ; n < 232; n++ {
		snapshot.Undecidable = append(snapshot.Undecidable, row(n, KindDegradedRun, "HISTORY_WARMING"))
	}
	for ; n < 238; n++ {
		snapshot.ByDesign = append(snapshot.ByDesign, row(n, KindDegradedRun, "CONFIG_DRIFT"))
	}
	for ; n < 438; n++ {
		empty := row(n, KindEmptyEveryRound, "")
		empty.ReasonCode = "FULL_EMPTY_COMPLETED"
		snapshot.NoData = append(snapshot.NoData, empty)
	}
	for ; n < 450; n++ {
		early := row(n, KindDegradedRun, "")
		early.ReadEarly = &ReadEarlyFacts{SuggestedDelaySeconds: int64(30 * (n % 4))}
		snapshot.ReadEarly = append(snapshot.ReadEarly, early)
	}
	snapshot.TotalAnomalies, snapshot.TotalDemoted = len(snapshot.Anomalies), len(snapshot.Demoted)
	snapshot.TotalUndecidable, snapshot.TotalByDesign = len(snapshot.Undecidable), len(snapshot.ByDesign)
	return snapshot
}

func benchParts(b *testing.B, replicas int) []ReplicaPart {
	b.Helper()
	parts := make([]ReplicaPart, 0, replicas)
	for index := 0; index < replicas; index++ {
		parts = append(parts, ReplicaPartOf(decidedView([]Snapshot{benchReplica(index)}), now))
	}
	return parts
}

// What a replica spends on its part once per publish.
func BenchmarkReplicaPartOf(b *testing.B) {
	view := decidedView([]Snapshot{benchReplica(0)})
	b.ResetTimer()
	var part ReplicaPart
	for i := 0; i < b.N; i++ {
		part = ReplicaPartOf(view, now)
	}
	b.StopTimer()
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(part); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(encoded.Len()), "gob-bytes/part")
}

// What the leader spends folding every replica's part, at the replica
// count of the deployment read today and of the one the design sizes for.
func BenchmarkMergeReplicaParts(b *testing.B) {
	for _, replicas := range []int{4, 210} {
		b.Run(fmt.Sprintf("replicas=%d", replicas), func(b *testing.B) {
			parts := benchParts(b, replicas)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				merged := MergeReplicaParts(parts...)
				_ = merged.Impact.Impact()
			}
		})
	}
}
