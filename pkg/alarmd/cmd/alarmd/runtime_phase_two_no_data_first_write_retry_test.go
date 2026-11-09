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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// A first no-data write the pool has no room for ends its Slot
// RESOURCE_HARD_STOP after the round's threshold events were sent and its
// State written, with the memory unwritten and Progress where the round
// before left it (worker: TestAFirstNoDataWriteThePoolHasNoRoomForIsRetried).
// The case builds that store from a round that ran whole - its memory taken
// away and its Progress put back - rather than by filling the pool, and a new
// process then retries the round: the threshold events are not sent twice,
// since the State written before carries the round's decision, and the memory
// is written by the retry as the first write it was.
func TestAFirstNoDataWriteRetriedAfterTheThresholdEventsWentOutSendsThemOnce(t *testing.T) {
	strategy := lifecycleStrategy{revision: 7, noDataOff: true, targets: []string{lifecycleHostA, lifecycleHostB}}
	fixture := startLifecycleFixture(t, strategy, nil)
	ctx := context.Background()
	fixture.serve(false, 95, lifecycleHostA, lifecycleHostB)
	fixture.mustAttempt(1)
	changed := strategy
	changed.revision, changed.noDataOff, changed.continuous, changed.dimensions = 8, false, 1, lifecycleHostPair
	first := fixture.change(changed, 1)
	for round := int64(2); round < first; round++ {
		fixture.mustAttempt(round)
	}
	if keys := fixture.memoryKeys(); len(keys) != 0 {
		t.Fatalf("no-data memory keys %v before round %d, the first with no-data on; want none", keys, first)
	}
	progressKey := fixture.progressKey()
	progressBefore, err := fixture.redis.Dump(ctx, progressKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	ttl, err := fixture.redis.PTTL(ctx, progressKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 0 {
		ttl = 0
	}

	fixture.mustAttempt(first)
	at := fixture.evaluationAt(first)
	sentAt := func() int {
		count := 0
		for _, event := range fixture.thresholdEventsFor(lifecycleHostA) {
			if event.EvaluationTime == at && event.EventKind == contract.TriggerEventAbnormal {
				count++
			}
		}
		return count
	}
	if sent := sentAt(); sent != 1 {
		t.Fatalf("round %d sent host A %d ABNORMAL threshold events, want one", first, sent)
	}
	keys := fixture.memoryKeys()
	if len(keys) != 1 {
		t.Fatalf("round %d left no-data memory keys %v, want its first write", first, keys)
	}

	// The refusal's store: the events out and the State written, the memory
	// not, and Progress as the round before left it.
	fixture.closeBundle(fixture.bundle)
	if err := fixture.redis.Del(ctx, keys...).Err(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.redis.RestoreReplace(ctx, progressKey, ttl, progressBefore).Err(); err != nil {
		t.Fatal(err)
	}
	if next, _ := fixture.progressSlots(); next != at {
		t.Fatalf("Progress names %d as next, want round %d's Slot %d", next, first, at)
	}
	writesBefore := len(fixture.memoryWrites())
	fixture.open("alarmd-worker-0", nil)
	fixture.mustAttempt(first)

	if next, lastFull := fixture.progressSlots(); lastFull != at || next != fixture.evaluationAt(first+1) {
		t.Fatalf("after the retry Progress has last full %d and next %d, want round %d's Slot %d completed", lastFull, next,
			first, at)
	}
	if sent := sentAt(); sent != 1 {
		t.Fatalf("the retry of round %d sent host A %d ABNORMAL threshold events in all; the first attempt sent it already",
			first, sent)
	}
	if keys := fixture.memoryKeys(); len(keys) != 1 || len(fixture.memoryWrites()) == writesBefore {
		t.Fatalf("after the retry no-data memory keys are %v with %d writes since; want the first write made by the retry",
			keys, len(fixture.memoryWrites())-writesBefore)
	}
}
