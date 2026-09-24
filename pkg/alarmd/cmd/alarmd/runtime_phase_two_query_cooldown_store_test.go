// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// The pool record on a real Redis: what one owner writes the next reads
// back; an owner that has been replaced cannot write over its successor;
// nothing there, and a record that does not decode, are both no record; and
// the record expires on its own.
func TestTheQueryCooldownRecordIsFencedByOwnerOnRedis(t *testing.T) {
	_, client := startPhaseTwoRedis(t)
	ctx := context.Background()
	store := newRedisQueryCooldownStore(client, "test.cooldown")
	if _, found, err := store.LoadQueryCooldown(ctx, "qg"); found || err != nil {
		t.Fatalf("empty store = (found %t, %v), want no record and no error", found, err)
	}
	fence := func(epoch uint64) execution.OwnerFence {
		return execution.OwnerFence{QueryGroup: "qg", OwnerID: "worker", OwnerEpoch: epoch, LeaseToken: "token"}
	}
	entered := time.Unix(1_000, 0).UTC()
	successor := scheduler.QueryCooldownRecord{QueryGroup: "qg", OwnerEpoch: 2, EnteredAt: entered, Until: entered.Add(time.Minute), Failures: 4}
	if err := store.SaveQueryCooldown(ctx, fence(2), successor); err != nil {
		t.Fatal(err)
	}
	stale := successor
	stale.OwnerEpoch, stale.Until = 1, time.Time{}
	if err := store.SaveQueryCooldown(ctx, fence(1), stale); !errors.Is(err, errQueryCooldownSuperseded) {
		t.Fatalf("a replaced owner's save = %v, want refused", err)
	}
	got, found, err := store.LoadQueryCooldown(ctx, "qg")
	if err != nil || !found || got.OwnerEpoch != 2 || !got.Until.Equal(successor.Until) || !got.EnteredAt.Equal(entered) {
		t.Fatalf("record = (%+v, %t, %v), want the successor's", got, found, err)
	}
	// The same owner, and a later one, write.
	later := successor
	later.OwnerEpoch, later.Failures = 3, 5
	if err := store.SaveQueryCooldown(ctx, fence(3), later); err != nil {
		t.Fatal(err)
	}
	if ttl := client.PTTL(ctx, "test.cooldown:qg").Val(); ttl <= 0 || ttl > queryCooldownRecordTTL {
		t.Fatalf("record TTL = %s, want it to expire within %s", ttl, queryCooldownRecordTTL)
	}
	if err := client.Set(ctx, "test.cooldown:qg", "not json", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadQueryCooldown(ctx, "qg"); found || err == nil {
		t.Fatalf("undecodable record = (found %t, %v), want no record with the reason", found, err)
	}
	// A record that does not decode does not block the next owner's write.
	if err := store.SaveQueryCooldown(ctx, fence(1), stale); err != nil {
		t.Fatalf("a save over an undecodable record = %v, want it written", err)
	}
	if newRedisQueryCooldownStore(nil, "p") != nil {
		t.Fatal("a store without a client is not nil")
	}
}
