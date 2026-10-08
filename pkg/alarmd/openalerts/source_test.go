// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

// The Redis source against a real server, on the contract's own keys: a
// missing heartbeat is nil (not an error), a malformed one is an error kept
// beside a nil heartbeat, an absent set is absent from Sets, and members
// come back as written. Also the one Redis fact the cache relies on: an
// emptied SET is a missing key.
func TestRedisSourceReadsTheContractKeys(t *testing.T) {
	executable := redistest.Server(t)
	address := reserveTCPAddress(t)
	startRedisServer(t, executable, address)
	client := redis.NewClient(&redis.Options{Addr: address, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })
	waitRedisReady(t, client)
	ctx := context.Background()
	source, err := NewRedisSource(client)
	if err != nil {
		t.Fatal(err)
	}
	source.batch = 2 // exercise the chunking on three keys
	keys := []StrategyKey{keyA, keyB, {TenantID: tenant, StrategyID: "1003"}}

	publication, err := source.Read(ctx, keys)
	if err != nil || publication.Heartbeat != nil || publication.HeartbeatErr != nil || len(publication.Sets) != 0 {
		t.Fatalf("empty store: %+v, %v; want nil heartbeat, no error, no sets", publication, err)
	}
	// A set present without a heartbeat is not read: it is not the
	// consumer's word without one, and the cache would not use it.
	if err := client.SAdd(ctx, SetKey(keyB), "orphan").Err(); err != nil {
		t.Fatal(err)
	}
	publication, err = source.Read(ctx, keys)
	if err != nil || len(publication.Sets) != 0 {
		t.Fatalf("sets without a heartbeat = %v, want none read", publication.Sets)
	}
	if err := client.Del(ctx, SetKey(keyB)).Err(); err != nil {
		t.Fatal(err)
	}

	if err := client.HSet(ctx, HeartbeatKey, HeartbeatPublishedAt, "1700000000", HeartbeatCycleSeconds, "60").Err(); err != nil {
		t.Fatal(err)
	}
	publication, err = source.Read(ctx, keys)
	if err != nil || publication.Heartbeat != nil || publication.HeartbeatErr == nil {
		t.Fatalf("heartbeat without a version: %+v, %v; want an unreadable heartbeat", publication, err)
	}

	if err := client.HSet(ctx, HeartbeatKey, HeartbeatFingerprintVersion, FingerprintVersion).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, SetKey(keyA), "f1", "f2").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, SetKey(keys[2]), "f9").Err(); err != nil {
		t.Fatal(err)
	}
	publication, err = source.Read(ctx, keys)
	if err != nil || publication.Heartbeat == nil || publication.HeartbeatErr != nil {
		t.Fatalf("complete heartbeat: %+v, %v", publication, err)
	}
	if publication.Heartbeat.Cycle != time.Minute || publication.Heartbeat.PublishedAt.Unix() != 1_700_000_000 || publication.Heartbeat.FingerprintVersion != FingerprintVersion {
		t.Fatalf("heartbeat = %+v", *publication.Heartbeat)
	}
	if len(publication.Sets) != 2 || len(publication.Sets[keyA]) != 2 || len(publication.Sets[keys[2]]) != 1 {
		t.Fatalf("sets = %v, want keyA with two members and 1003 with one, keyB absent", publication.Sets)
	}
	if _, present := publication.Sets[keyB]; present {
		t.Fatal("a strategy with no key is absent from Sets, not present and empty")
	}

	if err := client.SRem(ctx, SetKey(keyA), "f1", "f2").Err(); err != nil {
		t.Fatal(err)
	}
	exists, err := client.Exists(ctx, SetKey(keyA)).Result()
	if err != nil || exists != 0 {
		t.Fatalf("an emptied SET still exists (%d, %v); the cache reads empty as absent on this fact", exists, err)
	}
	publication, err = source.Read(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := publication.Sets[keyA]; present {
		t.Fatal("an emptied set is absent")
	}
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	return redistest.FreeAddress(t)
}

func startRedisServer(t *testing.T, _ string, address string) {
	t.Helper()
	redistest.StartAt(t, address)
}

func waitRedisReady(t *testing.T, client *redis.Client) {
	t.Helper()
	// The server answered before StartAt returned; what is left is this
	// client's own connection, under the same bound.
	deadline := time.Now().Add(redistest.ReadyWithin)
	for time.Now().Before(deadline) {
		if err := client.Ping(context.Background()).Err(); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("redis-server did not become ready")
}
