// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

func TestRedisBackendStoreRoundTripTTLAndReconnect(t *testing.T) {
	executable := redistest.Server(t)
	address := reserveTCPAddress(t)
	server := startRedisServer(t, executable, address)

	backend, err := NewRedisBackend(RedisBackendOptions{
		Address: address, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 2,
	})
	if err != nil {
		t.Fatalf("NewRedisBackend() error = %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	waitRedisReady(t, backend)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backend.MGet(cancelled, []string{"cancelled"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("MGet(cancelled) error = %v, want context.Canceled", err)
	}

	router, err := NewFixedRouter("state-01", backend)
	if err != nil {
		t.Fatalf("NewFixedRouter() error = %v", err)
	}
	codec, err := NewCodec(CodecLimits{MaxLevels: 4, MaxPoints: 16, MaxEncodedBytes: 4096})
	if err != nil {
		t.Fatalf("NewCodec() error = %v", err)
	}
	store, err := NewStore(StoreOptions{
		Prefix: "alarmd-integration", Codec: codec, Router: router,
		Limits: StoreLimits{MaxKeysPerBatch: 4, MaxKeyBytesPerBatch: 4096, MaxLoadedBytes: 16 << 10, MaxWrittenBytes: 16 << 10},
		MinTTL: time.Second, MaxTTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	requirement := requirement(5, "5", 1, 1)
	spec := LoadWindowSpec{Identity: storeIdentity("11", "a"), Requirements: []LevelRequirement{requirement}}
	loaded, err := store.LoadWindows(context.Background(), LoadWindowsRequest{Items: []LoadWindowSpec{spec}})
	if err != nil || loaded.Items[0].Status != LoadMissing {
		t.Fatalf("LoadWindows(missing) = (%+v, %v)", loaded, err)
	}
	mustApply(t, loaded.Items[0].Window, []StatePoint{point(100, "a", fact(requirement, LevelFactAnomalous))})
	written, err := store.WriteWindows(context.Background(), WriteWindowsRequest{Items: loaded.Items})
	if err != nil || written.Items[0].Status != WritePersisted {
		t.Fatalf("WriteWindows() = (%+v, %v)", written, err)
	}
	reloaded, err := store.LoadWindows(context.Background(), LoadWindowsRequest{Items: []LoadWindowSpec{spec}})
	if err != nil || reloaded.Items[0].Status != LoadFound {
		t.Fatalf("LoadWindows(found) = (%+v, %v)", reloaded, err)
	}
	history, _ := reloaded.Items[0].Window.History(5)
	if history.Summarize(100, 1).AnomalyCount != 1 {
		t.Fatal("binary packed state did not round-trip through Redis")
	}

	binaryValue := []byte{0, 1, 2, 0xff}
	if err := backend.SetMany(context.Background(), []BackendWrite{{Key: "binary", Value: binaryValue, TTL: 150 * time.Millisecond}}); err != nil {
		t.Fatalf("SetMany(binary) error = %v", err)
	}
	values, err := backend.MGet(context.Background(), []string{"missing", "binary"})
	if err != nil || values[0] != nil || !bytes.Equal(values[1], binaryValue) {
		t.Fatalf("MGet(binary) = (%v, %v)", values, err)
	}
	time.Sleep(250 * time.Millisecond)
	values, err = backend.MGet(context.Background(), []string{"binary"})
	if err != nil || values[0] != nil {
		t.Fatalf("MGet(expired) = (%v, %v)", values, err)
	}

	stopRedisServer(t, server)
	server = startRedisServer(t, executable, address)
	waitRedisReady(t, backend)
	if err := backend.Ping(context.Background()); err != nil {
		t.Fatalf("Ping(after restart) error = %v", err)
	}
	stopRedisServer(t, server)
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	return redistest.FreeAddress(t)
}

func startRedisServer(t *testing.T, _ string, address string) *redistest.Instance {
	t.Helper()
	return redistest.StartAt(t, address)
}

func stopRedisServer(t *testing.T, server *redistest.Instance) {
	t.Helper()
	server.Stop()
}

func waitRedisReady(t *testing.T, backend *RedisBackend) {
	t.Helper()
	// The wait is generous on purpose. Three seconds encoded an assumption about
	// machine load rather than about redis: under `go test ./...` dozens of
	// packages start their own server at the same moment, and a window that is
	// ample on an idle machine is not on a loaded one - which turned a whole-tree
	// "all green" into a function of load rather than of the code. A healthy
	// server answers Ping in milliseconds, so a longer budget costs the normal
	// path nothing; it only matters when the server genuinely cannot start, and
	// that case is meant to be read from the server's own output.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		lastErr = backend.Ping(ctx)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("redis-server at %s did not become ready: %v", fmt.Sprint(backend.Address()), lastErr)
}
