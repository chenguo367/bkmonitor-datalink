// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
)

// A CLI client does not reuse a connection the network may have cut while it
// sat idle. Against a server that closes a connection idle for a second, a
// call after a pause succeeds with the CLI's bound below that, and fails on
// go-redis's own five minutes -- the call the CLI used to lose.
func TestACLIClientDoesNotReuseAConnectionCutWhileIdle(t *testing.T) {
	address, admin := startPhaseTwoRedis(t)
	if err := admin.ConfigSet(context.Background(), "timeout", "1").Err(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Redis.Address = address
	connection := cfg.RuntimeStoreRedis()
	pause := func(options *redis.UniversalOptions) error {
		client := redis.NewUniversalClient(options)
		defer client.Close()
		ctx := context.Background()
		if err := client.Get(ctx, "absent").Err(); err != redis.Nil {
			t.Fatalf("first call: %v", err)
		}
		time.Sleep(2500 * time.Millisecond)
		err := client.Get(ctx, "absent").Err()
		if err == redis.Nil {
			return nil
		}
		return err
	}

	saved := cliIdleTimeout
	t.Cleanup(func() { cliIdleTimeout = saved })
	cliIdleTimeout = 500 * time.Millisecond
	if err := pause(cliRedisOptions(connection)); err != nil {
		t.Fatalf("with the CLI's idle bound below the cut, the call after a pause failed: %v", err)
	}
	unbounded := cliRedisOptions(connection)
	unbounded.IdleTimeout = 0 // go-redis's default, five minutes
	if err := pause(unbounded); err == nil {
		t.Fatal("without the bound the call after a pause succeeded: the server's cut is not being exercised")
	}
}

// The CLI's bound sits far below any idle cut a network makes, and above a
// request's burst of commands.
func TestTheCLIIdleBoundIsAtTheScaleOfARequest(t *testing.T) {
	options := cliRedisOptions(config.Default().RuntimeStoreRedis())
	if options.IdleTimeout != cliIdleTimeout || cliIdleTimeout < time.Second || cliIdleTimeout > 30*time.Second ||
		options.PoolSize != 2 || options.MinIdleConns != 0 || options.MaxRetries != -1 {
		t.Fatalf("options idle %v pool %d min idle %d retries %d", options.IdleTimeout, options.PoolSize, options.MinIdleConns, options.MaxRetries)
	}
}
