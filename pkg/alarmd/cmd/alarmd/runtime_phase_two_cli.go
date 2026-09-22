// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obevidence"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/platformsettings"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
)

type cliRuntimeFacts struct {
	Scope            string                            `json:"scope"`
	ReadAt           time.Time                         `json:"read_at"`
	Config           *observability.RuntimeConfigFacts `json:"config"`
	PlatformSettings *platformsettings.Observation     `json:"platform_settings"`
}

func cliRuntimeOperation(facts func() *observability.RuntimeConfigFacts, settings *platformsettings.Cache) obchannel.Operation {
	return obchannel.Operation{ID: "runtime.get", Summary: "读取回答请求的这一进程实际运行配置、预算、连接位置和已采用的动态配置；不是全副本一致性结论。", Fields: map[string]obchannel.Field{}, OutputSchema: obchannel.SchemaOf(cliRuntimeFacts{}), Limits: map[string]any{"redis_commands": 0, "scope": "answering_replica"}, Run: func(context.Context, obchannel.Params) obchannel.Outcome {
		value := cliRuntimeFacts{Scope: "answering_replica", ReadAt: time.Now().UTC(), Config: facts()}
		if settings != nil {
			observed := settings.Observe()
			value.PlatformSettings = &observed
		}
		out := obchannel.Outcome{Value: value, Complete: value.Config != nil && value.PlatformSettings != nil, Limitations: []string{"Use meta.answered_by to identify this process. These facts do not prove other replicas have the same version."}}
		return out
	}}
}

// buildPhaseTwoCLI allocates only small, lazy diagnostic pools. There is no
// startup Ping, refresh loop or detector dependency. Configuration or auth-store
// failures disable the CLI surface, never the existing execution path.
func buildPhaseTwoCLI(cfg config.Config, native http.Handler, catalog *controlplane.RedisCatalogRepository, progressStore *progress.Store, settings *platformsettings.Cache, facts func() *observability.RuntimeConfigFacts) (http.Handler, func() error) {
	if !cfg.CLI.Enabled {
		return native, func() error { return nil }
	}
	var clients []redis.UniversalClient
	closeClients := func() error {
		var errs []error
		for _, client := range clients {
			errs = append(errs, client.Close())
		}
		return errors.Join(errs...)
	}
	newClient := func(connection config.RedisConnectionConfig) redis.UniversalClient {
		options := observationRedisOptions(connection)
		options.PoolSize = 2
		options.MinIdleConns = 0
		client := redis.NewUniversalClient(options)
		clients = append(clients, client)
		return client
	}
	// Authentication has its own pool, so an evidence read cannot occupy it.
	manager, err := cliauth.New(cliauth.Options{Redis: newClient(cfg.RuntimeStoreRedis()), Prefix: cfg.Redis.StatePrefix, EnvironmentID: cfg.CLI.EnvironmentID, EnvironmentName: cfg.CLI.EnvironmentName, PublicBaseURL: cfg.CLI.PublicBaseURL, IssuerKey: cfg.CLI.IssuerKey})
	if err != nil {
		return composeCLI(native, nil, nil), closeClients
	}
	bind := func(role string, connection config.RedisConnectionConfig, prefix string) obevidence.RedisBinding {
		return obevidence.RedisBinding{Client: newClient(connection), Location: obevidence.Location{Role: role, Address: redisAddress(connection), Mode: connection.Mode, DB: connection.DB, Prefix: prefix}}
	}
	options := obevidence.Options{Catalog: catalog, Progress: progressStore}
	options.SourceStrategy = bind("strategy_cache", cfg.StrategySourceRedis(), cfg.PhaseTwo.Control.StrategyCachePrefix)
	// Catalog and progress share a diagnostic runtime pool, not detector I/O.
	options.Published = bind("runtime", cfg.RuntimeStoreRedis(), cfg.Redis.StatePrefix)
	options.QueryProgress = options.Published
	if connection, configured := cfg.TargetGroupRedis(); configured {
		options.TargetGroup = bind("target_group", connection, targetGroupPrefix(cfg))
	}
	if connection, configured := cfg.DynamicConfigRedis(); configured {
		options.DynamicConfig = bind("dynamic_config", connection, cfg.PhaseTwo.PlatformSettings.RedisKeyPrefix)
	}
	ops := append(obchannel.NativeOperations(native), obchannel.StoreOperations(obevidence.New(options))...)
	ops = append(ops, cliRuntimeOperation(facts, settings))
	channel, err := obchannel.New(obchannel.Options{Auth: manager, EnvironmentID: cfg.CLI.EnvironmentID, Replica: cfg.PhaseTwo.Worker.ID, Build: version + "/" + commit, Concurrency: 1, Operations: ops})
	if err != nil {
		return composeCLI(native, nil, nil), closeClients
	}
	return composeCLI(native, channel, manager.Handler()), closeClients
}

func composeCLI(native, channel, auth http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var handler http.Handler
		switch r.URL.Path {
		case "/api/cli/channel":
			handler = channel
		case "/api/cli/auth/grants", "/api/cli/auth/exchange", "/api/cli/session":
			handler = auth
		default:
			native.ServeHTTP(w, r)
			return
		}
		if handler != nil {
			handler.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": map[string]string{"code": "cli_not_configured", "message": "CLI configuration is invalid or unavailable; check environment identity, HTTPS public URL and issuer key configuration."}})
	})
}
