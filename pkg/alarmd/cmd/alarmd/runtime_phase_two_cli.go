// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/go-redis/redis/v8"

	accessuq "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access/uq"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/evidenceroute"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/k8sread"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obevidence"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/platformsettings"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

type cliRuntimeFacts struct {
	Scope            string                            `json:"scope"`
	ReadAt           time.Time                         `json:"read_at"`
	Config           *observability.RuntimeConfigFacts `json:"config"`
	PlatformSettings *platformsettings.Observation     `json:"platform_settings"`
}

type cliControlBinding struct {
	Incarnation string
	StreamToken string
	Server      *viewstream.Server
}

func cliRuntimeOperation(facts func() *observability.RuntimeConfigFacts, settings *platformsettings.Cache) obchannel.Operation {
	return obchannel.Operation{ID: "runtime.get", Summary: "读取实际回答进程的运行配置、预算、连接位置和已采用动态配置；可指定实例或按执行租约定位逻辑 Worker。", EvidenceScope: "process", Targetable: true, Fields: map[string]obchannel.Field{}, OutputSchema: obchannel.SchemaOf(cliRuntimeFacts{}), Limits: map[string]any{"redis_commands": 0, "scope": "answering_replica"}, Run: func(context.Context, obchannel.Params) obchannel.Outcome {
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
func buildPhaseTwoCLI(cfg config.Config, native http.Handler, catalog *controlplane.RedisCatalogRepository, progressStore *progress.Store, settings *platformsettings.Cache, facts func() *observability.RuntimeConfigFacts, control cliControlBinding) (http.Handler, func() error) {
	if !cfg.CLI.Enabled {
		return native, func() error { return nil }
	}
	var clients []redis.UniversalClient
	var closeQuery func()
	closeClients := func() error {
		if closeQuery != nil {
			closeQuery()
		}
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
	manager, err := cliauth.New(cliauth.Options{Redis: newClient(cfg.RuntimeStoreRedis()), Prefix: cfg.Redis.StatePrefix, EnvironmentID: cfg.CLI.EnvironmentID, EnvironmentName: cfg.CLI.EnvironmentName, PublicBaseURL: cfg.CLI.PublicBaseURL, AdminKey: cfg.CLI.AdminKey})
	if err != nil {
		return composeCLI(native, nil, nil), closeClients
	}
	bind := func(role string, connection config.RedisConnectionConfig, prefix string) obevidence.RedisBinding {
		return obevidence.RedisBinding{Client: newClient(connection), Location: obevidence.Location{Role: role, Address: redisAddress(connection), Mode: connection.Mode, DB: connection.DB, Prefix: prefix}}
	}
	options := obevidence.Options{Catalog: catalog, Progress: progressStore}
	options.SourceStrategy = bind("strategy_cache", cfg.StrategySourceRedis(), cfg.PhaseTwo.Control.StrategyCachePrefix)
	// Catalog and progress share a diagnostic runtime pool, not detector I/O.
	runtimeConnection := cfg.RuntimeStoreRedis()
	diagnosticRuntime := newClient(runtimeConnection)
	options.Published = obevidence.RedisBinding{Client: diagnosticRuntime, Location: obevidence.Location{Role: "runtime", Address: redisAddress(runtimeConnection), Mode: runtimeConnection.Mode, DB: runtimeConnection.DB, Prefix: cfg.Redis.StatePrefix}}
	options.QueryProgress = options.Published
	// Route discovery is evidence I/O too. Reuse the diagnostic runtime pool,
	// never the production ownership connection or its startup readiness path.
	routingStore, err := ownership.NewRedisStoreWithClient(diagnosticRuntime, productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "ownership"))
	if err != nil {
		return composeCLI(native, nil, nil), closeClients
	}
	if connection, configured := cfg.TargetGroupRedis(); configured {
		options.TargetGroup = bind("target_group", connection, targetGroupPrefix(cfg))
	}
	if connection, configured := cfg.DynamicConfigRedis(); configured {
		options.DynamicConfig = bind("dynamic_config", connection, cfg.PhaseTwo.PlatformSettings.RedisKeyPrefix)
	}
	ops := append(obchannel.NativeOperations(native), obchannel.StoreOperations(obevidence.New(options))...)
	ops = append(ops, cliRuntimeOperation(facts, settings))
	// alarmd's own workload, read through this Pod's ServiceAccount. Any
	// replica answers, so a crashing one is read from one that is up.
	// The owner chain starts at this Pod: its hostname is its name, whatever
	// the worker id is configured to.
	podName, _ := os.Hostname()
	ops = append(ops, obchannel.K8sOperations(k8sread.New(k8sread.Options{PodName: podName}))...)
	// A diagnostic query has independent sockets, no retries and no production
	// query permits. It never occupies the execution client's connection pool.
	queryTransport := &http.Transport{Proxy: http.ProxyFromEnvironment,
		DialContext:     (&net.Dialer{Timeout: obchannel.RequestTimeout}).DialContext,
		MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: obchannel.RequestTimeout,
		ResponseHeaderTimeout: obchannel.RequestTimeout}
	closeQuery = queryTransport.CloseIdleConnections
	queryClient, _ := accessuq.NewDiagnosticClient(cfg.PhaseTwo.Access.UQEndpoint, cfg.PhaseTwo.Access.QuerySource,
		&http.Client{Transport: queryTransport, Timeout: obchannel.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	ops = append(ops, obchannel.SlotOperations(obchannel.SlotOptions{Resolve: newCLISlotResolver(cfg, diagnosticRuntime),
		Evidence: newCLISlotEvidenceReader(cfg, diagnosticRuntime), UQ: queryClient})...)
	var router *evidenceroute.Router
	channelOptions := obchannel.Options{Auth: manager, EnvironmentID: cfg.CLI.EnvironmentID, Replica: cfg.PhaseTwo.Worker.ID, Incarnation: control.Incarnation, Build: version + "/" + commit, Concurrency: 1, Operations: ops}
	if control.Server != nil {
		channelOptions.Route = func(ctx context.Context, call obchannel.Invocation) obchannel.Response {
			return router.Invoke(ctx, call)
		}
	}
	channel, err := obchannel.New(channelOptions)
	if err != nil {
		return composeCLI(native, nil, nil), closeClients
	}
	if control.Server != nil {
		router, err = evidenceroute.New(evidenceroute.Options{Store: routingStore, WorkerID: cfg.PhaseTwo.Worker.ID, StreamToken: control.StreamToken, EnvironmentID: cfg.CLI.EnvironmentID,
			Build: channelOptions.Build, Incarnation: control.Incarnation, CatalogRevision: channel.CatalogRevision(), Execute: channel.ExecuteEvidence})
		if err != nil {
			return composeCLI(native, nil, nil), closeClients
		}
		control.Server.SetEvidenceHandler(router.Handle)
	}
	return composeCLI(native, channel, manager.Handler()), closeClients
}

func composeCLI(native, channel, auth http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var handler http.Handler
		switch {
		case r.URL.Path == "/api/cli/channel":
			handler = channel
		case slices.Contains(cliauth.Paths, r.URL.Path):
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
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": map[string]string{"code": "cli_not_configured", "message": "CLI configuration is invalid or unavailable; check environment identity, HTTP(S) public URL and administrator key configuration."}})
	})
}
