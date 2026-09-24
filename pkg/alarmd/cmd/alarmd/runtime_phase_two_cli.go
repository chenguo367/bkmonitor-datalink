// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	accessuq "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access/uq"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/evidenceroute"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/k8sread"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obevidence"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/platformsettings"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/publicsurface"
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
	// Metrics is this process's registry, read by metrics.get. Nil leaves
	// the operation listed and unavailable with its reason.
	Metrics prometheus.Gatherer
	// PublicWindows serves /api/windows on a restricted public surface
	// (fleet.NewPublicWindowsHandler). Nil leaves the route unserved there.
	PublicWindows http.Handler
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
//
// native is the whole API. The CLI operations read it as it is. The public
// surface serves it whole unless the configuration asks for a restricted
// surface and the CLI came up in full; restricted says which, and then the
// public surface is publicAPI's. A CLI that fails to come up leaves the
// surface open, since restricting it would leave no way in.
func buildPhaseTwoCLI(cfg config.Config, native http.Handler, catalog *controlplane.RedisCatalogRepository, progressStore *progress.Store, settings *platformsettings.Cache, facts func() *observability.RuntimeConfigFacts, control cliControlBinding) (handler http.Handler, closeCLI func() error, restricted bool) {
	if !cfg.CLI.Enabled {
		return native, func() error { return nil }, false
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
		return composeCLI(native, nil, nil), closeClients, false
	}
	bind := func(role string, connection config.RedisConnectionConfig, prefix string) obevidence.RedisBinding {
		return obevidence.RedisBinding{Client: newClient(connection), Location: obevidence.Location{Role: role, Address: redisAddress(connection), Mode: connection.Mode, DB: connection.DB, Prefix: prefix}}
	}
	options := obevidence.Options{Catalog: catalog, Progress: progressStore}
	options.SourceStrategy = bind("strategy_cache", cfg.StrategySourceRedis(), cfg.PhaseTwo.Control.StrategyCachePrefix)
	options.CMDBCache = bind("cmdb_cache", cfg.CMDBCacheRedis(), "")
	// Catalog and progress share a diagnostic runtime pool, not detector I/O.
	runtimeConnection := cfg.RuntimeStoreRedis()
	diagnosticRuntime := newClient(runtimeConnection)
	options.Published = obevidence.RedisBinding{Client: diagnosticRuntime, Location: obevidence.Location{Role: "runtime", Address: redisAddress(runtimeConnection), Mode: runtimeConnection.Mode, DB: runtimeConnection.DB, Prefix: cfg.Redis.StatePrefix}}
	options.QueryProgress = options.Published
	// Route discovery is evidence I/O too. Reuse the diagnostic runtime pool,
	// never the production ownership connection or its startup readiness path.
	routingStore, err := ownership.NewRedisStoreWithClient(diagnosticRuntime, productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "ownership"))
	if err != nil {
		return composeCLI(native, nil, nil), closeClients, false
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
	ops = append(ops, obchannel.MetricsOperations(control.Metrics)...)
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
	// The diagnosis composes the reads above, so it is registered last.
	ops = append(ops, obchannel.DiagnoseOperation(native, ops))
	channelOptions := obchannel.Options{Auth: manager, EnvironmentID: cfg.CLI.EnvironmentID, Replica: cfg.PhaseTwo.Worker.ID, Incarnation: control.Incarnation, Build: version + "/" + commit, Concurrency: 1, Operations: ops}
	if control.Server != nil {
		channelOptions.Route = func(ctx context.Context, call obchannel.Invocation) obchannel.Response {
			return router.Invoke(ctx, call)
		}
	}
	channel, err := obchannel.New(channelOptions)
	if err != nil {
		return composeCLI(native, nil, nil), closeClients, false
	}
	if control.Server != nil {
		router, err = evidenceroute.New(evidenceroute.Options{Store: routingStore, WorkerID: cfg.PhaseTwo.Worker.ID, StreamToken: control.StreamToken, EnvironmentID: cfg.CLI.EnvironmentID,
			Build: channelOptions.Build, Incarnation: control.Incarnation, CatalogRevision: channel.CatalogRevision(), Execute: channel.ExecuteEvidence})
		if err != nil {
			return composeCLI(native, nil, nil), closeClients, false
		}
		control.Server.SetEvidenceHandler(router.Handle)
	}
	if !cfg.PublicSurfaceRestrictionRequested() {
		return composeCLI(native, channel, manager.Handler()), closeClients, false
	}
	return composeCLI(publicAPI(native, control.PublicWindows), channel, manager.Handler()), closeClients, true
}

// publicSurfaceStanding is what the replica says about its public surface
// once the CLI is built. Both are named degradations and a startup warning;
// neither stops the process, since detection matters more than either.
type publicSurfaceStanding struct {
	// MetricsUnexported: the surface is restricted and there is no internal
	// listener, so /metrics is served nowhere.
	MetricsUnexported bool
	// CLIUnavailable: the configuration asks for a restricted surface and the
	// CLI did not come up, so the surface stays open -- restricting it would
	// leave no way in -- and the coordinates it carries are public.
	CLIUnavailable bool
}

func publicSurfaceStandingOf(cfg config.Config, restricted bool) publicSurfaceStanding {
	return publicSurfaceStanding{
		MetricsUnexported: restricted && cfg.HTTP.InternalListen == "",
		CLIUnavailable:    cfg.PublicSurfaceRestrictionRequested() && !restricted,
	}
}

func (standing publicSurfaceStanding) warn(logger *observability.Logger) {
	if standing.MetricsUnexported {
		logger.Warn(observability.StageStartup, observability.ResultDegraded, 0, 0,
			slog.String("reason_code", string(fleet.DegradationMetricsUnexported)),
			slog.String("detail", "the public listener is restricted and http.internal_listen is not set: /metrics is served nowhere"))
	}
	if standing.CLIUnavailable {
		logger.Warn(observability.StageStartup, observability.ResultDegraded, 0, 0,
			slog.String("reason_code", string(fleet.DegradationCLIAuthUnavailable)),
			slog.String("detail", "a CLI admin key is configured and the CLI did not come up: the public surface stays unrestricted"))
	}
}

// publicAPI is what a restricted public surface serves of the API: the
// summary without deployment coordinates and the observation windows; every
// other route is refused as moved behind the CLI session, with the way in
// named. The CLI routes -- the page's, issuing, exchange, pairing, renewal --
// are composed in front of it and stay public, so a caller whose every
// credential has expired can still get a new one.
func publicAPI(native, windows http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/health":
			publicHealth(w, r, native)
		case "/api/windows":
			if windows == nil {
				http.NotFound(w, r)
				return
			}
			windows.ServeHTTP(w, r)
		default:
			publicsurface.Refuse(w, r)
		}
	})
}

// publicHealth answers the health route with fleet.PublicHealth of what the
// API answers. Anything but a readable answer is reported in fixed words:
// the API's own error text is not public.
func publicHealth(w http.ResponseWriter, r *http.Request, native http.Handler) {
	captured := &capturedResponse{header: http.Header{}}
	native.ServeHTTP(captured, r)
	var full fleet.HealthResponse
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if captured.code != 0 && captured.code != http.StatusOK || json.Unmarshal(captured.body.Bytes(), &full) != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "health summary unavailable"})
		return
	}
	_ = json.NewEncoder(w).Encode(fleet.PublicHealth(full))
}

// capturedResponse holds a handler's answer so it can be reduced before it
// is sent.
type capturedResponse struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (c *capturedResponse) Header() http.Header         { return c.header }
func (c *capturedResponse) Write(b []byte) (int, error) { return c.body.Write(b) }
func (c *capturedResponse) WriteHeader(code int) {
	if c.code == 0 {
		c.code = code
	}
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
