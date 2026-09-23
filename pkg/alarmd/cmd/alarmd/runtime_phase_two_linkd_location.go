package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// discoverLinkdTarget asks the link which of its targets is this deployment's.
type discoverLinkdTarget func(context.Context, openalerts.HTTPReconcilerOptions) (openalerts.TargetBinding, error)

// linkdDiscoveryAttempts bounds how long startup waits on a Console that does
// not answer. Detection does not depend on the link; a process that could
// not ask keeps reading where it would have, and the difference refuses by
// name until a restart finds the Console.
const linkdDiscoveryAttempts = 3

var linkdDiscoveryPause = 2 * time.Second

// adoptLinkdLocation reads the open alert sets from where the link writes
// them. A deployment states the Console and nothing else: the link's target
// names the Redis and database its hook writes to, and when one of the Redis
// connections this process already holds is that Redis, its credentials are
// used with the link's database and prefix. The deployment therefore never
// restates the hook's Redis a second time. A stated linkd connection is left
// alone, and so is a target at a Redis this process holds no connection to:
// the reconciler then refuses with both locations named.
func adoptLinkdLocation(ctx context.Context, cfg config.Config, discover discoverLinkdTarget) config.Config {
	settings := cfg.PhaseTwo.Linkd
	if settings.ConsoleURL == "" || settings.Connection != nil {
		return cfg
	}
	options := openalerts.HTTPReconcilerOptions{BaseURL: settings.ConsoleURL, Username: settings.Username, Password: settings.Password,
		Client: &http.Client{Timeout: 5 * time.Second}, MaxResponseBytes: 1 << 20,
		Select: openalerts.TargetSelector{EventSourceID: settings.EventSourceID, HookName: settings.HookName}}
	var target openalerts.TargetBinding
	var err error
	for attempt := 0; attempt < linkdDiscoveryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return cfg
			case <-time.After(linkdDiscoveryPause):
			}
		}
		if target, err = discover(ctx, options); err == nil {
			break
		}
	}
	if err != nil {
		return cfg
	}
	for _, held := range heldRedisConnections(cfg) {
		if !strings.EqualFold(linkdLocation(held), target.Address) {
			continue
		}
		held.DB = target.Database
		cfg.PhaseTwo.Linkd.Connection = &held
		if settings.KeyPrefix == "" {
			cfg.PhaseTwo.Linkd.KeyPrefix = target.KeyPrefix
		}
		return cfg
	}
	return cfg
}

// heldRedisConnections are the Redis connections the deployment already gave
// this process, runtime Redis first.
func heldRedisConnections(cfg config.Config) []config.RedisConnectionConfig {
	held := []config.RedisConnectionConfig{cfg.RuntimeStoreRedis()}
	if group, ok := cfg.TargetGroupRedis(); ok {
		held = append(held, group)
	}
	if dynamic, ok := cfg.DynamicConfigRedis(); ok {
		held = append(held, dynamic)
	}
	held = append(held, cfg.CMDBCacheRedis())
	if strategy := cfg.PlatformCache.Strategy; strategy != nil {
		held = append(held, *strategy)
	}
	if service := cfg.Kafka.LegacyAdapter.ServiceRedis; service.Mode != "" {
		held = append(held, service)
	}
	return held
}

// linkdLocation is a connection written the way the link's Console names the
// Redis a target writes to.
func linkdLocation(connection config.RedisConnectionConfig) string {
	if connection.Mode == config.RedisModeSentinel {
		return "sentinel:" + connection.MasterName + " (" + strings.Join(connection.SentinelAddress, ", ") + ")"
	}
	return connection.Address
}
