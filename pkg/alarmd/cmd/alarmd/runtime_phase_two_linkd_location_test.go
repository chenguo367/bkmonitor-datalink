package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// linkdLocationConfig is a deployment whose runtime Redis is a sentinel and
// whose platform's own Redis - held for the dynamic groups - is a standalone
// instance, the shape where the link's hook writes to the latter.
func linkdLocationConfig() config.Config {
	var cfg config.Config
	cfg.Redis.Mode = config.RedisModeSentinel
	cfg.Redis.MasterName = "runtime-master"
	cfg.Redis.SentinelAddress = []string{"sentinel:26379"}
	cfg.Redis.Password = "runtime-secret"
	cfg.Redis.DB = 8
	prefix := "platform"
	cfg.PlatformCache.DynamicGroupKeyPrefix = &prefix
	cfg.PlatformCache.TargetGroup = &config.RedisConnectionConfig{Mode: config.RedisModeStandalone, Address: "platform-redis:6379", Password: "platform-secret"}
	cfg.PhaseTwo.Linkd.ConsoleURL = "http://console/base"
	cfg.PhaseTwo.Linkd.Username, cfg.PhaseTwo.Linkd.Password = "user", "secret"
	return cfg
}

func answering(target openalerts.TargetBinding, calls *int) discoverLinkdTarget {
	return func(_ context.Context, options openalerts.HTTPReconcilerOptions) (openalerts.TargetBinding, error) {
		*calls++
		if options.BaseURL != "http://console/base" || options.Username != "user" || options.Password != "secret" {
			return openalerts.TargetBinding{}, errors.New("unexpected options")
		}
		return target, nil
	}
}

func TestTheSetsAreReadWhereTheLinkWritesThemWithCredentialsAlreadyHeld(t *testing.T) {
	calls := 0
	cfg := adoptLinkdLocation(context.Background(), linkdLocationConfig(), answering(openalerts.TargetBinding{
		KeyPrefix: "hook:open", Address: "PLATFORM-redis:6379", Database: 8}, &calls))
	got := cfg.PhaseTwo.Linkd.Connection
	if got == nil || got.Mode != config.RedisModeStandalone || got.Address != "platform-redis:6379" || got.Password != "platform-secret" || got.DB != 8 {
		t.Fatalf("connection %+v", got)
	}
	if cfg.PhaseTwo.Linkd.Prefix() != "hook:open" || calls != 1 {
		t.Fatalf("prefix %q calls %d", cfg.PhaseTwo.Linkd.Prefix(), calls)
	}
	if cfg.PlatformCache.TargetGroup.DB != 0 {
		t.Fatal("the held connection itself was moved to the link's database")
	}
}

func TestASentinelTargetIsMatchedTheWayTheConsoleNamesIt(t *testing.T) {
	calls := 0
	cfg := adoptLinkdLocation(context.Background(), linkdLocationConfig(), answering(openalerts.TargetBinding{
		KeyPrefix: "alarmd:open_alerts", Address: "sentinel:runtime-master (sentinel:26379)", Database: 5}, &calls))
	got := cfg.PhaseTwo.Linkd.Connection
	if got == nil || got.MasterName != "runtime-master" || got.Password != "runtime-secret" || got.DB != 5 {
		t.Fatalf("connection %+v", got)
	}
}

// What is not adopted: a stated connection, a stated prefix, a Redis this
// process holds no connection to, and a Console that never answers.
func TestTheLinksLocationIsAdoptedOnlyWhenNothingElseDecidesIt(t *testing.T) {
	stated := linkdLocationConfig()
	stated.PhaseTwo.Linkd.Connection = &config.RedisConnectionConfig{Mode: config.RedisModeStandalone, Address: "stated:6379"}
	calls := 0
	if got := adoptLinkdLocation(context.Background(), stated, answering(openalerts.TargetBinding{Address: "platform-redis:6379"}, &calls)); got.PhaseTwo.Linkd.Connection.Address != "stated:6379" || calls != 0 {
		t.Fatalf("a stated connection was replaced: %+v", got.PhaseTwo.Linkd.Connection)
	}
	noConsole := linkdLocationConfig()
	noConsole.PhaseTwo.Linkd.ConsoleURL = ""
	if got := adoptLinkdLocation(context.Background(), noConsole, answering(openalerts.TargetBinding{Address: "platform-redis:6379"}, &calls)); got.PhaseTwo.Linkd.Connection != nil || calls != 0 {
		t.Fatal("adopted without a Console")
	}
	prefixed := linkdLocationConfig()
	prefixed.PhaseTwo.Linkd.KeyPrefix = "stated:prefix"
	if got := adoptLinkdLocation(context.Background(), prefixed, answering(openalerts.TargetBinding{KeyPrefix: "hook:open", Address: "platform-redis:6379", Database: 8}, &calls)); got.PhaseTwo.Linkd.Prefix() != "stated:prefix" {
		t.Fatal("a stated prefix was replaced")
	}
	if got := adoptLinkdLocation(context.Background(), linkdLocationConfig(), answering(openalerts.TargetBinding{Address: "elsewhere:6379", Database: 8}, &calls)); got.PhaseTwo.Linkd.Connection != nil {
		t.Fatalf("a Redis this process holds no credentials for was adopted: %+v", got.PhaseTwo.Linkd.Connection)
	}
	defer func(pause time.Duration) { linkdDiscoveryPause = pause }(linkdDiscoveryPause)
	linkdDiscoveryPause = 0
	failing := 0
	silent := func(context.Context, openalerts.HTTPReconcilerOptions) (openalerts.TargetBinding, error) {
		failing++
		return openalerts.TargetBinding{}, errors.New("console down")
	}
	if got := adoptLinkdLocation(context.Background(), linkdLocationConfig(), silent); got.PhaseTwo.Linkd.Connection != nil || failing != linkdDiscoveryAttempts {
		t.Fatalf("attempts %d", failing)
	}
}

// A Console that answers on a later attempt is still adopted.
func TestAConsoleThatAnswersOnRetryIsAdopted(t *testing.T) {
	defer func(pause time.Duration) { linkdDiscoveryPause = pause }(linkdDiscoveryPause)
	linkdDiscoveryPause = 0
	attempts := 0
	late := func(context.Context, openalerts.HTTPReconcilerOptions) (openalerts.TargetBinding, error) {
		attempts++
		if attempts < linkdDiscoveryAttempts {
			return openalerts.TargetBinding{}, errors.New("not yet")
		}
		return openalerts.TargetBinding{KeyPrefix: "alarmd:open_alerts", Address: "platform-redis:6379", Database: 8}, nil
	}
	if got := adoptLinkdLocation(context.Background(), linkdLocationConfig(), late); got.PhaseTwo.Linkd.Connection == nil || got.PhaseTwo.Linkd.Connection.DB != 8 {
		t.Fatal("a late answer was not adopted")
	}
}
