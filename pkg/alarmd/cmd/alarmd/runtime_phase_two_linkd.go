package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
	"github.com/go-redis/redis/v8"
)

func newLinkdIndex(cfg config.Config, client redis.UniversalClient, connection config.RedisConnectionConfig, now func() time.Time) (*openalerts.Cache, error) {
	capacity := config.DeriveLinkdCapacity(config.DetectCapacityInputs())
	settings := cfg.PhaseTwo.Linkd
	// One read cannot monopolize the allowance for the whole worker. These
	// are scan bounds, not an assertion that an oversized SET was empty.
	source, err := openalerts.NewSetSource(client, settings.Prefix(), openalerts.ReadLimits{
		MaxMembers: max(1, capacity.Members/4), MaxBytes: max(1, capacity.Bytes/4), MaxPages: 100, PageSize: 256})
	if err != nil {
		return nil, err
	}
	subscriber, err := openalerts.NewRedisSubscriber(client, settings.Prefix(), time.Second)
	if err != nil {
		return nil, err
	}
	var reconciler openalerts.Reconciler
	if settings.ConsoleURL != "" {
		address := connection.Address
		if connection.Mode == config.RedisModeSentinel {
			address = "sentinel:" + connection.MasterName + " (" + strings.Join(connection.SentinelAddress, ", ") + ")"
		}
		sources := settings.SharedSources
		if len(sources) == 0 {
			sources = []string{settings.EventSourceID}
		}
		reconciler, err = openalerts.NewHTTPReconciler(openalerts.HTTPReconcilerOptions{BaseURL: settings.ConsoleURL,
			Username: settings.Username, Password: settings.Password, Client: &http.Client{Timeout: 5 * time.Second}, MaxResponseBytes: int64(capacity.Bytes / 4),
			Binding: openalerts.TargetBinding{EventSourceID: settings.EventSourceID, HookName: settings.HookName, KeyPrefix: settings.Prefix(),
				Address: address, Database: connection.DB, Sources: sources}})
		if err != nil {
			return nil, err
		}
	}
	return openalerts.NewIndex(openalerts.IndexOptions{Source: source, Subscriber: subscriber, Reconciler: reconciler, Now: now,
		Policy: openalerts.PolicySelfMaintain, MaxStrategies: capacity.Strategies, MaxMembers: capacity.Members, MaxBytes: capacity.Bytes,
		MaxLocalEntries: capacity.LocalEntries, ReadBatch: capacity.ReadBatch, ReconcileBatch: 1, RefreshInterval: time.Second,
		IndexInterval: time.Minute, ReconcileInterval: settings.CalibrationInterval(), CalibrationMaxAge: 2 * settings.CalibrationInterval(),
		LocalRetention: time.Minute, CycleTimeout: 5 * time.Second})
}
